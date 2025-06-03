from fastapi import APIRouter, Query, HTTPException, Path
from typing import Dict, List, Optional
from app.services.kommo_api import KommoAPI
import traceback

router = APIRouter(prefix="/leads", tags=["Leads"])
api = KommoAPI()

@router.get("/")
async def get_all_leads(
    limit: int = Query(250, description="Número máximo de leads a retornar"),
    page: int = Query(1, description="Página de resultados"),
    with_params: Optional[str] = Query(None, description="Parâmetros adicionais (contacts,source_id,catalog_elements,loss_reason)")
):
    """Retorna uma lista de leads"""
    try:
        params = {"limit": limit, "page": page}
        
        if with_params:
            params["with"] = with_params
            
        data = api.get_leads(params)
        
        # Verificar se obtivemos uma resposta válida
        if not data:
            return {"_embedded": {"leads": []}, "message": "Não foi possível obter leads"}
            
        return data
    except Exception as e:
        print(f"Erro ao obter leads: {e}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/count")
async def get_leads_count():
    """Retorna o número total de leads"""
    try:
        # Abordagem 1: Usar paginação para estimar o total
        params = {"limit": 1, "page": 1}
        data = api.get_leads(params)
        
        # Verificar se obtivemos uma resposta válida
        if not data:
            return {"total_leads": 0, "message": "Não foi possível obter leads"}
        
        # Verificar se a API retorna diretamente o total
        if "_total_items" in data:
            return {"total_leads": data["_total_items"]}
        
        # Abordagem 2: Verificar informações de paginação
        links = data.get("_links", {})
        if "last" in links:
            last_link = links["last"]["href"]
            # Extrair o número da última página da URL
            import re
            page_match = re.search(r'page=(\d+)', last_link)
            if page_match:
                last_page = int(page_match.group(1))
                # Estimar o total com base na última página
                return {"total_leads": last_page * 250, "estimated": True}
        
        # Abordagem 3: Contar diretamente (pode ser lento para muitos leads)
        # Verificar se temos leads na resposta atual
        if "_embedded" in data and "leads" in data["_embedded"]:
            leads_count = len(data["_embedded"]["leads"])
            # Se tivermos leads e não houver próxima página, esse é o total
            if leads_count > 0 and "next" not in links:
                return {"total_leads": leads_count}
        
        # Se chegarmos aqui, fazemos uma abordagem mais simples
        total_leads = 0
        page = 1
        has_more = True
        
        while has_more:  # Processar todas as páginas
            data = api.get_leads({"limit": 250, "page": page})
            
            if not data or not data.get("_embedded"):
                break
                
            leads = data.get("_embedded", {}).get("leads", [])
            total_leads += len(leads)
            
            if "_links" in data and "next" in data.get("_links", {}):
                page += 1
            else:
                has_more = False
                
        return {"total_leads": total_leads, "pages_processed": page}
    except Exception as e:
        print(f"Erro ao contar leads: {e}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/by-source")
async def get_leads_by_source():
    """Retorna leads agrupados por fonte"""
    try:
        # Obter fontes para mapear IDs para nomes
        sources_data = api.get_sources()
        
        # Verificar se obtivemos uma resposta válida
        if not sources_data:
            return {"leads_by_source": {}, "message": "Não foi possível obter fontes"}
        
        sources_map = {}
        embedded = sources_data.get("_embedded", {})
        if embedded:
            sources = embedded.get("sources", [])
            for source in sources:
                source_id = source.get("id")
                if source_id is not None:
                    source_name = source.get("name", f"Fonte {source_id}")
                    sources_map[str(source_id)] = source_name
        
        # Obter leads com informações de fonte
        params = {"with": "source_id", "limit": 250}
        data = api.get_leads(params)
        
        # Verificar se obtivemos uma resposta válida
        if not data:
            return {"leads_by_source": {}, "message": "Não foi possível obter leads"}
        
        results = {}
        embedded = data.get("_embedded", {})
        if embedded:
            leads = embedded.get("leads", [])
            for lead in leads:
                source_id = lead.get("source_id")
                if source_id is not None:
                    source_id_str = str(source_id)
                    source_name = sources_map.get(source_id_str, f"Fonte {source_id}")
                    results[source_name] = results.get(source_name, 0) + 1
                else:
                    results["Sem fonte"] = results.get("Sem fonte", 0) + 1
            
        return {"leads_by_source": results}
    except Exception as e:
        print(f"Erro ao obter leads por fonte: {e}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/by-tag")
async def get_leads_by_tag():
    """Retorna leads agrupados por tag"""
    try:
        data = api.get_leads({"limit": 250})
        
        # Verificar se obtivemos uma resposta válida
        if not data:
            return {"leads_by_tag": {}, "message": "Não foi possível obter leads"}
            
        tags = {}
        embedded = data.get("_embedded", {})
        if embedded:
            leads = embedded.get("leads", [])
            for lead in leads:
                lead_embedded = lead.get("_embedded", {})
                if lead_embedded:
                    lead_tags = lead_embedded.get("tags", [])
                    for tag in lead_tags:
                        tag_name = tag.get("name", "Sem tag")
                        tags[tag_name] = tags.get(tag_name, 0) + 1
                
        # Se não encontramos nenhuma tag
        if not tags:
            tags["Sem tag"] = 0
                
        return {"leads_by_tag": tags}
    except Exception as e:
        print(f"Erro ao obter leads por tag: {e}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/by-advertisement")
async def get_leads_by_advertisement(
    field_name: str = Query("Anúncio", description="Nome do campo personalizado que armazena informações do anúncio")
):
    """Retorna leads agrupados por anúncio (com base em campo personalizado)"""
    try:
        # Obter definições de campos personalizados
        fields_data = api.get_custom_fields()
        
        # Verificar se a resposta contém dados válidos
        if not fields_data:
            return {"leads_by_advertisement": {}, "message": "Não foi possível obter campos personalizados"}
        
        embedded = fields_data.get("_embedded", {})
        if not embedded:
            return {"leads_by_advertisement": {}, "message": "Resposta da API não contém campo '_embedded'"}
            
        fields = embedded.get("custom_fields", [])
        if not fields or fields is None:
            return {"leads_by_advertisement": {}, "message": "Nenhum campo personalizado encontrado"}
        
        # Garantir que fields é uma lista
        if not isinstance(fields, list):
            return {"leads_by_advertisement": {}, "message": "Formato inválido de campos personalizados"}
        
        # Encontrar o campo personalizado pelo nome
        field_id = None
        for field in fields:
            if field.get("name", "").lower() == field_name.lower():
                field_id = field.get("id")
                break
        
        if not field_id:
            return {"leads_by_advertisement": {}, "message": f"Campo personalizado '{field_name}' não encontrado"}
        
        # Obter leads com valores de campos personalizados
        data = api.get_leads({"limit": 250})
        
        # Verificar se a resposta contém dados válidos
        if not data or not data.get("_embedded"):
            return {"leads_by_advertisement": {}, "message": "Não foi possível obter leads"}
            
        leads = data.get("_embedded", {}).get("leads", [])
        
        # Agrupar por valor do campo personalizado
        results = {}
        
        for lead in leads:
            custom_fields = lead.get("custom_fields_values", [])
            
            # Verificar se custom_fields não é None
            if not custom_fields or not isinstance(custom_fields, list):
                continue
                
            for custom_field in custom_fields:
                if custom_field.get("field_id") == field_id:
                    field_values = custom_field.get("values", [])
                    
                    for field_value in field_values:
                        value = str(field_value.get("value", ""))
                        if value:
                            results[value] = results.get(value, 0) + 1
                    break
        
        return {"leads_by_advertisement": results}
    except Exception as e:
        print(f"Erro ao processar leads por anúncio: {e}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/by-user")
async def get_leads_by_user():
    """Retorna leads agrupados por usuário responsável"""
    try:
        # Obter usuários para mapear IDs para nomes
        users_data = api.get_users()
        
        # Verificar se obtivemos uma resposta válida
        if not users_data:
            return {"leads_by_user": {}, "message": "Não foi possível obter usuários"}
            
        users_map = {}
        embedded = users_data.get("_embedded", {})
        if embedded:
            users = embedded.get("users", [])
            for user in users:
                user_id = user.get("id")
                if user_id is not None:
                    user_name = f"{user.get('name', '')} {user.get('lastname', '')}"
                    users_map[str(user_id)] = user_name.strip() or f"Usuário {user_id}"
        
        # Obter leads
        data = api.get_leads({"limit": 250})
        
        # Verificar se obtivemos uma resposta válida
        if not data:
            return {"leads_by_user": {}, "message": "Não foi possível obter leads"}
            
        results = {}
        embedded = data.get("_embedded", {})
        if embedded:
            leads = embedded.get("leads", [])
            for lead in leads:
                user_id = lead.get("responsible_user_id")
                if user_id is not None:
                    user_id_str = str(user_id)
                    user_name = users_map.get(user_id_str, f"Usuário {user_id}")
                    results[user_name] = results.get(user_name, 0) + 1
                else:
                    results["Sem responsável"] = results.get("Sem responsável", 0) + 1
            
        return {"leads_by_user": results}
    except Exception as e:
        print(f"Erro ao obter leads por usuário: {e}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/active-by-user")
async def get_active_leads_by_user():
    """Retorna leads ativos agrupados por usuário responsável"""
    try:
        # Obter usuários para mapear IDs para nomes
        users_data = api.get_users()
        
        # Verificar se obtivemos uma resposta válida
        if not users_data:
            return {"active_leads_by_user": {}, "message": "Não foi possível obter usuários"}
            
        users_map = {}
        embedded_users = users_data.get("_embedded", {})
        if embedded_users:
            users = embedded_users.get("users", [])
            for user in users:
                user_id = user.get("id")
                if user_id is not None:
                    user_name = f"{user.get('name', '')} {user.get('lastname', '')}"
                    users_map[str(user_id)] = user_name.strip() or f"Usuário {user_id}"
        
        # Obter pipelines e estágios para identificar os "ativos"
        pipelines_data = api.get_pipelines()
        
        # Verificar se obtivemos uma resposta válida
        if not pipelines_data:
            return {"active_leads_by_user": {}, "message": "Não foi possível obter pipelines"}
            
        active_statuses = []
        embedded_pipelines = pipelines_data.get("_embedded", {})
        
        if embedded_pipelines:
            pipelines = embedded_pipelines.get("pipelines", [])
            
            for pipeline in pipelines:
                pipeline_id = pipeline.get("id")
                if pipeline_id is None:
                    continue
                    
                statuses_data = api.get_pipeline_statuses(pipeline_id)
                embedded_statuses = statuses_data.get("_embedded", {})
                
                if embedded_statuses:
                    statuses = embedded_statuses.get("statuses", [])
                    
                    for status in statuses:
                        # Considerar estágios que não são nem ganho nem perdido como "ativos"
                        if status.get("type") not in ["won", "lost"]:
                            active_statuses.append({
                                "pipeline_id": pipeline_id,
                                "status_id": status.get("id")
                            })
        
        # Se não encontramos nenhum estágio ativo
        if not active_statuses:
            return {"active_leads_by_user": {}, "message": "Nenhum estágio ativo encontrado"}
            
        # Obter leads para cada estágio ativo
        results = {}
        
        for active_status in active_statuses:
            pipeline_id = active_status["pipeline_id"]
            status_id = active_status["status_id"]
            
            if pipeline_id is None or status_id is None:
                continue
                
            params = {
                "filter[statuses][0][pipeline_id]": pipeline_id,
                "filter[statuses][0][status_id]": status_id,
                "limit": 250
            }
            
            data = api.get_leads(params)
            
            if not data or not data.get("_embedded"):
                continue
                
            leads = data.get("_embedded", {}).get("leads", [])
            
            for lead in leads:
                user_id = lead.get("responsible_user_id")
                if user_id is not None:
                    user_id_str = str(user_id)
                    user_name = users_map.get(user_id_str, f"Usuário {user_id}")
                    results[user_name] = results.get(user_name, 0) + 1
        
        return {"active_leads_by_user": results}
    except Exception as e:
        print(f"Erro ao obter leads ativos por usuário: {e}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/lost-by-user")
async def get_lost_leads_by_user():
    """Retorna leads perdidos agrupados por usuário responsável"""
    try:
        # Obter usuários para mapear IDs para nomes
        users_data = api.get_users()
        
        # Verificar se obtivemos uma resposta válida
        if not users_data:
            return {"lost_leads_by_user": {}, "message": "Não foi possível obter usuários"}
            
        users_map = {}
        embedded_users = users_data.get("_embedded", {})
        if embedded_users:
            users = embedded_users.get("users", [])
            for user in users:
                user_id = user.get("id")
                if user_id is not None:
                    user_name = f"{user.get('name', '')} {user.get('lastname', '')}"
                    users_map[str(user_id)] = user_name.strip() or f"Usuário {user_id}"
        
        # Obter pipelines e estágios para identificar os "perdidos"
        pipelines_data = api.get_pipelines()
        
        # Verificar se obtivemos uma resposta válida
        if not pipelines_data:
            return {"lost_leads_by_user": {}, "message": "Não foi possível obter pipelines"}
            
        lost_statuses = []
        embedded_pipelines = pipelines_data.get("_embedded", {})
        
        if embedded_pipelines:
            pipelines = embedded_pipelines.get("pipelines", [])
            
            for pipeline in pipelines:
                pipeline_id = pipeline.get("id")
                if pipeline_id is None:
                    continue
                    
                statuses_data = api.get_pipeline_statuses(pipeline_id)
                embedded_statuses = statuses_data.get("_embedded", {})
                
                if embedded_statuses:
                    statuses = embedded_statuses.get("statuses", [])
                    
                    for status in statuses:
                        if status.get("type") == "lost":
                            lost_statuses.append({
                                "pipeline_id": pipeline_id,
                                "status_id": status.get("id")
                            })
        
        # Se não encontramos nenhum estágio perdido
        if not lost_statuses:
            return {"lost_leads_by_user": {}, "message": "Nenhum estágio perdido encontrado"}
            
        # Obter leads para cada estágio perdido
        results = {}
        
        for lost_status in lost_statuses:
            pipeline_id = lost_status["pipeline_id"]
            status_id = lost_status["status_id"]
            
            if pipeline_id is None or status_id is None:
                continue
                
            params = {
                "filter[statuses][0][pipeline_id]": pipeline_id,
                "filter[statuses][0][status_id]": status_id,
                "limit": 250
            }
            
            data = api.get_leads(params)
            
            if not data or not data.get("_embedded"):
                continue
                
            leads = data.get("_embedded", {}).get("leads", [])
            
            for lead in leads:
                user_id = lead.get("responsible_user_id")
                if user_id is not None:
                    user_id_str = str(user_id)
                    user_name = users_map.get(user_id_str, f"Usuário {user_id}")
                    results[user_name] = results.get(user_name, 0) + 1
        
        return {"lost_leads_by_user": results}
    except Exception as e:
        print(f"Erro ao obter leads perdidos por usuário: {e}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/by-stage")
async def get_leads_by_stage():
    """Retorna leads agrupados por estágio do pipeline"""
    try:
        # Obter pipelines e estágios
        pipelines_data = api.get_pipelines()
        
        # Verificar se obtivemos uma resposta válida
        if not pipelines_data:
            return {"leads_by_stage": {}, "message": "Não foi possível obter pipelines"}
            
        pipeline_stages_map = {}
        embedded_pipelines = pipelines_data.get("_embedded", {})
        
        if embedded_pipelines:
            pipelines = embedded_pipelines.get("pipelines", [])
            
            for pipeline in pipelines:
                pipeline_id = pipeline.get("id")
                if pipeline_id is None:
                    continue
                    
                pipeline_name = pipeline.get("name", f"Pipeline {pipeline_id}")
                
                statuses_data = api.get_pipeline_statuses(pipeline_id)
                embedded_statuses = statuses_data.get("_embedded", {})
                
                if embedded_statuses:
                    statuses = embedded_statuses.get("statuses", [])
                    
                    for status in statuses:
                        status_id = status.get("id")
                        if status_id is not None:
                            status_name = status.get("name", f"Estágio {status_id}")
                            
                            key = f"{pipeline_id}_{status_id}"
                            pipeline_stages_map[key] = f"{pipeline_name} - {status_name}"
        
        # Obter leads
        data = api.get_leads({"limit": 250})
        
        # Verificar se obtivemos uma resposta válida
        if not data:
            return {"leads_by_stage": {}, "message": "Não foi possível obter leads"}
            
        stages = {}
        embedded = data.get("_embedded", {})
        
        if embedded:
            leads = embedded.get("leads", [])
            
            for lead in leads:
                pipeline_id = lead.get("pipeline_id")
                status_id = lead.get("status_id")
                
                if pipeline_id is not None and status_id is not None:
                    key = f"{pipeline_id}_{status_id}"
                    stage_name = pipeline_stages_map.get(key, f"Pipeline {pipeline_id} - Estágio {status_id}")
                    
                    stages[stage_name] = stages.get(stage_name, 0) + 1
            
        return {"leads_by_stage": stages}
    except Exception as e:
        print(f"Erro ao obter leads por estágio: {e}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/by-status")
async def get_leads_by_status():
    """Retorna leads agrupados por status (won, lost, active)"""
    try:
        # Obter pipelines e estágios para categorizar
        pipelines_data = api.get_pipelines()
        
        if not pipelines_data:
            return {"leads_by_status": {"won": 0, "lost": 0, "active": 0}, "message": "Não foi possível obter pipelines"}
            
        status_categories = {"won": [], "lost": [], "active": []}
        embedded_pipelines = pipelines_data.get("_embedded", {})
        
        if embedded_pipelines:
            pipelines = embedded_pipelines.get("pipelines", [])
            
            for pipeline in pipelines:
                pipeline_id = pipeline.get("id")
                if pipeline_id is None:
                    continue
                    
                statuses_data = api.get_pipeline_statuses(pipeline_id)
                embedded_statuses = statuses_data.get("_embedded", {})
                
                if embedded_statuses:
                    statuses = embedded_statuses.get("statuses", [])
                    
                    for status in statuses:
                        status_id = status.get("id")
                        status_type = status.get("type", "active")
                        
                        if status_type == "won":
                            status_categories["won"].append(status_id)
                        elif status_type == "lost":
                            status_categories["lost"].append(status_id)
                        else:
                            status_categories["active"].append(status_id)
        
        # Contar leads por categoria
        results = {"won": 0, "lost": 0, "active": 0}
        
        for category, status_ids in status_categories.items():
            if status_ids:
                params = {
                    'filter[statuses]': status_ids,
                    'limit': 1
                }
                
                data = api.get_leads(params)
                
                # Tentar obter o total de diferentes formas
                if data:
                    # Método 1: _total_items
                    if "_total_items" in data:
                        results[category] = data["_total_items"]
                    # Método 2: Contar páginas
                    elif "_links" in data and "last" in data["_links"]:
                        import re
                        last_link = data["_links"]["last"]["href"]
                        page_match = re.search(r'page=(\d+)', last_link)
                        if page_match:
                            last_page = int(page_match.group(1))
                            results[category] = last_page * 250  # Estimativa
                    # Método 3: Contar diretamente
                    else:
                        total = 0
                        page = 1
                        while True:
                            params['page'] = page
                            params['limit'] = 250
                            data = api.get_leads(params)
                            
                            if not data or not data.get("_embedded"):
                                break
                                
                            leads = data.get("_embedded", {}).get("leads", [])
                            total += len(leads)
                            
                            if not data.get("_links", {}).get("next"):
                                break
                            page += 1
                        
                        results[category] = total
        
        return {"leads_by_status": results}
    except Exception as e:
        print(f"Erro ao obter leads por status: {e}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/recent")
async def get_recent_leads(
    days: int = Query(7, description="Número de dias para considerar como recente")
):
    """Retorna leads criados recentemente"""
    try:
        from datetime import datetime, timedelta
        
        # Calcular timestamp de corte
        cutoff_date = datetime.now() - timedelta(days=days)
        cutoff_timestamp = int(cutoff_date.timestamp())
        
        # Buscar leads com filtro de data
        params = {
            'filter[created_at][from]': cutoff_timestamp,
            'limit': 250,
            'order[created_at]': 'desc'
        }
        
        data = api.get_leads(params)
        
        if not data:
            return {"recent_leads": [], "total": 0, "days": days}
            
        leads = []
        if "_embedded" in data:
            raw_leads = data.get("_embedded", {}).get("leads", [])
            
            # Formatar leads para retorno
            for lead in raw_leads:
                leads.append({
                    "id": lead.get("id"),
                    "name": lead.get("name"),
                    "price": lead.get("price", 0),
                    "created_at": lead.get("created_at"),
                    "responsible_user_id": lead.get("responsible_user_id"),
                    "status_id": lead.get("status_id"),
                    "pipeline_id": lead.get("pipeline_id")
                })
        
        return {
            "recent_leads": leads,
            "total": len(leads),
            "days": days,
            "cutoff_date": cutoff_date.isoformat()
        }
    except Exception as e:
        print(f"Erro ao obter leads recentes: {e}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/sources")
async def get_leads_sources():
    """Retorna lista de fontes de leads com estatísticas"""
    try:
        # Primeiro obter as fontes
        sources_data = api.get_sources()
        
        if not sources_data:
            return {"sources": [], "message": "Não foi possível obter fontes"}
        
        sources_list = []
        sources_map = {}
        
        # Processar fontes
        embedded = sources_data.get("_embedded", {})
        if embedded:
            sources = embedded.get("sources", [])
            for source in sources:
                source_id = source.get("id")
                if source_id is not None:
                    source_info = {
                        "id": source_id,
                        "name": source.get("name", f"Fonte {source_id}"),
                        "external_id": source.get("external_id"),
                        "leads_count": 0
                    }
                    sources_list.append(source_info)
                    sources_map[str(source_id)] = source_info
        
        # Contar leads por fonte
        params = {"with": "source_id", "limit": 250}
        data = api.get_leads(params)
        
        if data and "_embedded" in data:
            leads = data.get("_embedded", {}).get("leads", [])
            
            for lead in leads:
                source_id = lead.get("source_id")
                if source_id is not None:
                    source_id_str = str(source_id)
                    if source_id_str in sources_map:
                        sources_map[source_id_str]["leads_count"] += 1
        
        # Ordenar por número de leads
        sources_list.sort(key=lambda x: x["leads_count"], reverse=True)
        
        return {
            "sources": sources_list,
            "total_sources": len(sources_list)
        }
    except Exception as e:
        print(f"Erro ao obter fontes de leads: {e}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/conversion-rate")
async def get_leads_conversion_rate(
    period_days: int = Query(30, description="Período em dias para análise")
):
    """Retorna taxa de conversão de leads"""
    try:
        from datetime import datetime, timedelta
        
        # Calcular período
        cutoff_date = datetime.now() - timedelta(days=period_days)
        cutoff_timestamp = int(cutoff_date.timestamp())
        
        # Obter pipelines e identificar status won
        pipelines_data = api.get_pipelines()
        won_statuses = []
        
        if pipelines_data and "_embedded" in pipelines_data:
            for pipeline in pipelines_data.get("_embedded", {}).get("pipelines", []):
                pipeline_id = pipeline.get("id")
                if pipeline_id:
                    statuses_data = api.get_pipeline_statuses(pipeline_id)
                    if statuses_data and "_embedded" in statuses_data:
                        for status in statuses_data.get("_embedded", {}).get("statuses", []):
                            if status.get("type") == "won":
                                won_statuses.append(status.get("id"))
        
        # Buscar todos os leads do período
        params = {
            'filter[created_at][from]': cutoff_timestamp,
            'limit': 250
        }
        
        all_leads_data = api.get_leads(params)
        total_leads = 0
        
        if all_leads_data and "_embedded" in all_leads_data:
            # Contar total de leads
            if "_total_items" in all_leads_data:
                total_leads = all_leads_data["_total_items"]
            else:
                # Contar manualmente
                page = 1
                while True:
                    params['page'] = page
                    data = api.get_leads(params)
                    
                    if not data or not data.get("_embedded"):
                        break
                        
                    leads = data.get("_embedded", {}).get("leads", [])
                    total_leads += len(leads)
                    
                    if not data.get("_links", {}).get("next"):
                        break
                    page += 1
        
        # Buscar leads convertidos do período
        converted_leads = 0
        
        if won_statuses:
            params = {
                'filter[statuses]': won_statuses,
                'filter[closed_at][from]': cutoff_timestamp,
                'limit': 250
            }
            
            won_leads_data = api.get_leads(params)
            
            if won_leads_data and "_embedded" in won_leads_data:
                if "_total_items" in won_leads_data:
                    converted_leads = won_leads_data["_total_items"]
                else:
                    # Contar manualmente
                    page = 1
                    while True:
                        params['page'] = page
                        data = api.get_leads(params)
                        
                        if not data or not data.get("_embedded"):
                            break
                            
                        leads = data.get("_embedded", {}).get("leads", [])
                        converted_leads += len(leads)
                        
                        if not data.get("_links", {}).get("next"):
                            break
                        page += 1
        
        # Calcular taxa de conversão
        conversion_rate = (converted_leads / total_leads * 100) if total_leads > 0 else 0
        
        return {
            "conversion_rate": round(conversion_rate, 2),
            "total_leads": total_leads,
            "converted_leads": converted_leads,
            "period_days": period_days,
            "period_start": cutoff_date.isoformat()
        }
    except Exception as e:
        print(f"Erro ao calcular taxa de conversão: {e}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

# Função auxiliar para filtrar leads por corretor (custom field)
def filter_leads_by_corretor(leads: list, corretor_name: str) -> list:
    """Filtra leads pelo campo personalizado 'Corretor' (field_id: 837920)"""
    if not corretor_name or not leads:
        return leads if leads else []
    
    filtered_leads = []
    for lead in leads:
        if not lead:  # Proteção contra leads None
            continue
            
        custom_fields = lead.get("custom_fields_values", [])
        if not custom_fields:  # Proteção contra custom_fields None
            continue
            
        for field in custom_fields:
            if not field:  # Proteção contra field None
                continue
                
            if field.get("field_id") == 837920:  # ID do campo Corretor
                values = field.get("values", [])
                if values and len(values) > 0:
                    value = values[0].get("value") if values[0] else None
                    if value == corretor_name:
                        filtered_leads.append(lead)
                        break
    
    return filtered_leads

# Função auxiliar para obter todos os leads (paginação automática)
def get_all_leads_with_custom_fields():
    """Busca todos os leads com campos personalizados"""
    try:
        all_leads = []
        page = 1
        max_pages = 10  # Limitar a 10 páginas para evitar timeout (2500 leads max)
        
        while page <= max_pages:
            params = {
                'limit': 250,
                'page': page,
                'with': 'custom_fields'
            }
            
            data = api.get_leads(params)
            
            if not data or not data.get("_embedded"):
                break
                
            leads = data.get("_embedded", {}).get("leads", [])
            if not leads:
                break
                
            all_leads.extend(leads)
            
            # Verificar se há próxima página
            if not data.get("_links", {}).get("next"):
                break
            page += 1
        
        return all_leads if all_leads else []
        
    except Exception as e:
        print(f"Erro ao buscar leads: {e}")
        # Retornar lista vazia em caso de erro para evitar NoneType
        return []

# NOVOS ENDPOINTS COM FILTRO POR CORRETOR

@router.get("/active-by-corretor")
async def get_active_leads_by_corretor(
    corretor_name: str = Query(None, description="Nome do corretor para filtrar"),
    include_all: bool = Query(False, description="Se True, retorna dados de todos os corretores")
):
    """Retorna leads ativos filtrados por corretor (custom field)"""
    try:
        # Buscar todos os leads com campos personalizados
        all_leads = get_all_leads_with_custom_fields()
        
        if include_all:
            # Retornar contagem por todos os corretores
            corretor_counts = {}
            
            for lead in all_leads:
                # Verificar se é ativo (não won e não lost)
                if lead.get("status_id") in [142, 143]:  # won ou lost
                    continue
                    
                custom_fields = lead.get("custom_fields_values", [])
                for field in custom_fields:
                    if field.get("field_id") == 837920:
                        values = field.get("values", [])
                        if values:
                            corretor = values[0].get("value", "")
                            if corretor:
                                corretor_counts[corretor] = corretor_counts.get(corretor, 0) + 1
                        break
            
            return {"active_leads_by_corretor": corretor_counts}
        
        elif corretor_name:
            # Filtrar por corretor específico
            corretor_leads = filter_leads_by_corretor(all_leads, corretor_name)
            
            # Filtrar apenas ativos
            active_leads = [lead for lead in corretor_leads if lead.get("status_id") not in [142, 143]]
            
            return {
                "corretor": corretor_name,
                "active_leads": active_leads,
                "count": len(active_leads)
            }
        
        else:
            return {"error": "Especifique corretor_name ou use include_all=true"}
            
    except Exception as e:
        print(f"Erro ao obter leads ativos por corretor: {e}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/lost-by-corretor")
async def get_lost_leads_by_corretor(
    corretor_name: str = Query(None, description="Nome do corretor para filtrar"),
    include_all: bool = Query(False, description="Se True, retorna dados de todos os corretores")
):
    """Retorna leads perdidos filtrados por corretor (custom field)"""
    try:
        all_leads = get_all_leads_with_custom_fields()
        
        if include_all:
            # Retornar contagem por todos os corretores
            corretor_counts = {}
            
            for lead in all_leads:
                # Verificar se é perdido (status lost)
                if lead.get("status_id") != 143:  # 143 = lost
                    continue
                    
                custom_fields = lead.get("custom_fields_values", [])
                for field in custom_fields:
                    if field.get("field_id") == 837920:
                        values = field.get("values", [])
                        if values:
                            corretor = values[0].get("value", "")
                            if corretor:
                                corretor_counts[corretor] = corretor_counts.get(corretor, 0) + 1
                        break
            
            return {"lost_leads_by_corretor": corretor_counts}
        
        elif corretor_name:
            # Filtrar por corretor específico
            corretor_leads = filter_leads_by_corretor(all_leads, corretor_name)
            
            # Filtrar apenas perdidos
            lost_leads = [lead for lead in corretor_leads if lead.get("status_id") == 143]
            
            return {
                "corretor": corretor_name,
                "lost_leads": lost_leads,
                "count": len(lost_leads)
            }
        
        else:
            return {"error": "Especifique corretor_name ou use include_all=true"}
            
    except Exception as e:
        print(f"Erro ao obter leads perdidos por corretor: {e}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/won-by-corretor")
async def get_won_leads_by_corretor(
    corretor_name: str = Query(None, description="Nome do corretor para filtrar"),
    include_all: bool = Query(False, description="Se True, retorna dados de todos os corretores")
):
    """Retorna leads ganhos (vendas) filtrados por corretor (custom field)"""
    try:
        all_leads = get_all_leads_with_custom_fields()
        
        if include_all:
            # Retornar contagem por todos os corretores
            corretor_counts = {}
            corretor_revenue = {}
            
            for lead in all_leads:
                # Verificar se é ganho (status won)
                if lead.get("status_id") != 142:  # 142 = won
                    continue
                    
                custom_fields = lead.get("custom_fields_values", [])
                for field in custom_fields:
                    if field.get("field_id") == 837920:
                        values = field.get("values", [])
                        if values:
                            corretor = values[0].get("value", "")
                            if corretor:
                                corretor_counts[corretor] = corretor_counts.get(corretor, 0) + 1
                                corretor_revenue[corretor] = corretor_revenue.get(corretor, 0) + (lead.get("price", 0) or 0)
                        break
            
            return {
                "won_leads_by_corretor": corretor_counts,
                "revenue_by_corretor": corretor_revenue
            }
        
        elif corretor_name:
            # Filtrar por corretor específico
            corretor_leads = filter_leads_by_corretor(all_leads, corretor_name)
            
            # Filtrar apenas ganhos
            won_leads = [lead for lead in corretor_leads if lead.get("status_id") == 142]
            total_revenue = sum(lead.get("price", 0) or 0 for lead in won_leads)
            
            return {
                "corretor": corretor_name,
                "won_leads": won_leads,
                "count": len(won_leads),
                "total_revenue": total_revenue,
                "average_deal_size": total_revenue / len(won_leads) if won_leads else 0
            }
        
        else:
            return {"error": "Especifique corretor_name ou use include_all=true"}
            
    except Exception as e:
        print(f"Erro ao obter leads ganhos por corretor: {e}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/by-stage-corretor")
async def get_leads_by_stage_and_corretor(
    corretor_name: str = Query(None, description="Nome do corretor para filtrar"),
    include_all: bool = Query(False, description="Se True, retorna dados de todos os corretores")
):
    """Retorna leads por etapa do funil filtrados por corretor"""
    try:
        # Buscar pipelines para mapear status
        pipelines_data = api.get_pipelines()
        stage_map = {}
        
        if pipelines_data and "_embedded" in pipelines_data:
            for pipeline in pipelines_data.get("_embedded", {}).get("pipelines", []):
                pipeline_id = pipeline.get("id")
                pipeline_name = pipeline.get("name", f"Pipeline {pipeline_id}")
                
                if pipeline_id:
                    statuses = pipeline.get("_embedded", {}).get("statuses", [])
                    for status in statuses:
                        status_id = status.get("id")
                        status_name = status.get("name", f"Status {status_id}")
                        stage_map[status_id] = f"{pipeline_name} - {status_name}"
        
        all_leads = get_all_leads_with_custom_fields()
        
        if include_all:
            # Retornar contagem por todos os corretores e estágios
            corretor_stages = {}
            
            for lead in all_leads:
                status_id = lead.get("status_id")
                stage_name = stage_map.get(status_id, f"Status {status_id}")
                
                custom_fields = lead.get("custom_fields_values", [])
                for field in custom_fields:
                    if field.get("field_id") == 837920:
                        values = field.get("values", [])
                        if values:
                            corretor = values[0].get("value", "")
                            if corretor:
                                if corretor not in corretor_stages:
                                    corretor_stages[corretor] = {}
                                corretor_stages[corretor][stage_name] = corretor_stages[corretor].get(stage_name, 0) + 1
                        break
            
            return {"leads_by_stage_and_corretor": corretor_stages}
        
        elif corretor_name:
            # Filtrar por corretor específico
            corretor_leads = filter_leads_by_corretor(all_leads, corretor_name)
            
            # Agrupar por estágio
            stage_counts = {}
            for lead in corretor_leads:
                status_id = lead.get("status_id")
                stage_name = stage_map.get(status_id, f"Status {status_id}")
                stage_counts[stage_name] = stage_counts.get(stage_name, 0) + 1
            
            return {
                "corretor": corretor_name,
                "leads_by_stage": stage_counts
            }
        
        else:
            return {"error": "Especifique corretor_name ou use include_all=true"}
            
    except Exception as e:
        print(f"Erro ao obter leads por estágio e corretor: {e}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/conversion-rate-by-corretor")
async def get_conversion_rate_by_corretor(
    corretor_name: str = Query(None, description="Nome do corretor para filtrar"),
    period_days: int = Query(30, description="Período em dias para análise"),
    include_all: bool = Query(False, description="Se True, retorna dados de todos os corretores")
):
    """Retorna taxa de conversão filtrada por corretor"""
    try:
        from datetime import datetime, timedelta
        
        # Calcular timestamp de corte
        cutoff_date = datetime.now() - timedelta(days=period_days)
        cutoff_timestamp = int(cutoff_date.timestamp())
        
        all_leads = get_all_leads_with_custom_fields()
        
        # Filtrar leads do período
        period_leads = [
            lead for lead in all_leads 
            if lead.get("created_at", 0) >= cutoff_timestamp
        ]
        
        if include_all:
            # Calcular para todos os corretores
            corretor_stats = {}
            
            for lead in period_leads:
                custom_fields = lead.get("custom_fields_values", [])
                for field in custom_fields:
                    if field.get("field_id") == 837920:
                        values = field.get("values", [])
                        if values:
                            corretor = values[0].get("value", "")
                            if corretor:
                                if corretor not in corretor_stats:
                                    corretor_stats[corretor] = {"total": 0, "converted": 0}
                                
                                corretor_stats[corretor]["total"] += 1
                                if lead.get("status_id") == 142:  # won
                                    corretor_stats[corretor]["converted"] += 1
                        break
            
            # Calcular taxas de conversão
            for corretor in corretor_stats:
                total = corretor_stats[corretor]["total"]
                converted = corretor_stats[corretor]["converted"]
                corretor_stats[corretor]["conversion_rate"] = (converted / total * 100) if total > 0 else 0
            
            return {
                "conversion_rates_by_corretor": corretor_stats,
                "period_days": period_days
            }
        
        elif corretor_name:
            # Filtrar por corretor específico
            corretor_leads = filter_leads_by_corretor(period_leads, corretor_name)
            
            total_leads = len(corretor_leads)
            converted_leads = len([lead for lead in corretor_leads if lead.get("status_id") == 142])
            conversion_rate = (converted_leads / total_leads * 100) if total_leads > 0 else 0
            
            return {
                "corretor": corretor_name,
                "conversion_rate": round(conversion_rate, 2),
                "total_leads": total_leads,
                "converted_leads": converted_leads,
                "period_days": period_days
            }
        
        else:
            return {"error": "Especifique corretor_name ou use include_all=true"}
            
    except Exception as e:
        print(f"Erro ao calcular taxa de conversão por corretor: {e}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/salesbot-recovery-by-corretor")
async def get_salesbot_recovery_by_corretor(
    corretor_name: str = Query(None, description="Nome do corretor para filtrar"),
    recovery_tag: str = Query("Recuperado pelo SalesBot", description="Nome da tag de recuperação"),
    include_all: bool = Query(False, description="Se True, retorna dados de todos os corretores")
):
    """Retorna leads recuperados pelo SalesBot filtrados por corretor"""
    try:
        all_leads = get_all_leads_with_custom_fields()
        
        if include_all:
            # Retornar dados de todos os corretores
            corretor_stats = {}
            
            for lead in all_leads:
                # Verificar se tem a tag de recuperação
                tags = lead.get("_embedded", {}).get("tags", [])
                has_recovery_tag = any(tag.get("name") == recovery_tag for tag in tags)
                
                if not has_recovery_tag:
                    continue
                
                custom_fields = lead.get("custom_fields_values", [])
                for field in custom_fields:
                    if field.get("field_id") == 837920:
                        values = field.get("values", [])
                        if values:
                            corretor = values[0].get("value", "")
                            if corretor:
                                if corretor not in corretor_stats:
                                    corretor_stats[corretor] = {
                                        "recovered_leads": 0,
                                        "recovered_converted": 0,
                                        "recovery_conversion_rate": 0
                                    }
                                
                                corretor_stats[corretor]["recovered_leads"] += 1
                                if lead.get("status_id") == 142:  # won
                                    corretor_stats[corretor]["recovered_converted"] += 1
                        break
            
            # Calcular taxas de conversão da recuperação
            for corretor in corretor_stats:
                recovered = corretor_stats[corretor]["recovered_leads"]
                converted = corretor_stats[corretor]["recovered_converted"]
                corretor_stats[corretor]["recovery_conversion_rate"] = (converted / recovered * 100) if recovered > 0 else 0
            
            return {
                "salesbot_recovery_by_corretor": corretor_stats,
                "recovery_tag": recovery_tag
            }
        
        elif corretor_name:
            # Filtrar por corretor específico
            corretor_leads = filter_leads_by_corretor(all_leads, corretor_name)
            
            # Filtrar leads com tag de recuperação
            recovered_leads = []
            for lead in corretor_leads:
                tags = lead.get("_embedded", {}).get("tags", [])
                if any(tag.get("name") == recovery_tag for tag in tags):
                    recovered_leads.append(lead)
            
            recovered_converted = len([lead for lead in recovered_leads if lead.get("status_id") == 142])
            recovery_rate = (recovered_converted / len(recovered_leads) * 100) if recovered_leads else 0
            
            return {
                "corretor": corretor_name,
                "recovered_leads": recovered_leads,
                "recovered_count": len(recovered_leads),
                "recovered_converted": recovered_converted,
                "recovery_conversion_rate": round(recovery_rate, 2),
                "recovery_tag": recovery_tag
            }
        
        else:
            return {"error": "Especifique corretor_name ou use include_all=true"}
            
    except Exception as e:
        print(f"Erro ao obter recuperação SalesBot por corretor: {e}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))