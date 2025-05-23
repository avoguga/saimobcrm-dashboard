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
        
        while has_more and page <= 5:  # Limitar a 5 páginas para evitar requisições demais
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
        if not fields:
            return {"leads_by_advertisement": {}, "message": "Nenhum campo personalizado encontrado"}
        
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