from fastapi import APIRouter, HTTPException, Query
from typing import Optional
import asyncio
import logging
from datetime import datetime, timedelta
from app.services.kommo_api import KommoAPI
from app.utils.date_helpers import validate_sale_in_period, get_lead_closure_date, extract_custom_field_value, format_proposal_date, format_timestamp_brazil, BRAZIL_TIMEZONE
import config

router = APIRouter()
logger = logging.getLogger(__name__)

# Instanciar APIs uma vez
kommo_api = KommoAPI()

# Função auxiliar global para buscar dados com fallback
def safe_get_data(func, *args, **kwargs):
    try:
        result = func(*args, **kwargs)
        logger.info(f"Safe get data resultado: {type(result)}, função: {func.__name__ if hasattr(func, '__name__') else 'unknown'}")
        if result is None:
            logger.warning(f"Função {func.__name__ if hasattr(func, '__name__') else 'unknown'} retornou None")
            return {}
        if not isinstance(result, dict):
            logger.warning(f"Função {func.__name__ if hasattr(func, '__name__') else 'unknown'} retornou tipo inválido: {type(result)}")
            return {}
        return result
    except Exception as e:
        logger.error(f"Erro ao buscar dados de {func.__name__ if hasattr(func, '__name__') else 'unknown'}: {e}")
        return {}

@router.get("/marketing-complete")
async def get_marketing_dashboard_complete(
    days: int = Query(90, description="Período em dias para análise"),
    start_date: Optional[str] = Query(None, description="Data de início (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="Data de fim (YYYY-MM-DD)"),
    fonte: Optional[str] = Query(None, description="Fonte para filtrar dados"),
):
    """
    Endpoint otimizado que retorna todos os dados do dashboard de marketing
    em uma única requisição, reduzindo latência e melhorando performance.
    
    Equivale a todas as chamadas que o frontend fazia separadamente:
    - /leads/count
    - /leads/by-source  
    - /leads/by-tag
    - /analytics/trends
    """
    try:
        logger.info(f"Iniciando dashboard marketing completo para {days} dias, start_date: {start_date}, end_date: {end_date}, fonte: {fonte}")
        
        # Calcular parâmetros de tempo
        import time
        
        if start_date and end_date:
            # Usar datas específicas
            try:
                start_dt = datetime.strptime(start_date, '%Y-%m-%d')
                end_dt = datetime.strptime(end_date, '%Y-%m-%d')
                end_dt = end_dt.replace(hour=23, minute=59, second=59)  # Fim do dia
                start_time = int(start_dt.timestamp())
                end_time = int(end_dt.timestamp())
            except ValueError as date_error:
                logger.error(f"Erro de validação de data: {date_error}")
                raise HTTPException(status_code=400, detail="Formato de data inválido. Use YYYY-MM-DD")
        else:
            # Usar período relativo em dias
            end_time = int(time.time())
            start_time = end_time - (days * 24 * 60 * 60)
        
        # CORREÇÃO: Buscar apenas pipelines Vendas + Remarketing (igual charts/leads-by-user)
        # IDs importantes (definidos depois no código)
        PIPELINE_VENDAS = 10516987
        PIPELINE_REMARKETING = 11059911
        
        # Buscar leads de ambos os pipelines separadamente
        leads_vendas_params = {
            "filter[pipeline_id]": PIPELINE_VENDAS,
            "filter[created_at][from]": start_time, 
            "filter[created_at][to]": end_time, 
            "limit": 250,
            "with": "contacts,tags,custom_fields_values"
        }
        
        leads_remarketing_params = {
            "filter[pipeline_id]": PIPELINE_REMARKETING,
            "filter[created_at][from]": start_time, 
            "filter[created_at][to]": end_time, 
            "limit": 250,
            "with": "contacts,tags,custom_fields_values"
        }
        
        # Buscar dados de ambos os pipelines - USAR PAGINAÇÃO COMPLETA
        try:
            leads_vendas_all = kommo_api.get_all_leads_old(leads_vendas_params)
            leads_vendas_data = {"_embedded": {"leads": leads_vendas_all}}
            logger.info(f"Leads Vendas (paginação completa): {len(leads_vendas_all)}")
        except Exception as e:
            logger.error(f"Erro ao buscar leads vendas: {e}")
            leads_vendas_data = {"_embedded": {"leads": []}}

        try:
            leads_remarketing_all = kommo_api.get_all_leads_old(leads_remarketing_params)
            leads_remarketing_data = {"_embedded": {"leads": leads_remarketing_all}}
            logger.info(f"Leads Remarketing (paginação completa): {len(leads_remarketing_all)}")
        except Exception as e:
            logger.error(f"Erro ao buscar leads remarketing: {e}")
            leads_remarketing_data = {"_embedded": {"leads": []}}
        
        # Combinar leads de ambos os pipelines
        combined_leads = []
        if leads_vendas_data and "_embedded" in leads_vendas_data:
            vendas_leads = leads_vendas_data["_embedded"].get("leads", [])
            if isinstance(vendas_leads, list):
                combined_leads.extend(vendas_leads)
                
        if leads_remarketing_data and "_embedded" in leads_remarketing_data:
            remarketing_leads = leads_remarketing_data["_embedded"].get("leads", [])
            if isinstance(remarketing_leads, list):
                combined_leads.extend(remarketing_leads)
        
        # Criar estrutura similar ao original para compatibilidade
        leads_data = {"_embedded": {"leads": combined_leads}}
        sources_data = safe_get_data(kommo_api.get_sources)
        tags_data = safe_get_data(kommo_api.get_tags)
        
        # Processar contagem de leads
        total_leads = 0
        if leads_data and "_embedded" in leads_data:
            all_leads = leads_data["_embedded"].get("leads", [])
            if fonte and isinstance(fonte, str) and fonte.strip():
                # Contar apenas leads da fonte especificada
                filtered_leads = []
                for lead in all_leads:
                    fonte_name = None
                    custom_fields = lead.get("custom_fields_values", [])
                    
                    # Buscar custom field "Fonte" (ID: 837886)
                    if custom_fields and isinstance(custom_fields, list):
                        for field in custom_fields:
                            if field and field.get("field_id") == 837886:
                                values = field.get("values", [])
                                if values and len(values) > 0:
                                    fonte_name = values[0].get("value")
                                    break
                    
                    # Suporta múltiplas fontes separadas por vírgula
                    if fonte and ',' in fonte:
                        fontes_list = [f.strip() for f in fonte.split(',')]
                        if fonte_name in fontes_list:
                            filtered_leads.append(lead)
                    else:
                        if fonte_name == fonte:
                            filtered_leads.append(lead)
                
                total_leads = len(filtered_leads)
            else:
                total_leads = len(all_leads)
                if "_total_items" in leads_data:
                    total_leads = leads_data["_total_items"]
        
        # Processar leads por fonte usando CUSTOM FIELD "Fonte" (ID: 837886) - mais detalhado
        leads_by_source_array = []
        
        # Preparar mapeamento de sources uma vez só (fallback)
        sources_map = {}
        if sources_data and "_embedded" in sources_data:
            for source in sources_data["_embedded"].get("sources", []):
                sources_map[source["id"]] = source["name"]
        
        if leads_data and "_embedded" in leads_data:
            source_counts = {}
            
            for lead in leads_data["_embedded"].get("leads", []):
                fonte_name = None
                custom_fields = lead.get("custom_fields_values", [])
                
                # Buscar custom field "Fonte" (ID: 837886)
                if custom_fields and isinstance(custom_fields, list):
                    for field in custom_fields:
                        if field and field.get("field_id") == 837886:  # ID do campo Fonte
                            values = field.get("values", [])
                            if values and len(values) > 0:
                                fonte_name = values[0].get("value")
                                break
                
                # Se não tiver custom field, usar source_id padrão como fallback
                if not fonte_name:
                    # Tentar obter source_id do lead
                    source_id = lead.get("source_id")
                    if not source_id and lead.get("_embedded", {}).get("source"):
                        source_id = lead["_embedded"]["source"]["id"]
                        
                    if source_id and source_id in sources_map:
                        fonte_name = sources_map[source_id]
                    else:
                        fonte_name = "Fonte Desconhecida"
                
                # Filtrar por fonte se especificado
                if fonte and isinstance(fonte, str) and fonte.strip() and fonte_name != fonte:
                    continue
                
                source_counts[fonte_name] = source_counts.get(fonte_name, 0) + 1
            
            # Ordenar por quantidade (mais importantes primeiro)
            if source_counts:
                sorted_sources = sorted(source_counts.items(), key=lambda x: x[1], reverse=True)
                leads_by_source_array = [
                    {"name": name, "value": count}
                    for name, count in sorted_sources
                ]
                
            logger.info(f"Leads por fonte (custom field): {len(leads_by_source_array)} fontes encontradas")
        
        # Processar leads por tag - similar ao endpoint /leads/by-tag  
        leads_by_tag_array = []
        tags_map = {}
        
        if tags_data and "_embedded" in tags_data:
            # Mapear IDs de tag para nomes
            for tag in tags_data["_embedded"].get("tags", []):
                tags_map[tag["id"]] = tag["name"]
        
        if leads_data and "_embedded" in leads_data:
            tag_counts = {}
            for lead in leads_data["_embedded"].get("leads", []):
                lead_tags = lead.get("_embedded", {}).get("tags", [])
                if lead_tags:
                    for tag in lead_tags:
                        tag_id = tag.get("id")
                        if tag_id:
                            tag_name = tags_map.get(tag_id, f"Tag {tag_id}")
                            tag_counts[tag_name] = tag_counts.get(tag_name, 0) + 1
            
            leads_by_tag_array = [
                {"name": name, "value": count}
                for name, count in tag_counts.items()
            ]
        
        # Métricas do Facebook removidas - dados zerados
        facebook_metrics = {
            "impressions": 0,
            "reach": 0,
            "clicks": 0,
            "ctr": 0,
            "cpc": 0,
            "totalSpent": 0,
            "costPerLead": 0,
            "engagement": {
                "likes": 0,
                "comments": 0,
                "shares": 0,
                "videoViews": 0,
                "profileVisits": 0
            }
        }
        
        # Tendência simples baseada nos leads obtidos
        metrics_trend = []
        
        # Montar resposta completa
        response = {
            "totalLeads": total_leads,
            "leadsBySource": leads_by_source_array,  # USANDO CUSTOM FIELD "Fonte"
            "leadsByTag": leads_by_tag_array,
            "leadsByAd": [],  # TODO: Implementar por anúncio específico
            "facebookMetrics": facebook_metrics,
            "facebookCampaigns": [],
            "metricsTrend": metrics_trend,
            "customFields": {  # NOVO: Custom fields implementados
                "fonte": leads_by_source_array,
                "available_fontes": [
                    "Tráfego Meta", "Escritório Patacho", "Canal Pro", "Site", 
                    "Redes Sociais", "Parceria com Construtoras", "Ação de Panfletagem",
                    "Eletromídia", "Orgânico", "LandingPage", "Chamada", "Anúncio Físico",
                    "Não atribuído", "Google", "Cliente", "Grupo Zap", "Celular do Plantão",
                    "Tráfego Séculos"
                ]
            },
            "analyticsOverview": None,  # Removido por otimização
            
            # Metadados de performance
            "_metadata": {
                "period_days": days,
                "generated_at": datetime.now().isoformat(),
                "data_sources": ["kommo_api"],
                "optimized": True,
                "single_request": True,
                "custom_fields_implemented": True,
                "facebook_enhanced": False
            }
        }
        
        logger.info(f"Dashboard marketing completo gerado com sucesso: {total_leads} leads, {len(leads_by_source_array)} fontes, {len(leads_by_tag_array)} tags")
        return response
        
    except HTTPException:
        # Re-raise HTTPExceptions (como 400 Bad Request) sem modificar
        raise
    except Exception as e:
        logger.error(f"Erro ao gerar dashboard marketing completo: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")


@router.get("/sales-complete")
async def get_sales_dashboard_complete(
    days: int = Query(90, description="Período em dias para análise"),
    corretor: Optional[str] = Query(None, description="Nome do corretor para filtrar dados"),
    start_date: Optional[str] = Query(None, description="Data de início (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="Data de fim (YYYY-MM-DD)"),
    fonte: Optional[str] = Query(None, description="Fonte para filtrar dados"),
    produto: Optional[str] = Query(None, description="Produto para filtrar dados"),
):
    """
    Endpoint otimizado que retorna todos os dados do dashboard de vendas
    em uma única requisição, com suporte a filtro por corretor.
    
    Equivale a todas as chamadas que o frontend fazia separadamente:
    - /leads/count
    - /leads/by-stage
    - /analytics/overview
    - /analytics/funnel
    - /analytics/lead-cycle-time
    - /analytics/win-rate
    - /analytics/average-deal-size
    - /analytics/salesbot-recovery
    - /meetings/stats
    - /corretor-dashboard/comparison
    - /analytics/team-performance
    """
    try:
        logger.info(f"Iniciando dashboard vendas completo para {days} dias, corretor: {corretor}, start_date: {start_date}, end_date: {end_date}, fonte: {fonte}")
        
        # Calcular parâmetros de tempo
        import time
        
        if start_date and end_date:
            # Usar datas específicas
            try:
                start_dt = datetime.strptime(start_date, '%Y-%m-%d')
                end_dt = datetime.strptime(end_date, '%Y-%m-%d')
                end_dt = end_dt.replace(hour=23, minute=59, second=59)  # Fim do dia
                start_time = int(start_dt.timestamp())
                end_time = int(end_dt.timestamp())
            except ValueError as date_error:
                logger.error(f"Erro de validação de data: {date_error}")
                raise HTTPException(status_code=400, detail="Formato de data inválido. Use YYYY-MM-DD")
        else:
            # Usar período relativo em dias
            end_time = int(time.time())
            start_time = end_time - (days * 24 * 60 * 60)
        
        # CORREÇÃO: Buscar leads APENAS dos pipelines Vendas + Remarketing (igual charts/leads-by-user)
        # IDs dos pipelines necessários
        PIPELINE_VENDAS = 10516987
        PIPELINE_REMARKETING = 11059911
        CUSTOM_FIELD_CORRETOR = 837920
        CUSTOM_FIELD_FONTE = 837886
        CUSTOM_FIELD_PRODUTO = 857264
        
        # Parâmetros para buscar leads do Funil de Vendas
        leads_vendas_params = {
            "filter[pipeline_id]": PIPELINE_VENDAS,
            "filter[created_at][from]": start_time,
            "filter[created_at][to]": end_time,
            "limit": 250,
            "with": "contacts,tags,custom_fields_values"
        }
        
        # Parâmetros para buscar leads do Remarketing
        leads_remarketing_params = {
            "filter[pipeline_id]": PIPELINE_REMARKETING,
            "filter[created_at][from]": start_time,
            "filter[created_at][to]": end_time,
            "limit": 250,
            "with": "contacts,tags,custom_fields_values"
        }
        
        # Calcular filtro de reuniões: incluir 23:59 do dia anterior (igual charts/leads-by-user)
        meetings_start_time = start_time - (24 * 60 * 60) + (23 * 60 * 60 + 59 * 60)  # -1 dia + 23:59
        
        # Parâmetros para buscar reuniões REAIS
        tasks_params = {
            'filter[task_type_id]': 2,  # Tipo de tarefa: reunião
            'filter[is_completed]': 1,  # Apenas concluídas
            'filter[complete_till][from]': meetings_start_time,  # IGUAL CHARTS
            'filter[complete_till][to]': end_time,
            'limit': 250
        }
        
        # Buscar dados REAIS - USAR PAGINAÇÃO COMPLETA
        try:
            all_leads_vendas = kommo_api.get_all_leads_old(leads_vendas_params)
            leads_vendas_data = {"_embedded": {"leads": all_leads_vendas}}
            logger.info(f"Leads Vendas (paginação completa): {len(all_leads_vendas)}")
        except Exception as e:
            logger.error(f"Erro ao buscar leads vendas: {e}")
            all_leads_vendas = []
            
        try:
            all_leads_remarketing = kommo_api.get_all_leads_old(leads_remarketing_params)
            leads_remarketing_data = {"_embedded": {"leads": all_leads_remarketing}}
            logger.info(f"Leads Remarketing (paginação completa): {len(all_leads_remarketing)}")
        except Exception as e:
            logger.error(f"Erro ao buscar leads remarketing: {e}")
            all_leads_remarketing = []
        
        # Combinar leads de ambos os pipelines (igual charts/leads-by-user)
        all_leads = all_leads_vendas + all_leads_remarketing
        leads_data = {"_embedded": {"leads": all_leads}}
        logger.info(f"Total de leads combinados: {len(all_leads)}")
            
        # BUSCAR REUNIÕES REAIS usando get_all_tasks
        try:
            all_tasks = kommo_api.get_all_tasks(tasks_params)
            tasks_data = {"_embedded": {"tasks": all_tasks}}
            logger.info(f"Total de tarefas de reunião encontradas: {len(all_tasks)}")
        except Exception as e:
            logger.error(f"Erro ao buscar tarefas de reunião: {e}")
            tasks_data = {"_embedded": {"tasks": []}}
            
        # Buscar usuários para fallback
        try:
            users_data = kommo_api.get_users()
        except Exception as e:
            logger.error(f"Erro ao buscar usuarios: {e}")
            users_data = {"_embedded": {"users": []}}
        
        # Função para filtrar leads por corretor usando custom field (igual aos outros endpoints)
        def filter_leads_by_corretor(leads: list, corretor_name) -> list:
            """Filtra leads pelo campo personalizado 'Corretor' (field_id: 837920)"""
            # Garantir que corretor_name seja string
            if not corretor_name or not isinstance(corretor_name, str) or not leads:
                return leads if leads else []
            
            filtered_leads = []
            for lead in leads:
                if not lead:
                    continue
                    
                custom_fields = lead.get("custom_fields_values", [])
                if not custom_fields:
                    continue
                    
                if custom_fields and isinstance(custom_fields, list):
                    for field in custom_fields:
                        if not field:
                            continue
                        
                        if field.get("field_id") == 837920:  # ID do campo Corretor
                            values = field.get("values", [])
                            if values and len(values) > 0:
                                value = values[0].get("value") if values[0] else None
                                # Suporta múltiplos corretores separados por vírgula
                                if corretor_name and ',' in corretor_name:
                                    corretores_list = [c.strip() for c in corretor_name.split(',')]
                                    if value in corretores_list:
                                        filtered_leads.append(lead)
                                        break
                                else:
                                    if value == corretor_name:
                                        filtered_leads.append(lead)
                                        break
            
            return filtered_leads
        
        # Função para filtrar leads por fonte usando custom field
        def filter_leads_by_fonte(leads: list, fonte_name) -> list:
            """Filtra leads pelo campo personalizado 'Fonte' (field_id: 837886)"""
            # Garantir que fonte_name seja string
            if not fonte_name or not isinstance(fonte_name, str) or not leads:
                return leads if leads else []
            
            filtered_leads = []
            for lead in leads:
                if not lead:
                    continue
                    
                custom_fields = lead.get("custom_fields_values", [])
                if not custom_fields:
                    continue
                    
                if custom_fields and isinstance(custom_fields, list):
                    for field in custom_fields:
                        if not field:
                            continue
                        
                        if field.get("field_id") == 837886:  # ID do campo Fonte
                            values = field.get("values", [])
                            if values and len(values) > 0:
                                value = values[0].get("value") if values[0] else None
                                if value == fonte_name:
                                    filtered_leads.append(lead)
                                    break
            
            return filtered_leads
        
        # Obter lista de leads com proteção
        all_leads = []
        try:
            logger.info(f"Processando leads_data: type={type(leads_data)}, keys={list(leads_data.keys()) if isinstance(leads_data, dict) else 'N/A'}")
            if leads_data and isinstance(leads_data, dict) and "_embedded" in leads_data:
                embedded = leads_data["_embedded"]
                logger.info(f"Embedded type: {type(embedded)}, keys: {list(embedded.keys()) if isinstance(embedded, dict) else 'N/A'}")
                if embedded and isinstance(embedded, dict):
                    leads_raw = embedded.get("leads", [])
                    logger.info(f"Leads raw type: {type(leads_raw)}, length: {len(leads_raw) if isinstance(leads_raw, list) else 'N/A'}")
                    if leads_raw and isinstance(leads_raw, list):
                        # Filtrar apenas leads válidos (não None)
                        all_leads = [lead for lead in leads_raw if lead is not None]
                        logger.info(f"Leads processados: {len(all_leads)} válidos de {len(leads_raw)} totais")
                    else:
                        logger.warning(f"Leads raw inválido: {type(leads_raw)}")
                else:
                    logger.warning(f"Embedded inválido: {type(embedded)}")
            else:
                logger.warning(f"Leads data inválido: {type(leads_data)}")
        except Exception as e:
            logger.error(f"Erro ao processar leads: {e}")
            all_leads = []
        
        # Se corretor específico, filtrar leads por esse corretor
        if corretor and all_leads:
            try:
                filtered_leads = filter_leads_by_corretor(all_leads, corretor)
                if filtered_leads is not None and isinstance(filtered_leads, list):
                    all_leads = filtered_leads
                else:
                    all_leads = []
                logger.info(f"Filtrando por corretor '{corretor}': {len(all_leads)} leads encontrados")
            except Exception as filter_error:
                logger.error(f"Erro ao filtrar por corretor: {filter_error}")
                all_leads = []
        
        # Se fonte específica, filtrar leads por essa fonte
        if fonte and all_leads:
            try:
                filtered_leads = filter_leads_by_fonte(all_leads, fonte)
                if filtered_leads is not None and isinstance(filtered_leads, list):
                    all_leads = filtered_leads
                else:
                    all_leads = []
                logger.info(f"Filtrando por fonte '{fonte}': {len(all_leads)} leads encontrados")
            except Exception as filter_error:
                logger.error(f"Erro ao filtrar por fonte: {filter_error}")
                all_leads = []
        
        # Processar contagem de leads (após filtro se aplicável)
        total_leads = len(all_leads) if all_leads else 0
        
        # Criar mapa de usuários
        users_map = {}
        if users_data and "_embedded" in users_data:
            for user in users_data["_embedded"].get("users", []):
                users_map[user["id"]] = user["name"]
        
        # NOVO: Criar mapa de leads para busca rápida das reuniões (igual charts/leads-by-user)
        leads_map = {}
        if leads_data and "_embedded" in leads_data:
            for lead in leads_data["_embedded"].get("leads", []):
                if lead and lead.get("id"):
                    leads_map[lead.get("id")] = lead
        
        # NOVO: Processar reuniões REAIS e contar por corretor (igual charts/leads-by-user)
        meetings_by_corretor = {}
        if tasks_data and "_embedded" in tasks_data:
            reunion_tasks = tasks_data["_embedded"].get("tasks", [])
            logger.info(f"Processando {len(reunion_tasks)} tarefas de reunião")
            
            # Coletar IDs de leads que não estão no mapa atual
            missing_lead_ids = set()
            for task in reunion_tasks:
                if task and task.get('entity_type') == 'leads':
                    lead_id = task.get('entity_id')
                    if lead_id and lead_id not in leads_map:
                        missing_lead_ids.add(lead_id)
            
            # Buscar leads faltantes se necessário
            if missing_lead_ids:
                logger.info(f"Buscando {len(missing_lead_ids)} leads adicionais para reuniões")
                try:
                    for lead_id in missing_lead_ids:
                        additional_lead = kommo_api.get_lead(lead_id)
                        if additional_lead:
                            leads_map[lead_id] = additional_lead
                except Exception as e:
                    logger.error(f"Erro ao buscar leads adicionais: {e}")
            
            # Processar cada reunião e contar por corretor
            for task in reunion_tasks:
                if not task or task.get('entity_type') != 'leads':
                    continue
                
                lead_id = task.get('entity_id')
                lead = leads_map.get(lead_id)
                
                if not lead:
                    continue
                
                # Extrair corretor do lead (mesma lógica dos leads)
                custom_fields = lead.get("custom_fields_values", [])
                corretor_lead = None
                fonte_lead = None
                produto_lead = None
                
                if custom_fields and isinstance(custom_fields, list):
                    for field in custom_fields:
                        if not field or not isinstance(field, dict):
                            continue
                        field_id = field.get("field_id")
                        values = field.get("values", [])
                        
                        if field_id == CUSTOM_FIELD_CORRETOR and values:
                            corretor_lead = values[0].get("value")
                        elif field_id == CUSTOM_FIELD_FONTE and values:
                            fonte_lead = values[0].get("value")
                        elif field_id == CUSTOM_FIELD_PRODUTO and values:
                            produto_lead = values[0].get("value")
                
                # Aplicar filtros APENAS se especificados (igual charts/leads-by-user)
                if corretor and isinstance(corretor, str) and corretor.strip() and corretor_lead != corretor:
                    continue
                if fonte and isinstance(fonte, str) and fonte.strip() and fonte_lead != fonte:
                    continue
                if produto and isinstance(produto, str) and produto.strip() and produto_lead != produto:
                    continue
                
                # Determinar corretor final (mesma lógica dos leads)
                final_corretor = corretor_lead or users_map.get(lead.get("responsible_user_id"), "Usuário Sem Nome")
                
                # Contar reunião para este corretor
                meetings_by_corretor[final_corretor] = meetings_by_corretor.get(final_corretor, 0) + 1
            
            logger.info(f"Reuniões contadas por corretor: {meetings_by_corretor}")
        
        # Processar dados por corretor usando custom field
        leads_by_user = []
        
        if all_leads:
            # Se filtrou por corretor específico, mostrar apenas esse corretor
            if corretor:
                # Calcular métricas REAIS para o corretor específico
                active_leads = len([lead for lead in all_leads if lead and lead.get("status_id") not in [142, 143]])
                won_leads = len([lead for lead in all_leads if lead and lead.get("status_id") == 142])
                lost_leads = len([lead for lead in all_leads if lead and lead.get("status_id") == 143])
                
                # Usar dados REAIS de reuniões
                real_meetings = meetings_by_corretor.get(corretor, 0)
                
                leads_by_user = [{
                    "name": corretor,
                    "value": total_leads,
                    "active": active_leads,
                    "lost": lost_leads,
                    "meetings": real_meetings,  # DADOS REAIS
                    "meetingsHeld": real_meetings,  # DADOS REAIS
                    "sales": won_leads
                }]
            else:
                # Agrupar por corretor usando custom field
                corretor_counts = {}
                
                for lead in all_leads:
                    if not lead:  # Proteção adicional
                        continue
                    corretor_name = None
                    custom_fields = lead.get("custom_fields_values", [])
                    
                    # Buscar campo corretor
                    if custom_fields and isinstance(custom_fields, list):
                        for field in custom_fields:
                            if field and field.get("field_id") == 837920:  # ID do campo Corretor
                                values = field.get("values", [])
                                if values and len(values) > 0:
                                    corretor_name = values[0].get("value")
                                    break
                    
                    if not corretor_name:
                        corretor_name = "Sem corretor"
                    
                    if corretor_name not in corretor_counts:
                        corretor_counts[corretor_name] = {
                            "total": 0,
                            "active": 0,
                            "lost": 0,
                            "won": 0
                        }
                    
                    corretor_counts[corretor_name]["total"] += 1
                    
                    # Verificar status do lead
                    status_id = lead.get("status_id")
                    if status_id == 142:  # Won
                        corretor_counts[corretor_name]["won"] += 1
                    elif status_id == 143:  # Lost
                        corretor_counts[corretor_name]["lost"] += 1
                    else:  # Active
                        corretor_counts[corretor_name]["active"] += 1
                
                # Criar array de dados por corretor com DADOS REAIS
                for corretor_name, counts in corretor_counts.items():
                    # Usar dados REAIS de reuniões do mapa meetings_by_corretor
                    real_meetings = meetings_by_corretor.get(corretor_name, 0)
                    
                    leads_by_user.append({
                        "name": corretor_name,
                        "value": counts["total"],
                        "active": counts["active"],
                        "lost": counts["lost"],
                        "meetings": real_meetings,  # DADOS REAIS
                        "meetingsHeld": real_meetings,  # DADOS REAIS
                        "sales": counts["won"]
                    })
        
        # Processar leads por estágio usando pipelines
        leads_by_stage_array = []
        
        try:
            logger.info("Iniciando processamento de pipelines...")
            # Buscar dados de pipelines para mapear status
            pipelines_data = safe_get_data(kommo_api.get_pipelines)
            logger.info(f"Pipelines data: {type(pipelines_data)}")
            stage_map = {}
            
            if pipelines_data and "_embedded" in pipelines_data:
                pipelines = pipelines_data["_embedded"].get("pipelines", [])
                logger.info(f"Pipelines count: {len(pipelines) if isinstance(pipelines, list) else 'N/A'}")
                if pipelines and isinstance(pipelines, list):
                    for i, pipeline in enumerate(pipelines):
                        logger.info(f"Processing pipeline {i}: {type(pipeline)}")
                        if not pipeline or not isinstance(pipeline, dict):
                            continue
                        embedded_statuses = pipeline.get("_embedded", {})
                        if embedded_statuses and isinstance(embedded_statuses, dict):
                            statuses = embedded_statuses.get("statuses")
                            if statuses and isinstance(statuses, list):
                                for j, status in enumerate(statuses):
                                    logger.info(f"Processing status {j}: {type(status)}")
                                    if (status and isinstance(status, dict) and 
                                        status.get("id") and status.get("name")):
                                        stage_map[status["id"]] = status["name"]
            
            logger.info(f"Stage map criado: {len(stage_map)} stages")
            
            # Contar leads por estágio
            if all_leads and stage_map:
                logger.info("Contando leads por estágio...")
                stage_counts = {}
                for i, lead in enumerate(all_leads):
                    if i % 50 == 0:  # Log a cada 50 leads
                        logger.info(f"Processando lead {i}/{len(all_leads)}")
                    if not lead or not isinstance(lead, dict):
                        continue
                    status_id = lead.get("status_id")
                    if status_id and status_id in stage_map:
                        stage_name = stage_map[status_id]
                        stage_counts[stage_name] = stage_counts.get(stage_name, 0) + 1
                
                # Ordenar por quantidade com proteção
                if stage_counts:
                    sorted_stages = sorted(stage_counts.items(), key=lambda x: x[1], reverse=True)
                    leads_by_stage_array = [
                        {"name": name, "value": count}
                        for name, count in sorted_stages
                    ]
                else:
                    leads_by_stage_array = []
                
                logger.info(f"Leads por estágio: {len(leads_by_stage_array)} estágios encontrados")
        except Exception as stage_error:
            logger.error(f"Erro no processamento de stages: {stage_error}")
            import traceback
            logger.error(f"Traceback stages: {traceback.format_exc()}")
            leads_by_stage_array = []
        
        # Processar leads por fonte usando custom field "Fonte" (ID: 837886) para vendas também
        leads_by_source_sales = []
        
        if all_leads:
            source_counts = {}
            
            for lead in all_leads:
                if not lead:  # Proteção adicional
                    continue
                    
                fonte_name = None
                custom_fields = lead.get("custom_fields_values", [])
                
                # Buscar custom field "Fonte" (ID: 837886)
                if custom_fields and isinstance(custom_fields, list):
                    for field in custom_fields:
                        if field and field.get("field_id") == 837886:  # ID do campo Fonte
                            values = field.get("values", [])
                            if values and len(values) > 0 and values[0]:
                                fonte_name = values[0].get("value")
                                if fonte_name:  # Verificar se não é None ou string vazia
                                    break
                
                if not fonte_name:
                    fonte_name = "Fonte Desconhecida"
                
                source_counts[fonte_name] = source_counts.get(fonte_name, 0) + 1
            
            # Ordenar por quantidade
            if source_counts:
                sorted_sources = sorted(source_counts.items(), key=lambda x: x[1], reverse=True)
                leads_by_source_sales = [
                    {"name": name, "value": count}
                    for name, count in sorted_sources
                ]
        
        # Calcular métricas de performance baseadas nos dados reais filtrados
        if all_leads:
            # Calcular métricas reais com base nos leads filtrados
            active_leads_count = len([lead for lead in all_leads if lead and lead.get("status_id") not in [142, 143]])
            won_leads_count = len([lead for lead in all_leads if lead and lead.get("status_id") == 142])
            lost_leads_count = len([lead for lead in all_leads if lead and lead.get("status_id") == 143])
            
            # Calcular taxas de conversão REAIS (sem estimativas)
            conversion_rate_sales = (won_leads_count / total_leads * 100) if total_leads > 0 else 0
            
            # Taxa de reuniões REAL: total de reuniões realizadas / total de leads
            total_meetings_held = sum(meetings_by_corretor.values())
            conversion_rate_meetings = (total_meetings_held / total_leads * 100) if total_leads > 0 else 0
            
            # Taxa de prospects: considerando leads ativos como prospects
            conversion_rate_prospects = (active_leads_count / total_leads * 100) if total_leads > 0 else 0
            
            # Calcular win rate (vendas vs perdas)
            total_closed = won_leads_count + lost_leads_count
            win_rate = (won_leads_count / total_closed * 100) if total_closed > 0 else 0
            
            # Calcular ticket médio baseado nos leads ganhos
            total_revenue = sum(lead.get("price", 0) or 0 for lead in all_leads if lead and lead.get("status_id") == 142)
            average_deal_size = (total_revenue / won_leads_count) if won_leads_count > 0 else 0
            
            # Calcular tempo médio de ciclo
            cycle_times = []
            for lead in all_leads:
                if (lead and lead.get("status_id") == 142 and 
                    lead.get("closed_at") and lead.get("created_at") and
                    isinstance(lead.get("closed_at"), (int, float)) and 
                    isinstance(lead.get("created_at"), (int, float))):
                    
                    cycle_time = (lead.get("closed_at") - lead.get("created_at")) / (24 * 60 * 60)
                    if cycle_time > 0:
                        cycle_times.append(cycle_time)
            
            lead_cycle_time = sum(cycle_times) / len(cycle_times) if cycle_times else 0
            
        else:
            # Valores padrão se não houver leads
            conversion_rate_sales = 0
            conversion_rate_meetings = 0  
            conversion_rate_prospects = 0
            win_rate = 0
            average_deal_size = 0
            lead_cycle_time = 0
            active_leads_count = 0
            lost_leads_count = 0
            won_leads_count = 0
        
        # Métricas baseadas nos dados reais (não mais fixas)
        response = {
            "totalLeads": total_leads,
            "leadsByUser": leads_by_user,
            "leadsByStage": leads_by_stage_array,  # AGORA IMPLEMENTADO
            "leadsBySource": leads_by_source_sales,  # NOVO: Leads por fonte (custom field)
            "conversionRates": {
                "meetings": round(conversion_rate_meetings, 1),
                "prospects": round(conversion_rate_prospects, 1),
                "sales": round(conversion_rate_sales, 1)
            },
            "leadCycleTime": round(lead_cycle_time, 1),
            "winRate": round(win_rate, 1),
            "averageDealSize": round(average_deal_size, 2),
            "salesbotRecovery": 0,  # TODO: Implementar busca por tag de recuperação
            "salesTrend": [],
            "customFields": {  # NOVO: Custom fields implementados
                "fonte": leads_by_source_sales,
                "available_fontes": [
                    "Tráfego Meta", "Escritório Patacho", "Canal Pro", "Site", 
                    "Redes Sociais", "Parceria com Construtoras", "Ação de Panfletagem",
                    "Eletromídia", "Orgânico", "LandingPage", "Chamada", "Anúncio Físico",
                    "Não atribuído", "Google", "Cliente", "Grupo Zap", "Celular do Plantão",
                    "Tráfego Séculos"
                ]
            },
            "analyticsOverview": {
                "leads": {
                    "total": total_leads,
                    "active": active_leads_count,
                    "lost": lost_leads_count,
                    "won": won_leads_count if all_leads else 0
                }
            },
            "analyticsFunnel": {},
            "analyticsTeam": {},
            "meetingsStats": {},
            
            # Metadados
            "_metadata": {
                "period_days": days,
                "corretor_filter": corretor,
                "generated_at": datetime.now().isoformat(),
                "data_sources": ["kommo_api"],
                "optimized": True,
                "single_request": True,
                "leads_filtered": len(all_leads),
                "performance_calculated": True,
                "custom_fields_implemented": True,
                "stages_implemented": True
            }
        }
        
        logger.info(f"Dashboard vendas completo gerado: {len(response['leadsByUser'])} usuários, {len(response['leadsByStage'])} estágios")
        return response
        
    except HTTPException:
        # Re-raise HTTPExceptions (como 400 Bad Request) sem modificar
        raise
    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        logger.error(f"Erro ao gerar dashboard vendas completo: {str(e)}")
        logger.error(f"Traceback completo: {error_details}")
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)} | Linha: {error_details.split('File')[1].split(',')[1] if 'File' in error_details else 'unknown'}")


#  Como as Datas são Determinadas:

#   1. Para Vendas (status_categoria = "venda"):
#     - Prioridade: Usa closed_at se disponível (data real da venda)
#     - Fallback: Se não tiver closed_at, usa created_at ou updated_at ou etapa de ganho
#   2. Para Reuniões:
#     - Usa created_at ou updated_at do lead


@router.get("/detailed-tables")
async def get_detailed_tables(
    corretor: Optional[str] = Query(None, description="Nome do corretor para filtrar dados"),
    fonte: Optional[str] = Query(None, description="Fonte para filtrar dados"),
    start_date: Optional[str] = Query(None, description="Data de início (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="Data de fim (YYYY-MM-DD)"),
    days: int = Query(30, description="Período em dias (usado se start_date/end_date não fornecidos)"),
    limit: int = Query(250, description="Limite de registros por página"),
):
    """
    Endpoint que retorna dados detalhados para 5 tabelas (COM DATA PROPOSTA):
    - Leads: Data de Criação, Nome do Lead, Corretor, Fonte, Anúncio, Público, Produto, Data da Proposta, Funil, Etapa, Status
    - Leads Orgânicos: Data de Criação, Nome do Lead, Corretor, Fonte, Anúncio, Público, Produto, Data da Proposta, Funil, Etapa, Status
    - Reuniões: Data da Reunião, Nome do Lead, Corretor, Fonte, Anúncio, Público, Produto, Funil, Etapa, Status
    - Vendas: Data da Venda, Nome do Lead, Corretor, Fonte, Anúncio, Público, Produto, Valor da Venda
    
    Retorna TODOS os dados sem filtro de período.
    """
    try:
        logger.info(f"Iniciando busca de tabelas detalhadas para TODOS os dados, corretor: {corretor}, fonte: {fonte}")
        
        # Status IDs corretos baseados na pipeline real
        STATUS_CONTRATO_ASSINADO = 80689759  # "Contrato Assinado"
        STATUS_VENDA_FINAL = 142  # "Closed - won" / "Venda ganha"
        PIPELINE_VENDAS = 10516987  # ID do Funil de Vendas
        CUSTOM_FIELD_DATA_FECHAMENTO = 858126  # ID do campo "Data Fechamento"
        CUSTOM_FIELD_ESTADO = 851638  # Campo ESTADO
        CUSTOM_FIELD_FONTE = 837886  # Campo "Fonte"
        CUSTOM_FIELD_ANUNCIO = 837846  # Campo "Anúncio"
        CUSTOM_FIELD_PUBLICO = 837844  # Campo "Público" (conjunto de anúncios)
        CUSTOM_FIELD_PRODUTO = 857264  # Campo "Produto"
        CUSTOM_FIELD_PROPOSTA = 861100  # Campo "Proposta" (boolean)
        CUSTOM_FIELD_DATA_PROPOSTA = 882618  # Campo "Data da Proposta"
        
        # Função auxiliar para extrair valores de custom fields
        def get_custom_field_value(lead, field_id):
            """Extrai valor de um custom field específico"""
            try:
                custom_fields = lead.get("custom_fields_values", [])
                if not custom_fields:
                    return None
                for field in custom_fields:
                    if not field:
                        continue
                    if field.get("field_id") == field_id:
                        values = field.get("values")
                        if values and isinstance(values, list) and len(values) > 0:
                            first_value = values[0]
                            if isinstance(first_value, dict):
                                return first_value.get("value")
                            elif isinstance(first_value, str):
                                return first_value
                return None
            except Exception as e:
                logger.error(f"Erro ao extrair custom field {field_id}: {e}")
                return None
        
        def is_proposta(lead):
            """Verifica se um lead é uma proposta usando o campo boolean 861100"""
            try:
                proposta_value = get_custom_field_value(lead, CUSTOM_FIELD_PROPOSTA)
                # Campo boolean pode retornar True, "true", "1", 1, etc.
                if proposta_value in [True, "true", "1", 1, "True", "TRUE"]:
                    return True
                return False
            except Exception as e:
                logger.error(f"Erro ao verificar se lead é proposta: {e}")
                return False
        
        def validate_proposta_in_period(lead, start_timestamp, end_timestamp):
            """Valida se a proposta deve ser incluída baseado na Data da Proposta"""
            try:
                # Verificar se é uma proposta
                if not is_proposta(lead):
                    return False
                
                # Extrair data da proposta
                data_proposta_timestamp = get_custom_field_value(lead, CUSTOM_FIELD_DATA_PROPOSTA)
                
                if not data_proposta_timestamp:
                    return False
                
                # Converter para timestamp se necessário
                if isinstance(data_proposta_timestamp, str):
                    try:
                        # Assumir formato ISO ou timestamp string
                        if data_proposta_timestamp.isdigit():
                            data_proposta_timestamp = int(data_proposta_timestamp)
                        else:
                            # Tentar parsing de data ISO
                            dt = datetime.fromisoformat(data_proposta_timestamp.replace('Z', '+00:00'))
                            data_proposta_timestamp = int(dt.timestamp())
                    except:
                        return False
                
                # Verificar se está no período
                return start_timestamp <= data_proposta_timestamp <= end_timestamp
                
            except Exception as e:
                logger.error(f"Erro ao validar proposta no período: {e}")
                return False
        
        # ABORDAGEM SIMPLIFICADA: Buscar TODOS os leads sem filtro
        # Calcular filtros de data
        import time
        
        if start_date and end_date:
            # Usar datas específicas
            try:
                start_dt = datetime.strptime(start_date, '%Y-%m-%d')
                end_dt = datetime.strptime(end_date, '%Y-%m-%d')
                end_dt = end_dt.replace(hour=23, minute=59, second=59)
                start_timestamp = int(start_dt.timestamp())
                end_timestamp = int(end_dt.timestamp())
                
                # Para reuniões: incluir 23:59 do dia anterior para capturar reuniões agendadas na virada do dia
                meetings_start_dt = start_dt - timedelta(days=1)
                meetings_start_dt = meetings_start_dt.replace(hour=23, minute=59, second=0)
                meetings_start_timestamp = int(meetings_start_dt.timestamp())
                
                logger.info(f"Filtro por período: {start_date} a {end_date}")
                logger.info(f"Filtro reuniões: {meetings_start_dt.strftime('%Y-%m-%d %H:%M')} a {end_dt.strftime('%Y-%m-%d %H:%M')}")
            except ValueError as date_error:
                logger.error(f"Erro de validação de data: {date_error}")
                raise HTTPException(status_code=400, detail="Formato de data inválido. Use YYYY-MM-DD")
        else:
            # Usar período em dias
            end_timestamp = int(time.time())
            start_timestamp = end_timestamp - (days * 24 * 60 * 60)
            start_dt = datetime.fromtimestamp(start_timestamp, tz=BRAZIL_TIMEZONE)
            end_dt = datetime.fromtimestamp(end_timestamp, tz=BRAZIL_TIMEZONE)
            
            # Para reuniões: incluir 23:59 do dia anterior
            meetings_start_timestamp = start_timestamp - (24 * 60 * 60) + (23 * 60 * 60 + 59 * 60)  # -1 dia + 23:59
            meetings_start_dt = datetime.fromtimestamp(meetings_start_timestamp, tz=BRAZIL_TIMEZONE)
            
            logger.info(f"Filtro por {days} dias: {start_dt.strftime('%Y-%m-%d')} a {end_dt.strftime('%Y-%m-%d')}")
            logger.info(f"Filtro reuniões: {meetings_start_dt.strftime('%Y-%m-%d %H:%M')} a {end_dt.strftime('%Y-%m-%d %H:%M')}")
        
        logger.info(f"Buscando leads do Funil de Vendas (pipeline {PIPELINE_VENDAS})")
        
        # IDs dos pipelines necessários
        PIPELINE_REMARKETING = 11059911  # ID do Remarketing
        
        # ================================================================
        # VENDAS E REUNIÕES
        # ================================================================
        # VENDAS: Buscar TODOS com status venda (filtrar por data_fechamento depois)
        # REUNIÕES: Já correto - buscar tasks por created_at
        # ================================================================
        
        
        # VENDAS: Buscar leads com status de venda - CORREÇÃO: usar formato correto da API
        # (ainda filtraremos por data_fechamento específica depois)
        vendas_vendas_params = {
            "filter[statuses][0][pipeline_id]": PIPELINE_VENDAS,
            "filter[statuses][0][status_id]": STATUS_VENDA_FINAL,
            "filter[statuses][1][pipeline_id]": PIPELINE_VENDAS,
            "filter[statuses][1][status_id]": STATUS_CONTRATO_ASSINADO,
            "limit": limit,
            "with": "contacts,tags,custom_fields_values"
        }
        
        vendas_remarketing_params = {
            "filter[statuses][0][pipeline_id]": PIPELINE_REMARKETING,
            "filter[statuses][0][status_id]": STATUS_VENDA_FINAL,
            "filter[statuses][1][pipeline_id]": PIPELINE_REMARKETING,
            "filter[statuses][1][status_id]": STATUS_CONTRATO_ASSINADO,
            "limit": limit,
            "with": "contacts,tags,custom_fields_values"
        }
        
        
        # Buscar VENDAS de ambos os pipelines - USAR PAGINAÇÃO COMPLETA
        try:
            vendas_vendas_all = kommo_api.get_all_leads_old(vendas_vendas_params)
            vendas_vendas_data = {"_embedded": {"leads": vendas_vendas_all}}
            logger.info(f"Vendas Vendas (paginação completa): {len(vendas_vendas_all)}")
        except Exception as e:
            logger.error(f"Erro ao buscar vendas vendas: {e}")
            vendas_vendas_data = {"_embedded": {"leads": []}}

        try:
            vendas_remarketing_all = kommo_api.get_all_leads_old(vendas_remarketing_params)
            vendas_remarketing_data = {"_embedded": {"leads": vendas_remarketing_all}}
            logger.info(f"Vendas Remarketing (paginação completa): {len(vendas_remarketing_all)}")
        except Exception as e:
            logger.error(f"Erro ao buscar vendas remarketing: {e}")
            vendas_remarketing_data = {"_embedded": {"leads": []}}
        
        users_data = safe_get_data(kommo_api.get_users)
        pipelines_data = safe_get_data(kommo_api.get_pipelines)
        
        # Criar mapa de usuários
        users_map = {}
        if users_data and "_embedded" in users_data:
            for user in users_data["_embedded"].get("users", []):
                users_map[user["id"]] = user["name"]
        
        # Criar mapa de status IDs para nomes reais
        status_map = {}
        if pipelines_data and "_embedded" in pipelines_data:
            pipelines_list = pipelines_data["_embedded"].get("pipelines", [])
            if isinstance(pipelines_list, list):
                for pipeline in pipelines_list:
                    if pipeline and isinstance(pipeline, dict):
                        embedded_statuses = pipeline.get("_embedded", {})
                        if isinstance(embedded_statuses, dict):
                            statuses = embedded_statuses.get("statuses", [])
                            if isinstance(statuses, list):
                                for status in statuses:
                                    if status and isinstance(status, dict):
                                        status_id = status.get("id")
                                        status_name = status.get("name", f"Status {status_id}")
                                        if status_id:
                                            status_map[status_id] = status_name
        
        
        # Combinar VENDAS de ambos os pipelines
        all_vendas = []
        if vendas_vendas_data and "_embedded" in vendas_vendas_data:
            vendas = vendas_vendas_data["_embedded"].get("leads", [])
            all_vendas.extend(vendas)
            logger.info(f"Vendas do Funil de Vendas: {len(vendas)}")
        
        if vendas_remarketing_data and "_embedded" in vendas_remarketing_data:
            vendas = vendas_remarketing_data["_embedded"].get("leads", [])
            all_vendas.extend(vendas)
            logger.info(f"Vendas do Remarketing: {len(vendas)}")
        
        logger.info(f"Encontradas {len(all_vendas)} vendas totais")
        
        # Buscar TODOS os leads para a seção leadsDetalhes (sem filtro de status)
        all_leads_params = {
            "filter[pipeline_id]": PIPELINE_VENDAS,
            "filter[created_at][from]": start_timestamp,  # Usar created_at para leads
            "filter[created_at][to]": end_timestamp,
            "limit": limit,
            "with": "contacts,tags,custom_fields_values"
        }
        
        all_leads_remarketing_params = {
            "filter[pipeline_id]": PIPELINE_REMARKETING,
            "filter[created_at][from]": start_timestamp,  # Usar created_at para leads
            "filter[created_at][to]": end_timestamp,
            "limit": limit,
            "with": "contacts,tags,custom_fields_values"
        }
        
        # Buscar todos os leads - USAR PAGINAÇÃO COMPLETA
        try:
            all_leads_vendas_all = kommo_api.get_all_leads_old(all_leads_params)
            all_leads_vendas_data = {"_embedded": {"leads": all_leads_vendas_all}}
            logger.info(f"Todos os Leads Vendas (paginação completa): {len(all_leads_vendas_all)}")
        except Exception as e:
            logger.error(f"Erro ao buscar todos os leads vendas: {e}")
            all_leads_vendas_data = {"_embedded": {"leads": []}}

        try:
            all_leads_remarketing_all = kommo_api.get_all_leads_old(all_leads_remarketing_params)
            all_leads_remarketing_data = {"_embedded": {"leads": all_leads_remarketing_all}}
            logger.info(f"Todos os Leads Remarketing (paginação completa): {len(all_leads_remarketing_all)}")
        except Exception as e:
            logger.error(f"Erro ao buscar todos os leads remarketing: {e}")
            all_leads_remarketing_data = {"_embedded": {"leads": []}}
        
        # Combinar TODOS os leads
        all_leads_for_details = []
        if all_leads_vendas_data and "_embedded" in all_leads_vendas_data:
            leads = all_leads_vendas_data["_embedded"].get("leads", [])
            all_leads_for_details.extend(leads)
            logger.info(f"Todos os leads do Funil de Vendas: {len(leads)}")
        
        if all_leads_remarketing_data and "_embedded" in all_leads_remarketing_data:
            leads = all_leads_remarketing_data["_embedded"].get("leads", [])
            all_leads_for_details.extend(leads)
            logger.info(f"Todos os leads do Remarketing: {len(leads)}")
        
        logger.info(f"Total de leads para leadsDetalhes: {len(all_leads_for_details)}")
        
        # Listas para as tabelas
        reunioes_detalhes = []  # Reuniões não-orgânicas
        reunioes_organicas_detalhes = []  # NOVA lista para reuniões orgânicas
        vendas_detalhes = []
        leads_detalhes = []  # Lista para leads não-orgânicos
        organicos_detalhes = []  # NOVA lista para leads orgânicos
        propostas_detalhes = []  # NOVA lista para propostas (unificada)
        
        # NOVO: Buscar tarefas de reunião realizadas COM filtro de data
        logger.info("Buscando tarefas de reunião realizadas...")
        tasks_params = {
            'filter[task_type_id]': 2,  # CORREÇÃO: usar task_type_id em vez de task_type
            'filter[is_completed]': 1,  # Apenas concluídas
            'filter[complete_till][from]': meetings_start_timestamp,  # CORREÇÃO: usar meetings_start_timestamp para incluir 23:59 do dia anterior
            'filter[complete_till][to]': end_timestamp,      # Filtro de data
            'limit': limit
        }
        
        # Usar get_all_tasks com paginação para períodos grandes
        try:
            all_tasks = kommo_api.get_all_tasks(tasks_params)
            tasks_data = {"_embedded": {"tasks": all_tasks}}
            logger.info(f"[detailed-tables] Total de tarefas encontradas: {len(all_tasks)}")
        except Exception as e:
            logger.error(f"Erro ao buscar todas as tarefas: {e}")
            tasks_data = safe_get_data(kommo_api.get_tasks, tasks_params)
        
        reunioes_tasks = []
        
        if tasks_data and '_embedded' in tasks_data:
            reunioes_tasks = tasks_data.get('_embedded', {}).get('tasks', [])
            logger.info(f"Encontradas {len(reunioes_tasks)} tarefas de reunião concluídas")
            
        
        # Criar mapa de lead_id para lead (usar todos os leads para lookup de reuniões)
        all_leads_combined = all_vendas + all_leads_for_details
        # Remover duplicatas usando um dicionário
        leads_map = {}
        for lead in all_leads_combined:
            if lead and lead.get("id"):
                leads_map[lead.get("id")] = lead
        
        # OTIMIZAÇÃO INTELIGENTE: Buscar apenas leads únicos das reuniões
        # Coletar IDs únicos dos leads das reuniões que não estão no mapa
        reunion_lead_ids = set()
        for task in reunioes_tasks:
            if task.get('entity_type') == 'leads':
                lead_id = task.get('entity_id')
                if lead_id and lead_id not in leads_map:
                    reunion_lead_ids.add(lead_id)
        
        print(f"DEBUG: {len(reunioes_tasks)} reuniões encontradas")
        print(f"DEBUG: {len(reunion_lead_ids)} leads únicos precisam ser buscados")
        
        # Buscar os leads faltantes em lote usando filtro de IDs
        if reunion_lead_ids:
            logger.info(f"Buscando {len(reunion_lead_ids)} leads adicionais para reuniões: {list(reunion_lead_ids)}")
            
            # DEBUG: Tentar busca em lote primeiro, mas com fallback garantido
            leads_found_batch = 0
            try:
                # Converter IDs para string separada por vírgula
                ids_string = ','.join(str(id) for id in reunion_lead_ids)
                print(f"DEBUG: Tentando busca em lote com IDs: {ids_string}")
                
                # Buscar múltiplos leads de uma vez
                batch_params = {
                    'filter[id]': ids_string,
                    'limit': len(reunion_lead_ids),
                    'with': 'contacts,custom_fields_values'
                }
                
                batch_result = kommo_api.get_leads(batch_params)
                print(f"DEBUG: Resultado busca em lote: {batch_result is not None}")
                
                if batch_result and '_embedded' in batch_result:
                    batch_leads = batch_result['_embedded'].get('leads', [])
                    print(f"DEBUG: Leads encontrados em lote: {len(batch_leads)}")
                    
                    # Adicionar todos os leads encontrados ao mapa
                    for lead in batch_leads:
                        if lead and lead.get('id'):
                            leads_map[lead.get('id')] = lead
                            leads_found_batch += 1
                            print(f"DEBUG: Lead {lead.get('id')} adicionado via lote")
                
            except Exception as e:
                print(f"DEBUG: Erro na busca em lote: {e}")
            
            # Busca paralela para IDs não encontrados (muito mais rápida)
            remaining_ids = reunion_lead_ids - set(leads_map.keys())
            if remaining_ids:
                print(f"DEBUG: Fazendo busca PARALELA para {len(remaining_ids)} leads restantes")
                
                from concurrent.futures import ThreadPoolExecutor, as_completed
                import time
                
                def fetch_lead(lead_id):
                    try:
                        return lead_id, kommo_api.get_lead(lead_id)
                    except Exception as e:
                        print(f"DEBUG: Erro ao buscar lead {lead_id}: {e}")
                        return lead_id, None
                
                start_time = time.time()
                # OTIMIZAÇÃO: Máximo 10 threads para melhor performance sem sobrecarregar
                max_threads = min(10, len(remaining_ids))
                print(f"DEBUG: Usando {max_threads} threads paralelas")
                with ThreadPoolExecutor(max_workers=max_threads) as executor:
                    # Submeter todas as tarefas
                    future_to_id = {executor.submit(fetch_lead, lead_id): lead_id for lead_id in remaining_ids}
                    
                    # Coletar resultados conforme ficam prontos
                    for future in as_completed(future_to_id):
                        lead_id, lead = future.result()
                        if lead:
                            leads_map[lead_id] = lead
                            print(f"DEBUG: Lead {lead_id} encontrado via thread")
                
                elapsed = time.time() - start_time
                print(f"DEBUG: Busca paralela concluída em {elapsed:.2f}s para {len(remaining_ids)} leads")
            
            logger.info(f"Total leads encontrados: {leads_found_batch} em lote + {len(reunion_lead_ids) - len(remaining_ids) - leads_found_batch} individual")
        
        # Processar tarefas de reunião (agora com todos os leads disponíveis)
        print(f"DEBUG: Processando {len(reunioes_tasks)} reuniões...")
        for task in reunioes_tasks:
            if not task or task.get('entity_type') != 'leads':
                continue
                
            lead_id = task.get('entity_id')
            lead = leads_map.get(lead_id)
            
            if not lead:
                continue
                
            # Lead já é do Funil de Vendas (filtrado na API)
                
            # Extrair dados do lead
            lead_name = lead.get("name", "")
            responsible_user_id = lead.get("responsible_user_id")
            pipeline_id = lead.get("pipeline_id")
            status_id = lead.get("status_id")
            
            # VALIDAÇÃO: Verificar se a reunião realmente aconteceu
            # Reunião é considerada verdadeira se:
            # 1. is_completed = true (já verificado na query)
            # 2. complete_till existe (data de agendamento da reunião)
            # PO: usar complete_till para filtrar reuniões no período
            complete_till = task.get('complete_till')
            if not complete_till:
                # Se não tem complete_till, pular
                continue
            
            data_reuniao = complete_till
            
            # Validação adicional: verificar se complete_till está dentro do período
            if data_reuniao < start_timestamp or data_reuniao > end_timestamp:
                continue
                
            # Extrair custom fields do lead
            custom_fields = lead.get("custom_fields_values", [])
            fonte_lead = "N/A"
            corretor_custom = None
            anuncio_lead = "N/A"  # Novo campo
            publico_lead = "N/A"  # Novo campo (conjunto de anúncios)
            produto_lead = "N/A"  # Campo Produto
            data_proposta_lead = format_proposal_date(lead, CUSTOM_FIELD_DATA_PROPOSTA)  # Campo Data da Proposta
            
            if custom_fields and isinstance(custom_fields, list):
                for field in custom_fields:
                    if field and isinstance(field, dict):
                        field_id = field.get("field_id")
                        values = field.get("values", [])
                        
                        if field_id == 837886 and values:  # Fonte
                            fonte_lead = values[0].get("value", "N/A")
                        elif field_id == 837920 and values:  # Corretor
                            corretor_custom = values[0].get("value")
                        elif field_id == 837846 and values:  # Anúncio
                            anuncio_lead = values[0].get("value", "N/A")
                        elif field_id == 837844 and values:  # Público (conjunto de anúncios)
                            publico_lead = values[0].get("value", "N/A")
                        elif field_id == 857264 and values:  # Produto
                            produto_lead = values[0].get("value", "N/A")
            
            # Determinar corretor final - apenas do custom field
            if corretor_custom:
                corretor_final = corretor_custom
            else:
                corretor_final = "Não atribuído"  # Sem fallback para responsible_user_id
            
            # Determinar funil baseado no pipeline_id
            if pipeline_id == PIPELINE_VENDAS:
                funil = "Funil de Vendas"
            elif pipeline_id == PIPELINE_REMARKETING:
                funil = "Remarketing"
            else:
                funil = "Não atribuído"
            
            # Determinar etapa baseado no status_id usando nomes reais da API
            etapa = status_map.get(status_id, f"Status {status_id}")
            
            # Filtrar por corretor se especificado
            if corretor and isinstance(corretor, str) and corretor.strip():
                if ',' in corretor:
                    corretores_list = [c.strip() for c in corretor.split(',')]
                    if corretor_final not in corretores_list:
                        continue
                else:
                    if corretor_final != corretor:
                        continue
                
            # Filtrar por fonte se especificado - suporta múltiplos valores separados por vírgula
            if fonte and isinstance(fonte, str) and fonte.strip():
                if ',' in fonte:
                    fontes_list = [f.strip() for f in fonte.split(',')]
                    if fonte_lead not in fontes_list:
                        continue
                else:
                    if fonte_lead != fonte:
                        continue
            
            # Formatar data com a data real de conclusão
            data_formatada = format_timestamp_brazil(data_reuniao)
            
            # Adicionar informação sobre quando a reunião estava agendada originalmente
            data_agendada = task.get('complete_till')
            data_agendada_formatada = format_timestamp_brazil(data_agendada) if data_agendada else "N/A"
            
            # Criar objeto da reunião
            reuniao_obj = {
                "Data da Reunião": data_formatada,  # Data em que foi marcada como concluída
                "Data Agendada": data_agendada_formatada,  # Data original do agendamento
                "Nome do Lead": lead_name,
                "Corretor": corretor_final,
                "Fonte": fonte_lead,
                "Anúncio": anuncio_lead,  # Novo campo
                "Público": publico_lead,  # Novo campo (conjunto de anúncios)
                "Produto": produto_lead,  # Campo Produto
                "Data da Proposta": data_proposta_lead,  # Campo Data da Proposta
                "Funil": funil,
                "Etapa": etapa,
                "Status": "Realizada"  # Confirmação visual de que a reunião aconteceu
            }
            
            # Separar entre reuniões orgânicas e não-orgânicas baseado na fonte
            if fonte_lead == "Orgânico":
                reunioes_organicas_detalhes.append(reuniao_obj)
            else:
                reunioes_detalhes.append(reuniao_obj)
        
        
        # Processar VENDAS (filtrar por data_fechamento no período)
        for lead in all_vendas:
            if not lead:
                continue
                
            lead_id = lead.get("id")
            lead_name = lead.get("name", "")
            price = lead.get("price", 0)
            created_at = lead.get("created_at")
            
            # Validar se a venda deve ser incluída (status + data no período)
            if not validate_sale_in_period(lead, start_timestamp, end_timestamp, CUSTOM_FIELD_DATA_FECHAMENTO):
                continue
            
            # Extrair campos customizados usando função padronizada
            fonte_lead = extract_custom_field_value(lead, 837886) or "N/A"  # Fonte
            corretor_custom = extract_custom_field_value(lead, 837920)  # Corretor
            anuncio_lead = extract_custom_field_value(lead, 837846) or "N/A"  # Anúncio
            publico_lead = extract_custom_field_value(lead, 837844) or "N/A"  # Público (conjunto de anúncios)
            produto_lead = extract_custom_field_value(lead, CUSTOM_FIELD_PRODUTO) or "N/A"  # Produto
            data_proposta_lead = format_proposal_date(lead, CUSTOM_FIELD_DATA_PROPOSTA)  # Campo Data da Proposta
            
            # Obter timestamp da data de fechamento para formatação
            data_timestamp = get_lead_closure_date(lead, CUSTOM_FIELD_DATA_FECHAMENTO)
            if not data_timestamp:
                continue  # Não deveria chegar aqui, mas por segurança
            
            # Determinar corretor final
            corretor_final = corretor_custom or "Não atribuído"
            
            # Filtrar por corretor se especificado
            if corretor and isinstance(corretor, str) and corretor.strip():
                if ',' in corretor:
                    corretores_list = [c.strip() for c in corretor.split(',')]
                    if corretor_final not in corretores_list:
                        continue
                else:
                    if corretor_final != corretor:
                        continue
                
            # Filtrar por fonte se especificado
            if fonte and isinstance(fonte, str) and fonte.strip():
                if ',' in fonte:
                    fontes_list = [f.strip() for f in fonte.split(',')]
                    if fonte_lead not in fontes_list:
                        continue
                else:
                    if fonte_lead != fonte:
                        continue
            
            # Formatar data usando data_fechamento
            data_formatada = format_timestamp_brazil(data_timestamp)
            valor_formatado = f"R$ {price:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')
            
            # Formatar data de criação
            if created_at:
                data_criacao_formatada = format_timestamp_brazil(created_at, "%Y-%m-%d")
            else:
                data_criacao_formatada = "N/A"
            
            venda_dict = {
                "Data de Criação": data_criacao_formatada,  # Data de criação do lead
                "Data da Venda": data_formatada,  # PO: usando data_fechamento
                "Nome do Lead": lead_name,
                "Corretor": corretor_final,
                "Fonte": fonte_lead,
                "Anúncio": anuncio_lead,  # Novo campo
                "Público": publico_lead,  # Novo campo (conjunto de anúncios)
                "Produto": produto_lead,  # Campo Produto
                "Data da Proposta": data_proposta_lead,  # Campo Data da Proposta
                "Valor da Venda": valor_formatado
            }
            
            # Adicionar à lista de vendas
            vendas_detalhes.append(venda_dict)
        
        # NOVO: Processar todos os leads para leadsDetalhes
        logger.info("Processando todos os leads para leadsDetalhes...")
        for lead in all_leads_for_details:
            if not lead:
                continue
                
            lead_name = lead.get("name", "")
            created_at = lead.get("created_at")
            status_id = lead.get("status_id")
            pipeline_id = lead.get("pipeline_id")
            
            # Extrair custom fields
            custom_fields = lead.get("custom_fields_values", [])
            fonte_lead = "N/A"
            corretor_custom = None
            anuncio_lead = "N/A"  # Novo campo
            publico_lead = "N/A"  # Novo campo (conjunto de anúncios)
            produto_lead = "N/A"  # Campo Produto
            data_proposta_lead = format_proposal_date(lead, CUSTOM_FIELD_DATA_PROPOSTA)  # Campo Data da Proposta
            
            if custom_fields and isinstance(custom_fields, list):
                for field in custom_fields:
                    if field and isinstance(field, dict):
                        field_id = field.get("field_id")
                        values = field.get("values", [])
                        
                        if field_id == 837886 and values:  # Fonte
                            fonte_lead = values[0].get("value", "N/A")
                        elif field_id == 837920 and values:  # Corretor
                            corretor_custom = values[0].get("value")
                        elif field_id == 837846 and values:  # Anúncio
                            anuncio_lead = values[0].get("value", "N/A")
                        elif field_id == 837844 and values:  # Público (conjunto de anúncios)
                            publico_lead = values[0].get("value", "N/A")
                        elif field_id == 857264 and values:  # Produto
                            produto_lead = values[0].get("value", "N/A")
            
            # Determinar corretor final
            if corretor_custom:
                corretor_final = corretor_custom
            else:
                corretor_final = "Não atribuído"
            
            # Determinar funil baseado no pipeline_id
            if pipeline_id == PIPELINE_VENDAS:
                funil = "Funil de Vendas"
            elif pipeline_id == PIPELINE_REMARKETING:
                funil = "Remarketing"
            else:
                funil = "Não atribuído"
            
            # Filtrar por corretor se especificado
            if corretor and isinstance(corretor, str) and corretor.strip():
                if ',' in corretor:
                    corretores_list = [c.strip() for c in corretor.split(',')]
                    if corretor_final not in corretores_list:
                        continue
                else:
                    if corretor_final != corretor:
                        continue
                
            # Filtrar por fonte se especificado - suporta múltiplos valores separados por vírgula
            if fonte and isinstance(fonte, str) and fonte.strip():
                if ',' in fonte:
                    fontes_list = [f.strip() for f in fonte.split(',')]
                    if fonte_lead not in fontes_list:
                        continue
                else:
                    if fonte_lead != fonte:
                        continue
            
            # Mapear status_id para nome do status
            status_name = "Ativo"  # Padrão
            if status_id == 142:
                status_name = "Venda Concluída"
            elif status_id == 143:
                status_name = "Perdido"
            elif status_id == STATUS_CONTRATO_ASSINADO:
                status_name = "Contrato Assinado"
            elif status_id in [80689711, 80689715, 80689719, 80689723, 80689727]:
                status_name = "Em Negociação"
            
            # Determinar etapa baseado no status_id usando nomes reais da API
            etapa = status_map.get(status_id, f"Status {status_id}")
            
            # Formatar data de criação
            if created_at:
                data_criacao_formatada = format_timestamp_brazil(created_at, "%Y-%m-%d")
            else:
                data_criacao_formatada = "N/A"
            
            # Verificar se é uma proposta usando o novo campo boolean
            is_lead_proposta = is_proposta(lead)
            
            # Criar objeto do lead
            lead_obj = {
                "Data de Criação": data_criacao_formatada,
                "Nome do Lead": lead_name,
                "Corretor": corretor_final,
                "Fonte": fonte_lead,
                "Anúncio": anuncio_lead,  # Novo campo
                "Público": publico_lead,  # Novo campo (conjunto de anúncios)
                "Produto": produto_lead,  # Campo Produto
                "Data da Proposta": data_proposta_lead,  # Campo Data da Proposta
                "Funil": funil,
                "Etapa": etapa,
                "Status": status_name,
                "É Proposta": is_lead_proposta  # NOVO: Campo boolean de proposta
            }
            
            # Separar entre orgânicos e leads não-orgânicos baseado na fonte
            if fonte_lead == "Orgânico":
                organicos_detalhes.append(lead_obj)
            else:
                leads_detalhes.append(lead_obj)
        
        # PROPOSTAS serão processadas depois dos totais serem calculados
        
        # Ordenar leads por data de criação (mais recentes primeiro)
        leads_detalhes.sort(key=lambda x: x["Data de Criação"], reverse=True)
        organicos_detalhes.sort(key=lambda x: x["Data de Criação"], reverse=True)
        
        # Ordenar as listas por data (mais recentes primeiro)
        reunioes_detalhes.sort(key=lambda x: datetime.strptime(x["Data da Reunião"], "%d/%m/%Y %H:%M"), reverse=True)
        reunioes_organicas_detalhes.sort(key=lambda x: datetime.strptime(x["Data da Reunião"], "%d/%m/%Y %H:%M"), reverse=True)
        vendas_detalhes.sort(key=lambda x: datetime.strptime(x["Data da Venda"], "%d/%m/%Y %H:%M"), reverse=True)
        
        # NOVO: Ordenar propostas por data da proposta (mais recentes primeiro)
        propostas_detalhes.sort(key=lambda x: datetime.strptime(x["Data da Proposta"], "%d/%m/%Y %H:%M") if x["Data da Proposta"] != "N/A" else datetime.min, reverse=True)
        
        # Calcular totais
        total_leads = len(leads_detalhes)  # Leads não-orgânicos
        total_organicos = len(organicos_detalhes)  # NOVO: Leads orgânicos
        total_reunioes = len(reunioes_detalhes)  # Reuniões não-orgânicas
        total_reunioes_organicas = len(reunioes_organicas_detalhes)  # NOVO: Reuniões orgânicas
        total_vendas = len(vendas_detalhes)
        
        # NOVO: Contar propostas usando o campo boolean (contagem anterior para compatibilidade)
        total_propostas_leads_boolean = sum(1 for lead in leads_detalhes if lead.get("É Proposta") == True)
        total_propostas_organicos_boolean = sum(1 for lead in organicos_detalhes if lead.get("É Proposta") == True)
        total_propostas_geral_boolean = total_propostas_leads_boolean + total_propostas_organicos_boolean
        
        # NOVO: Processar propostas detalhadas DEPOIS dos totais serem calculados
        logger.info("Processando propostas detalhadas buscando TODOS os leads...")
        
        # Buscar TODOS os leads sem filtro de data de criação para encontrar todas as propostas
        params_propostas_vendas = {
            'filter[pipeline_id]': PIPELINE_VENDAS,
            'limit': 500,
            'with': 'contacts,custom_fields_values'
        }
        
        params_propostas_remarketing = {
            'filter[pipeline_id]': PIPELINE_REMARKETING,
            'limit': 500,
            'with': 'contacts,custom_fields_values'
        }
        
        try:
            # Buscar TODOS os leads (sem filtro de data de criação)
            leads_vendas_propostas = kommo_api.get_all_leads(params_propostas_vendas)
            leads_remarketing_propostas = kommo_api.get_all_leads(params_propostas_remarketing)
            
            # Combinar todos os leads
            all_leads_propostas = leads_vendas_propostas + leads_remarketing_propostas
            
            # Processar propostas
            for lead in all_leads_propostas:
                if not lead:
                    continue
                    
                # Validar se é proposta no período correto
                if not validate_proposta_in_period(lead, start_timestamp, end_timestamp):
                    continue
                
                lead_name = lead.get("name", "")
                created_at = lead.get("created_at")
                status_id = lead.get("status_id")
                pipeline_id = lead.get("pipeline_id")
                
                # Extrair campos customizados
                fonte_lead = extract_custom_field_value(lead, 837886) or "N/A"
                corretor_custom = extract_custom_field_value(lead, 837920)
                anuncio_lead = extract_custom_field_value(lead, 837846) or "N/A"
                publico_lead = extract_custom_field_value(lead, 837844) or "N/A"
                produto_lead = extract_custom_field_value(lead, CUSTOM_FIELD_PRODUTO) or "N/A"
                data_proposta_lead = format_proposal_date(lead, CUSTOM_FIELD_DATA_PROPOSTA)
                
                # Determinar corretor final
                corretor_final = corretor_custom or "Não atribuído"
                
                # Filtrar por corretor se especificado
                if corretor and isinstance(corretor, str) and corretor.strip():
                    if ',' in corretor:
                        corretores_list = [c.strip() for c in corretor.split(',')]
                        if corretor_final not in corretores_list:
                            continue
                    else:
                        if corretor_final != corretor:
                            continue
                
                # Filtrar por fonte se especificado
                if fonte and isinstance(fonte, str) and fonte.strip():
                    if ',' in fonte:
                        fontes_list = [f.strip() for f in fonte.split(',')]
                        if fonte_lead not in fontes_list:
                            continue
                    else:
                        if fonte_lead != fonte:
                            continue
                
                # Determinar funil baseado no pipeline_id
                if pipeline_id == PIPELINE_VENDAS:
                    funil = "Funil de Vendas"
                elif pipeline_id == PIPELINE_REMARKETING:
                    funil = "Remarketing"
                else:
                    funil = "Não atribuído"
                
                # Mapear status_id para nome do status
                status_name = "Ativo"
                if status_id == 142:
                    status_name = "Venda Concluída"
                elif status_id == 143:
                    status_name = "Perdido"
                elif status_id == STATUS_CONTRATO_ASSINADO:
                    status_name = "Contrato Assinado"
                elif status_id in [80689711, 80689715, 80689719, 80689723, 80689727]:
                    status_name = "Em Negociação"
                
                # Determinar etapa baseado no status_id
                etapa = status_map.get(status_id, f"Status {status_id}")
                
                # Formatar data de criação do lead
                if created_at:
                    data_criacao_formatada = format_timestamp_brazil(created_at, "%Y-%m-%d")
                else:
                    data_criacao_formatada = "N/A"
                
                # Criar objeto da proposta
                proposta_dict = {
                    "Data de Criação do Lead": data_criacao_formatada,
                    "Data da Proposta": data_proposta_lead,
                    "Nome do Lead": lead_name,
                    "Corretor": corretor_final,
                    "Fonte": fonte_lead,
                    "Anúncio": anuncio_lead,
                    "Público": publico_lead,
                    "Produto": produto_lead,
                    "Funil": funil,
                    "Etapa": etapa,
                    "Status": status_name
                }
                
                # Adicionar proposta à lista
                propostas_detalhes.append(proposta_dict)
                
        except Exception as e:
            logger.error(f"Erro ao processar propostas: {e}")
        
        # Contar propostas detalhadas finais
        total_propostas_detalhes = len(propostas_detalhes)
        
        valor_total_vendas = sum(
            float(v["Valor da Venda"].replace("R$ ", "").replace(".", "").replace(",", "."))
            for v in vendas_detalhes
        )
        
        # Montar resposta
        response = {
            "leadsDetalhes": leads_detalhes,  # Leads não-orgânicos
            "organicosDetalhes": organicos_detalhes,  # NOVO: Leads orgânicos
            "reunioesDetalhes": reunioes_detalhes,  # Reuniões não-orgânicas
            "reunioesOrganicasDetalhes": reunioes_organicas_detalhes,  # NOVO: Reuniões orgânicas
            "vendasDetalhes": vendas_detalhes,
            "propostasDetalhes": propostas_detalhes,  # NOVO: Propostas unificadas (filtradas por Data da Proposta)
            "summary": {
                "total_leads": total_leads,  # Leads não-orgânicos
                "total_organicos": total_organicos,  # NOVO: Leads orgânicos
                "total_reunioes": total_reunioes,  # Reuniões não-orgânicas
                "total_reunioes_organicas": total_reunioes_organicas,  # NOVO: Reuniões orgânicas
                "total_vendas": total_vendas,
                "valor_total_vendas": valor_total_vendas,
                # NOVO: Estatísticas de propostas usando campo boolean (compatibilidade)
                "total_propostas": total_propostas_geral_boolean,
                "propostas_leads": total_propostas_leads_boolean,
                "propostas_organicos": total_propostas_organicos_boolean,
                # NOVO: Estatísticas de propostas detalhadas (filtradas por Data da Proposta - unificada)
                "total_propostas_detalhes": total_propostas_detalhes
            },
            "_metadata": {
                "periodo_dias": days if not (start_date and end_date) else "periodo_customizado",
                "data_inicio": start_dt.strftime('%Y-%m-%d %H:%M:%S'),
                "data_fim": end_dt.strftime('%Y-%m-%d %H:%M:%S'),
                "filtro_tipo": "filtro_por_data_implementado",
                "limit_registros": limit,
                "corretor_filter": corretor if isinstance(corretor, str) else None,
                "fonte_filter": fonte if isinstance(fonte, str) else None,
                "alinhamento_v2": "aplicado",
                "regras_sincronizadas": [
                    "pular_leads_sem_corretor_quando_sem_filtro",
                    "vendas_apenas_com_data_fechamento",
                    "usar_updated_at_para_consistencia",
                    "buscar_ambos_pipelines_vendas_remarketing",
                    "validacao_reuniao_verdadeira_com_completed_at"
                ],
                "status_ids_utilizados": {
                    "reuniao": "Tarefas tipo 2 (is_completed=true) do Funil de Vendas",
                    "venda": [STATUS_CONTRATO_ASSINADO, STATUS_VENDA_FINAL]
                },
                "custom_fields_utilizados": {
                    "fonte": 837886,
                    "corretor": 837920,
                    "data_fechamento": CUSTOM_FIELD_DATA_FECHAMENTO,
                    "proposta": CUSTOM_FIELD_PROPOSTA
                },
                "pipelines_utilizados": {
                    "funil_vendas": PIPELINE_VENDAS,
                    "remarketing": PIPELINE_REMARKETING
                }
            }
        }
        
        logger.info(f"Tabelas detalhadas geradas: {total_reunioes} reuniões, {total_vendas} vendas, {total_propostas_geral_boolean} propostas (boolean), {total_propostas_detalhes} propostas detalhadas (filtradas por Data da Proposta)")
        return response
        
    except Exception as e:
        logger.error(f"Erro ao gerar tabelas detalhadas: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")


