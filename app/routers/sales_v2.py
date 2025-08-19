from fastapi import APIRouter, Query, HTTPException
from typing import Optional
from datetime import datetime, timedelta
import traceback
import logging
from app.utils.date_helpers import validate_sale_in_period, get_lead_closure_date, extract_custom_field_value as extract_field, is_date_in_period

# Configurar logger
logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v2", tags=["Sales V2 API"])

# Função auxiliar para extrair valores de custom fields
def get_custom_field_value(lead, field_id):
    """Extrai valor de custom field de forma segura"""
    try:
        custom_fields = lead.get("custom_fields_values")
        if not custom_fields or not isinstance(custom_fields, list):
            return None
            
        for field in custom_fields:
            if not field or not isinstance(field, dict):
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

@router.get("/sales/kpis")
async def get_sales_kpis(
    days: int = Query(30, description="Período em dias para análise"),
    corretor: Optional[str] = Query(None, description="Nome do corretor para filtrar dados"),
    fonte: Optional[str] = Query(None, description="Fonte para filtrar dados"),
    start_date: Optional[str] = Query(None, description="Data de início (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="Data de fim (YYYY-MM-DD)")
):
    """
    Retorna KPIs básicos de vendas otimizado para performance.
    """
    try:
        logger.info(f"Buscando KPIs de vendas para {days} dias, corretor: {corretor}, fonte: {fonte}")
        
        from app.services.kommo_api import KommoAPI
        kommo_api = KommoAPI()
        
        # Calcular parâmetros de tempo
        import time
        
        if start_date and end_date:
            try:
                start_dt = datetime.strptime(start_date, '%Y-%m-%d')
                end_dt = datetime.strptime(end_date, '%Y-%m-%d')
                end_dt = end_dt.replace(hour=23, minute=59, second=59)
                start_time = int(start_dt.timestamp())
                end_time = int(end_dt.timestamp())
                
                # Para reuniões: incluir 23:59 do dia anterior para capturar reuniões agendadas na virada do dia
                meetings_start_dt = start_dt - timedelta(days=1)
                meetings_start_dt = meetings_start_dt.replace(hour=23, minute=59, second=0)
                meetings_start_time = int(meetings_start_dt.timestamp())
            except ValueError:
                raise HTTPException(status_code=400, detail="Formato de data inválido. Use YYYY-MM-DD")
        else:
            end_time = int(time.time())
            start_time = end_time - (days * 24 * 60 * 60)
            
            # Para reuniões: incluir 23:59 do dia anterior
            meetings_start_time = start_time - (24 * 60 * 60) + (23 * 60 * 60 + 59 * 60)  # -1 dia + 23:59
        
        # Período anterior para comparação
        period_duration = end_time - start_time
        previous_start_time = start_time - period_duration
        previous_end_time = start_time
        
        # IDs importantes
        PIPELINE_VENDAS = 10516987
        PIPELINE_REMARKETING = 11059911
        STATUS_CONTRATO_ASSINADO = 80689759
        STATUS_VENDA_FINAL = 142
        CUSTOM_FIELD_DATA_FECHAMENTO = 858126
        CUSTOM_FIELD_ESTADO = 851638  # Campo ESTADO

        # Buscar apenas vendas
        # VENDAS: Buscar leads com status de venda
        current_vendas_vendas_params = {
            "filter[statuses][0][pipeline_id]": PIPELINE_VENDAS,
            "filter[statuses][0][status_id]": STATUS_VENDA_FINAL,
            "filter[statuses][1][pipeline_id]": PIPELINE_VENDAS,
            "filter[statuses][1][status_id]": STATUS_CONTRATO_ASSINADO,
            "limit": 500,
            "with": "contacts,tags,custom_fields_values"
        }
        
        current_vendas_remarketing_params = {
            "filter[statuses][0][pipeline_id]": PIPELINE_REMARKETING,
            "filter[statuses][0][status_id]": STATUS_VENDA_FINAL,
            "filter[statuses][1][pipeline_id]": PIPELINE_REMARKETING,
            "filter[statuses][1][status_id]": STATUS_CONTRATO_ASSINADO,
            "limit": 500,
            "with": "contacts,tags,custom_fields_values"
        }
        
        # Vendas anteriores
        previous_vendas_vendas_params = {
            "filter[statuses][0][pipeline_id]": PIPELINE_VENDAS,
            "filter[statuses][0][status_id]": STATUS_VENDA_FINAL,
            "filter[statuses][1][pipeline_id]": PIPELINE_VENDAS,
            "filter[statuses][1][status_id]": STATUS_CONTRATO_ASSINADO,
            "limit": 500,
            "with": "contacts,tags,custom_fields_values"
        }
        
        previous_vendas_remarketing_params = {
            "filter[statuses][0][pipeline_id]": PIPELINE_REMARKETING,
            "filter[statuses][0][status_id]": STATUS_VENDA_FINAL,
            "filter[statuses][1][pipeline_id]": PIPELINE_REMARKETING,
            "filter[statuses][1][status_id]": STATUS_CONTRATO_ASSINADO,
            "limit": 500,
            "with": "contacts,tags,custom_fields_values"
        }
        
        # Buscar dados de vendas atuais
        try:
            current_vendas_vendas_leads = kommo_api.get_all_leads({k: v for k, v in current_vendas_vendas_params.items() if k != 'limit'})
            current_vendas_vendas_data = {"_embedded": {"leads": current_vendas_vendas_leads}}
        except Exception as e:
            logger.error(f"Erro ao buscar vendas atuais: {e}")
            current_vendas_vendas_data = {"_embedded": {"leads": []}}

        try:
            current_vendas_remarketing_leads = kommo_api.get_all_leads({k: v for k, v in current_vendas_remarketing_params.items() if k != 'limit'})
            current_vendas_remarketing_data = {"_embedded": {"leads": current_vendas_remarketing_leads}}
        except Exception as e:
            logger.error(f"Erro ao buscar vendas remarketing atuais: {e}")
            current_vendas_remarketing_data = {"_embedded": {"leads": []}}
        
        # Buscar dados de vendas anteriores
        try:
            previous_vendas_vendas_leads = kommo_api.get_all_leads({k: v for k, v in previous_vendas_vendas_params.items() if k != 'limit'})
            previous_vendas_vendas_data = {"_embedded": {"leads": previous_vendas_vendas_leads}}
        except Exception as e:
            logger.error(f"Erro ao buscar vendas anteriores: {e}")
            previous_vendas_vendas_data = {"_embedded": {"leads": []}}

        try:
            previous_vendas_remarketing_leads = kommo_api.get_all_leads({k: v for k, v in previous_vendas_remarketing_params.items() if k != 'limit'})
            previous_vendas_remarketing_data = {"_embedded": {"leads": previous_vendas_remarketing_leads}}
        except Exception as e:
            logger.error(f"Erro ao buscar vendas remarketing anteriores: {e}")
            previous_vendas_remarketing_data = {"_embedded": {"leads": []}}

        # Combinar VENDAS de ambos os pipelines
        current_vendas_all = []
        if current_vendas_vendas_data and "_embedded" in current_vendas_vendas_data:
            vendas_leads = current_vendas_vendas_data["_embedded"].get("leads", [])
            current_vendas_all.extend(vendas_leads)

        if current_vendas_remarketing_data and "_embedded" in current_vendas_remarketing_data:
            vendas_leads = current_vendas_remarketing_data["_embedded"].get("leads", [])
            current_vendas_all.extend(vendas_leads)

        # Criar estruturas para vendas
        current_vendas_data = {"_embedded": {"leads": current_vendas_all}}

        # Combinar vendas anteriores
        previous_vendas_all = []
        if previous_vendas_vendas_data and "_embedded" in previous_vendas_vendas_data:
            vendas_leads = previous_vendas_vendas_data["_embedded"].get("leads", [])
            previous_vendas_all.extend(vendas_leads)

        if previous_vendas_remarketing_data and "_embedded" in previous_vendas_remarketing_data:
            vendas_leads = previous_vendas_remarketing_data["_embedded"].get("leads", [])
            previous_vendas_all.extend(vendas_leads)

        previous_vendas_data = {"_embedded": {"leads": previous_vendas_all}}

        logger.info(f"Total vendas atuais: {len(current_vendas_all)}")
        logger.info(f"Total vendas anteriores: {len(previous_vendas_all)}")

        # Função para aplicar filtros
        def apply_filters(leads, corretor_filter=None, fonte_filter=None):
            """Aplica filtros de corretor e fonte"""
            filtered = []
            for lead in leads:
                if not lead:
                    continue
                
                # Filtrar por corretor se especificado
                if corretor_filter:
                    corretor_lead = get_custom_field_value(lead, 837920)  # Campo Corretor
                    if corretor_lead != corretor_filter:
                        continue
                
                # Filtrar por fonte se especificado
                if fonte_filter:
                    fonte_lead = get_custom_field_value(lead, 837886)  # Campo Fonte
                    if fonte_lead != fonte_filter:
                        continue
                
                filtered.append(lead)
            
            return filtered

        # Aplicar filtros nas vendas
        filtered_vendas = apply_filters(current_vendas_all, corretor, fonte)
        
        # Filtrar vendas por data_fechamento no período
        valid_sales = []
        total_revenue = 0
        
        for lead in filtered_vendas:
            if validate_sale_in_period(lead, start_time, end_time, CUSTOM_FIELD_DATA_FECHAMENTO):
                valid_sales.append(lead)
                price = lead.get("price", 0) or 0
                total_revenue += price

        sales_leads = len(valid_sales)
        avg_deal_size = (total_revenue / sales_leads) if sales_leads > 0 else 0

        # Aplicar filtros nas vendas anteriores
        filtered_previous_vendas = apply_filters(previous_vendas_all, corretor, fonte)
        
        # Filtrar vendas anteriores por data_fechamento no período
        previous_valid_sales = []
        previous_total_revenue = 0
        
        for lead in filtered_previous_vendas:
            if validate_sale_in_period(lead, previous_start_time, previous_end_time, CUSTOM_FIELD_DATA_FECHAMENTO):
                previous_valid_sales.append(lead)
                price = lead.get("price", 0) or 0
                previous_total_revenue += price

        previous_sales_leads = len(previous_valid_sales)

        # Calcular crescimento
        sales_growth = ((sales_leads - previous_sales_leads) / previous_sales_leads * 100) if previous_sales_leads > 0 else 0

        return {
            "kpis": {
                "sales": {
                    "total": sales_leads,
                    "previous": previous_sales_leads,
                    "growth": round(sales_growth, 2),
                    "total_revenue": round(total_revenue, 2),
                    "avg_deal_size": round(avg_deal_size, 2)
                }
            },
            "_metadata": {
                "period_days": days,
                "corretor_filter": corretor,
                "fonte_filter": fonte,
                "start_time": start_time,
                "end_time": end_time,
                "status_ids_utilizados": {
                    "venda": [STATUS_CONTRATO_ASSINADO, STATUS_VENDA_FINAL]
                }
            }
        }

    except Exception as e:
        logger.error(f"Erro em get_sales_kpis: {str(e)}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")

@router.get("/charts/leads-by-user")
async def get_leads_by_user_chart(
    days: int = Query(30, description="Período em dias para análise"),
    corretor: Optional[str] = Query(None, description="Nome do corretor para filtrar dados"),
    fonte: Optional[str] = Query(None, description="Fonte para filtrar dados"),
    start_date: Optional[str] = Query(None, description="Data de início (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="Data de fim (YYYY-MM-DD)")
):
    """
    Retorna dados de leads por usuário/corretor incluindo propostas.
    Inclui campo proposalsHeld baseado no campo boolean 861100.
    """
    try:
        logger.info(f"Buscando dados de leads por usuario para {days} dias, corretor: {corretor}, fonte: {fonte}")
        
        from app.services.kommo_api import KommoAPI
        kommo_api = KommoAPI()
        
        # Calcular parâmetros de tempo
        import time
        
        if start_date and end_date:
            try:
                start_dt = datetime.strptime(start_date, '%Y-%m-%d')
                end_dt = datetime.strptime(end_date, '%Y-%m-%d')
                end_dt = end_dt.replace(hour=23, minute=59, second=59)
                start_time = int(start_dt.timestamp())
                end_time = int(end_dt.timestamp())
            except ValueError:
                raise HTTPException(status_code=400, detail="Formato de data inválido. Use YYYY-MM-DD")
        else:
            end_time = int(time.time())
            start_time = end_time - (days * 24 * 60 * 60)
        
        # IDs dos campos customizados
        CUSTOM_FIELD_CORRETOR = 837920
        CUSTOM_FIELD_FONTE = 837886
        CUSTOM_FIELD_PROPOSTA = 861100  # Novo campo boolean
        
        # IDs dos pipelines
        PIPELINE_VENDAS = 10516987
        PIPELINE_REMARKETING = 11059911
        
        # Função auxiliar para verificar se é proposta
        def is_proposta(lead):
            """Verifica se um lead é uma proposta usando o campo boolean 861100"""
            try:
                custom_fields = lead.get("custom_fields_values", [])
                if not custom_fields:
                    return False
                    
                for field in custom_fields:
                    if not field:
                        continue
                    if field.get("field_id") == CUSTOM_FIELD_PROPOSTA:
                        values = field.get("values")
                        if values and isinstance(values, list) and len(values) > 0:
                            first_value = values[0]
                            if isinstance(first_value, dict):
                                value = first_value.get("value")
                            else:
                                value = first_value
                            # Campo boolean pode retornar True, "true", "1", 1, etc.
                            return value in [True, "true", "1", 1, "True", "TRUE"]
                return False
            except Exception as e:
                logger.error(f"Erro ao verificar se lead é proposta: {e}")
                return False
        
        # Buscar leads
        leads_params = {
            "filter[created_at][from]": start_time,
            "filter[created_at][to]": end_time,
            "limit": 500,
            "with": "custom_fields_values"
        }
        
        # NOVO: Buscar tarefas de reunião realizadas para contagem real
        # Para reuniões: incluir 23:59 do dia anterior para capturar reuniões agendadas na virada do dia
        if start_date and end_date:
            meetings_start_dt = datetime.strptime(start_date, '%Y-%m-%d') - timedelta(days=1)
            meetings_start_dt = meetings_start_dt.replace(hour=23, minute=59, second=0)
            meetings_start_time = int(meetings_start_dt.timestamp())
        else:
            meetings_start_time = start_time - (24 * 60 * 60) + (23 * 60 * 60 + 59 * 60)  # -1 dia + 23:59
        
        tasks_params = {
            'filter[task_type_id]': 2,  # Tipo de tarefa: reunião
            'filter[is_completed]': 1,  # Apenas concluídas
            'filter[complete_till][from]': meetings_start_time,  # Incluir 23:59 do dia anterior
            'filter[complete_till][to]': end_time,              # Filtro de data
            'limit': 500
        }
        
        try:
            leads_data = kommo_api.get_leads(leads_params)
        except Exception as e:
            logger.error(f"Erro ao buscar leads: {e}")
            leads_data = {"_embedded": {"leads": []}}
            
        # NOVO: Buscar tarefas de reunião
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
        
        # Buscar pipelines para mapear status para nomes de etapas
        try:
            pipelines_data = kommo_api.get_pipelines()
        except Exception as e:
            logger.error(f"Erro ao buscar pipelines: {e}")
            pipelines_data = {"_embedded": {"pipelines": []}}
        
        # Criar mapa de usuários
        users_map = {}
        if users_data and "_embedded" in users_data:
            for user in users_data["_embedded"].get("users", []):
                users_map[user["id"]] = user["name"]
        
        # Criar mapa de status e pipelines
        status_map = {}
        pipeline_map = {}
        if pipelines_data and "_embedded" in pipelines_data:
            for pipeline in pipelines_data["_embedded"].get("pipelines", []):
                pipeline_id = pipeline.get("id")
                pipeline_name = pipeline.get("name", f"Pipeline {pipeline_id}")
                pipeline_map[pipeline_id] = pipeline_name
                
                # Mapear status dentro do pipeline
                statuses = pipeline.get("_embedded", {}).get("statuses", [])
                for status in statuses:
                    status_id = status.get("id")
                    status_name = status.get("name", f"Status {status_id}")
                    status_map[status_id] = {
                        "name": status_name,
                        "pipeline_id": pipeline_id,
                        "pipeline_name": pipeline_name
                    }
        
        # NOVO: Criar mapa de leads para busca rápida das reuniões
        leads_map = {}
        if leads_data and "_embedded" in leads_data:
            for lead in leads_data["_embedded"].get("leads", []):
                if lead and lead.get("id"):
                    leads_map[lead.get("id")] = lead
        
        # NOVO: Processar reuniões reais e contar por corretor
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
            
            for task in reunion_tasks:
                if not task or task.get('entity_type') != 'leads':
                    continue
                
                lead_id = task.get('entity_id')
                lead = leads_map.get(lead_id)
                
                if not lead:
                    continue
                
                # Extrair corretor do lead usando custom field
                corretor_lead = get_custom_field_value(lead, CUSTOM_FIELD_CORRETOR)
                fonte_lead = get_custom_field_value(lead, CUSTOM_FIELD_FONTE)
                
                # Aplicar mesmos filtros que serão aplicados nos leads
                if corretor and corretor_lead != corretor:
                    continue
                if fonte and fonte_lead != fonte:
                    continue
                
                # Determinar corretor final (mesma lógica dos leads)
                final_corretor = corretor_lead or users_map.get(lead.get("responsible_user_id"), "Usuário Sem Nome")
                
                # Contar reunião para este corretor
                meetings_by_corretor[final_corretor] = meetings_by_corretor.get(final_corretor, 0) + 1
            
            logger.info(f"Reuniões contadas por corretor: {meetings_by_corretor}")
        
        # Processar leads por corretor
        leads_by_user = {}
        
        if leads_data and "_embedded" in leads_data:
            all_leads = leads_data["_embedded"].get("leads", [])
            
            for lead in all_leads:
                if not lead:
                    continue
                
                # Extrair custom fields
                custom_fields = lead.get("custom_fields_values", [])
                corretor_lead = None
                fonte_lead = None
                anuncio_lead = None
                publico_lead = None
                produto_lead = None
                
                for field in custom_fields:
                    if not field:
                        continue
                    field_id = field.get("field_id")
                    values = field.get("values", [])
                    
                    if field_id == CUSTOM_FIELD_CORRETOR and values:  # Corretor
                        corretor_lead = values[0].get("value") if values[0] else None
                    elif field_id == CUSTOM_FIELD_FONTE and values:  # Fonte
                        fonte_lead = values[0].get("value") if values[0] else None
                    elif field_id == 837846 and values:  # Anúncio
                        anuncio_lead = values[0].get("value") if values[0] else None
                    elif field_id == 837844 and values:  # Público (conjunto de anúncios)
                        publico_lead = values[0].get("value") if values[0] else None
                    elif field_id == 857264 and values:  # Produto
                        produto_lead = values[0].get("value") if values[0] else None
                
                # Aplicar filtros
                if corretor and corretor_lead != corretor:
                    continue
                if fonte and fonte_lead != fonte:
                    continue
                
                # Determinar corretor final
                final_corretor = corretor_lead or users_map.get(lead.get("responsible_user_id"), "Usuário Sem Nome")
                
                # Inicializar contador se não existir
                if final_corretor not in leads_by_user:
                    leads_by_user[final_corretor] = {
                        "name": final_corretor,
                        "value": 0,           # Total de leads
                        "active": 0,          # Leads ativos
                        "meetings": 0,        # Reuniões agendadas/realizadas
                        "meetingsHeld": 0,    # Reuniões realizadas (estimativa)
                        "proposalsHeld": 0,   # NOVO: Propostas usando campo boolean
                        "sales": 0,           # Vendas
                        "lost": 0,            # Leads perdidos
                        "leads": []           # NOVO: Array com detalhes de cada lead
                    }
                
                # Incrementar contadores
                leads_by_user[final_corretor]["value"] += 1
                
                # Verificar se é uma proposta usando o campo boolean
                if is_proposta(lead):
                    leads_by_user[final_corretor]["proposalsHeld"] += 1
                
                # NOVO: Criar objeto detalhado do lead
                lead_id = lead.get("id")
                lead_name = lead.get("name", "Lead sem nome")
                created_at = lead.get("created_at")
                pipeline_id = lead.get("pipeline_id")
                
                # Formatar data de criação
                if created_at:
                    created_date = datetime.fromtimestamp(created_at).strftime("%Y-%m-%d")
                else:
                    created_date = "N/A"
                
                # Obter informações de pipeline e status
                status_info = status_map.get(status_id, {})
                pipeline_name = status_info.get("pipeline_name", pipeline_map.get(pipeline_id, "Funil Desconhecido"))
                etapa_name = status_info.get("name", f"Status {status_id}")
                
                # Montar objeto detalhado do lead
                lead_detail = {
                    "id": lead_id,
                    "leadName": lead_name,
                    "fonte": fonte_lead or "N/A",
                    "anuncio": anuncio_lead or "N/A",
                    "publico": publico_lead or "N/A",
                    "produto": produto_lead or "N/A",
                    "funil": pipeline_name,
                    "etapa": etapa_name,
                    "createdDate": created_date,
                    "status_id": status_id,
                    "is_proposta": is_proposta(lead)
                }
                
                # Adicionar ao array de leads do corretor
                leads_by_user[final_corretor]["leads"].append(lead_detail)
                
                # Classificar por status
                status_id = lead.get("status_id")
                if status_id == 142:  # Won (Venda ganha)
                    leads_by_user[final_corretor]["sales"] += 1
                elif status_id == 143:  # Lost (Lead perdido)
                    leads_by_user[final_corretor]["lost"] += 1
                elif status_id in [80689731, 80689727]:  # Status de reunião
                    leads_by_user[final_corretor]["meetings"] += 1
                    leads_by_user[final_corretor]["active"] += 1
                else:  # Outros status ativos
                    leads_by_user[final_corretor]["active"] += 1
        
        # NOVO: Aplicar contagem real de reuniões em vez da estimativa
        for corretor_name in leads_by_user.keys():
            # Usar contagem real de reuniões do mapa meetings_by_corretor
            real_meetings = meetings_by_corretor.get(corretor_name, 0)
            leads_by_user[corretor_name]["meetingsHeld"] = real_meetings
            logger.info(f"Corretor {corretor_name}: {real_meetings} reuniões reais")
        
        # Converter para lista e ordenar por total de leads
        leads_by_user_list = list(leads_by_user.values())
        leads_by_user_list.sort(key=lambda x: x["value"], reverse=True)
        
        # Calcular totais para estatísticas
        total_propostas = sum(user["proposalsHeld"] for user in leads_by_user_list)
        total_meetings_held = sum(user["meetingsHeld"] for user in leads_by_user_list)  # Agora usando dados reais
        total_leads = sum(user["value"] for user in leads_by_user_list)
        
        logger.info(f"Totais calculados - Leads: {total_leads}, Reuniões realizadas: {total_meetings_held}, Propostas: {total_propostas}")
        
        return {
            "leadsByUser": leads_by_user_list,
            "totalProposals": total_propostas,  # NOVO: Total de propostas
            "totalMeetingsHeld": total_meetings_held,  # Total de reuniões realizadas
            "_metadata": {
                "period_days": days,
                "corretor_filter": corretor,
                "fonte_filter": fonte,
                "generated_at": datetime.now().isoformat(),
                "total_users": len(leads_by_user_list),
                "total_leads": total_leads,
                "total_meetings_real": total_meetings_held,  # NOVO: Reuniões reais
                "optimized": True,
                "endpoint_version": "v2_with_real_meetings_and_proposals",
                "custom_fields_used": {
                    "corretor": CUSTOM_FIELD_CORRETOR,
                    "fonte": CUSTOM_FIELD_FONTE,
                    "proposta": CUSTOM_FIELD_PROPOSTA
                }
            }
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro ao gerar dados de leads por usuario: {str(e)}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")

@router.get("/sales/conversion-rates")
async def get_conversion_rates(
    days: int = Query(30, description="Periodo em dias para analise"),
    corretor: Optional[str] = Query(None, description="Nome do corretor para filtrar dados"),
    fonte: Optional[str] = Query(None, description="Fonte para filtrar dados"),
    start_date: Optional[str] = Query(None, description="Data de inicio (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="Data de fim (YYYY-MM-DD)")
):
    """
    Retorna taxas de conversao otimizadas com dados de funil (SEM PROPOSTAS).
    """
    try:
        logger.info(f"Buscando taxas de conversao para {days} dias, corretor: {corretor}, fonte: {fonte}")
        
        from app.services.kommo_api import KommoAPI
        kommo_api = KommoAPI()
        
        # Calcular parametros de tempo
        import time
        
        if start_date and end_date:
            try:
                start_dt = datetime.strptime(start_date, '%Y-%m-%d')
                end_dt = datetime.strptime(end_date, '%Y-%m-%d')
                end_dt = end_dt.replace(hour=23, minute=59, second=59)
                start_time = int(start_dt.timestamp())
                end_time = int(end_dt.timestamp())
            except ValueError:
                raise HTTPException(status_code=400, detail="Formato de data invalido. Use YYYY-MM-DD")
        else:
            end_time = int(time.time())
            start_time = end_time - (days * 24 * 60 * 60)
        
        # IDs importantes
        PIPELINE_VENDAS = 10516987
        PIPELINE_REMARKETING = 11059911
        STATUS_CONTRATO_ASSINADO = 80689759
        STATUS_VENDA_FINAL = 142
        CUSTOM_FIELD_DATA_FECHAMENTO = 858126
        
        # Buscar leads de AMBOS os pipelines
        leads_vendas_params = {
            "filter[pipeline_id]": PIPELINE_VENDAS,
            "filter[created_at][from]": start_time,
            "filter[created_at][to]": end_time,
            "limit": 500,
            "with": "custom_fields_values"
        }
        
        leads_remarketing_params = {
            "filter[pipeline_id]": PIPELINE_REMARKETING,
            "filter[created_at][from]": start_time,
            "filter[created_at][to]": end_time,
            "limit": 500,
            "with": "custom_fields_values"
        }
        
        # Buscar tarefas de reuniao concluidas
        tasks_params = {
            'filter[task_type_id]': 2,
            'filter[is_completed]': 1,
            'filter[complete_till][from]': start_time,
            'filter[complete_till][to]': end_time,
            'limit': 250
        }
        
        # Buscar dados
        try:
            leads_vendas_data = kommo_api.get_leads(leads_vendas_params)
        except Exception as e:
            logger.error(f"Erro ao buscar leads de vendas: {e}")
            leads_vendas_data = {"_embedded": {"leads": []}}
            
        try:
            leads_remarketing_data = kommo_api.get_leads(leads_remarketing_params)
        except Exception as e:
            logger.error(f"Erro ao buscar leads de remarketing: {e}")
            leads_remarketing_data = {"_embedded": {"leads": []}}
        
        try:
            all_tasks = kommo_api.get_all_tasks(tasks_params)
            tasks_data = {"_embedded": {"tasks": all_tasks}}
        except Exception as e:
            logger.error(f"Erro ao buscar tarefas: {e}")
            tasks_data = {"_embedded": {"tasks": []}}
        
        # Combinar leads de ambos os pipelines
        all_leads = []
        if leads_vendas_data and "_embedded" in leads_vendas_data:
            vendas_leads = leads_vendas_data["_embedded"].get("leads", [])
            if isinstance(vendas_leads, list):
                all_leads.extend(vendas_leads)
                
        if leads_remarketing_data and "_embedded" in leads_remarketing_data:
            remarketing_leads = leads_remarketing_data["_embedded"].get("leads", [])
            if isinstance(remarketing_leads, list):
                all_leads.extend(remarketing_leads)
        
        # Criar mapa de leads
        leads_map = {}
        for lead in all_leads:
            if lead and lead.get("id"):
                leads_map[lead.get("id")] = lead
        
        # Criar mapa de reunioes realizadas por lead
        meetings_by_lead = {}
        if tasks_data and "_embedded" in tasks_data:
            tasks_list = tasks_data["_embedded"].get("tasks", [])
            if isinstance(tasks_list, list):
                for task in tasks_list:
                    if (task and isinstance(task, dict) and 
                        task.get('entity_type') == 'leads'):
                        lead_id = task.get('entity_id')
                        if lead_id and lead_id in leads_map:
                            meetings_by_lead[lead_id] = meetings_by_lead.get(lead_id, 0) + 1
        
        # Processar leads com filtros
        filtered_leads = []
        for lead in all_leads:
            if not lead or not isinstance(lead, dict):
                continue
            
            # Extrair valores de forma segura
            corretor_lead = get_custom_field_value(lead, 837920)  # Corretor
            fonte_lead = get_custom_field_value(lead, 837886)     # Fonte
            
            # Aplicar filtros
            if corretor and isinstance(corretor, str) and corretor.strip():
                if ',' in corretor:
                    corretores_list = [c.strip() for c in corretor.split(',')]
                    if corretor_lead not in corretores_list:
                        continue
                else:
                    if corretor_lead != corretor:
                        continue
            
            if fonte and isinstance(fonte, str) and fonte.strip():
                if ',' in fonte:
                    fontes_list = [f.strip() for f in fonte.split(',')]
                    if fonte_lead not in fontes_list:
                        continue
                else:
                    if fonte_lead != fonte:
                        continue
            
            filtered_leads.append(lead)
        
        # Calcular metricas de conversao (SEM PROPOSTAS)
        total_leads = len(filtered_leads)
        
        # Reunioes: contar leads que tiveram reuniao realizada
        meetings_leads = len([lead for lead in filtered_leads if lead.get("id") in meetings_by_lead])
        
        # Vendas: apenas status de venda + data valida
        valid_sale_status_conversion = [STATUS_VENDA_FINAL, STATUS_CONTRATO_ASSINADO]
        sales_leads = len([
            lead for lead in filtered_leads 
            if validate_sale_in_period(lead, start_time, end_time, CUSTOM_FIELD_DATA_FECHAMENTO, valid_sale_status_conversion)
        ])
        
        # Calcular taxas de conversao (SEM PROPOSTAS)
        meetings_rate = (meetings_leads / total_leads * 100) if total_leads > 0 else 0
        sales_rate = (sales_leads / meetings_leads * 100) if meetings_leads > 0 else 0
        
        # Dados do funil (SEM PROPOSTAS)
        funnel_data = [
            {"stage": "Leads", "value": total_leads, "rate": 100},
            {"stage": "Reunioes", "value": meetings_leads, "rate": round(meetings_rate, 1)},
            {"stage": "Vendas", "value": sales_leads, "rate": round(sales_rate, 1)}
        ]
        
        return {
            "conversionRates": {
                "meetings": round(meetings_rate, 1),
                "sales": round(sales_rate, 1)
            },
            "funnelData": funnel_data,
            "_metadata": {
                "period_days": days,
                "corretor_filter": corretor,
                "fonte_filter": fonte,
                "generated_at": datetime.now().isoformat(),
                "total_leads_analyzed": total_leads,
                "optimized": True,
                "endpoint_version": "v2_without_proposals",
                "sales_validation": "data_fechamento_required",
                "status_ids_used": {
                    "vendas": [STATUS_VENDA_FINAL, STATUS_CONTRATO_ASSINADO]
                },
                "breakdown": {
                    "total_leads": total_leads,
                    "meetings_leads": meetings_leads,
                    "sales_leads": sales_leads,
                    "total_meetings_found": sum(meetings_by_lead.values())
                }
            }
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro ao gerar taxas de conversao: {str(e)}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")

@router.get("/sales/pipeline-status")
async def get_pipeline_status(
    days: int = Query(30, description="Periodo em dias para analise"),
    corretor: Optional[str] = Query(None, description="Nome do corretor para filtrar dados"),
    fonte: Optional[str] = Query(None, description="Fonte para filtrar dados"),
    start_date: Optional[str] = Query(None, description="Data de inicio (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="Data de fim (YYYY-MM-DD)")
):
    """
    Retorna status do pipeline otimizado para dashboard (SEM PROPOSTAS).
    """
    try:
        logger.info(f"Buscando status do pipeline para {days} dias, corretor: {corretor}, fonte: {fonte}")
        
        from app.services.kommo_api import KommoAPI
        kommo_api = KommoAPI()
        
        # Calcular parametros de tempo
        import time
        
        if start_date and end_date:
            try:
                start_dt = datetime.strptime(start_date, '%Y-%m-%d')
                end_dt = datetime.strptime(end_date, '%Y-%m-%d')
                end_dt = end_dt.replace(hour=23, minute=59, second=59)
                start_time = int(start_dt.timestamp())
                end_time = int(end_dt.timestamp())
            except ValueError:
                raise HTTPException(status_code=400, detail="Formato de data invalido. Use YYYY-MM-DD")
        else:
            end_time = int(time.time())
            start_time = end_time - (days * 24 * 60 * 60)
        
        # IDs importantes
        PIPELINE_VENDAS = 10516987
        PIPELINE_REMARKETING = 11059911
        STATUS_CONTRATO_ASSINADO = 80689759
        STATUS_VENDA_FINAL = 142
        
        # Buscar leads de AMBOS os pipelines
        leads_vendas_params = {
            "filter[pipeline_id]": PIPELINE_VENDAS,
            "filter[created_at][from]": start_time,
            "filter[created_at][to]": end_time,
            "limit": 500,
            "with": "custom_fields_values"
        }
        
        leads_remarketing_params = {
            "filter[pipeline_id]": PIPELINE_REMARKETING,
            "filter[created_at][from]": start_time,
            "filter[created_at][to]": end_time,
            "limit": 500,
            "with": "custom_fields_values"
        }
        
        # Buscar pipelines para mapear status
        try:
            pipelines_data = kommo_api.get_pipelines()
        except Exception as e:
            logger.error(f"Erro ao buscar pipelines: {e}")
            pipelines_data = {"_embedded": {"pipelines": []}}
        
        # Buscar dados
        try:
            leads_vendas_data = kommo_api.get_leads(leads_vendas_params)
        except Exception as e:
            logger.error(f"Erro ao buscar leads de vendas: {e}")
            leads_vendas_data = {"_embedded": {"leads": []}}
            
        try:
            leads_remarketing_data = kommo_api.get_leads(leads_remarketing_params)
        except Exception as e:
            logger.error(f"Erro ao buscar leads de remarketing: {e}")
            leads_remarketing_data = {"_embedded": {"leads": []}}
        
        # Combinar leads de ambos os pipelines
        all_leads = []
        if leads_vendas_data and "_embedded" in leads_vendas_data:
            vendas_leads = leads_vendas_data["_embedded"].get("leads", [])
            if isinstance(vendas_leads, list):
                all_leads.extend(vendas_leads)
                
        if leads_remarketing_data and "_embedded" in leads_remarketing_data:
            remarketing_leads = leads_remarketing_data["_embedded"].get("leads", [])
            if isinstance(remarketing_leads, list):
                all_leads.extend(remarketing_leads)
        
        # Criar mapa de status
        status_map = {}
        if pipelines_data and "_embedded" in pipelines_data:
            for pipeline in pipelines_data["_embedded"].get("pipelines", []):
                statuses = pipeline.get("_embedded", {}).get("statuses", [])
                for status in statuses:
                    status_id = status.get("id")
                    status_name = status.get("name", f"Status {status_id}")
                    status_map[status_id] = status_name
        
        # Processar leads com filtros
        filtered_leads = []
        for lead in all_leads:
            if not lead or not isinstance(lead, dict):
                continue
            
            # Extrair valores de forma segura
            corretor_lead = get_custom_field_value(lead, 837920)  # Corretor
            fonte_lead = get_custom_field_value(lead, 837886)     # Fonte
            
            # Aplicar filtros
            if corretor and isinstance(corretor, str) and corretor.strip():
                if ',' in corretor:
                    corretores_list = [c.strip() for c in corretor.split(',')]
                    if corretor_lead not in corretores_list:
                        continue
                else:
                    if corretor_lead != corretor:
                        continue
            
            if fonte and isinstance(fonte, str) and fonte.strip():
                if ',' in fonte:
                    fontes_list = [f.strip() for f in fonte.split(',')]
                    if fonte_lead not in fontes_list:
                        continue
                else:
                    if fonte_lead != fonte:
                        continue
            
            filtered_leads.append(lead)
        
        # Agrupar leads por status
        status_counts = {}
        for lead in filtered_leads:
            status_id = lead.get("status_id")
            
            if status_id not in [142, 143]:  # Nao eh won nem lost
                status_name = status_map.get(status_id, f"Status {status_id}")
                
                # Agrupar status similares (SEM PROPOSTAS)
                if "negociac" in status_name.lower():
                    grouped_status = "Leads em Negociacao"
                elif "remarketing" in status_name.lower() or "reativa" in status_name.lower():
                    grouped_status = "Leads em Remarketing"
                elif "reativad" in status_name.lower():
                    grouped_status = "Leads Reativados"
                elif "reuniao" in status_name.lower() or "agend" in status_name.lower():
                    grouped_status = "Leads com Reuniao"
                elif "contato" in status_name.lower():
                    grouped_status = "Leads em Contato"
                else:
                    grouped_status = status_name
                
                status_counts[grouped_status] = status_counts.get(grouped_status, 0) + 1
        
        # Converter para lista ordenada
        status_list = [
            {"status": status, "count": count}
            for status, count in sorted(status_counts.items(), key=lambda x: x[1], reverse=True)
        ]
        
        total_active_leads = sum(status_counts.values())
        
        return {
            "pipelineStatus": status_list,
            "totalActiveLeads": total_active_leads,
            "_metadata": {
                "period_days": days,
                "corretor_filter": corretor,
                "fonte_filter": fonte,
                "generated_at": datetime.now().isoformat(),
                "total_leads_analyzed": len(filtered_leads),
                "active_leads_analyzed": total_active_leads,
                "optimized": True,
                "endpoint_version": "v2_without_proposals",
                "pipelines_analyzed": [PIPELINE_VENDAS, PIPELINE_REMARKETING],
                "status_ids_used": {
                    "vendas": [STATUS_VENDA_FINAL, STATUS_CONTRATO_ASSINADO]
                }
            }
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro ao gerar status do pipeline: {str(e)}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")