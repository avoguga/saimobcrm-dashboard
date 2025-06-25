from fastapi import APIRouter, Query, HTTPException
from typing import Optional
from datetime import datetime, timedelta
import traceback
import logging

# Configurar logger
logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v2", tags=["Sales V2 API"])

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
    Substitui parte do endpoint pesado /dashboard/sales-complete.
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
            except ValueError:
                raise HTTPException(status_code=400, detail="Formato de data inválido. Use YYYY-MM-DD")
        else:
            end_time = int(time.time())
            start_time = end_time - (days * 24 * 60 * 60)
        
        # Período anterior para comparação
        period_duration = end_time - start_time
        previous_start_time = start_time - period_duration
        previous_end_time = start_time
        
        # IDs importantes
        PIPELINE_VENDAS = 10516987
        STATUS_PROPOSTA = 80689735
        STATUS_CONTRATO_ASSINADO = 80689759
        STATUS_VENDA_FINAL = 142
        CUSTOM_FIELD_DATA_FECHAMENTO = 858126
        
        # Buscar leads do período atual APENAS do Funil de Vendas
        current_leads_params = {
            "filter[pipeline_id]": PIPELINE_VENDAS,  # Filtrar por pipeline
            "filter[updated_at][from]": start_time,   # Mudança: usar updated_at
            "filter[updated_at][to]": end_time,
            "limit": 250,
            "with": "custom_fields_values"
        }
        
        # Buscar leads do período anterior APENAS do Funil de Vendas
        previous_leads_params = {
            "filter[pipeline_id]": PIPELINE_VENDAS,  # Filtrar por pipeline
            "filter[updated_at][from]": previous_start_time,
            "filter[updated_at][to]": previous_end_time,
            "limit": 250,
            "with": "custom_fields_values"
        }
        
        try:
            current_leads_data = kommo_api.get_leads(current_leads_params)
        except Exception as e:
            logger.error(f"Erro ao buscar leads do período atual: {e}")
            current_leads_data = {"_embedded": {"leads": []}}
            
        try:
            previous_leads_data = kommo_api.get_leads(previous_leads_params)
        except Exception as e:
            logger.error(f"Erro ao buscar leads do período anterior: {e}")
            previous_leads_data = {"_embedded": {"leads": []}}
        
        # Função segura para extrair valor de custom fields
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
        
        # Função para filtrar e processar leads
        def process_leads(leads_data):
            if not leads_data or not isinstance(leads_data, dict):
                return []
                
            embedded = leads_data.get("_embedded")
            if not embedded or not isinstance(embedded, dict):
                return []
                
            all_leads = embedded.get("leads")
            if not all_leads or not isinstance(all_leads, list):
                return []
                
            filtered_leads = []
            
            for lead in all_leads:
                if not lead or not isinstance(lead, dict):
                    continue
                
                # Extrair valores de forma segura
                corretor_lead = get_custom_field_value(lead, 837920)  # Corretor
                fonte_lead = get_custom_field_value(lead, 837886)     # Fonte
                
                # Aplicar filtros
                if corretor and corretor_lead != corretor:
                    continue
                if fonte and fonte_lead != fonte:
                    continue
                
                filtered_leads.append(lead)
            
            return filtered_leads
        
        # Processar leads atuais e anteriores
        current_leads = process_leads(current_leads_data)
        previous_leads = process_leads(previous_leads_data)
        
        # Função para verificar se venda tem data válida
        def has_valid_sale_date(lead):
            """Verifica se a venda tem Data Fechamento ou closed_at válido"""
            # Priorizar Data Fechamento (custom field)
            data_fechamento = get_custom_field_value(lead, CUSTOM_FIELD_DATA_FECHAMENTO)
            if data_fechamento:
                return True
            # Fallback para closed_at
            closed_at = lead.get("closed_at")
            if closed_at:
                return True
            # Se não tiver nenhuma data válida, NÃO é venda válida
            return False
        
        # Calcular métricas do período atual
        total_leads = len(current_leads)
        active_leads = len([lead for lead in current_leads if lead.get("status_id") not in [142, 143]])
        
        # Vendas: apenas status de venda + data válida
        won_leads_with_date = [
            lead for lead in current_leads 
            if lead.get("status_id") in [STATUS_VENDA_FINAL, STATUS_CONTRATO_ASSINADO] and has_valid_sale_date(lead)
        ]
        won_leads = len(won_leads_with_date)
        
        lost_leads = len([lead for lead in current_leads if lead.get("status_id") == 143])
        
        # Propostas: apenas status de proposta
        proposal_leads = len([lead for lead in current_leads if lead.get("status_id") == STATUS_PROPOSTA])
        
        # Calcular revenue e average deal size (apenas vendas com data válida)
        total_revenue = sum((lead.get("price") or 0) for lead in won_leads_with_date)
        average_deal_size = (total_revenue / won_leads) if won_leads > 0 else 0
        
        # Calcular win rate
        total_closed = won_leads + lost_leads
        win_rate = (won_leads / total_closed * 100) if total_closed > 0 else 0
        
        # Calcular métricas do período anterior
        previous_total_leads = len(previous_leads)
        previous_active_leads = len([lead for lead in previous_leads if lead.get("status_id") not in [142, 143]])
        
        # Vendas anteriores: apenas status de venda + data válida
        previous_won_leads_with_date = [
            lead for lead in previous_leads 
            if lead.get("status_id") in [STATUS_VENDA_FINAL, STATUS_CONTRATO_ASSINADO] and has_valid_sale_date(lead)
        ]
        previous_won_leads = len(previous_won_leads_with_date)
        
        previous_lost_leads = len([lead for lead in previous_leads if lead.get("status_id") == 143])
        previous_proposal_leads = len([lead for lead in previous_leads if lead.get("status_id") == STATUS_PROPOSTA])
        
        previous_total_revenue = sum((lead.get("price") or 0) for lead in previous_won_leads_with_date)
        previous_average_deal_size = (previous_total_revenue / previous_won_leads) if previous_won_leads > 0 else 0
        
        previous_total_closed = previous_won_leads + previous_lost_leads
        previous_win_rate = (previous_won_leads / previous_total_closed * 100) if previous_total_closed > 0 else 0
        
        return {
            "totalLeads": total_leads,
            "activeLeads": active_leads,
            "wonLeads": won_leads,
            "lostLeads": lost_leads,
            "winRate": round(win_rate, 1),
            "averageDealSize": round(average_deal_size, 2),
            "totalRevenue": round(total_revenue, 2),
            "previousTotalLeads": previous_total_leads,
            "previousActiveLeads": previous_active_leads,
            "previousWonLeads": previous_won_leads,
            "previousWinRate": round(previous_win_rate, 1),
            "previousAverageDealSize": round(previous_average_deal_size, 2),
            # NOVO: Campo que o frontend usa
            "proposalStats": {
                "total": proposal_leads,
                "previous": previous_proposal_leads,
                "growth": ((proposal_leads - previous_proposal_leads) / previous_proposal_leads * 100) if previous_proposal_leads > 0 else 0
            },
            "_metadata": {
                "period_days": days,
                "corretor_filter": corretor,
                "fonte_filter": fonte,
                "generated_at": datetime.now().isoformat(),
                "optimized": True,
                "endpoint_version": "v2",
                "pipeline_filter": PIPELINE_VENDAS,
                "status_ids_used": {
                    "proposta": STATUS_PROPOSTA,
                    "vendas": [STATUS_VENDA_FINAL, STATUS_CONTRATO_ASSINADO]
                },
                "data_fechamento_field": CUSTOM_FIELD_DATA_FECHAMENTO
            }
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro ao gerar KPIs de vendas: {str(e)}")
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
    Retorna dados otimizados para gráfico de leads por usuário.
    """
    try:
        logger.info(f"Buscando dados de leads por usuário para {days} dias, corretor: {corretor}, fonte: {fonte}")
        
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
        
        # IDs importantes
        PIPELINE_VENDAS = 10516987
        STATUS_PROPOSTA = 80689735
        STATUS_CONTRATO_ASSINADO = 80689759
        STATUS_VENDA_FINAL = 142
        CUSTOM_FIELD_DATA_FECHAMENTO = 858126
        
        # Buscar leads APENAS do Funil de Vendas
        leads_params = {
            "filter[pipeline_id]": PIPELINE_VENDAS,  # Filtrar por pipeline
            "filter[updated_at][from]": start_time,   # Mudança: usar updated_at
            "filter[updated_at][to]": end_time,
            "limit": 250,
            "with": "custom_fields_values"
        }
        
        # Buscar tarefas de reunião concluídas
        tasks_params = {
            'filter[task_type]': 2,  # Tipo reunião
            'filter[is_completed]': 1,  # Apenas concluídas
            'filter[updated_at][from]': start_time,
            'filter[updated_at][to]': end_time,
            'limit': 250
        }
        
        try:
            leads_data = kommo_api.get_leads(leads_params)
        except Exception as e:
            logger.error(f"Erro ao buscar leads: {e}")
            leads_data = {"_embedded": {"leads": []}}
            
        try:
            tasks_data = kommo_api.get_tasks(tasks_params)
        except Exception as e:
            logger.error(f"Erro ao buscar tarefas: {e}")
            tasks_data = {"_embedded": {"tasks": []}}
            
        try:
            users_data = kommo_api.get_users()
        except Exception as e:
            logger.error(f"Erro ao buscar usuários: {e}")
            users_data = {"_embedded": {"users": []}}
        
        # Criar mapa de usuários
        users_map = {}
        if users_data and "_embedded" in users_data:
            users_list = users_data["_embedded"].get("users", [])
            if isinstance(users_list, list):
                for user in users_list:
                    if user and isinstance(user, dict):
                        users_map[user.get("id")] = user.get("name", "Desconhecido")
        
        # Criar mapa de reuniões realizadas por lead
        meetings_by_lead = {}
        if tasks_data and "_embedded" in tasks_data:
            tasks_list = tasks_data["_embedded"].get("tasks", [])
            if isinstance(tasks_list, list):
                for task in tasks_list:
                    if (task and isinstance(task, dict) and 
                        task.get('entity_type') == 'leads'):
                        lead_id = task.get('entity_id')
                        if lead_id:
                            meetings_by_lead[lead_id] = meetings_by_lead.get(lead_id, 0) + 1
        
        # Função segura para extrair valor de custom fields
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
        
        # Processar leads
        leads_by_user = {}
        
        if leads_data and "_embedded" in leads_data:
            leads_list = leads_data["_embedded"].get("leads", [])
            if isinstance(leads_list, list):
                for lead in leads_list:
                    if not lead or not isinstance(lead, dict):
                        continue
                    
                    # Extrair valores de forma segura
                    corretor_lead = get_custom_field_value(lead, 837920)  # Corretor
                    fonte_lead = get_custom_field_value(lead, 837886)     # Fonte
                    
                    # Aplicar filtros
                    if corretor and corretor_lead != corretor:
                        continue
                    if fonte and fonte_lead != fonte:
                        continue
                    
                    # Determinar corretor final - apenas custom field
                    final_corretor = corretor_lead or "N/A"
                    
                    # Pular se não tiver corretor definido
                    if final_corretor == "N/A":
                        continue
                    
                    # Inicializar contador se não existir
                    if final_corretor not in leads_by_user:
                        leads_by_user[final_corretor] = {
                            "name": final_corretor,
                            "value": 0,
                            "active": 0,
                            "meetingsHeld": 0,  # Campo que o frontend usa
                            "sales": 0,
                            "lost": 0
                        }
                    
                    # Incrementar contadores
                    leads_by_user[final_corretor]["value"] += 1
                    
                    # Verificar se tem Data Fechamento para vendas
                    def has_valid_sale_date(lead):
                        data_fechamento = get_custom_field_value(lead, CUSTOM_FIELD_DATA_FECHAMENTO)
                        if data_fechamento:
                            return True
                        closed_at = lead.get("closed_at")
                        if closed_at:
                            return True
                        return False
                    
                    status_id = lead.get("status_id")
                    lead_id = lead.get("id")
                    
                    # Vendas: apenas com data válida
                    if (status_id in [STATUS_VENDA_FINAL, STATUS_CONTRATO_ASSINADO] and 
                        has_valid_sale_date(lead)):
                        leads_by_user[final_corretor]["sales"] += 1
                    elif status_id == 143:  # Lost
                        leads_by_user[final_corretor]["lost"] += 1
                    else:  # Active
                        leads_by_user[final_corretor]["active"] += 1
                    
                    # Reuniões realizadas: do mapa de tarefas
                    if lead_id in meetings_by_lead:
                        leads_by_user[final_corretor]["meetingsHeld"] += meetings_by_lead[lead_id]
        
        # Converter para lista e ordenar
        leads_by_user_list = list(leads_by_user.values())
        leads_by_user_list.sort(key=lambda x: x["value"], reverse=True)
        
        return {
            "leadsByUser": leads_by_user_list,
            "_metadata": {
                "period_days": days,
                "corretor_filter": corretor,
                "fonte_filter": fonte,
                "generated_at": datetime.now().isoformat(),
                "total_users": len(leads_by_user_list),
                "optimized": True,
                "endpoint_version": "v2",
                "pipeline_filter": PIPELINE_VENDAS,
                "meetings_source": "tasks_completed",
                "sales_validation": "data_fechamento_required",
                "status_ids_used": {
                    "vendas": [STATUS_VENDA_FINAL, STATUS_CONTRATO_ASSINADO]
                },
                "total_meetings_found": sum(meetings_by_lead.values()),
                "total_leads_processed": sum(user["value"] for user in leads_by_user_list)
            }
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro ao gerar dados de leads por usuário: {str(e)}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")


@router.get("/sales/conversion-rates")
async def get_conversion_rates(
    days: int = Query(30, description="Período em dias para análise"),
    corretor: Optional[str] = Query(None, description="Nome do corretor para filtrar dados"),
    fonte: Optional[str] = Query(None, description="Fonte para filtrar dados"),
    start_date: Optional[str] = Query(None, description="Data de início (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="Data de fim (YYYY-MM-DD)")
):
    """
    Retorna taxas de conversão otimizadas com dados de funil.
    """
    try:
        logger.info(f"Buscando taxas de conversão para {days} dias, corretor: {corretor}, fonte: {fonte}")
        
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
        
        # IDs importantes
        PIPELINE_VENDAS = 10516987
        STATUS_PROPOSTA = 80689735
        STATUS_CONTRATO_ASSINADO = 80689759
        STATUS_VENDA_FINAL = 142
        CUSTOM_FIELD_DATA_FECHAMENTO = 858126
        
        # Buscar leads APENAS do Funil de Vendas
        leads_params = {
            "filter[pipeline_id]": PIPELINE_VENDAS,  # Filtrar por pipeline
            "filter[updated_at][from]": start_time,   # Mudança: usar updated_at
            "filter[updated_at][to]": end_time,
            "limit": 250,
            "with": "custom_fields_values"
        }
        
        # Buscar tarefas de reunião concluídas
        tasks_params = {
            'filter[task_type]': 2,  # Tipo reunião
            'filter[is_completed]': 1,  # Apenas concluídas
            'filter[updated_at][from]': start_time,
            'filter[updated_at][to]': end_time,
            'limit': 250
        }
        
        try:
            leads_data = kommo_api.get_leads(leads_params)
        except Exception as e:
            logger.error(f"Erro ao buscar leads: {e}")
            leads_data = {"_embedded": {"leads": []}}
            
        try:
            tasks_data = kommo_api.get_tasks(tasks_params)
        except Exception as e:
            logger.error(f"Erro ao buscar tarefas: {e}")
            tasks_data = {"_embedded": {"tasks": []}}
        
        # Função segura para extrair valor de custom fields
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
        
        # Criar mapa de reuniões realizadas por lead
        meetings_by_lead = {}
        if tasks_data and "_embedded" in tasks_data:
            tasks_list = tasks_data["_embedded"].get("tasks", [])
            if isinstance(tasks_list, list):
                for task in tasks_list:
                    if (task and isinstance(task, dict) and 
                        task.get('entity_type') == 'leads'):
                        lead_id = task.get('entity_id')
                        if lead_id:
                            meetings_by_lead[lead_id] = meetings_by_lead.get(lead_id, 0) + 1
        
        # Processar leads com filtros
        filtered_leads = []
        
        if leads_data and "_embedded" in leads_data:
            leads_list = leads_data["_embedded"].get("leads", [])
            if isinstance(leads_list, list):
                for lead in leads_list:
                    if not lead or not isinstance(lead, dict):
                        continue
                    
                    # Extrair valores de forma segura
                    corretor_lead = get_custom_field_value(lead, 837920)  # Corretor
                    fonte_lead = get_custom_field_value(lead, 837886)     # Fonte
                    
                    # Aplicar filtros
                    if corretor and corretor_lead != corretor:
                        continue
                    if fonte and fonte_lead != fonte:
                        continue
                    
                    # Pular leads sem corretor (se não estiver filtrando por corretor específico)
                    if not corretor and not corretor_lead:
                        continue
                    
                    filtered_leads.append(lead)
        
        # Função para verificar se venda tem data válida
        def has_valid_sale_date(lead):
            """Verifica se a venda tem Data Fechamento ou closed_at válido"""
            data_fechamento = get_custom_field_value(lead, CUSTOM_FIELD_DATA_FECHAMENTO)
            if data_fechamento:
                return True
            closed_at = lead.get("closed_at")
            if closed_at:
                return True
            return False
        
        # Calcular métricas de conversão com nova lógica
        total_leads = len(filtered_leads)
        
        # Reuniões: contar leads que tiveram reunião realizada (do mapa de tarefas)
        meetings_leads = len([lead for lead in filtered_leads if lead.get("id") in meetings_by_lead])
        
        # Propostas: apenas status de proposta
        proposals_leads = len([lead for lead in filtered_leads if lead.get("status_id") == STATUS_PROPOSTA])
        
        # Vendas: apenas status de venda + data válida
        sales_leads = len([
            lead for lead in filtered_leads 
            if (lead.get("status_id") in [STATUS_VENDA_FINAL, STATUS_CONTRATO_ASSINADO] and 
                has_valid_sale_date(lead))
        ])
        
        # Calcular taxas de conversão
        meetings_rate = (meetings_leads / total_leads * 100) if total_leads > 0 else 0
        proposals_rate = (proposals_leads / meetings_leads * 100) if meetings_leads > 0 else 0
        sales_rate = (sales_leads / proposals_leads * 100) if proposals_leads > 0 else 0
        
        # Dados do funil
        funnel_data = [
            {"stage": "Leads", "value": total_leads, "rate": 100},
            {"stage": "Reuniões", "value": meetings_leads, "rate": round(meetings_rate, 1)},
            {"stage": "Propostas", "value": proposals_leads, "rate": round(proposals_rate, 1)},
            {"stage": "Vendas", "value": sales_leads, "rate": round(sales_rate, 1)}
        ]
        
        return {
            "conversionRates": {
                "meetings": round(meetings_rate, 1),
                "prospects": round(proposals_rate, 1),
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
                "endpoint_version": "v2",
                "pipeline_filter": PIPELINE_VENDAS,
                "meetings_source": "tasks_completed",
                "sales_validation": "data_fechamento_required",
                "status_ids_used": {
                    "proposta": STATUS_PROPOSTA,
                    "vendas": [STATUS_VENDA_FINAL, STATUS_CONTRATO_ASSINADO]
                },
                "breakdown": {
                    "total_leads": total_leads,
                    "meetings_leads": meetings_leads,
                    "proposals_leads": proposals_leads,
                    "sales_leads": sales_leads,
                    "total_meetings_found": sum(meetings_by_lead.values())
                }
            }
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro ao gerar taxas de conversão: {str(e)}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")


@router.get("/sales/pipeline-status")
async def get_pipeline_status(
    days: int = Query(30, description="Período em dias para análise"),
    corretor: Optional[str] = Query(None, description="Nome do corretor para filtrar dados"),
    fonte: Optional[str] = Query(None, description="Fonte para filtrar dados"),
    start_date: Optional[str] = Query(None, description="Data de início (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="Data de fim (YYYY-MM-DD)")
):
    """
    Retorna status do pipeline otimizado para dashboard.
    """
    try:
        logger.info(f"Buscando status do pipeline para {days} dias, corretor: {corretor}, fonte: {fonte}")
        
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
        
        # IDs importantes
        PIPELINE_VENDAS = 10516987
        STATUS_PROPOSTA = 80689735
        STATUS_CONTRATO_ASSINADO = 80689759
        STATUS_VENDA_FINAL = 142
        CUSTOM_FIELD_DATA_FECHAMENTO = 858126
        
        # Buscar leads APENAS do Funil de Vendas
        leads_params = {
            "filter[pipeline_id]": PIPELINE_VENDAS,  # Filtrar por pipeline
            "filter[updated_at][from]": start_time,   # Mudança: usar updated_at
            "filter[updated_at][to]": end_time,
            "limit": 250,
            "with": "custom_fields_values"
        }
        
        try:
            leads_data = kommo_api.get_leads(leads_params)
        except Exception as e:
            logger.error(f"Erro ao buscar leads: {e}")
            leads_data = {"_embedded": {"leads": []}}
            
        try:
            pipelines_data = kommo_api.get_pipelines()
        except Exception as e:
            logger.error(f"Erro ao buscar pipelines: {e}")
            pipelines_data = {"_embedded": {"pipelines": []}}
        
        # Mapear status IDs para nomes (foco no Funil de Vendas)
        status_map = {}
        if pipelines_data and "_embedded" in pipelines_data:
            pipelines_list = pipelines_data["_embedded"].get("pipelines", [])
            if isinstance(pipelines_list, list):
                for pipeline in pipelines_list:
                    if (pipeline and isinstance(pipeline, dict) and 
                        pipeline.get("id") == PIPELINE_VENDAS):  # Apenas Funil de Vendas
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
                        break  # Encontrou o pipeline, pode parar
        
        # Função segura para extrair valor de custom fields
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
        
        # Função para verificar se venda tem data válida
        def has_valid_sale_date(lead):
            """Verifica se a venda tem Data Fechamento ou closed_at válido"""
            data_fechamento = get_custom_field_value(lead, CUSTOM_FIELD_DATA_FECHAMENTO)
            if data_fechamento:
                return True
            closed_at = lead.get("closed_at")
            if closed_at:
                return True
            return False
        
        # Processar leads com filtros
        pipeline_status = {}
        total_in_pipeline = 0
        
        if leads_data and "_embedded" in leads_data:
            leads_list = leads_data["_embedded"].get("leads", [])
            if isinstance(leads_list, list):
                for lead in leads_list:
                    if not lead or not isinstance(lead, dict):
                        continue
                    
                    # Extrair valores de forma segura
                    corretor_lead = get_custom_field_value(lead, 837920)  # Corretor
                    fonte_lead = get_custom_field_value(lead, 837886)     # Fonte
                    
                    # Aplicar filtros
                    if corretor and corretor_lead != corretor:
                        continue
                    if fonte and fonte_lead != fonte:
                        continue
                    
                    # Pular leads sem corretor (se não estiver filtrando por corretor específico)
                    if not corretor and not corretor_lead:
                        continue
                    
                    status_id = lead.get("status_id")
                    status_name = status_map.get(status_id, f"Status {status_id}")
                    
                    # Agrupar usando status específicos do Funil de Vendas
                    if status_id == STATUS_PROPOSTA:
                        grouped_status = "Proposta"
                    elif status_id in [STATUS_CONTRATO_ASSINADO, STATUS_VENDA_FINAL]:
                        # Verificar se venda tem data válida
                        if has_valid_sale_date(lead):
                            grouped_status = "Venda Concluída"
                        else:
                            grouped_status = "Venda sem Data"
                    elif status_id == 143:  # Closed - lost
                        grouped_status = "Lead Perdido"
                    elif "agend" in status_name.lower():
                        grouped_status = "Agendamento"
                    elif "reunião" in status_name.lower():
                        grouped_status = "Reunião Realizada"
                    elif "atendimento" in status_name.lower():
                        grouped_status = "Atendimento"
                    elif "contato" in status_name.lower():
                        grouped_status = "Contato Feito"
                    elif "follow" in status_name.lower():
                        grouped_status = "Follow-up"
                    elif "novo" in status_name.lower():
                        grouped_status = "Lead Novo"
                    elif "acompanhamento" in status_name.lower():
                        grouped_status = "Acompanhamento"
                    else:
                        grouped_status = status_name
                    
                    pipeline_status[grouped_status] = pipeline_status.get(grouped_status, 0) + 1
                    total_in_pipeline += 1
        
        # Converter para formato de resposta
        pipeline_status_list = [
            {"name": name, "value": count}
            for name, count in sorted(pipeline_status.items(), key=lambda x: x[1], reverse=True)
        ]
        
        return {
            "pipelineStatus": pipeline_status_list,
            "totalInPipeline": total_in_pipeline,
            "_metadata": {
                "period_days": days,
                "corretor_filter": corretor,
                "fonte_filter": fonte,
                "generated_at": datetime.now().isoformat(),
                "status_groups": len(pipeline_status_list),
                "optimized": True,
                "endpoint_version": "v2",
                "pipeline_filter": PIPELINE_VENDAS,
                "sales_validation": "data_fechamento_required",
                "status_ids_used": {
                    "proposta": STATUS_PROPOSTA,
                    "vendas": [STATUS_VENDA_FINAL, STATUS_CONTRATO_ASSINADO],
                    "perdido": 143
                },
                "breakdown": {
                    "leads_processed": total_in_pipeline,
                    "unique_statuses": len(status_map),
                    "grouped_categories": len(pipeline_status_list)
                }
            }
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro ao gerar status do pipeline: {str(e)}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")