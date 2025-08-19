from fastapi import APIRouter, Query, HTTPException
from typing import Optional
from datetime import datetime, timedelta
import traceback
import logging

# Configurar logger
logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v2", tags=["Sales V2 API"])

# Função helper para dados seguros (reutilizada do dashboard.py)
def safe_get_data(func, *args, **kwargs):
    try:
        return func(*args, **kwargs)
    except Exception as e:
        logger.error(f"Erro ao buscar dados de {func.__name__ if hasattr(func, '__name__') else 'unknown'}: {e}")
        return {}

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
        
        # Buscar leads do período atual
        current_leads_params = {
            "filter[created_at][from]": start_time,
            "filter[created_at][to]": end_time,
            "limit": 250,
            "with": "custom_fields_values"
        }
        
        # Buscar leads do período anterior
        previous_leads_params = {
            "filter[created_at][from]": previous_start_time,
            "filter[created_at][to]": previous_end_time,
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
        
        # Função para filtrar e processar leads
        def process_leads(leads_data):
            if not leads_data or "_embedded" not in leads_data:
                return []
            
            all_leads = leads_data["_embedded"].get("leads", [])
            filtered_leads = []
            
            for lead in all_leads:
                if not lead:
                    continue
                
                # Extrair custom fields
                custom_fields = lead.get("custom_fields_values", [])
                corretor_lead = None
                fonte_lead = None
                
                for field in custom_fields:
                    if not field:
                        continue
                    field_id = field.get("field_id")
                    values = field.get("values", [])
                    
                    if field_id == 837920 and values:  # Corretor
                        corretor_lead = values[0].get("value")
                    elif field_id == 837886 and values:  # Fonte
                        fonte_lead = values[0].get("value")
                
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
        
        # Calcular métricas do período atual
        total_leads = len(current_leads)
        active_leads = len([lead for lead in current_leads if lead.get("status_id") not in [142, 143]])
        won_leads = len([lead for lead in current_leads if lead.get("status_id") == 142])
        lost_leads = len([lead for lead in current_leads if lead.get("status_id") == 143])
        
        # Calcular revenue e average deal size
        total_revenue = sum(lead.get("price", 0) or 0 for lead in current_leads if lead.get("status_id") == 142)
        average_deal_size = (total_revenue / won_leads) if won_leads > 0 else 0
        
        # Calcular win rate
        total_closed = won_leads + lost_leads
        win_rate = (won_leads / total_closed * 100) if total_closed > 0 else 0
        
        # Calcular métricas do período anterior
        previous_total_leads = len(previous_leads)
        previous_active_leads = len([lead for lead in previous_leads if lead.get("status_id") not in [142, 143]])
        previous_won_leads = len([lead for lead in previous_leads if lead.get("status_id") == 142])
        previous_lost_leads = len([lead for lead in previous_leads if lead.get("status_id") == 143])
        
        previous_total_revenue = sum(lead.get("price", 0) or 0 for lead in previous_leads if lead.get("status_id") == 142)
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
            "_metadata": {
                "period_days": days,
                "corretor_filter": corretor,
                "fonte_filter": fonte,
                "generated_at": datetime.now().isoformat(),
                "optimized": True,
                "endpoint_version": "v2"
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
        
        # Buscar leads
        leads_params = {
            "filter[created_at][from]": start_time,
            "filter[created_at][to]": end_time,
            "limit": 250,
            "with": "custom_fields_values"
        }
        
        try:
            leads_data = kommo_api.get_leads(leads_params)
        except Exception as e:
            logger.error(f"Erro ao buscar leads: {e}")
            leads_data = {"_embedded": {"leads": []}}
            
        try:
            users_data = kommo_api.get_users()
        except Exception as e:
            logger.error(f"Erro ao buscar usuários: {e}")
            users_data = {"_embedded": {"users": []}}
        
        # Criar mapa de usuários
        users_map = {}
        if users_data and "_embedded" in users_data:
            for user in users_data["_embedded"].get("users", []):
                users_map[user["id"]] = user["name"]
        
        # Processar leads
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
                
                for field in custom_fields:
                    if not field:
                        continue
                    field_id = field.get("field_id")
                    values = field.get("values", [])
                    
                    if field_id == 837920 and values:  # Corretor
                        corretor_lead = values[0].get("value")
                    elif field_id == 837886 and values:  # Fonte
                        fonte_lead = values[0].get("value")
                
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
                        "value": 0,
                        "active": 0,
                        "meetings": 0,
                        "sales": 0,
                        "lost": 0
                    }
                
                # Incrementar contadores
                leads_by_user[final_corretor]["value"] += 1
                
                status_id = lead.get("status_id")
                if status_id == 142:  # Won
                    leads_by_user[final_corretor]["sales"] += 1
                elif status_id == 143:  # Lost
                    leads_by_user[final_corretor]["lost"] += 1
                elif status_id in [80689731, 80689727]:  # Reunião/Agendamento
                    leads_by_user[final_corretor]["meetings"] += 1
                    leads_by_user[final_corretor]["active"] += 1
                else:  # Active
                    leads_by_user[final_corretor]["active"] += 1
        
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
                "endpoint_version": "v2"
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
        
        # Buscar leads
        leads_params = {
            "filter[created_at][from]": start_time,
            "filter[created_at][to]": end_time,
            "limit": 250,
            "with": "custom_fields_values"
        }
        
        try:
            leads_data = kommo_api.get_leads(leads_params)
        except Exception as e:
            logger.error(f"Erro ao buscar leads: {e}")
            leads_data = {"_embedded": {"leads": []}}
        
        # Processar leads com filtros
        filtered_leads = []
        
        if leads_data and "_embedded" in leads_data:
            all_leads = leads_data["_embedded"].get("leads", [])
            
            for lead in all_leads:
                if not lead:
                    continue
                
                # Extrair custom fields
                custom_fields = lead.get("custom_fields_values", [])
                corretor_lead = None
                fonte_lead = None
                
                for field in custom_fields:
                    if not field:
                        continue
                    field_id = field.get("field_id")
                    values = field.get("values", [])
                    
                    if field_id == 837920 and values:  # Corretor
                        corretor_lead = values[0].get("value")
                    elif field_id == 837886 and values:  # Fonte
                        fonte_lead = values[0].get("value")
                
                # Aplicar filtros
                if corretor and corretor_lead != corretor:
                    continue
                if fonte and fonte_lead != fonte:
                    continue
                
                filtered_leads.append(lead)
        
        # Calcular métricas de conversão
        total_leads = len(filtered_leads)
        meetings_leads = len([lead for lead in filtered_leads if lead.get("status_id") in [80689731, 80689727, 80645875]])
        sales_leads = len([lead for lead in filtered_leads if lead.get("status_id") == 142])
        
        # Calcular taxas de conversão
        meetings_rate = (meetings_leads / total_leads * 100) if total_leads > 0 else 0
        sales_rate = (sales_leads / meetings_leads * 100) if meetings_leads > 0 else 0
        
        # Dados do funil
        funnel_data = [
            {"stage": "Leads", "value": total_leads, "rate": 100},
            {"stage": "Reuniões", "value": meetings_leads, "rate": round(meetings_rate, 1)},
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
                "endpoint_version": "v2"
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
        
        # Buscar leads e pipeline data
        leads_params = {
            "filter[created_at][from]": start_time,
            "filter[created_at][to]": end_time,
            "limit": 250,
            "with": "custom_fields_values"
        }
        
        try:
            leads_data = kommo_api.get_leads(leads_params)
        except Exception as e:
            logger.error(f"Erro ao buscar leads: {e}")
            leads_data = {"_embedded": {"leads": []}}
        pipelines_data = safe_get_data(kommo_api.get_pipelines)
        
        # Mapear status IDs para nomes
        status_map = {}
        if pipelines_data and "_embedded" in pipelines_data:
            for pipeline in pipelines_data["_embedded"].get("pipelines", []):
                if pipeline.get("_embedded", {}).get("statuses"):
                    for status in pipeline["_embedded"]["statuses"]:
                        status_id = status.get("id")
                        status_name = status.get("name", f"Status {status_id}")
                        status_map[status_id] = status_name
        
        # Processar leads com filtros
        pipeline_status = {}
        total_in_pipeline = 0
        
        if leads_data and "_embedded" in leads_data:
            all_leads = leads_data["_embedded"].get("leads", [])
            
            for lead in all_leads:
                if not lead:
                    continue
                
                # Extrair custom fields
                custom_fields = lead.get("custom_fields_values", [])
                corretor_lead = None
                fonte_lead = None
                
                for field in custom_fields:
                    if not field:
                        continue
                    field_id = field.get("field_id")
                    values = field.get("values", [])
                    
                    if field_id == 837920 and values:  # Corretor
                        corretor_lead = values[0].get("value")
                    elif field_id == 837886 and values:  # Fonte
                        fonte_lead = values[0].get("value")
                
                # Aplicar filtros
                if corretor and corretor_lead != corretor:
                    continue
                if fonte and fonte_lead != fonte:
                    continue
                
                # Contar apenas leads ativos (não won/lost)
                status_id = lead.get("status_id")
                if status_id not in [142, 143]:  # Não é won nem lost
                    status_name = status_map.get(status_id, f"Status {status_id}")
                    
                    # Agrupar status similares
                    if "negociac" in status_name.lower():
                        grouped_status = "Leads em Negociação"
                    elif "remarketing" in status_name.lower() or "reativa" in status_name.lower():
                        grouped_status = "Leads em Remarketing"
                    elif "reativad" in status_name.lower():
                        grouped_status = "Leads Reativados"
                    elif "reunião" in status_name.lower() or "agend" in status_name.lower():
                        grouped_status = "Leads com Reunião"
                    elif "contato" in status_name.lower():
                        grouped_status = "Leads em Contato"
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
                "endpoint_version": "v2"
            }
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro ao gerar status do pipeline: {str(e)}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")