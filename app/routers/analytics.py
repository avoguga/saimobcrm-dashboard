from fastapi import APIRouter, HTTPException, Query
from typing import Dict, List, Optional
from app.services.kommo_api import KommoAPI
import time
from datetime import datetime, timedelta
import traceback

router = APIRouter(prefix="/analytics", tags=["Analytics"])
api = KommoAPI()

@router.get("/lead-cycle-time")
async def get_lead_cycle_time(
    days: int = Query(90, description="Período em dias para analisar")
):
    """Calcula o tempo médio para converter leads em vendas"""
    try:
        # Calcular timestamp para o período solicitado
        end_time = int(time.time())
        start_time = end_time - (days * 24 * 60 * 60)
        
        # Obter leads fechados com sucesso
        params = {
            "filter[closed_at][from]": start_time,
            "filter[closed_at][to]": end_time,
            "limit": 250
        }
        
        data = api.get_leads(params)
        
        # Verificar se obtivemos uma resposta válida
        if not data or not data.get("_embedded"):
            return {"lead_cycle_time": 0, "count": 0, "message": "Não foi possível obter leads fechados"}
            
        leads = data.get("_embedded", {}).get("leads", [])
        
        if not leads:
            return {"lead_cycle_time": 0, "count": 0, "message": "Nenhum lead fechado no período"}
        
        # Calcular tempos de ciclo
        total_time = 0
        count = 0
        
        for lead in leads:
            created_at = lead.get("created_at")
            closed_at = lead.get("closed_at")
            
            if created_at and closed_at:
                cycle_time = api.calculate_duration_days(created_at, closed_at)
                total_time += cycle_time
                count += 1
        
        avg_cycle_time = total_time / count if count > 0 else 0
        
        return {
            "lead_cycle_time_days": round(avg_cycle_time, 2),
            "count": count
        }
    except Exception as e:
        print(f"Erro ao calcular tempo de ciclo de lead: {e}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/win-rate")
async def get_win_rate(
    days: int = Query(90, description="Período em dias para analisar"),
    pipeline_id: Optional[int] = Query(None, description="ID do pipeline para filtrar")
):
    """Calcula a taxa de conversão de leads em vendas"""
    try:
        # Obter estágios de pipeline para identificar "ganho" vs "perdido"
        pipeline_data = api.get_pipelines()
        
        # Verificar se obtivemos uma resposta válida
        if not pipeline_data or not pipeline_data.get("_embedded"):
            return {"win_rate_percentage": 0, "won_count": 0, "lost_count": 0, "total_closed": 0, 
                    "message": "Não foi possível obter pipelines"}
            
        pipelines = pipeline_data.get("_embedded", {}).get("pipelines", [])
        
        # Se pipeline_id não for fornecido, use o primeiro
        if not pipeline_id and pipelines:
            pipeline_id = pipelines[0].get("id")
        
        if not pipeline_id:
            return {"win_rate_percentage": 0, "won_count": 0, "lost_count": 0, "total_closed": 0, 
                    "message": "Não foi possível determinar um pipeline"}
        
        # Obter estágios do pipeline
        statuses_data = api.get_pipeline_statuses(pipeline_id)
        
        # Verificar se obtivemos uma resposta válida
        if not statuses_data or not statuses_data.get("_embedded"):
            return {"win_rate_percentage": 0, "won_count": 0, "lost_count": 0, "total_closed": 0, 
                    "message": "Não foi possível obter estágios do pipeline"}
            
        statuses = statuses_data.get("_embedded", {}).get("statuses", [])
        
        # Identificar estágios "ganho" e "perdido"
        won_status_ids = []
        lost_status_ids = []
        
        for status in statuses:
            if status.get("type") == "won":
                won_status_ids.append(status.get("id"))
            elif status.get("type") == "lost":
                lost_status_ids.append(status.get("id"))
        
        # Calcular timestamp para o período solicitado
        end_time = int(time.time())
        start_time = end_time - (days * 24 * 60 * 60)
        
        # Contar leads ganhos
        won_count = 0
        for status_id in won_status_ids:
            if status_id is None:
                continue
                
            params = {
                "filter[statuses][0][pipeline_id]": pipeline_id,
                "filter[statuses][0][status_id]": status_id,
                "filter[closed_at][from]": start_time,
                "filter[closed_at][to]": end_time,
                "limit": 250
            }
            data = api.get_leads(params)
            
            # Verificar se obtivemos uma resposta válida
            if data and data.get("_embedded"):
                won_count += len(data.get("_embedded", {}).get("leads", []))
        
        # Contar leads perdidos
        lost_count = 0
        for status_id in lost_status_ids:
            if status_id is None:
                continue
                
            params = {
                "filter[statuses][0][pipeline_id]": pipeline_id,
                "filter[statuses][0][status_id]": status_id,
                "filter[closed_at][from]": start_time,
                "filter[closed_at][to]": end_time,
                "limit": 250
            }
            data = api.get_leads(params)
            
            # Verificar se obtivemos uma resposta válida
            if data and data.get("_embedded"):
                lost_count += len(data.get("_embedded", {}).get("leads", []))
        
        total_closed = won_count + lost_count
        win_rate = (won_count / total_closed) * 100 if total_closed > 0 else 0
        
        return {
            "win_rate_percentage": round(win_rate, 2),
            "won_count": won_count,
            "lost_count": lost_count,
            "total_closed": total_closed
        }
    except Exception as e:
        print(f"Erro ao calcular taxa de conversão: {e}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/average-deal-size")
async def get_average_deal_size(
    days: int = Query(90, description="Período em dias para analisar"),
    pipeline_id: Optional[int] = Query(None, description="ID do pipeline para filtrar")
):
    """Calcula o valor médio das vendas concluídas"""
    try:
        # Obter estágios de pipeline para identificar "ganho"
        pipeline_data = api.get_pipelines()
        
        # Verificar se obtivemos uma resposta válida
        if not pipeline_data or not pipeline_data.get("_embedded"):
            return {"average_deal_size": 0, "total_value": 0, "count": 0, 
                    "message": "Não foi possível obter pipelines"}
            
        pipelines = pipeline_data.get("_embedded", {}).get("pipelines", [])
        
        # Se pipeline_id não for fornecido, use o primeiro
        if not pipeline_id and pipelines:
            pipeline_id = pipelines[0].get("id")
        
        if not pipeline_id:
            return {"average_deal_size": 0, "total_value": 0, "count": 0, 
                    "message": "Não foi possível determinar um pipeline"}
        
        # Obter estágios do pipeline
        statuses_data = api.get_pipeline_statuses(pipeline_id)
        
        # Verificar se obtivemos uma resposta válida
        if not statuses_data or not statuses_data.get("_embedded"):
            return {"average_deal_size": 0, "total_value": 0, "count": 0, 
                    "message": "Não foi possível obter estágios do pipeline"}
            
        statuses = statuses_data.get("_embedded", {}).get("statuses", [])
        
        # Identificar estágios "ganho"
        won_status_ids = []
        
        for status in statuses:
            if status.get("type") == "won":
                won_status_ids.append(status.get("id"))
        
        if not won_status_ids:
            return {"average_deal_size": 0, "count": 0, "message": "Nenhum estágio 'ganho' encontrado"}
        
        # Calcular timestamp para o período solicitado
        end_time = int(time.time())
        start_time = end_time - (days * 24 * 60 * 60)
        
        # Obter leads ganhos
        all_won_leads = []
        for status_id in won_status_ids:
            if status_id is None:
                continue
                
            params = {
                "filter[statuses][0][pipeline_id]": pipeline_id,
                "filter[statuses][0][status_id]": status_id,
                "filter[closed_at][from]": start_time,
                "filter[closed_at][to]": end_time,
                "limit": 250
            }
            data = api.get_leads(params)
            
            # Verificar se obtivemos uma resposta válida
            if data and data.get("_embedded"):
                all_won_leads.extend(data.get("_embedded", {}).get("leads", []))
        
        # Calcular valor médio
        total_value = 0
        count = 0
        
        for lead in all_won_leads:
            price = lead.get("price")
            if price:
                total_value += price
                count += 1
        
        avg_deal_size = total_value / count if count > 0 else 0
        
        return {
            "average_deal_size": round(avg_deal_size, 2),
            "total_value": total_value,
            "count": count
        }
    except Exception as e:
        print(f"Erro ao calcular valor médio das vendas: {e}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/conversion-rates")
async def get_conversion_rates(
    days: int = Query(90, description="Período em dias para analisar"),
    pipeline_id: Optional[int] = Query(None, description="ID do pipeline para filtrar")
):
    """Calcula taxas de conversão em diferentes estágios do funil"""
    try:
        # Obter pipeline e estágios
        pipeline_data = api.get_pipelines()
        
        # Verificar se obtivemos uma resposta válida
        if not pipeline_data or not pipeline_data.get("_embedded"):
            return {"total_leads": 0, "stage_counts": {}, "conversion_rates_percentage": {}, 
                    "message": "Não foi possível obter pipelines"}
            
        pipelines = pipeline_data.get("_embedded", {}).get("pipelines", [])
        
        # Se pipeline_id não for fornecido, use o primeiro
        if not pipeline_id and pipelines:
            pipeline_id = pipelines[0].get("id")
        
        if not pipeline_id:
            return {"total_leads": 0, "stage_counts": {}, "conversion_rates_percentage": {}, 
                    "message": "Não foi possível determinar um pipeline"}
        
        # Obter estágios do pipeline
        statuses_data = api.get_pipeline_statuses(pipeline_id)
        
        # Verificar se obtivemos uma resposta válida
        if not statuses_data or not statuses_data.get("_embedded"):
            return {"total_leads": 0, "stage_counts": {}, "conversion_rates_percentage": {}, 
                    "message": "Não foi possível obter estágios do pipeline"}
            
        statuses = statuses_data.get("_embedded", {}).get("statuses", [])
        
        # Calcular timestamp para o período solicitado
        end_time = int(time.time())
        start_time = end_time - (days * 24 * 60 * 60)
        
        # Contar leads em cada estágio
        stage_counts = {}
        total_leads = 0
        
        for status in statuses:
            status_id = status.get("id")
            if status_id is None:
                continue
                
            status_name = status.get("name", f"Estágio {status_id}")
            
            params = {
                "filter[statuses][0][pipeline_id]": pipeline_id,
                "filter[statuses][0][status_id]": status_id,
                "filter[created_at][from]": start_time,
                "filter[created_at][to]": end_time,
                "limit": 250
            }
            
            data = api.get_leads(params)
            
            # Verificar se obtivemos uma resposta válida
            if data and data.get("_embedded"):
                leads_count = len(data.get("_embedded", {}).get("leads", []))
                stage_counts[status_name] = leads_count
                total_leads += leads_count
        
        # Calcular taxas de conversão
        conversion_rates = {}
        
        if total_leads > 0:
            for status_name, count in stage_counts.items():
                conversion_rates[status_name] = round((count / total_leads) * 100, 2)
        
        return {
            "total_leads": total_leads,
            "stage_counts": stage_counts,
            "conversion_rates_percentage": conversion_rates
        }
    except Exception as e:
        print(f"Erro ao calcular taxas de conversão: {e}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/salesbot-recovery")
async def get_salesbot_recovery(
    recovery_tag: str = Query("Recuperado pelo SalesBot", description="Tag usada para marcar leads recuperados pelo SalesBot"),
    days: int = Query(90, description="Período em dias para analisar")
):
    """Análise de leads recuperados pelo SalesBot"""
    try:
        # Calcular timestamp para o período solicitado
        end_time = int(time.time())
        start_time = end_time - (days * 24 * 60 * 60)
        
        # Obter todos os leads criados no período
        params = {
            "filter[created_at][from]": start_time,
            "filter[created_at][to]": end_time,
            "limit": 250
        }
        
        data = api.get_leads(params)
        
        # Verificar se obtivemos uma resposta válida
        if not data or not data.get("_embedded"):
            return {"recovered_leads_count": 0, "converted_count": 0, "conversion_rate_percentage": 0, 
                    "message": "Não foi possível obter leads"}
            
        leads = data.get("_embedded", {}).get("leads", [])
        
        # Contar leads recuperados pelo SalesBot
        recovered_leads = []
        
        for lead in leads:
            lead_embedded = lead.get("_embedded", {})
            if not lead_embedded:
                continue
                
            tags = lead_embedded.get("tags", [])
            
            for tag in tags:
                if tag.get("name", "").lower() == recovery_tag.lower():
                    recovered_leads.append(lead)
                    break
        
        # Contar quantos dos leads recuperados foram convertidos em vendas
        converted_count = 0
        
        for lead in recovered_leads:
            if lead.get("closed_at") and not lead.get("loss_reason_id"):
                converted_count += 1
        
        recovery_count = len(recovered_leads)
        conversion_rate = (converted_count / recovery_count) * 100 if recovery_count > 0 else 0
        
        return {
            "recovered_leads_count": recovery_count,
            "converted_count": converted_count,
            "conversion_rate_percentage": round(conversion_rate, 2)
        }
    except Exception as e:
        print(f"Erro ao analisar recuperação de SalesBot: {e}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/overview")
async def get_analytics_overview(
    days: int = Query(30, description="Período em dias para analisar")
):
    """Retorna uma visão geral das métricas principais"""
    try:
        # Calcular timestamp para o período solicitado
        end_time = int(time.time())
        start_time = end_time - (days * 24 * 60 * 60)
        
        # Obter dados básicos
        leads_data = api.get_leads({"limit": 250})
        users_data = api.get_users()
        pipelines_data = api.get_pipelines()
        
        # Contadores
        total_leads = 0
        new_leads = 0
        active_leads = 0
        won_leads = 0
        lost_leads = 0
        total_value = 0
        
        if leads_data and leads_data.get("_embedded"):
            all_leads = leads_data.get("_embedded", {}).get("leads", [])
            total_leads = len(all_leads)
            
            for lead in all_leads:
                created_at = lead.get("created_at", 0)
                status_id = lead.get("status_id")
                closed_at = lead.get("closed_at")
                
                # Novos leads no período
                if created_at >= start_time:
                    new_leads += 1
                
                # Leads ativos (não fechados)
                if not closed_at:
                    active_leads += 1
                
                # Leads ganhos e perdidos
                if closed_at and closed_at >= start_time:
                    if lead.get("loss_reason_id"):
                        lost_leads += 1
                    else:
                        won_leads += 1
                        if lead.get("price"):
                            total_value += lead.get("price", 0)
        
        # Contar usuários ativos
        total_users = 0
        if isinstance(users_data, dict) and users_data.get("_embedded"):
            total_users = len(users_data.get("_embedded", {}).get("users", []))
        elif isinstance(users_data, list):
            total_users = len(users_data)
        
        # Contar pipelines
        total_pipelines = 0
        if pipelines_data and pipelines_data.get("_embedded"):
            total_pipelines = len(pipelines_data.get("_embedded", {}).get("pipelines", []))
        
        # Calcular taxas
        win_rate = (won_leads / (won_leads + lost_leads) * 100) if (won_leads + lost_leads) > 0 else 0
        avg_deal_size = total_value / won_leads if won_leads > 0 else 0
        
        return {
            "period_days": days,
            "leads": {
                "total": total_leads,
                "new": new_leads,
                "active": active_leads,
                "won": won_leads,
                "lost": lost_leads
            },
            "performance": {
                "win_rate_percentage": round(win_rate, 2),
                "total_revenue": round(total_value, 2),
                "average_deal_size": round(avg_deal_size, 2)
            },
            "system": {
                "total_users": total_users,
                "total_pipelines": total_pipelines
            }
        }
    except Exception as e:
        print(f"Erro ao obter visão geral: {e}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/trends")
async def get_analytics_trends(
    days: int = Query(30, description="Período em dias para analisar"),
    metric: str = Query("leads", description="Métrica para analisar (leads, revenue, conversion)")
):
    """Retorna tendências de uma métrica específica ao longo do tempo"""
    try:
        # Calcular timestamps
        end_time = int(time.time())
        start_time = end_time - (days * 24 * 60 * 60)
        
        # Dividir período em intervalos
        interval_days = 1 if days <= 30 else (7 if days <= 90 else 30)
        intervals = []
        
        current_start = start_time
        while current_start < end_time:
            current_end = min(current_start + (interval_days * 24 * 60 * 60), end_time)
            intervals.append({
                "start": current_start,
                "end": current_end,
                "date": datetime.fromtimestamp(current_start).strftime("%Y-%m-%d")
            })
            current_start = current_end
        
        # Coletar dados por intervalo
        trend_data = []
        
        for interval in intervals:
            params = {
                "filter[created_at][from]": interval["start"],
                "filter[created_at][to]": interval["end"],
                "limit": 250
            }
            
            data = api.get_leads(params)
            
            if metric == "leads":
                # Contar novos leads
                count = 0
                if data and data.get("_embedded"):
                    count = len(data.get("_embedded", {}).get("leads", []))
                
                trend_data.append({
                    "date": interval["date"],
                    "value": count,
                    "metric": "new_leads"
                })
                
            elif metric == "revenue":
                # Somar receita de leads ganhos
                revenue = 0
                if data and data.get("_embedded"):
                    leads = data.get("_embedded", {}).get("leads", [])
                    for lead in leads:
                        if lead.get("closed_at") and not lead.get("loss_reason_id"):
                            revenue += lead.get("price", 0)
                
                trend_data.append({
                    "date": interval["date"],
                    "value": round(revenue, 2),
                    "metric": "revenue"
                })
                
            elif metric == "conversion":
                # Calcular taxa de conversão
                won = 0
                lost = 0
                if data and data.get("_embedded"):
                    leads = data.get("_embedded", {}).get("leads", [])
                    for lead in leads:
                        if lead.get("closed_at"):
                            if lead.get("loss_reason_id"):
                                lost += 1
                            else:
                                won += 1
                
                conversion_rate = (won / (won + lost) * 100) if (won + lost) > 0 else 0
                
                trend_data.append({
                    "date": interval["date"],
                    "value": round(conversion_rate, 2),
                    "metric": "conversion_rate_percentage"
                })
        
        # Calcular estatísticas
        values = [item["value"] for item in trend_data]
        avg_value = sum(values) / len(values) if values else 0
        max_value = max(values) if values else 0
        min_value = min(values) if values else 0
        
        # Calcular tendência (crescente, decrescente, estável)
        trend = "stable"
        if len(values) >= 2:
            first_half_avg = sum(values[:len(values)//2]) / (len(values)//2)
            second_half_avg = sum(values[len(values)//2:]) / (len(values) - len(values)//2)
            
            if second_half_avg > first_half_avg * 1.1:
                trend = "increasing"
            elif second_half_avg < first_half_avg * 0.9:
                trend = "decreasing"
        
        return {
            "metric": metric,
            "period_days": days,
            "interval_days": interval_days,
            "trend": trend,
            "data": trend_data,
            "statistics": {
                "average": round(avg_value, 2),
                "maximum": round(max_value, 2),
                "minimum": round(min_value, 2)
            }
        }
    except Exception as e:
        print(f"Erro ao obter tendências: {e}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/funnel")
async def get_analytics_funnel(
    pipeline_id: Optional[int] = Query(None, description="ID do pipeline para analisar"),
    days: int = Query(30, description="Período em dias para analisar")
):
    """Retorna análise do funil de vendas"""
    try:
        # Obter pipelines
        pipeline_data = api.get_pipelines()
        
        if not pipeline_data or not pipeline_data.get("_embedded"):
            return {"funnel": [], "conversion_rate": 0, "message": "Não foi possível obter pipelines"}
        
        pipelines = pipeline_data.get("_embedded", {}).get("pipelines", [])
        
        # Se pipeline_id não for fornecido, use o primeiro
        if not pipeline_id and pipelines:
            pipeline_id = pipelines[0].get("id")
        
        if not pipeline_id:
            return {"funnel": [], "conversion_rate": 0, "message": "Não foi possível determinar um pipeline"}
        
        # Obter estágios do pipeline
        statuses_data = api.get_pipeline_statuses(pipeline_id)
        
        if not statuses_data or not statuses_data.get("_embedded"):
            return {"funnel": [], "conversion_rate": 0, "message": "Não foi possível obter estágios"}
        
        statuses = statuses_data.get("_embedded", {}).get("statuses", [])
        
        # Calcular timestamp para o período
        end_time = int(time.time())
        start_time = end_time - (days * 24 * 60 * 60)
        
        # Analisar leads por estágio
        funnel_data = []
        total_leads_initial = 0
        total_leads_final = 0
        
        for i, status in enumerate(statuses):
            status_id = status.get("id")
            if status_id is None:
                continue
            
            params = {
                "filter[statuses][0][pipeline_id]": pipeline_id,
                "filter[statuses][0][status_id]": status_id,
                "limit": 250
            }
            
            # Para o primeiro estágio, contar leads criados no período
            if i == 0:
                params["filter[created_at][from]"] = start_time
                params["filter[created_at][to]"] = end_time
            
            data = api.get_leads(params)
            
            leads_count = 0
            total_value = 0
            
            if data and data.get("_embedded"):
                leads = data.get("_embedded", {}).get("leads", [])
                leads_count = len(leads)
                
                # Somar valores
                for lead in leads:
                    total_value += lead.get("price", 0)
            
            # Calcular porcentagem do funil
            if i == 0:
                total_leads_initial = leads_count
                percentage = 100
            else:
                percentage = (leads_count / total_leads_initial * 100) if total_leads_initial > 0 else 0
            
            # Verificar se é estágio final (ganho)
            if status.get("type") == "won":
                total_leads_final = leads_count
            
            funnel_data.append({
                "stage": status.get("name", f"Estágio {status_id}"),
                "stage_id": status_id,
                "type": status.get("type", "regular"),
                "leads_count": leads_count,
                "total_value": round(total_value, 2),
                "percentage": round(percentage, 2),
                "color": status.get("color", "#3366CC")
            })
        
        # Calcular taxa de conversão total
        overall_conversion = (total_leads_final / total_leads_initial * 100) if total_leads_initial > 0 else 0
        
        return {
            "pipeline_id": pipeline_id,
            "period_days": days,
            "funnel": funnel_data,
            "overall_conversion_rate": round(overall_conversion, 2),
            "total_leads": total_leads_initial,
            "converted_leads": total_leads_final
        }
    except Exception as e:
        print(f"Erro ao obter funil: {e}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/team-performance")
async def get_team_performance(
    days: int = Query(30, description="Período em dias para analisar")
):
    """Retorna análise de performance da equipe"""
    try:
        # Obter usuários e leads
        users_response = api.get_users()
        
        # Verificar se há erro na resposta de usuários
        if isinstance(users_response, str):
            return {"period_days": days, "team_stats": {}, "user_performance": [], 
                    "message": f"Erro na API de usuários: {users_response}"}
        
        if isinstance(users_response, dict) and users_response.get('_error'):
            return {"period_days": days, "team_stats": {}, "user_performance": [], 
                    "message": f"Erro na API de usuários: {users_response.get('_error_message')}"}
        
        # Extrair lista de usuários da resposta da API
        users = []
        if isinstance(users_response, dict) and '_embedded' in users_response and 'users' in users_response['_embedded']:
            users = users_response['_embedded']['users']
        elif isinstance(users_response, list):
            users = users_response
        else:
            return {"period_days": days, "team_stats": {}, "user_performance": [], 
                    "message": "Estrutura de resposta de usuários não reconhecida"}
        
        # Calcular timestamp para o período
        end_time = int(time.time())
        start_time = end_time - (days * 24 * 60 * 60)
        
        params = {
            "filter[created_at][from]": start_time,
            "filter[created_at][to]": end_time,
            "limit": 500
        }
        
        leads_data = api.get_leads(params)
        
        if not leads_data or not leads_data.get("_embedded"):
            leads = []
        else:
            leads = leads_data.get("_embedded", {}).get("leads", [])
        
        # Analisar performance por usuário
        user_performance = {}
        
        for user in users:
            if not isinstance(user, dict):
                continue
            user_id = user.get("id")
            if user_id is None:
                continue
            
            user_performance[user_id] = {
                "user_id": user_id,
                "user_name": user.get("name", "Unknown"),
                "new_leads": 0,
                "won_deals": 0,
                "lost_deals": 0,
                "active_leads": 0,
                "total_revenue": 0,
                "activities": 0,
                "conversion_rate": 0,
                "avg_deal_size": 0,
                "avg_response_time_hours": 0
            }
        
        # Processar leads
        for lead in leads:
            if not isinstance(lead, dict):
                continue
                
            responsible_user_id = lead.get("responsible_user_id")
            if responsible_user_id not in user_performance:
                continue
            
            # Novos leads
            user_performance[responsible_user_id]["new_leads"] += 1
            
            # Status do lead
            if lead.get("closed_at"):
                if lead.get("loss_reason_id"):
                    user_performance[responsible_user_id]["lost_deals"] += 1
                else:
                    user_performance[responsible_user_id]["won_deals"] += 1
                    user_performance[responsible_user_id]["total_revenue"] += lead.get("price", 0)
            else:
                user_performance[responsible_user_id]["active_leads"] += 1
        
        # Calcular métricas derivadas
        for user_data in user_performance.values():
            # Taxa de conversão
            total_closed = user_data["won_deals"] + user_data["lost_deals"]
            if total_closed > 0:
                user_data["conversion_rate"] = round((user_data["won_deals"] / total_closed) * 100, 2)
            
            # Ticket médio
            if user_data["won_deals"] > 0:
                user_data["avg_deal_size"] = round(user_data["total_revenue"] / user_data["won_deals"], 2)
            
            # Arredondar receita total
            user_data["total_revenue"] = round(user_data["total_revenue"], 2)
        
        # Converter para lista e ordenar por receita
        performance_list = list(user_performance.values())
        performance_list.sort(key=lambda x: x["total_revenue"], reverse=True)
        
        # Calcular estatísticas da equipe
        team_stats = {
            "total_users": len(performance_list),
            "total_new_leads": sum(u["new_leads"] for u in performance_list),
            "total_won_deals": sum(u["won_deals"] for u in performance_list),
            "total_lost_deals": sum(u["lost_deals"] for u in performance_list),
            "total_revenue": sum(u["total_revenue"] for u in performance_list),
            "avg_conversion_rate": round(
                sum(u["conversion_rate"] for u in performance_list) / len(performance_list)
                if performance_list else 0, 2
            ),
            "top_performer": performance_list[0]["user_name"] if performance_list else None
        }
        
        return {
            "period_days": days,
            "team_stats": team_stats,
            "user_performance": performance_list
        }
    except Exception as e:
        print(f"Erro ao obter performance da equipe: {e}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

# Função auxiliar para filtrar leads por corretor (reutilizada de leads.py)
def filter_leads_by_corretor_analytics(leads: list, corretor_name: str) -> list:
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

# NOVOS ENDPOINTS DE ANALYTICS COM FILTRO POR CORRETOR

@router.get("/lead-cycle-time-by-corretor")
async def get_lead_cycle_time_by_corretor(
    corretor_name: str = Query(None, description="Nome do corretor para filtrar"),
    days: int = Query(90, description="Período em dias para análise"),
    include_all: bool = Query(False, description="Se True, retorna dados de todos os corretores")
):
    """Retorna tempo médio de conversão filtrado por corretor"""
    try:
        from datetime import datetime, timedelta
        
        # Buscar leads ganhos no período
        cutoff_date = datetime.now() - timedelta(days=days)
        cutoff_timestamp = int(cutoff_date.timestamp())
        
        # Buscar leads com campos personalizados
        from app.routers.leads import get_all_leads_with_custom_fields
        all_leads = get_all_leads_with_custom_fields()
        
        # Filtrar leads ganhos no período
        won_leads = [
            lead for lead in all_leads 
            if (lead.get("status_id") == 142 and  # won
                lead.get("closed_at", 0) >= cutoff_timestamp)
        ]
        
        if include_all:
            # Calcular para todos os corretores
            corretor_cycle_times = {}
            
            for lead in won_leads:
                custom_fields = lead.get("custom_fields_values", [])
                for field in custom_fields:
                    if field.get("field_id") == 837920:
                        values = field.get("values", [])
                        if values:
                            corretor = values[0].get("value", "")
                            if corretor:
                                # Calcular tempo de ciclo
                                created_at = lead.get("created_at", 0)
                                closed_at = lead.get("closed_at", 0)
                                cycle_time_days = (closed_at - created_at) / (24 * 60 * 60)
                                
                                if corretor not in corretor_cycle_times:
                                    corretor_cycle_times[corretor] = []
                                corretor_cycle_times[corretor].append(cycle_time_days)
                        break
            
            # Calcular médias
            result = {}
            for corretor, times in corretor_cycle_times.items():
                result[corretor] = {
                    "average_cycle_time_days": round(sum(times) / len(times), 1),
                    "leads_analyzed": len(times),
                    "fastest_conversion_days": round(min(times), 1),
                    "slowest_conversion_days": round(max(times), 1)
                }
            
            return {
                "lead_cycle_time_by_corretor": result,
                "period_days": days
            }
        
        elif corretor_name:
            # Filtrar por corretor específico
            corretor_leads = filter_leads_by_corretor_analytics(won_leads, corretor_name)
            
            if not corretor_leads:
                return {
                    "corretor": corretor_name,
                    "average_cycle_time_days": 0,
                    "leads_analyzed": 0,
                    "period_days": days,
                    "message": "Nenhum lead convertido encontrado para este corretor no período"
                }
            
            # Calcular tempos de ciclo
            cycle_times = []
            for lead in corretor_leads:
                created_at = lead.get("created_at", 0)
                closed_at = lead.get("closed_at", 0)
                cycle_time_days = (closed_at - created_at) / (24 * 60 * 60)
                cycle_times.append(cycle_time_days)
            
            return {
                "corretor": corretor_name,
                "average_cycle_time_days": round(sum(cycle_times) / len(cycle_times), 1),
                "leads_analyzed": len(cycle_times),
                "fastest_conversion_days": round(min(cycle_times), 1),
                "slowest_conversion_days": round(max(cycle_times), 1),
                "period_days": days
            }
        
        else:
            return {"error": "Especifique corretor_name ou use include_all=true"}
            
    except Exception as e:
        print(f"Erro ao calcular cycle time por corretor: {e}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/win-rate-by-corretor")
async def get_win_rate_by_corretor(
    corretor_name: str = Query(None, description="Nome do corretor para filtrar"),
    days: int = Query(90, description="Período em dias para análise"),
    include_all: bool = Query(False, description="Se True, retorna dados de todos os corretores")
):
    """Retorna win rate (taxa de vitória) filtrado por corretor"""
    try:
        from datetime import datetime, timedelta
        
        cutoff_date = datetime.now() - timedelta(days=days)
        cutoff_timestamp = int(cutoff_date.timestamp())
        
        from app.routers.leads import get_all_leads_with_custom_fields
        all_leads = get_all_leads_with_custom_fields()
        
        # Filtrar leads fechados no período
        closed_leads = []
        for lead in all_leads:
            if not lead:
                continue
            closed_at = lead.get("closed_at")
            status_id = lead.get("status_id")
            
            if (closed_at is not None and closed_at >= cutoff_timestamp and
                status_id in [142, 143]):  # won ou lost
                closed_leads.append(lead)
        
        if include_all:
            # Calcular para todos os corretores
            corretor_win_rates = {}
            
            for lead in closed_leads:
                custom_fields = lead.get("custom_fields_values", [])
                for field in custom_fields:
                    if field.get("field_id") == 837920:
                        values = field.get("values", [])
                        if values:
                            corretor = values[0].get("value", "")
                            if corretor:
                                if corretor not in corretor_win_rates:
                                    corretor_win_rates[corretor] = {"won": 0, "lost": 0}
                                
                                if lead.get("status_id") == 142:  # won
                                    corretor_win_rates[corretor]["won"] += 1
                                else:  # lost
                                    corretor_win_rates[corretor]["lost"] += 1
                        break
            
            # Calcular win rates
            result = {}
            for corretor, stats in corretor_win_rates.items():
                total_closed = stats["won"] + stats["lost"]
                win_rate = (stats["won"] / total_closed * 100) if total_closed > 0 else 0
                
                result[corretor] = {
                    "win_rate": round(win_rate, 2),
                    "total_won": stats["won"],
                    "total_lost": stats["lost"],
                    "total_closed": total_closed
                }
            
            return {
                "win_rates_by_corretor": result,
                "period_days": days
            }
        
        elif corretor_name:
            # Filtrar por corretor específico
            corretor_leads = filter_leads_by_corretor_analytics(closed_leads, corretor_name)
            
            won_leads = len([lead for lead in corretor_leads if lead.get("status_id") == 142])
            lost_leads = len([lead for lead in corretor_leads if lead.get("status_id") == 143])
            total_closed = won_leads + lost_leads
            
            win_rate = (won_leads / total_closed * 100) if total_closed > 0 else 0
            
            return {
                "corretor": corretor_name,
                "win_rate": round(win_rate, 2),
                "total_won": won_leads,
                "total_lost": lost_leads,
                "total_closed": total_closed,
                "period_days": days
            }
        
        else:
            return {"error": "Especifique corretor_name ou use include_all=true"}
            
    except Exception as e:
        print(f"Erro ao calcular win rate por corretor: {e}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/average-deal-size-by-corretor")
async def get_average_deal_size_by_corretor(
    corretor_name: str = Query(None, description="Nome do corretor para filtrar"),
    days: int = Query(90, description="Período em dias para análise"),
    include_all: bool = Query(False, description="Se True, retorna dados de todos os corretores")
):
    """Retorna ticket médio filtrado por corretor"""
    try:
        from datetime import datetime, timedelta
        
        cutoff_date = datetime.now() - timedelta(days=days)
        cutoff_timestamp = int(cutoff_date.timestamp())
        
        from app.routers.leads import get_all_leads_with_custom_fields
        all_leads = get_all_leads_with_custom_fields()
        
        # Filtrar leads ganhos no período
        won_leads = [
            lead for lead in all_leads 
            if (lead.get("status_id") == 142 and  # won
                lead.get("closed_at", 0) >= cutoff_timestamp)
        ]
        
        if include_all:
            # Calcular para todos os corretores
            corretor_deal_sizes = {}
            
            for lead in won_leads:
                custom_fields = lead.get("custom_fields_values", [])
                for field in custom_fields:
                    if field.get("field_id") == 837920:
                        values = field.get("values", [])
                        if values:
                            corretor = values[0].get("value", "")
                            if corretor:
                                price = lead.get("price", 0) or 0
                                
                                if corretor not in corretor_deal_sizes:
                                    corretor_deal_sizes[corretor] = []
                                corretor_deal_sizes[corretor].append(price)
                        break
            
            # Calcular médias
            result = {}
            for corretor, prices in corretor_deal_sizes.items():
                if prices:
                    result[corretor] = {
                        "average_deal_size": round(sum(prices) / len(prices), 2),
                        "total_revenue": round(sum(prices), 2),
                        "deals_count": len(prices),
                        "largest_deal": round(max(prices), 2),
                        "smallest_deal": round(min(prices), 2)
                    }
            
            return {
                "average_deal_size_by_corretor": result,
                "period_days": days
            }
        
        elif corretor_name:
            # Filtrar por corretor específico
            corretor_leads = filter_leads_by_corretor_analytics(won_leads, corretor_name)
            
            if not corretor_leads:
                return {
                    "corretor": corretor_name,
                    "average_deal_size": 0,
                    "total_revenue": 0,
                    "deals_count": 0,
                    "period_days": days,
                    "message": "Nenhuma venda encontrada para este corretor no período"
                }
            
            prices = [lead.get("price", 0) or 0 for lead in corretor_leads]
            
            return {
                "corretor": corretor_name,
                "average_deal_size": round(sum(prices) / len(prices), 2),
                "total_revenue": round(sum(prices), 2),
                "deals_count": len(prices),
                "largest_deal": round(max(prices), 2),
                "smallest_deal": round(min(prices), 2),
                "period_days": days
            }
        
        else:
            return {"error": "Especifique corretor_name ou use include_all=true"}
            
    except Exception as e:
        print(f"Erro ao calcular ticket médio por corretor: {e}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/overview-by-corretor")
async def get_analytics_overview_by_corretor(
    corretor_name: str = Query(None, description="Nome do corretor para filtrar"),
    days: int = Query(30, description="Período em dias para análise"),
    include_all: bool = Query(False, description="Se True, retorna dados de todos os corretores")
):
    """Retorna visão geral de analytics filtrada por corretor"""
    try:
        from datetime import datetime, timedelta
        
        cutoff_date = datetime.now() - timedelta(days=days)
        cutoff_timestamp = int(cutoff_date.timestamp())
        
        from app.routers.leads import get_all_leads_with_custom_fields
        all_leads = get_all_leads_with_custom_fields()
        
        # Filtrar leads do período
        period_leads = [
            lead for lead in all_leads 
            if lead.get("created_at", 0) >= cutoff_timestamp
        ]
        
        if include_all:
            # Calcular para todos os corretores
            corretor_overviews = {}
            
            for lead in period_leads:
                custom_fields = lead.get("custom_fields_values", [])
                for field in custom_fields:
                    if field.get("field_id") == 837920:
                        values = field.get("values", [])
                        if values:
                            corretor = values[0].get("value", "")
                            if corretor:
                                if corretor not in corretor_overviews:
                                    corretor_overviews[corretor] = {
                                        "total_leads": 0,
                                        "active_leads": 0,
                                        "won_leads": 0,
                                        "lost_leads": 0,
                                        "total_revenue": 0
                                    }
                                
                                stats = corretor_overviews[corretor]
                                stats["total_leads"] += 1
                                
                                if lead.get("status_id") == 142:  # won
                                    stats["won_leads"] += 1
                                    stats["total_revenue"] += lead.get("price", 0) or 0
                                elif lead.get("status_id") == 143:  # lost
                                    stats["lost_leads"] += 1
                                else:  # active
                                    stats["active_leads"] += 1
                        break
            
            # Calcular métricas derivadas
            for corretor, stats in corretor_overviews.items():
                total_closed = stats["won_leads"] + stats["lost_leads"]
                stats["conversion_rate"] = round(
                    (stats["won_leads"] / stats["total_leads"] * 100) if stats["total_leads"] > 0 else 0, 2
                )
                stats["win_rate"] = round(
                    (stats["won_leads"] / total_closed * 100) if total_closed > 0 else 0, 2
                )
                stats["average_deal_size"] = round(
                    (stats["total_revenue"] / stats["won_leads"]) if stats["won_leads"] > 0 else 0, 2
                )
                stats["total_revenue"] = round(stats["total_revenue"], 2)
            
            return {
                "analytics_overview_by_corretor": corretor_overviews,
                "period_days": days
            }
        
        elif corretor_name:
            # Filtrar por corretor específico
            corretor_leads = filter_leads_by_corretor_analytics(period_leads, corretor_name)
            
            total_leads = len(corretor_leads)
            active_leads = len([lead for lead in corretor_leads if lead.get("status_id") not in [142, 143]])
            won_leads = len([lead for lead in corretor_leads if lead.get("status_id") == 142])
            lost_leads = len([lead for lead in corretor_leads if lead.get("status_id") == 143])
            
            total_revenue = sum(lead.get("price", 0) or 0 for lead in corretor_leads if lead.get("status_id") == 142)
            
            total_closed = won_leads + lost_leads
            conversion_rate = (won_leads / total_leads * 100) if total_leads > 0 else 0
            win_rate = (won_leads / total_closed * 100) if total_closed > 0 else 0
            average_deal_size = (total_revenue / won_leads) if won_leads > 0 else 0
            
            return {
                "corretor": corretor_name,
                "total_leads": total_leads,
                "active_leads": active_leads,
                "won_leads": won_leads,
                "lost_leads": lost_leads,
                "total_revenue": round(total_revenue, 2),
                "conversion_rate": round(conversion_rate, 2),
                "win_rate": round(win_rate, 2),
                "average_deal_size": round(average_deal_size, 2),
                "period_days": days
            }
        
        else:
            return {"error": "Especifique corretor_name ou use include_all=true"}
            
    except Exception as e:
        print(f"Erro ao obter overview por corretor: {e}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))