from fastapi import APIRouter, Query, HTTPException
from typing import Optional
from datetime import datetime, timedelta
import traceback

router = APIRouter(prefix="/corretor-dashboard", tags=["Corretor Dashboard"])

@router.get("/complete")
async def get_complete_corretor_dashboard(
    corretor_name: str = Query(..., description="Nome do corretor"),
    days: int = Query(30, description="Período em dias para análise")
):
    """
    Retorna dashboard completo com todas as métricas de um corretor específico.
    Este endpoint agrega dados de leads, vendas, analytics, reuniões e SalesBot.
    """
    try:
        from datetime import datetime, timedelta
        from app.routers.leads import get_all_leads_with_custom_fields, filter_leads_by_corretor
        from app.services.kommo_api import KommoAPI
        
        # Buscar todos os leads com campos personalizados
        all_leads = get_all_leads_with_custom_fields()
        
        if not all_leads:
            return {
                "corretor": corretor_name,
                "message": "Não foi possível obter leads da API",
                "metrics": {}
            }
        
        # Filtrar leads do corretor
        corretor_leads = filter_leads_by_corretor(all_leads, corretor_name)
        
        if not corretor_leads:
            return {
                "corretor": corretor_name,
                "message": "Nenhum lead encontrado para este corretor",
                "metrics": {}
            }
        
        # Calcular período
        cutoff_date = datetime.now() - timedelta(days=days)
        cutoff_timestamp = int(cutoff_date.timestamp())
        
        # Filtrar leads do período (com proteção contra None)
        period_leads = []
        for lead in corretor_leads:
            if not lead:
                continue
            created_at = lead.get("created_at")
            if created_at is not None and created_at >= cutoff_timestamp:
                period_leads.append(lead)
        
        # === MÉTRICAS DE LEADS ===
        total_leads = len(period_leads)
        active_leads = len([lead for lead in period_leads if lead.get("status_id") not in [142, 143]])
        won_leads = len([lead for lead in period_leads if lead.get("status_id") == 142])
        lost_leads = len([lead for lead in period_leads if lead.get("status_id") == 143])
        
        # === MÉTRICAS DE VENDAS ===
        won_leads_all_time = [lead for lead in corretor_leads if lead.get("status_id") == 142]
        won_leads_period = [lead for lead in period_leads if lead.get("status_id") == 142]
        
        total_revenue = sum(lead.get("price", 0) or 0 for lead in won_leads_period)
        total_revenue_all_time = sum(lead.get("price", 0) or 0 for lead in won_leads_all_time)
        
        # === TAXAS DE CONVERSÃO ===
        conversion_rate = (won_leads / total_leads * 100) if total_leads > 0 else 0
        
        total_closed = won_leads + lost_leads
        win_rate = (won_leads / total_closed * 100) if total_closed > 0 else 0
        
        # === TICKET MÉDIO ===
        average_deal_size = (total_revenue / len(won_leads_period)) if won_leads_period else 0
        
        # === LEAD CYCLE TIME ===
        cycle_times = []
        for lead in won_leads_period:
            if lead.get("closed_at") and lead.get("created_at"):
                cycle_time_days = (lead.get("closed_at") - lead.get("created_at")) / (24 * 60 * 60)
                cycle_times.append(cycle_time_days)
        
        average_cycle_time = sum(cycle_times) / len(cycle_times) if cycle_times else 0
        
        # === LEADS POR ETAPA DO FUNIL ===
        stage_counts = {}
        try:
            from app.services.kommo_api import KommoAPI
            api = KommoAPI()
            
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
            
            # Agrupar por estágio
            for lead in period_leads:
                status_id = lead.get("status_id")
                stage_name = stage_map.get(status_id, f"Status {status_id}")
                stage_counts[stage_name] = stage_counts.get(stage_name, 0) + 1
        except Exception as e:
            print(f"Erro ao buscar estágios: {e}")
            stage_counts = {"Erro ao carregar estágios": 0}
        
        # === RECUPERAÇÃO SALESBOT ===
        salesbot_tag = "Recuperado pelo SalesBot"
        salesbot_leads = []
        
        for lead in period_leads:
            tags = lead.get("_embedded", {}).get("tags", [])
            if any(tag.get("name") == salesbot_tag for tag in tags):
                salesbot_leads.append(lead)
        
        salesbot_recovered = len(salesbot_leads)
        salesbot_converted = len([lead for lead in salesbot_leads if lead.get("status_id") == 142])
        salesbot_conversion_rate = (salesbot_converted / salesbot_recovered * 100) if salesbot_recovered > 0 else 0
        
        # === REUNIÕES ===
        try:
            api = KommoAPI()
            
            # Buscar reuniões do corretor
            params = {'filter[task_type_id]': 2, 'limit': 250}
            tasks_response = api.get_tasks(params)
            
            meetings_scheduled = 0
            meetings_completed = 0
            
            if tasks_response and '_embedded' in tasks_response:
                all_meetings = tasks_response.get('_embedded', {}).get('tasks', [])
                
                # Filtrar reuniões relacionadas aos leads do corretor
                corretor_lead_ids = [lead.get("id") for lead in corretor_leads]
                
                for meeting in all_meetings:
                    if (meeting.get("entity_type") == "leads" and 
                        meeting.get("entity_id") in corretor_lead_ids):
                        
                        if meeting.get('is_completed'):
                            # Verificar se foi completada no período
                            completed_at = meeting.get('completed_at')
                            if completed_at is not None and completed_at >= cutoff_timestamp:
                                meetings_completed += 1
                        else:
                            meetings_scheduled += 1
            
        except Exception as e:
            print(f"Erro ao buscar reuniões: {e}")
            meetings_scheduled = 0
            meetings_completed = 0
        
        # === COMPARAÇÃO COM PERÍODO ANTERIOR ===
        previous_start = cutoff_date - timedelta(days=days)
        previous_timestamp = int(previous_start.timestamp())
        
        # Filtrar leads do período anterior (com proteção contra None)
        previous_leads = []
        for lead in corretor_leads:
            if not lead:
                continue
            created_at = lead.get("created_at")
            if (created_at is not None and 
                previous_timestamp <= created_at < cutoff_timestamp):
                previous_leads.append(lead)
        
        previous_won = len([lead for lead in previous_leads if lead.get("status_id") == 142])
        previous_revenue = sum(lead.get("price", 0) or 0 for lead in previous_leads if lead.get("status_id") == 142)
        
        # Calcular crescimento
        leads_growth = 0
        if len(previous_leads) > 0:
            leads_growth = ((total_leads - len(previous_leads)) / len(previous_leads)) * 100
        elif total_leads > 0:
            leads_growth = 100
        
        revenue_growth = 0
        if previous_revenue > 0:
            revenue_growth = ((total_revenue - previous_revenue) / previous_revenue) * 100
        elif total_revenue > 0:
            revenue_growth = 100
        
        # === MONTAR RESPOSTA FINAL ===
        dashboard = {
            "corretor": corretor_name,
            "period_days": days,
            "period_start": cutoff_date.isoformat(),
            "period_end": datetime.now().isoformat(),
            
            # Métricas principais
            "leads_metrics": {
                "total_leads": total_leads,
                "active_leads": active_leads,
                "won_leads": won_leads,
                "lost_leads": lost_leads,
                "leads_by_stage": stage_counts
            },
            
            # Métricas de vendas
            "sales_metrics": {
                "total_sales": len(won_leads_period),
                "total_revenue": round(total_revenue, 2),
                "total_revenue_all_time": round(total_revenue_all_time, 2),
                "average_deal_size": round(average_deal_size, 2),
                "largest_deal": round(max([lead.get("price", 0) for lead in won_leads_period], default=0), 2),
                "smallest_deal": round(min([lead.get("price", 0) for lead in won_leads_period if lead.get("price", 0) > 0], default=0), 2)
            },
            
            # Taxas e performance
            "performance_metrics": {
                "conversion_rate": round(conversion_rate, 2),
                "win_rate": round(win_rate, 2),
                "average_cycle_time_days": round(average_cycle_time, 1),
                "fastest_conversion_days": round(min(cycle_times), 1) if cycle_times else 0,
                "slowest_conversion_days": round(max(cycle_times), 1) if cycle_times else 0
            },
            
            # Reuniões
            "meetings_metrics": {
                "scheduled_meetings": meetings_scheduled,
                "completed_meetings": meetings_completed,
                "total_meetings": meetings_scheduled + meetings_completed,
                "completion_rate": round((meetings_completed / (meetings_scheduled + meetings_completed) * 100), 2) if (meetings_scheduled + meetings_completed) > 0 else 0
            },
            
            # SalesBot
            "salesbot_metrics": {
                "recovered_leads": salesbot_recovered,
                "recovered_converted": salesbot_converted,
                "recovery_conversion_rate": round(salesbot_conversion_rate, 2)
            },
            
            # Crescimento comparado ao período anterior
            "growth_metrics": {
                "leads_growth_percentage": round(leads_growth, 2),
                "revenue_growth_percentage": round(revenue_growth, 2),
                "previous_period": {
                    "leads": len(previous_leads),
                    "won_leads": previous_won,
                    "revenue": round(previous_revenue, 2)
                }
            },
            
            # Resumo executivo
            "summary": {
                "total_leads_all_time": len(corretor_leads),
                "total_won_all_time": len(won_leads_all_time),
                "total_revenue_all_time": round(total_revenue_all_time, 2),
                "overall_conversion_rate": round((len(won_leads_all_time) / len(corretor_leads) * 100), 2) if corretor_leads else 0,
                "rank_position": None  # Pode ser calculado comparando com outros corretores
            }
        }
        
        return dashboard
        
    except Exception as e:
        print(f"Erro ao gerar dashboard do corretor: {e}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")

@router.get("/comparison")
async def get_corretores_comparison(
    days: int = Query(30, description="Período em dias para análise"),
    top_n: int = Query(10, description="Número de top corretores para retornar")
):
    """
    Retorna comparação entre todos os corretores ordenados por performance
    """
    try:
        from app.routers.leads import get_all_leads_with_custom_fields
        
        # Buscar todos os leads
        all_leads = get_all_leads_with_custom_fields()
        
        if not all_leads:
            return {
                "team_statistics": {"message": "Não foi possível obter leads da API"},
                "top_corretores": [],
                "period_days": days
            }
        
        # Extrair lista de corretores únicos
        corretores = set()
        for lead in all_leads:
            if not lead:
                continue
            custom_fields = lead.get("custom_fields_values", [])
            if not custom_fields:
                continue
            for field in custom_fields:
                if not field:
                    continue
                if field.get("field_id") == 837920:  # ID do campo Corretor
                    values = field.get("values", [])
                    if values and len(values) > 0:
                        value = values[0].get("value") if values[0] else None
                        corretor = value
                        if corretor:
                            corretores.add(corretor)
                    break
        
        # Calcular métricas para cada corretor
        corretor_metrics = []
        
        for corretor in corretores:
            try:
                # Usar o endpoint completo para cada corretor
                dashboard_data = await get_complete_corretor_dashboard(corretor, days)
                
                if "metrics" not in dashboard_data:  # Se tem dados válidos
                    summary = {
                        "corretor": corretor,
                        "total_leads": dashboard_data["leads_metrics"]["total_leads"],
                        "won_leads": dashboard_data["leads_metrics"]["won_leads"],
                        "total_revenue": dashboard_data["sales_metrics"]["total_revenue"],
                        "conversion_rate": dashboard_data["performance_metrics"]["conversion_rate"],
                        "win_rate": dashboard_data["performance_metrics"]["win_rate"],
                        "average_deal_size": dashboard_data["sales_metrics"]["average_deal_size"],
                        "completed_meetings": dashboard_data["meetings_metrics"]["completed_meetings"],
                        "leads_growth": dashboard_data["growth_metrics"]["leads_growth_percentage"],
                        "revenue_growth": dashboard_data["growth_metrics"]["revenue_growth_percentage"]
                    }
                    corretor_metrics.append(summary)
                    
            except Exception as e:
                print(f"Erro ao processar corretor {corretor}: {e}")
                continue
        
        # Ordenar por receita total (ou outro critério)
        corretor_metrics.sort(key=lambda x: x["total_revenue"], reverse=True)
        
        # Adicionar posição no ranking
        for i, corretor_data in enumerate(corretor_metrics):
            corretor_data["rank_position"] = i + 1
        
        # Retornar apenas os top N
        top_corretores = corretor_metrics[:top_n]
        
        # Calcular estatísticas gerais
        if corretor_metrics:
            team_stats = {
                "total_corretores": len(corretor_metrics),
                "total_leads": sum(c["total_leads"] for c in corretor_metrics),
                "total_revenue": sum(c["total_revenue"] for c in corretor_metrics),
                "average_conversion_rate": sum(c["conversion_rate"] for c in corretor_metrics) / len(corretor_metrics),
                "average_deal_size": sum(c["average_deal_size"] for c in corretor_metrics) / len(corretor_metrics),
                "top_performer": top_corretores[0]["corretor"] if top_corretores else None,
                "period_days": days
            }
        else:
            team_stats = {"message": "Nenhum dado encontrado"}
        
        return {
            "team_statistics": team_stats,
            "top_corretores": top_corretores,
            "period_days": days
        }
        
    except Exception as e:
        print(f"Erro ao gerar comparação de corretores: {e}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")

