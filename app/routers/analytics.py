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