from fastapi import APIRouter, HTTPException, Query
from typing import Dict, List, Optional
from app.services.kommo_api import KommoAPI
from datetime import datetime, timedelta
import time

router = APIRouter(prefix="/events", tags=["Events"])
api = KommoAPI()

@router.get("/")
async def get_all_events(
    limit: int = Query(100, description="Número máximo de eventos a retornar"),
    page: int = Query(1, description="Página de resultados"),
    type: Optional[str] = Query(None, description="Tipo de evento (ex: lead_status_changed)")
):
    """Retorna eventos do Kommo com filtros opcionais"""
    try:
        params = {"limit": limit, "page": page}
        
        if type:
            params["filter[type]"] = type
        
        data = api.get_events(params)
        return data
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/lead-status-changes")
async def get_lead_status_changes(
    days: int = Query(30, description="Período em dias para analisar"),
    pipeline_id: Optional[int] = Query(None, description="ID do pipeline para filtrar"),
    status_id: Optional[int] = Query(None, description="ID do status para filtrar")
):
    """Retorna eventos de mudança de status de leads"""
    try:
        # Calcular timestamp para o período solicitado
        end_time = int(time.time())
        start_time = end_time - (days * 24 * 60 * 60)
        
        params = {
            "filter[type]": "lead_status_changed",
            "filter[created_at][from]": start_time,
            "filter[created_at][to]": end_time,
            "limit": 250
        }
        
        data = api.get_events(params)
        events = data.get("_embedded", {}).get("events", [])
        
        # Filtrar por pipeline e status se necessário
        filtered_events = []
        for event in events:
            value_after = event.get("value_after", {})
            
            if pipeline_id and value_after.get("pipeline_id") != pipeline_id:
                continue
                
            if status_id and value_after.get("status_id") != status_id:
                continue
                
            filtered_events.append(event)
        
        return {"lead_status_changes": filtered_events}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/types")
async def get_event_types():
    """Retorna os tipos de eventos disponíveis no Kommo"""
    try:
        # Tipos de eventos conhecidos do Kommo
        event_types = [
            {
                "type": "lead_added",
                "name": "Lead Adicionado",
                "description": "Quando um novo lead é criado"
            },
            {
                "type": "lead_deleted",
                "name": "Lead Deletado",
                "description": "Quando um lead é removido"
            },
            {
                "type": "lead_status_changed",
                "name": "Status do Lead Alterado",
                "description": "Quando o status de um lead é modificado"
            },
            {
                "type": "lead_restored",
                "name": "Lead Restaurado",
                "description": "Quando um lead deletado é restaurado"
            },
            {
                "type": "contact_added",
                "name": "Contato Adicionado",
                "description": "Quando um novo contato é criado"
            },
            {
                "type": "contact_deleted",
                "name": "Contato Deletado",
                "description": "Quando um contato é removido"
            },
            {
                "type": "company_added",
                "name": "Empresa Adicionada",
                "description": "Quando uma nova empresa é criada"
            },
            {
                "type": "company_deleted",
                "name": "Empresa Deletada",
                "description": "Quando uma empresa é removida"
            },
            {
                "type": "customer_added",
                "name": "Cliente Adicionado",
                "description": "Quando um novo cliente é criado"
            },
            {
                "type": "customer_deleted",
                "name": "Cliente Deletado",
                "description": "Quando um cliente é removido"
            },
            {
                "type": "task_added",
                "name": "Tarefa Adicionada",
                "description": "Quando uma nova tarefa é criada"
            },
            {
                "type": "task_deleted",
                "name": "Tarefa Deletada",
                "description": "Quando uma tarefa é removida"
            },
            {
                "type": "task_completed",
                "name": "Tarefa Concluída",
                "description": "Quando uma tarefa é marcada como concluída"
            },
            {
                "type": "task_deadline_changed",
                "name": "Prazo da Tarefa Alterado",
                "description": "Quando o prazo de uma tarefa é modificado"
            },
            {
                "type": "incoming_call",
                "name": "Chamada Recebida",
                "description": "Quando uma chamada é recebida"
            },
            {
                "type": "outgoing_call",
                "name": "Chamada Realizada",
                "description": "Quando uma chamada é realizada"
            },
            {
                "type": "incoming_sms",
                "name": "SMS Recebido",
                "description": "Quando um SMS é recebido"
            },
            {
                "type": "outgoing_sms",
                "name": "SMS Enviado",
                "description": "Quando um SMS é enviado"
            },
            {
                "type": "entity_linked",
                "name": "Entidade Vinculada",
                "description": "Quando entidades são vinculadas"
            },
            {
                "type": "entity_unlinked",
                "name": "Entidade Desvinculada",
                "description": "Quando entidades são desvinculadas"
            }
        ]
        
        return {"event_types": event_types}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/timeline")
async def get_events_timeline(
    hours: int = Query(24, description="Número de horas para analisar (máximo 168)"),
    type: Optional[str] = Query(None, description="Filtrar por tipo de evento")
):
    """Retorna uma linha do tempo dos eventos agrupados por hora"""
    try:
        # Limitar o período máximo
        if hours > 168:  # 7 dias
            hours = 168
            
        # Calcular timestamps
        end_time = int(time.time())
        start_time = end_time - (hours * 60 * 60)
        
        params = {
            "filter[created_at][from]": start_time,
            "filter[created_at][to]": end_time,
            "limit": 500
        }
        
        if type:
            params["filter[type]"] = type
        
        data = api.get_events(params)
        events = data.get("_embedded", {}).get("events", [])
        
        # Agrupar eventos por hora
        timeline = {}
        
        for event in events:
            created_at = event.get("created_at", 0)
            # Arredondar para a hora
            hour_timestamp = (created_at // 3600) * 3600
            hour_str = datetime.fromtimestamp(hour_timestamp).strftime("%Y-%m-%d %H:00")
            
            if hour_str not in timeline:
                timeline[hour_str] = {
                    "hour": hour_str,
                    "timestamp": hour_timestamp,
                    "total_events": 0,
                    "events_by_type": {}
                }
            
            timeline[hour_str]["total_events"] += 1
            
            event_type = event.get("type", "unknown")
            if event_type not in timeline[hour_str]["events_by_type"]:
                timeline[hour_str]["events_by_type"][event_type] = 0
            timeline[hour_str]["events_by_type"][event_type] += 1
        
        # Converter para lista e ordenar por timestamp
        timeline_list = list(timeline.values())
        timeline_list.sort(key=lambda x: x["timestamp"])
        
        # Calcular estatísticas
        total_events = sum(hour["total_events"] for hour in timeline_list)
        avg_events_per_hour = round(total_events / hours if hours > 0 else 0, 2)
        
        # Encontrar hora mais ativa
        busiest_hour = max(timeline_list, key=lambda x: x["total_events"]) if timeline_list else None
        
        return {
            "timeline": timeline_list,
            "statistics": {
                "total_events": total_events,
                "total_hours": hours,
                "avg_events_per_hour": avg_events_per_hour,
                "busiest_hour": busiest_hour["hour"] if busiest_hour else None,
                "busiest_hour_events": busiest_hour["total_events"] if busiest_hour else 0
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))