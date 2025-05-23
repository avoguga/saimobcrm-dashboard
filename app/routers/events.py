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