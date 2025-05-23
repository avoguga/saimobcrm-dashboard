from fastapi import APIRouter, Query, HTTPException
from typing import Dict, List, Optional
from app.services.kommo_api import KommoAPI
import time

router = APIRouter(prefix="/leads", tags=["Leads"])
api = KommoAPI()

@router.get("/")
async def get_all_leads(
    limit: int = Query(250, description="Número máximo de leads a retornar"),
    page: int = Query(1, description="Página de resultados")
):
    """Retorna uma lista de leads"""
    try:
        params = {"limit": limit, "page": page}
        data = api.get_leads(params)
        return data
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/count")
async def get_leads_count():
    """Retorna o número total de leads"""
    try:
        data = api.get_leads({"limit": 1})
        total = data.get("_total_items", 0)
        return {"total_leads": total}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/by-source")
async def get_leads_by_source():
    """Retorna leads agrupados por fonte"""
    try:
        params = {"with": "source_id", "limit": 250}
        data = api.get_leads(params)
        leads = data.get("_embedded", {}).get("leads", [])
        
        sources = {}
        for lead in leads:
            source_id = lead.get("source_id")
            source_name = str(source_id) if source_id else "Sem fonte"
            sources[source_name] = sources.get(source_name, 0) + 1
            
        return {"leads_by_source": sources}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/by-tag")
async def get_leads_by_tag():
    """Retorna leads agrupados por tag"""
    try:
        data = api.get_leads({"limit": 250})
        leads = data.get("_embedded", {}).get("leads", [])
        
        tags = {}
        for lead in leads:
            lead_tags = lead.get("_embedded", {}).get("tags", [])
            for tag in lead_tags:
                tag_name = tag.get("name", "Sem tag")
                tags[tag_name] = tags.get(tag_name, 0) + 1
                
        return {"leads_by_tag": tags}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/by-user")
async def get_leads_by_user():
    """Retorna leads agrupados por usuário responsável"""
    try:
        data = api.get_leads({"limit": 250})
        leads = data.get("_embedded", {}).get("leads", [])
        
        users = {}
        for lead in leads:
            user_id = lead.get("responsible_user_id", "Sem responsável")
            users[str(user_id)] = users.get(str(user_id), 0) + 1
            
        return {"leads_by_user": users}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/by-stage")
async def get_leads_by_stage():
    """Retorna leads agrupados por estágio do pipeline"""
    try:
        data = api.get_leads({"limit": 250})
        leads = data.get("_embedded", {}).get("leads", [])
        
        stages = {}
        for lead in leads:
            pipeline_id = lead.get("pipeline_id", "")
            status_id = lead.get("status_id", "")
            stage_key = f"{pipeline_id}_{status_id}"
            stages[stage_key] = stages.get(stage_key, 0) + 1
            
        return {"leads_by_stage": stages}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))