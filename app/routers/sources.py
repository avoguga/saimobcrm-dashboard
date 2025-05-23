from fastapi import APIRouter, HTTPException
from typing import Dict, List
from app.services.kommo_api import KommoAPI
import traceback

router = APIRouter(prefix="/sources", tags=["Sources"])
api = KommoAPI()

@router.get("/")
async def get_all_sources():
    """Retorna todas as fontes de leads disponíveis"""
    try:
        data = api.get_sources()
        
        # Verificar se a resposta contém dados válidos
        if not data:
            return {"sources": [], "message": "Resposta vazia da API"}
            
        embedded = data.get("_embedded")
        if not embedded:
            return {"sources": [], "message": "Resposta da API não contém campo '_embedded'"}
            
        sources = embedded.get("sources", [])
        return {"sources": sources}
    except Exception as e:
        print(f"Erro ao obter fontes: {e}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/map")
async def get_sources_map():
    """Retorna um mapeamento de IDs de fonte para nomes legíveis"""
    try:
        data = api.get_sources()
        
        # Verificar se a resposta contém dados válidos
        if not data:
            return {"sources_map": {}, "message": "Resposta vazia da API"}
            
        embedded = data.get("_embedded")
        if not embedded:
            return {"sources_map": {}, "message": "Resposta da API não contém campo '_embedded'"}
            
        sources = embedded.get("sources", [])
        
        sources_map = {}
        for source in sources:
            source_id = source.get("id")
            if source_id is not None:
                source_name = source.get("name", f"Fonte {source_id}")
                sources_map[str(source_id)] = source_name
        
        return {"sources_map": sources_map}
    except Exception as e:
        print(f"Erro ao obter mapa de fontes: {e}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))