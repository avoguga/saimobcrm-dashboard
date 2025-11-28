from fastapi import APIRouter, HTTPException
from app.services.kommo_api import get_kommo_api

router = APIRouter(prefix="/tags", tags=["Tags"])
api = get_kommo_api()

@router.get("/")
async def get_all_tags():
    """Retorna todas as tags disponíveis"""
    try:
        tags = api.get_tags()
        return {"tags": tags}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/popular")
async def get_popular_tags():
    """Retorna as tags mais populares"""
    try:
        tags = api.get_tags()
        # Ordenar tags por número de uso (assumindo que há um campo 'count' ou similar)
        # Se não houver, retorna todas as tags
        if tags and isinstance(tags, list) and len(tags) > 0:
            # Verifica se há campo de contagem
            if 'count' in tags[0]:
                sorted_tags = sorted(tags, key=lambda x: x.get('count', 0), reverse=True)
                return {"popular_tags": sorted_tags[:10]}  # Top 10 tags
            else:
                # Retorna as primeiras 10 tags se não houver contagem
                return {"popular_tags": tags[:10]}
        return {"popular_tags": []}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))