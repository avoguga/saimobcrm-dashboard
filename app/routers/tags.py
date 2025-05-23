from fastapi import APIRouter, HTTPException
from app.services.kommo_api import KommoAPI

router = APIRouter(prefix="/tags", tags=["Tags"])
api = KommoAPI()

@router.get("/")
async def get_all_tags():
    """Retorna todas as tags disponíveis"""
    try:
        tags = api.get_tags()
        return {"tags": tags}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))