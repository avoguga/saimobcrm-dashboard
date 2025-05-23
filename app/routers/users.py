from fastapi import APIRouter, HTTPException
from app.services.kommo_api import KommoAPI

router = APIRouter(prefix="/users", tags=["Users"])
api = KommoAPI()

@router.get("/")
async def get_all_users():
    """Retorna todos os usuários/corretores"""
    try:
        users = api.get_users()
        return {"users": users}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))