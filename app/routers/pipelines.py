from fastapi import APIRouter, Path, HTTPException
from app.services.kommo_api import KommoAPI

router = APIRouter(prefix="/pipelines", tags=["Pipelines"])
api = KommoAPI()

@router.get("/")
async def get_all_pipelines():
    """Retorna todos os pipelines disponíveis"""
    try:
        pipelines = api.get_pipelines()
        return {"pipelines": pipelines}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/{pipeline_id}/statuses")
async def get_pipeline_statuses(
    pipeline_id: int = Path(..., description="ID do pipeline")
):
    """Retorna todos os estágios de um pipeline específico"""
    try:
        statuses = api.get_pipeline_statuses(pipeline_id)
        return {"statuses": statuses}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))