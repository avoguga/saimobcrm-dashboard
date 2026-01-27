"""
Router de Webhooks do Kommo
Recebe eventos em tempo real do Kommo e processa em background
"""

from fastapi import APIRouter, Request, BackgroundTasks, HTTPException, Query
from fastapi.responses import JSONResponse
from typing import Optional
import logging
from datetime import datetime

from app.services.webhook_processor import get_webhook_processor
from app.services.kommo_sync import get_sync_service
from app.models.kommo_models import (
    leads_collection,
    tasks_collection,
    webhook_events_collection,
    sync_status_collection,
    get_leads_stats,
    connect_kommo_mongodb,
)

logger = logging.getLogger(__name__)
router = APIRouter()


# =============================================================================
# WEBHOOKS DO KOMMO
# =============================================================================

@router.post("/kommo")
async def webhook_kommo(request: Request, background_tasks: BackgroundTasks):
    """
    Endpoint UNIFICADO para receber webhooks do Kommo.
    Processa automaticamente leads E tasks.

    O Kommo envia POST com dados quando:
    - Lead e adicionado/atualizado/deletado
    - Status do lead muda
    - Responsavel do lead muda
    - Task e adicionada/atualizada/deletada

    IMPORTANTE: Deve responder em menos de 2 segundos!
    Processamento real e feito em background.
    """
    try:
        # Obter payload do webhook
        # Kommo pode enviar como JSON ou form-data
        content_type = request.headers.get("content-type", "")

        if "application/json" in content_type:
            payload = await request.json()
        else:
            # Form data
            form_data = await request.form()
            payload = dict(form_data)

        # Detectar tipo de evento (lead ou task)
        event_type = "unknown"
        if any(key.startswith("leads[") for key in payload.keys()):
            event_type = "lead"
        elif any(key.startswith("task[") for key in payload.keys()):
            event_type = "task"

        logger.info(f"Webhook Kommo recebido ({event_type}): {list(payload.keys())}")

        # Processar em background para responder rapido
        processor = get_webhook_processor()
        background_tasks.add_task(processor.process_in_background, payload)

        # Resposta imediata
        return JSONResponse(
            status_code=200,
            content={
                "status": "received",
                "event_type": event_type,
                "message": "Webhook recebido e sendo processado",
                "timestamp": datetime.utcnow().isoformat()
            }
        )

    except Exception as e:
        logger.error(f"Erro ao receber webhook Kommo: {e}")
        # Ainda retorna 200 para evitar retry excessivo do Kommo
        return JSONResponse(
            status_code=200,
            content={
                "status": "error",
                "message": str(e),
                "timestamp": datetime.utcnow().isoformat()
            }
        )


# Manter endpoints antigos como alias para compatibilidade
@router.post("/kommo/lead")
async def webhook_kommo_lead(request: Request, background_tasks: BackgroundTasks):
    """Alias para /kommo - mantido para compatibilidade"""
    return await webhook_kommo(request, background_tasks)


@router.post("/kommo/task")
async def webhook_kommo_task(request: Request, background_tasks: BackgroundTasks):
    """Alias para /kommo - mantido para compatibilidade"""
    return await webhook_kommo(request, background_tasks)


# =============================================================================
# ENDPOINTS DE SINCRONIZACAO
# =============================================================================

@router.post("/sync/full")
async def sync_full(
    background_tasks: BackgroundTasks,
    days: int = Query(None, description="Dias para buscar (None = TUDO, sem limite)"),
    all_data: bool = Query(True, description="Buscar TODO o historico (ignora days)"),
    wait: bool = Query(False, description="Aguardar conclusao (default: False)")
):
    """
    Inicia sincronizacao COMPLETA do Kommo para MongoDB.

    - Por padrao busca TODO o historico (all_data=True)
    - Se all_data=False, usa parametro days
    - Busca todas as tasks/reunioes
    - Processa em background por padrao

    Args:
        days: Quantos dias para tras buscar (ignorado se all_data=True)
        all_data: Se True, busca TODO o historico sem limite de data
        wait: Se True, aguarda conclusao (pode demorar varios minutos!)
    """
    sync_service = get_sync_service()

    if sync_service.is_running():
        return JSONResponse(
            status_code=409,
            content={"error": "Sincronizacao ja em execucao"}
        )

    # Se all_data=True, nao usar filtro de dias
    sync_days = None if all_data else (days or 365)

    if wait:
        # Executar e aguardar
        result = await sync_service.sync_all_leads(days=sync_days)
        return result
    else:
        # Executar em background
        background_tasks.add_task(sync_service.sync_all_leads, sync_days)
        msg = "TODO o historico" if sync_days is None else f"ultimos {sync_days} dias"
        return {
            "status": "started",
            "message": f"Sincronizacao completa iniciada ({msg})",
            "check_status": "/webhooks/sync/status"
        }


@router.post("/sync/incremental")
async def sync_incremental(
    background_tasks: BackgroundTasks,
    minutes: int = Query(20, description="Minutos para buscar (default: 20)"),
    wait: bool = Query(False, description="Aguardar conclusao")
):
    """
    Inicia sincronizacao INCREMENTAL.

    - Busca apenas leads atualizados nos ultimos X minutos
    - Muito mais rapido que sync completo
    """
    sync_service = get_sync_service()

    if sync_service.is_running():
        return JSONResponse(
            status_code=409,
            content={"error": "Sincronizacao ja em execucao"}
        )

    if wait:
        result = await sync_service.sync_incremental(minutes=minutes)
        return result
    else:
        background_tasks.add_task(sync_service.sync_incremental, minutes)
        return {
            "status": "started",
            "message": f"Sincronizacao incremental iniciada (ultimos {minutes} minutos)",
            "check_status": "/webhooks/sync/status"
        }


@router.get("/sync/status")
async def sync_status():
    """
    Retorna status da sincronizacao atual e estatisticas.
    """
    sync_service = get_sync_service()
    stats = await get_leads_stats()

    return {
        "is_running": sync_service.is_running(),
        "mongodb_stats": stats,
        "timestamp": datetime.utcnow().isoformat()
    }


@router.get("/sync/history")
async def sync_history(limit: int = Query(10, description="Numero de registros")):
    """
    Retorna historico de sincronizacoes.
    """
    sync_service = get_sync_service()
    history = await sync_service.get_sync_history(limit=limit)
    return {"history": history}


@router.post("/sync/reset")
async def sync_reset():
    """
    Reseta o estado de execucao do sync.
    Usar quando o sync travar e mostrar 'ja em execucao' mesmo sem estar rodando.
    """
    sync_service = get_sync_service()
    result = sync_service.reset_running_state()
    return {
        "status": "reset",
        "was_running": result["was_running"],
        "message": "Estado de sync resetado. Agora pode executar novo sync.",
        "timestamp": datetime.utcnow().isoformat()
    }


# =============================================================================
# ENDPOINTS DE DEBUG/ADMIN
# =============================================================================

@router.get("/events")
async def list_webhook_events(
    limit: int = Query(50, description="Numero de eventos"),
    event_type: Optional[str] = Query(None, description="Filtrar por tipo")
):
    """
    Lista eventos de webhook recebidos (para debug).
    """
    query = {}
    if event_type:
        query["event_type"] = event_type

    cursor = webhook_events_collection.find(query).sort("received_at", -1).limit(limit)

    events = []
    async for doc in cursor:
        doc["_id"] = str(doc["_id"])
        events.append(doc)

    return {"events": events, "count": len(events)}


@router.get("/stats")
async def get_stats():
    """
    Retorna estatisticas gerais do sistema.
    """
    try:
        # Contagens
        total_leads = await leads_collection.count_documents({"is_deleted": False})
        total_tasks = await tasks_collection.count_documents({"is_deleted": False})
        total_events = await webhook_events_collection.count_documents({})
        pending_events = await webhook_events_collection.count_documents({"processed": False})

        # Ultimo evento
        last_event = await webhook_events_collection.find_one(
            {},
            sort=[("received_at", -1)]
        )

        # Ultimo sync
        last_sync = await sync_status_collection.find_one(
            {"status": "completed"},
            sort=[("completed_at", -1)]
        )

        return {
            "leads": {
                "total": total_leads,
            },
            "tasks": {
                "total": total_tasks,
            },
            "webhook_events": {
                "total": total_events,
                "pending": pending_events,
                "last_received": last_event.get("received_at").isoformat() if last_event else None
            },
            "last_sync": {
                "type": last_sync.get("sync_type") if last_sync else None,
                "completed_at": last_sync.get("completed_at").isoformat() if last_sync and last_sync.get("completed_at") else None,
                "total_leads": last_sync.get("total_leads") if last_sync else 0,
            },
            "timestamp": datetime.utcnow().isoformat()
        }
    except Exception as e:
        logger.error(f"Erro ao obter stats: {e}")
        return {"error": str(e)}


@router.post("/init")
async def init_mongodb():
    """
    Inicializa MongoDB e cria indices.
    Util para setup inicial.
    """
    try:
        result = await connect_kommo_mongodb()
        if result:
            return {"status": "success", "message": "MongoDB inicializado com indices"}
        else:
            return JSONResponse(
                status_code=500,
                content={"status": "error", "message": "Falha ao inicializar MongoDB"}
            )
    except Exception as e:
        logger.error(f"Erro ao inicializar MongoDB: {e}")
        return JSONResponse(
            status_code=500,
            content={"status": "error", "message": str(e)}
        )


@router.delete("/leads/all")
async def delete_all_leads(confirm: bool = Query(False, description="Confirmar exclusao")):
    """
    DANGER: Deleta todos os leads do MongoDB.
    Usar apenas para reset/debug.
    """
    if not confirm:
        return JSONResponse(
            status_code=400,
            content={"error": "Adicione ?confirm=true para confirmar exclusao"}
        )

    try:
        result = await leads_collection.delete_many({})
        return {
            "status": "success",
            "deleted_count": result.deleted_count,
            "message": "Todos os leads foram removidos"
        }
    except Exception as e:
        logger.error(f"Erro ao deletar leads: {e}")
        return JSONResponse(
            status_code=500,
            content={"error": str(e)}
        )
