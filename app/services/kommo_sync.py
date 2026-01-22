"""
Servico de Sincronizacao Kommo -> MongoDB
Implementa sync completo e incremental
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
import time

from app.services.kommo_api import get_kommo_api
from app.models.kommo_models import (
    leads_collection,
    tasks_collection,
    sync_status_collection,
    kommo_lead_to_model,
    kommo_task_to_model,
    PIPELINE_VENDAS,
    PIPELINE_REMARKETING,
)

logger = logging.getLogger(__name__)

# Instancia da API Kommo
kommo_api = get_kommo_api()


class KommoSyncService:
    """Servico para sincronizar dados do Kommo com MongoDB"""

    def __init__(self):
        self.kommo_api = get_kommo_api()
        self._is_running = False

    async def _create_sync_status(self, sync_type: str) -> str:
        """Cria registro de status de sync"""
        status = {
            "sync_type": sync_type,
            "status": "running",
            "total_leads": 0,
            "processed_leads": 0,
            "total_tasks": 0,
            "processed_tasks": 0,
            "errors": 0,
            "started_at": datetime.utcnow(),
            "created_at": datetime.utcnow(),
        }
        result = await sync_status_collection.insert_one(status)
        return str(result.inserted_id)

    async def _update_sync_status(self, sync_id: str, updates: Dict):
        """Atualiza status de sync"""
        from bson import ObjectId
        await sync_status_collection.update_one(
            {"_id": ObjectId(sync_id)},
            {"$set": updates}
        )

    async def _upsert_leads_batch(self, leads: List[Dict], source: str = "sync") -> Dict:
        """
        Faz upsert de um batch de leads no MongoDB.
        Retorna estatisticas do batch.
        """
        if not leads:
            return {"inserted": 0, "updated": 0, "errors": 0}

        inserted = 0
        updated = 0
        errors = 0

        for lead_data in leads:
            try:
                model_data = kommo_lead_to_model(lead_data, source)
                lead_id = model_data["lead_id"]

                result = await leads_collection.update_one(
                    {"lead_id": lead_id},
                    {"$set": model_data},
                    upsert=True
                )

                if result.upserted_id:
                    inserted += 1
                elif result.modified_count > 0:
                    updated += 1

            except Exception as e:
                errors += 1
                logger.error(f"Erro ao upsert lead {lead_data.get('id')}: {e}")

        return {"inserted": inserted, "updated": updated, "errors": errors}

    async def _upsert_tasks_batch(self, tasks: List[Dict], source: str = "sync") -> Dict:
        """
        Faz upsert de um batch de tasks no MongoDB.
        """
        if not tasks:
            return {"inserted": 0, "updated": 0, "errors": 0}

        inserted = 0
        updated = 0
        errors = 0

        for task_data in tasks:
            try:
                model_data = kommo_task_to_model(task_data, source)
                task_id = model_data["task_id"]

                result = await tasks_collection.update_one(
                    {"task_id": task_id},
                    {"$set": model_data},
                    upsert=True
                )

                if result.upserted_id:
                    inserted += 1
                elif result.modified_count > 0:
                    updated += 1

            except Exception as e:
                errors += 1
                logger.error(f"Erro ao upsert task {task_data.get('id')}: {e}")

        return {"inserted": inserted, "updated": updated, "errors": errors}

    async def sync_all_leads(self, days: int = None, max_pages: int = 50) -> Dict:
        """
        Sincronizacao COMPLETA de todos os leads.
        Busca leads dos pipelines Vendas e Remarketing.

        Args:
            days: Quantos dias para tras buscar (None = TUDO, sem limite de data)
            max_pages: Maximo de paginas por pipeline (default: 50 = 12500 leads)

        Returns:
            Estatisticas da sincronizacao
        """
        if self._is_running:
            return {"error": "Sync ja em execucao"}

        self._is_running = True
        sync_id = await self._create_sync_status("full")

        start_time = time.time()

        if days:
            logger.info(f"Iniciando SYNC COMPLETO - ultimos {days} dias")
        else:
            logger.info(f"Iniciando SYNC COMPLETO - TODO O HISTORICO (sem limite de data)")

        try:
            # Calcular periodo (None = sem filtro de data)
            end_timestamp = int(time.time())
            start_timestamp = end_timestamp - (days * 24 * 60 * 60) if days else None

            total_stats = {
                "leads_vendas": 0,
                "leads_remarketing": 0,
                "tasks": 0,
                "inserted": 0,
                "updated": 0,
                "errors": 0,
            }

            # ===== SYNC LEADS VENDAS =====
            logger.info(f"Buscando leads do pipeline VENDAS ({PIPELINE_VENDAS})...")

            vendas_params = {
                "filter[pipeline_id]": PIPELINE_VENDAS,
                "with": "contacts,tags,custom_fields_values"
            }
            # Adicionar filtro de data apenas se especificado
            if start_timestamp:
                vendas_params["filter[created_at][from]"] = start_timestamp
                vendas_params["filter[created_at][to]"] = end_timestamp

            try:
                # Usar metodo async para melhor performance
                leads_vendas = await self.kommo_api.get_all_leads_async(vendas_params, max_pages=max_pages)
                logger.info(f"Leads Vendas encontrados: {len(leads_vendas)}")
                total_stats["leads_vendas"] = len(leads_vendas)

                # Upsert em batches
                batch_size = 100
                for i in range(0, len(leads_vendas), batch_size):
                    batch = leads_vendas[i:i + batch_size]
                    result = await self._upsert_leads_batch(batch, source="sync_full")
                    total_stats["inserted"] += result["inserted"]
                    total_stats["updated"] += result["updated"]
                    total_stats["errors"] += result["errors"]

                    # Atualizar progresso
                    await self._update_sync_status(sync_id, {
                        "processed_leads": total_stats["inserted"] + total_stats["updated"]
                    })

            except Exception as e:
                logger.error(f"Erro ao sincronizar leads Vendas: {e}")
                total_stats["errors"] += 1

            # ===== SYNC LEADS REMARKETING =====
            logger.info(f"Buscando leads do pipeline REMARKETING ({PIPELINE_REMARKETING})...")

            remarketing_params = {
                "filter[pipeline_id]": PIPELINE_REMARKETING,
                "with": "contacts,tags,custom_fields_values"
            }
            # Adicionar filtro de data apenas se especificado
            if start_timestamp:
                remarketing_params["filter[created_at][from]"] = start_timestamp
                remarketing_params["filter[created_at][to]"] = end_timestamp

            try:
                leads_remarketing = await self.kommo_api.get_all_leads_async(remarketing_params, max_pages=max_pages)
                logger.info(f"Leads Remarketing encontrados: {len(leads_remarketing)}")
                total_stats["leads_remarketing"] = len(leads_remarketing)

                # Upsert em batches
                for i in range(0, len(leads_remarketing), batch_size):
                    batch = leads_remarketing[i:i + batch_size]
                    result = await self._upsert_leads_batch(batch, source="sync_full")
                    total_stats["inserted"] += result["inserted"]
                    total_stats["updated"] += result["updated"]
                    total_stats["errors"] += result["errors"]

                    await self._update_sync_status(sync_id, {
                        "processed_leads": total_stats["inserted"] + total_stats["updated"]
                    })

            except Exception as e:
                logger.error(f"Erro ao sincronizar leads Remarketing: {e}")
                total_stats["errors"] += 1

            # ===== SYNC TASKS (REUNIOES) =====
            logger.info("Buscando tasks/reunioes...")

            tasks_params = {
                "filter[task_type_id]": 2,  # Reunioes
            }
            # Adicionar filtro de data apenas se especificado
            if start_timestamp:
                tasks_params["filter[created_at][from]"] = start_timestamp
                tasks_params["filter[created_at][to]"] = end_timestamp

            try:
                all_tasks = await self.kommo_api.get_all_tasks_async(tasks_params, max_pages=10)
                logger.info(f"Tasks encontradas: {len(all_tasks)}")
                total_stats["tasks"] = len(all_tasks)

                # Upsert tasks
                for i in range(0, len(all_tasks), batch_size):
                    batch = all_tasks[i:i + batch_size]
                    result = await self._upsert_tasks_batch(batch, source="sync_full")
                    total_stats["inserted"] += result["inserted"]
                    total_stats["updated"] += result["updated"]
                    total_stats["errors"] += result["errors"]

                await self._update_sync_status(sync_id, {
                    "processed_tasks": len(all_tasks)
                })

            except Exception as e:
                logger.error(f"Erro ao sincronizar tasks: {e}")
                total_stats["errors"] += 1

            # Finalizar sync
            elapsed = time.time() - start_time
            total_stats["elapsed_seconds"] = round(elapsed, 2)
            total_stats["total_leads"] = total_stats["leads_vendas"] + total_stats["leads_remarketing"]

            await self._update_sync_status(sync_id, {
                "status": "completed",
                "completed_at": datetime.utcnow(),
                "total_leads": total_stats["total_leads"],
                "total_tasks": total_stats["tasks"],
                "processed_leads": total_stats["inserted"] + total_stats["updated"],
                "errors": total_stats["errors"],
            })

            logger.info(f"SYNC COMPLETO finalizado em {elapsed:.2f}s - {total_stats}")

            return {
                "success": True,
                "sync_type": "full",
                "stats": total_stats
            }

        except Exception as e:
            logger.error(f"Erro fatal no sync completo: {e}")
            await self._update_sync_status(sync_id, {
                "status": "failed",
                "error_message": str(e),
                "completed_at": datetime.utcnow(),
            })
            return {"success": False, "error": str(e)}

        finally:
            self._is_running = False

    async def sync_incremental(self, minutes: int = 20) -> Dict:
        """
        Sincronizacao INCREMENTAL - apenas leads atualizados recentemente.
        Usa filtro updated_at da API do Kommo.

        Args:
            minutes: Buscar leads atualizados nos ultimos X minutos (default: 20)

        Returns:
            Estatisticas da sincronizacao
        """
        if self._is_running:
            return {"error": "Sync ja em execucao"}

        self._is_running = True
        sync_id = await self._create_sync_status("incremental")

        start_time = time.time()
        logger.info(f"Iniciando SYNC INCREMENTAL - ultimos {minutes} minutos")

        try:
            # Calcular periodo
            end_timestamp = int(time.time())
            start_timestamp = end_timestamp - (minutes * 60)

            total_stats = {
                "leads": 0,
                "tasks": 0,
                "inserted": 0,
                "updated": 0,
                "errors": 0,
            }

            # ===== SYNC LEADS ATUALIZADOS =====
            # Buscar de ambos os pipelines leads atualizados recentemente

            for pipeline_id, pipeline_name in [
                (PIPELINE_VENDAS, "Vendas"),
                (PIPELINE_REMARKETING, "Remarketing")
            ]:
                logger.info(f"Buscando leads atualizados em {pipeline_name}...")

                params = {
                    "filter[pipeline_id]": pipeline_id,
                    "filter[updated_at][from]": start_timestamp,
                    "filter[updated_at][to]": end_timestamp,
                    "with": "contacts,tags,custom_fields_values"
                }

                try:
                    # Para incremental, usar menos paginas
                    leads = await self.kommo_api.get_all_leads_async(params, max_pages=5)
                    logger.info(f"Leads {pipeline_name} atualizados: {len(leads)}")
                    total_stats["leads"] += len(leads)

                    # Upsert
                    result = await self._upsert_leads_batch(leads, source="sync_incremental")
                    total_stats["inserted"] += result["inserted"]
                    total_stats["updated"] += result["updated"]
                    total_stats["errors"] += result["errors"]

                except Exception as e:
                    logger.error(f"Erro ao sync incremental {pipeline_name}: {e}")
                    total_stats["errors"] += 1

            # ===== SYNC TASKS ATUALIZADAS =====
            logger.info("Buscando tasks atualizadas...")

            tasks_params = {
                "filter[task_type_id]": 2,
                "filter[updated_at][from]": start_timestamp,
                "filter[updated_at][to]": end_timestamp,
            }

            try:
                tasks = await self.kommo_api.get_all_tasks_async(tasks_params, max_pages=3)
                logger.info(f"Tasks atualizadas: {len(tasks)}")
                total_stats["tasks"] = len(tasks)

                result = await self._upsert_tasks_batch(tasks, source="sync_incremental")
                total_stats["inserted"] += result["inserted"]
                total_stats["updated"] += result["updated"]
                total_stats["errors"] += result["errors"]

            except Exception as e:
                logger.error(f"Erro ao sync tasks incremental: {e}")
                total_stats["errors"] += 1

            # Finalizar
            elapsed = time.time() - start_time
            total_stats["elapsed_seconds"] = round(elapsed, 2)

            await self._update_sync_status(sync_id, {
                "status": "completed",
                "completed_at": datetime.utcnow(),
                "total_leads": total_stats["leads"],
                "total_tasks": total_stats["tasks"],
                "processed_leads": total_stats["inserted"] + total_stats["updated"],
                "errors": total_stats["errors"],
            })

            logger.info(f"SYNC INCREMENTAL finalizado em {elapsed:.2f}s - {total_stats}")

            return {
                "success": True,
                "sync_type": "incremental",
                "stats": total_stats
            }

        except Exception as e:
            logger.error(f"Erro fatal no sync incremental: {e}")
            await self._update_sync_status(sync_id, {
                "status": "failed",
                "error_message": str(e),
                "completed_at": datetime.utcnow(),
            })
            return {"success": False, "error": str(e)}

        finally:
            self._is_running = False

    async def sync_single_lead(self, lead_id: int, source: str = "webhook") -> Dict:
        """
        Sincroniza um unico lead pelo ID.
        Usado quando recebe webhook de update.

        Args:
            lead_id: ID do lead no Kommo
            source: Origem da sincronizacao

        Returns:
            Resultado da operacao
        """
        logger.info(f"Sincronizando lead individual: {lead_id}")

        try:
            # Buscar lead completo da API
            lead_data = self.kommo_api.get_lead(lead_id)

            if not lead_data or lead_data.get("_error"):
                return {"success": False, "error": "Lead nao encontrado na API"}

            # Converter e salvar
            model_data = kommo_lead_to_model(lead_data, source)

            result = await leads_collection.update_one(
                {"lead_id": lead_id},
                {"$set": model_data},
                upsert=True
            )

            action = "inserted" if result.upserted_id else "updated"
            logger.info(f"Lead {lead_id} {action} via {source}")

            return {"success": True, "action": action, "lead_id": lead_id}

        except Exception as e:
            logger.error(f"Erro ao sincronizar lead {lead_id}: {e}")
            return {"success": False, "error": str(e)}

    async def delete_lead(self, lead_id: int) -> Dict:
        """
        Marca um lead como deletado (soft delete).
        """
        logger.info(f"Marcando lead {lead_id} como deletado")

        try:
            result = await leads_collection.update_one(
                {"lead_id": lead_id},
                {
                    "$set": {
                        "is_deleted": True,
                        "synced_at": datetime.utcnow(),
                        "source": "webhook_delete"
                    }
                }
            )

            if result.modified_count > 0:
                logger.info(f"Lead {lead_id} marcado como deletado")
                return {"success": True, "action": "deleted"}
            else:
                return {"success": False, "error": "Lead nao encontrado"}

        except Exception as e:
            logger.error(f"Erro ao deletar lead {lead_id}: {e}")
            return {"success": False, "error": str(e)}

    async def get_sync_history(self, limit: int = 10) -> List[Dict]:
        """Retorna historico de sincronizacoes"""
        cursor = sync_status_collection.find().sort("created_at", -1).limit(limit)
        history = []
        async for doc in cursor:
            doc["_id"] = str(doc["_id"])
            history.append(doc)
        return history

    def is_running(self) -> bool:
        """Verifica se ha sync em execucao"""
        return self._is_running


# Instancia singleton
_sync_service: Optional[KommoSyncService] = None


def get_sync_service() -> KommoSyncService:
    """Retorna instancia singleton do servico de sync"""
    global _sync_service
    if _sync_service is None:
        _sync_service = KommoSyncService()
    return _sync_service
