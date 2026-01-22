"""
Sistema de agendamento para sincronizacao hibrida do Kommo -> MongoDB
- Sync incremental a cada 15 minutos (backup dos webhooks)
- Sync completo diario as 3h da manha
"""

import asyncio
import schedule
import time
import threading
from datetime import datetime
import logging
from typing import Optional

logger = logging.getLogger(__name__)


class KommoScheduler:
    """
    Scheduler para sincronizacao Kommo -> MongoDB.

    Estrategia hibrida:
    1. Webhooks: atualizacao em tempo real (principal)
    2. Sync incremental: a cada 15 min (backup, garante consistencia)
    3. Sync completo: 1x por dia as 3h (garantia total)
    """

    def __init__(self):
        self.running = False
        self.scheduler_thread: Optional[threading.Thread] = None
        self.sync_status = {
            "scheduler_running": False,
            "incremental_running": False,
            "full_running": False,
            "last_incremental": None,
            "last_full": None,
            "next_incremental": None,
            "next_full": None,
            "incremental_count": 0,
            "full_count": 0,
            "errors": []
        }

    async def run_incremental_sync(self):
        """Executa sincronizacao incremental (ultimos 20 minutos)"""
        if self.sync_status["incremental_running"]:
            logger.warning("[Kommo Scheduler] Sync incremental ja em execucao, pulando")
            return

        self.sync_status["incremental_running"] = True
        sync_start = datetime.now()

        logger.info("[Kommo Scheduler] Iniciando SYNC INCREMENTAL")

        try:
            from app.services.kommo_sync import get_sync_service

            sync_service = get_sync_service()

            # Verificar se nao ha sync em execucao
            if sync_service.is_running():
                logger.info("[Kommo Scheduler] Sync ja em execucao (provavelmente full), pulando incremental")
                return

            # Executar sync incremental (ultimos 20 minutos)
            result = await sync_service.sync_incremental(minutes=20)

            if result.get("success"):
                stats = result.get("stats", {})
                logger.info(
                    f"[Kommo Scheduler] Sync incremental concluido: "
                    f"{stats.get('leads', 0)} leads, {stats.get('tasks', 0)} tasks "
                    f"em {stats.get('elapsed_seconds', 0):.2f}s"
                )
                self.sync_status["incremental_count"] += 1
            else:
                error = result.get("error", "Erro desconhecido")
                logger.error(f"[Kommo Scheduler] Erro no sync incremental: {error}")
                self.sync_status["errors"].append(f"[{sync_start}] Incremental: {error}")

            self.sync_status["last_incremental"] = sync_start

        except Exception as e:
            error_msg = f"[{sync_start}] Incremental exception: {str(e)}"
            logger.error(f"[Kommo Scheduler] Erro fatal no sync incremental: {e}")
            self.sync_status["errors"].append(error_msg)

        finally:
            self.sync_status["incremental_running"] = False

    async def run_full_sync(self):
        """Executa sincronizacao completa (ultimos 365 dias)"""
        if self.sync_status["full_running"]:
            logger.warning("[Kommo Scheduler] Sync completo ja em execucao, pulando")
            return

        self.sync_status["full_running"] = True
        sync_start = datetime.now()

        logger.info("[Kommo Scheduler] Iniciando SYNC COMPLETO")

        try:
            from app.services.kommo_sync import get_sync_service

            sync_service = get_sync_service()

            # Executar sync completo (TODO o historico)
            result = await sync_service.sync_all_leads(days=None, max_pages=50)

            if result.get("success"):
                stats = result.get("stats", {})
                logger.info(
                    f"[Kommo Scheduler] Sync completo concluido: "
                    f"{stats.get('total_leads', 0)} leads, {stats.get('tasks', 0)} tasks "
                    f"em {stats.get('elapsed_seconds', 0):.2f}s"
                )
                self.sync_status["full_count"] += 1
            else:
                error = result.get("error", "Erro desconhecido")
                logger.error(f"[Kommo Scheduler] Erro no sync completo: {error}")
                self.sync_status["errors"].append(f"[{sync_start}] Full: {error}")

            self.sync_status["last_full"] = sync_start

        except Exception as e:
            error_msg = f"[{sync_start}] Full exception: {str(e)}"
            logger.error(f"[Kommo Scheduler] Erro fatal no sync completo: {e}")
            self.sync_status["errors"].append(error_msg)

        finally:
            self.sync_status["full_running"] = False

    def _run_incremental_job(self):
        """Wrapper para executar sync incremental em thread separada"""
        logger.info("[Kommo Scheduler] Executando job incremental...")

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        try:
            loop.run_until_complete(self.run_incremental_sync())
        except Exception as e:
            logger.error(f"[Kommo Scheduler] Erro no job incremental: {e}")
        finally:
            loop.close()

    def _run_full_job(self):
        """Wrapper para executar sync completo em thread separada"""
        logger.info("[Kommo Scheduler] Executando job completo...")

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        try:
            loop.run_until_complete(self.run_full_sync())
        except Exception as e:
            logger.error(f"[Kommo Scheduler] Erro no job completo: {e}")
        finally:
            loop.close()

    def schedule_jobs(self):
        """Agenda os jobs de sincronizacao"""
        # Sync incremental a cada 15 minutos
        schedule.every(15).minutes.do(self._run_incremental_job)
        logger.info("[Kommo Scheduler] Sync incremental agendado: a cada 15 minutos")

        # Sync completo diario as 3h da manha
        schedule.every().day.at("03:00").do(self._run_full_job)
        logger.info("[Kommo Scheduler] Sync completo agendado: 03:00 todos os dias")

    def start_scheduler(self):
        """Inicia o scheduler em thread separada"""
        if self.running:
            logger.warning("[Kommo Scheduler] Scheduler ja esta rodando")
            return False

        self.running = True
        self.sync_status["scheduler_running"] = True
        self.schedule_jobs()

        def run_scheduler():
            logger.info("[Kommo Scheduler] Scheduler iniciado")
            while self.running:
                schedule.run_pending()
                time.sleep(30)  # Verificar a cada 30 segundos
            logger.info("[Kommo Scheduler] Scheduler parado")

        self.scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
        self.scheduler_thread.start()

        # Atualizar proximas execucoes
        self._update_next_runs()

        logger.info("[Kommo Scheduler] Scheduler Kommo iniciado com sucesso")
        return True

    def stop_scheduler(self):
        """Para o scheduler"""
        if not self.running:
            return False

        self.running = False
        self.sync_status["scheduler_running"] = False

        # Limpar jobs do Kommo (manter outros schedulers como Facebook)
        # Note: schedule.clear() limparia TODOS os jobs, entao fazemos manualmente
        jobs_to_remove = []
        for job in schedule.jobs:
            # Identificar jobs do Kommo pelos nomes das funcoes
            if hasattr(job.job_func, '__name__'):
                if job.job_func.__name__ in ['_run_incremental_job', '_run_full_job']:
                    jobs_to_remove.append(job)

        for job in jobs_to_remove:
            schedule.cancel_job(job)

        if self.scheduler_thread and self.scheduler_thread.is_alive():
            self.scheduler_thread.join(timeout=5)

        logger.info("[Kommo Scheduler] Scheduler parado")
        return True

    def _update_next_runs(self):
        """Atualiza datas das proximas execucoes"""
        for job in schedule.jobs:
            if hasattr(job.job_func, '__name__'):
                if job.job_func.__name__ == '_run_incremental_job':
                    self.sync_status["next_incremental"] = job.next_run
                elif job.job_func.__name__ == '_run_full_job':
                    self.sync_status["next_full"] = job.next_run

    def get_status(self):
        """Retorna status atual do scheduler"""
        self._update_next_runs()

        return {
            "scheduler_running": self.running,
            "incremental": {
                "running": self.sync_status["incremental_running"],
                "last_run": self.sync_status["last_incremental"].isoformat() if self.sync_status["last_incremental"] else None,
                "next_run": self.sync_status["next_incremental"].isoformat() if self.sync_status["next_incremental"] else None,
                "total_runs": self.sync_status["incremental_count"]
            },
            "full": {
                "running": self.sync_status["full_running"],
                "last_run": self.sync_status["last_full"].isoformat() if self.sync_status["last_full"] else None,
                "next_run": self.sync_status["next_full"].isoformat() if self.sync_status["next_full"] else None,
                "total_runs": self.sync_status["full_count"]
            },
            "errors": self.sync_status["errors"][-5:]  # Ultimos 5 erros
        }

    async def run_initial_sync(self):
        """
        Executa sync inicial se o MongoDB estiver vazio.
        Chamado no startup da aplicacao.
        """
        try:
            from app.models.kommo_models import leads_collection, connect_kommo_mongodb

            # Conectar e criar indices
            await connect_kommo_mongodb()

            # Verificar se ha dados
            count = await leads_collection.count_documents({})

            if count == 0:
                logger.info("[Kommo Scheduler] MongoDB vazio, executando sync inicial...")
                await self.run_full_sync()
            else:
                logger.info(f"[Kommo Scheduler] MongoDB ja tem {count} leads, pulando sync inicial")
                # Executar sync incremental para pegar atualizacoes recentes
                await self.run_incremental_sync()

        except Exception as e:
            logger.error(f"[Kommo Scheduler] Erro no sync inicial: {e}")


# Instancia global do scheduler
kommo_scheduler = KommoScheduler()
