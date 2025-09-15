"""
Agendador de sincronização automática com Facebook
Executa sincronização diária para evitar rate limits
"""

import asyncio
import logging
from datetime import datetime, date, timedelta, time
from typing import Optional
import schedule
from app.services.facebook_sync import facebook_sync
from app.models.facebook_models import sync_jobs_collection

logger = logging.getLogger(__name__)

class FacebookSyncScheduler:
    def __init__(self):
        self.is_running = False
        self.last_sync = None
        self.sync_hour = 3  # Horário da sincronização (3:00 AM)

    async def daily_sync(self):
        """Executa sincronização diária completa"""
        try:
            logger.info("🚀 Iniciando sincronização diária automática...")

            # Verificar última sincronização
            last_job = await sync_jobs_collection.find_one(
                {"status": "completed", "job_type": "full_sync"},
                sort=[("completed_at", -1)]
            )

            if last_job:
                last_sync_time = last_job.get("completed_at")
                if last_sync_time:
                    hours_since_last = (datetime.utcnow() - last_sync_time).total_seconds() / 3600
                    if hours_since_last < 20:  # Evita sincronização muito frequente
                        logger.info(f"⏰ Última sincronização há {hours_since_last:.1f} horas. Pulando...")
                        return

            # Executar sincronização completa (últimos 30 dias)
            success = await facebook_sync.full_sync(days_back=30)

            if success:
                self.last_sync = datetime.utcnow()
                logger.info(f"✅ Sincronização diária completa com sucesso às {self.last_sync}")

                # Registrar job de sucesso
                await sync_jobs_collection.insert_one({
                    "job_type": "full_sync",
                    "account_id": "act_1051414772388438",
                    "status": "completed",
                    "started_at": self.last_sync - timedelta(minutes=5),  # Estimativa
                    "completed_at": self.last_sync,
                    "days_synced": 30,
                    "error_message": None
                })
            else:
                logger.error("❌ Falha na sincronização diária")

                # Registrar job de falha
                await sync_jobs_collection.insert_one({
                    "job_type": "full_sync",
                    "account_id": "act_1051414772388438",
                    "status": "failed",
                    "started_at": datetime.utcnow() - timedelta(minutes=5),
                    "completed_at": datetime.utcnow(),
                    "error_message": "Sincronização falhou - verificar logs"
                })

        except Exception as e:
            logger.error(f"❌ Erro na sincronização diária: {e}")

    async def incremental_sync(self):
        """Sincronização incremental (apenas hoje e ontem)"""
        try:
            logger.info("🔄 Iniciando sincronização incremental...")

            # Sincronizar apenas últimos 2 dias (mais rápido)
            end_date = date.today()
            start_date = end_date - timedelta(days=1)

            success = await facebook_sync.sync_metrics_for_date_range(start_date, end_date)

            if success:
                logger.info("✅ Sincronização incremental completa")
            else:
                logger.warning("⚠️ Problemas na sincronização incremental")

        except Exception as e:
            logger.error(f"❌ Erro na sincronização incremental: {e}")

    async def start_scheduler(self):
        """Inicia o agendador de sincronização"""
        self.is_running = True
        logger.info(f"⏰ Agendador iniciado - Sincronização diária às {self.sync_hour}:00")

        while self.is_running:
            try:
                now = datetime.now()

                # Verificar se é hora da sincronização diária
                if now.hour == self.sync_hour and now.minute == 0:
                    await self.daily_sync()
                    # Aguardar 1 hora para evitar múltiplas execuções
                    await asyncio.sleep(3600)

                # Sincronização incremental a cada 4 horas (exceto na hora da sync completa)
                elif now.hour % 4 == 0 and now.minute == 30 and now.hour != self.sync_hour:
                    await self.incremental_sync()
                    await asyncio.sleep(60)

                # Verificar a cada minuto
                await asyncio.sleep(60)

            except Exception as e:
                logger.error(f"❌ Erro no agendador: {e}")
                await asyncio.sleep(300)  # Aguardar 5 minutos em caso de erro

    def stop_scheduler(self):
        """Para o agendador"""
        self.is_running = False
        logger.info("⏹️ Agendador parado")

    async def force_sync(self, days_back: int = 30):
        """Força uma sincronização manual imediata"""
        logger.info(f"🔄 Sincronização manual forçada - {days_back} dias")
        return await facebook_sync.full_sync(days_back=days_back)

    async def get_sync_status(self):
        """Retorna status da última sincronização"""
        last_job = await sync_jobs_collection.find_one(
            {"status": "completed", "job_type": "full_sync"},
            sort=[("completed_at", -1)]
        )

        if last_job:
            return {
                "last_sync": last_job.get("completed_at"),
                "days_synced": last_job.get("days_synced", 0),
                "next_sync": datetime.combine(
                    date.today() + timedelta(days=1),
                    time(self.sync_hour, 0)
                ),
                "status": "active" if self.is_running else "stopped"
            }

        return {
            "last_sync": None,
            "message": "Nenhuma sincronização encontrada",
            "next_sync": datetime.combine(
                date.today() + timedelta(days=1),
                time(self.sync_hour, 0)
            ),
            "status": "active" if self.is_running else "stopped"
        }

# Instância global do agendador
sync_scheduler = FacebookSyncScheduler()

# Função para iniciar o agendador em background
async def start_background_scheduler():
    """Inicia o agendador em background"""
    asyncio.create_task(sync_scheduler.start_scheduler())
    logger.info("✅ Agendador de sincronização iniciado em background")