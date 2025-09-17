"""
Sistema de agendamento para sincronização automática diária do Facebook
Executa sincronização completa às 5h da manhã todos os dias
"""
import asyncio
import schedule
import time
import threading
from datetime import datetime, date, timedelta
import logging
from typing import Optional

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class FacebookScheduler:
    def __init__(self):
        self.running = False
        self.scheduler_thread: Optional[threading.Thread] = None
        self.last_sync_time: Optional[datetime] = None
        self.sync_status = {
            "running": False,
            "last_run": None,
            "next_run": None,
            "total_campaigns": 0,
            "total_leads": 0,
            "total_spend": 0.0,
            "errors": []
        }

    async def run_daily_sync(self):
        """Executa sincronização completa diária"""
        if self.sync_status["running"]:
            logger.warning("Sincronização já está rodando, pulando execução")
            return

        self.sync_status["running"] = True
        self.sync_status["errors"] = []
        sync_start = datetime.now()

        logger.info("🚀 INICIANDO SINCRONIZAÇÃO DIÁRIA AUTOMÁTICA")
        logger.info(f"Horário: {sync_start.strftime('%Y-%m-%d %H:%M:%S')}")

        try:
            from app.services.facebook_sync import facebook_sync
            from app.models.facebook_models import (
                connect_mongodb, campaigns_collection,
                adsets_collection, ads_collection
            )

            # Conectar MongoDB
            if not await connect_mongodb():
                raise Exception("Falha ao conectar MongoDB")

            # Inicializar API
            if not facebook_sync.initialize_api():
                raise Exception("Falha ao inicializar API Facebook")

            # Configuração conservadora para sincronização automática
            facebook_sync.min_request_interval = 15  # 15 segundos entre requests
            facebook_sync.base_delay = 300  # 5 minutos para rate limit

            # Período: últimos 30 dias
            end_date = date.today()
            start_date = end_date - timedelta(days=30)

            logger.info(f"Sincronizando período: {start_date} a {end_date} (30 dias)")

            # 1. SINCRONIZAR TODAS AS CAMPANHAS (apenas estrutura)
            logger.info("1. Sincronizando estrutura das campanhas...")
            campaigns_success = await facebook_sync.sync_campaigns()

            if not campaigns_success:
                raise Exception("Falha na sincronização das campanhas")

            # Buscar todas as campanhas sincronizadas
            campaigns = await campaigns_collection.find().to_list(None)
            total_campaigns = len(campaigns)

            logger.info(f"Campanhas encontradas: {total_campaigns}")

            # 2. SINCRONIZAR MÉTRICAS DE TODAS AS CAMPANHAS
            logger.info("2. Sincronizando métricas das campanhas...")

            success_count = 0
            total_leads = 0
            total_spend = 0.0

            for i, campaign in enumerate(campaigns, 1):
                campaign_id = campaign['campaign_id']
                campaign_name = campaign.get('name', 'N/A')[:40]

                logger.info(f"[{i}/{total_campaigns}] {campaign_name}")

                try:
                    # Sincronizar métricas da campanha
                    metrics_success = await facebook_sync.sync_metrics_for_date_range_single_campaign(
                        campaign_id, start_date, end_date
                    )

                    if metrics_success:
                        # Verificar métricas sincronizadas
                        updated_campaign = await campaigns_collection.find_one(
                            {"campaign_id": campaign_id}
                        )

                        if updated_campaign and updated_campaign.get('metrics'):
                            metrics = updated_campaign['metrics']
                            campaign_leads = sum(day.get('leads', 0) for day in metrics.values())
                            campaign_spend = sum(day.get('spend', 0) for day in metrics.values())

                            total_leads += campaign_leads
                            total_spend += campaign_spend

                            if campaign_leads > 0:
                                logger.info(f"   ✓ {campaign_leads} leads, R$ {campaign_spend:.2f}")

                        success_count += 1

                        # 3. SINCRONIZAR ADSETS E ADS (apenas para campanhas com leads)
                        if updated_campaign and updated_campaign.get('metrics'):
                            metrics = updated_campaign['metrics']
                            campaign_leads = sum(day.get('leads', 0) for day in metrics.values())

                            if campaign_leads > 0:  # Apenas campanhas com leads
                                logger.info(f"   Sincronizando hierarquia completa...")

                                # Sincronizar AdSets
                                adsets_success = await facebook_sync.sync_adsets_for_campaign(campaign_id)

                                if adsets_success:
                                    # Buscar AdSets da campanha
                                    campaign_adsets = await adsets_collection.find(
                                        {"campaign_id": campaign_id}
                                    ).to_list(None)

                                    logger.info(f"   AdSets: {len(campaign_adsets)}")

                                    # Para cada AdSet: sincronizar Ads e métricas
                                    for adset in campaign_adsets:
                                        adset_id = adset['adset_id']

                                        # Sincronizar Ads
                                        await facebook_sync.sync_ads_for_adset(adset_id, campaign_id)

                                        # Sincronizar métricas do AdSet
                                        await facebook_sync.sync_adset_metrics(
                                            adset_id, start_date, end_date
                                        )

                                        # Sincronizar métricas dos Ads
                                        adset_ads = await ads_collection.find(
                                            {"adset_id": adset_id}
                                        ).to_list(None)

                                        for ad in adset_ads:
                                            ad_id = ad['ad_id']
                                            await facebook_sync.sync_ad_metrics(
                                                ad_id, start_date, end_date
                                            )

                                        # Pausa entre AdSets
                                        await asyncio.sleep(10)

                    else:
                        logger.warning(f"   Falha métricas: {campaign_name}")

                except Exception as e:
                    error_msg = f"Erro campanha {campaign_name}: {str(e)[:100]}"
                    logger.error(error_msg)
                    self.sync_status["errors"].append(error_msg)

                # Pausa entre campanhas
                if i < total_campaigns:
                    await asyncio.sleep(20)

                # Status a cada 10 campanhas
                if i % 10 == 0:
                    logger.info(f"Progresso: {i}/{total_campaigns} campanhas")
                    logger.info(f"Total até agora: {total_leads} leads, R$ {total_spend:.2f}")

            # 4. RESULTADO FINAL
            sync_end = datetime.now()
            duration = (sync_end - sync_start).total_seconds() / 60

            logger.info("=" * 60)
            logger.info("SINCRONIZAÇÃO DIÁRIA CONCLUÍDA")
            logger.info("=" * 60)
            logger.info(f"Duração: {duration:.1f} minutos")
            logger.info(f"Campanhas processadas: {success_count}/{total_campaigns}")
            logger.info(f"Total Leads: {total_leads}")
            logger.info(f"Total Gasto: R$ {total_spend:.2f}")
            logger.info(f"Erros: {len(self.sync_status['errors'])}")

            # Atualizar status
            self.sync_status.update({
                "last_run": sync_end,
                "total_campaigns": success_count,
                "total_leads": total_leads,
                "total_spend": total_spend
            })

            self.last_sync_time = sync_end

        except Exception as e:
            error_msg = f"ERRO GERAL na sincronização: {e}"
            logger.error(error_msg)
            self.sync_status["errors"].append(error_msg)

        finally:
            self.sync_status["running"] = False

    def schedule_daily_sync(self):
        """Agenda sincronização para 5h da manhã todos os dias"""
        schedule.every().day.at("05:00").do(self._run_sync_job)
        logger.info("✓ Sincronização agendada para 5:00 AM todos os dias")

    def _run_sync_job(self):
        """Wrapper para executar sync async em thread separada"""
        logger.info("Executando job de sincronização diária...")

        # Criar novo loop para a sincronização
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        try:
            loop.run_until_complete(self.run_daily_sync())
        except Exception as e:
            logger.error(f"Erro no job de sincronização: {e}")
        finally:
            loop.close()

    def start_scheduler(self):
        """Inicia o scheduler em thread separada"""
        if self.running:
            logger.warning("Scheduler já está rodando")
            return

        self.running = True
        self.schedule_daily_sync()

        def run_scheduler():
            logger.info("🚀 Scheduler iniciado - sincronização diária às 5:00 AM")
            while self.running:
                schedule.run_pending()
                time.sleep(60)  # Verificar a cada minuto
            logger.info("Scheduler parado")

        self.scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
        self.scheduler_thread.start()

        # Calcular próxima execução
        next_run = schedule.jobs[0].next_run if schedule.jobs else None
        self.sync_status["next_run"] = next_run

        return True

    def stop_scheduler(self):
        """Para o scheduler"""
        if not self.running:
            return False

        self.running = False
        schedule.clear()

        if self.scheduler_thread and self.scheduler_thread.is_alive():
            self.scheduler_thread.join(timeout=5)

        logger.info("Scheduler parado")
        return True

    def get_status(self):
        """Retorna status atual do scheduler"""
        next_run = schedule.jobs[0].next_run if schedule.jobs else None

        return {
            "scheduler_running": self.running,
            "sync_running": self.sync_status["running"],
            "last_sync": self.sync_status["last_run"],
            "next_sync": next_run,
            "total_campaigns": self.sync_status["total_campaigns"],
            "total_leads": self.sync_status["total_leads"],
            "total_spend": self.sync_status["total_spend"],
            "errors": self.sync_status["errors"][-5:],  # Últimos 5 erros
            "total_errors": len(self.sync_status["errors"])
        }

    async def run_manual_sync(self):
        """Executa sincronização manual (fora do agendamento)"""
        logger.info("🔧 Executando sincronização manual...")
        await self.run_daily_sync()

# Instância global do scheduler
facebook_scheduler = FacebookScheduler()