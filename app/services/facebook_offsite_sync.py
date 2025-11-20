"""
Serviço de sincronização otimizado para métricas offsite do Facebook
Foca em offsite_complete_registration_add_meta_leads
"""

import asyncio
import logging
import time
from datetime import datetime, date, timedelta
from typing import List, Dict, Any, Optional
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.campaign import Campaign
from facebook_business.exceptions import FacebookRequestError

from app.models.facebook_models import (
    FacebookCampaign, FacebookMetrics,
    campaigns_collection, sync_jobs_collection,
    connect_mongodb
)
from config import FACEBOOK_ACCESS_TOKEN, FACEBOOK_APP_ID

logger = logging.getLogger(__name__)

class FacebookOffsiteSyncService:
    """Serviço focado em métricas offsite de conversão"""

    def __init__(self):
        self.api_initialized = False
        self.account_id = "act_1051414772388438"
        self.last_request_time = 0
        self.min_request_interval = 2
        self.max_retries = 5
        self.base_delay = 60

    def initialize_api(self):
        """Inicializa API do Facebook com timeout configurado"""
        try:
            if FACEBOOK_ACCESS_TOKEN and FACEBOOK_APP_ID:
                # Inicializar com timeout de 180s (3 minutos) para requisições longas
                FacebookAdsApi.init(
                    app_id=FACEBOOK_APP_ID,
                    app_secret=None,
                    access_token=FACEBOOK_ACCESS_TOKEN,
                    timeout=180  # 180 segundos (3 minutos) para insights de 30 dias
                )
                self.api_initialized = True
                logger.info("Facebook API inicializada para métricas offsite (timeout: 180s)")
                return True
        except Exception as e:
            logger.error(f"Erro ao inicializar Facebook API: {e}")
            return False

    async def wait_for_rate_limit(self):
        """Aguarda o tempo necessário para evitar rate limit"""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time

        if time_since_last < self.min_request_interval:
            wait_time = self.min_request_interval - time_since_last
            await asyncio.sleep(wait_time)

        self.last_request_time = time.time()

    async def handle_facebook_request_with_retry(self, request_func, *args, **kwargs):
        """Executa requisição ao Facebook com retry automático"""
        for attempt in range(self.max_retries):
            try:
                await self.wait_for_rate_limit()
                result = request_func(*args, **kwargs)
                return result

            except FacebookRequestError as e:
                error_code = e.api_error_code()
                error_subcode = e.api_error_subcode()

                if error_code == 17 or error_subcode == 2446079:
                    wait_time = self.base_delay * (2 ** attempt)
                    logger.warning(f"Rate limit. Tentativa {attempt + 1}/{self.max_retries}. Aguardando {wait_time}s...")

                    if attempt < self.max_retries - 1:
                        await asyncio.sleep(wait_time)
                        continue
                    else:
                        raise e
                else:
                    raise e

            except Exception as e:
                logger.error(f"Erro inesperado: {e}")
                raise e

        raise Exception(f"Falha após {self.max_retries} tentativas")

    def extract_offsite_metrics(self, actions: List[Dict]) -> Dict[str, int]:
        """
        Extrai apenas métricas offsite relevantes
        Foca em offsite_complete_registration_add_meta_leads
        """
        metrics = {
            'offsite_complete_registration': 0,
            'offsite_leads_total': 0,
            'offsite_search': 0,
            'offsite_content_view': 0,
            'fb_pixel_lead': 0,
            'fb_pixel_complete_registration': 0,
            'lead_grouped': 0,
            'standard_leads': 0
        }

        for action in actions:
            action_type = action.get('action_type', '')
            value = int(action.get('value', 0))

            # Métricas principais offsite
            if action_type == 'offsite_complete_registration_add_meta_leads':
                metrics['offsite_complete_registration'] = value
            elif action_type == 'offsite_search_add_meta_leads':
                metrics['offsite_search'] = value
            elif action_type == 'offsite_content_view_add_meta_leads':
                metrics['offsite_content_view'] = value
            elif action_type == 'offsite_conversion.fb_pixel_lead':
                metrics['fb_pixel_lead'] = value
            elif action_type == 'offsite_conversion.fb_pixel_complete_registration':
                metrics['fb_pixel_complete_registration'] = value
            elif action_type == 'onsite_conversion.lead_grouped':
                metrics['lead_grouped'] = value
            elif action_type == 'lead':
                metrics['standard_leads'] = value

            # Soma total de leads offsite
            if 'offsite' in action_type and 'lead' in action_type.lower():
                metrics['offsite_leads_total'] += value

        return metrics

    async def get_campaign_offsite_metrics(
        self,
        campaign_id: str,
        start_date: str,
        end_date: str
    ) -> Dict[str, Any]:
        """
        Busca métricas offsite de uma campanha específica
        """
        if not self.api_initialized:
            if not self.initialize_api():
                return None

        try:
            campaign = Campaign(campaign_id)

            # Parâmetros otimizados para métricas offsite
            params = {
                'time_range': {'since': start_date, 'until': end_date},
                'fields': [
                    'campaign_name',
                    'spend',
                    'impressions',
                    'reach',
                    'actions',
                    'cost_per_action_type'
                ],
                'level': 'campaign',
                'time_increment': 1
            }

            insights = await self.handle_facebook_request_with_retry(
                lambda: list(campaign.get_insights(params=params))
            )

            results = []
            for insight in insights:
                date_str = insight.get('date_start', start_date)
                actions = insight.get('actions', [])

                # Extrair métricas offsite
                offsite_metrics = self.extract_offsite_metrics(actions)

                # Calcular custo por conversão offsite
                spend = float(insight.get('spend', 0))
                offsite_registrations = offsite_metrics['offsite_complete_registration']
                cost_per_offsite_registration = spend / offsite_registrations if offsite_registrations > 0 else 0

                result = {
                    'date': date_str,
                    'campaign_name': insight.get('campaign_name', ''),
                    'spend': spend,
                    'impressions': int(insight.get('impressions', 0)),
                    'reach': int(insight.get('reach', 0)),

                    # Métricas offsite principais
                    'offsite_complete_registration': offsite_metrics['offsite_complete_registration'],
                    'cost_per_offsite_registration': cost_per_offsite_registration,

                    # Métricas offsite detalhadas
                    'offsite_metrics': offsite_metrics,

                    # Comparação com leads padrão
                    'standard_leads': offsite_metrics['standard_leads'],
                    'offsite_vs_standard_ratio': (
                        offsite_metrics['offsite_complete_registration'] / offsite_metrics['standard_leads']
                        if offsite_metrics['standard_leads'] > 0 else 0
                    )
                }

                results.append(result)

            return {
                'campaign_id': campaign_id,
                'period': {'start': start_date, 'end': end_date},
                'daily_metrics': results,
                'summary': self._calculate_summary(results)
            }

        except Exception as e:
            logger.error(f"Erro ao buscar métricas offsite: {e}")
            return None

    def _calculate_summary(self, daily_metrics: List[Dict]) -> Dict[str, Any]:
        """Calcula resumo das métricas offsite"""
        if not daily_metrics:
            return {}

        total_spend = sum(d['spend'] for d in daily_metrics)
        total_offsite_registrations = sum(d['offsite_complete_registration'] for d in daily_metrics)
        total_standard_leads = sum(d['standard_leads'] for d in daily_metrics)
        total_impressions = sum(d['impressions'] for d in daily_metrics)
        total_reach = sum(d['reach'] for d in daily_metrics)

        # Agregar todas as métricas offsite
        aggregated_offsite = {}
        for day in daily_metrics:
            for key, value in day['offsite_metrics'].items():
                aggregated_offsite[key] = aggregated_offsite.get(key, 0) + value

        return {
            'total_spend': total_spend,
            'total_offsite_registrations': total_offsite_registrations,
            'average_cost_per_offsite_registration': (
                total_spend / total_offsite_registrations if total_offsite_registrations > 0 else 0
            ),
            'total_standard_leads': total_standard_leads,
            'offsite_conversion_rate': (
                total_offsite_registrations / total_standard_leads * 100
                if total_standard_leads > 0 else 0
            ),
            'total_impressions': total_impressions,
            'total_reach': total_reach,
            'days_analyzed': len(daily_metrics),
            'offsite_metrics_breakdown': aggregated_offsite
        }

    async def sync_offsite_metrics_to_mongodb(
        self,
        campaign_id: str,
        metrics_data: Dict[str, Any]
    ) -> bool:
        """Salva métricas offsite no MongoDB"""
        try:
            # Conectar ao MongoDB se necessário
            await connect_mongodb()

            # Preparar documento para salvar
            document = {
                'campaign_id': campaign_id,
                'sync_type': 'offsite_metrics',
                'synced_at': datetime.utcnow(),
                'period': metrics_data['period'],
                'summary': metrics_data['summary'],
                'daily_metrics': metrics_data['daily_metrics']
            }

            # Upsert no MongoDB
            result = await campaigns_collection.update_one(
                {'facebook_id': campaign_id},
                {
                    '$set': {
                        'offsite_metrics': document,
                        'last_offsite_sync': datetime.utcnow()
                    }
                },
                upsert=True
            )

            logger.info(f"Métricas offsite salvas para campanha {campaign_id}")
            return True

        except Exception as e:
            logger.error(f"Erro ao salvar métricas offsite no MongoDB: {e}")
            return False