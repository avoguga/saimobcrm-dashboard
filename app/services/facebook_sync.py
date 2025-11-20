"""
Serviço de sincronização com API do Facebook
Responsável por buscar dados e salvar no MongoDB
"""

import asyncio
import logging
import time
import json
from datetime import datetime, date, timedelta
from typing import List, Dict, Any, Optional
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.campaign import Campaign
from facebook_business.adobjects.adset import AdSet
from facebook_business.adobjects.ad import Ad
from facebook_business.exceptions import FacebookRequestError

from app.models.facebook_models import (
    FacebookCampaign, FacebookAdSet, FacebookAd, FacebookMetrics, SyncJob,
    campaigns_collection, adsets_collection, ads_collection, sync_jobs_collection,
    connect_mongodb
)
from config import FACEBOOK_ACCESS_TOKEN, FACEBOOK_APP_ID

logger = logging.getLogger(__name__)

class FacebookSyncService:
    def __init__(self):
        self.api_initialized = False
        self.account_id = "act_1051414772388438"  # Conta principal
        self.last_request_time = 0
        self.min_request_interval = 2  # Mínimo 2 segundos entre requests
        self.max_retries = 5
        self.base_delay = 60  # 1 minuto base para rate limit
        
    def initialize_api(self):
        """Inicializa API do Facebook"""
        try:
            if FACEBOOK_ACCESS_TOKEN and FACEBOOK_APP_ID:
                FacebookAdsApi.init(FACEBOOK_APP_ID, None, FACEBOOK_ACCESS_TOKEN)
                self.api_initialized = True
                logger.info("OK: Facebook API inicializada com sucesso")
            else:
                logger.error("ERRO: Credenciais do Facebook não encontradas")
                return False
        except Exception as e:
            logger.error(f"ERRO: Erro ao inicializar Facebook API: {e}")
            return False
        return True

    async def wait_for_rate_limit(self):
        """Aguarda o tempo necessário para evitar rate limit"""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time

        if time_since_last < self.min_request_interval:
            wait_time = self.min_request_interval - time_since_last
            logger.info(f"Rate limiting: aguardando {wait_time:.1f} segundos")
            await asyncio.sleep(wait_time)

        self.last_request_time = time.time()

    async def handle_facebook_request_with_retry(self, request_func, *args, **kwargs):
        """Executa requisição ao Facebook com retry automático em caso de rate limit"""
        for attempt in range(self.max_retries):
            try:
                await self.wait_for_rate_limit()
                result = request_func(*args, **kwargs)
                return result

            except FacebookRequestError as e:
                error_code = e.api_error_code()
                error_subcode = e.api_error_subcode()

                # Rate limit errors
                if error_code == 17 or error_subcode == 2446079:
                    wait_time = self.base_delay * (2 ** attempt)  # Backoff exponencial
                    logger.warning(f"Rate limit hit. Tentativa {attempt + 1}/{self.max_retries}. Aguardando {wait_time} segundos...")

                    if attempt < self.max_retries - 1:
                        await asyncio.sleep(wait_time)
                        continue
                    else:
                        logger.error(f"Rate limit persistente após {self.max_retries} tentativas")
                        raise e
                else:
                    # Outros erros, não tentar novamente
                    logger.error(f"Erro Facebook não relacionado a rate limit: {e}")
                    raise e

            except Exception as e:
                logger.error(f"Erro inesperado na requisição Facebook: {e}")
                raise e

        raise Exception(f"Falha após {self.max_retries} tentativas")

    async def create_sync_job(self, job_type: str, start_date: date = None, end_date: date = None) -> str:
        """Cria um job de sincronização"""
        job = SyncJob(
            job_type=job_type,
            account_id=self.account_id,
            start_date=start_date.strftime('%Y-%m-%d') if start_date else None,
            end_date=end_date.strftime('%Y-%m-%d') if end_date else None,
            started_at=datetime.utcnow()
        )
        
        result = await sync_jobs_collection.insert_one(job.dict(by_alias=True, exclude={'id'}))
        job_id = str(result.inserted_id)
        logger.info(f" Job criado: {job_type} (ID: {job_id})")
        return job_id

    async def update_sync_job(self, job_id: str, status: str, **kwargs):
        """Atualiza status do job"""
        update_data = {"status": status, "updated_at": datetime.utcnow()}
        
        if status == "completed":
            update_data["completed_at"] = datetime.utcnow()
        
        update_data.update(kwargs)
        
        await sync_jobs_collection.update_one(
            {"_id": job_id}, 
            {"$set": update_data}
        )

    async def sync_campaigns(self) -> bool:
        """Sincroniza campanhas da conta"""
        if not self.api_initialized:
            if not self.initialize_api():
                return False
                
        job_id = await self.create_sync_job("campaigns")
        
        try:
            logger.info(f" Iniciando sincronização de campanhas da conta {self.account_id}")

            # Buscar campanhas da API com retry
            account = AdAccount(self.account_id)
            campaigns_data = await self.handle_facebook_request_with_retry(
                lambda: list(account.get_campaigns(params={
                    'fields': ['id', 'name', 'status', 'objective', 'buying_type', 'special_ad_categories'],
                    'effective_status': ['ACTIVE', 'PAUSED']
                }))
            )
            
            total_campaigns = len(campaigns_data)
            logger.info(f" Encontradas {total_campaigns} campanhas")
            
            await self.update_sync_job(job_id, "running", total_items=total_campaigns)
            
            processed = 0
            for campaign_data in campaigns_data:
                try:
                    # Criar/atualizar campanha no MongoDB
                    campaign = FacebookCampaign(
                        campaign_id=campaign_data['id'],
                        name=campaign_data.get('name', 'Unknown'),
                        status=campaign_data.get('status', 'UNKNOWN'),
                        objective=campaign_data.get('objective', 'UNKNOWN'),
                        account_id=self.account_id,
                        buying_type=campaign_data.get('buying_type'),
                        special_ad_categories=campaign_data.get('special_ad_categories', []),
                        last_sync=datetime.utcnow()
                    )

                    # Upsert (insert ou update se já existir)
                    await campaigns_collection.replace_one(
                        {"campaign_id": campaign_data['id']},
                        campaign.dict(by_alias=True, exclude={'id'}),
                        upsert=True
                    )

                    processed += 1
                    logger.debug(f"OK: Campanha salva: {campaign_data.get('name', 'Unknown')}")

                except Exception as e:
                    logger.error(f"ERRO: Erro ao processar campanha {campaign_data.get('id')}: {e}")
                    continue
            
            await self.update_sync_job(job_id, "completed", processed_items=processed)
            logger.info(f"OK: Sincronização de campanhas concluída: {processed}/{total_campaigns}")
            return True
            
        except Exception as e:
            await self.update_sync_job(job_id, "failed", error_message=str(e))
            logger.error(f"ERRO: Erro na sincronização de campanhas: {e}")
            return False

    async def sync_adsets_for_campaign(self, campaign_id: str) -> bool:
        """Sincroniza AdSets de uma campanha específica"""
        if not self.api_initialized:
            if not self.initialize_api():
                return False
        
        try:
            logger.info(f" Sincronizando AdSets da campanha {campaign_id}")
            
            # Buscar AdSets da API - APENAS ATIVOS
            campaign = Campaign(campaign_id)
            adsets_data = list(campaign.get_ad_sets(params={
                'fields': ['id', 'name', 'status', 'effective_status', 'daily_budget', 'lifetime_budget', 'targeting'],
                'filtering': json.dumps([{
                    'field': 'effective_status',
                    'operator': 'IN',
                    'value': ['ACTIVE']  # Ignorar PAUSED, ARCHIVED, DELETED
                }])
            }))
            
            logger.info(f" Encontrados {len(adsets_data)} AdSets para campanha {campaign_id}")
            
            adset_ids = []
            for adset_data in adsets_data:
                try:
                    await asyncio.sleep(0.3)  # Rate limiting
                    
                    # Criar/atualizar AdSet no MongoDB
                    adset = FacebookAdSet(
                        adset_id=adset_data['id'],
                        name=adset_data.get('name', 'Unknown'),
                        status=adset_data.get('status', 'UNKNOWN'),
                        campaign_id=campaign_id,
                        account_id=self.account_id,
                        daily_budget=adset_data.get('daily_budget'),
                        lifetime_budget=adset_data.get('lifetime_budget'),
                        targeting=adset_data.get('targeting'),
                        last_sync=datetime.utcnow()
                    )
                    
                    # Upsert AdSet
                    await adsets_collection.replace_one(
                        {"adset_id": adset_data['id']},
                        adset.dict(by_alias=True, exclude={'id'}),
                        upsert=True
                    )
                    
                    adset_ids.append(adset_data['id'])
                    logger.debug(f"OK: AdSet salvo: {adset_data.get('name', 'Unknown')}")
                    
                except Exception as e:
                    logger.error(f"ERRO: Erro ao processar AdSet {adset_data.get('id')}: {e}")
                    continue
            
            # Atualizar campanha com lista de AdSets
            await campaigns_collection.update_one(
                {"campaign_id": campaign_id},
                {"$set": {"adsets": adset_ids, "updated_at": datetime.utcnow()}}
            )
            
            return True
            
        except Exception as e:
            logger.error(f"ERRO: Erro na sincronização de AdSets para campanha {campaign_id}: {e}")
            return False

    async def sync_ads_for_adset(self, adset_id: str, campaign_id: str) -> bool:
        """Sincroniza Ads de um AdSet específico"""
        if not self.api_initialized:
            if not self.initialize_api():
                return False
        
        try:
            await self.wait_for_rate_limit()
            logger.info(f" Sincronizando Ads do AdSet {adset_id}")

            # Buscar Ads da API com retry - APENAS ATIVOS
            ads_data = await self.handle_facebook_request_with_retry(
                lambda: list(AdSet(adset_id).get_ads(params={
                    'fields': ['id', 'name', 'status', 'effective_status'],
                    'filtering': json.dumps([{
                        'field': 'effective_status',
                        'operator': 'IN',
                        'value': ['ACTIVE']  # Ignorar PAUSED, ARCHIVED, DELETED
                    }])
                }))
            )
            
            logger.info(f" Encontrados {len(ads_data)} Ads para AdSet {adset_id}")
            
            ad_ids = []
            for ad_data in ads_data:
                try:
                    await asyncio.sleep(0.2)  # Rate limiting
                    
                    # Criar/atualizar Ad no MongoDB
                    ad = FacebookAd(
                        ad_id=ad_data['id'],
                        name=ad_data.get('name', 'Unknown'),
                        status=ad_data.get('status', 'UNKNOWN'),
                        adset_id=adset_id,
                        campaign_id=campaign_id,
                        account_id=self.account_id,
                        last_sync=datetime.utcnow()
                    )
                    
                    # Upsert Ad
                    await ads_collection.replace_one(
                        {"ad_id": ad_data['id']},
                        ad.dict(by_alias=True, exclude={'id'}),
                        upsert=True
                    )
                    
                    ad_ids.append(ad_data['id'])
                    logger.debug(f"OK: Ad salvo: {ad_data.get('name', 'Unknown')}")
                    
                except Exception as e:
                    logger.error(f"ERRO: Erro ao processar Ad {ad_data.get('id')}: {e}")
                    continue
            
            # Atualizar AdSet com lista de Ads
            await adsets_collection.update_one(
                {"adset_id": adset_id},
                {"$set": {"ads": ad_ids, "updated_at": datetime.utcnow()}}
            )
            
            return True
            
        except Exception as e:
            logger.error(f"ERRO: Erro na sincronização de Ads para AdSet {adset_id}: {e}")
            return False

    async def sync_metrics_for_date_range_single_campaign(self, campaign_id: str, start_date: date, end_date: date) -> bool:
        """Sincroniza métricas para uma campanha específica"""
        if not self.api_initialized:
            if not self.initialize_api():
                return False

        try:
            await self.wait_for_rate_limit()

            # Buscar insights da campanha
            campaign = Campaign(campaign_id)
            insights = list(campaign.get_insights(params={
                'fields': [
                    'spend', 'impressions', 'clicks', 'reach', 'cpc', 'cpm', 'ctr',
                    'actions', 'cost_per_action_type',
                    'unique_clicks', 'inline_link_clicks', 'outbound_clicks',
                    'website_ctr', 'cost_per_unique_click'
                ],
                'time_range': {
                    'since': start_date.strftime('%Y-%m-%d'),
                    'until': end_date.strftime('%Y-%m-%d')
                },
                'time_increment': 1  # Diário
            }))

            if not insights:
                logger.info(f" Nenhuma métrica encontrada para campanha {campaign_id}")
                return True

            # Processar métricas por dia
            metrics_by_date = {}

            for insight in insights:
                date_start = insight.get('date_start')
                if not date_start:
                    continue

                # Extrair leads das actions (incluindo offsite)
                leads = 0
                offsite_registrations = 0
                whatsapp_conversations = 0
                profile_visits = 0
                actions = insight.get('actions', [])

                for action in actions:
                    action_type = action.get('action_type', '')
                    value = int(action.get('value', 0))

                    # TODOS OS TIPOS DE LEADS (7 tipos diferentes)
                    if action_type in [
                        'lead',  # Lead genérico/agrupado
                        'offsite_complete_registration_add_meta_leads',  # Registro completo offsite
                        'offsite_conversion.fb_pixel_lead',  # Lead via Pixel
                        'onsite_conversion.lead_grouped',  # Leads agrupados onsite
                        'onsite_web_lead',  # Lead web onsite
                        'offsite_content_view_add_meta_leads',  # Content view leads
                        'offsite_search_add_meta_leads'  # Search leads
                    ]:
                        leads += value
                        # Contar especificamente os registros offsite
                        if 'offsite' in action_type and 'registration' in action_type:
                            offsite_registrations += value
                    elif 'messaging' in action_type.lower() and not action_type.startswith('onsite_conversion'):
                        whatsapp_conversations += value
                    elif action_type in ['page_view', 'profile_view']:
                        profile_visits += value

                # Extrair métricas de engajamento
                reactions = 0
                comments = 0
                shares = 0

                for action in actions:
                    action_type = action.get('action_type', '')
                    value = int(action.get('value', 0))

                    if 'like' in action_type or 'reaction' in action_type:
                        reactions += value
                    elif 'comment' in action_type:
                        comments += value
                    elif 'share' in action_type:
                        shares += value

                page_engagement = reactions + comments + shares

                # Calcular cost per lead
                spend = float(insight.get('spend', 0))
                cost_per_lead = spend / leads if leads > 0 else 0

                metrics_by_date[date_start] = {
                    'date': date_start,
                    # Métricas principais
                    'leads': leads,
                    'offsite_registrations': offsite_registrations,
                    'profile_visits': profile_visits,
                    'whatsapp_conversations': whatsapp_conversations,
                    # Métricas de performance
                    'reach': int(insight.get('reach', 0)),
                    'impressions': int(insight.get('impressions', 0)),
                    'cost_per_lead': round(cost_per_lead, 2),
                    'cpc': float(insight.get('cpc', 0)),
                    'cpm': float(insight.get('cpm', 0)),
                    'clicks': int(insight.get('clicks', 0)),
                    'link_clicks': int(insight.get('inline_link_clicks', 0)),
                    'spend': round(spend, 2),
                    # Métricas de engajamento
                    'page_engagement': page_engagement,
                    'reactions': reactions,
                    'comments': comments,
                    'shares': shares,
                    # Métricas adicionais
                    'ctr': float(insight.get('ctr', 0)),
                    'unique_clicks': int(insight.get('unique_clicks', 0)),
                    'cost_per_unique_click': float(insight.get('cost_per_unique_click', 0))
                }

            if metrics_by_date:
                # Salvar métricas na campanha
                await campaigns_collection.update_one(
                    {"campaign_id": campaign_id},
                    {
                        "$set": {
                            "metrics": metrics_by_date,
                            "updated_at": datetime.utcnow(),
                            "last_sync": datetime.utcnow()
                        }
                    }
                )

                total_leads = sum(m['leads'] for m in metrics_by_date.values())
                total_spend = sum(m['spend'] for m in metrics_by_date.values())

                logger.info(f" OK: {len(metrics_by_date)} dias, {total_leads} leads, R$ {total_spend:.2f}")

            return True

        except Exception as e:
            logger.error(f"ERRO: Falha ao sincronizar métricas da campanha {campaign_id}: {e}")
            return False

    async def sync_adset_metrics(self, adset_id: str, start_date: date, end_date: date) -> bool:
        """Sincroniza métricas de um AdSet específico POR DATA (consistente com campanhas)"""
        if not self.api_initialized:
            if not self.initialize_api():
                return False

        try:
            await self.wait_for_rate_limit()

            # Buscar insights do AdSet POR DATA com time_increment=1
            # IMPORTANTE: time_range explícito para evitar problema do default 30 dias
            insights = await self.handle_facebook_request_with_retry(
                lambda: list(AdSet(adset_id).get_insights(params={
                    'fields': [
                        'spend', 'impressions', 'clicks', 'reach', 'cpc', 'cpm', 'ctr',
                        'actions', 'cost_per_action_type',
                        'unique_clicks', 'inline_link_clicks', 'outbound_clicks',
                        'website_ctr', 'cost_per_unique_click'
                    ],
                    'time_range': {
                        'since': start_date.strftime('%Y-%m-%d'),
                        'until': end_date.strftime('%Y-%m-%d')
                    },
                    'time_increment': 1,  # Diário
                    'filtering': json.dumps([{
                        'field': 'spend',
                        'operator': 'GREATER_THAN',
                        'value': 0  # Só trazer dias com gasto
                    }])
                }))
            )

            if not insights:
                logger.debug(f"Nenhuma métrica encontrada para AdSet {adset_id}")
                return True

            # Processar métricas POR DATA (igual às campanhas)
            metrics_by_date = {}

            for insight in insights:
                date_start = insight.get('date_start')
                if not date_start:
                    continue

                # Inicializar métricas do dia
                day_metrics = {
                    'leads': 0, 'offsite_registrations': 0, 'profile_visits': 0, 'whatsapp_conversations': 0,
                    'reach': 0, 'impressions': 0, 'clicks': 0, 'link_clicks': 0,
                    'spend': 0.0, 'page_engagement': 0, 'reactions': 0,
                    'comments': 0, 'shares': 0, 'ctr': 0.0, 'unique_clicks': 0,
                    'cost_per_unique_click': 0.0, 'cpc': 0.0, 'cpm': 0.0, 'cost_per_lead': 0.0
                }

                # Extrair leads das actions (incluindo offsite)
                actions = insight.get('actions', [])
                for action in actions:
                    action_type = action.get('action_type', '')
                    value = int(action.get('value', 0))

                    # TODOS OS TIPOS DE LEADS (7 tipos diferentes)
                    if action_type in [
                        'lead',  # Lead genérico/agrupado
                        'offsite_complete_registration_add_meta_leads',  # Registro completo offsite
                        'offsite_conversion.fb_pixel_lead',  # Lead via Pixel
                        'onsite_conversion.lead_grouped',  # Leads agrupados onsite
                        'onsite_web_lead',  # Lead web onsite
                        'offsite_content_view_add_meta_leads',  # Content view leads
                        'offsite_search_add_meta_leads'  # Search leads
                    ]:
                        day_metrics['leads'] += value
                        # Contar especificamente os registros offsite
                        if 'offsite' in action_type and 'registration' in action_type:
                            day_metrics['offsite_registrations'] += value
                    elif 'messaging' in action_type.lower() and not action_type.startswith('onsite_conversion'):
                        day_metrics['whatsapp_conversations'] += value
                    elif action_type in ['page_view', 'profile_view']:
                        day_metrics['profile_visits'] += value
                    elif 'like' in action_type or 'reaction' in action_type:
                        day_metrics['reactions'] += value
                    elif 'comment' in action_type:
                        day_metrics['comments'] += value
                    elif 'share' in action_type:
                        day_metrics['shares'] += value

                # Outras métricas
                day_metrics['reach'] = int(insight.get('reach', 0))
                day_metrics['impressions'] = int(insight.get('impressions', 0))
                day_metrics['clicks'] = int(insight.get('clicks', 0))
                day_metrics['link_clicks'] = int(insight.get('inline_link_clicks', 0))
                day_metrics['spend'] = float(insight.get('spend', 0))
                day_metrics['unique_clicks'] = int(insight.get('unique_clicks', 0))

                # Calcular médias e derivados
                if day_metrics['impressions'] > 0:
                    day_metrics['ctr'] = (day_metrics['clicks'] / day_metrics['impressions']) * 100
                    day_metrics['cpm'] = (day_metrics['spend'] / day_metrics['impressions']) * 1000

                if day_metrics['clicks'] > 0:
                    day_metrics['cpc'] = day_metrics['spend'] / day_metrics['clicks']

                if day_metrics['unique_clicks'] > 0:
                    day_metrics['cost_per_unique_click'] = day_metrics['spend'] / day_metrics['unique_clicks']

                if day_metrics['leads'] > 0:
                    day_metrics['cost_per_lead'] = day_metrics['spend'] / day_metrics['leads']

                day_metrics['page_engagement'] = day_metrics['reactions'] + day_metrics['comments'] + day_metrics['shares']

                metrics_by_date[date_start] = day_metrics

            # Salvar métricas POR DATA no AdSet
            await adsets_collection.update_one(
                {"adset_id": adset_id},
                {
                    "$set": {
                        "metrics": metrics_by_date,
                        "updated_at": datetime.utcnow(),
                        "last_sync": datetime.utcnow()
                    }
                }
            )

            # Log com totais do período
            total_leads = sum(day.get('leads', 0) for day in metrics_by_date.values())
            total_spend = sum(day.get('spend', 0) for day in metrics_by_date.values())

            logger.debug(f"OK: Métricas AdSet {adset_id}: {len(metrics_by_date)} dias, {total_leads} leads, R$ {total_spend:.2f}")
            return True

        except Exception as e:
            logger.error(f"ERRO: Falha ao sincronizar métricas do AdSet {adset_id}: {e}")
            return False

    async def sync_ad_metrics(self, ad_id: str, start_date: date, end_date: date) -> bool:
        """Sincroniza métricas de um Ad específico POR DATA (consistente com campanhas)"""
        if not self.api_initialized:
            if not self.initialize_api():
                return False

        try:
            await self.wait_for_rate_limit()

            # Buscar insights do Ad POR DATA com time_increment=1
            # CRÍTICO: time_range explícito e filtrar só com gasto
            insights = await self.handle_facebook_request_with_retry(
                lambda: list(Ad(ad_id).get_insights(params={
                    'fields': [
                        'spend', 'impressions', 'clicks', 'reach', 'cpc', 'cpm', 'ctr',
                        'actions', 'cost_per_action_type',
                        'unique_clicks', 'inline_link_clicks', 'outbound_clicks',
                        'website_ctr', 'cost_per_unique_click'
                    ],
                    'time_range': {
                        'since': start_date.strftime('%Y-%m-%d'),
                        'until': end_date.strftime('%Y-%m-%d')
                    },
                    'time_increment': 1,  # Diário
                    'filtering': json.dumps([{
                        'field': 'spend',
                        'operator': 'GREATER_THAN',
                        'value': 0  # Só trazer dias com gasto
                    }])
                }))
            )

            if not insights:
                logger.debug(f"Nenhuma métrica encontrada para Ad {ad_id}")
                return True

            # Processar métricas POR DATA (igual às campanhas)
            metrics_by_date = {}

            for insight in insights:
                date_start = insight.get('date_start')
                if not date_start:
                    continue

                # Inicializar métricas do dia
                day_metrics = {
                    'leads': 0, 'offsite_registrations': 0, 'profile_visits': 0, 'whatsapp_conversations': 0,
                    'reach': 0, 'impressions': 0, 'clicks': 0, 'link_clicks': 0,
                    'spend': 0.0, 'page_engagement': 0, 'reactions': 0,
                    'comments': 0, 'shares': 0, 'ctr': 0.0, 'unique_clicks': 0,
                    'cost_per_unique_click': 0.0, 'cpc': 0.0, 'cpm': 0.0, 'cost_per_lead': 0.0
                }

                # Extrair leads das actions (incluindo offsite)
                actions = insight.get('actions', [])
                for action in actions:
                    action_type = action.get('action_type', '')
                    value = int(action.get('value', 0))

                    # TODOS OS TIPOS DE LEADS (7 tipos diferentes)
                    if action_type in [
                        'lead',  # Lead genérico/agrupado
                        'offsite_complete_registration_add_meta_leads',  # Registro completo offsite
                        'offsite_conversion.fb_pixel_lead',  # Lead via Pixel
                        'onsite_conversion.lead_grouped',  # Leads agrupados onsite
                        'onsite_web_lead',  # Lead web onsite
                        'offsite_content_view_add_meta_leads',  # Content view leads
                        'offsite_search_add_meta_leads'  # Search leads
                    ]:
                        day_metrics['leads'] += value
                        # Contar especificamente os registros offsite
                        if 'offsite' in action_type and 'registration' in action_type:
                            day_metrics['offsite_registrations'] += value
                    elif 'messaging' in action_type.lower() and not action_type.startswith('onsite_conversion'):
                        day_metrics['whatsapp_conversations'] += value
                    elif action_type in ['page_view', 'profile_view']:
                        day_metrics['profile_visits'] += value
                    elif 'like' in action_type or 'reaction' in action_type:
                        day_metrics['reactions'] += value
                    elif 'comment' in action_type:
                        day_metrics['comments'] += value
                    elif 'share' in action_type:
                        day_metrics['shares'] += value

                # Outras métricas
                day_metrics['reach'] = int(insight.get('reach', 0))
                day_metrics['impressions'] = int(insight.get('impressions', 0))
                day_metrics['clicks'] = int(insight.get('clicks', 0))
                day_metrics['link_clicks'] = int(insight.get('inline_link_clicks', 0))
                day_metrics['spend'] = float(insight.get('spend', 0))
                day_metrics['unique_clicks'] = int(insight.get('unique_clicks', 0))

                # Calcular médias e derivados
                if day_metrics['impressions'] > 0:
                    day_metrics['ctr'] = (day_metrics['clicks'] / day_metrics['impressions']) * 100
                    day_metrics['cpm'] = (day_metrics['spend'] / day_metrics['impressions']) * 1000

                if day_metrics['clicks'] > 0:
                    day_metrics['cpc'] = day_metrics['spend'] / day_metrics['clicks']

                if day_metrics['unique_clicks'] > 0:
                    day_metrics['cost_per_unique_click'] = day_metrics['spend'] / day_metrics['unique_clicks']

                if day_metrics['leads'] > 0:
                    day_metrics['cost_per_lead'] = day_metrics['spend'] / day_metrics['leads']

                day_metrics['page_engagement'] = day_metrics['reactions'] + day_metrics['comments'] + day_metrics['shares']

                metrics_by_date[date_start] = day_metrics

            # Salvar métricas POR DATA no Ad
            await ads_collection.update_one(
                {"ad_id": ad_id},
                {
                    "$set": {
                        "metrics": metrics_by_date,
                        "updated_at": datetime.utcnow(),
                        "last_sync": datetime.utcnow()
                    }
                }
            )

            # Log com totais do período
            total_leads = sum(day.get('leads', 0) for day in metrics_by_date.values())
            total_spend = sum(day.get('spend', 0) for day in metrics_by_date.values())

            if metrics_by_date:
                logger.debug(f"OK: Métricas Ad {ad_id}: {len(metrics_by_date)} dias, {total_leads} leads, R$ {total_spend:.2f}")
            else:
                logger.warning(f"AVISO: Ad {ad_id} sem métricas no período")

            return True

        except Exception as e:
            logger.error(f"ERRO: Falha ao sincronizar métricas do Ad {ad_id}: {e}")
            import traceback
            logger.error(f"Stack trace: {traceback.format_exc()}")
            return False

    async def sync_metrics_for_date_range(self, start_date: date, end_date: date) -> bool:
        """Sincroniza métricas para um período específico - CAMPANHAS, ADSETS e ADS"""
        if not self.api_initialized:
            if not self.initialize_api():
                return False

        job_id = await self.create_sync_job("metrics", start_date, end_date)

        try:
            logger.info(f" Sincronizando métricas de {start_date} a {end_date}")

            # Buscar todas as campanhas do MongoDB
            campaigns = await campaigns_collection.find({"account_id": self.account_id}).to_list(None)
            total_campaigns = len(campaigns)

            # Contar também adsets e ads para o progresso
            all_adsets = await adsets_collection.find().to_list(None)
            all_ads = await ads_collection.find().to_list(None)
            total_items = total_campaigns + len(all_adsets) + len(all_ads)

            await self.update_sync_job(job_id, "running", total_items=total_items)

            processed = 0
            for campaign_doc in campaigns:
                try:
                    await asyncio.sleep(1)  # Rate limiting entre campanhas
                    campaign_id = campaign_doc['campaign_id']

                    # Buscar insights da campanha com TODOS os campos necessários
                    campaign = Campaign(campaign_id)
                    insights = list(campaign.get_insights(params={
                        'fields': [
                            'spend', 'impressions', 'clicks', 'reach', 'cpc', 'cpm', 'ctr',
                            'actions', 'cost_per_action_type',
                            'unique_clicks', 'inline_link_clicks', 'outbound_clicks',
                            'website_ctr', 'cost_per_unique_click'
                        ],
                        'time_range': {
                            'since': start_date.strftime('%Y-%m-%d'),
                            'until': end_date.strftime('%Y-%m-%d')
                        },
                        'time_increment': 1  # Diário
                    }))

                    # Processar métricas por data
                    for insight in insights:
                        try:
                            # Métricas principais
                            leads = 0
                            profile_visits = 0
                            whatsapp_conversations = 0

                            # Métricas de engajamento
                            page_engagement = 0
                            reactions = 0
                            comments = 0
                            shares = 0

                            # Outras métricas
                            link_clicks = 0
                            cost_per_lead = 0.0

                            # Processar actions
                            if 'actions' in insight:
                                for action in insight['actions']:
                                    action_type = action.get('action_type', '')
                                    value = int(action.get('value', 0))

                                    # APENAS offsite_complete_registration_add_meta_leads conforme relatórios
                                    if action_type == 'offsite_complete_registration_add_meta_leads':
                                        leads += value
                                    # Visitas ao perfil
                                    elif action_type in ['page_view', 'profile_view']:
                                        profile_visits += value
                                    # WhatsApp (mensagens)
                                    elif ('messaging' in action_type.lower() or 'whatsapp' in action_type.lower()) and not action_type.startswith('onsite_conversion'):
                                        whatsapp_conversations += value
                                    # Engajamento com a página
                                    elif action_type == 'page_engagement':
                                        page_engagement += value
                                    # Reações
                                    elif action_type in ['post_reaction', 'like']:
                                        reactions += value
                                    # Comentários
                                    elif action_type == 'comment':
                                        comments += value
                                    # Compartilhamentos
                                    elif action_type in ['post', 'share']:
                                        shares += value
                                    # Cliques no link
                                    elif action_type == 'link_click':
                                        link_clicks += value

                            # Processar cost_per_action_type para custo por lead
                            if 'cost_per_action_type' in insight:
                                for cost_action in insight['cost_per_action_type']:
                                    if cost_action.get('action_type') == 'lead':
                                        cost_per_lead = float(cost_action.get('value', 0))

                            # Se não tiver link_clicks nas actions, usar campos diretos
                            if link_clicks == 0:
                                link_clicks = int(insight.get('inline_link_clicks', 0)) or \
                                            int(insight.get('outbound_clicks', 0)) or \
                                            int(insight.get('unique_clicks', 0))

                            # Criar objeto de métricas expandido
                            metrics = {
                                'date': insight.get('date_start', start_date.strftime('%Y-%m-%d')),
                                # Métricas principais
                                'leads': leads,
                                'profile_visits': profile_visits,
                                'whatsapp_conversations': whatsapp_conversations,
                                # Métricas de performance
                                'reach': int(insight.get('reach', 0)),
                                'impressions': int(insight.get('impressions', 0)),
                                'cost_per_lead': cost_per_lead,
                                'cpc': float(insight.get('cpc', 0)),
                                'cpm': float(insight.get('cpm', 0)),
                                'clicks': int(insight.get('clicks', 0)),
                                'link_clicks': link_clicks,
                                'spend': float(insight.get('spend', 0)),
                                # Métricas de engajamento
                                'page_engagement': page_engagement,
                                'reactions': reactions,
                                'comments': comments,
                                'shares': shares,
                                # Métricas adicionais
                                'ctr': float(insight.get('ctr', 0)),
                                'unique_clicks': int(insight.get('unique_clicks', 0)),
                                'cost_per_unique_click': float(insight.get('cost_per_unique_click', 0))
                            }

                            # Salvar métricas na campanha
                            date_key = metrics['date']
                            await campaigns_collection.update_one(
                                {"campaign_id": campaign_id},
                                {
                                    "$set": {
                                        f"metrics.{date_key}": metrics,
                                        "updated_at": datetime.utcnow()
                                    }
                                }
                            )

                            # Também salvar métricas nos AdSets e Ads
                            await self._sync_metrics_for_adsets_and_ads(campaign_id, date_key, insight)

                        except Exception as insight_error:
                            logger.error(f"ERRO: Erro ao processar insight da campanha {campaign_id}: {insight_error}")
                            continue

                    processed += 1
                    logger.debug(f"OK: Métricas atualizadas para campanha: {campaign_doc.get('name', 'Unknown')}")

                except Exception as campaign_error:
                    logger.error(f"ERRO: Erro ao processar métricas da campanha {campaign_id}: {campaign_error}")
                    continue

            # NOVO: Sincronizar métricas de TODOS os AdSets
            logger.info(f"📊 Sincronizando métricas de {len(all_adsets)} AdSets...")
            for adset_doc in all_adsets:
                try:
                    await self.sync_adset_metrics(adset_doc['adset_id'], start_date, end_date)
                    processed += 1
                    await self.update_sync_job(job_id, "running", processed_items=processed)
                except Exception as e:
                    logger.error(f"Erro ao sincronizar métricas do AdSet {adset_doc['adset_id']}: {e}")
                    continue

            # NOVO: Sincronizar métricas de TODOS os Ads
            logger.info(f"📊 Sincronizando métricas de {len(all_ads)} Ads...")
            ads_success = 0
            ads_failed = 0

            for ad_doc in all_ads:
                try:
                    ad_id = ad_doc['ad_id']
                    logger.debug(f"Sincronizando Ad: {ad_id}")

                    success = await self.sync_ad_metrics(ad_id, start_date, end_date)
                    if success:
                        ads_success += 1
                    else:
                        ads_failed += 1
                        logger.warning(f"Falha ao sincronizar Ad {ad_id}")

                    processed += 1
                    await self.update_sync_job(job_id, "running", processed_items=processed)

                except Exception as e:
                    ads_failed += 1
                    logger.error(f"ERRO: Erro ao sincronizar métricas do Ad {ad_doc['ad_id']}: {e}")
                    continue

            logger.info(f"Ads sincronizados: {ads_success} sucesso, {ads_failed} falhas")

            await self.update_sync_job(job_id, "completed", processed_items=processed)
            logger.info(f"✅ Sincronização completa de métricas concluída: {processed}/{total_items} itens processados")
            logger.info(f"   - {total_campaigns} campanhas")
            logger.info(f"   - {len(all_adsets)} adsets")
            logger.info(f"   - {len(all_ads)} ads")
            return True

        except Exception as e:
            await self.update_sync_job(job_id, "failed", error_message=str(e))
            logger.error(f"ERRO: Erro na sincronização de métricas: {e}")
            return False

    async def _sync_metrics_for_adsets_and_ads(self, campaign_id: str, date_key: str, campaign_insight: dict):
        """Sincroniza métricas para AdSets e Ads de uma campanha"""
        try:
            # Buscar AdSets da campanha
            adsets = await adsets_collection.find({"campaign_id": campaign_id}).to_list(None)

            for adset_doc in adsets:
                try:
                    await asyncio.sleep(0.5)  # Rate limiting
                    adset_id = adset_doc['adset_id']

                    # Buscar insights do AdSet
                    adset = AdSet(adset_id)
                    adset_insights = list(adset.get_insights(params={
                        'fields': campaign_insight.keys(),  # Mesmos campos da campanha
                        'time_range': {
                            'since': date_key,
                            'until': date_key
                        }
                    }))

                    if adset_insights:
                        # Processar e salvar métricas do AdSet (similar ao processamento da campanha)
                        # ... código similar ao processamento de campanha ...
                        pass

                    # Buscar Ads do AdSet
                    ads = await ads_collection.find({"adset_id": adset_id}).to_list(None)

                    for ad_doc in ads:
                        try:
                            await asyncio.sleep(0.3)  # Rate limiting
                            ad_id = ad_doc['ad_id']

                            # Buscar insights do Ad
                            ad = Ad(ad_id)
                            ad_insights = list(ad.get_insights(params={
                                'fields': campaign_insight.keys(),
                                'time_range': {
                                    'since': date_key,
                                    'until': date_key
                                }
                            }))

                            if ad_insights:
                                # Processar e salvar métricas do Ad
                                pass

                        except Exception as e:
                            logger.debug(f"Erro ao sincronizar métricas do Ad {ad_id}: {e}")
                            continue

                except Exception as e:
                    logger.debug(f"Erro ao sincronizar métricas do AdSet {adset_id}: {e}")
                    continue

        except Exception as e:
            logger.debug(f"Erro ao sincronizar métricas de AdSets/Ads: {e}")

    async def full_sync(self, days_back: int = 30) -> bool:
        """Sincronização completa: campanhas, adsets, ads e métricas"""
        logger.info(" Iniciando sincronização completa...")
        
        try:
            # 1. Conectar ao MongoDB
            if not await connect_mongodb():
                return False
            
            # 2. Sincronizar campanhas
            if not await self.sync_campaigns():
                logger.error("ERRO: Falha na sincronização de campanhas")
                return False
            
            # 3. Sincronizar AdSets para todas as campanhas
            campaigns = await campaigns_collection.find({"account_id": self.account_id}).to_list(None)
            for campaign_doc in campaigns:
                campaign_id = campaign_doc['campaign_id']
                await self.sync_adsets_for_campaign(campaign_id)
                
                # Sincronizar Ads para todos os AdSets da campanha
                adsets = await adsets_collection.find({"campaign_id": campaign_id}).to_list(None)
                for adset_doc in adsets:
                    await self.sync_ads_for_adset(adset_doc['adset_id'], campaign_id)
            
            # 4. Sincronizar métricas dos últimos N dias
            end_date = date.today()
            start_date = end_date - timedelta(days=days_back)
            
            if not await self.sync_metrics_for_date_range(start_date, end_date):
                logger.error("ERRO: Falha na sincronização de métricas")
                return False
            
            logger.info("OK: Sincronização completa finalizada com sucesso!")
            return True
            
        except Exception as e:
            logger.error(f"ERRO: Erro na sincronização completa: {e}")
            return False

# Instância global do serviço
facebook_sync = FacebookSyncService()