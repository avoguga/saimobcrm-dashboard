from fastapi import APIRouter, HTTPException, Query, BackgroundTasks
from typing import Optional, Dict, Any, List
import asyncio
import logging
from datetime import datetime, timedelta, date
import time
import hashlib
import json
import pandas as pd
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.campaign import Campaign
from facebook_business.adobjects.adset import AdSet
from facebook_business.adobjects.ad import Ad
from facebook_business.exceptions import FacebookRequestError
import config
from .cache_admin import get_redis_client
# from app.services.scheduler import facebook_scheduler

router = APIRouter()
logger = logging.getLogger(__name__)

# Variável global para controlar sincronização
sync_status = {
    "running": False,
    "progress": 0,
    "total": 0,
    "current_campaign": "",
    "total_leads": 0,
    "total_spend": 0.0,
    "start_time": None,
    "errors": []
}

# Redis-based cache for Facebook metrics
class FacebookCache:
    def __init__(self):
        self.redis_client = None
        self.key_prefix = "facebook:"
    
    def _get_redis(self):
        """Get Redis client"""
        if not self.redis_client:
            self.redis_client = get_redis_client()
        return self.redis_client
    
    def _generate_key(self, campaign_id, start_date, end_date, adset_id=None, ad_id=None):
        """Generate cache key from parameters"""
        key_data = f"{campaign_id}_{start_date}_{end_date}_{adset_id}_{ad_id}"
        hash_key = hashlib.md5(key_data.encode()).hexdigest()
        return f"{self.key_prefix}{hash_key}"
    
    def _get_ttl_seconds(self, end_date):
        """Determine TTL based on period type"""
        end_dt = datetime.strptime(end_date, '%Y-%m-%d')
        today = datetime.now().date()
        
        if end_dt.date() < today:
            # Historical data - longer TTL
            return 24 * 60 * 60  # 24 hours
        elif end_dt.date() == today:
            # Current data - shorter TTL
            return 30 * 60  # 30 minutes
        else:
            # Future data - medium TTL
            return 2 * 60 * 60  # 2 hours
    
    def get(self, campaign_id, start_date, end_date, adset_id=None, ad_id=None):
        """Get cached data if available and not expired"""
        try:
            redis_client = self._get_redis()
            if not redis_client:
                logger.warning("Redis not available, cache miss")
                return None
                
            key = self._generate_key(campaign_id, start_date, end_date, adset_id, ad_id)
            cached_data = redis_client.get(key)
            
            if cached_data:
                logger.info(f"Redis Cache HIT for key: {key[:20]}...")
                return json.loads(cached_data)
            
            logger.info(f"Redis Cache MISS for key: {key[:20]}...")
            return None
            
        except Exception as e:
            logger.error(f"Redis cache get error: {e}")
            return None
    
    def set(self, campaign_id, start_date, end_date, data, adset_id=None, ad_id=None):
        """Cache data with TTL"""
        try:
            redis_client = self._get_redis()
            if not redis_client:
                logger.warning("Redis not available, skipping cache")
                return
                
            key = self._generate_key(campaign_id, start_date, end_date, adset_id, ad_id)
            ttl_seconds = self._get_ttl_seconds(end_date)
            
            redis_client.setex(key, ttl_seconds, json.dumps(data))
            logger.info(f"Cached data to Redis: {key[:20]}... (TTL: {ttl_seconds//60}min)")
            
        except Exception as e:
            logger.error(f"Redis cache set error: {e}")
    
    def clear_all(self):
        """Clear all Facebook cache entries"""
        try:
            redis_client = self._get_redis()
            if not redis_client:
                logger.warning("Redis not available")
                return 0
                
            keys = redis_client.keys(f"{self.key_prefix}*")
            if keys:
                deleted = redis_client.delete(*keys)
                logger.info(f"Cleared {deleted} Facebook cache entries")
                return deleted
            return 0
            
        except Exception as e:
            logger.error(f"Redis cache clear error: {e}")
            return 0

# Global cache instance
facebook_cache = FacebookCache()

# Configurações do Facebook (similar ao padrão do projeto)
FACEBOOK_ACCESS_TOKEN = getattr(config, 'FACEBOOK_ACCESS_TOKEN', None)
FACEBOOK_APP_ID = getattr(config, 'FACEBOOK_APP_ID', None)
FACEBOOK_APP_SECRET = getattr(config, 'FACEBOOK_APP_SECRET', None)
DEFAULT_AD_ACCOUNT = getattr(config, 'DEFAULT_FACEBOOK_AD_ACCOUNT', None)

# Função auxiliar global para buscar dados com fallback (similar ao dashboard.py)
def safe_get_facebook_data(func, *args, **kwargs):
    """
    Função auxiliar para buscar dados do Facebook com fallback - Similar ao safe_get_data existente
    """
    try:
        result = func(*args, **kwargs)
        logger.info(f"Safe get Facebook data resultado: {type(result)}, função: {func.__name__ if hasattr(func, '__name__') else 'unknown'}")
        if result is None:
            logger.warning(f"Função {func.__name__ if hasattr(func, '__name__') else 'unknown'} retornou None")
            return {}
        return result
    except FacebookRequestError as fb_error:
        logger.error(f"Erro da API do Facebook em {func.__name__ if hasattr(func, '__name__') else 'unknown'}: {fb_error}")
        return {}
    except Exception as e:
        logger.error(f"Erro ao buscar dados do Facebook de {func.__name__ if hasattr(func, '__name__') else 'unknown'}: {e}")
        return {}

class FacebookDashboardService:
    def __init__(self, access_token: str, app_id: str, app_secret: str = None):
        """
        Inicializa o serviço do Facebook - Similar ao KommoAPI
        """
        try:
            # Inicializar sem app_secret para evitar erro de appsecret_proof
            FacebookAdsApi.init(app_id, None, access_token)
            
            # Inicializar ad_account com ID do config
            from config import settings
            if hasattr(settings, 'FACEBOOK_AD_ACCOUNT_ID') and settings.FACEBOOK_AD_ACCOUNT_ID:
                self.ad_account = AdAccount(f"act_{settings.FACEBOOK_AD_ACCOUNT_ID}")
            else:
                # Fallback: usar ID padrão se não configurado
                self.ad_account = AdAccount("act_1502147036843154")
            
            self.initialized = True
            logger.info("FacebookAdsApi inicializada com sucesso")
        except Exception as e:
            logger.error(f"Erro ao inicializar FacebookAdsApi: {e}")
            self.initialized = False
            self.ad_account = None
    
    def _calculate_time_range(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Calcula período de tempo - Similar ao código existente do dashboard.py
        """
        try:
            if params.get('start_date') and params.get('end_date'):
                # Usar datas específicas
                start_dt = datetime.strptime(params['start_date'], '%Y-%m-%d')
                end_dt = datetime.strptime(params['end_date'], '%Y-%m-%d')
                end_dt = end_dt.replace(hour=23, minute=59, second=59)
            else:
                # Usar período em dias
                days = params.get('days', 7)
                end_dt = datetime.now()
                start_dt = end_dt - timedelta(days=days)
            
            return {
                'since': start_dt.strftime('%Y-%m-%d'),
                'until': end_dt.strftime('%Y-%m-%d'),
                'start_timestamp': int(start_dt.timestamp()),
                'end_timestamp': int(end_dt.timestamp()),
                'start_dt': start_dt,
                'end_dt': end_dt
            }
        except ValueError as date_error:
            logger.error(f"Erro de validação de data: {date_error}")
            raise HTTPException(status_code=400, detail="Formato de data inválido. Use YYYY-MM-DD")
    
    def _calculate_previous_period(self, current_period: Dict[str, Any]) -> Dict[str, Any]:
        """
        Calcula período anterior para comparação
        """
        try:
            start_dt = current_period['start_dt']
            end_dt = current_period['end_dt']
            
            # Calcular duração do período atual
            duration = end_dt - start_dt
            
            # Período anterior: mesmo duration, mas anterior
            prev_end_dt = start_dt - timedelta(seconds=1)
            prev_start_dt = prev_end_dt - duration
            
            return {
                'since': prev_start_dt.strftime('%Y-%m-%d'),
                'until': prev_end_dt.strftime('%Y-%m-%d'),
                'start_timestamp': int(prev_start_dt.timestamp()),
                'end_timestamp': int(prev_end_dt.timestamp()),
                'start_dt': prev_start_dt,
                'end_dt': prev_end_dt
            }
        except Exception as e:
            logger.error(f"Erro ao calcular período anterior: {e}")
            return current_period
    
    # Métodos antigos removidos - usando apenas dashboard-metrics
    
    # Métodos de hierarquia removidos - usar dashboard-metrics com filtros
    
    # Métodos de processamento removidos - código limpo
        """
        Processa dados da conta - Similar ao processamento de leads
        """
        processed_data = {
            'reach': 0,
            'impressions': 0,
            'spend': 0.0,
            'clicks': 0,
            'link_clicks': 0,
            'cpc': 0.0,
            'cpm': 0.0,
            'ctr': 0.0,
            'leads': 0,
            'cost_per_lead': 0.0,
            'page_engagement': 0,
            'reactions': 0,
            'comments': 0,
            'shares': 0
        }
        
        try:
            for insight in insights:
                # Métricas básicas
                processed_data['reach'] += int(insight.get('reach', 0))
                processed_data['impressions'] += int(insight.get('impressions', 0))
                processed_data['spend'] += float(insight.get('spend', 0))
                processed_data['clicks'] += int(insight.get('clicks', 0))
                processed_data['link_clicks'] += int(insight.get('link_clicks', 0))
                
                # Métricas calculadas (pegar a última válida)
                if insight.get('cpc'):
                    processed_data['cpc'] = float(insight.get('cpc', 0))
                if insight.get('cpm'):
                    processed_data['cpm'] = float(insight.get('cpm', 0))
                if insight.get('ctr'):
                    processed_data['ctr'] = float(insight.get('ctr', 0))
                
                # Processar ações (similar ao extract_custom_field_value)
                actions = insight.get('actions', [])
                cost_per_actions = insight.get('cost_per_action_type', [])
                
                for action in actions:
                    action_type = action.get('action_type', '')
                    value = int(action.get('value', 0))
                    
                    if action_type == 'lead':
                        processed_data['leads'] += value
                    elif action_type == 'page_engagement':
                        processed_data['page_engagement'] += value
                    elif action_type == 'post_reaction':
                        processed_data['reactions'] += value
                    elif action_type == 'comment':
                        processed_data['comments'] += value
                    elif action_type == 'post':
                        processed_data['shares'] += value
                
                # Processar custos por ação
                for cost_action in cost_per_actions:
                    if cost_action.get('action_type') == 'lead':
                        processed_data['cost_per_lead'] = float(cost_action.get('value', 0))
                        
        except Exception as e:
            logger.error(f"Erro ao processar insights da conta: {e}")
        
        return processed_data
    
    def _process_demographic_data(self, insights) -> Dict[str, Any]:
        """
        Processa dados demográficos - Similar ao processamento por fonte
        """
        demographic_data = {
            'male': {'leads': 0, 'reach': 0, 'spend': 0.0, 'clicks': 0},
            'female': {'leads': 0, 'reach': 0, 'spend': 0.0, 'clicks': 0},
            'unknown': {'leads': 0, 'reach': 0, 'spend': 0.0, 'clicks': 0}
        }
        
        try:
            for insight in insights:
                gender = insight.get('gender', 'unknown')
                
                if gender not in demographic_data:
                    demographic_data[gender] = {'leads': 0, 'reach': 0, 'spend': 0.0, 'clicks': 0}
                
                demographic_data[gender]['reach'] += int(insight.get('reach', 0))
                demographic_data[gender]['spend'] += float(insight.get('spend', 0))
                demographic_data[gender]['clicks'] += int(insight.get('clicks', 0))
                
                # Extrair leads
                actions = insight.get('actions', [])
                for action in actions:
                    if action.get('action_type') == 'lead':
                        demographic_data[gender]['leads'] += int(action.get('value', 0))
                        
        except Exception as e:
            logger.error(f"Erro ao processar dados demográficos: {e}")
        
        return demographic_data
    
    def _process_campaigns_data(self, insights) -> List[Dict[str, Any]]:
        """
        Processa dados de campanhas
        """
        campaigns_data = []
        
        try:
            for insight in insights:
                campaign_data = {
                    'campaign_name': insight.get('campaign_name', 'N/A'),
                    'reach': int(insight.get('reach', 0)),
                    'impressions': int(insight.get('impressions', 0)),
                    'spend': float(insight.get('spend', 0)),
                    'clicks': int(insight.get('clicks', 0)),
                    'link_clicks': int(insight.get('link_clicks', 0)),
                    'cpc': float(insight.get('cpc', 0)),
                    'cpm': float(insight.get('cpm', 0)),
                    'leads': 0
                }
                
                # Extrair leads
                actions = insight.get('actions', [])
                for action in actions:
                    if action.get('action_type') == 'lead':
                        campaign_data['leads'] = int(action.get('value', 0))
                        break
                
                campaigns_data.append(campaign_data)
                
        except Exception as e:
            logger.error(f"Erro ao processar dados de campanhas: {e}")
        
        return campaigns_data
    
    def _calculate_percentage_changes(self, current_data: Dict[str, Any], previous_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Calcula mudanças percentuais - Similar ao calculate_performance_changes
        """
        changes = {}
        
        for metric in current_data.keys():
            current_value = current_data.get(metric, 0)
            previous_value = previous_data.get(metric, 0)
            
            if previous_value > 0:
                change_percent = ((current_value - previous_value) / previous_value) * 100
                trend = '↗' if change_percent > 0 else '↘' if change_percent < 0 else '→'
            else:
                change_percent = 0
                trend = '→'
            
            changes[metric] = {
                'current': current_value,
                'previous': previous_value,
                'change_percent': round(change_percent, 1),
                'trend': trend
            }
        
        return changes
    
    def _get_empty_metrics(self) -> Dict[str, Any]:
        """
        Retorna métricas vazias em caso de erro
        """
        return {
            'reach': 0,
            'impressions': 0,
            'spend': 0.0,
            'clicks': 0,
            'link_clicks': 0,
            'cpc': 0.0,
            'cpm': 0.0,
            'ctr': 0.0,
            'leads': 0,
            'cost_per_lead': 0.0,
            'page_engagement': 0,
            'reactions': 0,
            'comments': 0,
            'shares': 0,
            'profile_visits': 0,
            'whatsapp_conversations': 0
        }
    
    def _extract_comprehensive_metrics(self, insight: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extrai todas as 12 métricas necessárias do dashboard de um insight
        """
        if not insight:
            return self._get_empty_metrics()
        
        # Métricas básicas do insight
        metrics = {
            'reach': int(insight.get('reach', 0)),
            'impressions': int(insight.get('impressions', 0)),
            'spend': float(insight.get('spend', 0)),
            'clicks': int(insight.get('clicks', 0)),
            'cpc': float(insight.get('cpc', 0)),
            'cpm': float(insight.get('cpm', 0)),
            'ctr': float(insight.get('ctr', 0)),
        }
        
        # Processar actions para métricas específicas
        actions = insight.get('actions', [])
        metrics.update({
            'leads': self._extract_action_value(actions, 'lead'),
            'profile_visits': self._extract_action_value(actions, 'page_view'),
            'whatsapp_conversations': self._extract_messaging_actions(actions),
            'link_clicks': self._extract_link_clicks(insight, actions),
            'page_engagement': self._extract_action_value(actions, 'page_engagement'),
            'reactions': self._extract_action_value(actions, 'post_reaction'),
            'comments': self._extract_action_value(actions, 'comment')
        })
        
        # Processar cost_per_action_type para custo por lead
        cost_per_actions = insight.get('cost_per_action_type', [])
        metrics['cost_per_lead'] = self._extract_cost_per_action(cost_per_actions, 'lead')
        
        return metrics
    
    def _extract_action_value(self, actions: List[Dict], action_type: str) -> int:
        """Extrai valor específico das actions"""
        for action in actions:
            if action.get('action_type') == action_type:
                return int(action.get('value', 0))
        return 0
    
    def _extract_messaging_actions(self, actions: List[Dict]) -> int:
        """Extrai conversações do WhatsApp (messaging actions)"""
        messaging_count = 0
        for action in actions:
            action_type = action.get('action_type', '')
            if 'messaging' in action_type.lower():
                messaging_count += int(action.get('value', 0))
        return messaging_count
    
    def _extract_link_clicks(self, insight: Dict[str, Any], actions: List[Dict]) -> int:
        """
        Extrai cliques no link com fallback
        Priority: actions[link_click] -> insight[link_clicks] -> insight[clicks]
        """
        # Método 1: Buscar nas actions (mais preciso)
        link_clicks = self._extract_action_value(actions, 'link_click')
        if link_clicks > 0:
            return link_clicks
        
        # Método 2: Fallback para field direto
        link_clicks = int(insight.get('link_clicks', 0))
        if link_clicks > 0:
            return link_clicks
        
        # Método 3: Fallback para clicks total
        return int(insight.get('clicks', 0))
    
    def _extract_cost_per_action(self, cost_per_actions: List[Dict], action_type: str) -> float:
        """Extrai custo por ação específica"""
        for cost_action in cost_per_actions:
            if cost_action.get('action_type') == action_type:
                return float(cost_action.get('value', 0))
        return 0.0
    
    def _calculate_percentage_variation(self, current: float, previous: float) -> Dict[str, Any]:
        """Calcula variação percentual entre dois valores"""
        if previous > 0:
            change_percent = ((current - previous) / previous) * 100
            trend = '↗' if change_percent > 0 else '↘' if change_percent < 0 else '→'
        else:
            change_percent = 0 if current == 0 else 100
            trend = '→' if current == 0 else '↗'
        
        return {
            'current': current,
            'previous': previous,
            'change_percent': round(change_percent, 1),
            'trend': trend,
            'formatted': f"{trend} {change_percent:+.1f}%"
        }
    
    async def get_dashboard_metrics_with_cache(
        self, 
        campaign_id: str, 
        start_date: str, 
        end_date: str,
        adset_id: Optional[str] = None,
        ad_id: Optional[str] = None,
        compare_with_previous: bool = True
    ) -> Dict[str, Any]:
        """
        Busca métricas do dashboard com cache inteligente e rate limiting
        """
        try:
            # 1. Verificar cache primeiro
            cached_data = facebook_cache.get(campaign_id, start_date, end_date, adset_id, ad_id)
            if cached_data:
                return cached_data
            
            # 2. Cache Redis gerencia TTL automaticamente
            
            # 3. Buscar dados frescos da API
            logger.info(f"Fetching fresh data for campaign {campaign_id}, period {start_date} to {end_date}")
            
            # Delay para rate limiting
            await asyncio.sleep(2)
            
            current_metrics = await self._fetch_campaign_metrics(
                campaign_id, start_date, end_date, adset_id, ad_id
            )
            
            result_data = {
                'period': {
                    'start_date': start_date,
                    'end_date': end_date,
                    'days': (datetime.strptime(end_date, '%Y-%m-%d') - datetime.strptime(start_date, '%Y-%m-%d')).days + 1
                },
                'metrics': current_metrics,
                'cache_info': {
                    'cached': False,
                    'cache_age': '0 minutes'
                }
            }
            
            # 4. Se comparação solicitada, buscar período anterior
            if compare_with_previous:
                await asyncio.sleep(2)  # Rate limiting
                
                previous_period = self._calculate_previous_period_simple(start_date, end_date)
                previous_metrics = await self._fetch_campaign_metrics(
                    campaign_id, 
                    previous_period['start'], 
                    previous_period['end'], 
                    adset_id, 
                    ad_id
                )
                
                # Calcular variações
                variations = {}
                for metric_name in current_metrics.keys():
                    current_value = current_metrics[metric_name]
                    previous_value = previous_metrics.get(metric_name, 0)
                    variations[f"{metric_name}_variation"] = self._calculate_percentage_variation(
                        current_value, previous_value
                    )
                
                result_data['variations'] = variations
                result_data['previous_period'] = previous_period
                result_data['previous_metrics'] = previous_metrics
            
            # 5. Cachear resultado
            facebook_cache.set(campaign_id, start_date, end_date, result_data, adset_id, ad_id)
            
            return result_data
            
        except Exception as e:
            logger.error(f"Erro ao buscar métricas do dashboard: {e}")
            return {
                'period': {'start_date': start_date, 'end_date': end_date, 'days': 0},
                'metrics': self._get_empty_metrics(),
                'error': str(e)
            }
    
    def _calculate_previous_period_simple(self, start_date: str, end_date: str) -> Dict[str, str]:
        """Calcula período anterior simples"""
        start_dt = datetime.strptime(start_date, '%Y-%m-%d')
        end_dt = datetime.strptime(end_date, '%Y-%m-%d')
        duration = end_dt - start_dt
        
        prev_end_dt = start_dt - timedelta(days=1)
        prev_start_dt = prev_end_dt - duration
        
        return {
            'start': prev_start_dt.strftime('%Y-%m-%d'),
            'end': prev_end_dt.strftime('%Y-%m-%d')
        }
    
    async def _fetch_campaign_metrics(
        self, 
        campaign_id: str, 
        start_date: str, 
        end_date: str,
        adset_id: Optional[str] = None,
        ad_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Busca métricas de campanha/adset/ad com rate limiting
        """
        try:
            # Determinar nível e objeto
            if ad_id:
                fb_object = Ad(ad_id)
                level = 'ad'
            elif adset_id:
                fb_object = AdSet(adset_id)
                level = 'adset'
            else:
                fb_object = Campaign(campaign_id)
                level = 'campaign'
            
            # Parâmetros para insights
            insights_params = {
                'time_range': {
                    'since': start_date,
                    'until': end_date
                },
                'fields': [
                    'reach', 'impressions', 'spend', 'clicks', 
                    'cpc', 'cpm', 'ctr', 'actions', 'cost_per_action_type',
                    'unique_clicks'
                ],
                'level': level
            }
            
            # Buscar insights
            insights = list(fb_object.get_insights(params=insights_params))
            
            if insights:
                return self._extract_comprehensive_metrics(insights[0])
            else:
                logger.warning(f"Nenhum insight encontrado para {level} {campaign_id or adset_id or ad_id}")
                return self._get_empty_metrics()
                
        except FacebookRequestError as fb_error:
            logger.error(f"Facebook API error: {fb_error}")
            if "rate limit" in str(fb_error).lower():
                logger.warning("Rate limit detectado, aguardando...")
                await asyncio.sleep(10)  # Aguardar mais em caso de rate limit
            return self._get_empty_metrics()
        except Exception as e:
            logger.error(f"Erro ao buscar métricas: {e}")
            return self._get_empty_metrics()
    
    async def get_multiple_campaigns_metrics(
        self, 
        campaign_ids: List[str], 
        start_date: str, 
        end_date: str,
        compare_with_previous: bool = True
    ) -> Dict[str, Any]:
        """
        Busca métricas consolidadas de múltiplas campanhas
        """
        try:
            logger.info(f"Fetching metrics for {len(campaign_ids)} campaigns: {campaign_ids}")
            
            # Inicializar métricas consolidadas
            consolidated_metrics = self._get_empty_metrics()
            
            # Buscar métricas de cada campanha
            for campaign_id in campaign_ids:
                await asyncio.sleep(1)  # Rate limiting entre campanhas
                
                campaign_metrics = await self._fetch_campaign_metrics(
                    campaign_id, start_date, end_date
                )
                
                # Consolidar métricas (somar valores)
                for metric, value in campaign_metrics.items():
                    if isinstance(value, (int, float)) and metric in consolidated_metrics:
                        consolidated_metrics[metric] += value
            
            result_data = {
                'period': {
                    'start_date': start_date,
                    'end_date': end_date,
                    'days': (datetime.strptime(end_date, '%Y-%m-%d') - datetime.strptime(start_date, '%Y-%m-%d')).days + 1
                },
                'metrics': consolidated_metrics,
                'cache_info': {
                    'cached': False,
                    'cache_age': '0 minutes'
                },
                'campaigns_included': campaign_ids,
                'total_campaigns': len(campaign_ids)
            }
            
            # Comparação com período anterior se solicitada
            if compare_with_previous:
                await asyncio.sleep(1)  # Rate limiting
                
                previous_period = self._calculate_previous_period_simple(start_date, end_date)
                previous_consolidated = self._get_empty_metrics()
                
                # Buscar período anterior para todas as campanhas
                for campaign_id in campaign_ids:
                    await asyncio.sleep(1)  # Rate limiting
                    
                    previous_metrics = await self._fetch_campaign_metrics(
                        campaign_id, 
                        previous_period['start'], 
                        previous_period['end']
                    )
                    
                    # Consolidar métricas do período anterior
                    for metric, value in previous_metrics.items():
                        if isinstance(value, (int, float)) and metric in previous_consolidated:
                            previous_consolidated[metric] += value
                
                # Calcular variações
                variations = {}
                for metric_name in consolidated_metrics.keys():
                    current_value = consolidated_metrics[metric_name]
                    previous_value = previous_consolidated.get(metric_name, 0)
                    variations[f"{metric_name}_variation"] = self._calculate_percentage_variation(
                        current_value, previous_value
                    )
                
                result_data['variations'] = variations
                result_data['previous_period'] = previous_period
                result_data['previous_metrics'] = previous_consolidated
            
            logger.info(f"Multiple campaigns metrics consolidated: {consolidated_metrics['leads']} leads, R$ {consolidated_metrics['spend']:.2f} spent")
            return result_data
            
        except Exception as e:
            logger.error(f"Erro ao buscar métricas de múltiplas campanhas: {e}")
            return {
                'period': {'start_date': start_date, 'end_date': end_date, 'days': 0},
                'metrics': self._get_empty_metrics(),
                'error': str(e),
                'campaigns_included': campaign_ids
            }

    async def get_all_campaigns_from_accounts(
        self,
        ad_account_ids: List[str],
        start_date: str,
        end_date: str,
        compare_with_previous: bool = True
    ) -> Dict[str, Any]:
        """
        Busca dados de todas as campanhas de múltiplas contas de anúncios
        Retorna dados individuais por campanha + totais consolidados
        """
        try:
            logger.info(f"Fetching all campaigns from {len(ad_account_ids)} ad accounts")
            
            all_campaigns = []
            consolidated_totals = self._get_empty_metrics()
            
            # Para cada conta de anúncio
            for account_id in ad_account_ids:
                try:
                    await asyncio.sleep(2)  # Rate limiting entre contas
                    logger.info(f"Processing account: {account_id}")
                    
                    # Conectar à conta
                    if not account_id.startswith('act_'):
                        account_id = f"act_{account_id}"
                    
                    account = AdAccount(account_id)
                    
                    # Buscar campanhas da conta
                    campaigns_params = {
                        'fields': [
                            'id',
                            'name', 
                            'status',
                            'objective',
                            'created_time',
                            'updated_time'
                        ],
                        'effective_status': ['ACTIVE', 'PAUSED']  # Apenas campanhas ativas/pausadas
                    }
                    
                    campaigns = list(account.get_campaigns(params=campaigns_params))
                    logger.info(f"Found {len(campaigns)} campaigns in account {account_id}")
                    
                    # Processar cada campanha
                    for campaign in campaigns:
                        await asyncio.sleep(1)  # Rate limiting entre campanhas
                        
                        campaign_id = campaign['id']
                        campaign_name = campaign.get('name', 'Unknown')
                        
                        logger.info(f"Processing campaign: {campaign_name} (ID: {campaign_id})")
                        
                        # Buscar métricas da campanha
                        campaign_metrics = await self._fetch_campaign_metrics(
                            campaign_id, start_date, end_date
                        )
                        
                        # Adicionar dados da campanha
                        campaign_data = {
                            'id': campaign_id,
                            'name': campaign_name,
                            'status': campaign.get('status', 'UNKNOWN'),
                            'objective': campaign.get('objective', 'UNKNOWN'),
                            'account_id': account_id,
                            'metrics': campaign_metrics,
                            'created_time': campaign.get('created_time'),
                            'updated_time': campaign.get('updated_time')
                        }
                        
                        all_campaigns.append(campaign_data)
                        
                        # Consolidar nos totais
                        for metric, value in campaign_metrics.items():
                            if isinstance(value, (int, float)) and metric in consolidated_totals:
                                consolidated_totals[metric] += value
                
                except Exception as account_error:
                    logger.error(f"Erro ao processar conta {account_id}: {account_error}")
                    continue
            
            result_data = {
                'success': True,
                'period': {
                    'start_date': start_date,
                    'end_date': end_date,
                    'days': (datetime.strptime(end_date, '%Y-%m-%d') - datetime.strptime(start_date, '%Y-%m-%d')).days + 1
                },
                'campaigns': all_campaigns,
                'totals': consolidated_totals,
                'summary': {
                    'total_campaigns': len(all_campaigns),
                    'accounts_processed': len(ad_account_ids),
                    'total_leads': consolidated_totals.get('leads', 0),
                    'total_spend': consolidated_totals.get('spend', 0),
                    'total_impressions': consolidated_totals.get('impressions', 0),
                    'total_clicks': consolidated_totals.get('clicks', 0)
                }
            }
            
            # Comparação com período anterior se solicitada
            if compare_with_previous and all_campaigns:
                await asyncio.sleep(1)
                
                previous_period = self._calculate_previous_period_simple(start_date, end_date)
                previous_totals = self._get_empty_metrics()
                
                # Buscar período anterior para todas as campanhas
                for campaign_data in all_campaigns:
                    await asyncio.sleep(1)  # Rate limiting
                    
                    previous_metrics = await self._fetch_campaign_metrics(
                        campaign_data['id'], 
                        previous_period['start'], 
                        previous_period['end']
                    )
                    
                    # Adicionar métricas anteriores à campanha
                    campaign_data['previous_metrics'] = previous_metrics
                    
                    # Consolidar nos totais anteriores
                    for metric, value in previous_metrics.items():
                        if isinstance(value, (int, float)) and metric in previous_totals:
                            previous_totals[metric] += value
                
                # Calcular variações totais
                variations = {}
                for metric_name in consolidated_totals.keys():
                    current_value = consolidated_totals[metric_name]
                    previous_value = previous_totals.get(metric_name, 0)
                    variations[f"{metric_name}_variation"] = self._calculate_percentage_variation(
                        current_value, previous_value
                    )
                
                result_data['variations'] = variations
                result_data['previous_period'] = previous_period
                result_data['previous_totals'] = previous_totals
            
            logger.info(f"All campaigns processed: {len(all_campaigns)} campaigns, {consolidated_totals['leads']} total leads, R$ {consolidated_totals['spend']:.2f} total spent")
            return result_data
            
        except Exception as e:
            logger.error(f"Erro ao buscar todas as campanhas: {e}")
            return {
                'success': False,
                'error': str(e),
                'campaigns': [],
                'totals': self._get_empty_metrics(),
                'period': {'start_date': start_date, 'end_date': end_date, 'days': 0}
            }

    async def get_all_campaigns_metrics(
        self, 
        start_date: str, 
        end_date: str,
        compare_with_previous: bool = True
    ) -> Dict[str, Any]:
        """
        Busca métricas de TODAS as campanhas da conta
        """
        try:
            logger.info(f"Fetching ALL campaigns metrics for period {start_date} to {end_date}")
            
            # Buscar TODAS as campanhas da conta
            campaigns = self.ad_account.get_campaigns(
                fields=['id', 'name', 'status'],
                params={'effective_status': ['ACTIVE', 'PAUSED']}
            )
            
            campaign_ids = []
            campaigns_info = []
            
            for campaign in campaigns:
                campaign_ids.append(campaign['id'])
                campaigns_info.append({
                    'id': campaign['id'],
                    'name': campaign.get('name', 'Sem nome'),
                    'status': campaign.get('status', 'UNKNOWN')
                })
            
            logger.info(f"Found {len(campaign_ids)} campaigns in account")
            
            if not campaign_ids:
                return {
                    "success": True,
                    "rawMetrics": self._get_empty_metrics(),
                    "campaigns_list": [],
                    "total_campaigns": 0,
                    "message": "Nenhuma campanha encontrada na conta"
                }
            
            # Usar o método existente para múltiplas campanhas
            metrics_data = await self.get_multiple_campaigns_metrics(
                campaign_ids=campaign_ids,
                start_date=start_date,
                end_date=end_date,
                compare_with_previous=compare_with_previous
            )
            
            # Adicionar lista de campanhas ao resultado
            metrics_data['campaigns_list'] = campaigns_info
            metrics_data['account_id'] = str(self.ad_account.get_id())
            
            logger.info(f"Account overview: {len(campaign_ids)} campaigns, {metrics_data['metrics']['leads']} total leads")
            
            # Formatar resposta similar ao dashboard
            response = {
                "success": True,
                "rawMetrics": metrics_data['metrics'],
                "metricsData": self._format_metrics_array(metrics_data['metrics'], metrics_data.get('variations', {})),
                "campaigns_list": campaigns_info,
                "total_campaigns": len(campaign_ids),
                "period": metrics_data['period'],
                "account_id": metrics_data['account_id']
            }
            
            if compare_with_previous and 'variations' in metrics_data:
                response['variations'] = metrics_data['variations']
                response['previousPeriod'] = metrics_data.get('previous_period')
                response['previousMetrics'] = metrics_data.get('previous_metrics')
            
            return response
            
        except Exception as e:
            logger.error(f"Error fetching all campaigns metrics: {e}")
            return {
                "success": False,
                "error": str(e),
                "rawMetrics": self._get_empty_metrics()
            }
    
    def _format_metrics_array(self, metrics: Dict, variations: Dict) -> List[Dict]:
        """Formata métricas em array para frontend"""
        formatted = []
        
        def get_variation(key):
            var = variations.get(f"{key}_variation", {})
            return {
                "change_percent": var.get('change_percent', 0),
                "trend": var.get('trend', '→'),
                "previous": var.get('previous', 0)
            }
        
        metric_configs = [
            ('leads', 'Total de Leads'),
            ('reach', 'Alcance'),
            ('impressions', 'Impressões'),
            ('spend', 'Valor Investido'),
            ('clicks', 'Cliques'),
            ('link_clicks', 'Cliques no Link'),
            ('cost_per_lead', 'Custo por Lead'),
            ('cpc', 'Custo por Clique'),
            ('cpm', 'CPM')
        ]
        
        for key, label in metric_configs:
            value = metrics.get(key, 0)
            var = get_variation(key)
            
            formatted.append({
                "key": key,
                "label": label,
                "value": value,
                "formatted": f"R$ {value:.2f}" if key in ['spend', 'cost_per_lead', 'cpc', 'cpm'] else str(value),
                "change_percent": var['change_percent'],
                "trend": var['trend'],
                "previous": var['previous']
            })
        
        return formatted
    
    async def get_campaign_structure(self, campaign_id: str) -> Dict[str, Any]:
        """
        Busca estrutura de AdSets e Ads de uma campanha (usando abordagem que funciona)
        """
        try:
            logger.info(f"Fetching campaign structure for {campaign_id}")
            
            # Delay para rate limiting
            await asyncio.sleep(1)
            
            # USAR MESMA ABORDAGEM DO /hierarchy QUE FUNCIONA
            # Campaign().get_ad_sets() em vez de AdAccount().get_ad_sets() com filtro
            campaign = Campaign(campaign_id)
            
            # Buscar AdSets da campanha diretamente
            adsets_params = {
                'fields': ['id', 'name', 'status'],
                'limit': 50
            }
            
            adsets = list(campaign.get_ad_sets(params=adsets_params))
            
            adsets_data = []
            total_ads = 0
            
            # Para cada AdSet, buscar seus Ads
            for adset in adsets:
                await asyncio.sleep(0.5)  # Rate limiting mais suave
                
                # Usar AdSet().get_ads() diretamente 
                adset_obj = AdSet(adset['id'])
                ads_params = {
                    'fields': ['id', 'name', 'status'],
                    'limit': 20
                }
                
                ads = list(adset_obj.get_ads(params=ads_params))
                
                ads_list = []
                for ad in ads:
                    ads_list.append({
                        'id': ad['id'],
                        'name': ad['name'],
                        'status': ad.get('status', 'UNKNOWN')
                    })
                
                adsets_data.append({
                    'id': adset['id'],
                    'name': adset['name'],
                    'status': adset.get('status', 'UNKNOWN'),
                    'ads': ads_list,
                    'ads_count': len(ads_list)
                })
                
                total_ads += len(ads_list)
            
            structure = {
                'campaign_id': campaign_id,
                'adsets': adsets_data,
                'summary': {
                    'total_adsets': len(adsets_data),
                    'total_ads': total_ads
                }
            }
            
            logger.info(f"Campaign structure: {len(adsets_data)} adsets, {total_ads} ads")
            return structure
            
        except Exception as e:
            logger.error(f"Error fetching campaign structure: {e}")
            # Em caso de erro, retornar estrutura vazia
            return {
                'campaign_id': campaign_id,
                'adsets': [],
                'summary': {
                    'total_adsets': 0,
                    'total_ads': 0
                },
                'error': str(e)
            }

    # Métodos antigos removidos - usando apenas dashboard-metrics com cache

# Instanciar serviço uma vez (similar ao kommo_api)
facebook_service = None
if FACEBOOK_ACCESS_TOKEN and FACEBOOK_APP_ID:
    facebook_service = FacebookDashboardService(FACEBOOK_ACCESS_TOKEN, FACEBOOK_APP_ID)
    logger.info("FacebookDashboardService inicializado com sucesso")
else:
    logger.warning("Credenciais do Facebook não configuradas. Serviço não inicializado.")

# Endpoints antigos removidos - usando apenas /dashboard-metrics

@router.get("/dashboard")
async def get_facebook_dashboard(
    campaign_id: str = Query(..., description="ID da campanha OU múltiplas separadas por vírgula"),
    start_date: str = Query(..., description="Data de início (YYYY-MM-DD)"),
    end_date: str = Query(..., description="Data de fim (YYYY-MM-DD)"),
    adset_id: Optional[str] = Query(None, description="ID do conjunto de anúncios (opcional)"),
    ad_id: Optional[str] = Query(None, description="ID do anúncio (opcional)"),
    compare_with_previous: bool = Query(True, description="Incluir comparação com período anterior")
):
    """
    Endpoint único do dashboard Facebook - Retorna TUDO que o frontend precisa:
    
    - Métricas numéricas para gráficos
    - Métricas formatadas para cards
    - Variações percentuais
    - Cache info
    
    Similar ao detailed-tables: um único endpoint com tudo!
    """
    try:
        # Verificar se o serviço está inicializado
        if not facebook_service or not facebook_service.initialized:
            logger.error("Serviço do Facebook não inicializado")
            raise HTTPException(
                status_code=500,
                detail="Serviço do Facebook não configurado. Verifique as credenciais no config."
            )
        
        # Validar datas
        try:
            start_dt = datetime.strptime(start_date, '%Y-%m-%d')
            end_dt = datetime.strptime(end_date, '%Y-%m-%d')
            if start_dt > end_dt:
                raise HTTPException(status_code=400, detail="Data de início não pode ser posterior à data de fim")
        except ValueError:
            raise HTTPException(status_code=400, detail="Formato de data inválido. Use YYYY-MM-DD")
        
        # Detectar se são múltiplas campanhas (separadas por vírgula)
        campaign_ids = [cid.strip() for cid in campaign_id.split(',')]
        is_multiple_campaigns = len(campaign_ids) > 1
        
        if is_multiple_campaigns:
            logger.info(f"Dashboard request - Multiple campaigns: {campaign_ids}, Period: {start_date} to {end_date}")
            # Buscar dados consolidados de múltiplas campanhas
            dashboard_data = await facebook_service.get_multiple_campaigns_metrics(
                campaign_ids=campaign_ids,
                start_date=start_date,
                end_date=end_date,
                compare_with_previous=compare_with_previous
            )
        else:
            logger.info(f"Dashboard request - Single campaign: {campaign_id}, Period: {start_date} to {end_date}")
            # Buscar dados com cache (campanha única)
            dashboard_data = await facebook_service.get_dashboard_metrics_with_cache(
                campaign_id=campaign_id,
                start_date=start_date,
                end_date=end_date,
                adset_id=adset_id,
                ad_id=ad_id,
                compare_with_previous=compare_with_previous
            )
        
        metrics = dashboard_data['metrics']
        variations = dashboard_data.get('variations', {})
        
        # Função auxiliar para formatar número
        def format_number(value):
            if isinstance(value, float):
                if value >= 1000:
                    return f"{value:,.0f}".replace(',', '.')
                else:
                    return f"{value:.2f}"
            return f"{value:,}".replace(',', '.') if value >= 1000 else str(value)
        
        # Função auxiliar para criar objeto de métrica completo
        def create_metric_object(key, label, value, variation_key=None):
            var_data = variations.get(f"{variation_key or key}_variation", {})
            return {
                "label": label,
                "value": value,
                "formatted": format_number(value) if isinstance(value, (int, float)) else value,
                "change_percent": var_data.get('change_percent', 0),
                "trend": var_data.get('trend', '→'),
                "previous": var_data.get('previous', 0),
                "display": f"{label}: {format_number(value) if isinstance(value, (int, float)) else value} {var_data.get('trend', '→')} {var_data.get('change_percent', 0):+.1f}%"
            }
        
        # Criar resposta unificada (similar ao detailed-tables)
        response = {
            "success": True,
            
            # Dados principais organizados
            "metricsData": {
                "leads": create_metric_object('total_leads', 'Total de Leads (META)', metrics['leads'], 'leads'),
                "profile_visits": create_metric_object('profile_visits', 'Total de Visitas ao Perfil', metrics['profile_visits']),
                "whatsapp": create_metric_object('whatsapp_conversations', 'Conversas pelo WhatsApp', metrics['whatsapp_conversations']),
                "reach": create_metric_object('reach', 'Alcance', metrics['reach']),
                "impressions": create_metric_object('impressions', 'Impressões', metrics['impressions']),
                "cost_per_lead": create_metric_object('cost_per_lead', 'Custo por Lead', metrics['cost_per_lead']),
                "cost_per_click": create_metric_object('cpc', 'Custo por Clique', metrics['cpc'], 'cpc'),
                "cpm": create_metric_object('cpm', 'CPM', metrics['cpm']),
                "clicks": create_metric_object('clicks', 'Cliques', metrics['clicks']),
                "link_clicks": create_metric_object('link_clicks', 'Cliques no Link', metrics['link_clicks']),
                "total_spent": create_metric_object('spend', 'Valor Investido', metrics['spend'], 'spend'),
                "page_engagement": create_metric_object('page_engagement', 'Engajamento com a Página', metrics['page_engagement']),
                "reactions": create_metric_object('reactions', 'Reações', metrics['reactions']),
                "comments": create_metric_object('comments', 'Comentários', metrics['comments'])
            },
            
            # Valores brutos para gráficos
            "rawMetrics": metrics,
            
            # Array formatado para exibição direta
            "formattedMetrics": [
                f"Total de Leads (META): {format_number(metrics['leads'])} {variations.get('leads_variation', {}).get('trend', '→')} {variations.get('leads_variation', {}).get('change_percent', 0):+.1f}%",
                f"Total de Visitas ao Perfil: {format_number(metrics['profile_visits'])} {variations.get('profile_visits_variation', {}).get('trend', '→')} {variations.get('profile_visits_variation', {}).get('change_percent', 0):+.1f}%",
                f"Conversas pelo WhatsApp: {format_number(metrics['whatsapp_conversations'])} {variations.get('whatsapp_conversations_variation', {}).get('trend', '→')} {variations.get('whatsapp_conversations_variation', {}).get('change_percent', 0):+.1f}%",
                f"Alcance: {format_number(metrics['reach'])} {variations.get('reach_variation', {}).get('trend', '→')} {variations.get('reach_variation', {}).get('change_percent', 0):+.1f}%",
                f"Impressões: {format_number(metrics['impressions'])} {variations.get('impressions_variation', {}).get('trend', '→')} {variations.get('impressions_variation', {}).get('change_percent', 0):+.1f}%",
                f"Custo por Lead: R$ {metrics['cost_per_lead']:.2f} {variations.get('cost_per_lead_variation', {}).get('trend', '→')} {variations.get('cost_per_lead_variation', {}).get('change_percent', 0):+.1f}%",
                f"Custo por Clique: R$ {metrics['cpc']:.2f} {variations.get('cpc_variation', {}).get('trend', '→')} {variations.get('cpc_variation', {}).get('change_percent', 0):+.1f}%",
                f"CPM: R$ {metrics['cpm']:.2f} {variations.get('cpm_variation', {}).get('trend', '→')} {variations.get('cpm_variation', {}).get('change_percent', 0):+.1f}%",
                f"Cliques: {format_number(metrics['clicks'])} {variations.get('clicks_variation', {}).get('trend', '→')} {variations.get('clicks_variation', {}).get('change_percent', 0):+.1f}%",
                f"Cliques no Link: {format_number(metrics['link_clicks'])} {variations.get('link_clicks_variation', {}).get('trend', '→')} {variations.get('link_clicks_variation', {}).get('change_percent', 0):+.1f}%",
                f"Valor Investido: R$ {metrics['spend']:.2f} {variations.get('spend_variation', {}).get('trend', '→')} {variations.get('spend_variation', {}).get('change_percent', 0):+.1f}%",
                f"Engajamento com a Página: {format_number(metrics['page_engagement'])} {variations.get('page_engagement_variation', {}).get('trend', '→')} {variations.get('page_engagement_variation', {}).get('change_percent', 0):+.1f}%",
                f"Reações: {format_number(metrics['reactions'])} {variations.get('reactions_variation', {}).get('trend', '→')} {variations.get('reactions_variation', {}).get('change_percent', 0):+.1f}%",
                f"Comentários: {format_number(metrics['comments'])} {variations.get('comments_variation', {}).get('trend', '→')} {variations.get('comments_variation', {}).get('change_percent', 0):+.1f}%"
            ],
            
            # Informações do período
            "period": dashboard_data['period'],
            
            # Informações do cache
            "cacheInfo": dashboard_data.get('cache_info', {'cached': False}),
            
            # Meta informações
            "meta": {
                "campaign_id": campaign_id,
                "adset_id": adset_id,
                "ad_id": ad_id,
                "level": "ad" if ad_id else "adset" if adset_id else "campaign",
                "comparison_enabled": compare_with_previous,
                "total_days": dashboard_data['period']['days']
            },
            
            # Período anterior (se comparação habilitada)
            "previousPeriod": dashboard_data.get('previous_period') if compare_with_previous else None,
            
            # Métricas anteriores (se comparação habilitada)
            "previousMetrics": dashboard_data.get('previous_metrics') if compare_with_previous else None
        }
        
        # Adicionar estrutura de AdSets e Ads se campanha única
        if not is_multiple_campaigns:
            try:
                structure = await facebook_service.get_campaign_structure(campaign_id)
                response["structure"] = structure
                logger.info(f"Added campaign structure: {structure['summary']['total_adsets']} adsets, {structure['summary']['total_ads']} ads")
            except Exception as e:
                logger.warning(f"Failed to fetch campaign structure: {e}")
                response["structure"] = {
                    'campaign_id': campaign_id,
                    'adsets': [],
                    'summary': {'total_adsets': 0, 'total_ads': 0},
                    'error': str(e)
                }
        else:
            # Para múltiplas campanhas, estrutura não é fornecida (muito complexa)
            response["structure"] = {
                'multiple_campaigns': True,
                'message': 'Structure not available for multiple campaigns'
            }
        
        logger.info(f"Dashboard delivered successfully - {metrics['leads']} leads, R$ {metrics['spend']:.2f} spent")
        return response
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro no endpoint dashboard: {e}")
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")

@router.get("/account-overview")
async def get_account_overview(
    start_date: str = Query(..., description="Data de início (YYYY-MM-DD)"),
    end_date: str = Query(..., description="Data de fim (YYYY-MM-DD)"),
    compare_with_previous: bool = Query(True, description="Incluir comparação com período anterior")
):
    """
    Retorna métricas consolidadas de TODAS as campanhas da conta
    Ideal para visão geral do dashboard sem filtros
    """
    try:
        if not facebook_service or not facebook_service.initialized:
            raise HTTPException(status_code=500, detail="Serviço do Facebook não configurado")
        
        logger.info(f"Account overview request - Period: {start_date} to {end_date}")
        
        # Buscar TODAS as campanhas da conta
        all_campaigns_data = await facebook_service.get_all_campaigns_metrics(
            start_date=start_date,
            end_date=end_date,
            compare_with_previous=compare_with_previous
        )
        
        return all_campaigns_data
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro no endpoint account-overview: {e}")
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")

@router.get("/hierarchy")
async def get_facebook_hierarchy(
    campaign_id: str = Query(..., description="ID da campanha"),
    start_date: str = Query(..., description="Data de início (YYYY-MM-DD)"),
    end_date: str = Query(..., description="Data de fim (YYYY-MM-DD)"),
    include_metrics: bool = Query(True, description="Incluir métricas de cada item")
):
    """
    Retorna hierarquia completa: Campanha -> AdSets -> Ads
    Para o frontend fazer filtros por campanha/adset/ad
    """
    try:
        if not facebook_service or not facebook_service.initialized:
            raise HTTPException(status_code=500, detail="Serviço do Facebook não configurado")
        
        logger.info(f"Fetching hierarchy for campaign {campaign_id}")
        
        # Buscar campanha
        campaign = Campaign(campaign_id)
        campaign_info = campaign.api_get(fields=['name', 'status', 'objective'])
        
        result = {
            "success": True,
            "campaign": {
                "id": campaign_id,
                "name": campaign_info.get('name', 'N/A'),
                "status": campaign_info.get('status', 'N/A'),
                "objective": campaign_info.get('objective', 'N/A'),
                "adsets": []
            },
            "summary": {
                "total_adsets": 0,
                "total_ads": 0
            }
        }
        
        # Buscar todos os AdSets da campanha
        adsets = campaign.get_ad_sets(fields=['name', 'status', 'campaign_id'])
        
        for adset in adsets:
            adset_data = {
                "id": adset.get('id'),
                "name": adset.get('name'),
                "status": adset.get('status'),
                "ads": []
            }
            
            # Buscar todos os Ads do AdSet
            ads = AdSet(adset.get('id')).get_ads(fields=['name', 'status', 'adset_id'])
            
            for ad in ads:
                ad_data = {
                    "id": ad.get('id'),
                    "name": ad.get('name'),
                    "status": ad.get('status')
                }
                
                # Incluir métricas se solicitado
                if include_metrics:
                    await asyncio.sleep(0.5)  # Rate limiting
                    ad_metrics = await facebook_service._fetch_campaign_metrics(
                        campaign_id, start_date, end_date, adset.get('id'), ad.get('id')
                    )
                    ad_data["metrics"] = {
                        "leads": ad_metrics.get('leads', 0),
                        "reach": ad_metrics.get('reach', 0),
                        "spend": ad_metrics.get('spend', 0),
                        "clicks": ad_metrics.get('clicks', 0)
                    }
                
                adset_data["ads"].append(ad_data)
            
            # Incluir métricas do AdSet se solicitado
            if include_metrics:
                await asyncio.sleep(0.5)  # Rate limiting
                adset_metrics = await facebook_service._fetch_campaign_metrics(
                    campaign_id, start_date, end_date, adset.get('id')
                )
                adset_data["metrics"] = {
                    "leads": adset_metrics.get('leads', 0),
                    "reach": adset_metrics.get('reach', 0),
                    "spend": adset_metrics.get('spend', 0),
                    "clicks": adset_metrics.get('clicks', 0)
                }
            
            result["campaign"]["adsets"].append(adset_data)
            result["summary"]["total_ads"] += len(adset_data["ads"])
        
        result["summary"]["total_adsets"] = len(result["campaign"]["adsets"])
        
        logger.info(f"Hierarchy loaded: {result['summary']['total_adsets']} adsets, {result['summary']['total_ads']} ads")
        return result
        
    except Exception as e:
        logger.error(f"Erro ao buscar hierarquia: {e}")
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")

@router.get("/account")
async def get_facebook_account_overview(
    start_date: str = Query(..., description="Data de início (YYYY-MM-DD)"),
    end_date: str = Query(..., description="Data de fim (YYYY-MM-DD)"),
    compare_with_previous: bool = Query(True, description="Incluir comparação com período anterior"),
    ad_account_id: Optional[str] = Query(None, description="ID da conta publicitária (opcional)")
):
    """
    Retorna TODAS as métricas da conta publicitária Facebook - SEM filtro por campanha
    
    Dados consolidados de TODAS as campanhas, adsets e ads da conta.
    Similar ao dashboard geral do Facebook Ads Manager.
    """
    try:
        if not facebook_service or not facebook_service.initialized:
            raise HTTPException(status_code=500, detail="Serviço do Facebook não configurado")
        
        # Usar conta padrão se não especificada
        account_id = ad_account_id or DEFAULT_AD_ACCOUNT
        if not account_id:
            raise HTTPException(status_code=400, detail="ID da conta publicitária não configurado. Configure DEFAULT_FACEBOOK_AD_ACCOUNT no config.py")
        
        # Garantir formato correto do ID (act_XXXXXXX)
        if not account_id.startswith('act_'):
            account_id = f'act_{account_id}'
        
        logger.info(f"Fetching account overview for {account_id}, period {start_date} to {end_date}")
        
        # Buscar dados da conta publicitária
        account_data = await facebook_service._fetch_account_overview(
            account_id=account_id,
            start_date=start_date,
            end_date=end_date,
            compare_with_previous=compare_with_previous
        )
        
        return account_data
        
    except Exception as e:
        logger.error(f"Erro no endpoint account overview: {e}")
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")

@router.get("/unified-data")
async def get_unified_facebook_data(
    start_date: str = Query(..., description="Data de início (YYYY-MM-DD)"),
    end_date: str = Query(..., description="Data de fim (YYYY-MM-DD)"),
    campaign_id: Optional[str] = Query(None, description="Filtrar por campanha específica"),
    adset_id: Optional[str] = Query(None, description="Filtrar por adset específico"),
    ad_id: Optional[str] = Query(None, description="Filtrar por ad específico"),
    status_filter: Optional[str] = Query(None, description="Filtrar por status: ACTIVE, PAUSED")
):
    """
    ENDPOINT ÚNICO E DEFINITIVO - Retorna TODOS os dados do Facebook

    ✅ Usa MongoDB (sem rate limit!)
    ✅ Cache Redis para performance
    ✅ Todas as métricas necessárias
    ✅ Filtros por campanha/adset/ad

    Métricas retornadas:
    - PRINCIPAIS: leads, profile_visits, whatsapp_conversations
    - PERFORMANCE: reach, impressions, cost_per_lead, cpc, cpm, clicks, link_clicks, spend
    - ENGAJAMENTO: page_engagement, reactions, comments, shares

    Estrutura hierárquica completa:
    Campaign -> AdSets -> Ads com todas as métricas
    """
    import hashlib
    import json

    try:
        from app.models.facebook_models import campaigns_collection, adsets_collection, ads_collection
        from datetime import datetime, date

        logger.info(f"🚀 Buscando dados unificados para período {start_date} a {end_date}")

        # Gerar chave de cache baseada nos parâmetros
        cache_key = f"facebook:unified:{hashlib.md5(f'{start_date}_{end_date}_{campaign_id}_{adset_id}_{ad_id}_{status_filter}'.encode()).hexdigest()}"

        # Verificar cache Redis primeiro
        redis_client = get_redis_client()
        if redis_client:
            cached_data = redis_client.get(cache_key)
            if cached_data:
                logger.info(f"✅ Cache HIT para unified-data")
                result = json.loads(cached_data)
                result['cache_info'] = {
                    'from_cache': True,
                    'cache_key': cache_key[:20] + '...',
                    'ttl_seconds': redis_client.ttl(cache_key)
                }
                return result

        # Validar datas
        try:
            start_dt = datetime.strptime(start_date, '%Y-%m-%d').date()
            end_dt = datetime.strptime(end_date, '%Y-%m-%d').date()
            if start_dt > end_dt:
                raise HTTPException(status_code=400, detail="Data de início não pode ser posterior à data de fim")
        except ValueError:
            raise HTTPException(status_code=400, detail="Formato de data inválido. Use YYYY-MM-DD")

        # Construir filtros
        campaign_filter = {"account_id": "act_1051414772388438"}
        if campaign_id:
            campaign_filter["campaign_id"] = campaign_id
        if status_filter:
            campaign_filter["status"] = status_filter

        # Buscar campanhas do MongoDB
        campaigns_cursor = campaigns_collection.find(campaign_filter)
        campaigns_data = await campaigns_cursor.to_list(None)

        logger.info(f"📊 Encontradas {len(campaigns_data)} campanhas no MongoDB")

        if not campaigns_data:
            # Retornar estrutura vazia mas válida
            empty_result = {
                "success": True,
                "message": "Nenhuma campanha encontrada. Execute /facebook/sync-data primeiro.",
                "campaigns": [],
                "totals": {
                    'leads': 0, 'profile_visits': 0, 'whatsapp_conversations': 0,
                    'reach': 0, 'impressions': 0, 'cost_per_lead': 0,
                    'cpc': 0, 'cpm': 0, 'clicks': 0, 'link_clicks': 0, 'spend': 0,
                    'page_engagement': 0, 'reactions': 0, 'comments': 0, 'shares': 0
                },
                "summary": {
                    "total_campaigns": 0,
                    "total_adsets": 0,
                    "total_ads": 0,
                    "period_days": (end_dt - start_dt).days + 1
                },
                "cache_info": {"from_cache": False},
                "sync_required": True
            }
            return empty_result

        # Processar campanhas com hierarquia completa
        result_campaigns = []
        consolidated_totals = {
            'leads': 0, 'profile_visits': 0, 'whatsapp_conversations': 0,
            'reach': 0, 'impressions': 0, 'cost_per_lead': 0,
            'cpc': 0, 'cpm': 0, 'clicks': 0, 'link_clicks': 0, 'spend': 0,
            'page_engagement': 0, 'reactions': 0, 'comments': 0, 'shares': 0,
            'ctr': 0, 'video_views': 0, 'unique_clicks': 0
        }

        total_adsets = 0
        total_ads = 0

        for campaign_doc in campaigns_data:
            campaign_id_current = campaign_doc['campaign_id']

            # Calcular métricas da campanha para o período
            campaign_metrics = _calculate_comprehensive_metrics(
                campaign_doc.get('metrics', {}), start_dt, end_dt
            )

            # Buscar AdSets da campanha
            adset_filter = {"campaign_id": campaign_id_current}
            if status_filter:
                adset_filter["status"] = status_filter
            if adset_id:
                adset_filter["adset_id"] = adset_id

            adsets_cursor = adsets_collection.find(adset_filter)
            adsets_data = await adsets_cursor.to_list(None)

            total_adsets += len(adsets_data)

            # Processar AdSets
            result_adsets = []
            for adset_doc in adsets_data:
                adset_id_current = adset_doc['adset_id']

                # Calcular métricas do AdSet
                # Para AdSets individuais, as métricas já estão consolidadas
                raw_adset_metrics = adset_doc.get('metrics', {})
                if isinstance(raw_adset_metrics, dict) and 'leads' in raw_adset_metrics:
                    # Métricas já consolidadas (individuais)
                    adset_metrics = _normalize_individual_metrics(raw_adset_metrics)
                else:
                    # Métricas por data (campanhas)
                    adset_metrics = _calculate_comprehensive_metrics(raw_adset_metrics, start_dt, end_dt)

                # Buscar Ads do AdSet
                ads_filter = {"adset_id": adset_id_current}
                if status_filter:
                    ads_filter["status"] = status_filter
                if ad_id:
                    ads_filter["ad_id"] = ad_id

                ads_cursor = ads_collection.find(ads_filter)
                ads_data = await ads_cursor.to_list(None)

                total_ads += len(ads_data)

                # Processar Ads
                result_ads = []
                for ad_doc in ads_data:
                    # Para Ads individuais, as métricas já estão consolidadas
                    raw_ad_metrics = ad_doc.get('metrics', {})
                    if isinstance(raw_ad_metrics, dict) and 'leads' in raw_ad_metrics:
                        # Métricas já consolidadas (individuais)
                        ad_metrics = _normalize_individual_metrics(raw_ad_metrics)
                    else:
                        # Métricas por data
                        ad_metrics = _calculate_comprehensive_metrics(raw_ad_metrics, start_dt, end_dt)

                    result_ads.append({
                        'id': ad_doc['ad_id'],
                        'name': ad_doc['name'],
                        'status': ad_doc['status'],
                        'metrics': ad_metrics,
                        'last_sync': ad_doc.get('last_sync')
                    })

                result_adsets.append({
                    'id': adset_doc['adset_id'],
                    'name': adset_doc['name'],
                    'status': adset_doc['status'],
                    'daily_budget': adset_doc.get('daily_budget'),
                    'lifetime_budget': adset_doc.get('lifetime_budget'),
                    'metrics': adset_metrics,
                    'ads': result_ads,
                    'ads_count': len(result_ads),
                    'last_sync': adset_doc.get('last_sync')
                })

            result_campaigns.append({
                'id': campaign_doc['campaign_id'],
                'name': campaign_doc['name'],
                'status': campaign_doc['status'],
                'objective': campaign_doc['objective'],
                'account_id': campaign_doc['account_id'],
                'metrics': campaign_metrics,
                'adsets': result_adsets,
                'adsets_count': len(result_adsets),
                'last_sync': campaign_doc.get('last_sync')
            })

            # Consolidar totais
            for metric, value in campaign_metrics.items():
                if isinstance(value, (int, float)) and metric in consolidated_totals:
                    if metric in ['cpc', 'cpm', 'ctr', 'cost_per_lead']:
                        # Para médias, vamos recalcular depois
                        continue
                    consolidated_totals[metric] += value

        # Calcular médias corretamente
        if consolidated_totals['clicks'] > 0:
            consolidated_totals['cpc'] = consolidated_totals['spend'] / consolidated_totals['clicks']
        if consolidated_totals['impressions'] > 0:
            consolidated_totals['cpm'] = (consolidated_totals['spend'] / consolidated_totals['impressions']) * 1000
            consolidated_totals['ctr'] = (consolidated_totals['clicks'] / consolidated_totals['impressions']) * 100
        if consolidated_totals['leads'] > 0:
            consolidated_totals['cost_per_lead'] = consolidated_totals['spend'] / consolidated_totals['leads']

        logger.info(f"✅ Dados processados com sucesso: {len(result_campaigns)} campanhas")

        result = {
            "success": True,
            "period": {
                "start_date": start_date,
                "end_date": end_date,
                "days": (end_dt - start_dt).days + 1
            },
            "campaigns": result_campaigns,
            "totals": consolidated_totals,
            "summary": {
                "total_campaigns": len(result_campaigns),
                "total_adsets": total_adsets,
                "total_ads": total_ads,
                "total_leads": consolidated_totals.get('leads', 0),
                "total_spend": consolidated_totals.get('spend', 0),
                "data_source": "MongoDB"
            },
            "filters_applied": {
                "campaign_id": campaign_id,
                "adset_id": adset_id,
                "ad_id": ad_id,
                "status_filter": status_filter
            },
            "cache_info": {
                "from_cache": False,
                "cache_key": cache_key[:20] + '...'
            }
        }

        # Salvar no cache Redis (TTL de 5 minutos para dados recentes)
        if redis_client:
            ttl = 300  # 5 minutos
            redis_client.setex(cache_key, ttl, json.dumps(result, default=str))
            result['cache_info']['ttl_seconds'] = ttl
            logger.info(f"💾 Dados salvos no cache Redis com TTL de {ttl} segundos")

        return result

    except Exception as e:
        logger.error(f"❌ Erro no endpoint unified-data: {e}")
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")

def _calculate_comprehensive_metrics(metrics_dict: dict, start_date: date, end_date: date) -> dict:
    """Calcula TODAS as métricas necessárias para um período"""
    consolidated = {
        # Métricas principais
        'leads': 0, 'profile_visits': 0, 'whatsapp_conversations': 0,
        # Métricas de performance
        'reach': 0, 'impressions': 0, 'cost_per_lead': 0.0,
        'cpc': 0.0, 'cpm': 0.0, 'clicks': 0, 'link_clicks': 0, 'spend': 0.0,
        # Métricas de engajamento
        'page_engagement': 0, 'reactions': 0, 'comments': 0, 'shares': 0,
        # Métricas adicionais
        'ctr': 0.0, 'video_views': 0, 'unique_clicks': 0, 'cost_per_unique_click': 0.0
    }

    current_date = start_date
    days_with_data = 0

    while current_date <= end_date:
        date_key = current_date.strftime('%Y-%m-%d')

        if date_key in metrics_dict:
            day_metrics = metrics_dict[date_key]
            days_with_data += 1

            # Somar métricas absolutas
            for metric in ['leads', 'profile_visits', 'whatsapp_conversations',
                          'reach', 'impressions', 'clicks', 'link_clicks', 'spend',
                          'page_engagement', 'reactions', 'comments', 'shares',
                          'video_views', 'unique_clicks']:
                consolidated[metric] += day_metrics.get(metric, 0)

            # Acumular para médias
            consolidated['cpc'] += day_metrics.get('cpc', 0)
            consolidated['cpm'] += day_metrics.get('cpm', 0)
            consolidated['ctr'] += day_metrics.get('ctr', 0)
            consolidated['cost_per_lead'] += day_metrics.get('cost_per_lead', 0)
            consolidated['cost_per_unique_click'] += day_metrics.get('cost_per_unique_click', 0)

        current_date += timedelta(days=1)

    # Calcular médias
    if days_with_data > 0:
        consolidated['cpc'] = consolidated['cpc'] / days_with_data
        consolidated['cpm'] = consolidated['cpm'] / days_with_data
        consolidated['ctr'] = consolidated['ctr'] / days_with_data
        consolidated['cost_per_lead'] = consolidated['cost_per_lead'] / days_with_data
        consolidated['cost_per_unique_click'] = consolidated['cost_per_unique_click'] / days_with_data

    return consolidated

def _normalize_individual_metrics(metrics_dict: dict) -> dict:
    """Normaliza métricas individuais já consolidadas de AdSets/Ads"""
    # Retorna as métricas já consolidadas, garantindo que todos os campos existam
    normalized = {
        # Métricas principais
        'leads': metrics_dict.get('leads', 0),
        'profile_visits': metrics_dict.get('profile_visits', 0),
        'whatsapp_conversations': metrics_dict.get('whatsapp_conversations', 0),
        # Métricas de performance
        'reach': metrics_dict.get('reach', 0),
        'impressions': metrics_dict.get('impressions', 0),
        'cost_per_lead': metrics_dict.get('cost_per_lead', 0.0),
        'cpc': metrics_dict.get('cpc', 0.0),
        'cpm': metrics_dict.get('cpm', 0.0),
        'clicks': metrics_dict.get('clicks', 0),
        'link_clicks': metrics_dict.get('link_clicks', 0),
        'spend': metrics_dict.get('spend', 0.0),
        # Métricas de engajamento
        'page_engagement': metrics_dict.get('page_engagement', 0),
        'reactions': metrics_dict.get('reactions', 0),
        'comments': metrics_dict.get('comments', 0),
        'shares': metrics_dict.get('shares', 0),
        # Métricas adicionais
        'ctr': metrics_dict.get('ctr', 0.0),
        'video_views': metrics_dict.get('video_views', 0),
        'unique_clicks': metrics_dict.get('unique_clicks', 0),
        'cost_per_unique_click': metrics_dict.get('cost_per_unique_click', 0.0)
    }

    return normalized

# ========================================
# ENDPOINTS DO SCHEDULER AUTOMÁTICO
# ========================================

@router.post("/scheduler/start")
async def start_daily_scheduler():
    """
    Inicia o agendamento automático de sincronização diária

    ✅ Executa sincronização completa às 1:00 AM todos os dias
    ✅ Sincroniza estrutura + métricas de todas as campanhas
    ✅ Sincroniza hierarquia completa para campanhas com leads
    """
    try:
        # Import aqui para evitar problemas de circular import
        from app.services.scheduler import facebook_scheduler

        success = facebook_scheduler.start_scheduler()

        if success:
            status = facebook_scheduler.get_status()
            return {
                "success": True,
                "message": "Scheduler iniciado com sucesso",
                "next_sync": status["next_sync"],
                "schedule": "Todos os dias às 1:00 AM",
                "status": status
            }
        else:
            return {
                "success": False,
                "message": "Scheduler já está rodando",
                "status": facebook_scheduler.get_status()
            }

    except Exception as e:
        logger.error(f"Erro ao iniciar scheduler: {e}")
        return {
            "success": False,
            "message": f"Erro ao iniciar scheduler: {str(e)}"
        }

@router.post("/scheduler/stop")
async def stop_daily_scheduler():
    """Para o agendamento automático de sincronização"""
    try:
        from app.services.scheduler import facebook_scheduler

        success = facebook_scheduler.stop_scheduler()

        return {
            "success": success,
            "message": "Scheduler parado" if success else "Scheduler não estava rodando",
            "status": facebook_scheduler.get_status()
        }

    except Exception as e:
        logger.error(f"Erro ao parar scheduler: {e}")
        return {
            "success": False,
            "message": f"Erro ao parar scheduler: {str(e)}"
        }

@router.get("/scheduler/status")
async def get_scheduler_status():
    """
    Retorna o status atual do scheduler automático

    Informações retornadas:
    - Se o scheduler está rodando
    - Última sincronização
    - Próxima sincronização agendada
    - Totais da última sincronização
    - Erros recentes
    """
    try:
        from app.services.scheduler import facebook_scheduler

        status = facebook_scheduler.get_status()

        return {
            "success": True,
            "scheduler": status,
            "schedule_info": {
                "frequency": "Diário",
                "time": "1:00 AM",
                "timezone": "Local",
                "description": "Sincronização completa de todas as campanhas Facebook"
            }
        }

    except Exception as e:
        logger.error(f"Erro ao obter status do scheduler: {e}")
        return {
            "success": False,
            "message": f"Erro ao obter status: {str(e)}"
        }

@router.post("/scheduler/run-now")
async def run_manual_sync():
    """
    Executa sincronização manual imediatamente

    ⚠️  Esta operação pode demorar 30-60 minutos para completar
    ✅ Sincroniza TODAS as campanhas + AdSets + Ads
    ✅ Últimos 30 dias de métricas
    """
    try:
        from app.services.scheduler import facebook_scheduler

        # Verificar se já está rodando
        status = facebook_scheduler.get_status()

        if status["sync_running"]:
            return {
                "success": False,
                "message": "Sincronização já está em execução",
                "status": status
            }

        # Executar sincronização em background
        import asyncio
        asyncio.create_task(facebook_scheduler.run_manual_sync())

        return {
            "success": True,
            "message": "Sincronização manual iniciada",
            "estimated_duration": "30-60 minutos",
            "note": "Use /scheduler/status para acompanhar o progresso"
        }

    except Exception as e:
        logger.error(f"Erro ao executar sincronização manual: {e}")
        return {
            "success": False,
            "message": f"Erro ao executar sincronização: {str(e)}"
        }

@router.get("/complete-data")
async def get_complete_facebook_data(
    start_date: str = Query(..., description="Data de início (YYYY-MM-DD)"),
    end_date: str = Query(..., description="Data de fim (YYYY-MM-DD)"),
    campaign_id: Optional[str] = Query(None, description="Filtrar por campanha específica"),
    adset_id: Optional[str] = Query(None, description="Filtrar por adset específico"),
    status_filter: Optional[str] = Query(None, description="Filtrar por status: ACTIVE, PAUSED")
):
    """
    ENDPOINT ÚNICO - Retorna dados completos do Facebook do MongoDB
    
    SUPER RÁPIDO - sem rate limiting da API do Facebook!
    
    Funcionalidades:
    - Lista TODAS as campanhas com AdSets e Ads
    - Métricas consolidadas por período
    - Filtros por campanha, adset, status
    - Hierarquia completa: Campaign -> AdSets -> Ads
    - Totais consolidados
    
    Estrutura de resposta:
    {
        "success": true,
        "campaigns": [
            {
                "id": "123",
                "name": "Campaign Name", 
                "status": "ACTIVE",
                "metrics": {...},
                "adsets": [
                    {
                        "id": "456",
                        "name": "AdSet Name",
                        "metrics": {...},
                        "ads": [
                            {"id": "789", "name": "Ad Name", "metrics": {...}}
                        ]
                    }
                ]
            }
        ],
        "totals": {"leads": 143, "spend": 1234.56, ...},
        "summary": {"total_campaigns": 25, "total_adsets": 89, "total_ads": 234}
    }
    """
    try:
        from app.models.facebook_models import campaigns_collection, adsets_collection, ads_collection
        from datetime import datetime, date
        import logging
        
        logger.info(f"🚀 Buscando dados completos do MongoDB para período {start_date} a {end_date}")
        
        # Validar datas
        try:
            start_dt = datetime.strptime(start_date, '%Y-%m-%d').date()
            end_dt = datetime.strptime(end_date, '%Y-%m-%d').date()
            if start_dt > end_dt:
                raise HTTPException(status_code=400, detail="Data de início não pode ser posterior à data de fim")
        except ValueError:
            raise HTTPException(status_code=400, detail="Formato de data inválido. Use YYYY-MM-DD")
        
        # Construir filtros
        campaign_filter = {"account_id": "act_1051414772388438"}
        if campaign_id:
            campaign_filter["campaign_id"] = campaign_id
        if status_filter:
            campaign_filter["status"] = status_filter
        
        # Buscar campanhas do MongoDB
        campaigns_cursor = campaigns_collection.find(campaign_filter)
        campaigns_data = await campaigns_cursor.to_list(None)
        
        logger.info(f"📊 Encontradas {len(campaigns_data)} campanhas no MongoDB")
        
        if not campaigns_data:
            return {
                "success": True,
                "message": "Nenhuma campanha encontrada. Execute a sincronização primeiro.",
                "campaigns": [],
                "totals": {},
                "summary": {"total_campaigns": 0, "total_adsets": 0, "total_ads": 0},
                "sync_required": True
            }
        
        # Processar campanhas com hierarquia completa
        result_campaigns = []
        consolidated_totals = {
            'leads': 0, 'spend': 0.0, 'impressions': 0, 'clicks': 0, 'reach': 0,
            'cpc': 0.0, 'cpm': 0.0, 'ctr': 0.0, 'cpp': 0.0,
            'video_views': 0, 'actions_like': 0, 'link_clicks': 0
        }
        
        total_adsets = 0
        total_ads = 0
        
        for campaign_doc in campaigns_data:
            campaign_id_current = campaign_doc['campaign_id']
            
            # Calcular métricas da campanha para o período
            campaign_metrics = _calculate_metrics_for_period(campaign_doc.get('metrics', {}), start_dt, end_dt)
            
            # Buscar AdSets da campanha
            adset_filter = {"campaign_id": campaign_id_current}
            if status_filter:
                adset_filter["status"] = status_filter
            if adset_id:
                adset_filter["adset_id"] = adset_id
            
            adsets_cursor = adsets_collection.find(adset_filter)
            adsets_data = await adsets_cursor.to_list(None)
            
            total_adsets += len(adsets_data)
            
            # Processar AdSets
            result_adsets = []
            for adset_doc in adsets_data:
                adset_id_current = adset_doc['adset_id']
                
                # Calcular métricas do AdSet para o período
                adset_metrics = _calculate_metrics_for_period(adset_doc.get('metrics', {}), start_dt, end_dt)
                
                # Buscar Ads do AdSet
                ads_filter = {"adset_id": adset_id_current}
                if status_filter:
                    ads_filter["status"] = status_filter
                
                ads_cursor = ads_collection.find(ads_filter)
                ads_data = await ads_cursor.to_list(None)
                
                total_ads += len(ads_data)
                
                # Processar Ads
                result_ads = []
                for ad_doc in ads_data:
                    ad_metrics = _calculate_metrics_for_period(ad_doc.get('metrics', {}), start_dt, end_dt)
                    
                    result_ads.append({
                        'id': ad_doc['ad_id'],
                        'name': ad_doc['name'],
                        'status': ad_doc['status'],
                        'metrics': ad_metrics,
                        'last_sync': ad_doc.get('last_sync')
                    })
                
                result_adsets.append({
                    'id': adset_doc['adset_id'],
                    'name': adset_doc['name'],
                    'status': adset_doc['status'],
                    'daily_budget': adset_doc.get('daily_budget'),
                    'lifetime_budget': adset_doc.get('lifetime_budget'),
                    'metrics': adset_metrics,
                    'ads': result_ads,
                    'ads_count': len(result_ads),
                    'last_sync': adset_doc.get('last_sync')
                })
            
            result_campaigns.append({
                'id': campaign_doc['campaign_id'],
                'name': campaign_doc['name'],
                'status': campaign_doc['status'],
                'objective': campaign_doc['objective'],
                'account_id': campaign_doc['account_id'],
                'metrics': campaign_metrics,
                'adsets': result_adsets,
                'adsets_count': len(result_adsets),
                'last_sync': campaign_doc.get('last_sync')
            })
            
            # Consolidar totais
            for metric, value in campaign_metrics.items():
                if isinstance(value, (int, float)) and metric in consolidated_totals:
                    consolidated_totals[metric] += value
        
        # Calcular médias para métricas que precisam ser médias
        if len(result_campaigns) > 0:
            consolidated_totals['cpc'] = consolidated_totals['spend'] / max(consolidated_totals['clicks'], 1)
            consolidated_totals['cpm'] = (consolidated_totals['spend'] / max(consolidated_totals['impressions'], 1)) * 1000
            consolidated_totals['ctr'] = (consolidated_totals['clicks'] / max(consolidated_totals['impressions'], 1)) * 100
        
        logger.info(f"✅ Dados processados: {len(result_campaigns)} campanhas, {total_adsets} adsets, {total_ads} ads")
        logger.info(f"📈 Total leads encontrados: {consolidated_totals['leads']}")
        
        return {
            "success": True,
            "period": {
                "start_date": start_date,
                "end_date": end_date,
                "days": (end_dt - start_dt).days + 1
            },
            "campaigns": result_campaigns,
            "totals": consolidated_totals,
            "summary": {
                "total_campaigns": len(result_campaigns),
                "total_adsets": total_adsets,
                "total_ads": total_ads,
                "total_leads": consolidated_totals.get('leads', 0),
                "total_spend": consolidated_totals.get('spend', 0),
                "data_source": "MongoDB (sincronizado)"
            },
            "filters_applied": {
                "campaign_id": campaign_id,
                "adset_id": adset_id,
                "status_filter": status_filter
            }
        }
        
    except Exception as e:
        logger.error(f"❌ Erro no endpoint complete-data: {e}")
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")

def _calculate_metrics_for_period(metrics_dict: dict, start_date: date, end_date: date) -> dict:
    """Calcula métricas consolidadas para um período específico"""
    consolidated = {
        'leads': 0, 'spend': 0.0, 'impressions': 0, 'clicks': 0, 'reach': 0,
        'cpc': 0.0, 'cpm': 0.0, 'ctr': 0.0, 'cpp': 0.0,
        'video_views': 0, 'actions_like': 0, 'link_clicks': 0
    }
    
    current_date = start_date
    days_with_data = 0
    
    while current_date <= end_date:
        date_key = current_date.strftime('%Y-%m-%d')
        
        if date_key in metrics_dict:
            day_metrics = metrics_dict[date_key]
            days_with_data += 1
            
            # Somar métricas absolutas
            for metric in ['leads', 'spend', 'impressions', 'clicks', 'reach', 'video_views', 'actions_like', 'link_clicks']:
                consolidated[metric] += day_metrics.get(metric, 0)
            
            # Somar métricas de custo/taxa para calcular média depois
            consolidated['cpc'] += day_metrics.get('cpc', 0)
            consolidated['cpm'] += day_metrics.get('cpm', 0)
            consolidated['ctr'] += day_metrics.get('ctr', 0)
            consolidated['cpp'] += day_metrics.get('cpp', 0)
        
        current_date += timedelta(days=1)
    
    # Calcular médias
    if days_with_data > 0:
        consolidated['cpc'] = consolidated['cpc'] / days_with_data
        consolidated['cpm'] = consolidated['cpm'] / days_with_data  
        consolidated['ctr'] = consolidated['ctr'] / days_with_data
        consolidated['cpp'] = consolidated['cpp'] / days_with_data
    
    return consolidated

@router.post("/sync-data")
async def sync_facebook_data(
    days_back: int = Query(30, description="Quantos dias buscar (padrão: 30)")
):
    """
    Endpoint para sincronizar dados do Facebook com MongoDB
    
    Executa sincronização completa:
    - Campanhas da conta act_1051414772388438
    - AdSets de todas as campanhas
    - Ads de todos os AdSets  
    - Métricas dos últimos N dias
    
    Use este endpoint para atualizar os dados antes de usar /complete-data
    """
    try:
        from app.services.facebook_sync import facebook_sync
        
        logger.info(f"🚀 Iniciando sincronização com {days_back} dias de histórico")
        
        # Executar sincronização completa
        success = await facebook_sync.full_sync(days_back=days_back)
        
        if success:
            return {
                "success": True,
                "message": f"Sincronização completa executada com sucesso para {days_back} dias",
                "next_step": "Use o endpoint /complete-data para buscar os dados sincronizados"
            }
        else:
            raise HTTPException(
                status_code=500, 
                detail="Falha na sincronização. Verifique os logs para mais detalhes."
            )
            
    except Exception as e:
        logger.error(f"❌ Erro na sincronização: {e}")
        raise HTTPException(status_code=500, detail=f"Erro na sincronização: {str(e)}")

async def sync_all_campaigns_background():
    """Função para sincronizar todas as campanhas em background"""
    global sync_status

    try:
        from app.services.facebook_sync import facebook_sync
        from app.models.facebook_models import connect_mongodb, campaigns_collection

        # Conectar
        if not await connect_mongodb():
            sync_status["errors"].append("Falha ao conectar MongoDB")
            return

        if not facebook_sync.initialize_api():
            sync_status["errors"].append("Falha ao inicializar API Facebook")
            return

        # Configuração conservadora
        facebook_sync.min_request_interval = 8

        # Buscar campanhas
        campaigns = await campaigns_collection.find().to_list(None)
        sync_status["total"] = len(campaigns)

        # Período - últimos 30 dias
        end_date = date.today()
        start_date = end_date - timedelta(days=30)

        logger.info(f"Iniciando sincronização de {len(campaigns)} campanhas")

        for i, campaign in enumerate(campaigns):
            if not sync_status["running"]:  # Parar se cancelado
                break

            campaign_id = campaign['campaign_id']
            campaign_name = campaign.get('name', 'N/A')[:50]

            sync_status["progress"] = i + 1
            sync_status["current_campaign"] = campaign_name

            try:
                success = await facebook_sync.sync_metrics_for_date_range_single_campaign(
                    campaign_id, start_date, end_date
                )

                if success:
                    # Verificar dados sincronizados
                    updated_campaign = await campaigns_collection.find_one({"campaign_id": campaign_id})
                    if updated_campaign and updated_campaign.get('metrics'):
                        metrics = updated_campaign['metrics']
                        leads = sum(day.get('leads', 0) for day in metrics.values())
                        spend = sum(day.get('spend', 0) for day in metrics.values())

                        sync_status["total_leads"] += leads
                        sync_status["total_spend"] += spend

                        logger.info(f"[{i+1}/{len(campaigns)}] {campaign_name}: {leads} leads, R$ {spend:.2f}")

            except Exception as e:
                error_msg = f"Erro na campanha {campaign_name}: {str(e)}"
                sync_status["errors"].append(error_msg)
                logger.error(error_msg)

            # Pausa entre campanhas
            await asyncio.sleep(10)

        sync_status["running"] = False
        logger.info(f"Sincronização concluída: {sync_status['total_leads']} leads, R$ {sync_status['total_spend']:.2f}")

    except Exception as e:
        sync_status["running"] = False
        sync_status["errors"].append(f"Erro geral: {str(e)}")
        logger.error(f"Erro na sincronização: {e}")

@router.post("/sync-all-campaigns")
async def start_sync_all_campaigns(background_tasks: BackgroundTasks):
    """
    Inicia sincronização de TODAS as campanhas em background

    Esta rota:
    - Sincroniza todas as 118 campanhas gradualmente
    - Respeita rate limits (8s entre requests)
    - Roda em background
    - Permite monitoramento via /sync-status
    """
    global sync_status

    if sync_status["running"]:
        raise HTTPException(
            status_code=400,
            detail="Sincronização já está rodando. Use /sync-status para monitorar."
        )

    # Resetar status
    sync_status = {
        "running": True,
        "progress": 0,
        "total": 0,
        "current_campaign": "",
        "total_leads": 0,
        "total_spend": 0.0,
        "start_time": datetime.now().isoformat(),
        "errors": []
    }

    # Iniciar sincronização em background
    background_tasks.add_task(sync_all_campaigns_background)

    return {
        "success": True,
        "message": "Sincronização iniciada em background",
        "status": "Use /facebook/sync-status para monitorar o progresso",
        "estimated_time": "~40 minutos para 118 campanhas"
    }

@router.get("/sync-status")
async def get_sync_status():
    """
    Monitora o progresso da sincronização

    Retorna:
    - Status atual (rodando/parado)
    - Progresso (campanhas processadas)
    - Campanha atual sendo processada
    - Total de leads e gastos sincronizados
    - Erros encontrados
    """
    global sync_status

    status = sync_status.copy()

    if status["start_time"]:
        start_time = datetime.fromisoformat(status["start_time"])
        elapsed = datetime.now() - start_time
        status["elapsed_time"] = str(elapsed).split('.')[0]  # Remove microseconds

        if status["progress"] > 0 and status["running"]:
            campaigns_per_minute = status["progress"] / (elapsed.total_seconds() / 60)
            remaining_campaigns = status["total"] - status["progress"]
            eta_minutes = remaining_campaigns / campaigns_per_minute if campaigns_per_minute > 0 else 0
            status["eta"] = f"{eta_minutes:.0f} minutos restantes"

    return status

@router.post("/sync-stop")
async def stop_sync():
    """
    Para a sincronização em andamento
    """
    global sync_status

    if not sync_status["running"]:
        raise HTTPException(status_code=400, detail="Nenhuma sincronização em andamento")

    sync_status["running"] = False

    return {
        "success": True,
        "message": "Sincronização interrompida",
        "final_stats": {
            "campanhas_processadas": sync_status["progress"],
            "total_leads": sync_status["total_leads"],
            "total_spend": sync_status["total_spend"]
        }
    }

# =============================================================================
# SCHEDULER ENDPOINTS - Sistema de Agendamento Automático
# =============================================================================

@router.get("/scheduler/status")
async def get_scheduler_status():
    """
    Obtém status atual do scheduler de sincronização automática

    Retorna:
    - scheduler_running: Se o scheduler está ativo
    - sync_running: Se há sincronização em andamento
    - last_sync: Última sincronização executada
    - next_sync: Próxima sincronização agendada
    - total_campaigns: Total de campanhas sincronizadas
    - total_leads: Total de leads sincronizados
    - total_spend: Total gasto sincronizado
    - errors: Últimos erros encontrados
    """
    try:
        from app.services.scheduler import facebook_scheduler
        status = facebook_scheduler.get_status()

        return {
            "success": True,
            "message": "Status do scheduler obtido com sucesso",
            "scheduler": status
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao obter status: {str(e)}")

@router.post("/scheduler/start")
async def start_daily_scheduler():
    """
    Inicia o scheduler para sincronização automática diária às 1:00 AM

    O scheduler irá:
    - Executar sincronização completa todos os dias às 1:00 AM
    - Sincronizar campanhas, AdSets, Ads e métricas dos últimos 30 dias
    - Aplicar rate limiting conservador para evitar bloqueios
    - Armazenar dados em MongoDB para consultas rápidas
    """
    try:
        from app.services.scheduler import facebook_scheduler

        if facebook_scheduler.running:
            return {
                "success": True,
                "message": "Scheduler já está ativo",
                "next_sync": facebook_scheduler.get_status()["next_sync"]
            }

        success = facebook_scheduler.start_scheduler()

        if success:
            return {
                "success": True,
                "message": "Scheduler iniciado com sucesso - sincronização diária às 1:00 AM",
                "next_sync": facebook_scheduler.get_status()["next_sync"]
            }
        else:
            raise HTTPException(status_code=500, detail="Falha ao iniciar scheduler")

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao iniciar scheduler: {str(e)}")

@router.post("/scheduler/stop")
async def stop_daily_scheduler():
    """
    Para o scheduler de sincronização automática

    Cancela todas as execuções agendadas e para o processo do scheduler.
    Sincronizações em andamento não são interrompidas.
    """
    try:
        from app.services.scheduler import facebook_scheduler

        if not facebook_scheduler.running:
            return {
                "success": True,
                "message": "Scheduler já está parado"
            }

        success = facebook_scheduler.stop_scheduler()

        if success:
            return {
                "success": True,
                "message": "Scheduler parado com sucesso"
            }
        else:
            raise HTTPException(status_code=500, detail="Falha ao parar scheduler")

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao parar scheduler: {str(e)}")

@router.post("/scheduler/run-now")
async def run_manual_sync():
    """
    Executa sincronização manual imediatamente (fora do agendamento)

    Executa uma sincronização completa dos últimos 30 dias:
    - Campanhas: estrutura e métricas
    - AdSets: estrutura e métricas (apenas campanhas com leads)
    - Ads: estrutura e métricas (apenas campanhas com leads)

    Duração estimada: 30-60 minutos dependendo do número de campanhas
    """
    try:
        from app.services.scheduler import facebook_scheduler

        if facebook_scheduler.sync_status["running"]:
            raise HTTPException(
                status_code=400,
                detail="Sincronização já está em andamento. Use /scheduler/status para monitorar."
            )

        # Executar sincronização em background
        import asyncio
        asyncio.create_task(facebook_scheduler.run_manual_sync())

        return {
            "success": True,
            "message": "Sincronização manual iniciada em background",
            "estimated_duration": "30-60 minutos",
            "monitor_url": "/facebook/scheduler/status",
            "note": "Use o endpoint /scheduler/status para monitorar o progresso"
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao executar sincronização manual: {str(e)}")

# Fim do arquivo