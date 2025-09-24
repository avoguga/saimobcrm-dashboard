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

# Configurações Redis Cache
try:
    import redis
    from config import REDIS_URL, CACHE_TTL

    if REDIS_URL:
        redis_client = redis.from_url(REDIS_URL, decode_responses=True)
        logger.info("✓ Redis conectado para cache Facebook")
    else:
        redis_client = None
        logger.warning("Redis URL não configurada - cache desabilitado")
except Exception as e:
    redis_client = None
    logger.warning(f"Erro ao conectar Redis: {e} - cache desabilitado")

class FacebookCache:
    """Cache Redis para dados do Facebook"""

    def __init__(self):
        self.redis = redis_client
        self.ttl = getattr(config, 'CACHE_TTL', 600)  # 10 minutos default

    def _get_cache_key(self, key_parts: list) -> str:
        """Gera chave do cache"""
        return f"facebook:{':'.join(str(part) for part in key_parts)}"

    def get(self, key_parts: list):
        """Busca dados do cache"""
        if not self.redis:
            return None

        try:
            key = self._get_cache_key(key_parts)
            data = self.redis.get(key)
            if data:
                return json.loads(data)
        except Exception as e:
            logger.warning(f"Erro ao ler cache: {e}")
        return None

    def set(self, key_parts: list, data, ttl=None):
        """Salva dados no cache"""
        if not self.redis:
            return False

        try:
            key = self._get_cache_key(key_parts)
            ttl = ttl or self.ttl
            self.redis.setex(key, ttl, json.dumps(data, default=str))
            return True
        except Exception as e:
            logger.warning(f"Erro ao salvar cache: {e}")
        return False

    def delete(self, key_parts: list):
        """Remove dados do cache"""
        if not self.redis:
            return False

        try:
            key = self._get_cache_key(key_parts)
            self.redis.delete(key)
            return True
        except Exception as e:
            logger.warning(f"Erro ao deletar cache: {e}")
        return False

    def clear_all(self):
        """Limpa todo o cache do Facebook"""
        if not self.redis:
            return False

        try:
            keys = self.redis.keys("facebook:*")
            if keys:
                self.redis.delete(*keys)
                logger.info(f"Cache limpo: {len(keys)} chaves removidas")
            return True
        except Exception as e:
            logger.warning(f"Erro ao limpar cache: {e}")
        return False

# Cache global
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

                    # APENAS offsite_complete_registration_add_meta_leads conforme relatórios
                    if action_type == 'offsite_complete_registration_add_meta_leads':
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
                    action_type = action.get('action_type', '')
                    if action_type == 'offsite_complete_registration_add_meta_leads':
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
                
                # APENAS offsite_complete_registration_add_meta_leads conforme relatórios
                actions = insight.get('actions', [])
                leads_total = 0
                for action in actions:
                    action_type = action.get('action_type', '')
                    if action_type == 'offsite_complete_registration_add_meta_leads':
                        leads_total += int(action.get('value', 0))

                campaign_data['leads'] = leads_total
                
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
        # APENAS offsite_complete_registration_add_meta_leads conforme relatórios
        leads_total = 0
        for action in actions:
            action_type = action.get('action_type', '')
            if action_type == 'offsite_complete_registration_add_meta_leads':
                leads_total += int(action.get('value', 0))

        metrics.update({
            'leads': leads_total,
            'offsite_registrations': self._extract_action_value(actions, 'offsite_complete_registration_add_meta_leads'),
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
            if 'messaging' in action_type.lower() and not action_type.startswith('onsite_conversion'):
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
            # Buscar dados diretos do MongoDB
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
            
            # Cache removido - dados diretos do MongoDB
            
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
        from app.models.facebook_models import campaigns_collection, adsets_collection, ads_collection, connect_mongodb
        from datetime import datetime, date

        logger.info(f"🚀 Buscando dados unificados para período {start_date} a {end_date}")
        logger.info("DEBUG: Inicio da funcao unified-data")

        # Conectar ao MongoDB
        await connect_mongodb()

        # Verificar cache primeiro (incluir demographics na chave do cache)
        cache_key_parts = ["unified", start_date, end_date, campaign_id or "all", adset_id or "all", ad_id or "all", status_filter or "all", "with_demographics"]
        cached_data = facebook_cache.get(cache_key_parts)

        if cached_data:
            logger.info("✓ Dados encontrados no cache Redis")
            cached_data["data_source"] = "Cache Redis"
            return cached_data

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
        campaigns_data = await campaigns_collection.find(campaign_filter).to_list(None)

        logger.info(f"📊 Encontradas {len(campaigns_data)} campanhas no MongoDB")

        # DEBUG: Log das campanhas encontradas
        if len(campaigns_data) > 1:
            logger.info(f"🔍 DEBUG: Campanhas encontradas:")
            for i, camp in enumerate(campaigns_data[:3]):  # Mostrar primeiras 3
                logger.info(f"   {i+1}. {camp['campaign_id']} - {camp.get('name', 'N/A')[:50]}")
            if len(campaigns_data) > 3:
                logger.info(f"   ... e mais {len(campaigns_data) - 3} campanhas")

        if not campaigns_data:
            # Retornar estrutura vazia mas válida
            empty_result = {
                "success": True,
                "message": "Nenhuma campanha encontrada. Execute /facebook/sync-data primeiro.",
                "campaigns": [],
                "totals": {
                    'leads': 0, 'offsite_registrations': 0, 'profile_visits': 0, 'whatsapp_conversations': 0,
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
            'leads': 0, 'offsite_registrations': 0, 'profile_visits': 0, 'whatsapp_conversations': 0,
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

            adsets_data = await adsets_collection.find(adset_filter).to_list(None)

            total_adsets += len(adsets_data)

            # Processar AdSets
            result_adsets = []
            for adset_doc in adsets_data:
                adset_id_current = adset_doc['adset_id']

                # Calcular métricas do AdSet
                # Para AdSets individuais, as métricas já estão consolidadas
                raw_adset_metrics = adset_doc.get('metrics', {})

                # CORREÇÃO: Forçar consistência de período
                if isinstance(raw_adset_metrics, dict) and raw_adset_metrics:
                    # Verificar se é métrica por data vs métrica consolidada
                    # Métrica por data: chaves são datas como '2025-08-16'
                    # Métrica consolidada: chaves são nomes de métricas como 'leads', 'spend'
                    sample_key = list(raw_adset_metrics.keys())[0] if raw_adset_metrics else ""
                    is_date_based = bool(sample_key and '-' in str(sample_key) and len(str(sample_key)) == 10)

                    if is_date_based:
                        # Métricas por data - pode filtrar por período
                        adset_metrics = _calculate_comprehensive_metrics(raw_adset_metrics, start_dt, end_dt)
                        logger.info(f"✅ AdSet {adset_id_current}: Usando métricas por data (período respeitado)")
                    else:
                        # Métricas consolidadas - INCONSISTÊNCIA DE PERÍODO
                        logger.warning(f"⚠️  AdSet {adset_id_current}: Usando métricas consolidadas (podem incluir dados fora do período {start_date} a {end_date})")
                        adset_metrics = _normalize_individual_metrics(raw_adset_metrics)
                else:
                    # Sem métricas
                    adset_metrics = _normalize_individual_metrics({})

                # Buscar Ads do AdSet
                ads_filter = {"adset_id": adset_id_current}
                if status_filter:
                    ads_filter["status"] = status_filter
                if ad_id:
                    ads_filter["ad_id"] = ad_id

                ads_data = await ads_collection.find(ads_filter).to_list(None)

                total_ads += len(ads_data)

                # Processar Ads
                result_ads = []
                for ad_doc in ads_data:
                    # Para Ads individuais, as métricas já estão consolidadas
                    raw_ad_metrics = ad_doc.get('metrics', {})

                    # CORREÇÃO: Forçar consistência de período
                    if isinstance(raw_ad_metrics, dict) and raw_ad_metrics:
                        # Verificar se é métrica por data vs métrica consolidada
                        sample_key = list(raw_ad_metrics.keys())[0] if raw_ad_metrics else ""
                        is_date_based = bool(sample_key and '-' in str(sample_key) and len(str(sample_key)) == 10)

                        if is_date_based:
                            # Métricas por data - pode filtrar por período
                            ad_metrics = _calculate_comprehensive_metrics(raw_ad_metrics, start_dt, end_dt)
                            logger.info(f"✅ Ad {ad_doc['ad_id']}: Usando métricas por data (período respeitado)")
                        else:
                            # Métricas consolidadas - INCONSISTÊNCIA DE PERÍODO
                            logger.warning(f"⚠️  Ad {ad_doc['ad_id']}: Usando métricas consolidadas (podem incluir dados fora do período {start_date} a {end_date})")
                            ad_metrics = _normalize_individual_metrics(raw_ad_metrics)
                    else:
                        # Sem métricas
                        ad_metrics = _normalize_individual_metrics({})

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

        logger.info(f"📈 DEBUG: Totais finais consolidados: {consolidated_totals['leads']} leads, R$ {consolidated_totals['spend']:.2f}")
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
            "data_source": "MongoDB"
        }

        logger.info("DEBUG: Chegou antes da secao de demographics")
        # SEMPRE buscar dados demográficos por gênero
        logger.info("DEBUG: Iniciando busca de demographics por gênero...")

        # FORÇAR um campo de teste para verificar se esta linha é executada
        result["DEBUG_demographics_processing"] = True

        # Converter campanhas para formato esperado (limitar a 5 para teste)
        campaigns_list = [{"id": c["id"], "name": c["name"]} for c in result_campaigns[:5]]
        logger.info(f"DEBUG: Processando demographics para {len(campaigns_list)} campanhas")

        # Buscar demographics usando Facebook API
        try:
            logger.info("DEBUG: Chamando get_gender_demographics_direct...")
            demographics_result = await get_gender_demographics_direct(campaigns_list, start_dt, end_dt)
            logger.info("DEBUG: Demographics obtidas com sucesso")
        except Exception as e:
            logger.error(f"DEBUG: Erro ao buscar demographics: {e}")
            demographics_result = {
                "error": str(e),
                "male": {"leads": 0, "spend": 0, "impressions": 0, "reach": 0, "clicks": 0},
                "female": {"leads": 0, "spend": 0, "impressions": 0, "reach": 0, "clicks": 0},
                "unknown": {"leads": 0, "spend": 0, "impressions": 0, "reach": 0, "clicks": 0},
                "summary": {"total_leads": 0, "total_spend": 0, "male_percentage": 0, "female_percentage": 0, "unknown_percentage": 0}
            }

        # Adicionar demographics ao resultado
        logger.info("DEBUG: Adicionando demographics ao resultado...")
        result["gender_demographics"] = demographics_result
        logger.info(f"DEBUG: Result keys após adicionar demographics: {list(result.keys())}")

        # Salvar no cache antes de retornar
        facebook_cache.set(cache_key_parts, result, ttl=600)  # 10 minutos
        logger.info("✓ Dados salvos no cache Redis")

        return result

    except Exception as e:
        logger.error(f"❌ Erro no endpoint unified-data: {e}")
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")

@router.delete("/cache/clear")
async def clear_facebook_cache():
    """Limpa todo o cache do Facebook"""
    try:
        success = facebook_cache.clear_all()
        if success:
            return {
                "success": True,
                "message": "Cache do Facebook limpo com sucesso"
            }
        else:
            return {
                "success": False,
                "message": "Cache não disponível ou erro ao limpar"
            }
    except Exception as e:
        logger.error(f"Erro ao limpar cache: {e}")
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")

@router.get("/cache/status")
async def get_cache_status():
    """Verifica status do cache Redis"""
    try:
        if not facebook_cache.redis:
            return {
                "cache_enabled": False,
                "message": "Redis não conectado"
            }

        # Testar conexão
        try:
            facebook_cache.redis.ping()
            keys_count = len(facebook_cache.redis.keys("facebook:*"))

            return {
                "cache_enabled": True,
                "redis_connected": True,
                "facebook_keys_count": keys_count,
                "ttl_default": facebook_cache.ttl
            }
        except Exception as e:
            return {
                "cache_enabled": False,
                "redis_connected": False,
                "error": str(e)
            }

    except Exception as e:
        logger.error(f"Erro ao verificar status do cache: {e}")
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")

def _calculate_comprehensive_metrics(metrics_dict: dict, start_date: date, end_date: date) -> dict:
    """Calcula TODAS as métricas necessárias para um período"""
    consolidated = {
        # Métricas principais
        'leads': 0, 'offsite_registrations': 0, 'profile_visits': 0, 'whatsapp_conversations': 0,
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
            for metric in ['leads', 'offsite_registrations', 'profile_visits', 'whatsapp_conversations',
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

async def get_gender_demographics_direct(campaigns: List[Dict], start_date: date, end_date: date) -> Dict[str, Any]:
    """
    Busca dados demográficos por gênero usando a Facebook Marketing API
    """
    try:
        if not facebook_service or not facebook_service.initialized:
            logger.warning("Facebook service não inicializado para breakdown demográfico")
            return {
                "error": "Facebook service não disponível",
                "male": {"leads": 0, "spend": 0, "impressions": 0, "reach": 0, "clicks": 0},
                "female": {"leads": 0, "spend": 0, "impressions": 0, "reach": 0, "clicks": 0},
                "unknown": {"leads": 0, "spend": 0, "impressions": 0, "reach": 0, "clicks": 0},
                "summary": {
                    "total_leads": 0,
                    "total_spend": 0,
                    "male_percentage": 0,
                    "female_percentage": 0,
                    "unknown_percentage": 0
                }
            }

        demographics_data = {
            "male": {"leads": 0, "spend": 0, "impressions": 0, "reach": 0, "clicks": 0},
            "female": {"leads": 0, "spend": 0, "impressions": 0, "reach": 0, "clicks": 0},
            "unknown": {"leads": 0, "spend": 0, "impressions": 0, "reach": 0, "clicks": 0}
        }

        # Buscar insights com breakdown por gênero para cada campanha
        for campaign in campaigns:
            try:
                await asyncio.sleep(2)  # Rate limiting

                campaign_id = campaign['id']
                campaign_obj = Campaign(campaign_id)

                # Buscar insights com breakdown por gênero
                insights = list(campaign_obj.get_insights(params={
                    'fields': [
                        'spend', 'impressions', 'clicks', 'reach', 'actions'
                    ],
                    'breakdowns': ['gender'],
                    'time_range': {
                        'since': start_date.strftime('%Y-%m-%d'),
                        'until': end_date.strftime('%Y-%m-%d')
                    }
                }))

                # Processar insights por gênero
                for insight in insights:
                    gender = insight.get('gender', 'unknown')

                    # Normalizar gêneros
                    if gender not in demographics_data:
                        gender = 'unknown'

                    # Métricas básicas
                    demographics_data[gender]['spend'] += float(insight.get('spend', 0))
                    demographics_data[gender]['impressions'] += int(insight.get('impressions', 0))
                    demographics_data[gender]['clicks'] += int(insight.get('clicks', 0))
                    demographics_data[gender]['reach'] += int(insight.get('reach', 0))

                    # Extrair leads das actions
                    actions = insight.get('actions', [])
                    for action in actions:
                        if action.get('action_type') == 'offsite_complete_registration_add_meta_leads':
                            demographics_data[gender]['leads'] += int(action.get('value', 0))

            except Exception as e:
                logger.error(f"Erro ao buscar demographics da campanha {campaign.get('id')}: {e}")
                continue

        # Calcular totais e percentuais
        total_leads = sum(data['leads'] for data in demographics_data.values())
        total_spend = sum(data['spend'] for data in demographics_data.values())

        # Adicionar summary diretamente na estrutura
        demographics_data["summary"] = {
            "total_leads": total_leads,
            "total_spend": total_spend,
            "male_percentage": round((demographics_data['male']['leads'] / total_leads * 100) if total_leads > 0 else 0, 1),
            "female_percentage": round((demographics_data['female']['leads'] / total_leads * 100) if total_leads > 0 else 0, 1),
            "unknown_percentage": round((demographics_data['unknown']['leads'] / total_leads * 100) if total_leads > 0 else 0, 1)
        }

        logger.info(f"✅ Demographics obtidas: {total_leads} leads total, {demographics_data['male']['leads']} M, {demographics_data['female']['leads']} F")
        return demographics_data

    except Exception as e:
        logger.error(f"Erro ao buscar gender demographics: {e}")
        return {
            "error": str(e),
            "male": {"leads": 0, "spend": 0, "impressions": 0, "reach": 0, "clicks": 0},
            "female": {"leads": 0, "spend": 0, "impressions": 0, "reach": 0, "clicks": 0},
            "unknown": {"leads": 0, "spend": 0, "impressions": 0, "reach": 0, "clicks": 0},
            "summary": {
                "total_leads": 0,
                "total_spend": 0,
                "male_percentage": 0,
                "female_percentage": 0,
                "unknown_percentage": 0
            }
        }

# ========================================
# ENDPOINTS DO SCHEDULER AUTOMÁTICO
# ========================================

@router.post("/scheduler/start")
async def start_daily_scheduler():
    """
    Inicia o agendamento automático de sincronização diária

    ✅ Executa sincronização completa às 5:00 AM todos os dias
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
                "schedule": "Todos os dias às 5:00 AM",
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
                "time": "5:00 AM",
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

@router.get("/offsite-metrics/{campaign_id}")
async def get_offsite_metrics(
    campaign_id: str,
    start_date: str = Query(..., description="Data inicial (YYYY-MM-DD)"),
    end_date: str = Query(..., description="Data final (YYYY-MM-DD)")
):
    """
    Busca métricas offsite de conversão para uma campanha específica.
    Foca em offsite_complete_registration_add_meta_leads e métricas relacionadas.

    Retorna:
    - offsite_complete_registration: Cadastros completos offsite
    - cost_per_offsite_registration: Custo por cadastro offsite
    - Comparação com leads padrão
    - Breakdown de todas as métricas offsite
    """
    try:
        from app.services.facebook_offsite_sync import FacebookOffsiteSyncService

        offsite_service = FacebookOffsiteSyncService()

        logger.info(f"Buscando métricas offsite para campanha {campaign_id}")

        # Buscar métricas offsite
        metrics = await offsite_service.get_campaign_offsite_metrics(
            campaign_id=campaign_id,
            start_date=start_date,
            end_date=end_date
        )

        if not metrics:
            raise HTTPException(
                status_code=404,
                detail="Não foi possível buscar métricas offsite para esta campanha"
            )

        # Salvar no MongoDB para cache
        await offsite_service.sync_offsite_metrics_to_mongodb(campaign_id, metrics)

        return {
            "success": True,
            "campaign_id": campaign_id,
            "period": {
                "start_date": start_date,
                "end_date": end_date
            },
            "summary": metrics['summary'],
            "daily_breakdown": metrics['daily_metrics'],
            "offsite_focus": {
                "total_offsite_registrations": metrics['summary']['total_offsite_registrations'],
                "average_cost": metrics['summary']['average_cost_per_offsite_registration'],
                "conversion_rate": f"{metrics['summary']['offsite_conversion_rate']:.2f}%"
            }
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro ao buscar métricas offsite: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Erro ao buscar métricas offsite: {str(e)}"
        )

@router.post("/sync-offsite-all")
async def sync_all_offsite_metrics(
    days_back: int = Query(30, description="Quantos dias buscar (padrão: 30)")
):
    """
    Sincroniza métricas offsite de todas as campanhas ativas.
    Foca apenas em métricas de conversão offsite.
    """
    try:
        from app.services.facebook_offsite_sync import FacebookOffsiteSyncService
        from app.models.facebook_models import connect_mongodb, campaigns_collection

        await connect_mongodb()
        offsite_service = FacebookOffsiteSyncService()

        # Buscar campanhas do MongoDB
        campaigns = await campaigns_collection.find(
            {"status": {"$in": ["ACTIVE", "PAUSED"]}}
        ).to_list(None)

        end_date = date.today().strftime('%Y-%m-%d')
        start_date = (date.today() - timedelta(days=days_back)).strftime('%Y-%m-%d')

        results = []
        errors = []

        for campaign in campaigns:
            campaign_id = campaign.get('facebook_id')
            campaign_name = campaign.get('name', 'N/A')

            try:
                metrics = await offsite_service.get_campaign_offsite_metrics(
                    campaign_id=campaign_id,
                    start_date=start_date,
                    end_date=end_date
                )

                if metrics:
                    await offsite_service.sync_offsite_metrics_to_mongodb(campaign_id, metrics)
                    results.append({
                        'campaign_id': campaign_id,
                        'campaign_name': campaign_name,
                        'offsite_registrations': metrics['summary']['total_offsite_registrations']
                    })

            except Exception as e:
                errors.append({
                    'campaign_id': campaign_id,
                    'error': str(e)
                })

        return {
            "success": True,
            "total_campaigns": len(campaigns),
            "synced": len(results),
            "errors": len(errors),
            "period": {
                "start_date": start_date,
                "end_date": end_date
            },
            "results": results,
            "error_details": errors if errors else None
        }

    except Exception as e:
        logger.error(f"Erro na sincronização offsite: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Erro na sincronização offsite: {str(e)}"
        )

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

# =============================================================================
# Fim do arquivo