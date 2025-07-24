import requests
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

class FacebookAPI:
    def __init__(self, access_token: str, api_version: str = "v23.0"):
        self.access_token = access_token
        self.api_version = api_version
        self.base_url = f"https://graph.facebook.com/{api_version}"
        
    def _make_request(self, endpoint: str, params: Dict[str, Any] = None) -> Dict[str, Any]:
        """Make a request to Facebook API"""
        if params is None:
            params = {}
        
        params['access_token'] = self.access_token
        
        try:
            response = requests.get(f"{self.base_url}/{endpoint}", params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Facebook API request error: {str(e)}")
            if hasattr(e, 'response') and e.response is not None:
                try:
                    error_data = e.response.json()
                    logger.error(f"Facebook API error details: {error_data}")
                except:
                    logger.error(f"Response text: {e.response.text}")
            raise
    
    def get_campaign_insights(
        self, 
        object_id: str,
        date_preset: Optional[str] = None,
        time_range: Optional[Dict[str, str]] = None,
        level: str = "campaign",
        breakdowns: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Get insights for campaigns, adsets or ads
        
        Args:
            object_id: The Facebook object ID (campaign, adset, ad, or ad account)
            date_preset: Preset date range (e.g., 'last_7d', 'last_30d')
            time_range: Custom date range with 'since' and 'until' keys
            level: Level of data aggregation ('campaign', 'adset', 'ad')
            breakdowns: List of dimensions to segment data by
        """
        
        # Define the metrics we want to retrieve - start with basic fields
        fields = [
            # Basic metrics that are always available
            "impressions",
            "reach",
            "clicks",
            "ctr",
            "cpc",
            "cpm",
            "spend",
            
            # Action metrics (includes leads, purchases, etc)
            "actions",
            
            # Cost per action
            "cost_per_action_type",
            
            # Inline link clicks (more accurate than generic clicks)
            "inline_link_clicks",
            "inline_link_click_ctr",
            "cost_per_inline_link_click"
        ]
        
        params = {
            'fields': ','.join(fields),
            'level': level
        }
        
        # Facebook requires either date_preset or time_range
        if date_preset:
            params['date_preset'] = date_preset
        elif time_range:
            params['time_range'] = str(time_range)
        else:
            # Default to last 7 days if no date range specified
            params['date_preset'] = 'last_7d'
            
        if breakdowns:
            params['breakdowns'] = ','.join(breakdowns)
            
        return self._make_request(f"{object_id}/insights", params)
    
    def get_lead_metrics(self, insights_data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract lead-specific metrics from insights data"""
        lead_metrics = {
            'total_leads': 0,
            'cost_per_lead': 0,
            'lead_actions': []
        }
        
        # Extract leads from actions
        if 'actions' in insights_data:
            for action in insights_data['actions']:
                if action['action_type'] in ['lead', 'onsite_conversion.lead_grouped']:
                    lead_metrics['lead_actions'].append(action)
                    lead_metrics['total_leads'] += int(action.get('value', 0))
        
        # Direct lead count if available
        if 'leads' in insights_data:
            lead_metrics['total_leads'] = insights_data['leads']
            
        # Cost per lead
        if 'cost_per_lead' in insights_data:
            lead_metrics['cost_per_lead'] = insights_data['cost_per_lead']
        elif lead_metrics['total_leads'] > 0 and 'spend' in insights_data:
            lead_metrics['cost_per_lead'] = float(insights_data['spend']) / lead_metrics['total_leads']
            
        return lead_metrics
    
    def get_engagement_metrics(self, insights_data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract engagement metrics from insights data"""
        engagement_metrics = {
            'likes': 0,
            'comments': 0,
            'shares': 0,
            'video_views': 0,
            'profile_views': 0,
            'total_engagement': 0
        }
        
        # Extract from actions
        if 'actions' in insights_data:
            for action in insights_data['actions']:
                action_type = action['action_type']
                value = int(action.get('value', 0))
                
                if action_type in ['post_reaction', 'like']:
                    engagement_metrics['likes'] += value
                elif action_type == 'comment':
                    engagement_metrics['comments'] += value
                elif action_type in ['post', 'share']:
                    engagement_metrics['shares'] += value
                elif action_type == 'link_click' and 'profile' in action.get('action_destination', ''):
                    engagement_metrics['profile_views'] += value
        
        # Video views
        if 'video_plays' in insights_data:
            engagement_metrics['video_views'] = insights_data['video_plays']
        elif 'video_view' in insights_data:
            engagement_metrics['video_views'] = insights_data['video_view']
            
        # Total engagement
        if 'post_engagement' in insights_data:
            engagement_metrics['total_engagement'] = insights_data['post_engagement']
        elif 'page_engagement' in insights_data:
            engagement_metrics['total_engagement'] = insights_data['page_engagement']
            
        return engagement_metrics
    
    def get_ad_accounts(self) -> List[Dict[str, Any]]:
        """Get all ad accounts for the authenticated user"""
        response = self._make_request("me/adaccounts", {
            'fields': 'id,name,account_status,currency,timezone_name'
        })
        return response.get('data', [])
    
    def get_campaigns(self, ad_account_id: str) -> List[Dict[str, Any]]:
        """Get all campaigns for an ad account"""
        response = self._make_request(f"{ad_account_id}/campaigns", {
            'fields': 'id,name,status,objective,created_time,updated_time'
        })
        return response.get('data', [])
    
    def get_adsets(self, campaign_id: str) -> List[Dict[str, Any]]:
        """Get all ad sets for a campaign"""
        fields_to_request = [
            'id',
            'name',
            'status',
            'daily_budget',
            'lifetime_budget',
            'bid_strategy',
            'created_time',
            'start_time',
            'end_time',
            'objective'
        ]
        
        response = self._make_request(f"{campaign_id}/adsets", {
            'fields': ','.join(fields_to_request)
        })
        return response.get('data', [])
    
    def get_all_adsets(self, ad_account_id: str) -> List[Dict[str, Any]]:
        """Get all ad sets for an ad account"""
        fields_to_request = [
            'id',
            'name',
            'status',
            'campaign_id',
            'daily_budget',
            'lifetime_budget',
            'bid_strategy',
            'created_time',
            'start_time',
            'end_time',
            'objective'
        ]
        
        response = self._make_request(f"{ad_account_id}/adsets", {
            'fields': ','.join(fields_to_request)
        })
        return response.get('data', [])
    
    def get_ads(self, adset_id: str) -> List[Dict[str, Any]]:
        """Get all ads for an ad set"""
        fields_to_request = [
            'id',
            'name',
            'status',
            'adset_id',
            'campaign_id',
            'creative',
            'created_time',
            'updated_time'
        ]
        
        response = self._make_request(f"{adset_id}/ads", {
            'fields': ','.join(fields_to_request)
        })
        return response.get('data', [])
    
    def get_all_ads(self, ad_account_id: str) -> List[Dict[str, Any]]:
        """Get all ads for an ad account"""
        fields_to_request = [
            'id',
            'name',
            'status',
            'adset_id',
            'campaign_id',
            'creative',
            'created_time',
            'updated_time'
        ]
        
        response = self._make_request(f"{ad_account_id}/ads", {
            'fields': ','.join(fields_to_request)
        })
        return response.get('data', [])
    
    def get_adset_targeting(self, adset_id: str) -> Dict[str, Any]:
        """Get targeting details for an ad set"""
        response = self._make_request(f"{adset_id}", {
            'fields': 'targeting,name,status'
        })
        return response
    
    def get_insights_with_geo_breakdown(
        self, 
        object_id: str,
        breakdown_type: str = "region",
        date_preset: str = "last_30d",
        level: str = None
    ) -> Dict[str, Any]:
        """Get insights with geographic breakdown"""
        
        params = {
            'fields': 'impressions,reach,clicks,ctr,cpc,cpm,spend,actions',
            'date_preset': date_preset,
            'breakdowns': breakdown_type
        }
        
        # Para city breakdown, NÃO usar level - deixar implícito baseado no object_id
        # Para region/country, pode usar level=campaign se for account ID
        if breakdown_type in ["region", "country"] and level:
            params['level'] = level
        
        return self._make_request(f"{object_id}/insights", params)
    
    def get_city_insights_from_adsets(
        self, 
        ad_account_id: str,
        date_preset: str = "last_30d"
    ) -> Dict[str, Any]:
        """Get city insights by iterating through adsets"""
        
        # 1. Primeiro buscar todos os adsets
        adsets = self.get_all_adsets(ad_account_id)
        
        all_city_data = []
        processing_stats = {
            'total_adsets': len(adsets),
            'adsets_with_city_data': 0,
            'adsets_without_city_data': 0,
            'adsets_with_errors': 0,
            'city_targeting_adsets': []
        }
        
        # 2. Para cada adset, buscar insights por cidade
        for adset in adsets:
            adset_id = adset.get('id')
            adset_name = adset.get('name', '')
            
            if not adset_id:
                continue
                
            try:
                # Insights por cidade para este adset específico
                city_insights = self._make_request(f"{adset_id}/insights", {
                    'fields': 'impressions,reach,clicks,ctr,cpc,cpm,spend,actions',
                    'date_preset': date_preset,
                    'breakdowns': 'city'
                })
                
                # Verificar se retornou dados com campo 'city'
                has_city_data = False
                
                for insight in city_insights.get('data', []):
                    # PONTO CRÍTICO: Verificar se a chave 'city' existe!
                    if 'city' in insight:
                        has_city_data = True
                        insight['adset_id'] = adset_id
                        insight['adset_name'] = adset_name
                        all_city_data.append(insight)
                
                # Estatísticas de processamento
                if has_city_data:
                    processing_stats['adsets_with_city_data'] += 1
                    processing_stats['city_targeting_adsets'].append({
                        'id': adset_id,
                        'name': adset_name
                    })
                else:
                    processing_stats['adsets_without_city_data'] += 1
                    
            except Exception as e:
                processing_stats['adsets_with_errors'] += 1
                continue
        
        return {
            'data': all_city_data,
            'total_city_records': len(all_city_data),
            'processing_stats': processing_stats,
            '_metadata': {
                'note': 'Only adsets with city-level targeting return city data',
                'tip': 'AdSets targeting entire countries will not have city breakdown'
            }
        }
    
    def find_city_targeted_adsets(self, ad_account_id: str) -> List[Dict[str, Any]]:
        """Find adsets that are specifically targeted to cities"""
        
        adsets = self.get_all_adsets(ad_account_id)
        city_targeted_adsets = []
        
        for adset in adsets:
            adset_id = adset.get('id')
            adset_name = adset.get('name', '')
            
            # Buscar por pistas no nome do adset
            city_indicators = ['[MACEIÓ]', '[CIDADES', '[CIDADE', 'NATAL', 'FORTALEZA', 'RECIFE', 'SALVADOR']
            
            has_city_indicator = any(indicator in adset_name.upper() for indicator in city_indicators)
            
            if has_city_indicator:
                city_targeted_adsets.append({
                    'id': adset_id,
                    'name': adset_name,
                    'reason': 'Name contains city indicator'
                })
            
            # Opcional: Verificar targeting real (mais lento)
            # try:
            #     targeting = self.get_adset_targeting(adset_id)
            #     geo_locations = targeting.get('targeting', {}).get('geo_locations', {})
            #     if 'cities' in geo_locations:
            #         city_targeted_adsets.append({
            #             'id': adset_id,
            #             'name': adset_name,
            #             'reason': 'Has city targeting'
            #         })
            # except:
            #     pass
        
        return city_targeted_adsets
    
    def get_whatsapp_campaign_insights(
        self,
        object_id: str,
        date_preset: Optional[str] = None,
        time_range: Optional[Dict[str, str]] = None,
        level: str = "campaign"
    ) -> Dict[str, Any]:
        """
        Get WhatsApp-specific insights for campaigns, adsets or ads.
        Extracts conversations started and link clicks metrics.
        
        Args:
            object_id: The Facebook object ID (campaign, adset, ad, or ad account)
            date_preset: Preset date range (e.g., 'last_7d', 'last_30d')
            time_range: Custom date range with 'since' and 'until' keys
            level: Level of data aggregation ('campaign', 'adset', 'ad')
        """
        
        # Fields needed for WhatsApp campaign metrics
        fields = [
            "campaign_name",
            "clicks",
            "actions",
            "spend",
            "impressions",
            "reach"
        ]
        
        params = {
            'fields': ','.join(fields),
            'level': level,
            'action_breakdowns': 'action_type'  # Essential for getting detailed action types
        }
        
        # Facebook requires either date_preset or time_range
        if date_preset:
            params['date_preset'] = date_preset
        elif time_range:
            params['time_range'] = str(time_range)
        else:
            # Default to last 7 days if no date range specified
            params['date_preset'] = 'last_7d'
            
        return self._make_request(f"{object_id}/insights", params)
    
    def extract_whatsapp_metrics(self, insights_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract WhatsApp campaign metrics from insights data.
        
        Returns:
        - conversations_started: Number of WhatsApp conversations initiated (onsite_conversion.messaging_conversation_started_7d)
        - profile_visits: Number of visits to company profile/page on FB/IG (profile_view)
        """
        whatsapp_metrics = {
            'conversations_started': 0,
            'profile_visits': 0,
            'whatsapp_clicks': 0,  # Clicks specifically to WhatsApp
            'clicks_breakdown': {
                'link_click': 0,
                'inline_link_click': 0,
                'outbound_click': 0,
                'messaging_contact': 0
            },
            'total_spend': 0,
            'campaign_name': '',
            'impressions': 0,
            'reach': 0,
            'raw_actions': []  # For debugging purposes
        }
        
        # Extract basic metrics with error handling
        whatsapp_metrics['campaign_name'] = insights_data.get('campaign_name', '')
        
        try:
            whatsapp_metrics['total_spend'] = float(insights_data.get('spend', 0))
        except (ValueError, TypeError):
            whatsapp_metrics['total_spend'] = 0.0
            
        try:
            whatsapp_metrics['impressions'] = int(insights_data.get('impressions', 0))
        except (ValueError, TypeError):
            whatsapp_metrics['impressions'] = 0
            
        try:
            whatsapp_metrics['reach'] = int(insights_data.get('reach', 0))
        except (ValueError, TypeError):
            whatsapp_metrics['reach'] = 0
        
        # Extract WhatsApp-specific actions
        if 'actions' in insights_data and isinstance(insights_data['actions'], list):
            for action in insights_data['actions']:
                if not isinstance(action, dict):
                    continue
                    
                action_type = action.get('action_type', '')
                try:
                    value = int(action.get('value', 0))
                except (ValueError, TypeError):
                    value = 0
                
                # Store raw actions for debugging
                whatsapp_metrics['raw_actions'].append({
                    'action_type': action_type,
                    'value': value
                })
                
                # WhatsApp conversations started (7-day attribution)
                if action_type == 'onsite_conversion.messaging_conversation_started_7d':
                    whatsapp_metrics['conversations_started'] = value
                
                # Profile visits - visits to company FB/IG profile/page (NOT WhatsApp)
                elif action_type == 'profile_view':
                    whatsapp_metrics['profile_visits'] = value
                
                # WhatsApp-specific clicks - clicks that lead to WhatsApp chat
                elif action_type == 'link_click':
                    whatsapp_metrics['clicks_breakdown']['link_click'] = value
                    whatsapp_metrics['whatsapp_clicks'] += value
                
                elif action_type == 'inline_link_click':
                    whatsapp_metrics['clicks_breakdown']['inline_link_click'] = value
                    whatsapp_metrics['whatsapp_clicks'] += value
                    
                elif action_type == 'outbound_click':
                    whatsapp_metrics['clicks_breakdown']['outbound_click'] = value
                    whatsapp_metrics['whatsapp_clicks'] += value
                    
                elif action_type == 'messaging_contact':
                    whatsapp_metrics['clicks_breakdown']['messaging_contact'] = value
                    whatsapp_metrics['whatsapp_clicks'] += value
        
        # Calculate derived metrics
        if whatsapp_metrics['conversations_started'] > 0 and whatsapp_metrics['total_spend'] > 0:
            whatsapp_metrics['cost_per_conversation'] = whatsapp_metrics['total_spend'] / whatsapp_metrics['conversations_started']
        else:
            whatsapp_metrics['cost_per_conversation'] = 0
            
        if whatsapp_metrics['whatsapp_clicks'] > 0 and whatsapp_metrics['total_spend'] > 0:
            whatsapp_metrics['cost_per_whatsapp_click'] = whatsapp_metrics['total_spend'] / whatsapp_metrics['whatsapp_clicks']
        else:
            whatsapp_metrics['cost_per_whatsapp_click'] = 0
            
        if whatsapp_metrics['whatsapp_clicks'] > 0 and whatsapp_metrics['conversations_started'] > 0:
            whatsapp_metrics['whatsapp_conversion_rate'] = (whatsapp_metrics['conversations_started'] / whatsapp_metrics['whatsapp_clicks']) * 100
        else:
            whatsapp_metrics['whatsapp_conversion_rate'] = 0
            
        # Additional metrics for profile visits
        if whatsapp_metrics['profile_visits'] > 0 and whatsapp_metrics['total_spend'] > 0:
            whatsapp_metrics['cost_per_profile_visit'] = whatsapp_metrics['total_spend'] / whatsapp_metrics['profile_visits']
        else:
            whatsapp_metrics['cost_per_profile_visit'] = 0
        
        return whatsapp_metrics
    
    def get_leads_segmentation_insights(
        self,
        object_id: str,
        date_preset: Optional[str] = None,
        time_range: Optional[Dict[str, str]] = None,
        breakdowns: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Get leads segmentation insights by gender and location (state/city).
        
        Args:
            object_id: The Facebook object ID (campaign, adset, ad, or ad account)
            date_preset: Preset date range (e.g., 'last_7d', 'last_30d')
            time_range: Custom date range with 'since' and 'until' keys
            breakdowns: List of breakdowns (gender, region, city)
        """
        
        # Fields needed for leads segmentation
        fields = [
            "actions"
        ]
        
        # Default breakdowns for gender and location
        if not breakdowns:
            breakdowns = ["gender", "region", "city"]
        
        params = {
            'fields': ','.join(fields),
            'action_breakdowns': 'action_type',
            'breakdowns': ','.join(breakdowns)
        }
        
        # Facebook requires either date_preset or time_range
        if date_preset:
            params['date_preset'] = date_preset
        elif time_range:
            params['time_range'] = str(time_range)
        else:
            # Default to last 30 days for leads analysis
            params['date_preset'] = 'last_30d'
            
        return self._make_request(f"{object_id}/insights", params)
    
    def process_leads_segmentation(self, insights_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Process leads segmentation data from Facebook API response.
        
        Returns a list of segmented leads data with gender, state, city and lead count.
        """
        segmented_results = []
        
        if not insights_data.get('data'):
            return segmented_results
        
        # Process each segment in the response
        for segment in insights_data['data']:
            # Extract demographic and location data
            gender = segment.get('gender', 'unknown')
            state = segment.get('region', 'unknown')
            city = segment.get('city', 'unknown')
            
            # Initialize leads count for this segment
            leads_count = 0
            
            # Extract leads from actions
            if 'actions' in segment and isinstance(segment['actions'], list):
                for action in segment['actions']:
                    if isinstance(action, dict) and action.get('action_type') == 'lead':
                        try:
                            leads_count = int(action.get('value', 0))
                            break  # Found the lead action, stop looking
                        except (ValueError, TypeError):
                            leads_count = 0
            
            # Add processed segment to results
            segmented_results.append({
                'genero': gender,
                'estado': state,
                'cidade': city,
                'leads': leads_count,
                'raw_segment': segment  # Keep raw data for debugging
            })
        
        return segmented_results
    
    def get_leads_summary(self, segmented_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Generate summary statistics from segmented leads data.
        """
        if not segmented_data:
            return {
                'total_leads': 0,
                'total_segments': 0,
                'by_gender': {},
                'by_state': {},
                'by_city': {},
                'top_segments': []
            }
        
        # Calculate totals
        total_leads = sum(segment['leads'] for segment in segmented_data)
        total_segments = len(segmented_data)
        
        # Group by gender
        by_gender = {}
        for segment in segmented_data:
            gender = segment['genero']
            by_gender[gender] = by_gender.get(gender, 0) + segment['leads']
        
        # Group by state
        by_state = {}
        for segment in segmented_data:
            state = segment['estado']
            by_state[state] = by_state.get(state, 0) + segment['leads']
        
        # Group by city
        by_city = {}
        for segment in segmented_data:
            city = segment['cidade']
            by_city[city] = by_city.get(city, 0) + segment['leads']
        
        # Top performing segments (top 10)
        top_segments = sorted(
            segmented_data, 
            key=lambda x: x['leads'], 
            reverse=True
        )[:10]
        
        return {
            'total_leads': total_leads,
            'total_segments': total_segments,
            'by_gender': by_gender,
            'by_state': by_state,
            'by_city': by_city,
            'top_segments': top_segments
        }