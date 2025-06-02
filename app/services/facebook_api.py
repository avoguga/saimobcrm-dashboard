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