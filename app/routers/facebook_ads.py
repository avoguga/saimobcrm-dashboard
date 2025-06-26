from fastapi import APIRouter, HTTPException, Query
from typing import Dict, List, Optional
from datetime import datetime, timedelta
from app.services.facebook_api import FacebookAPI
from config import settings

router = APIRouter(prefix="/facebook-ads", tags=["Facebook Ads"])

def get_facebook_client():
    """Get Facebook API client"""
    if not settings.FACEBOOK_ACCESS_TOKEN:
        raise HTTPException(status_code=500, detail="Facebook access token not configured")
    return FacebookAPI(settings.FACEBOOK_ACCESS_TOKEN)

@router.get("/campaigns")
async def get_campaigns(
    fonte: Optional[str] = Query(None, description="Filtro por fonte (ex: 'Tráfego Meta')")
):
    """Get all campaigns for the configured ad account"""
    try:
        client = get_facebook_client()
        campaigns = client.get_campaigns(settings.FACEBOOK_AD_ACCOUNT_ID)
        
        # Se fonte foi especificada, adicionar metadados de filtro
        if fonte and campaigns.get('data'):
            campaigns['_metadata'] = {
                'fonte_filter': fonte,
                'note': 'Filtro por fonte aplicado - campanhas Facebook Ads'
            }
        
        return campaigns
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/insights")
async def get_insights(
    date_preset: Optional[str] = Query(None, description="Preset date range (e.g., 'last_7d', 'last_30d')"),
    since: Optional[str] = Query(None, description="Start date for custom range (YYYY-MM-DD)"),
    until: Optional[str] = Query(None, description="End date for custom range (YYYY-MM-DD)"),
    level: str = Query("campaign", description="Level of data aggregation"),
    breakdowns: Optional[str] = Query(None, description="Comma-separated list of breakdowns (e.g., 'age,gender')")
):
    """
    Get insights for the configured ad account
    
    Returns metrics including:
    - Impressions, reach, clicks, CTR, CPC, CPM, spend
    - Lead metrics (total leads, cost per lead)
    - Engagement metrics (likes, comments, shares, video views)
    """
    try:
        client = get_facebook_client()
        
        # Prepare time range
        time_range = None
        if since and until:
            time_range = {'since': since, 'until': until}
        
        # Prepare breakdowns
        breakdowns_list = breakdowns.split(',') if breakdowns else None
        
        # Get insights
        insights = client.get_campaign_insights(
            object_id=settings.FACEBOOK_AD_ACCOUNT_ID,
            date_preset=date_preset,
            time_range=time_range,
            level=level,
            breakdowns=breakdowns_list
        )
        
        return insights
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/insights/summary")
async def get_insights_summary(
    date_preset: Optional[str] = Query("last_7d", description="Preset date range"),
    since: Optional[str] = Query(None, description="Start date for custom range (YYYY-MM-DD)"),
    until: Optional[str] = Query(None, description="End date for custom range (YYYY-MM-DD)")
):
    """
    Get a summarized view of insights with calculated metrics
    """
    try:
        client = get_facebook_client()
        
        # Prepare time range
        time_range = None
        if since and until:
            time_range = {'since': since, 'until': until}
        
        # Get raw insights
        insights_data = client.get_campaign_insights(
            object_id=settings.FACEBOOK_AD_ACCOUNT_ID,
            date_preset=date_preset if not time_range else None,
            time_range=time_range
        )
        
        if not insights_data.get('data'):
            return {"error": "No data available for the specified period"}
        
        # Process the first data point (or aggregate if needed)
        data_list = insights_data.get('data', [])
        data = data_list[0] if data_list else {}
        
        # Extract lead metrics
        lead_metrics = client.get_lead_metrics(data)
        
        # Extract engagement metrics
        engagement_metrics = client.get_engagement_metrics(data)
        
        # Build summary response
        summary = {
            "basic_metrics": {
                "impressions": data.get('impressions', 0),
                "reach": data.get('reach', 0),
                "clicks": data.get('clicks', 0),
                "inline_link_clicks": data.get('inline_link_clicks', 0),
                "ctr": data.get('ctr', 0),
                "inline_link_click_ctr": data.get('inline_link_click_ctr', 0),
                "cpc": data.get('cpc', 0),
                "cost_per_inline_link_click": data.get('cost_per_inline_link_click', 0),
                "cpm": data.get('cpm', 0),
                "spend": data.get('spend', 0)
            },
            "lead_metrics": lead_metrics,
            "engagement_metrics": engagement_metrics,
            "period": {
                "date_preset": date_preset,
                "time_range": time_range
            }
        }
        
        return summary
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/campaigns/{campaign_id}/insights")
async def get_campaign_insights(
    campaign_id: str,
    date_preset: Optional[str] = Query("last_7d", description="Preset date range"),
    since: Optional[str] = Query(None, description="Start date for custom range (YYYY-MM-DD)"),
    until: Optional[str] = Query(None, description="End date for custom range (YYYY-MM-DD)"),
    fonte: Optional[str] = Query(None, description="Filtro por fonte (ex: 'Tráfego Meta')")
):
    """
    Get insights for a specific campaign
    """
    try:
        client = get_facebook_client()
        
        time_range = None
        if since and until:
            time_range = {'since': since, 'until': until}
        
        insights = client.get_campaign_insights(
            object_id=campaign_id,
            date_preset=date_preset if not time_range else None,
            time_range=time_range,
            level="campaign"
        )
        
        # Adicionar metadados de filtro se fonte foi especificada
        if fonte and insights:
            if '_metadata' not in insights:
                insights['_metadata'] = {}
            insights['_metadata']['fonte_filter'] = fonte
            insights['_metadata']['campaign_id'] = campaign_id
        
        return insights
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/campaigns/{campaign_id}/adsets")
async def get_adsets_for_campaign(campaign_id: str):
    """
    Get all ad sets for a specific campaign
    """
    try:
        client = get_facebook_client()
        adsets = client.get_adsets(campaign_id)
        return adsets
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/adsets/{adset_id}/insights")
async def get_adset_insights(
    adset_id: str,
    date_preset: Optional[str] = Query("last_7d", description="Preset date range"),
    since: Optional[str] = Query(None, description="Start date for custom range (YYYY-MM-DD)"),
    until: Optional[str] = Query(None, description="End date for custom range (YYYY-MM-DD)")
):
    """
    Get insights for a specific ad set
    """
    try:
        client = get_facebook_client()
        
        time_range = None
        if since and until:
            time_range = {'since': since, 'until': until}
        
        insights = client.get_campaign_insights(
            object_id=adset_id,
            date_preset=date_preset if not time_range else None,
            time_range=time_range,
            level="adset"
        )
        
        return insights
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/ads/{ad_id}/insights")
async def get_ad_insights(
    ad_id: str,
    date_preset: Optional[str] = Query("last_7d", description="Preset date range"),
    since: Optional[str] = Query(None, description="Start date for custom range (YYYY-MM-DD)"),
    until: Optional[str] = Query(None, description="End date for custom range (YYYY-MM-DD)")
):
    """
    Get insights for a specific ad
    """
    try:
        client = get_facebook_client()
        
        time_range = None
        if since and until:
            time_range = {'since': since, 'until': until}
        
        insights = client.get_campaign_insights(
            object_id=ad_id,
            date_preset=date_preset if not time_range else None,
            time_range=time_range,
            level="ad"
        )
        
        return insights
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/webhook")
async def facebook_webhook(data: dict):
    """
    Webhook endpoint para receber notificações do Facebook
    
    Este endpoint pode ser usado para:
    - Receber notificações de novos leads
    - Atualizações de campanhas
    - Eventos de conversão
    """
    try:
        # Log do webhook recebido
        print(f"Facebook webhook received: {data}")
        
        # Verificar se é uma notificação de lead
        if data.get("object") == "page":
            entries = data.get("entry", [])
            
            for entry in entries:
                changes = entry.get("changes", [])
                
                for change in changes:
                    if change.get("field") == "leadgen":
                        # Processar novo lead
                        lead_data = change.get("value", {})
                        leadgen_id = lead_data.get("leadgen_id")
                        page_id = lead_data.get("page_id")
                        form_id = lead_data.get("form_id")
                        adgroup_id = lead_data.get("adgroup_id")
                        
                        print(f"New lead received: {leadgen_id} from form {form_id}")
                        
                        # Aqui você pode implementar a lógica para:
                        # 1. Buscar os dados completos do lead via API do Facebook
                        # 2. Integrar com o Kommo CRM
                        # 3. Enviar notificações
                        
                        response_data = {
                            "status": "processed",
                            "leadgen_id": leadgen_id,
                            "page_id": page_id,
                            "form_id": form_id,
                            "adgroup_id": adgroup_id,
                            "timestamp": datetime.now().isoformat()
                        }
                        
                        return response_data
        
        # Para outros tipos de webhook
        return {
            "status": "received",
            "message": "Webhook received successfully",
            "data": data,
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        print(f"Error processing Facebook webhook: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error processing webhook: {str(e)}")

@router.get("/webhook")
async def verify_facebook_webhook(
    hub_mode: str = Query(alias="hub.mode"),
    hub_challenge: str = Query(alias="hub.challenge"),
    hub_verify_token: str = Query(alias="hub.verify_token")
):
    """
    Endpoint de verificação do webhook do Facebook
    
    O Facebook chama este endpoint para verificar se o webhook é válido
    """
    try:
        # Token de verificação deve ser configurado no Facebook e aqui
        VERIFY_TOKEN = settings.FACEBOOK_WEBHOOK_VERIFY_TOKEN if hasattr(settings, 'FACEBOOK_WEBHOOK_VERIFY_TOKEN') else "your_verify_token"
        
        if hub_mode == "subscribe" and hub_verify_token == VERIFY_TOKEN:
            print("Facebook webhook verified successfully")
            return int(hub_challenge)
        else:
            print(f"Facebook webhook verification failed. Mode: {hub_mode}, Token: {hub_verify_token}")
            raise HTTPException(status_code=403, detail="Webhook verification failed")
            
    except Exception as e:
        print(f"Error verifying Facebook webhook: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error verifying webhook: {str(e)}")