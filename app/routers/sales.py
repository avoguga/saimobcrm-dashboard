from fastapi import APIRouter, Query, HTTPException
from typing import Dict, Optional
from datetime import datetime, timedelta
from app.services.kommo_api import KommoAPI

router = APIRouter(prefix="/sales", tags=["Sales"])

@router.get("/revenue")
async def get_sales_revenue(
    days: int = Query(30, description="Período em dias para analisar"),
    group_by: str = Query("day", description="Agrupar por: day, week, month")
):
    """Retorna análise de receita de vendas"""
    try:
        api = KommoAPI()
        
        # Buscar pipelines e status de ganho
        pipelines_response = api.get_pipelines()
        won_statuses = []
        
        if pipelines_response and '_embedded' in pipelines_response:
            for pipeline in pipelines_response.get('_embedded', {}).get('pipelines', []):
                pipeline_id = pipeline['id']
                statuses = api.get_pipeline_statuses(pipeline_id)
                
                if statuses and '_embedded' in statuses:
                    for status in statuses.get('_embedded', {}).get('statuses', []):
                        if status.get('type') == 'won':
                            won_statuses.append(status['id'])
        
        if not won_statuses:
            return {
                "total_revenue": 0,
                "revenue_by_period": [],
                "average_deal_size": 0,
                "total_deals": 0,
                "period_days": days,
                "group_by": group_by,
                "message": "Nenhum status de ganho encontrado"
            }
        
        # Calcular timestamp para o período
        cutoff_timestamp = int((datetime.now() - timedelta(days=days)).timestamp())
        
        # Buscar leads ganhos
        params = {
            'filter[statuses]': won_statuses,
            'limit': 500
        }
        
        leads_response = api.get_leads(params)
        
        if not leads_response or '_embedded' not in leads_response:
            return {
                "total_revenue": 0,
                "revenue_by_period": [],
                "average_deal_size": 0,
                "total_deals": 0,
                "period_days": days,
                "group_by": group_by,
                "message": "Nenhuma venda encontrada"
            }
        
        leads = leads_response.get('_embedded', {}).get('leads', [])
        
        # Filtrar leads por período e agrupar
        revenue_by_period = {}
        total_revenue = 0
        total_deals = 0
        
        for lead in leads:
            closed_at = lead.get('closed_at')
            if not closed_at or closed_at < cutoff_timestamp:
                continue
            
            price = lead.get('price', 0)
            total_revenue += price
            total_deals += 1
            
            # Agrupar por período
            closed_date = datetime.fromtimestamp(closed_at)
            
            if group_by == "day":
                period_key = closed_date.strftime("%Y-%m-%d")
            elif group_by == "week":
                # Primeira data da semana
                week_start = closed_date - timedelta(days=closed_date.weekday())
                period_key = week_start.strftime("%Y-%m-%d")
            elif group_by == "month":
                period_key = closed_date.strftime("%Y-%m")
            else:
                period_key = closed_date.strftime("%Y-%m-%d")
            
            if period_key not in revenue_by_period:
                revenue_by_period[period_key] = {
                    "period": period_key,
                    "revenue": 0,
                    "deals_count": 0
                }
            
            revenue_by_period[period_key]["revenue"] += price
            revenue_by_period[period_key]["deals_count"] += 1
        
        # Converter para lista e ordenar
        revenue_list = list(revenue_by_period.values())
        revenue_list.sort(key=lambda x: x["period"])
        
        # Calcular ticket médio
        average_deal_size = total_revenue / total_deals if total_deals > 0 else 0
        
        return {
            "total_revenue": round(total_revenue, 2),
            "revenue_by_period": revenue_list,
            "average_deal_size": round(average_deal_size, 2),
            "total_deals": total_deals,
            "period_days": days,
            "group_by": group_by
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao analisar receita: {str(e)}")

@router.get("/by-pipeline")
async def get_sales_by_pipeline(
    days: int = Query(30, description="Período em dias para analisar")
):
    """Retorna vendas segmentadas por pipeline"""
    try:
        api = KommoAPI()
        
        # Buscar pipelines
        pipelines_response = api.get_pipelines()
        
        if not pipelines_response or '_embedded' not in pipelines_response:
            return {
                "sales_by_pipeline": [],
                "total_revenue": 0,
                "total_deals": 0,
                "period_days": days,
                "message": "Nenhum pipeline encontrado"
            }
        
        pipelines = pipelines_response.get('_embedded', {}).get('pipelines', [])
        
        # Calcular timestamp para o período
        cutoff_timestamp = int((datetime.now() - timedelta(days=days)).timestamp())
        
        sales_by_pipeline = []
        total_revenue = 0
        total_deals = 0
        
        for pipeline in pipelines:
            pipeline_id = pipeline['id']
            pipeline_name = pipeline.get('name', f'Pipeline {pipeline_id}')
            
            # Buscar status de ganho para este pipeline
            statuses_response = api.get_pipeline_statuses(pipeline_id)
            won_statuses = []
            
            if statuses_response and '_embedded' in statuses_response:
                for status in statuses_response.get('_embedded', {}).get('statuses', []):
                    if status.get('type') == 'won':
                        won_statuses.append(status['id'])
            
            if not won_statuses:
                # Adicionar pipeline mesmo sem vendas para completude
                sales_by_pipeline.append({
                    "pipeline_id": pipeline_id,
                    "pipeline_name": pipeline_name,
                    "revenue": 0,
                    "deals_count": 0,
                    "average_deal_size": 0
                })
                continue
            
            # Buscar leads ganhos para este pipeline
            params = {
                'filter[statuses]': won_statuses,
                'limit': 500
            }
            
            leads_response = api.get_leads(params)
            
            pipeline_revenue = 0
            pipeline_deals = 0
            
            if leads_response and '_embedded' in leads_response:
                leads = leads_response.get('_embedded', {}).get('leads', [])
                
                for lead in leads:
                    closed_at = lead.get('closed_at')
                    if not closed_at or closed_at < cutoff_timestamp:
                        continue
                    
                    # Verificar se o lead pertence a este pipeline
                    if lead.get('pipeline_id') == pipeline_id:
                        price = lead.get('price', 0)
                        pipeline_revenue += price
                        pipeline_deals += 1
            
            # Calcular ticket médio do pipeline
            avg_deal_size = pipeline_revenue / pipeline_deals if pipeline_deals > 0 else 0
            
            sales_by_pipeline.append({
                "pipeline_id": pipeline_id,
                "pipeline_name": pipeline_name,
                "revenue": round(pipeline_revenue, 2),
                "deals_count": pipeline_deals,
                "average_deal_size": round(avg_deal_size, 2)
            })
            
            total_revenue += pipeline_revenue
            total_deals += pipeline_deals
        
        # Ordenar por receita (decrescente)
        sales_by_pipeline.sort(key=lambda x: x["revenue"], reverse=True)
        
        return {
            "sales_by_pipeline": sales_by_pipeline,
            "total_revenue": round(total_revenue, 2),
            "total_deals": total_deals,
            "period_days": days
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao analisar vendas por pipeline: {str(e)}")

@router.get("/growth")
async def get_sales_growth(
    current_days: int = Query(30, description="Período atual em dias"),
    previous_days: int = Query(30, description="Período anterior em dias para comparação")
):
    """Retorna análise de crescimento de vendas comparando dois períodos"""
    try:
        api = KommoAPI()
        
        # Buscar pipelines e status de ganho
        pipelines_response = api.get_pipelines()
        won_statuses = []
        
        if pipelines_response and '_embedded' in pipelines_response:
            for pipeline in pipelines_response.get('_embedded', {}).get('pipelines', []):
                pipeline_id = pipeline['id']
                statuses = api.get_pipeline_statuses(pipeline_id)
                
                if statuses and '_embedded' in statuses:
                    for status in statuses.get('_embedded', {}).get('statuses', []):
                        if status.get('type') == 'won':
                            won_statuses.append(status['id'])
        
        if not won_statuses:
            return {
                "current_period": {"revenue": 0, "deals": 0},
                "previous_period": {"revenue": 0, "deals": 0},
                "growth": {"revenue_percentage": 0, "deals_percentage": 0},
                "message": "Nenhum status de ganho encontrado"
            }
        
        # Calcular timestamps
        now = datetime.now()
        current_start = int((now - timedelta(days=current_days)).timestamp())
        previous_start = int((now - timedelta(days=current_days + previous_days)).timestamp())
        previous_end = current_start
        
        # Buscar leads ganhos
        params = {
            'filter[statuses]': won_statuses,
            'limit': 500
        }
        
        leads_response = api.get_leads(params)
        
        if not leads_response or '_embedded' not in leads_response:
            return {
                "current_period": {"revenue": 0, "deals": 0},
                "previous_period": {"revenue": 0, "deals": 0},
                "growth": {"revenue_percentage": 0, "deals_percentage": 0},
                "message": "Nenhuma venda encontrada"
            }
        
        leads = leads_response.get('_embedded', {}).get('leads', [])
        
        # Separar leads por período
        current_revenue = 0
        current_deals = 0
        previous_revenue = 0
        previous_deals = 0
        
        for lead in leads:
            closed_at = lead.get('closed_at')
            if not closed_at:
                continue
            
            price = lead.get('price', 0)
            
            if closed_at >= current_start:
                # Período atual
                current_revenue += price
                current_deals += 1
            elif closed_at >= previous_start and closed_at < previous_end:
                # Período anterior
                previous_revenue += price
                previous_deals += 1
        
        # Calcular crescimento
        revenue_growth = 0
        if previous_revenue > 0:
            revenue_growth = ((current_revenue - previous_revenue) / previous_revenue) * 100
        elif current_revenue > 0:
            revenue_growth = 100  # 100% de crescimento se não havia receita anterior
        
        deals_growth = 0
        if previous_deals > 0:
            deals_growth = ((current_deals - previous_deals) / previous_deals) * 100
        elif current_deals > 0:
            deals_growth = 100  # 100% de crescimento se não havia deals anteriores
        
        # Calcular ticket médio para cada período
        current_avg_ticket = current_revenue / current_deals if current_deals > 0 else 0
        previous_avg_ticket = previous_revenue / previous_deals if previous_deals > 0 else 0
        
        return {
            "current_period": {
                "revenue": round(current_revenue, 2),
                "deals": current_deals,
                "average_ticket": round(current_avg_ticket, 2),
                "days": current_days
            },
            "previous_period": {
                "revenue": round(previous_revenue, 2),
                "deals": previous_deals,
                "average_ticket": round(previous_avg_ticket, 2),
                "days": previous_days
            },
            "growth": {
                "revenue_percentage": round(revenue_growth, 2),
                "deals_percentage": round(deals_growth, 2),
                "revenue_absolute": round(current_revenue - previous_revenue, 2),
                "deals_absolute": current_deals - previous_deals
            },
            "trend": "positive" if revenue_growth > 0 else ("negative" if revenue_growth < 0 else "stable")
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao analisar crescimento: {str(e)}")

@router.get("/by-user")
async def get_sales_by_user(
    days: int = Query(30, description="Número de dias para análise")
):
    """
    Obtém quantidade e valor de vendas por corretor
    """
    try:
        api = KommoAPI()
        
        # Buscar usuários
        users_response = api.get_users()
        if not users_response or '_embedded' not in users_response:
            return {"error": "Não foi possível obter usuários", "sales_by_user": {}, "total_sales": {"count": 0, "value": 0}, "period_days": days}
        
        users = users_response.get('_embedded', {}).get('users', [])
        users_dict = {user['id']: user['name'] for user in users}
        
        # Buscar pipelines e status de ganho
        pipelines_response = api.get_pipelines()
        won_statuses = []
        
        if pipelines_response and '_embedded' in pipelines_response:
            for pipeline in pipelines_response.get('_embedded', {}).get('pipelines', []):
                pipeline_id = pipeline['id']
                statuses = api.get_pipeline_statuses(pipeline_id)
                
                if statuses and '_embedded' in statuses:
                    for status in statuses.get('_embedded', {}).get('statuses', []):
                        # Identificar status de ganho dinamicamente
                        status_name = status.get('name') or ''
                        if status.get('type') == 'won' or 'ganho' in status_name.lower() or 'won' in status_name.lower():
                            won_statuses.append(status['id'])
        
        if not won_statuses:
            return {"error": "Nenhum status de ganho encontrado", "sales_by_user": {}, "total_sales": {"count": 0, "value": 0}, "period_days": days}
        
        # Buscar leads ganhos
        params = {
            'filter[statuses]': won_statuses,
            'limit': 250
        }
        
        leads_response = api.get_leads(params)
        sales_by_user = {}
        
        if leads_response and '_embedded' in leads_response:
            leads = leads_response.get('_embedded', {}).get('leads', [])
            
            # Filtrar por período
            cutoff_timestamp = int((datetime.now() - timedelta(days=days)).timestamp())
            
            for lead in leads:
                # Verificar se o lead foi fechado no período desejado
                closed_at = lead.get('closed_at')
                if closed_at is None or closed_at < cutoff_timestamp:
                    continue
                
                user_id = lead.get('responsible_user_id')
                if user_id:
                    user_name = users_dict.get(user_id, f"Usuário {user_id}")
                    
                    if user_name not in sales_by_user:
                        sales_by_user[user_name] = {
                            'count': 0,
                            'total_value': 0
                        }
                    
                    sales_by_user[user_name]['count'] += 1
                    sales_by_user[user_name]['total_value'] += lead.get('price', 0)
        
        # Calcular totais
        total_count = sum(data['count'] for data in sales_by_user.values())
        total_value = sum(data['total_value'] for data in sales_by_user.values())
        
        return {
            "sales_by_user": sales_by_user,
            "total_sales": {
                "count": total_count,
                "value": total_value
            },
            "period_days": days
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao buscar vendas por usuário: {str(e)}")

@router.get("/by-utm")
async def get_sales_by_utm(
    days: int = Query(30, description="Número de dias para análise")
):
    """
    Obtém vendas segmentadas por parâmetros UTM
    """
    try:
        api = KommoAPI()
        
        # Buscar campos personalizados para identificar UTMs
        custom_fields_response = api.get_custom_fields()
        utm_fields = {}
        
        if custom_fields_response and '_embedded' in custom_fields_response:
            for field in custom_fields_response.get('_embedded', {}).get('custom_fields', []):
                field_code = field.get('code') or ''
                if 'utm' in field_code.lower():
                    utm_fields[field['id']] = field['name']
        
        # Buscar pipelines e status de ganho
        pipelines_response = api.get_pipelines()
        won_statuses = []
        
        if pipelines_response and '_embedded' in pipelines_response:
            for pipeline in pipelines_response.get('_embedded', {}).get('pipelines', []):
                pipeline_id = pipeline['id']
                statuses = api.get_pipeline_statuses(pipeline_id)
                
                if statuses and '_embedded' in statuses:
                    for status in statuses.get('_embedded', {}).get('statuses', []):
                        # Identificar status de ganho dinamicamente
                        status_name = status.get('name') or ''
                        if status.get('type') == 'won' or 'ganho' in status_name.lower() or 'won' in status_name.lower():
                            won_statuses.append(status['id'])
        
        if not won_statuses:
            return {"error": "Nenhum status de ganho encontrado", "sales_by_utm": {}, "utm_fields_found": [], "period_days": days}
        
        # Buscar leads ganhos com campos personalizados
        params = {
            'filter[statuses]': won_statuses,
            'with': 'custom_fields_values',
            'limit': 250
        }
        
        leads_response = api.get_leads(params)
        sales_by_utm = {}
        
        if leads_response and '_embedded' in leads_response:
            leads = leads_response.get('_embedded', {}).get('leads', [])
            
            # Filtrar por período
            cutoff_timestamp = int((datetime.now() - timedelta(days=days)).timestamp())
            
            for lead in leads:
                closed_at = lead.get('closed_at')
                if closed_at is None or closed_at < cutoff_timestamp:
                    continue
                
                # Extrair valores UTM
                utm_data = {}
                if 'custom_fields_values' in lead:
                    for custom_field in lead['custom_fields_values']:
                        field_id = custom_field['field_id']
                        if field_id in utm_fields and 'values' in custom_field and custom_field['values']:
                            utm_data[utm_fields[field_id]] = custom_field['values'][0].get('value', '')
                
                # Criar chave combinada dos UTMs
                utm_key = ' | '.join([f"{k}: {v}" for k, v in utm_data.items()]) if utm_data else "Sem UTM"
                
                if utm_key not in sales_by_utm:
                    sales_by_utm[utm_key] = {
                        'count': 0,
                        'total_value': 0,
                        'utm_params': utm_data
                    }
                
                sales_by_utm[utm_key]['count'] += 1
                sales_by_utm[utm_key]['total_value'] += lead.get('price', 0)
        
        return {
            "sales_by_utm": sales_by_utm,
            "utm_fields_found": list(utm_fields.values()),
            "period_days": days
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao buscar vendas por UTM: {str(e)}")