from fastapi import APIRouter, Query, HTTPException
from typing import Optional
from datetime import datetime, timedelta
import traceback
import logging

# Configurar logger
logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v2", tags=["Sales V2 API"])

@router.get("/sales/kpis")
async def get_sales_kpis(
    days: int = Query(30, description="Período em dias para análise"),
    corretor: Optional[str] = Query(None, description="Nome do corretor para filtrar dados"),
    fonte: Optional[str] = Query(None, description="Fonte para filtrar dados"),
    start_date: Optional[str] = Query(None, description="Data de início (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="Data de fim (YYYY-MM-DD)")
):
    """
    Retorna KPIs básicos de vendas otimizado para performance.
    Substitui parte do endpoint pesado /dashboard/sales-complete.
    """
    try:
        logger.info(f"Buscando KPIs de vendas para {days} dias, corretor: {corretor}, fonte: {fonte}")
        
        from app.services.kommo_api import KommoAPI
        kommo_api = KommoAPI()
        
        # Calcular parâmetros de tempo
        import time
        
        if start_date and end_date:
            try:
                start_dt = datetime.strptime(start_date, '%Y-%m-%d')
                end_dt = datetime.strptime(end_date, '%Y-%m-%d')
                end_dt = end_dt.replace(hour=23, minute=59, second=59)
                start_time = int(start_dt.timestamp())
                end_time = int(end_dt.timestamp())
            except ValueError:
                raise HTTPException(status_code=400, detail="Formato de data inválido. Use YYYY-MM-DD")
        else:
            end_time = int(time.time())
            start_time = end_time - (days * 24 * 60 * 60)
        
        # Período anterior para comparação
        period_duration = end_time - start_time
        previous_start_time = start_time - period_duration
        previous_end_time = start_time
        
        # IDs importantes
        PIPELINE_VENDAS = 10516987
        PIPELINE_REMARKETING = 11059911
        STATUS_PROPOSTA = 80689735
        STATUS_CONTRATO_ASSINADO = 80689759
        STATUS_VENDA_FINAL = 142
        CUSTOM_FIELD_DATA_FECHAMENTO = 858126
        
        # ============================================================================
        # IMPLEMENTAÇÃO CONFORME ESPECIFICAÇÃO DO PO - SEPARAÇÃO COMPLETA
        # ============================================================================
        # PROPOSTAS: Filtrar por updated_at + status_proposta (evolução para proposta)
        # VENDAS: Buscar TODOS com status venda + filtrar por data_fechamento no período
        # REUNIÕES: Filtrar por created_at da task (já implementado corretamente)
        # ============================================================================
        
        # PROPOSTAS: Buscar leads que evoluíram para proposta no período
        current_propostas_vendas_params = {
            "filter[pipeline_id]": PIPELINE_VENDAS,  # Funil de Vendas
            "filter[updated_at][from]": start_time,   # PO: usar updated_at para propostas
            "filter[updated_at][to]": end_time,
            "filter[status_id][0]": STATUS_PROPOSTA,  # Propostas
            "filter[status_id][1]": STATUS_CONTRATO_ASSINADO,  # Contrato assinado
            "limit": 250,
            "with": "custom_fields_values"
        }
        
        # VENDAS: Buscar leads com status de venda + filtro temporal amplo para performance
        current_vendas_vendas_params = {
            "filter[pipeline_id]": PIPELINE_VENDAS,  # Funil de Vendas
            "filter[status_id][0]": STATUS_VENDA_FINAL,
            "filter[status_id][1]": STATUS_CONTRATO_ASSINADO,
            "filter[updated_at][from]": start_time - (365 * 24 * 60 * 60),  # 1 ano atrás para dar margem
            "filter[updated_at][to]": end_time,
            "limit": 250,
            "with": "custom_fields_values"
        }
        
        current_leads_remarketing_params = {
            "filter[pipeline_id]": PIPELINE_REMARKETING,  # Remarketing
            "filter[created_at][from]": start_time,  # Usar created_at para leads, igual ao dashboard
            "filter[created_at][to]": end_time,
            "limit": 250,
            "with": "custom_fields_values"
        }
        
        # PROPOSTAS ANTERIORES
        previous_propostas_vendas_params = {
            "filter[pipeline_id]": PIPELINE_VENDAS,  # Funil de Vendas
            "filter[updated_at][from]": previous_start_time,
            "filter[updated_at][to]": previous_end_time,
            "filter[status_id][0]": STATUS_PROPOSTA,  # Propostas
            "filter[status_id][1]": STATUS_CONTRATO_ASSINADO,  # Contrato assinado
            "limit": 250,
            "with": "custom_fields_values"
        }
        
        # VENDAS ANTERIORES
        previous_vendas_vendas_params = {
            "filter[pipeline_id]": PIPELINE_VENDAS,  # Funil de Vendas
            "filter[status_id][0]": STATUS_VENDA_FINAL,
            "filter[status_id][1]": STATUS_CONTRATO_ASSINADO,
            "filter[updated_at][from]": previous_start_time - (365 * 24 * 60 * 60),  # 1 ano atrás para dar margem
            "filter[updated_at][to]": previous_end_time,
            "limit": 250,
            "with": "custom_fields_values"
        }
        
        previous_propostas_remarketing_params = {
            "filter[pipeline_id]": PIPELINE_REMARKETING,  # Remarketing
            "filter[updated_at][from]": previous_start_time,
            "filter[updated_at][to]": previous_end_time,
            "filter[status_id][0]": STATUS_PROPOSTA,  # Propostas
            "filter[status_id][1]": STATUS_CONTRATO_ASSINADO,  # Contrato assinado
            "limit": 250,
            "with": "custom_fields_values"
        }
        
        # LEADS PARA SEÇÃO GERAL (todos os leads criados no período)
        current_leads_vendas_params = {
            "filter[pipeline_id]": PIPELINE_VENDAS,
            "filter[created_at][from]": start_time,  # Usar created_at para leads
            "filter[created_at][to]": end_time,
            "limit": 250,
            "with": "custom_fields_values"
        }
        
        previous_leads_vendas_params = {
            "filter[pipeline_id]": PIPELINE_VENDAS,
            "filter[created_at][from]": previous_start_time,  # Usar created_at para leads
            "filter[created_at][to]": previous_end_time,
            "limit": 250,
            "with": "custom_fields_values"
        }
        
        previous_leads_remarketing_params = {
            "filter[pipeline_id]": PIPELINE_REMARKETING,
            "filter[created_at][from]": previous_start_time,  # Usar created_at para leads, igual ao dashboard
            "filter[created_at][to]": previous_end_time,
            "limit": 250,
            "with": "custom_fields_values"
        }
        
        # Buscar PROPOSTAS do período atual de ambos os pipelines
        try:
            current_propostas_vendas_data = kommo_api.get_leads(current_propostas_vendas_params)
        except Exception as e:
            logger.error(f"Erro ao buscar propostas vendas atuais: {e}")
            current_propostas_vendas_data = {"_embedded": {"leads": []}}
            
        try:
            current_vendas_vendas_data = kommo_api.get_leads(current_vendas_vendas_params)
        except Exception as e:
            logger.error(f"Erro ao buscar vendas vendas atuais: {e}")
            current_vendas_vendas_data = {"_embedded": {"leads": []}}
            
        try:
            current_remarketing_data = kommo_api.get_leads(current_leads_remarketing_params)
        except Exception as e:
            logger.error(f"Erro ao buscar leads remarketing atuais: {e}")
            current_remarketing_data = {"_embedded": {"leads": []}}
            
        try:
            current_leads_vendas_data = kommo_api.get_leads(current_leads_vendas_params)
        except Exception as e:
            logger.error(f"Erro ao buscar todos os leads vendas atuais: {e}")
            current_leads_vendas_data = {"_embedded": {"leads": []}}
            
        # Buscar PROPOSTAS do período anterior
        try:
            previous_propostas_vendas_data = kommo_api.get_leads(previous_propostas_vendas_params)
        except Exception as e:
            logger.error(f"Erro ao buscar propostas vendas anteriores: {e}")
            previous_propostas_vendas_data = {"_embedded": {"leads": []}}
            
        try:
            previous_vendas_vendas_data = kommo_api.get_leads(previous_vendas_vendas_params)
        except Exception as e:
            logger.error(f"Erro ao buscar vendas vendas anteriores: {e}")
            previous_vendas_vendas_data = {"_embedded": {"leads": []}}
            
        try:
            previous_propostas_remarketing_data = kommo_api.get_leads(previous_propostas_remarketing_params)
        except Exception as e:
            logger.error(f"Erro ao buscar propostas remarketing anteriores: {e}")
            previous_propostas_remarketing_data = {"_embedded": {"leads": []}}
        
        # Buscar TODOS os leads do período anterior
        try:
            previous_leads_vendas_data = kommo_api.get_leads(previous_leads_vendas_params)
        except Exception as e:
            logger.error(f"Erro ao buscar leads vendas anteriores: {e}")
            previous_leads_vendas_data = {"_embedded": {"leads": []}}
            
        try:
            previous_leads_remarketing_data = kommo_api.get_leads(previous_leads_remarketing_params)
        except Exception as e:
            logger.error(f"Erro ao buscar leads remarketing anteriores: {e}")
            previous_leads_remarketing_data = {"_embedded": {"leads": []}}
        
        # Combinar PROPOSTAS de ambos os pipelines
        current_propostas_all = []
        if current_propostas_vendas_data and "_embedded" in current_propostas_vendas_data:
            current_propostas_all.extend(current_propostas_vendas_data["_embedded"].get("leads", []))
        
        # Combinar VENDAS (sem adicionar remarketing para vendas)
        current_vendas_all = []
        if current_vendas_vendas_data and "_embedded" in current_vendas_vendas_data:
            current_vendas_all.extend(current_vendas_vendas_data["_embedded"].get("leads", []))
        
        # Combinar TODOS os leads para seção geral
        current_all_leads = []
        if current_leads_vendas_data and "_embedded" in current_leads_vendas_data:
            current_all_leads.extend(current_leads_vendas_data["_embedded"].get("leads", []))
        if current_remarketing_data and "_embedded" in current_remarketing_data:
            current_all_leads.extend(current_remarketing_data["_embedded"].get("leads", []))
            
        previous_all_leads = []
        if previous_leads_vendas_data and "_embedded" in previous_leads_vendas_data:
            previous_all_leads.extend(previous_leads_vendas_data["_embedded"].get("leads", []))
        if previous_leads_remarketing_data and "_embedded" in previous_leads_remarketing_data:
            previous_all_leads.extend(previous_leads_remarketing_data["_embedded"].get("leads", []))
        
        # Criar estruturas de dados compatíveis
        current_leads_data = {"_embedded": {"leads": current_all_leads}}
        previous_leads_data = {"_embedded": {"leads": previous_all_leads}}
        
        # Criar estruturas para propostas e vendas  
        current_propostas_data = {"_embedded": {"leads": current_propostas_all}}
        current_sales_data = {"_embedded": {"leads": current_vendas_all}}
        
        # Combinar propostas anteriores
        previous_propostas_all = []
        if previous_propostas_vendas_data and "_embedded" in previous_propostas_vendas_data:
            previous_propostas_all.extend(previous_propostas_vendas_data["_embedded"].get("leads", []))
        if previous_propostas_remarketing_data and "_embedded" in previous_propostas_remarketing_data:
            previous_propostas_all.extend(previous_propostas_remarketing_data["_embedded"].get("leads", []))
            
        # Combinar vendas anteriores  
        previous_vendas_all = []
        if previous_vendas_vendas_data and "_embedded" in previous_vendas_vendas_data:
            previous_vendas_all.extend(previous_vendas_vendas_data["_embedded"].get("leads", []))
            
        previous_propostas_data = {"_embedded": {"leads": previous_propostas_all}}
        previous_sales_data = {"_embedded": {"leads": previous_vendas_all}}
        
        # Função segura para extrair valor de custom fields
        def get_custom_field_value(lead, field_id):
            """Extrai valor de custom field de forma segura"""
            try:
                custom_fields = lead.get("custom_fields_values")
                if not custom_fields or not isinstance(custom_fields, list):
                    return None
                    
                for field in custom_fields:
                    if not field or not isinstance(field, dict):
                        continue
                    if field.get("field_id") == field_id:
                        values = field.get("values")
                        if values and isinstance(values, list) and len(values) > 0:
                            first_value = values[0]
                            if isinstance(first_value, dict):
                                return first_value.get("value")
                            elif isinstance(first_value, str):
                                return first_value
                return None
            except Exception as e:
                logger.error(f"Erro ao extrair custom field {field_id}: {e}")
                return None
        
        # Função para filtrar e processar leads
        def process_leads(leads_data):
            if not leads_data or not isinstance(leads_data, dict):
                return []
                
            embedded = leads_data.get("_embedded")
            if not embedded or not isinstance(embedded, dict):
                return []
                
            all_leads = embedded.get("leads")
            if not all_leads or not isinstance(all_leads, list):
                return []
                
            filtered_leads = []
            
            for lead in all_leads:
                if not lead or not isinstance(lead, dict):
                    continue
                
                # Extrair valores de forma segura
                corretor_lead = get_custom_field_value(lead, 837920)  # Corretor
                fonte_lead = get_custom_field_value(lead, 837886)     # Fonte
                
                # Aplicar filtros - suporta múltiplos valores separados por vírgula
                if corretor:
                    # Se corretor contém vírgula, é multi-select
                    if ',' in corretor:
                        corretores_list = [c.strip() for c in corretor.split(',')]
                        if corretor_lead not in corretores_list:
                            continue
                    else:
                        # Filtro único
                        if corretor_lead != corretor:
                            continue
                
                if fonte:
                    # Se fonte contém vírgula, é multi-select
                    if ',' in fonte:
                        fontes_list = [f.strip() for f in fonte.split(',')]
                        if fonte_lead not in fontes_list:
                            continue
                    else:
                        # Filtro único
                        if fonte_lead != fonte:
                            continue
                
                filtered_leads.append(lead)
            
            return filtered_leads
        
        # Processar leads atuais e anteriores
        current_leads = process_leads(current_leads_data)
        previous_leads = process_leads(previous_leads_data)
        
        # Função para verificar se venda tem data válida E está no período (ESPECIFICAÇÃO PO)
        def has_valid_sale_date(lead):
            """Verifica se a venda tem Data Fechamento válido E está no período especificado"""
            # APENAS Data Fechamento (custom field) - sem fallback para closed_at
            data_fechamento = get_custom_field_value(lead, CUSTOM_FIELD_DATA_FECHAMENTO)
            if not data_fechamento:
                return False
            
            # Verificar se a data de fechamento está no período especificado (PO)
            try:
                # Assumindo formato timestamp ou data string
                if isinstance(data_fechamento, (int, float)):
                    fechamento_timestamp = int(data_fechamento)
                elif isinstance(data_fechamento, str):
                    # Tentar converter string para timestamp (formato YYYY-MM-DD)
                    fechamento_dt = datetime.strptime(data_fechamento, '%Y-%m-%d')
                    fechamento_timestamp = int(fechamento_dt.timestamp())
                else:
                    return False
                
                # Verificar se está no período - PO: usar data_fechamento para filtrar período
                return start_time <= fechamento_timestamp <= end_time
            except Exception as e:
                logger.error(f"Erro ao processar data_fechamento {data_fechamento}: {e}")
                # Se não conseguir converter, considerar inválida
                return False
        
        # Calcular métricas do período atual
        total_leads = len(current_leads)
        active_leads = len([lead for lead in current_leads if lead.get("status_id") not in [142, 143]])
        
        # Vendas: apenas status de venda + data válida
        won_leads_with_date = [
            lead for lead in current_leads 
            if lead.get("status_id") in [STATUS_VENDA_FINAL, STATUS_CONTRATO_ASSINADO] and has_valid_sale_date(lead)
        ]
        won_leads = len(won_leads_with_date)
        
        lost_leads = len([lead for lead in current_leads if lead.get("status_id") == 143])
        
        # Propostas: apenas status de proposta
        proposal_leads = len([lead for lead in current_leads if lead.get("status_id") == STATUS_PROPOSTA])
        
        # Calcular revenue e average deal size (apenas vendas com data válida)
        total_revenue = sum((lead.get("price") or 0) for lead in won_leads_with_date)
        average_deal_size = (total_revenue / won_leads) if won_leads > 0 else 0
        
        # Calcular win rate
        total_closed = won_leads + lost_leads
        win_rate = (won_leads / total_closed * 100) if total_closed > 0 else 0
        
        # Função para verificar vendas do período anterior
        def has_valid_sale_date_previous(lead):
            """Verifica se a venda tem Data Fechamento válido E está no período anterior"""
            data_fechamento = get_custom_field_value(lead, CUSTOM_FIELD_DATA_FECHAMENTO)
            if not data_fechamento:
                return False
            
            try:
                if isinstance(data_fechamento, (int, float)):
                    fechamento_timestamp = int(data_fechamento)
                elif isinstance(data_fechamento, str):
                    fechamento_dt = datetime.strptime(data_fechamento, '%Y-%m-%d')
                    fechamento_timestamp = int(fechamento_dt.timestamp())
                else:
                    return False
                
                # Verificar se está no período anterior
                return previous_start_time <= fechamento_timestamp <= previous_end_time
            except Exception as e:
                logger.error(f"Erro ao processar data_fechamento anterior {data_fechamento}: {e}")
                return False
        
        # Calcular métricas do período anterior
        previous_total_leads = len(previous_leads)
        previous_active_leads = len([lead for lead in previous_leads if lead.get("status_id") not in [142, 143]])
        
        # Vendas anteriores: apenas status de venda + data válida no período anterior
        previous_won_leads_with_date = [
            lead for lead in previous_leads 
            if lead.get("status_id") in [STATUS_VENDA_FINAL, STATUS_CONTRATO_ASSINADO] and has_valid_sale_date_previous(lead)
        ]
        previous_won_leads = len(previous_won_leads_with_date)
        
        previous_lost_leads = len([lead for lead in previous_leads if lead.get("status_id") == 143])
        previous_proposal_leads = len([lead for lead in previous_leads if lead.get("status_id") == STATUS_PROPOSTA])
        
        previous_total_revenue = sum((lead.get("price") or 0) for lead in previous_won_leads_with_date)
        previous_average_deal_size = (previous_total_revenue / previous_won_leads) if previous_won_leads > 0 else 0
        
        previous_total_closed = previous_won_leads + previous_lost_leads
        previous_win_rate = (previous_won_leads / previous_total_closed * 100) if previous_total_closed > 0 else 0
        
        return {
            "totalLeads": total_leads,
            "activeLeads": active_leads,
            "wonLeads": won_leads,
            "lostLeads": lost_leads,
            "winRate": round(win_rate, 1),
            "averageDealSize": round(average_deal_size, 2),
            "totalRevenue": round(total_revenue, 2),
            "previousTotalLeads": previous_total_leads,
            "previousActiveLeads": previous_active_leads,
            "previousWonLeads": previous_won_leads,
            "previousWinRate": round(previous_win_rate, 1),
            "previousAverageDealSize": round(previous_average_deal_size, 2),
            # NOVO: Campo que o frontend usa
            "proposalStats": {
                "total": proposal_leads,
                "previous": previous_proposal_leads,
                "growth": ((proposal_leads - previous_proposal_leads) / previous_proposal_leads * 100) if previous_proposal_leads > 0 else 0
            },
            "_metadata": {
                "period_days": days,
                "corretor_filter": corretor,
                "fonte_filter": fonte,
                "generated_at": datetime.now().isoformat(),
                "optimized": True,
                "endpoint_version": "v2",
                "pipeline_filter": PIPELINE_VENDAS,
                "status_ids_used": {
                    "proposta": STATUS_PROPOSTA,
                    "vendas": [STATUS_VENDA_FINAL, STATUS_CONTRATO_ASSINADO]
                },
                "data_fechamento_field": CUSTOM_FIELD_DATA_FECHAMENTO
            }
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro ao gerar KPIs de vendas: {str(e)}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")


@router.get("/charts/leads-by-user")
async def get_leads_by_user_chart(
    days: int = Query(30, description="Período em dias para análise"),
    corretor: Optional[str] = Query(None, description="Nome do corretor para filtrar dados"),
    fonte: Optional[str] = Query(None, description="Fonte para filtrar dados"),
    start_date: Optional[str] = Query(None, description="Data de início (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="Data de fim (YYYY-MM-DD)")
):
    """
    Retorna dados otimizados para gráfico de leads por usuário.
    """
    try:
        logger.info(f"Buscando dados de leads por usuário para {days} dias, corretor: {corretor}, fonte: {fonte}")
        
        from app.services.kommo_api import KommoAPI
        kommo_api = KommoAPI()
        
        # Calcular parâmetros de tempo
        import time
        
        if start_date and end_date:
            try:
                start_dt = datetime.strptime(start_date, '%Y-%m-%d')
                end_dt = datetime.strptime(end_date, '%Y-%m-%d')
                end_dt = end_dt.replace(hour=23, minute=59, second=59)
                start_time = int(start_dt.timestamp())
                end_time = int(end_dt.timestamp())
            except ValueError:
                raise HTTPException(status_code=400, detail="Formato de data inválido. Use YYYY-MM-DD")
        else:
            end_time = int(time.time())
            start_time = end_time - (days * 24 * 60 * 60)
        
        # IDs importantes
        PIPELINE_VENDAS = 10516987
        PIPELINE_REMARKETING = 11059911
        STATUS_PROPOSTA = 80689735
        STATUS_CONTRATO_ASSINADO = 80689759
        STATUS_VENDA_FINAL = 142
        CUSTOM_FIELD_DATA_FECHAMENTO = 858126
        
        # Buscar leads de AMBOS os pipelines (Vendas + Remarketing)
        leads_vendas_params = {
            "filter[pipeline_id]": PIPELINE_VENDAS,  # Funil de Vendas
            "filter[created_at][from]": start_time,   # CORREÇÃO: usar created_at para leads (igual detailed-tables)
            "filter[created_at][to]": end_time,
            "limit": 250,
            "with": "custom_fields_values"
        }
        
        leads_remarketing_params = {
            "filter[pipeline_id]": PIPELINE_REMARKETING,  # Remarketing
            "filter[created_at][from]": start_time,   # CORREÇÃO: usar created_at para leads (igual detailed-tables)
            "filter[created_at][to]": end_time,
            "limit": 250,
            "with": "custom_fields_values"
        }
        
        # ADICIONAR: Buscar propostas e vendas para completar o mapa de leads (igual detailed-tables)
        propostas_vendas_params = {
            "filter[pipeline_id]": PIPELINE_VENDAS,
            "filter[updated_at][from]": start_time,   # PO: usar updated_at para propostas
            "filter[updated_at][to]": end_time,
            "filter[status_id][0]": STATUS_PROPOSTA,  # Propostas
            "filter[status_id][1]": STATUS_CONTRATO_ASSINADO,  # Contrato assinado
            "limit": 250,
            "with": "custom_fields_values"
        }
        
        vendas_vendas_params = {
            "filter[pipeline_id]": PIPELINE_VENDAS,
            "filter[status_id][0]": STATUS_VENDA_FINAL,
            "filter[status_id][1]": STATUS_CONTRATO_ASSINADO,
            "filter[updated_at][from]": start_time - (365 * 24 * 60 * 60),  # 1 ano atrás para dar margem
            "filter[updated_at][to]": end_time,
            "limit": 250,
            "with": "custom_fields_values"
        }

        # Buscar tarefas de reunião concluídas
        tasks_params = {
            'filter[task_type]': 2,  # Tipo reunião
            'filter[is_completed]': 1,  # Apenas concluídas
            'filter[created_at][from]': start_time,  # PO: usar created_at para reuniões
            'filter[created_at][to]': end_time,
            'limit': 250
        }
        
        # Buscar leads de vendas
        try:
            leads_vendas_data = kommo_api.get_leads(leads_vendas_params)
        except Exception as e:
            logger.error(f"Erro ao buscar leads de vendas: {e}")
            leads_vendas_data = {"_embedded": {"leads": []}}
            
        # Buscar leads de remarketing
        try:
            leads_remarketing_data = kommo_api.get_leads(leads_remarketing_params)
        except Exception as e:
            logger.error(f"Erro ao buscar leads de remarketing: {e}")
            leads_remarketing_data = {"_embedded": {"leads": []}}
        
        # Combinar leads de ambos os pipelines
        all_leads = []
        if leads_vendas_data and "_embedded" in leads_vendas_data:
            vendas_leads = leads_vendas_data["_embedded"].get("leads", [])
            if isinstance(vendas_leads, list):
                all_leads.extend(vendas_leads)
                
        if leads_remarketing_data and "_embedded" in leads_remarketing_data:
            remarketing_leads = leads_remarketing_data["_embedded"].get("leads", [])
            if isinstance(remarketing_leads, list):
                all_leads.extend(remarketing_leads)
        
        # ADICIONAR: Buscar propostas e vendas para completar mapa de leads (igual detailed-tables)
        try:
            propostas_data = kommo_api.get_leads(propostas_vendas_params)
        except Exception as e:
            logger.error(f"Erro ao buscar propostas: {e}")
            propostas_data = {"_embedded": {"leads": []}}
            
        try:
            vendas_data = kommo_api.get_leads(vendas_vendas_params)
        except Exception as e:
            logger.error(f"Erro ao buscar vendas: {e}")
            vendas_data = {"_embedded": {"leads": []}}
        
        # Log para debug - comparar com detailed-tables
        logger.info(f"[charts/leads-by-user] Total leads encontrados: {len(all_leads)}")
        
        leads_data = {"_embedded": {"leads": all_leads}}
            
        try:
            tasks_data = kommo_api.get_tasks(tasks_params)
        except Exception as e:
            logger.error(f"Erro ao buscar tarefas: {e}")
            tasks_data = {"_embedded": {"tasks": []}}
            
        try:
            users_data = kommo_api.get_users()
        except Exception as e:
            logger.error(f"Erro ao buscar usuários: {e}")
            users_data = {"_embedded": {"users": []}}
        
        # Criar mapa de usuários
        users_map = {}
        if users_data and "_embedded" in users_data:
            users_list = users_data["_embedded"].get("users", [])
            if isinstance(users_list, list):
                for user in users_list:
                    if user and isinstance(user, dict):
                        users_map[user.get("id")] = user.get("name", "Usuário Sem Nome")
        
        # Criar mapa de leads COMPLETO (igual detailed-tables)
        # Combinar TODOS os leads: normais + propostas + vendas
        all_propostas = []
        if propostas_data and "_embedded" in propostas_data:
            all_propostas = propostas_data["_embedded"].get("leads", [])
            
        all_vendas = []
        if vendas_data and "_embedded" in vendas_data:
            all_vendas = vendas_data["_embedded"].get("leads", [])
        
        # Criar mapa combinado (igual detailed-tables linha 1084-1089)
        all_leads_combined = all_propostas + all_vendas + all_leads
        leads_map = {}
        for lead in all_leads_combined:
            if lead and lead.get("id"):
                leads_map[lead.get("id")] = lead
                
        # Log para debug das reuniões
        logger.info(f"[charts/leads-by-user] Leads combinados: normais={len(all_leads)}, propostas={len(all_propostas)}, vendas={len(all_vendas)}, total_mapa={len(leads_map)}")
        
        # Criar mapa de reuniões realizadas por lead (LÓGICA DOS MODAIS)
        meetings_by_lead = {}
        if tasks_data and "_embedded" in tasks_data:
            tasks_list = tasks_data["_embedded"].get("tasks", [])
            if isinstance(tasks_list, list):
                for task in tasks_list:
                    if (task and isinstance(task, dict) and 
                        task.get('entity_type') == 'leads'):
                        lead_id = task.get('entity_id')
                        
                        # ADICIONAR: Validação dupla de data (igual detailed-tables linha 1121)
                        created_at = task.get('created_at')
                        if not created_at:
                            continue
                        if created_at < start_time or created_at > end_time:
                            continue
                        
                        # MODAL: Verificar se o lead existe nos pipelines filtrados
                        if lead_id and lead_id in leads_map:
                            meetings_by_lead[lead_id] = meetings_by_lead.get(lead_id, 0) + 1
                            
        # Log para debug das reuniões encontradas
        total_meetings = sum(meetings_by_lead.values())
        logger.info(f"[charts/leads-by-user] Reuniões mapeadas: {len(meetings_by_lead)} leads com reuniões, total_reuniões={total_meetings}")
        
        # Log adicional: contar total de tasks processadas vs filtradas
        total_tasks = len(tasks_list) if 'tasks_list' in locals() and tasks_list else 0
        logger.info(f"[charts/leads-by-user] Tasks: total_api={total_tasks}, reunioes_validas={total_meetings}, periodo={start_time}-{end_time}")
        
        # Função segura para extrair valor de custom fields
        def get_custom_field_value(lead, field_id):
            """Extrai valor de custom field de forma segura"""
            try:
                custom_fields = lead.get("custom_fields_values")
                if not custom_fields or not isinstance(custom_fields, list):
                    return None
                    
                for field in custom_fields:
                    if not field or not isinstance(field, dict):
                        continue
                    if field.get("field_id") == field_id:
                        values = field.get("values")
                        if values and isinstance(values, list) and len(values) > 0:
                            first_value = values[0]
                            if isinstance(first_value, dict):
                                return first_value.get("value")
                            elif isinstance(first_value, str):
                                return first_value
                return None
            except Exception as e:
                logger.error(f"Erro ao extrair custom field {field_id}: {e}")
                return None
        
        # Processar leads
        leads_by_user = {}
        
        if leads_data and "_embedded" in leads_data:
            leads_list = leads_data["_embedded"].get("leads", [])
            if isinstance(leads_list, list):
                for lead in leads_list:
                    if not lead or not isinstance(lead, dict):
                        continue
                    
                    # Extrair valores de forma segura
                    corretor_lead = get_custom_field_value(lead, 837920)  # Corretor
                    fonte_lead = get_custom_field_value(lead, 837886)     # Fonte
                    
                    # Aplicar filtros - suporta múltiplos valores separados por vírgula
                    if corretor:
                        # Se corretor contém vírgula, é multi-select
                        if ',' in corretor:
                            corretores_list = [c.strip() for c in corretor.split(',')]
                            if corretor_lead not in corretores_list:
                                continue
                        else:
                            # Filtro único
                            if corretor_lead != corretor:
                                continue
                    
                    if fonte:
                        # Se fonte contém vírgula, é multi-select
                        if ',' in fonte:
                            fontes_list = [f.strip() for f in fonte.split(',')]
                            if fonte_lead not in fontes_list:
                                continue
                        else:
                            # Filtro único
                            if fonte_lead != fonte:
                                continue
                    
                    # Determinar corretor final - tratar como "desconhecido" conforme PO
                    final_corretor = corretor_lead or "Desconhecido"
                    
                    
                    # Inicializar contador se não existir
                    if final_corretor not in leads_by_user:
                        leads_by_user[final_corretor] = {
                            "name": final_corretor,
                            "value": 0,
                            "active": 0,
                            "meetingsHeld": 0,  # Campo que o frontend usa
                            "meetings": 0,      # Fallback para compatibilidade
                            "sales": 0,
                            "lost": 0
                        }
                    
                    # Incrementar contadores
                    leads_by_user[final_corretor]["value"] += 1
                    
                    # Verificar se tem Data Fechamento para vendas E está no período (ESPECIFICAÇÃO PO)
                    def has_valid_sale_date_local(lead):
                        data_fechamento = get_custom_field_value(lead, CUSTOM_FIELD_DATA_FECHAMENTO)
                        if not data_fechamento:
                            return False
                        
                        try:
                            if isinstance(data_fechamento, (int, float)):
                                fechamento_timestamp = int(data_fechamento)
                            elif isinstance(data_fechamento, str):
                                fechamento_dt = datetime.strptime(data_fechamento, '%Y-%m-%d')
                                fechamento_timestamp = int(fechamento_dt.timestamp())
                            else:
                                return False
                            
                            # PO: usar data_fechamento para filtrar período de vendas
                            return start_time <= fechamento_timestamp <= end_time
                        except Exception:
                            return False
                    
                    status_id = lead.get("status_id")
                    lead_id = lead.get("id")
                    
                    # Vendas: apenas com data válida E no período
                    if (status_id in [STATUS_VENDA_FINAL, STATUS_CONTRATO_ASSINADO] and 
                        has_valid_sale_date_local(lead)):
                        leads_by_user[final_corretor]["sales"] += 1
                    elif status_id == 143:  # Lost
                        leads_by_user[final_corretor]["lost"] += 1
                    else:  # Active
                        leads_by_user[final_corretor]["active"] += 1
                    
                    # Reuniões realizadas: do mapa de tarefas
                    if lead_id in meetings_by_lead:
                        meetings_count = meetings_by_lead[lead_id]
                        leads_by_user[final_corretor]["meetingsHeld"] += meetings_count
                        leads_by_user[final_corretor]["meetings"] += meetings_count  # Fallback
        
        # Converter para lista e ordenar
        leads_by_user_list = list(leads_by_user.values())
        leads_by_user_list.sort(key=lambda x: x["value"], reverse=True)
        
        # Criar estrutura analyticsTeam esperada pelo frontend
        user_performance = []
        for user_data in leads_by_user_list:
            user_performance.append({
                "user_name": user_data["name"],
                "new_leads": user_data["value"],
                "activities": user_data["meetingsHeld"],
                "won_deals": user_data["sales"]
            })
        
        return {
            "leadsByUser": leads_by_user_list,
            "analyticsTeam": {
                "user_performance": user_performance
            },
            "_metadata": {
                "period_days": days,
                "corretor_filter": corretor,
                "fonte_filter": fonte,
                "generated_at": datetime.now().isoformat(),
                "total_users": len(leads_by_user_list),
                "optimized": True,
                "endpoint_version": "v2",
                "pipeline_filter": PIPELINE_VENDAS,
                "meetings_source": "tasks_completed",
                "sales_validation": "data_fechamento_required",
                "status_ids_used": {
                    "vendas": [STATUS_VENDA_FINAL, STATUS_CONTRATO_ASSINADO]
                },
                "total_meetings_found": sum(meetings_by_lead.values()),
                "total_leads_processed": sum(user["value"] for user in leads_by_user_list)
            }
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro ao gerar dados de leads por usuário: {str(e)}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")


@router.get("/sales/conversion-rates")
async def get_conversion_rates(
    days: int = Query(30, description="Período em dias para análise"),
    corretor: Optional[str] = Query(None, description="Nome do corretor para filtrar dados"),
    fonte: Optional[str] = Query(None, description="Fonte para filtrar dados"),
    start_date: Optional[str] = Query(None, description="Data de início (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="Data de fim (YYYY-MM-DD)")
):
    """
    Retorna taxas de conversão otimizadas com dados de funil.
    """
    try:
        logger.info(f"Buscando taxas de conversão para {days} dias, corretor: {corretor}, fonte: {fonte}")
        
        from app.services.kommo_api import KommoAPI
        kommo_api = KommoAPI()
        
        # Calcular parâmetros de tempo
        import time
        
        if start_date and end_date:
            try:
                start_dt = datetime.strptime(start_date, '%Y-%m-%d')
                end_dt = datetime.strptime(end_date, '%Y-%m-%d')
                end_dt = end_dt.replace(hour=23, minute=59, second=59)
                start_time = int(start_dt.timestamp())
                end_time = int(end_dt.timestamp())
            except ValueError:
                raise HTTPException(status_code=400, detail="Formato de data inválido. Use YYYY-MM-DD")
        else:
            end_time = int(time.time())
            start_time = end_time - (days * 24 * 60 * 60)
        
        # IDs importantes
        PIPELINE_VENDAS = 10516987
        PIPELINE_REMARKETING = 11059911
        STATUS_PROPOSTA = 80689735
        STATUS_CONTRATO_ASSINADO = 80689759
        STATUS_VENDA_FINAL = 142
        CUSTOM_FIELD_DATA_FECHAMENTO = 858126
        
        # Buscar leads de AMBOS os pipelines (Vendas + Remarketing)
        leads_vendas_params = {
            "filter[pipeline_id]": PIPELINE_VENDAS,  # Funil de Vendas
            "filter[updated_at][from]": start_time,   # PO: usar updated_at para propostas
            "filter[updated_at][to]": end_time,
            "limit": 250,
            "with": "custom_fields_values"
        }
        
        leads_remarketing_params = {
            "filter[pipeline_id]": PIPELINE_REMARKETING,  # Remarketing
            "filter[updated_at][from]": start_time,   # PO: usar updated_at para propostas
            "filter[updated_at][to]": end_time,
            "limit": 250,
            "with": "custom_fields_values"
        }
        
        # Buscar tarefas de reunião concluídas
        tasks_params = {
            'filter[task_type]': 2,  # Tipo reunião
            'filter[is_completed]': 1,  # Apenas concluídas
            'filter[created_at][from]': start_time,  # PO: usar created_at para reuniões
            'filter[created_at][to]': end_time,
            'limit': 250
        }
        
        # Buscar leads de vendas
        try:
            leads_vendas_data = kommo_api.get_leads(leads_vendas_params)
        except Exception as e:
            logger.error(f"Erro ao buscar leads de vendas: {e}")
            leads_vendas_data = {"_embedded": {"leads": []}}
            
        # Buscar leads de remarketing
        try:
            leads_remarketing_data = kommo_api.get_leads(leads_remarketing_params)
        except Exception as e:
            logger.error(f"Erro ao buscar leads de remarketing: {e}")
            leads_remarketing_data = {"_embedded": {"leads": []}}
        
        # Combinar leads de ambos os pipelines
        all_leads = []
        if leads_vendas_data and "_embedded" in leads_vendas_data:
            vendas_leads = leads_vendas_data["_embedded"].get("leads", [])
            if isinstance(vendas_leads, list):
                all_leads.extend(vendas_leads)
                
        if leads_remarketing_data and "_embedded" in leads_remarketing_data:
            remarketing_leads = leads_remarketing_data["_embedded"].get("leads", [])
            if isinstance(remarketing_leads, list):
                all_leads.extend(remarketing_leads)
        
        leads_data = {"_embedded": {"leads": all_leads}}
        
        try:
            tasks_data = kommo_api.get_tasks(tasks_params)
        except Exception as e:
            logger.error(f"Erro ao buscar tarefas: {e}")
            tasks_data = {"_embedded": {"tasks": []}}
        
        # Função segura para extrair valor de custom fields
        def get_custom_field_value(lead, field_id):
            """Extrai valor de custom field de forma segura"""
            try:
                custom_fields = lead.get("custom_fields_values")
                if not custom_fields or not isinstance(custom_fields, list):
                    return None
                    
                for field in custom_fields:
                    if not field or not isinstance(field, dict):
                        continue
                    if field.get("field_id") == field_id:
                        values = field.get("values")
                        if values and isinstance(values, list) and len(values) > 0:
                            first_value = values[0]
                            if isinstance(first_value, dict):
                                return first_value.get("value")
                            elif isinstance(first_value, str):
                                return first_value
                return None
            except Exception as e:
                logger.error(f"Erro ao extrair custom field {field_id}: {e}")
                return None
        
        # Criar mapa de leads (LÓGICA DOS MODAIS)
        leads_map = {}
        for lead in all_leads:
            if lead and lead.get("id"):
                leads_map[lead.get("id")] = lead
        
        # Criar mapa de reuniões realizadas por lead (LÓGICA DOS MODAIS)
        meetings_by_lead = {}
        if tasks_data and "_embedded" in tasks_data:
            tasks_list = tasks_data["_embedded"].get("tasks", [])
            if isinstance(tasks_list, list):
                for task in tasks_list:
                    if (task and isinstance(task, dict) and 
                        task.get('entity_type') == 'leads'):
                        lead_id = task.get('entity_id')
                        # MODAL: Verificar se o lead existe nos pipelines filtrados
                        if lead_id and lead_id in leads_map:
                            meetings_by_lead[lead_id] = meetings_by_lead.get(lead_id, 0) + 1
        
        # Processar leads com filtros
        filtered_leads = []
        
        if leads_data and "_embedded" in leads_data:
            leads_list = leads_data["_embedded"].get("leads", [])
            if isinstance(leads_list, list):
                for lead in leads_list:
                    if not lead or not isinstance(lead, dict):
                        continue
                    
                    # Extrair valores de forma segura
                    corretor_lead = get_custom_field_value(lead, 837920)  # Corretor
                    fonte_lead = get_custom_field_value(lead, 837886)     # Fonte
                    
                    # Aplicar filtros - suporta múltiplos valores separados por vírgula
                    if corretor:
                        # Se corretor contém vírgula, é multi-select
                        if ',' in corretor:
                            corretores_list = [c.strip() for c in corretor.split(',')]
                            if corretor_lead not in corretores_list:
                                continue
                        else:
                            # Filtro único
                            if corretor_lead != corretor:
                                continue
                    
                    if fonte:
                        # Se fonte contém vírgula, é multi-select
                        if ',' in fonte:
                            fontes_list = [f.strip() for f in fonte.split(',')]
                            if fonte_lead not in fontes_list:
                                continue
                        else:
                            # Filtro único
                            if fonte_lead != fonte:
                                continue
                    
                    
                    filtered_leads.append(lead)
        
        # Função para verificar se venda tem data válida E está no período (ESPECIFICAÇÃO PO)
        def has_valid_sale_date(lead):
            """Verifica se a venda tem Data Fechamento válido E está no período especificado"""
            data_fechamento = get_custom_field_value(lead, CUSTOM_FIELD_DATA_FECHAMENTO)
            if not data_fechamento:
                return False
            
            try:
                if isinstance(data_fechamento, (int, float)):
                    fechamento_timestamp = int(data_fechamento)
                elif isinstance(data_fechamento, str):
                    fechamento_dt = datetime.strptime(data_fechamento, '%Y-%m-%d')
                    fechamento_timestamp = int(fechamento_dt.timestamp())
                else:
                    return False
                
                # PO: usar data_fechamento para filtrar período de vendas
                return start_time <= fechamento_timestamp <= end_time
            except Exception:
                return False
        
        # Calcular métricas de conversão com nova lógica
        total_leads = len(filtered_leads)
        
        # Reuniões: contar leads que tiveram reunião realizada (do mapa de tarefas)
        meetings_leads = len([lead for lead in filtered_leads if lead.get("id") in meetings_by_lead])
        
        # Propostas: apenas status de proposta
        proposals_leads = len([lead for lead in filtered_leads if lead.get("status_id") == STATUS_PROPOSTA])
        
        # Vendas: apenas status de venda + data válida
        sales_leads = len([
            lead for lead in filtered_leads 
            if (lead.get("status_id") in [STATUS_VENDA_FINAL, STATUS_CONTRATO_ASSINADO] and 
                has_valid_sale_date(lead))
        ])
        
        # Calcular taxas de conversão
        meetings_rate = (meetings_leads / total_leads * 100) if total_leads > 0 else 0
        proposals_rate = (proposals_leads / meetings_leads * 100) if meetings_leads > 0 else 0
        sales_rate = (sales_leads / proposals_leads * 100) if proposals_leads > 0 else 0
        
        # Dados do funil
        funnel_data = [
            {"stage": "Leads", "value": total_leads, "rate": 100},
            {"stage": "Reuniões", "value": meetings_leads, "rate": round(meetings_rate, 1)},
            {"stage": "Propostas", "value": proposals_leads, "rate": round(proposals_rate, 1)},
            {"stage": "Vendas", "value": sales_leads, "rate": round(sales_rate, 1)}
        ]
        
        return {
            "conversionRates": {
                "meetings": round(meetings_rate, 1),
                "prospects": round(proposals_rate, 1),
                "sales": round(sales_rate, 1)
            },
            "funnelData": funnel_data,
            "_metadata": {
                "period_days": days,
                "corretor_filter": corretor,
                "fonte_filter": fonte,
                "generated_at": datetime.now().isoformat(),
                "total_leads_analyzed": total_leads,
                "optimized": True,
                "endpoint_version": "v2",
                "pipeline_filter": PIPELINE_VENDAS,
                "meetings_source": "tasks_completed",
                "sales_validation": "data_fechamento_required",
                "status_ids_used": {
                    "proposta": STATUS_PROPOSTA,
                    "vendas": [STATUS_VENDA_FINAL, STATUS_CONTRATO_ASSINADO]
                },
                "breakdown": {
                    "total_leads": total_leads,
                    "meetings_leads": meetings_leads,
                    "proposals_leads": proposals_leads,
                    "sales_leads": sales_leads,
                    "total_meetings_found": sum(meetings_by_lead.values())
                }
            }
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro ao gerar taxas de conversão: {str(e)}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")


@router.get("/sales/pipeline-status")
async def get_pipeline_status(
    days: int = Query(30, description="Período em dias para análise"),
    corretor: Optional[str] = Query(None, description="Nome do corretor para filtrar dados"),
    fonte: Optional[str] = Query(None, description="Fonte para filtrar dados"),
    start_date: Optional[str] = Query(None, description="Data de início (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="Data de fim (YYYY-MM-DD)")
):
    """
    Retorna status do pipeline otimizado para dashboard.
    """
    try:
        logger.info(f"Buscando status do pipeline para {days} dias, corretor: {corretor}, fonte: {fonte}")
        
        from app.services.kommo_api import KommoAPI
        kommo_api = KommoAPI()
        
        # Calcular parâmetros de tempo
        import time
        
        if start_date and end_date:
            try:
                start_dt = datetime.strptime(start_date, '%Y-%m-%d')
                end_dt = datetime.strptime(end_date, '%Y-%m-%d')
                end_dt = end_dt.replace(hour=23, minute=59, second=59)
                start_time = int(start_dt.timestamp())
                end_time = int(end_dt.timestamp())
            except ValueError:
                raise HTTPException(status_code=400, detail="Formato de data inválido. Use YYYY-MM-DD")
        else:
            end_time = int(time.time())
            start_time = end_time - (days * 24 * 60 * 60)
        
        # IDs importantes
        PIPELINE_VENDAS = 10516987
        PIPELINE_REMARKETING = 11059911
        STATUS_PROPOSTA = 80689735
        STATUS_CONTRATO_ASSINADO = 80689759
        STATUS_VENDA_FINAL = 142
        CUSTOM_FIELD_DATA_FECHAMENTO = 858126
        
        # Buscar leads de AMBOS os pipelines (Vendas + Remarketing)
        leads_vendas_params = {
            "filter[pipeline_id]": PIPELINE_VENDAS,  # Funil de Vendas
            "filter[updated_at][from]": start_time,   # PO: usar updated_at para propostas
            "filter[updated_at][to]": end_time,
            "limit": 250,
            "with": "custom_fields_values"
        }
        
        leads_remarketing_params = {
            "filter[pipeline_id]": PIPELINE_REMARKETING,  # Remarketing
            "filter[updated_at][from]": start_time,   # PO: usar updated_at para propostas
            "filter[updated_at][to]": end_time,
            "limit": 250,
            "with": "custom_fields_values"
        }
        
        # Buscar leads de vendas
        try:
            leads_vendas_data = kommo_api.get_leads(leads_vendas_params)
        except Exception as e:
            logger.error(f"Erro ao buscar leads de vendas: {e}")
            leads_vendas_data = {"_embedded": {"leads": []}}
            
        # Buscar leads de remarketing
        try:
            leads_remarketing_data = kommo_api.get_leads(leads_remarketing_params)
        except Exception as e:
            logger.error(f"Erro ao buscar leads de remarketing: {e}")
            leads_remarketing_data = {"_embedded": {"leads": []}}
        
        # Combinar leads de ambos os pipelines
        all_leads = []
        if leads_vendas_data and "_embedded" in leads_vendas_data:
            vendas_leads = leads_vendas_data["_embedded"].get("leads", [])
            if isinstance(vendas_leads, list):
                all_leads.extend(vendas_leads)
                
        if leads_remarketing_data and "_embedded" in leads_remarketing_data:
            remarketing_leads = leads_remarketing_data["_embedded"].get("leads", [])
            if isinstance(remarketing_leads, list):
                all_leads.extend(remarketing_leads)
        
        leads_data = {"_embedded": {"leads": all_leads}}
        
        try:
            pipelines_data = kommo_api.get_pipelines()
        except Exception as e:
            logger.error(f"Erro ao buscar pipelines: {e}")
            pipelines_data = {"_embedded": {"pipelines": []}}
        
        # Mapear status IDs para nomes (foco no Funil de Vendas)
        status_map = {}
        if pipelines_data and "_embedded" in pipelines_data:
            pipelines_list = pipelines_data["_embedded"].get("pipelines", [])
            if isinstance(pipelines_list, list):
                for pipeline in pipelines_list:
                    if (pipeline and isinstance(pipeline, dict) and 
                        pipeline.get("id") == PIPELINE_VENDAS):  # Apenas Funil de Vendas
                        embedded_statuses = pipeline.get("_embedded", {})
                        if isinstance(embedded_statuses, dict):
                            statuses = embedded_statuses.get("statuses", [])
                            if isinstance(statuses, list):
                                for status in statuses:
                                    if status and isinstance(status, dict):
                                        status_id = status.get("id")
                                        status_name = status.get("name", f"Status {status_id}")
                                        if status_id:
                                            status_map[status_id] = status_name
                        break  # Encontrou o pipeline, pode parar
        
        # Função segura para extrair valor de custom fields
        def get_custom_field_value(lead, field_id):
            """Extrai valor de custom field de forma segura"""
            try:
                custom_fields = lead.get("custom_fields_values")
                if not custom_fields or not isinstance(custom_fields, list):
                    return None
                    
                for field in custom_fields:
                    if not field or not isinstance(field, dict):
                        continue
                    if field.get("field_id") == field_id:
                        values = field.get("values")
                        if values and isinstance(values, list) and len(values) > 0:
                            first_value = values[0]
                            if isinstance(first_value, dict):
                                return first_value.get("value")
                            elif isinstance(first_value, str):
                                return first_value
                return None
            except Exception as e:
                logger.error(f"Erro ao extrair custom field {field_id}: {e}")
                return None
        
        # Função para verificar se venda tem data válida E está no período (ESPECIFICAÇÃO PO)
        def has_valid_sale_date(lead):
            """Verifica se a venda tem Data Fechamento válido E está no período especificado"""
            data_fechamento = get_custom_field_value(lead, CUSTOM_FIELD_DATA_FECHAMENTO)
            if not data_fechamento:
                return False
            
            try:
                if isinstance(data_fechamento, (int, float)):
                    fechamento_timestamp = int(data_fechamento)
                elif isinstance(data_fechamento, str):
                    fechamento_dt = datetime.strptime(data_fechamento, '%Y-%m-%d')
                    fechamento_timestamp = int(fechamento_dt.timestamp())
                else:
                    return False
                
                # PO: usar data_fechamento para filtrar período de vendas
                return start_time <= fechamento_timestamp <= end_time
            except Exception:
                return False
        
        # Processar leads com filtros
        pipeline_status = {}
        total_in_pipeline = 0
        
        if leads_data and "_embedded" in leads_data:
            leads_list = leads_data["_embedded"].get("leads", [])
            if isinstance(leads_list, list):
                for lead in leads_list:
                    if not lead or not isinstance(lead, dict):
                        continue
                    
                    # Extrair valores de forma segura
                    corretor_lead = get_custom_field_value(lead, 837920)  # Corretor
                    fonte_lead = get_custom_field_value(lead, 837886)     # Fonte
                    
                    # Aplicar filtros - suporta múltiplos valores separados por vírgula
                    if corretor:
                        # Se corretor contém vírgula, é multi-select
                        if ',' in corretor:
                            corretores_list = [c.strip() for c in corretor.split(',')]
                            if corretor_lead not in corretores_list:
                                continue
                        else:
                            # Filtro único
                            if corretor_lead != corretor:
                                continue
                    
                    if fonte:
                        # Se fonte contém vírgula, é multi-select
                        if ',' in fonte:
                            fontes_list = [f.strip() for f in fonte.split(',')]
                            if fonte_lead not in fontes_list:
                                continue
                        else:
                            # Filtro único
                            if fonte_lead != fonte:
                                continue
                    
                    
                    status_id = lead.get("status_id")
                    status_name = status_map.get(status_id, f"Status {status_id}")
                    
                    # Agrupar usando status específicos do Funil de Vendas
                    if status_id == STATUS_PROPOSTA:
                        grouped_status = "Proposta"
                    elif status_id in [STATUS_CONTRATO_ASSINADO, STATUS_VENDA_FINAL]:
                        # Verificar se venda tem data válida
                        if has_valid_sale_date(lead):
                            grouped_status = "Venda Concluída"
                        else:
                            grouped_status = "Venda sem Data"
                    elif status_id == 143:  # Closed - lost
                        grouped_status = "Lead Perdido"
                    elif "agend" in status_name.lower():
                        grouped_status = "Agendamento"
                    elif "reunião" in status_name.lower():
                        grouped_status = "Reunião Realizada"
                    elif "atendimento" in status_name.lower():
                        grouped_status = "Atendimento"
                    elif "contato" in status_name.lower():
                        grouped_status = "Contato Feito"
                    elif "follow" in status_name.lower():
                        grouped_status = "Follow-up"
                    elif "novo" in status_name.lower():
                        grouped_status = "Lead Novo"
                    elif "acompanhamento" in status_name.lower():
                        grouped_status = "Acompanhamento"
                    else:
                        grouped_status = status_name
                    
                    pipeline_status[grouped_status] = pipeline_status.get(grouped_status, 0) + 1
                    total_in_pipeline += 1
        
        # Converter para formato de resposta
        pipeline_status_list = [
            {"name": name, "value": count}
            for name, count in sorted(pipeline_status.items(), key=lambda x: x[1], reverse=True)
        ]
        
        return {
            "pipelineStatus": pipeline_status_list,
            "totalInPipeline": total_in_pipeline,
            "_metadata": {
                "period_days": days,
                "corretor_filter": corretor,
                "fonte_filter": fonte,
                "generated_at": datetime.now().isoformat(),
                "status_groups": len(pipeline_status_list),
                "optimized": True,
                "endpoint_version": "v2",
                "pipeline_filter": PIPELINE_VENDAS,
                "sales_validation": "data_fechamento_required",
                "status_ids_used": {
                    "proposta": STATUS_PROPOSTA,
                    "vendas": [STATUS_VENDA_FINAL, STATUS_CONTRATO_ASSINADO],
                    "perdido": 143
                },
                "breakdown": {
                    "leads_processed": total_in_pipeline,
                    "unique_statuses": len(status_map),
                    "grouped_categories": len(pipeline_status_list)
                }
            }
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro ao gerar status do pipeline: {str(e)}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")


@router.get("/debug/sources")
async def debug_sources_data(
    days: int = Query(30, description="Período em dias para análise"),
    fonte: Optional[str] = Query(None, description="Fonte para debug"),
    start_date: Optional[str] = Query(None, description="Data de início (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="Data de fim (YYYY-MM-DD)")
):
    """
    Endpoint de debug para verificar quais fontes existem nos dados
    """
    try:
        logger.info(f"DEBUG: Buscando fontes para debug, filtro: {fonte}")
        
        from app.services.kommo_api import KommoAPI
        kommo_api = KommoAPI()
        
        # Calcular parâmetros de tempo
        import time
        
        if start_date and end_date:
            try:
                start_dt = datetime.strptime(start_date, '%Y-%m-%d')
                end_dt = datetime.strptime(end_date, '%Y-%m-%d')
                end_dt = end_dt.replace(hour=23, minute=59, second=59)
                start_time = int(start_dt.timestamp())
                end_time = int(end_dt.timestamp())
            except ValueError:
                raise HTTPException(status_code=400, detail="Formato de data inválido. Use YYYY-MM-DD")
        else:
            end_time = int(time.time())
            start_time = end_time - (days * 24 * 60 * 60)
        
        # IDs importantes
        PIPELINE_VENDAS = 10516987
        
        # Buscar leads
        leads_params = {
            "filter[pipeline_id]": PIPELINE_VENDAS,
            "filter[updated_at][from]": start_time,   # PO: usar updated_at para propostas
            "filter[updated_at][to]": end_time,
            "limit": 50,  # Apenas alguns para debug
            "with": "custom_fields_values"
        }
        
        try:
            leads_data = kommo_api.get_leads(leads_params)
        except Exception as e:
            logger.error(f"Erro ao buscar leads: {e}")
            leads_data = {"_embedded": {"leads": []}}
        
        # Função para extrair valor de custom fields
        def get_custom_field_value(lead, field_id):
            try:
                custom_fields = lead.get("custom_fields_values")
                if not custom_fields or not isinstance(custom_fields, list):
                    return None
                    
                for field in custom_fields:
                    if not field or not isinstance(field, dict):
                        continue
                    if field.get("field_id") == field_id:
                        values = field.get("values")
                        if values and isinstance(values, list) and len(values) > 0:
                            first_value = values[0]
                            if isinstance(first_value, dict):
                                return first_value.get("value")
                            elif isinstance(first_value, str):
                                return first_value
                return None
            except Exception as e:
                logger.error(f"Erro ao extrair custom field {field_id}: {e}")
                return None
        
        # Analisar dados
        sources_found = {}
        total_leads = 0
        leads_with_source = 0
        leads_without_source = 0
        
        if leads_data and "_embedded" in leads_data:
            leads_list = leads_data["_embedded"].get("leads", [])
            if isinstance(leads_list, list):
                total_leads = len(leads_list)
                
                for lead in leads_list:
                    if not lead or not isinstance(lead, dict):
                        continue
                    
                    fonte_lead = get_custom_field_value(lead, 837886)  # Fonte
                    
                    if fonte_lead:
                        leads_with_source += 1
                        sources_found[fonte_lead] = sources_found.get(fonte_lead, 0) + 1
                    else:
                        leads_without_source += 1
        
        # Se fonte foi especificada, testar filtro
        filtro_resultado = None
        if fonte:
            if ',' in fonte:
                fontes_list = [f.strip() for f in fonte.split(',')]
                filtro_resultado = {
                    "tipo": "multi-select",
                    "fontes_solicitadas": fontes_list,
                    "fontes_encontradas": list(sources_found.keys()),
                    "matches": [f for f in fontes_list if f in sources_found],
                    "total_leads_que_passariam": sum(sources_found.get(f, 0) for f in fontes_list)
                }
            else:
                filtro_resultado = {
                    "tipo": "single",
                    "fonte_solicitada": fonte,
                    "fonte_existe": fonte in sources_found,
                    "total_leads_que_passariam": sources_found.get(fonte, 0)
                }
        
        return {
            "debug_info": {
                "period_days": days,
                "start_date": start_date,
                "end_date": end_date,
                "filter_fonte": fonte,
                "pipeline_id": PIPELINE_VENDAS,
                "total_leads_analisados": total_leads,
                "leads_com_fonte": leads_with_source,
                "leads_sem_fonte": leads_without_source
            },
            "sources_encontradas": sources_found,
            "filtro_teste": filtro_resultado,
            "sugestoes": {
                "fontes_disponiveis": list(sources_found.keys()),
                "total_por_fonte": sources_found
            }
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro no debug de fontes: {str(e)}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")