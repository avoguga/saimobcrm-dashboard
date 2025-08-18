from fastapi import APIRouter, Query, HTTPException
from typing import Optional
from datetime import datetime, timedelta
import traceback
import logging
from app.utils.date_helpers import validate_sale_in_period, get_lead_closure_date, extract_custom_field_value as extract_field, is_date_in_period

# Configurar logger
logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v2", tags=["Sales V2 API"])

# Função auxiliar para extrair valores de custom fields
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
                
                # Para reuniões: incluir 23:59 do dia anterior para capturar reuniões agendadas na virada do dia
                meetings_start_dt = start_dt - timedelta(days=1)
                meetings_start_dt = meetings_start_dt.replace(hour=23, minute=59, second=0)
                meetings_start_time = int(meetings_start_dt.timestamp())
            except ValueError:
                raise HTTPException(status_code=400, detail="Formato de data inválido. Use YYYY-MM-DD")
        else:
            end_time = int(time.time())
            start_time = end_time - (days * 24 * 60 * 60)
            
            # Para reuniões: incluir 23:59 do dia anterior
            meetings_start_time = start_time - (24 * 60 * 60) + (23 * 60 * 60 + 59 * 60)  # -1 dia + 23:59
        
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
        CUSTOM_FIELD_ESTADO = 851638  # Campo ESTADO
        
        # ============================================================================
        # IMPLEMENTAÇÃO CONFORME ESPECIFICAÇÃO DO PO - SEPARAÇÃO COMPLETA
        # ============================================================================
        # PROPOSTAS: Filtrar por updated_at + status_proposta (evolução para proposta)
        # VENDAS: Buscar TODOS com status venda + filtrar por data_fechamento no período
        # REUNIÕES: Filtrar por created_at da task (já implementado corretamente)
        # ============================================================================
        
        # PROPOSTAS: Buscar TODOS os leads e filtrar por campo ESTADO = "Proposta Feita"
        current_propostas_vendas_params = {
            "filter[pipeline_id]": PIPELINE_VENDAS,  # Funil de Vendas
            "filter[updated_at][from]": start_time,   # PO: usar updated_at para propostas
            "filter[updated_at][to]": end_time,
            "limit": 500,  # AUMENTAR limite para evitar perder dados
            "with": "contacts,tags,custom_fields_values"  # CORREÇÃO: usar mesmo parâmetro que detailed-tables
        }
        
        # ADICIONAR: propostas do REMARKETING para período atual
        current_propostas_remarketing_params = {
            "filter[pipeline_id]": PIPELINE_REMARKETING,
            "filter[updated_at][from]": start_time,   # PO: usar updated_at para propostas
            "filter[updated_at][to]": end_time,
            "limit": 500,  # AUMENTAR limite para evitar perder dados
            "with": "contacts,tags,custom_fields_values"  # CORREÇÃO: usar mesmo parâmetro que detailed-tables
        }
        
        # VENDAS: Buscar leads com status de venda - CORREÇÃO: usar formato correto da API
        current_vendas_vendas_params = {
            "filter[statuses][0][pipeline_id]": PIPELINE_VENDAS,
            "filter[statuses][0][status_id]": STATUS_VENDA_FINAL,
            "filter[statuses][1][pipeline_id]": PIPELINE_VENDAS,
            "filter[statuses][1][status_id]": STATUS_CONTRATO_ASSINADO,
            "limit": 500,
            "with": "custom_fields_values"
        }
        
        current_leads_remarketing_params = {
            "filter[pipeline_id]": PIPELINE_REMARKETING,  # Remarketing
            "filter[created_at][from]": start_time,  # Usar created_at para leads, igual ao dashboard
            "filter[created_at][to]": end_time,
            "limit": 500,  # AUMENTAR limite para evitar perder dados
            "with": "custom_fields_values"
        }
        
        # PROPOSTAS ANTERIORES
        previous_propostas_vendas_params = {
            "filter[pipeline_id]": PIPELINE_VENDAS,  # Funil de Vendas
            "filter[updated_at][from]": previous_start_time,
            "filter[updated_at][to]": previous_end_time,
            "limit": 500,  # AUMENTAR limite para evitar perder dados
            "with": "contacts,tags,custom_fields_values"  # CORREÇÃO: usar mesmo parâmetro que detailed-tables
        }
        
        # VENDAS ANTERIORES - CORREÇÃO: usar formato correto da API
        previous_vendas_vendas_params = {
            "filter[statuses][0][pipeline_id]": PIPELINE_VENDAS,
            "filter[statuses][0][status_id]": STATUS_VENDA_FINAL,
            "filter[statuses][1][pipeline_id]": PIPELINE_VENDAS,
            "filter[statuses][1][status_id]": STATUS_CONTRATO_ASSINADO,
            "limit": 500,
            "with": "custom_fields_values"
        }
        
        previous_propostas_remarketing_params = {
            "filter[pipeline_id]": PIPELINE_REMARKETING,  # Remarketing
            "filter[updated_at][from]": previous_start_time,
            "filter[updated_at][to]": previous_end_time,
            "limit": 500,  # AUMENTAR limite para evitar perder dados
            "with": "contacts,tags,custom_fields_values"  # CORREÇÃO: usar mesmo parâmetro que detailed-tables
        }
        
        # LEADS PARA SEÇÃO GERAL (todos os leads criados no período)
        current_leads_vendas_params = {
            "filter[pipeline_id]": PIPELINE_VENDAS,
            "filter[created_at][from]": start_time,  # Usar created_at para leads
            "filter[created_at][to]": end_time,
            "limit": 500,  # AUMENTAR limite para evitar perder dados
            "with": "custom_fields_values"
        }
        
        previous_leads_vendas_params = {
            "filter[pipeline_id]": PIPELINE_VENDAS,
            "filter[created_at][from]": previous_start_time,  # Usar created_at para leads
            "filter[created_at][to]": previous_end_time,
            "limit": 500,  # AUMENTAR limite para evitar perder dados
            "with": "custom_fields_values"
        }
        
        previous_leads_remarketing_params = {
            "filter[pipeline_id]": PIPELINE_REMARKETING,
            "filter[created_at][from]": previous_start_time,  # Usar created_at para leads, igual ao dashboard
            "filter[created_at][to]": previous_end_time,
            "limit": 500,  # AUMENTAR limite para evitar perder dados
            "with": "custom_fields_values"
        }
        
        # Buscar PROPOSTAS do período atual de ambos os pipelines - USAR PAGINAÇÃO
        try:
            # Remover limit para usar get_all_leads
            current_propostas_vendas_params_all = {k: v for k, v in current_propostas_vendas_params.items() if k != 'limit'}
            logger.info(f"DEBUG PAGINAÇÃO: Chamando get_all_leads com params: {current_propostas_vendas_params_all}")
            current_propostas_vendas_leads = kommo_api.get_all_leads(current_propostas_vendas_params_all)
            logger.info(f"DEBUG PAGINAÇÃO: get_all_leads retornou {len(current_propostas_vendas_leads)} leads")
            current_propostas_vendas_data = {"_embedded": {"leads": current_propostas_vendas_leads}}
        except Exception as e:
            logger.error(f"Erro ao buscar propostas vendas atuais: {e}")
            current_propostas_vendas_data = {"_embedded": {"leads": []}}
            
        # ADICIONAR: propostas do REMARKETING para período atual - USAR PAGINAÇÃO
        try:
            # Remover limit para usar get_all_leads
            current_propostas_remarketing_params_all = {k: v for k, v in current_propostas_remarketing_params.items() if k != 'limit'}
            current_propostas_remarketing_leads = kommo_api.get_all_leads(current_propostas_remarketing_params_all)
            current_propostas_remarketing_data = {"_embedded": {"leads": current_propostas_remarketing_leads}}
        except Exception as e:
            logger.error(f"Erro ao buscar propostas remarketing atuais: {e}")
            current_propostas_remarketing_data = {"_embedded": {"leads": []}}
            
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
            
        # Buscar PROPOSTAS do período anterior - USAR PAGINAÇÃO
        try:
            # Remover limit para usar get_all_leads
            previous_propostas_vendas_params_all = {k: v for k, v in previous_propostas_vendas_params.items() if k != 'limit'}
            previous_propostas_vendas_leads = kommo_api.get_all_leads(previous_propostas_vendas_params_all)
            previous_propostas_vendas_data = {"_embedded": {"leads": previous_propostas_vendas_leads}}
        except Exception as e:
            logger.error(f"Erro ao buscar propostas vendas anteriores: {e}")
            previous_propostas_vendas_data = {"_embedded": {"leads": []}}
            
        try:
            previous_vendas_vendas_data = kommo_api.get_leads(previous_vendas_vendas_params)
        except Exception as e:
            logger.error(f"Erro ao buscar vendas vendas anteriores: {e}")
            previous_vendas_vendas_data = {"_embedded": {"leads": []}}
            
        try:
            # Remover limit para usar get_all_leads
            previous_propostas_remarketing_params_all = {k: v for k, v in previous_propostas_remarketing_params.items() if k != 'limit'}
            previous_propostas_remarketing_leads = kommo_api.get_all_leads(previous_propostas_remarketing_params_all)
            previous_propostas_remarketing_data = {"_embedded": {"leads": previous_propostas_remarketing_leads}}
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
        
        # Combinar PROPOSTAS de ambos os pipelines - filtrar por campo ESTADO = "Proposta Feita"
        current_propostas_all = []
        
        # DEBUG: Ver quantos leads vieram da busca
        logger.info(f"DEBUG PROPOSTAS: current_propostas_vendas_data tem dados? {bool(current_propostas_vendas_data and '_embedded' in current_propostas_vendas_data)}")
        
        # Filtrar propostas do VENDAS por campo ESTADO
        if current_propostas_vendas_data and "_embedded" in current_propostas_vendas_data:
            vendas_leads = current_propostas_vendas_data["_embedded"].get("leads", [])
            logger.info(f"DEBUG PROPOSTAS: {len(vendas_leads)} leads encontrados no pipeline vendas")
            
            # DEBUG: Ver valores do campo ESTADO e contar propostas
            estados_encontrados = {}
            propostas_exatas = 0
            for lead in vendas_leads:
                if lead:
                    estado = get_custom_field_value(lead, CUSTOM_FIELD_ESTADO)
                    if estado:
                        estados_encontrados[estado] = estados_encontrados.get(estado, 0) + 1
                        if estado == "Proposta Feita":
                            propostas_exatas += 1
            
            logger.info(f"DEBUG PROPOSTAS: Estados encontrados: {estados_encontrados}")
            logger.info(f"DEBUG PROPOSTAS: Propostas exatas encontradas: {propostas_exatas}")
            
            for lead in vendas_leads:
                if lead and get_custom_field_value(lead, CUSTOM_FIELD_ESTADO) == "Proposta Feita":
                    current_propostas_all.append(lead)
        
        # Filtrar propostas do REMARKETING por campo ESTADO
        if current_propostas_remarketing_data and "_embedded" in current_propostas_remarketing_data:
            remarketing_leads = current_propostas_remarketing_data["_embedded"].get("leads", [])
            logger.info(f"DEBUG PROPOSTAS: {len(remarketing_leads)} leads encontrados no pipeline remarketing")
            
            for lead in remarketing_leads:
                if lead and get_custom_field_value(lead, CUSTOM_FIELD_ESTADO) == "Proposta Feita":
                    current_propostas_all.append(lead)
        
        logger.info(f"DEBUG PROPOSTAS: Total de propostas após filtrar por ESTADO='Proposta Feita': {len(current_propostas_all)}")
        
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
        
        # Combinar propostas anteriores - filtrar por campo ESTADO = "Proposta Feita"
        previous_propostas_all = []
        
        # Filtrar propostas anteriores do VENDAS por campo ESTADO
        if previous_propostas_vendas_data and "_embedded" in previous_propostas_vendas_data:
            vendas_leads = previous_propostas_vendas_data["_embedded"].get("leads", [])
            for lead in vendas_leads:
                if lead and get_custom_field_value(lead, CUSTOM_FIELD_ESTADO) == "Proposta Feita":
                    previous_propostas_all.append(lead)
        
        # Filtrar propostas anteriores do REMARKETING por campo ESTADO
        if previous_propostas_remarketing_data and "_embedded" in previous_propostas_remarketing_data:
            remarketing_leads = previous_propostas_remarketing_data["_embedded"].get("leads", [])
            for lead in remarketing_leads:
                if lead and get_custom_field_value(lead, CUSTOM_FIELD_ESTADO) == "Proposta Feita":
                    previous_propostas_all.append(lead)
            
        # Combinar vendas anteriores  
        previous_vendas_all = []
        if previous_vendas_vendas_data and "_embedded" in previous_vendas_vendas_data:
            previous_vendas_all.extend(previous_vendas_vendas_data["_embedded"].get("leads", []))
            
        previous_propostas_data = {"_embedded": {"leads": previous_propostas_all}}
        previous_sales_data = {"_embedded": {"leads": previous_vendas_all}}
        
        
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
        
        # Usar função centralizada para validação de vendas
        
        # Calcular métricas do período atual
        total_leads = len(current_leads)
        active_leads = len([lead for lead in current_leads if lead.get("status_id") not in [142, 143]])
        
        # Vendas: aplicar filtros de corretor e fonte nas vendas
        logger.info(f"DEBUG: current_vendas_all tem {len(current_vendas_all)} vendas")
        logger.info(f"DEBUG: Filtros - corretor: {corretor}, fonte: {fonte}")
        
        filtered_vendas = []
        for lead in current_vendas_all:
            if not lead:
                continue
            
            # Validar data no período primeiro
            if not (get_lead_closure_date(lead, CUSTOM_FIELD_DATA_FECHAMENTO) and 
                   is_date_in_period(get_lead_closure_date(lead, CUSTOM_FIELD_DATA_FECHAMENTO), start_time, end_time)):
                continue
            
            # Extrair corretor e fonte
            corretor_lead = get_custom_field_value(lead, 837920)
            fonte_lead = get_custom_field_value(lead, 837886)
            
            # Determinar corretor final
            corretor_final = corretor_lead or "Vazio"
            
            # Aplicar filtros de corretor
            if corretor and isinstance(corretor, str) and corretor.strip():
                if ',' in corretor:
                    corretores_list = [c.strip() for c in corretor.split(',')]
                    if corretor_final not in corretores_list:
                        continue
                else:
                    if corretor_final != corretor:
                        continue
            
            # Aplicar filtros de fonte
            if fonte and isinstance(fonte, str) and fonte.strip():
                if ',' in fonte:
                    fontes_list = [f.strip() for f in fonte.split(',')]
                    if fonte_lead not in fontes_list:
                        continue
                else:
                    if fonte_lead != fonte:
                        continue
            
            filtered_vendas.append(lead)
        
        won_leads_with_date = filtered_vendas
        won_leads = len(won_leads_with_date)
        
        lost_leads = len([lead for lead in current_leads if lead.get("status_id") == 143])
        
        # Propostas: usar current_propostas_all que já tem ambos os pipelines (VENDAS + REMARKETING)
        # Aplicar filtros de corretor e fonte nas propostas
        filtered_propostas = []
        for lead in current_propostas_all:
            if not lead:
                continue
            
            # Extrair corretor e fonte
            corretor_lead = get_custom_field_value(lead, 837920)
            fonte_lead = get_custom_field_value(lead, 837886)
            
            # Determinar corretor final - "Vazio" conforme PO (igual detailed-tables)
            corretor_final = corretor_lead or "Vazio"
            
            # Aplicar filtros
            if corretor and isinstance(corretor, str) and corretor.strip():
                if ',' in corretor:
                    corretores_list = [c.strip() for c in corretor.split(',')]
                    if corretor_final not in corretores_list:
                        continue
                else:
                    if corretor_final != corretor:
                        continue
            
            if fonte and isinstance(fonte, str) and fonte.strip():
                if ',' in fonte:
                    fontes_list = [f.strip() for f in fonte.split(',')]
                    if fonte_lead not in fontes_list:
                        continue
                else:
                    if fonte_lead != fonte:
                        continue
            
            filtered_propostas.append(lead)
        
        proposal_leads = len(filtered_propostas)
        
        # Calcular revenue e average deal size (apenas vendas com data válida)
        total_revenue = sum((lead.get("price") or 0) for lead in won_leads_with_date)
        average_deal_size = (total_revenue / won_leads) if won_leads > 0 else 0
        
        # Calcular win rate
        total_closed = won_leads + lost_leads
        win_rate = (won_leads / total_closed * 100) if total_closed > 0 else 0
        
        # Usar função centralizada para validação de vendas anteriores
        
        # Calcular métricas do período anterior
        previous_total_leads = len(previous_leads)
        previous_active_leads = len([lead for lead in previous_leads if lead.get("status_id") not in [142, 143]])
        
        # Vendas anteriores: aplicar filtros de corretor e fonte nas vendas anteriores
        filtered_previous_vendas = []
        for lead in previous_vendas_all:
            if not lead:
                continue
            
            # Validar data no período anterior primeiro
            if not (get_lead_closure_date(lead, CUSTOM_FIELD_DATA_FECHAMENTO) and 
                   is_date_in_period(get_lead_closure_date(lead, CUSTOM_FIELD_DATA_FECHAMENTO), previous_start_time, previous_end_time)):
                continue
            
            # Extrair corretor e fonte
            corretor_lead = get_custom_field_value(lead, 837920)
            fonte_lead = get_custom_field_value(lead, 837886)
            
            # Determinar corretor final
            corretor_final = corretor_lead or "Vazio"
            
            # Aplicar filtros de corretor
            if corretor and isinstance(corretor, str) and corretor.strip():
                if ',' in corretor:
                    corretores_list = [c.strip() for c in corretor.split(',')]
                    if corretor_final not in corretores_list:
                        continue
                else:
                    if corretor_final != corretor:
                        continue
            
            # Aplicar filtros de fonte
            if fonte and isinstance(fonte, str) and fonte.strip():
                if ',' in fonte:
                    fontes_list = [f.strip() for f in fonte.split(',')]
                    if fonte_lead not in fontes_list:
                        continue
                else:
                    if fonte_lead != fonte:
                        continue
            
            filtered_previous_vendas.append(lead)
        
        previous_won_leads_with_date = filtered_previous_vendas
        previous_won_leads = len(previous_won_leads_with_date)
        
        previous_lost_leads = len([lead for lead in previous_leads if lead.get("status_id") == 143])
        
        # Propostas anteriores: usar previous_propostas_all que já tem ambos os pipelines
        filtered_previous_propostas = []
        for lead in previous_propostas_all:
            if not lead:
                continue
            
            # Extrair corretor e fonte
            corretor_lead = get_custom_field_value(lead, 837920)
            fonte_lead = get_custom_field_value(lead, 837886)
            
            # Determinar corretor final - "Vazio" conforme PO (igual detailed-tables)
            corretor_final = corretor_lead or "Vazio"
            
            # Aplicar filtros
            if corretor and isinstance(corretor, str) and corretor.strip():
                if ',' in corretor:
                    corretores_list = [c.strip() for c in corretor.split(',')]
                    if corretor_final not in corretores_list:
                        continue
                else:
                    if corretor_final != corretor:
                        continue
            
            if fonte and isinstance(fonte, str) and fonte.strip():
                if ',' in fonte:
                    fontes_list = [f.strip() for f in fonte.split(',')]
                    if fonte_lead not in fontes_list:
                        continue
                else:
                    if fonte_lead != fonte:
                        continue
            
            filtered_previous_propostas.append(lead)
        
        previous_proposal_leads = len(filtered_previous_propostas)
        
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
        
        # 🧠 OTIMIZAÇÃO INTELIGENTE: Ajustar filtros baseado no período solicitado
        period_days = (end_time - start_time) / (24 * 60 * 60)
        print(f"📅 Período solicitado: {period_days:.1f} dias")
        
        # Definir estratégia baseada no período
        if period_days <= 14:  # 2 semanas
            lookback_days = 30  # Buscar vendas até 30 dias atrás
            max_workers = 4     # Menos threads
            estimated_leads = "poucos"
            print("🚀 Estratégia RÁPIDA: Período curto detectado")
        elif period_days <= 30:  # 1 mês  
            lookback_days = 90   # Buscar vendas até 3 meses atrás
            max_workers = 6      # Threads médias
            estimated_leads = "moderados"
            print("⚡ Estratégia MÉDIA: Período moderado detectado")
        elif period_days <= 90:  # 3 meses
            lookback_days = 180  # Buscar vendas até 6 meses atrás
            max_workers = 8      # Mais threads
            estimated_leads = "muitos"
            print("🔥 Estratégia INTENSA: Período longo detectado")
        else:  # Mais de 3 meses
            lookback_days = 365  # Buscar vendas até 1 ano atrás (original)
            max_workers = 8      # Máximo de threads
            estimated_leads = "massivos"
            print("🌊 Estratégia COMPLETA: Período extenso detectado")
        
        # Calcular timestamps baseados na estratégia
        sales_start_time = start_time - (lookback_days * 24 * 60 * 60)
        
        print(f"📊 Leads estimados: {estimated_leads}")
        print(f"🔍 Buscando vendas desde: {lookback_days} dias atrás")
        print(f"⚙️ Usando {max_workers} threads paralelas")
        
        # Buscar leads de AMBOS os pipelines (Vendas + Remarketing) - PADRÃO CORRETO
        leads_vendas_params = {
            "filter[pipeline_id]": PIPELINE_VENDAS,  # Funil de Vendas
            "filter[created_at][from]": start_time,   # CORREÇÃO: usar created_at para leads (igual detailed-tables)
            "filter[created_at][to]": end_time,
            "limit": 500,  # AUMENTAR limite para evitar perder dados
            "with": "contacts,tags,custom_fields_values"  # CORREÇÃO: usar mesmo parâmetro que detailed-tables
        }
        
        leads_remarketing_params = {
            "filter[pipeline_id]": PIPELINE_REMARKETING,  # Remarketing
            "filter[created_at][from]": start_time,   # CORREÇÃO: usar created_at para leads (igual detailed-tables)
            "filter[created_at][to]": end_time,
            "limit": 500,  # AUMENTAR limite para evitar perder dados
            "with": "contacts,tags,custom_fields_values"  # CORREÇÃO: usar mesmo parâmetro que detailed-tables
        }
        
        # ADICIONAR: Buscar propostas e vendas para completar o mapa de leads (igual detailed-tables)
        propostas_vendas_params = {
            "filter[statuses][0][pipeline_id]": PIPELINE_VENDAS,
            "filter[statuses][0][status_id]": STATUS_PROPOSTA,
            "filter[statuses][1][pipeline_id]": PIPELINE_VENDAS,
            "filter[statuses][1][status_id]": STATUS_CONTRATO_ASSINADO,
            "filter[updated_at][from]": start_time,   # PO: usar updated_at para propostas
            "filter[updated_at][to]": end_time,
            "limit": 500,  # AUMENTAR limite para evitar perder dados
            "with": "contacts,tags,custom_fields_values"  # CORREÇÃO: usar mesmo parâmetro que detailed-tables
        }
        
        vendas_vendas_params = {
            "filter[statuses][0][pipeline_id]": PIPELINE_VENDAS,
            "filter[statuses][0][status_id]": STATUS_VENDA_FINAL,
            "filter[statuses][1][pipeline_id]": PIPELINE_VENDAS,
            "filter[statuses][1][status_id]": STATUS_CONTRATO_ASSINADO,
            "filter[updated_at][from]": sales_start_time,  # 🧠 INTELIGENTE: adapta baseado no período
            "filter[updated_at][to]": end_time,
            "limit": 500,  # AUMENTAR limite para evitar perder dados
            "with": "contacts,tags,custom_fields_values"  # CORREÇÃO: usar mesmo parâmetro que detailed-tables
        }
        
        # CORREÇÃO: Adicionar propostas e vendas do REMARKETING (igual detailed-tables)
        propostas_remarketing_params = {
            "filter[statuses][0][pipeline_id]": PIPELINE_REMARKETING,
            "filter[statuses][0][status_id]": STATUS_PROPOSTA,
            "filter[statuses][1][pipeline_id]": PIPELINE_REMARKETING,
            "filter[statuses][1][status_id]": STATUS_CONTRATO_ASSINADO,
            "filter[updated_at][from]": start_time,   # PO: usar updated_at para propostas
            "filter[updated_at][to]": end_time,
            "limit": 500,  # AUMENTAR limite para evitar perder dados
            "with": "contacts,tags,custom_fields_values"
        }
        
        vendas_remarketing_params = {
            "filter[statuses][0][pipeline_id]": PIPELINE_REMARKETING,
            "filter[statuses][0][status_id]": STATUS_VENDA_FINAL,
            "filter[statuses][1][pipeline_id]": PIPELINE_REMARKETING,
            "filter[statuses][1][status_id]": STATUS_CONTRATO_ASSINADO,
            "filter[updated_at][from]": sales_start_time,  # 🧠 INTELIGENTE: adapta baseado no período
            "filter[updated_at][to]": end_time,
            "limit": 500,  # AUMENTAR limite para evitar perder dados
            "with": "contacts,tags,custom_fields_values"
        }
        

        # Calcular meetings_start_time (um dia antes para capturar reuniões)
        meetings_start_time = start_time - (24 * 60 * 60)  # 1 dia antes
        
        # Buscar tarefas de reunião concluídas
        tasks_params = {
            'filter[task_type_id]': 2,  # Tipo reunião
            'filter[is_completed]': 1,  # Apenas concluídas
            'filter[complete_till][from]': meetings_start_time,  # usar meetings_start_time para incluir 23:59 do dia anterior
            'filter[complete_till][to]': end_time,
            'limit': 250
        }
        
        # OTIMIZAÇÃO: Fazer todas as consultas em PARALELO para reduzir tempo drasticamente
        from concurrent.futures import ThreadPoolExecutor
        import time as time_module
        
        start_queries = time_module.time()
        print(f"🚀 Iniciando {6} consultas PARALELAS...")
        
        def safe_api_call(api_method, params, description):
            """Wrapper seguro para chamadas da API"""
            try:
                result = api_method(params)
                print(f"✅ {description}: Sucesso")
                return result
            except Exception as e:
                logger.error(f"Erro em {description}: {e}")
                print(f"❌ {description}: Erro")
                return {"_embedded": {"leads": []}}
        
        # Executar todas as consultas em paralelo com threads adaptativas
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submeter todas as consultas
            future_leads_vendas = executor.submit(safe_api_call, kommo_api.get_leads, leads_vendas_params, "Leads Vendas")
            future_leads_remarketing = executor.submit(safe_api_call, kommo_api.get_leads, leads_remarketing_params, "Leads Remarketing")
            future_propostas_vendas = executor.submit(safe_api_call, kommo_api.get_leads, propostas_vendas_params, "Propostas Vendas")
            future_propostas_remarketing = executor.submit(safe_api_call, kommo_api.get_leads, propostas_remarketing_params, "Propostas Remarketing")
            future_vendas_vendas = executor.submit(safe_api_call, kommo_api.get_leads, vendas_vendas_params, "Vendas Vendas")
            future_vendas_remarketing = executor.submit(safe_api_call, kommo_api.get_leads, vendas_remarketing_params, "Vendas Remarketing")
            
            # Aguardar resultados
            leads_vendas_data = future_leads_vendas.result(timeout=30)
            leads_remarketing_data = future_leads_remarketing.result(timeout=30)
            propostas_vendas_data = future_propostas_vendas.result(timeout=30)
            propostas_remarketing_data = future_propostas_remarketing.result(timeout=30)
            vendas_vendas_data = future_vendas_vendas.result(timeout=30)
            vendas_remarketing_data = future_vendas_remarketing.result(timeout=30)
        
        end_queries = time_module.time()
        print(f"🎉 6 consultas PARALELAS concluídas em {end_queries - start_queries:.2f}s")
        
        # Combinar leads de TODOS os resultados usando deduplicação por ID
        all_leads_dict = {}
        
        # Adicionar leads de vendas
        if leads_vendas_data and "_embedded" in leads_vendas_data:
            vendas_leads = leads_vendas_data["_embedded"].get("leads", [])
            if isinstance(vendas_leads, list):
                for lead in vendas_leads:
                    if lead and lead.get("id"):
                        all_leads_dict[lead.get("id")] = lead
                
        # Adicionar leads de remarketing        
        if leads_remarketing_data and "_embedded" in leads_remarketing_data:
            remarketing_leads = leads_remarketing_data["_embedded"].get("leads", [])
            if isinstance(remarketing_leads, list):
                for lead in remarketing_leads:
                    if lead and lead.get("id"):
                        all_leads_dict[lead.get("id")] = lead
        
        # Adicionar propostas de vendas
        if propostas_vendas_data and "_embedded" in propostas_vendas_data:
            propostas_leads = propostas_vendas_data["_embedded"].get("leads", [])
            if isinstance(propostas_leads, list):
                for lead in propostas_leads:
                    if lead and lead.get("id"):
                        all_leads_dict[lead.get("id")] = lead
        
        # Adicionar propostas de remarketing
        if propostas_remarketing_data and "_embedded" in propostas_remarketing_data:
            propostas_remarketing_leads = propostas_remarketing_data["_embedded"].get("leads", [])
            if isinstance(propostas_remarketing_leads, list):
                for lead in propostas_remarketing_leads:
                    if lead and lead.get("id"):
                        all_leads_dict[lead.get("id")] = lead
        
        # Adicionar vendas de vendas
        if vendas_vendas_data and "_embedded" in vendas_vendas_data:
            vendas_leads = vendas_vendas_data["_embedded"].get("leads", [])
            if isinstance(vendas_leads, list):
                for lead in vendas_leads:
                    if lead and lead.get("id"):
                        all_leads_dict[lead.get("id")] = lead
                
        # Adicionar vendas de remarketing
        if vendas_remarketing_data and "_embedded" in vendas_remarketing_data:
            vendas_remarketing_leads = vendas_remarketing_data["_embedded"].get("leads", [])
            if isinstance(vendas_remarketing_leads, list):
                for lead in vendas_remarketing_leads:
                    if lead and lead.get("id"):
                        all_leads_dict[lead.get("id")] = lead
        
        # CORREÇÃO: Separar leads por categoria igual detailed-tables
        # Apenas leads criados no período + vendas válidas no período para contagem por usuário
        leads_for_user_count = {}
        
        # 1. Adicionar leads criados no período (igual detailed-tables all_leads_for_details)
        if leads_vendas_data and "_embedded" in leads_vendas_data:
            vendas_leads = leads_vendas_data["_embedded"].get("leads", [])
            if isinstance(vendas_leads, list):
                for lead in vendas_leads:
                    if lead and lead.get("id"):
                        leads_for_user_count[lead.get("id")] = lead
                
        if leads_remarketing_data and "_embedded" in leads_remarketing_data:
            remarketing_leads = leads_remarketing_data["_embedded"].get("leads", [])
            if isinstance(remarketing_leads, list):
                for lead in remarketing_leads:
                    if lead and lead.get("id"):
                        leads_for_user_count[lead.get("id")] = lead
        
        # 2. ADICIONAR: Vendas válidas no período (mesmo que criadas fora do período)
        # Isso garante que vendas como Mario Henrique sejam contadas para o corretor correto
        valid_sale_status_local = [STATUS_VENDA_FINAL, STATUS_CONTRATO_ASSINADO]
        
        if vendas_vendas_data and "_embedded" in vendas_vendas_data:
            vendas_leads = vendas_vendas_data["_embedded"].get("leads", [])
            if isinstance(vendas_leads, list):
                for lead in vendas_leads:
                    if lead and lead.get("id"):
                        # Validar se a venda é do período atual
                        if validate_sale_in_period(lead, start_time, end_time, CUSTOM_FIELD_DATA_FECHAMENTO, valid_sale_status_local):
                            leads_for_user_count[lead.get("id")] = lead
                            logger.info(f"[charts/leads-by-user] Venda válida adicionada: {lead.get('name')} - {lead.get('id')}")
                
        if vendas_remarketing_data and "_embedded" in vendas_remarketing_data:
            vendas_leads = vendas_remarketing_data["_embedded"].get("leads", [])
            if isinstance(vendas_leads, list):
                for lead in vendas_leads:
                    if lead and lead.get("id"):
                        # Validar se a venda é do período atual
                        if validate_sale_in_period(lead, start_time, end_time, CUSTOM_FIELD_DATA_FECHAMENTO, valid_sale_status_local):
                            leads_for_user_count[lead.get("id")] = lead
                            logger.info(f"[charts/leads-by-user] Venda válida adicionada: {lead.get('name')} - {lead.get('id')}")
        
        # Converter para lista
        all_leads_for_user_count = list(leads_for_user_count.values())
        
        # Manter all_leads completo para lookup de reuniões (inclui propostas e vendas antigas)
        all_leads = list(all_leads_dict.values())
        
        # Log para debug - comparar com detailed-tables
        logger.info(f"[charts/leads-by-user] Leads para contagem: {len(all_leads_for_user_count)}")
        logger.info(f"[charts/leads-by-user] Total leads (com propostas/vendas): {len(all_leads)}")
        
        leads_data = {"_embedded": {"leads": all_leads_for_user_count}}
            
        try:
            # Usar get_all_tasks com paginação para períodos grandes
            all_tasks = kommo_api.get_all_tasks(tasks_params)
            tasks_data = {"_embedded": {"tasks": all_tasks}}
            logger.info(f"[charts/leads-by-user] Total de tarefas encontradas: {len(all_tasks)}")
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
        
        # REMOVER lógica de leads antigos - usar apenas a mesma lógica do detailed-tables
        
        # ADICIONAR: Buscar propostas e vendas para completar o mapa de leads (igual detailed-tables)
        # Combinar PROPOSTAS de ambos os pipelines
        all_propostas = []
        if propostas_vendas_data and "_embedded" in propostas_vendas_data:
            propostas = propostas_vendas_data["_embedded"].get("leads", [])
            all_propostas.extend(propostas)
            logger.info(f"Propostas do Funil de Vendas: {len(propostas)}")
        
        if propostas_remarketing_data and "_embedded" in propostas_remarketing_data:
            propostas = propostas_remarketing_data["_embedded"].get("leads", [])
            all_propostas.extend(propostas)
            logger.info(f"Propostas do Remarketing: {len(propostas)}")
            
        # Combinar VENDAS de ambos os pipelines
        all_vendas = []
        if vendas_vendas_data and "_embedded" in vendas_vendas_data:
            vendas = vendas_vendas_data["_embedded"].get("leads", [])
            all_vendas.extend(vendas)
            logger.info(f"Vendas do Funil de Vendas: {len(vendas)}")
        
        if vendas_remarketing_data and "_embedded" in vendas_remarketing_data:
            vendas = vendas_remarketing_data["_embedded"].get("leads", [])
            all_vendas.extend(vendas)
            logger.info(f"Vendas do Remarketing: {len(vendas)}")
        
        logger.info(f"Encontradas {len(all_propostas)} propostas e {len(all_vendas)} vendas totais")
        
        # Criar mapa de leads COMPLETO - incluir TODOS os leads para reuniões (igual detailed-tables)
        # CORREÇÃO: usar a mesma lógica do detailed-tables: all_propostas + all_vendas + all_leads_for_details
        # all_leads já contém vendas+remarketing, então é nosso all_leads_for_details
        all_leads_for_details = all_leads
        all_leads_combined = all_propostas + all_vendas + all_leads_for_details
        leads_map = {}
        for lead in all_leads_combined:
            if lead and lead.get("id"):
                leads_map[lead.get("id")] = lead
                
        logger.info(f"[charts/leads-by-user] Mapa de leads criado: {len(leads_map)} leads únicos (propostas={len(all_propostas)}, vendas={len(all_vendas)}, leads_for_details={len(all_leads_for_details)})")
        
        # Função segura para extrair valor de custom fields
        def get_custom_field_value(lead, field_id):
            """Extrai valor de custom field de forma segura"""
            try:
                custom_fields = lead.get("custom_fields_values")
                if not custom_fields or not isinstance(custom_fields, list):
                    return None
                for field in custom_fields:
                    if field and field.get("field_id") == field_id:
                        values = field.get("values")
                        if values and isinstance(values, list) and len(values) > 0:
                            return values[0].get("value")
                return None
            except Exception as e:
                logger.error(f"Erro ao extrair custom field {field_id}: {e}")
                return None
        
        # OTIMIZAÇÃO INTELIGENTE: Buscar apenas leads únicos das reuniões que não estão no mapa
        reunion_lead_ids = set()
        if tasks_data and "_embedded" in tasks_data:
            tasks_list = tasks_data["_embedded"].get("tasks", [])
            if isinstance(tasks_list, list):
                for task in tasks_list:
                    if task.get('entity_type') == 'leads':
                        lead_id = task.get('entity_id')
                        
                        # Validação de data primeiro
                        complete_till = task.get('complete_till')
                        if not complete_till:
                            continue
                        if complete_till < start_time or complete_till > end_time:
                            continue
                        
                        # Se o lead não está no mapa, adicionar para busca
                        if lead_id and lead_id not in leads_map:
                            reunion_lead_ids.add(lead_id)
        
        print(f"DEBUG: {len(tasks_list) if 'tasks_list' in locals() else 0} reuniões encontradas")
        print(f"DEBUG: {len(reunion_lead_ids)} leads únicos precisam ser buscados")
        
        # Buscar os leads faltantes em lote usando filtro de IDs
        if reunion_lead_ids:
            logger.info(f"Buscando {len(reunion_lead_ids)} leads adicionais para reuniões: {list(reunion_lead_ids)}")
            
            # DEBUG: Tentar busca em lote primeiro
            leads_found_batch = 0
            try:
                # Converter IDs para string separada por vírgula
                ids_string = ','.join(str(id) for id in reunion_lead_ids)
                print(f"DEBUG: Tentando busca em lote com IDs: {ids_string}")
                
                # Buscar múltiplos leads de uma vez
                batch_params = {
                    'filter[id]': ids_string,
                    'limit': len(reunion_lead_ids),
                    'with': 'contacts,custom_fields_values'
                }
                
                batch_result = kommo_api.get_leads(batch_params)
                print(f"DEBUG: Resultado busca em lote: {batch_result is not None}")
                
                if batch_result and '_embedded' in batch_result:
                    batch_leads = batch_result['_embedded'].get('leads', [])
                    print(f"DEBUG: Leads encontrados em lote: {len(batch_leads)}")
                    
                    # Adicionar todos os leads encontrados ao mapa
                    for lead in batch_leads:
                        if lead and lead.get('id'):
                            leads_map[lead.get('id')] = lead
                            leads_found_batch += 1
                            print(f"DEBUG: Lead {lead.get('id')} adicionado via lote")
                
            except Exception as e:
                print(f"DEBUG: Erro na busca em lote: {e}")
            
            # Busca paralela para IDs não encontrados
            remaining_ids = reunion_lead_ids - set(leads_map.keys())
            if remaining_ids:
                print(f"DEBUG: Fazendo busca PARALELA para {len(remaining_ids)} leads restantes")
                
                from concurrent.futures import ThreadPoolExecutor, as_completed
                
                def fetch_lead(lead_id):
                    try:
                        return lead_id, kommo_api.get_lead(lead_id)
                    except Exception as e:
                        print(f"DEBUG: Erro ao buscar lead {lead_id}: {e}")
                        return lead_id, None
                
                max_threads = min(10, len(remaining_ids))
                with ThreadPoolExecutor(max_workers=max_threads) as executor:
                    future_to_id = {executor.submit(fetch_lead, lead_id): lead_id for lead_id in remaining_ids}
                    
                    for future in as_completed(future_to_id):
                        lead_id, lead = future.result()
                        if lead:
                            leads_map[lead_id] = lead
                            print(f"DEBUG: Lead {lead_id} encontrado via thread")
            
            logger.info(f"Total leads encontrados: {leads_found_batch} em lote + {len(reunion_lead_ids) - len(remaining_ids) - leads_found_batch} individual")
        
        # Processar tarefas de reunião (agora com todos os leads disponíveis)
        print(f"DEBUG: Processando reuniões com leads_map atualizado...")
        
        # Criar mapa de reuniões realizadas por lead
        meetings_by_lead = {}
        missing_leads = []  # Para debug
        
        if tasks_data and "_embedded" in tasks_data:
            tasks_list = tasks_data["_embedded"].get("tasks", [])
            if isinstance(tasks_list, list):
                for task in tasks_list:
                    if (task and isinstance(task, dict) and 
                        task.get('entity_type') == 'leads'):
                        lead_id = task.get('entity_id')
                        
                        # Validação de data (igual detailed-tables)
                        # PO: usar complete_till para filtrar reuniões
                        complete_till = task.get('complete_till')
                        if not complete_till:
                            continue
                        if complete_till < start_time or complete_till > end_time:
                            continue
                        
                        # Verificar se o lead existe no mapa
                        if lead_id and lead_id in leads_map:
                            lead = leads_map[lead_id]
                            
                            # CORREÇÃO: Aplicar filtros POR REUNIÃO (igual detailed-tables)
                            corretor_lead = get_custom_field_value(lead, 837920)
                            fonte_lead = get_custom_field_value(lead, 837886) or "N/A"
                            
                            # Determinar corretor final - igual detailed-tables
                            if corretor_lead:
                                corretor_final = corretor_lead
                            else:
                                corretor_final = "Vazio"
                            
                            # Aplicar filtros de corretor se especificado
                            if corretor and isinstance(corretor, str) and corretor.strip():
                                if ',' in corretor:
                                    corretores_list = [c.strip() for c in corretor.split(',')]
                                    if corretor_final not in corretores_list:
                                        continue
                                else:
                                    if corretor_final != corretor:
                                        continue
                            
                            # Aplicar filtros de fonte se especificado
                            if fonte and isinstance(fonte, str) and fonte.strip():
                                if ',' in fonte:
                                    fontes_list = [f.strip() for f in fonte.split(',')]
                                    if fonte_lead not in fontes_list:
                                        continue
                                else:
                                    if fonte_lead != fonte:
                                        continue
                            
                            # Só contar se passou em todos os filtros
                            meetings_by_lead[lead_id] = meetings_by_lead.get(lead_id, 0) + 1
                            
                            # DEBUG: Mostrar qual reunião está sendo contada
                            lead_name = lead.get("name", "Nome não encontrado")
                            complete_till_formatted = datetime.fromtimestamp(complete_till).strftime("%d/%m/%Y %H:%M")
                            print(f"DEBUG REUNIÃO CONTADA: {complete_till_formatted} - {lead_name} - {corretor_final} - {fonte_lead}")
                        elif lead_id:
                            missing_leads.append(lead_id)
                            complete_till_formatted = datetime.fromtimestamp(complete_till).strftime("%d/%m/%Y %H:%M")
                            print(f"DEBUG REUNIÃO IGNORADA - Lead não encontrado: {lead_id} - {complete_till_formatted}")
                            
        # Log para debug
        total_meetings = sum(meetings_by_lead.values())
        logger.info(f"[charts/leads-by-user] Reuniões encontradas: {total_meetings} de {len(tasks_list) if 'tasks_list' in locals() else 0} tasks")
        if missing_leads:
            logger.warning(f"[charts/leads-by-user] {len(missing_leads)} reuniões ignoradas - leads não encontrados: {missing_leads[:10]}")
        
        # Função já definida acima

        # CORREÇÃO: Incluir leads que têm reuniões no período (mesmo que criados fora)
        leads_to_process = []
        
        # 1. Adicionar leads criados no período
        if leads_data and "_embedded" in leads_data:
            leads_list = leads_data["_embedded"].get("leads", [])
            if isinstance(leads_list, list):
                leads_to_process.extend(leads_list)
        
        # 2. Adicionar leads que têm reuniões no período (mesmo que criados fora)
        for lead_id in meetings_by_lead.keys():
            lead = leads_map.get(lead_id)
            if lead and lead not in leads_to_process:
                leads_to_process.append(lead)
                print(f"DEBUG: Lead {lead_id} ({lead.get('name', 'Nome não encontrado')}) adicionado por ter reuniões")
        
        # Processar leads
        leads_by_user = {}
        
        for lead in leads_to_process:
            if not lead or not isinstance(lead, dict):
                continue
            
            # CORREÇÃO: NÃO filtrar por created_at aqui pois já foi filtrado na seleção dos leads
            # O all_leads_for_user_count já contém leads criados no período + vendas válidas no período
            # Filtrar por created_at aqui removeria vendas válidas criadas fora do período
            
            # Extrair valores de forma segura
            corretor_lead = get_custom_field_value(lead, 837920)  # Corretor
            fonte_lead = get_custom_field_value(lead, 837886)     # Fonte
            
            # Aplicar filtros - suporta múltiplos valores separados por vírgula
            if corretor and isinstance(corretor, str) and corretor.strip():
                # Se corretor contém vírgula, é multi-select
                if ',' in corretor:
                    corretores_list = [c.strip() for c in corretor.split(',')]
                    if corretor_lead not in corretores_list:
                        continue
                else:
                    # Filtro único
                    if corretor_lead != corretor:
                        continue
            
            if fonte and isinstance(fonte, str) and fonte.strip():
                # Se fonte contém vírgula, é multi-select
                if ',' in fonte:
                    fontes_list = [f.strip() for f in fonte.split(',')]
                    if fonte_lead not in fontes_list:
                        continue
                else:
                    # Filtro único
                    if fonte_lead != fonte:
                        continue
                    
            # Determinar corretor final - tratar como "vazio" conforme PO
            final_corretor = corretor_lead or "Vazio"
            
            
            # Inicializar contador se não existir
            if final_corretor not in leads_by_user:
                leads_by_user[final_corretor] = {
                    "name": final_corretor,
                    "value": 0,
                    "active": 0,
                    "meetingsHeld": 0,  # Campo que o frontend usa
                    "meetings": 0,      # Fallback para compatibilidade
                    "sales": 0,
                    "lost": 0,
                    # NOVOS campos para separação orgânica vs paga
                    "organicLeads": 0,
                    "paidLeads": 0,
                    "organicMeetings": 0,
                    "paidMeetings": 0
                }
                logger.info(f"DEBUG: Inicializando contador para corretor: {final_corretor} com campos orgânicos")
            
            # Incrementar contadores
            leads_by_user[final_corretor]["value"] += 1
                    
            # Separar leads entre orgânicos e pagos
            if fonte_lead == "Orgânico":
                leads_by_user[final_corretor]["organicLeads"] += 1
                logger.info(f"DEBUG: Lead orgânico encontrado - Corretor: {final_corretor}, Fonte: {fonte_lead}")
            else:
                leads_by_user[final_corretor]["paidLeads"] += 1
                logger.info(f"DEBUG: Lead pago encontrado - Corretor: {final_corretor}, Fonte: {fonte_lead}")
            
            # Usar função centralizada para validação de vendas
            valid_sale_status_local = [STATUS_VENDA_FINAL, STATUS_CONTRATO_ASSINADO]
            
            status_id = lead.get("status_id")
            lead_id = lead.get("id")
            
            # Vendas: apenas com data válida E no período
            if validate_sale_in_period(lead, start_time, end_time, CUSTOM_FIELD_DATA_FECHAMENTO, valid_sale_status_local):
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
                
                # Separar reuniões entre orgânicas e pagas
                if fonte_lead == "Orgânico":
                    leads_by_user[final_corretor]["organicMeetings"] += meetings_count
                else:
                    leads_by_user[final_corretor]["paidMeetings"] += meetings_count
        
        # REMOVIDO: Lógica extra que contava meetings de leads fora do período
        # Para alinhar com detailed-tables, só contamos meetings de leads do período atual
        
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
        
        # Calcular proposalStats baseado nos dados já processados
        # Contar propostas totais (status proposta + contrato assinado) após filtros
        proposal_count = 0
        for lead in all_propostas:
            if not lead:
                continue
            
            # Extrair corretor e fonte do lead
            corretor_lead = get_custom_field_value(lead, 837920)
            fonte_lead = get_custom_field_value(lead, 837886)
            
            # Aplicar filtros
            if corretor and isinstance(corretor, str) and corretor.strip():
                if ',' in corretor:
                    corretores_list = [c.strip() for c in corretor.split(',')]
                    if corretor_lead not in corretores_list:
                        continue
                else:
                    if corretor_lead != corretor:
                        continue
            
            if fonte and isinstance(fonte, str) and fonte.strip():
                if ',' in fonte:
                    fontes_list = [f.strip() for f in fonte.split(',')]
                    if fonte_lead not in fontes_list:
                        continue
                else:
                    if fonte_lead != fonte:
                        continue
            
            proposal_count += 1
        
        return {
            "leadsByUser": leads_by_user_list,
            "analyticsTeam": {
                "user_performance": user_performance
            },
            "proposalStats": {
                "total": proposal_count,
                "inProposal": sum(1 for lead in all_propostas if lead and lead.get("status_id") == STATUS_PROPOSTA),
                "contractSigned": sum(1 for lead in all_propostas if lead and lead.get("status_id") == STATUS_CONTRATO_ASSINADO)
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
                "total_meetings_found": sum(user["meetingsHeld"] for user in leads_by_user_list),
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
                
                # Para reuniões: incluir 23:59 do dia anterior para capturar reuniões agendadas na virada do dia
                meetings_start_dt = start_dt - timedelta(days=1)
                meetings_start_dt = meetings_start_dt.replace(hour=23, minute=59, second=0)
                meetings_start_time = int(meetings_start_dt.timestamp())
            except ValueError:
                raise HTTPException(status_code=400, detail="Formato de data inválido. Use YYYY-MM-DD")
        else:
            end_time = int(time.time())
            start_time = end_time - (days * 24 * 60 * 60)
            
            # Para reuniões: incluir 23:59 do dia anterior
            meetings_start_time = start_time - (24 * 60 * 60) + (23 * 60 * 60 + 59 * 60)  # -1 dia + 23:59
        
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
            "limit": 500,  # AUMENTAR limite para evitar perder dados
            "with": "custom_fields_values"
        }
        
        leads_remarketing_params = {
            "filter[pipeline_id]": PIPELINE_REMARKETING,  # Remarketing
            "filter[updated_at][from]": start_time,   # PO: usar updated_at para propostas
            "filter[updated_at][to]": end_time,
            "limit": 500,  # AUMENTAR limite para evitar perder dados
            "with": "custom_fields_values"
        }
        
        # Garantir que meetings_start_time está definido (caso não esteja definido acima)
        if 'meetings_start_time' not in locals():
            meetings_start_time = start_time - (24 * 60 * 60)  # 1 dia antes como fallback
        
        # Buscar tarefas de reunião concluídas
        tasks_params = {
            'filter[task_type_id]': 2,  # Tipo reunião
            'filter[is_completed]': 1,  # Apenas concluídas
            'filter[complete_till][from]': meetings_start_time,  # usar meetings_start_time para incluir 23:59 do dia anterior
            'filter[complete_till][to]': end_time,
            'limit': 250  # Usar mesmo limite do detailed-tables
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
            # Usar get_all_tasks com paginação para períodos grandes
            all_tasks = kommo_api.get_all_tasks(tasks_params)
            tasks_data = {"_embedded": {"tasks": all_tasks}}
            logger.info(f"[sales/kpis] Total de tarefas encontradas: {len(all_tasks)}")
        except Exception as e:
            logger.error(f"Erro ao buscar tarefas: {e}")
            tasks_data = {"_embedded": {"tasks": []}}
        
        
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
                    if corretor and isinstance(corretor, str) and corretor.strip():
                        # Se corretor contém vírgula, é multi-select
                        if ',' in corretor:
                            corretores_list = [c.strip() for c in corretor.split(',')]
                            if corretor_lead not in corretores_list:
                                continue
                        else:
                            # Filtro único
                            if corretor_lead != corretor:
                                continue
                    
                    if fonte and isinstance(fonte, str) and fonte.strip():
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
        valid_sale_status_conversion = [STATUS_VENDA_FINAL, STATUS_CONTRATO_ASSINADO]
        sales_leads = len([
            lead for lead in filtered_leads 
            if validate_sale_in_period(lead, start_time, end_time, CUSTOM_FIELD_DATA_FECHAMENTO, valid_sale_status_conversion)
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
            "limit": 500,  # AUMENTAR limite para evitar perder dados
            "with": "custom_fields_values"
        }
        
        leads_remarketing_params = {
            "filter[pipeline_id]": PIPELINE_REMARKETING,  # Remarketing
            "filter[updated_at][from]": start_time,   # PO: usar updated_at para propostas
            "filter[updated_at][to]": end_time,
            "limit": 500,  # AUMENTAR limite para evitar perder dados
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
                    if corretor and isinstance(corretor, str) and corretor.strip():
                        # Se corretor contém vírgula, é multi-select
                        if ',' in corretor:
                            corretores_list = [c.strip() for c in corretor.split(',')]
                            if corretor_lead not in corretores_list:
                                continue
                        else:
                            # Filtro único
                            if corretor_lead != corretor:
                                continue
                    
                    if fonte and isinstance(fonte, str) and fonte.strip():
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
                        valid_sale_status_pipeline = [STATUS_CONTRATO_ASSINADO, STATUS_VENDA_FINAL]
                        if validate_sale_in_period(lead, start_time, end_time, CUSTOM_FIELD_DATA_FECHAMENTO, valid_sale_status_pipeline):
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

@router.get("/dashboard/optimized")
async def get_dashboard_optimized(
    days: int = Query(30, description="Período em dias para análise"),
    use_cache: bool = Query(True, description="Usar cache Redis"),
    force_refresh: bool = Query(False, description="Forçar refresh do cache")
):
    """
    Dashboard otimizado com carregamento paralelo e cache Redis
    Substitui chamadas sequenciais por requisições paralelas
    """
    try:
        import time
        start_time = time.time()
        
        logger.info(f"Dashboard otimizado iniciado - {days} dias, cache: {use_cache}")
        
        # Instanciar API do Kommo
        from app.services.kommo_api import KommoAPI
        api = KommoAPI()
        
        # Configurar endpoints para busca paralela
        base_url = "http://localhost:8000"  # URL interna
        endpoints_config = [
            {
                "endpoint": f"{base_url}/api/v2/sales/kpis?days={days}",
                "cache_key": f"dashboard:kpis:{days}" if use_cache and not force_refresh else None
            },
            {
                "endpoint": f"{base_url}/api/v2/charts/leads-by-user?days={days}",
                "cache_key": f"dashboard:leads_by_user:{days}" if use_cache and not force_refresh else None
            },
            {
                "endpoint": f"{base_url}/api/v2/sales/conversion-rates?days={days}",
                "cache_key": f"dashboard:conversion_rates:{days}" if use_cache and not force_refresh else None
            },
            {
                "endpoint": f"{base_url}/api/v2/sales/pipeline-status?days={days}",
                "cache_key": f"dashboard:pipeline_status:{days}" if use_cache and not force_refresh else None
            }
        ]
        
        # Buscar dados usando requests paralelos simples
        import asyncio
        import aiohttp
        
        async def fetch_endpoint(session, config):
            cache_key = config.get('cache_key')
            
            # Verificar cache primeiro
            if cache_key and api.redis_client:
                try:
                    cached = api.redis_client.get(cache_key)
                    if cached:
                        import pickle
                        data = pickle.loads(cached)
                        return {"endpoint": config["endpoint"], "data": data, "from_cache": True}
                except:
                    pass
            
            # Buscar da API
            try:
                async with session.get(config["endpoint"], timeout=aiohttp.ClientTimeout(total=30)) as response:
                    if response.status == 200:
                        data = await response.json()
                        
                        # Salvar no cache
                        if cache_key and api.redis_client:
                            try:
                                import pickle
                                serialized = pickle.dumps(data)
                                api.redis_client.setex(cache_key, api._cache_ttl, serialized)
                            except:
                                pass
                        
                        return {"endpoint": config["endpoint"], "data": data, "from_cache": False}
                    else:
                        return {"endpoint": config["endpoint"], "error": f"HTTP {response.status}", "from_cache": False}
            except Exception as e:
                return {"endpoint": config["endpoint"], "error": str(e), "from_cache": False}
        
        # Executar requisições em paralelo
        async with aiohttp.ClientSession() as session:
            tasks = [fetch_endpoint(session, config) for config in endpoints_config]
            results = await asyncio.gather(*tasks)
        
        # Organizar resposta
        dashboard_data = {}
        cache_hits = 0
        successful_requests = 0
        
        for result in results:
            endpoint_url = result["endpoint"]
            endpoint_name = endpoint_url.split("/")[-1].split("?")[0].replace("-", "_")
            
            if "data" in result:
                dashboard_data[endpoint_name] = result["data"]
                successful_requests += 1
                if result.get("from_cache"):
                    cache_hits += 1
            else:
                dashboard_data[endpoint_name] = {"error": result.get("error", "Unknown error")}
        
        total_time = time.time() - start_time
        
        return {
            "dashboard_data": dashboard_data,
            "performance_metrics": {
                "total_time": round(total_time, 2),
                "total_endpoints": len(endpoints_config),
                "successful_requests": successful_requests,
                "cache_hits": cache_hits,
                "cache_hit_ratio": round((cache_hits / len(endpoints_config)) * 100, 1),
                "loading_method": "parallel_optimized",
                "redis_enabled": api.redis_client is not None,
                "days": days
            }
        }
        
    except Exception as e:
        logger.error(f"Erro no dashboard otimizado: {str(e)}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")

@router.delete("/cache/clear")
async def clear_dashboard_cache():
    """Limpa cache do dashboard"""
    try:
        from app.services.kommo_api import KommoAPI
        api = KommoAPI()
        api.clear_cache()
        
        return {
            "message": "Cache limpo com sucesso",
            "redis_enabled": api.redis_client is not None
        }
        
    except Exception as e:
        logger.error(f"Erro ao limpar cache: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")

@router.get("/cache/status")
async def get_cache_status():
    """Retorna status do cache Redis"""
    try:
        from app.services.kommo_api import KommoAPI
        api = KommoAPI()
        
        redis_status = "disconnected"
        redis_info = None
        
        if api.redis_client:
            try:
                info = api.redis_client.info()
                redis_status = "connected"
                redis_info = {
                    "connected_clients": info.get("connected_clients"),
                    "used_memory_human": info.get("used_memory_human"),
                    "total_commands_processed": info.get("total_commands_processed")
                }
            except Exception as e:
                redis_status = f"error: {str(e)}"
        
        return {
            "redis_status": redis_status,
            "redis_info": redis_info,
            "cache_ttl": api._cache_ttl,
            "memory_cache_keys": len(api._memory_cache) if hasattr(api, '_memory_cache') else 0
        }
        
    except Exception as e:
        logger.error(f"Erro ao obter status do cache: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")
