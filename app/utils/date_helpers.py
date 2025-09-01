"""
Funções auxiliares para processamento de datas no sistema
"""
from datetime import datetime
from typing import Optional, Union, Dict, Any
import logging

logger = logging.getLogger(__name__)


def extract_custom_field_value(lead: Dict[str, Any], field_id: int) -> Optional[Any]:
    """
    Extrai valor de um campo customizado de forma padronizada
    
    Args:
        lead: Dicionário do lead
        field_id: ID do campo customizado
        
    Returns:
        Valor do campo ou None se não encontrado
    """
    try:
        custom_fields = lead.get("custom_fields_values", [])
        if not custom_fields:
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
                    return first_value
        return None
    except Exception as e:
        logger.error(f"Erro ao extrair campo customizado {field_id}: {e}")
        return None


def parse_closure_date(date_value: Any) -> Optional[int]:
    """
    Converte valor de data de fechamento para timestamp Unix
    Aceita múltiplos formatos para garantir consistência
    
    Args:
        date_value: Valor da data em qualquer formato suportado
        
    Returns:
        Timestamp Unix ou None se inválido
    """
    if not date_value:
        return None
        
    try:
        # 1. Se já for int ou float, retornar diretamente
        if isinstance(date_value, (int, float)):
            return int(date_value)
            
        # 2. Se for string
        if isinstance(date_value, str):
            # 2.1 String numérica (timestamp como string)
            if date_value.isdigit():
                return int(date_value)
                
            # 2.2 Formato ISO/datetime comum
            # Tentar vários formatos comuns
            date_formats = [
                '%Y-%m-%d',                    # 2025-06-28
                '%Y-%m-%d %H:%M:%S',          # 2025-06-28 10:30:00
                '%d/%m/%Y',                    # 28/06/2025
                '%d/%m/%Y %H:%M',             # 28/06/2025 10:30
                '%d/%m/%Y %H:%M:%S',          # 28/06/2025 10:30:00
            ]
            
            for fmt in date_formats:
                try:
                    dt = datetime.strptime(date_value, fmt)
                    return int(dt.timestamp())
                except ValueError:
                    continue
                    
            # Se nenhum formato funcionou, logar aviso
            logger.warning(f"Data em formato não reconhecido: {date_value}")
            return None
            
        # 3. Outros tipos não suportados
        logger.warning(f"Tipo de data não suportado: {type(date_value)}")
        return None
        
    except Exception as e:
        logger.error(f"Erro ao processar data {date_value}: {e}")
        return None


def is_date_in_period(timestamp: int, start_timestamp: int, end_timestamp: int) -> bool:
    """
    Verifica se uma data está dentro de um período
    
    Args:
        timestamp: Timestamp Unix da data a verificar
        start_timestamp: Timestamp Unix do início do período
        end_timestamp: Timestamp Unix do fim do período
        
    Returns:
        True se está no período, False caso contrário
    """
    return start_timestamp <= timestamp <= end_timestamp


def get_lead_closure_date(lead: Dict[str, Any], field_id: int = 858126) -> Optional[int]:
    """
    Obtém a data de fechamento de um lead de forma padronizada
    
    Args:
        lead: Dicionário do lead
        field_id: ID do campo de data de fechamento (padrão: 858126)
        
    Returns:
        Timestamp Unix da data de fechamento ou None
    """
    date_value = extract_custom_field_value(lead, field_id)
    return parse_closure_date(date_value)


def get_lead_proposal_date(lead: Dict[str, Any], field_id: int = 882618) -> Optional[int]:
    """
    Obtém a data da proposta de um lead de forma padronizada
    
    Args:
        lead: Dicionário do lead
        field_id: ID do campo de data da proposta (padrão: 882618)
        
    Returns:
        Timestamp Unix da data da proposta ou None
    """
    date_value = extract_custom_field_value(lead, field_id)
    return parse_closure_date(date_value)


def format_proposal_date(lead: Dict[str, Any], field_id: int = 882618) -> str:
    """
    Formata a data da proposta de um lead no formato brasileiro
    
    Args:
        lead: Dicionário do lead
        field_id: ID do campo de data da proposta (padrão: 882618)
        
    Returns:
        Data formatada (DD/MM/YYYY HH:MM) ou "N/A" se não encontrada
    """
    timestamp = get_lead_proposal_date(lead, field_id)
    if timestamp:
        return datetime.fromtimestamp(timestamp).strftime("%d/%m/%Y %H:%M")
    return "N/A"


def validate_sale_in_period(
    lead: Dict[str, Any], 
    start_timestamp: int, 
    end_timestamp: int,
    closure_date_field_id: int = 858126,
    valid_status_ids: list = None
) -> bool:
    """
    Valida se uma venda deve ser contabilizada em um período específico
    
    Args:
        lead: Dicionário do lead
        start_timestamp: Timestamp Unix do início do período
        end_timestamp: Timestamp Unix do fim do período  
        closure_date_field_id: ID do campo de data de fechamento
        valid_status_ids: Lista de status_ids considerados como venda
        
    Returns:
        True se a venda é válida para o período, False caso contrário
    """
    # Status padrão de venda
    if valid_status_ids is None:
        valid_status_ids = [142, 80689759]  # Closed-won, Contrato Assinado
        
    # Verificar status
    if lead.get("status_id") not in valid_status_ids:
        return False
        
    # Obter e validar data de fechamento
    closure_timestamp = get_lead_closure_date(lead, closure_date_field_id)
    if not closure_timestamp:
        return False
        
    # Verificar se está no período
    return is_date_in_period(closure_timestamp, start_timestamp, end_timestamp)