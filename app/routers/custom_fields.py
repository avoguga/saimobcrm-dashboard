from fastapi import APIRouter, HTTPException, Query, Path
from typing import Dict, List, Optional
from app.services.kommo_api import KommoAPI

router = APIRouter(prefix="/custom-fields", tags=["Custom Fields"])
api = KommoAPI()

@router.get("/")
async def get_all_custom_fields():
    """Retorna todos os campos personalizados definidos para leads"""
    try:
        data = api.get_custom_fields()
        custom_fields = data.get("_embedded", {}).get("custom_fields", [])
        return {"custom_fields": custom_fields}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/statistics")
async def get_custom_fields_statistics():
    """Retorna estatísticas de uso dos campos personalizados"""
    try:
        # Obter todos os campos personalizados
        fields_data = api.get_custom_fields()
        
        if not fields_data or not isinstance(fields_data, dict):
            return {"statistics": [], "total_fields": 0, "message": "Erro ao obter campos personalizados"}
        
        custom_fields = fields_data.get("_embedded", {}).get("custom_fields", [])
        
        if not custom_fields or not isinstance(custom_fields, list):
            return {"statistics": [], "total_fields": 0, "message": "Nenhum campo personalizado encontrado"}
        
        # Obter leads com campos personalizados
        leads_data = api.get_leads({"limit": 250, "with": "custom_fields_values"})
        
        if not leads_data or not isinstance(leads_data, dict):
            return {"statistics": [], "total_fields": len(custom_fields), "message": "Erro ao obter leads"}
        
        leads = leads_data.get("_embedded", {}).get("leads", [])
        
        if not isinstance(leads, list):
            leads = []
        
        statistics = []
        
        for field in custom_fields:
            field_id = field.get("id")
            field_name = field.get("name", f"Campo {field_id}")
            field_type = field.get("type", "unknown")
            
            # Contar uso deste campo
            usage_count = 0
            unique_values = set()
            
            for lead in leads:
                custom_fields_values = lead.get("custom_fields_values", [])
                
                # Verificar se custom_fields_values não é None
                if not custom_fields_values or not isinstance(custom_fields_values, list):
                    continue
                
                for custom_field_value in custom_fields_values:
                    if custom_field_value.get("field_id") == field_id:
                        usage_count += 1
                        
                        # Contar valores únicos
                        field_values = custom_field_value.get("values", [])
                        for value_obj in field_values:
                            value = str(value_obj.get("value", ""))
                            if value:
                                unique_values.add(value)
                        break
            
            # Calcular porcentagem de uso
            usage_percentage = (usage_count / len(leads) * 100) if leads else 0
            
            stat = {
                "field_id": field_id,
                "field_name": field_name,
                "field_type": field_type,
                "usage_count": usage_count,
                "unique_values_count": len(unique_values),
                "usage_percentage": round(usage_percentage, 2),
                "is_required": field.get("is_required", False),
                "is_computed": field.get("is_computed", False)
            }
            
            # Adicionar informações específicas por tipo
            if field_type == "select" or field_type == "multiselect":
                enums = field.get("enums", [])
                stat["available_options"] = len(enums)
                stat["options"] = [{"id": e.get("id"), "value": e.get("value")} for e in enums]
            
            statistics.append(stat)
        
        # Ordenar por uso
        statistics.sort(key=lambda x: x["usage_count"], reverse=True)
        
        return {
            "statistics": statistics,
            "total_fields": len(custom_fields),
            "total_leads_analyzed": len(leads),
            "summary": {
                "most_used_field": statistics[0]["field_name"] if statistics else None,
                "avg_usage_percentage": round(sum(s["usage_percentage"] for s in statistics) / len(statistics), 2) if statistics else 0,
                "total_unique_values": sum(s["unique_values_count"] for s in statistics)
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/{field_id}")
async def get_custom_field(field_id: int = Path(..., description="ID do campo personalizado")):
    """Retorna detalhes de um campo personalizado específico"""
    try:
        data = api.get_custom_fields()
        custom_fields = data.get("_embedded", {}).get("custom_fields", [])
        
        for field in custom_fields:
            if field.get("id") == field_id:
                return {"custom_field": field}
        
        raise HTTPException(status_code=404, detail=f"Campo personalizado com ID {field_id} não encontrado")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/values/{field_id}")
async def get_custom_field_values(
    field_id: int = Path(..., description="ID do campo personalizado"),
    limit: int = Query(250, description="Número máximo de leads a analisar")
):
    """Retorna os valores de um campo personalizado em todos os leads"""
    try:
        # Primeiro, obter detalhes do campo personalizado
        field_data = await get_custom_field(field_id)
        field = field_data.get("custom_field", {})
        
        # Obter leads com valores de campos personalizados
        leads_data = api.get_leads({"limit": limit})
        leads = leads_data.get("_embedded", {}).get("leads", [])
        
        values = {}
        
        for lead in leads:
            custom_fields = lead.get("custom_fields_values", [])
            
            # Verificar se custom_fields não é None
            if not custom_fields or not isinstance(custom_fields, list):
                continue
                
            for custom_field in custom_fields:
                if custom_field.get("field_id") == field_id:
                    field_values = custom_field.get("values", [])
                    
                    for field_value in field_values:
                        value = str(field_value.get("value", ""))
                        if field.get("type") == "select":
                            # Para campos de seleção, obter o texto da opção
                            enum_id = field_value.get("enum_id")
                            enums = field.get("enums", [])
                            if enums and isinstance(enums, list):
                                for enum in enums:
                                    if enum.get("id") == enum_id:
                                        value = enum.get("value", "")
                                        break
                        
                        values[value] = values.get(value, 0) + 1
                    break
        
        return {
            "field_name": field.get("name", ""),
            "field_type": field.get("type", ""),
            "values": values
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))