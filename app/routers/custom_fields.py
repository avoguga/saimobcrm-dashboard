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
            
            for custom_field in custom_fields:
                if custom_field.get("field_id") == field_id:
                    field_values = custom_field.get("values", [])
                    
                    for field_value in field_values:
                        value = str(field_value.get("value", ""))
                        if field.get("type") == "select":
                            # Para campos de seleção, obter o texto da opção
                            enum_id = field_value.get("enum_id")
                            for enum in field.get("enums", []):
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