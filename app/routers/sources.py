from fastapi import APIRouter, HTTPException
from typing import Dict, List
from app.services.kommo_api import KommoAPI
import traceback

router = APIRouter(prefix="/sources", tags=["Sources"])
api = KommoAPI()

@router.get("/")
async def get_all_sources():
    """Retorna todas as fontes de leads disponíveis"""
    try:
        data = api.get_sources()
        
        # Verificar se a resposta contém dados válidos
        if not data:
            return {"sources": [], "message": "Resposta vazia da API"}
            
        embedded = data.get("_embedded")
        if not embedded:
            return {"sources": [], "message": "Resposta da API não contém campo '_embedded'"}
            
        sources = embedded.get("sources", [])
        return {"sources": sources}
    except Exception as e:
        print(f"Erro ao obter fontes: {e}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/map")
async def get_sources_map():
    """Retorna um mapeamento de IDs de fonte para nomes legíveis"""
    try:
        data = api.get_sources()
        
        # Verificar se a resposta contém dados válidos
        if not data:
            return {"sources_map": {}, "message": "Resposta vazia da API"}
            
        embedded = data.get("_embedded")
        if not embedded:
            return {"sources_map": {}, "message": "Resposta da API não contém campo '_embedded'"}
            
        sources = embedded.get("sources", [])
        
        sources_map = {}
        for source in sources:
            source_id = source.get("id")
            if source_id is not None:
                source_name = source.get("name", f"Fonte {source_id}")
                sources_map[str(source_id)] = source_name
        
        return {"sources_map": sources_map}
    except Exception as e:
        print(f"Erro ao obter mapa de fontes: {e}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/performance")
async def get_sources_performance():
    """Retorna métricas de performance por fonte de lead"""
    try:
        # Obter dados de fontes e leads
        sources_data = api.get_sources()
        leads_data = api.get_leads()
        
        # Verificar se a resposta de leads contém dados válidos
        if not leads_data:
            return {"performance": [], "message": "Resposta vazia da API de leads"}
        
        # Verificar se leads_data é uma string (erro) ou dict válido
        if isinstance(leads_data, str):
            return {"performance": [], "message": f"Erro na API de leads: {leads_data}"}
        
        # Extrair leads
        leads = []
        if isinstance(leads_data, dict) and "_embedded" in leads_data:
            leads = leads_data["_embedded"].get("leads", [])
        elif isinstance(leads_data, list):
            leads = leads_data
        
        # Extrair fontes
        sources = []
        if sources_data and "_embedded" in sources_data:
            sources = sources_data["_embedded"].get("sources", [])
        
        # Criar mapa de fontes
        sources_map = {str(source.get("id")): source.get("name", f"Fonte {source.get('id')}") 
                      for source in sources if source.get("id") is not None}
        
        # Calcular métricas por fonte
        performance_data = {}
        
        for lead in leads:
            # Verificar se lead é um dicionário válido
            if not isinstance(lead, dict):
                continue
                
            # Obter ID da fonte do lead
            custom_fields = lead.get("custom_fields_values", [])
            source_id = None
            
            # Verificar se custom_fields não é None
            if not custom_fields or not isinstance(custom_fields, list):
                continue
            
            # Procurar campo de fonte nos custom fields
            for field in custom_fields:
                # Verificar se field é um dicionário válido
                if not isinstance(field, dict):
                    continue
                    
                field_id = field.get("field_id")
                # IDs comuns para campo de fonte: 629203, 629205 (ajustar conforme necessário)
                if field_id in [629203, 629205]:
                    values = field.get("values", [])
                    if values and len(values) > 0:
                        # Verificar se values[0] é um dicionário válido
                        if isinstance(values[0], dict):
                            source_id = str(values[0].get("value", ""))
                            break
            
            # Se não encontrou fonte, tentar no pipeline_id ou outro campo
            if not source_id:
                source_id = str(lead.get("pipeline_id", "unknown"))
            
            # Inicializar contadores para a fonte
            if source_id not in performance_data:
                source_name = sources_map.get(source_id, f"Fonte {source_id}")
                performance_data[source_id] = {
                    "source_id": source_id,
                    "source_name": source_name,
                    "total_leads": 0,
                    "qualified_leads": 0,
                    "converted_leads": 0,
                    "conversion_rate": 0.0
                }
            
            # Incrementar contadores
            performance_data[source_id]["total_leads"] += 1
            
            # Verificar status do lead
            status_id = lead.get("status_id")
            if status_id in [143, 142]:  # IDs de exemplo para leads qualificados
                performance_data[source_id]["qualified_leads"] += 1
            if status_id == 142:  # ID de exemplo para leads convertidos
                performance_data[source_id]["converted_leads"] += 1
        
        # Calcular taxa de conversão
        for source_data in performance_data.values():
            if source_data["total_leads"] > 0:
                source_data["conversion_rate"] = round(
                    (source_data["converted_leads"] / source_data["total_leads"]) * 100, 2
                )
        
        # Converter para lista e ordenar por total de leads
        performance_list = list(performance_data.values())
        performance_list.sort(key=lambda x: x["total_leads"], reverse=True)
        
        return {"performance": performance_list}
    except Exception as e:
        print(f"Erro ao obter performance de fontes: {e}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))