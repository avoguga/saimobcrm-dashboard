import requests
from typing import Dict, List, Optional, Union, Any
import config

class KommoAPI:
    def __init__(self):
        self.base_url = config.KOMMO_API_URL
        self.headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {config.KOMMO_TOKEN}"
        }
    
    def _make_request(self, endpoint: str, params: Optional[Dict] = None) -> Dict:
        """Método genérico para fazer requisições à API Kommo"""
        url = f"{self.base_url}/{endpoint}"
        response = requests.get(url, headers=self.headers, params=params)
        response.raise_for_status()
        return response.json()
    
    # Métodos para Leads
    def get_leads(self, params: Optional[Dict] = None) -> Dict:
        """Obtém a lista de leads com parâmetros opcionais"""
        return self._make_request("leads", params)
    
    def get_lead(self, lead_id: int) -> Dict:
        """Obtém detalhes de um lead específico"""
        return self._make_request(f"leads/{lead_id}")
    
    # Métodos para Tags
    def get_tags(self) -> List[Dict]:
        """Obtém todas as tags disponíveis"""
        response = self._make_request("leads/tags")
        return response.get("_embedded", {}).get("tags", [])
    
    # Métodos para Pipelines
    def get_pipelines(self) -> List[Dict]:
        """Obtém todos os pipelines"""
        response = self._make_request("leads/pipelines")
        return response.get("_embedded", {}).get("pipelines", [])
    
    def get_pipeline_statuses(self, pipeline_id: int) -> List[Dict]:
        """Obtém todos os estágios de um pipeline"""
        response = self._make_request(f"leads/pipelines/{pipeline_id}/statuses")
        return response.get("_embedded", {}).get("statuses", [])
    
    # Métodos para Usuários
    def get_users(self) -> List[Dict]:
        """Obtém todos os usuários/corretores"""
        response = self._make_request("users")
        return response.get("_embedded", {}).get("users", [])