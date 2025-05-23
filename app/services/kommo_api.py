import requests
from typing import Dict, List, Optional, Union, Any
import config
from datetime import datetime
import json

class KommoAPI:
    def __init__(self):
        self.base_url = config.KOMMO_API_URL
        self.headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {config.KOMMO_TOKEN}"
        }
    
    def _make_request(self, endpoint: str, params: Optional[Dict] = None) -> Dict:
        """Método genérico para fazer requisições à API Kommo com tratamento de erro melhorado"""
        url = f"{self.base_url}/{endpoint}"
        try:
            response = requests.get(url, headers=self.headers, params=params)
            
            # Imprimir informações para debug
            print(f"Request URL: {response.url}")
            print(f"Status Code: {response.status_code}")
            print(f"Response Headers: {dict(response.headers)}")
            
            # Verificar se a resposta foi bem-sucedida
            response.raise_for_status()
            
            # Verificar se a resposta contém conteúdo
            if not response.text:
                print("Resposta vazia recebida da API")
                return {}
            
            # Tentar fazer o parse do JSON
            try:
                return response.json()
            except ValueError as e:
                print(f"Erro ao analisar JSON: {e}")
                print(f"Conteúdo da resposta: {response.text[:200]}...")  # Mostrar os primeiros 200 caracteres
                raise ValueError(f"Resposta inválida da API Kommo: {e}")
        
        except requests.exceptions.RequestException as e:
            print(f"Erro de requisição HTTP: {e}")
            # Para fins de depuração, retornar um objeto vazio em vez de propagar o erro
            return {}
    
    # Métodos para Leads
    def get_leads(self, params: Optional[Dict] = None) -> Dict:
        """Obtém a lista de leads com parâmetros opcionais"""
        return self._make_request("leads", params)
    
    def get_lead(self, lead_id: int) -> Dict:
        """Obtém detalhes de um lead específico"""
        return self._make_request(f"leads/{lead_id}")
    
    # Métodos para Tags
    def get_tags(self) -> Dict:
        """Obtém todas as tags disponíveis"""
        return self._make_request("leads/tags")
    
    # Métodos para Pipelines
    def get_pipelines(self) -> Dict:
        """Obtém todos os pipelines"""
        return self._make_request("leads/pipelines")
    
    def get_pipeline_statuses(self, pipeline_id: int) -> Dict:
        """Obtém todos os estágios de um pipeline"""
        return self._make_request(f"leads/pipelines/{pipeline_id}/statuses")
    
    # Métodos para Usuários
    def get_users(self) -> Dict:
        """Obtém todos os usuários/corretores"""
        return self._make_request("users")
    
    # Métodos para Campos Personalizados
    def get_custom_fields(self) -> Dict:
        """Obtém definições de campos personalizados para leads"""
        return self._make_request("leads/custom_fields")
    
    # Métodos para Fontes
    def get_sources(self) -> Dict:
        """Obtém todas as fontes de leads disponíveis"""
        return self._make_request("sources")
    
    # Métodos para Eventos
    def get_events(self, params: Optional[Dict] = None) -> Dict:
        """Obtém eventos do Kommo com filtros opcionais"""
        return self._make_request("events", params)
    
    # Métodos de Utilidade
    def unix_to_datetime(self, timestamp: int) -> datetime:
        """Converte Unix timestamp para objeto datetime"""
        if not timestamp:
            return None
        return datetime.fromtimestamp(timestamp)
    
    def calculate_duration_days(self, start_timestamp: int, end_timestamp: int) -> float:
        """Calcula a duração em dias entre dois timestamps"""
        if not start_timestamp or not end_timestamp:
            return 0
        return (end_timestamp - start_timestamp) / (60 * 60 * 24)