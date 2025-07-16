import requests
from typing import Dict, List, Optional, Union, Any
import config
from datetime import datetime
import json
from concurrent.futures import ThreadPoolExecutor
import time
import hashlib
from functools import lru_cache

class KommoAPI:
    def __init__(self):
        self.base_url = config.KOMMO_API_URL
        self.headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {config.KOMMO_TOKEN}"
        }
        # Cache simples em memória com timestamp
        self._cache = {}
        self._cache_ttl = 300  # 5 minutos
    
    def _get_cache_key(self, endpoint: str, params: Optional[Dict] = None) -> str:
        """Gera uma chave única para o cache baseada no endpoint e parâmetros"""
        if params:
            # Ordenar parâmetros para gerar chave consistente
            sorted_params = sorted(params.items())
            params_str = json.dumps(sorted_params, sort_keys=True)
        else:
            params_str = ""
        
        cache_string = f"{endpoint}|{params_str}"
        return hashlib.md5(cache_string.encode()).hexdigest()
    
    def _get_from_cache(self, cache_key: str) -> Optional[Dict]:
        """Recupera dados do cache se ainda válidos"""
        if cache_key in self._cache:
            cached_data, timestamp = self._cache[cache_key]
            if time.time() - timestamp < self._cache_ttl:
                print(f"💾 Cache HIT para {cache_key[:8]}...")
                return cached_data
            else:
                # Cache expirado, remover
                del self._cache[cache_key]
                print(f"⏰ Cache EXPIRADO para {cache_key[:8]}...")
        return None
    
    def _save_to_cache(self, cache_key: str, data: Dict):
        """Salva dados no cache com timestamp"""
        self._cache[cache_key] = (data, time.time())
        print(f"💾 Cache SAVE para {cache_key[:8]}...")
    
    def clear_cache(self):
        """Limpa todo o cache"""
        self._cache.clear()
        print("💾 Cache LIMPO")
    
    def _make_request(self, endpoint: str, params: Optional[Dict] = None, use_cache: bool = True, retry_on_429: bool = True) -> Dict:
        """Método genérico para fazer requisições à API Kommo com cache e tratamento de erro melhorado"""
        # Verificar cache primeiro
        if use_cache:
            cache_key = self._get_cache_key(endpoint, params)
            cached_result = self._get_from_cache(cache_key)
            if cached_result is not None:
                return cached_result
        url = f"{self.base_url}/{endpoint}"
        
        # Implementar retry com backoff exponencial para 429 errors
        max_retries = 3 if retry_on_429 else 1
        base_delay = 1.0  # 1 segundo inicial
        
        for attempt in range(max_retries):
            try:
                response = requests.get(url, headers=self.headers, params=params)
                
                # Imprimir informações para debug (apenas na primeira tentativa)
                if attempt == 0:
                    print(f"Request URL: {response.url}")
                    print(f"Status Code: {response.status_code}")
                    print(f"Response Headers: {dict(response.headers)}")
                
                # Se receber 429, fazer retry com delay
                if response.status_code == 429 and attempt < max_retries - 1:
                    delay = base_delay * (2 ** attempt)  # Backoff exponencial
                    print(f"⚠️ Rate limit atingido (429) - Tentativa {attempt + 1}/{max_retries}. Aguardando {delay}s...")
                    time.sleep(delay)
                    continue
                
                # Verificar se a resposta foi bem-sucedida
                response.raise_for_status()
                
                # Verificar se a resposta contém conteúdo
                if not response.text:
                    print("Resposta vazia recebida da API")
                    return {}
                
                # Tentar fazer o parse do JSON
                try:
                    result = response.json()
                    # Salvar no cache se a requisição foi bem-sucedida
                    if use_cache and result:
                        cache_key = self._get_cache_key(endpoint, params)
                        self._save_to_cache(cache_key, result)
                    return result
                except ValueError as e:
                    print(f"Erro ao analisar JSON: {e}")
                    print(f"Conteúdo da resposta: {response.text[:200]}...")  # Mostrar os primeiros 200 caracteres
                    raise ValueError(f"Resposta inválida da API Kommo: {e}")
            
            except requests.exceptions.RequestException as e:
                print(f"Erro de requisição HTTP: {e}")
                if hasattr(e, 'response') and e.response is not None:
                    print(f"Status Code: {e.response.status_code}")
                    print(f"Response Content: {e.response.text[:500]}")
                    
                    # Se for 429 e não for a última tentativa, tentar novamente
                    if e.response.status_code == 429 and attempt < max_retries - 1:
                        delay = base_delay * (2 ** attempt)
                        print(f"⚠️ Rate limit atingido (429) - Tentativa {attempt + 1}/{max_retries}. Aguardando {delay}s...")
                        time.sleep(delay)
                        continue
                
                # Retornar estrutura vazia mas com indicador de erro
                return {"_error": True, "_error_message": str(e)}
    
    # Métodos para Leads
    def get_leads(self, params: Optional[Dict] = None) -> Dict:
        """Obtém a lista de leads com parâmetros opcionais"""
        result = self._make_request("leads", params)
        
        # Validação adicional para evitar erros downstream
        if not isinstance(result, dict):
            print(f"⚠️ get_leads: Retorno inválido (tipo: {type(result)}) - retornando estrutura vazia")
            return {"_embedded": {"leads": []}, "_page": {"total": 0}}
        
        # Se há indicador de erro, retornar estrutura vazia
        if result.get("_error"):
            print(f"⚠️ get_leads: Erro na API - {result.get('_error_message', 'Erro desconhecido')}")
            return {"_embedded": {"leads": []}, "_page": {"total": 0}}
        
        return result
    
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
    
    # Métodos para Tarefas
    def get_tasks(self, params: Optional[Dict] = None) -> Dict:
        """Obtém tarefas com filtros opcionais"""
        return self._make_request("tasks", params)
    
    # Método para buscar leads com paginação completa (versão antiga sequencial)
    def get_all_leads_old(self, params: Optional[Dict] = None) -> List[Dict]:
        """Obtém todos os leads usando paginação automática (MÉTODO ANTIGO LENTO)"""
        all_leads = []
        page = 1
        max_pages = 20  # LIMITE DE SEGURANÇA: máximo 20 páginas = 5000 leads
        
        if params is None:
            params = {}
        
        print(f"🔍 get_all_leads_old: Iniciando busca com params: {params}")
        
        while page <= max_pages:
            params['page'] = page
            params['limit'] = 250  # Máximo por página
            
            print(f"📄 get_all_leads_old: Buscando página {page}...")
            response = self.get_leads(params)
            
            if not response or '_embedded' not in response or 'leads' not in response['_embedded']:
                print(f"❌ get_all_leads_old: Página {page} sem dados")
                break
            
            leads = response['_embedded']['leads']
            if not leads:
                print(f"❌ get_all_leads_old: Página {page} lista vazia")
                break
                
            all_leads.extend(leads)
            print(f"✅ get_all_leads_old: Página {page} adicionou {len(leads)} leads (total: {len(all_leads)})")
            
            # Verificar se há mais páginas
            if '_links' in response and 'next' in response['_links']:
                if len(leads) < 250:
                    print(f"🏁 get_all_leads_old: Página {page} incompleta, parando")
                    break
                page += 1
            else:
                print(f"🏁 get_all_leads_old: Página {page} sem 'next' link, parando")
                break
        
        if page > max_pages:
            print(f"⚠️ get_all_leads_old: ATINGIU LIMITE de {max_pages} páginas!")
        
        print(f"📊 get_all_leads_old: CONCLUÍDO - {len(all_leads)} leads em {page-1} páginas")
        return all_leads
    
    
    def _fetch_page_parallel(self, params: Dict, page: int) -> Dict:
        """Busca uma página específica usando requests em thread pool"""
        params_copy = params.copy()
        params_copy['page'] = page
        params_copy['limit'] = 250
        
        url = f"{self.base_url}/leads"
        
        try:
            response = requests.get(url, headers=self.headers, params=params_copy, timeout=30)
            print(f"📄 Página {page}: Status {response.status_code}")
            if response.status_code == 200:
                return response.json()
            else:
                print(f"❌ Página {page}: Erro {response.status_code}")
                return {}
        except Exception as e:
            print(f"❌ Página {page}: Exceção {str(e)}")
            return {}
    
    def get_all_leads(self, params: Optional[Dict] = None, use_parallel: bool = True, max_workers: int = 8, max_pages: Optional[int] = None) -> List[Dict]:
        """Obtém todos os leads usando paginação PARALELA otimizada
        
        Args:
            params: Parâmetros da consulta
            use_parallel: Se deve usar requisições paralelas (default: True)
            max_workers: Número máximo de threads paralelas (default: 8)
        """
        if params is None:
            params = {}
        
        start_time = time.time()
        print(f"🚀 get_all_leads: Iniciando busca PARALELA com params: {params}")
        
        # Primeiro, fazer uma requisição para descobrir quantas páginas existem
        test_params = params.copy()
        test_params['page'] = 1
        test_params['limit'] = 250
        
        print(f"🔍 Descobrindo número total de páginas...")
        first_response = self.get_leads(test_params)
        
        # Validação robusta do retorno da API
        if not first_response:
            print(f"❌ Erro: Resposta vazia da API")
            return []
        
        if not isinstance(first_response, dict):
            print(f"❌ Erro: Resposta inválida da API (tipo: {type(first_response)})")
            return []
        
        if '_embedded' not in first_response:
            print(f"❌ Erro: Resposta sem '_embedded' - estrutura inválida")
            return []
        
        # Calcular número total de páginas com limite inteligente
        page_info = first_response.get('_page', {})
        total_count = page_info.get('total', 0) if isinstance(page_info, dict) else 0
        
        print(f"📊 Total de leads encontrados: {total_count}")
        
        items_per_page = 250
        calculated_pages = (total_count + items_per_page - 1) // items_per_page if total_count > 0 else 1
        
        # Usar max_pages customizado ou padrão baseado na quantidade de dados
        if max_pages is not None:
            total_pages = min(calculated_pages, max_pages)
            print(f"🎯 Limite personalizado: {max_pages} páginas")
        else:
            # Limite inteligente baseado no volume de dados
            if total_count <= 1000:  # Poucos dados
                total_pages = min(calculated_pages, 5)
                print(f"🚀 Limite RÁPIDO: {total_pages} páginas (dados pequenos)")
            elif total_count <= 3000:  # Dados moderados
                total_pages = min(calculated_pages, 12)
                print(f"⚡ Limite MÉDIO: {total_pages} páginas (dados moderados)")
            else:  # Muitos dados
                total_pages = min(calculated_pages, 20)
                print(f"🔥 Limite PADRÃO: {total_pages} páginas (dados extensos)")
        
        if total_pages == 0:
            return []
        
        print(f"📊 Total estimado: {total_count} leads em {total_pages} páginas")
        
        if not use_parallel or total_pages == 1:
            # Se não usar paralelo ou só tem 1 página, usar método sequencial otimizado
            return self.get_all_leads_old(params)
        
        # Usar ThreadPoolExecutor para requisições paralelas
        all_leads = []
        
        # Adicionar leads da primeira página
        if 'leads' in first_response['_embedded']:
            all_leads.extend(first_response['_embedded']['leads'])
            print(f"✅ Página 1: {len(first_response['_embedded']['leads'])} leads")
        
        # Se há mais páginas, buscar em paralelo
        if total_pages > 1:
            pages_to_fetch = list(range(2, total_pages + 1))
            
            print(f"🔄 Buscando páginas {pages_to_fetch} em paralelo com {max_workers} threads...")
            
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # Submeter todas as requisições
                future_to_page = {
                    executor.submit(self._fetch_page_parallel, params, page): page 
                    for page in pages_to_fetch
                }
                
                # Processar resultados conforme chegam
                for future in future_to_page:
                    page = future_to_page[future]
                    try:
                        response = future.result(timeout=60)  # 60 segundos timeout
                        if response and '_embedded' in response and 'leads' in response['_embedded']:
                            leads = response['_embedded']['leads']
                            all_leads.extend(leads)
                            print(f"✅ Página {page}: {len(leads)} leads")
                        else:
                            print(f"⚠️ Página {page}: Sem dados")
                    except Exception as e:
                        print(f"❌ Página {page}: Erro {str(e)}")
        
        elapsed_time = time.time() - start_time
        print(f"🎉 get_all_leads: CONCLUÍDO - {len(all_leads)} leads em {total_pages} páginas em {elapsed_time:.2f}s")
        
        return all_leads
    
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