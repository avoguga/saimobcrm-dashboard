import requests
from typing import Dict, List, Optional, Union, Any
import config
from datetime import datetime
import json
from concurrent.futures import ThreadPoolExecutor
import time
import hashlib
from functools import lru_cache
import redis
import pickle
import logging
import asyncio
import aiohttp
import threading

logger = logging.getLogger(__name__)

# Rate limiter global (compartilhado entre todas as threads)
class GlobalRateLimiter:
    """Rate limiter thread-safe para respeitar limite de 7 req/s da Kommo"""
    def __init__(self, max_requests_per_second: float = 6.0):
        self._lock = threading.Lock()
        self._min_interval = 1.0 / max_requests_per_second  # ~0.166s entre requests
        self._last_request_time = 0.0
        self._request_count = 0

    def wait(self):
        """Aguarda até que seja seguro fazer uma nova requisição"""
        with self._lock:
            now = time.time()
            time_since_last = now - self._last_request_time

            if time_since_last < self._min_interval:
                sleep_time = self._min_interval - time_since_last
                time.sleep(sleep_time)

            self._last_request_time = time.time()
            self._request_count += 1

            # Log a cada 50 requests
            if self._request_count % 50 == 0:
                logger.info(f"Rate limiter: {self._request_count} requests processados")

# Instância global do rate limiter (para requisições síncronas)
# Kommo permite 7 req/s - usar máximo
_rate_limiter = GlobalRateLimiter(max_requests_per_second=7.0)

# Rate limiter async global (para aiohttp)
class AsyncGlobalRateLimiter:
    """Rate limiter async thread-safe para requisições aiohttp"""
    def __init__(self, max_requests_per_second: float = 6.0):
        self._lock = asyncio.Lock()
        self._min_interval = 1.0 / max_requests_per_second
        self._last_request_time = 0.0
        self._request_count = 0

    async def wait(self):
        """Aguarda até que seja seguro fazer uma nova requisição"""
        async with self._lock:
            now = time.time()
            time_since_last = now - self._last_request_time

            if time_since_last < self._min_interval:
                sleep_time = self._min_interval - time_since_last
                await asyncio.sleep(sleep_time)

            self._last_request_time = time.time()
            self._request_count += 1

            if self._request_count % 50 == 0:
                logger.info(f"Async rate limiter: {self._request_count} requests processados")

# Instância global do rate limiter async
_async_rate_limiter = None

def get_async_rate_limiter() -> AsyncGlobalRateLimiter:
    """Obtém ou cria o rate limiter async global"""
    global _async_rate_limiter
    if _async_rate_limiter is None:
        # Kommo permite 7 req/s - usar máximo
        _async_rate_limiter = AsyncGlobalRateLimiter(max_requests_per_second=7.0)
    return _async_rate_limiter

class KommoAPI:
    def __init__(self):
        self.base_url = config.KOMMO_API_URL
        self.headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {config.KOMMO_TOKEN}"
        }
        # Redis cache
        self.redis_client = None
        self._cache_ttl = config.CACHE_TTL
        self._init_redis()

        # Fallback cache em memória
        self._memory_cache = {}

        # Referência ao rate limiter global
        self._rate_limiter = _rate_limiter

    def _init_redis(self):
        """Inicializa conexão Redis"""
        try:
            self.redis_client = redis.from_url(
                config.REDIS_URL, 
                decode_responses=False,
                socket_timeout=5,
                socket_connect_timeout=5
            )
            # Testar conexão
            self.redis_client.ping()
            logger.info(f"Redis conectado: {config.REDIS_URL[:50]}...")
        except Exception as e:
            logger.warning(f"Redis não conectado, usando cache em memória: {e}")
            self.redis_client = None
    
    def _get_cache_key(self, endpoint: str, params: Optional[Dict] = None) -> str:
        """Gera uma chave única para o cache baseada no endpoint e parâmetros"""
        if params:
            # Ordenar parâmetros para gerar chave consistente
            sorted_params = sorted(params.items())
            params_str = json.dumps(sorted_params, sort_keys=True)
        else:
            params_str = ""
        
        cache_string = f"{endpoint}|{params_str}"
        cache_hash = hashlib.md5(cache_string.encode()).hexdigest()
        return f"kommo:{cache_hash}"
    
    def _get_from_cache(self, cache_key: str) -> Optional[Dict]:
        """Recupera dados do cache (Redis primeiro, memória como fallback)"""
        # Tentar Redis primeiro
        if self.redis_client:
            try:
                cached_data = self.redis_client.get(cache_key)
                if cached_data:
                    data = pickle.loads(cached_data)
                    logger.info(f"Redis Cache HIT para {cache_key[:8]}...")
                    return data
            except Exception as e:
                logger.warning(f"Erro no Redis cache: {e}")
        
        # Fallback para cache em memória
        if cache_key in self._memory_cache:
            cached_data, timestamp = self._memory_cache[cache_key]
            if time.time() - timestamp < self._cache_ttl:
                logger.info(f"Memory Cache HIT para {cache_key[:8]}...")
                return cached_data
            else:
                # Cache expirado, remover
                del self._memory_cache[cache_key]
        return None
    
    def _save_to_cache(self, cache_key: str, data: Dict):
        """Salva dados no cache (Redis primeiro, memória como fallback)"""
        # Tentar Redis primeiro
        if self.redis_client:
            try:
                serialized_data = pickle.dumps(data)
                self.redis_client.setex(cache_key, self._cache_ttl, serialized_data)
                logger.info(f"Redis Cache SAVE para {cache_key[:8]}...")
                return
            except Exception as e:
                logger.warning(f"Erro ao salvar no Redis: {e}")
        
        # Fallback para cache em memória
        self._memory_cache[cache_key] = (data, time.time())
        logger.info(f"Memory Cache SAVE para {cache_key[:8]}...")
    
    def clear_cache(self):
        """Limpa todo o cache"""
        # Limpar Redis
        if self.redis_client:
            try:
                # Buscar chaves que começam com kommo:
                keys = self.redis_client.keys("kommo:*")
                if keys:
                    self.redis_client.delete(*keys)
                    logger.info(f"Redis Cache LIMPO ({len(keys)} chaves)")
            except Exception as e:
                logger.warning(f"Erro ao limpar Redis: {e}")
        
        # Limpar cache em memória
        self._memory_cache.clear()
        logger.info("Memory Cache LIMPO")
    
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
                # Aplicar rate limiter ANTES de cada requisição
                self._rate_limiter.wait()

                response = requests.get(url, headers=self.headers, params=params)
                
                # Imprimir informações para debug (apenas na primeira tentativa)
                if attempt == 0:
                    print(f"Request URL: {response.url}")
                    print(f"Status Code: {response.status_code}")
                    print(f"Response Headers: {dict(response.headers)}")
                
                # Se receber 429, fazer retry com delay
                if response.status_code == 429 and attempt < max_retries - 1:
                    delay = base_delay * (2 ** attempt)  # Backoff exponencial
                    print(f"Rate limit atingido (429) - Tentativa {attempt + 1}/{max_retries}. Aguardando {delay}s...")
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
                        print(f"Rate limit atingido (429) - Tentativa {attempt + 1}/{max_retries}. Aguardando {delay}s...")
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
            print(f"get_leads: Retorno inválido (tipo: {type(result)}) - retornando estrutura vazia")
            return {"_embedded": {"leads": []}, "_page": {"total": 0}}
        
        # Se há indicador de erro, retornar estrutura vazia
        if result.get("_error"):
            print(f"get_leads: Erro na API - {result.get('_error_message', 'Erro desconhecido')}")
            return {"_embedded": {"leads": []}, "_page": {"total": 0}}
        
        return result
    
    def get_lead(self, lead_id: int, use_cache: bool = False) -> Dict:
        """Obtém detalhes de um lead específico

        Args:
            lead_id: ID do lead no Kommo
            use_cache: Se deve usar cache (default: False para garantir dados frescos)
        """
        params = {"with": "custom_fields_values,contacts,tags"}
        return self._make_request(f"leads/{lead_id}", params=params, use_cache=use_cache)
    
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
    
    def get_all_tasks(self, params: Optional[Dict] = None, max_pages: int = 20) -> List[Dict]:
        """Obtém todas as tarefas usando paginação automática
        
        Args:
            params: Parâmetros da consulta
            max_pages: Número máximo de páginas para buscar (default: 20)
        
        Returns:
            Lista de todas as tarefas encontradas
        """
        all_tasks = []
        page = 1
        
        if params is None:
            params = {}
        
        print(f"get_all_tasks: Iniciando busca com params: {params}")
        
        while page <= max_pages:
            params_copy = params.copy()
            params_copy['page'] = page
            params_copy['limit'] = 250  # Máximo por página
            
            print(f"get_all_tasks: Buscando página {page}...")
            response = self.get_tasks(params_copy)
            
            if not response or '_embedded' not in response or 'tasks' not in response['_embedded']:
                print(f"get_all_tasks: Página {page} sem dados")
                break
            
            tasks = response['_embedded']['tasks']
            if not tasks:
                print(f"get_all_tasks: Página {page} lista vazia")
                break
                
            all_tasks.extend(tasks)
            print(f"get_all_tasks: Página {page} adicionou {len(tasks)} tarefas (total: {len(all_tasks)})")
            
            # Verificar se há mais páginas
            if '_links' in response and 'next' in response['_links']:
                if len(tasks) < 250:
                    print(f"get_all_tasks: Página {page} incompleta, parando")
                    break
                page += 1
            else:
                print(f"get_all_tasks: Página {page} sem 'next' link, parando")
                break
        
        if page > max_pages:
            print(f"get_all_tasks: ATINGIU LIMITE de {max_pages} páginas!")
        
        print(f"get_all_tasks: CONCLUÍDO - {len(all_tasks)} tarefas em {page-1} páginas")
        return all_tasks
    
    # Método para buscar leads com paginação completa (versão antiga sequencial)
    def get_all_leads_old(self, params: Optional[Dict] = None) -> List[Dict]:
        """Obtém todos os leads usando paginação automática (MÉTODO ANTIGO LENTO)"""
        all_leads = []
        page = 1
        max_pages = 30  # LIMITE DE SEGURANÇA: máximo 30 páginas = 7500 leads
        
        if params is None:
            params = {}
        
        print(f"get_all_leads_old: Iniciando busca com params: {params}")
        
        while page <= max_pages:
            params['page'] = page
            params['limit'] = 250  # Máximo por página
            
            print(f"get_all_leads_old: Buscando página {page}...")
            response = self.get_leads(params)
            
            if not response or '_embedded' not in response or 'leads' not in response['_embedded']:
                print(f"get_all_leads_old: Página {page} sem dados")
                break
            
            leads = response['_embedded']['leads']
            if not leads:
                print(f"get_all_leads_old: Página {page} lista vazia")
                break
                
            all_leads.extend(leads)
            print(f"get_all_leads_old: Página {page} adicionou {len(leads)} leads (total: {len(all_leads)})")
            
            # Verificar se há mais páginas
            if '_links' in response and 'next' in response['_links']:
                if len(leads) < 250:
                    print(f"get_all_leads_old: Página {page} incompleta, parando")
                    break
                page += 1
            else:
                print(f"get_all_leads_old: Página {page} sem 'next' link, parando")
                break
        
        if page > max_pages:
            print(f"get_all_leads_old: ATINGIU LIMITE de {max_pages} páginas!")
        
        print(f"get_all_leads_old: CONCLUÍDO - {len(all_leads)} leads em {page-1} páginas")
        return all_leads
    
    
    def _fetch_page_parallel(self, params: Dict, page: int) -> Dict:
        """Busca uma página específica usando requests em thread pool"""
        params_copy = params.copy()
        params_copy['page'] = page
        params_copy['limit'] = 250
        
        url = f"{self.base_url}/leads"
        
        try:
            response = requests.get(url, headers=self.headers, params=params_copy, timeout=30)
            print(f"Página {page}: Status {response.status_code}")
            if response.status_code == 200:
                return response.json()
            else:
                print(f"Página {page}: Erro {response.status_code}")
                return {}
        except Exception as e:
            print(f"Página {page}: Exceção {str(e)}")
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
        print(f"get_all_leads: Iniciando busca PARALELA com params: {params}")
        
        # Primeiro, fazer uma requisição para descobrir quantas páginas existem
        test_params = params.copy()
        test_params['page'] = 1
        test_params['limit'] = 250
        
        print(f"Descobrindo número total de páginas...")
        first_response = self.get_leads(test_params)
        
        # Validação robusta do retorno da API
        if not first_response:
            print(f"Erro: Resposta vazia da API")
            return []
        
        if not isinstance(first_response, dict):
            print(f"Erro: Resposta inválida da API (tipo: {type(first_response)})")
            return []
        
        if '_embedded' not in first_response:
            print(f"Erro: Resposta sem '_embedded' - estrutura inválida")
            return []
        
        # Calcular número total de páginas
        # CORREÇÃO: Kommo nem sempre retorna _page.total, então usamos heurística
        page_info = first_response.get('_page', {})
        total_count = page_info.get('total', 0) if isinstance(page_info, dict) else 0

        # Verificar quantos leads vieram na primeira página
        first_page_leads = first_response.get('_embedded', {}).get('leads', [])
        first_page_count = len(first_page_leads)

        # Se não temos total_count mas temos 250 leads na primeira página, há mais páginas
        # Usar heurística: se primeira página está cheia, assumir mais páginas
        if total_count == 0 and first_page_count >= 250:
            # Estimar baseado no fato de que há mais páginas
            has_next = '_links' in first_response and 'next' in first_response.get('_links', {})
            if has_next:
                total_count = 250 * 15  # Assumir até 15 páginas para busca paralela
                print(f"Total desconhecido, primeira página cheia ({first_page_count}), estimando {total_count}")

        print(f"Total de leads encontrados: {total_count} (primeira página: {first_page_count})")

        items_per_page = 250
        calculated_pages = (total_count + items_per_page - 1) // items_per_page if total_count > 0 else 1

        # Usar max_pages customizado ou padrão baseado na quantidade de dados
        if max_pages is not None:
            total_pages = min(calculated_pages, max_pages)
            print(f"Limite personalizado: {max_pages} páginas")
        else:
            # Limite inteligente baseado no volume de dados
            if total_count <= 1000:  # Poucos dados
                total_pages = min(calculated_pages, 5)
                print(f"Limite RÁPIDO: {total_pages} páginas (dados pequenos)")
            elif total_count <= 3000:  # Dados moderados
                total_pages = min(calculated_pages, 12)
                print(f"Limite MÉDIO: {total_pages} páginas (dados moderados)")
            else:  # Muitos dados
                total_pages = min(calculated_pages, 20)
                print(f"Limite PADRÃO: {total_pages} páginas (dados extensos)")

        # Se primeira página não está cheia, só há uma página
        if first_page_count < 250:
            print(f"Primeira página não está cheia ({first_page_count}), retornando apenas primeira página")
            return first_page_leads

        if total_pages == 0:
            return []

        print(f"Total estimado: {total_count} leads em {total_pages} páginas")

        if not use_parallel or total_pages == 1:
            # Se não usar paralelo ou só tem 1 página, usar método sequencial otimizado
            return self.get_all_leads_old(params)
        
        # Usar ThreadPoolExecutor para requisições paralelas
        all_leads = []
        
        # Adicionar leads da primeira página
        if 'leads' in first_response['_embedded']:
            all_leads.extend(first_response['_embedded']['leads'])
            print(f"Página 1: {len(first_response['_embedded']['leads'])} leads")
        
        # Se há mais páginas, buscar em paralelo
        if total_pages > 1:
            pages_to_fetch = list(range(2, total_pages + 1))
            
            print(f"Buscando páginas {pages_to_fetch} em paralelo com {max_workers} threads...")
            
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
                            print(f"Página {page}: {len(leads)} leads")
                        else:
                            print(f"Página {page}: Sem dados")
                    except Exception as e:
                        print(f"Página {page}: Erro {str(e)}")
        
        elapsed_time = time.time() - start_time
        print(f"get_all_leads: CONCLUÍDO - {len(all_leads)} leads em {total_pages} páginas em {elapsed_time:.2f}s")
        
        return all_leads

    async def get_all_leads_async(self, params: Optional[Dict] = None, max_pages: int = 15) -> List[Dict]:
        """
        Obtém todos os leads usando aiohttp para requisições paralelas controladas.

        Implementa:
        - Semáforo para controlar concorrência (respeita rate limit Kommo: 7 req/s)
        - Retry com backoff exponencial em caso de falha
        - Tratamento adequado de rate limiting (429)

        Args:
            params: Parâmetros da consulta
            max_pages: Máximo de páginas a buscar (default: 15)

        Returns:
            Lista com todos os leads
        """
        if params is None:
            params = {}

        start_time = time.time()
        logger.info(f"get_all_leads_async: Iniciando busca com aiohttp, params: {params}")

        all_leads = []
        base_url = f"{self.base_url}/leads"

        # Rate limiter global async (compartilhado entre todas as chamadas)
        rate_limiter = get_async_rate_limiter()

        async def fetch_page_with_retry(session: aiohttp.ClientSession, page: int, max_retries: int = 3) -> Dict:
            """Busca uma página com retry e backoff exponencial"""
            page_params = params.copy()
            page_params['page'] = page
            page_params['limit'] = 250

            for attempt in range(max_retries):
                try:
                    # Aplicar rate limiter ANTES de cada requisição
                    await rate_limiter.wait()
                    async with session.get(base_url, params=page_params) as response:
                        if response.status == 200:
                            data = await response.json()
                            return {"page": page, "data": data, "success": True}
                        elif response.status == 204:
                            return {"page": page, "data": None, "success": True, "empty": True}
                        elif response.status == 429:  # Rate limited
                            wait_time = (2 ** attempt) * 0.5  # Backoff: 0.5s, 1s, 2s
                            logger.warning(f"Página {page}: Rate limited, aguardando {wait_time}s...")
                            await asyncio.sleep(wait_time)
                            continue
                        else:
                            logger.warning(f"Página {page}: Status {response.status}")
                            if attempt < max_retries - 1:
                                await asyncio.sleep(0.5 * (attempt + 1))
                                continue
                            return {"page": page, "data": None, "success": False}
                except asyncio.TimeoutError:
                    logger.warning(f"Página {page}: Timeout (tentativa {attempt + 1}/{max_retries})")
                    if attempt < max_retries - 1:
                        await asyncio.sleep(1 * (attempt + 1))
                        continue
                    return {"page": page, "data": None, "success": False, "error": "timeout"}
                except Exception as e:
                    logger.error(f"Página {page}: Erro {str(e)} (tentativa {attempt + 1}/{max_retries})")
                    if attempt < max_retries - 1:
                        await asyncio.sleep(0.5 * (attempt + 1))
                        continue
                    return {"page": page, "data": None, "success": False, "error": str(e)}

            return {"page": page, "data": None, "success": False, "error": "max_retries"}

        # Usar um único ClientSession para melhor performance (connection pooling)
        # Otimizado: mais conexões para maximizar throughput com rate limit de 7 req/s
        connector = aiohttp.TCPConnector(limit=15, limit_per_host=10)
        timeout = aiohttp.ClientTimeout(total=30, connect=10)

        async with aiohttp.ClientSession(
            headers=self.headers,
            connector=connector,
            timeout=timeout
        ) as session:
            # Primeira requisição para verificar se há dados
            first_result = await fetch_page_with_retry(session, 1)

            if not first_result["success"] or first_result.get("empty"):
                logger.info("get_all_leads_async: Nenhum dado encontrado")
                return []

            first_data = first_result["data"]
            if not first_data or "_embedded" not in first_data:
                return []

            first_leads = first_data.get("_embedded", {}).get("leads", [])
            all_leads.extend(first_leads)
            logger.info(f"Página 1: {len(first_leads)} leads")

            # Se primeira página não está cheia, não há mais páginas
            if len(first_leads) < 250:
                elapsed = time.time() - start_time
                logger.info(f"get_all_leads_async: CONCLUÍDO - {len(all_leads)} leads em 1 página em {elapsed:.2f}s")
                return all_leads

            # Buscar páginas 2 a max_pages em paralelo com controle de concorrência
            pages_to_fetch = list(range(2, max_pages + 1))
            logger.info(f"Buscando páginas {pages_to_fetch} em paralelo (semaphore=5)...")

            # Criar tasks para todas as páginas
            tasks = [fetch_page_with_retry(session, page) for page in pages_to_fetch]

            # Executar todas em paralelo (semáforo controla a concorrência real)
            results = await asyncio.gather(*tasks, return_exceptions=True)

            # Processar resultados e contar falhas
            failed_pages = []
            for result in results:
                if isinstance(result, Exception):
                    logger.error(f"Exceção: {str(result)}")
                    continue

                if not result["success"]:
                    failed_pages.append(result["page"])
                    continue

                if result.get("empty"):
                    continue

                data = result["data"]
                if data and "_embedded" in data and "leads" in data["_embedded"]:
                    leads = data["_embedded"]["leads"]
                    all_leads.extend(leads)
                    logger.info(f"Página {result['page']}: {len(leads)} leads")

            if failed_pages:
                logger.warning(f"Páginas com falha: {failed_pages}")

        elapsed = time.time() - start_time
        logger.info(f"get_all_leads_async: CONCLUÍDO - {len(all_leads)} leads em {elapsed:.2f}s")

        return all_leads

    async def get_all_leads_parallel_async(self, params_list: List[Dict], max_pages: int = 15) -> List[List[Dict]]:
        """
        Busca leads de MÚLTIPLOS pipelines em paralelo usando aiohttp.

        Args:
            params_list: Lista de parâmetros, um para cada pipeline
            max_pages: Máximo de páginas por pipeline

        Returns:
            Lista de listas, cada uma contendo os leads de um pipeline
        """
        start_time = time.time()
        logger.info(f"get_all_leads_parallel_async: Buscando {len(params_list)} pipelines em paralelo")

        # Criar tasks para cada pipeline
        tasks = [self.get_all_leads_async(params, max_pages) for params in params_list]

        # Executar todos em paralelo
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Processar resultados
        final_results = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(f"Pipeline {i}: Exceção {str(result)}")
                final_results.append([])
            else:
                final_results.append(result)

        elapsed = time.time() - start_time
        total_leads = sum(len(r) for r in final_results)
        logger.info(f"get_all_leads_parallel_async: CONCLUÍDO - {total_leads} leads total em {elapsed:.2f}s")

        return final_results

    async def get_all_tasks_async(self, params: Optional[Dict] = None, max_pages: int = 10) -> List[Dict]:
        """
        Obtém todas as tasks usando aiohttp para requisições paralelas.

        Args:
            params: Parâmetros da consulta
            max_pages: Máximo de páginas a buscar

        Returns:
            Lista com todas as tasks
        """
        if params is None:
            params = {}

        start_time = time.time()
        logger.info(f"get_all_tasks_async: Iniciando busca com params: {params}")

        all_tasks = []
        base_url = f"{self.base_url}/tasks"
        rate_limiter = get_async_rate_limiter()

        async def fetch_page(session: aiohttp.ClientSession, page: int) -> Dict:
            """Busca uma página de tasks"""
            page_params = params.copy()
            page_params['page'] = page
            page_params['limit'] = 250

            await rate_limiter.wait()
            try:
                async with session.get(base_url, params=page_params) as response:
                    if response.status == 200:
                        data = await response.json()
                        return {"page": page, "data": data, "success": True}
                    elif response.status == 204:
                        return {"page": page, "data": None, "success": True, "empty": True}
                    return {"page": page, "data": None, "success": False}
            except Exception as e:
                logger.error(f"Tasks página {page}: Erro {str(e)}")
                return {"page": page, "data": None, "success": False}

        connector = aiohttp.TCPConnector(limit=15, limit_per_host=10)
        timeout = aiohttp.ClientTimeout(total=30, connect=10)

        async with aiohttp.ClientSession(
            headers=self.headers,
            connector=connector,
            timeout=timeout
        ) as session:
            # Primeira página
            first_result = await fetch_page(session, 1)

            if not first_result["success"] or first_result.get("empty"):
                return []

            first_data = first_result["data"]
            if not first_data or "_embedded" not in first_data:
                return []

            first_tasks = first_data.get("_embedded", {}).get("tasks", [])
            all_tasks.extend(first_tasks)
            logger.info(f"Tasks página 1: {len(first_tasks)}")

            # Se primeira página não cheia, não há mais
            if len(first_tasks) < 250:
                elapsed = time.time() - start_time
                logger.info(f"get_all_tasks_async: CONCLUÍDO - {len(all_tasks)} tasks em {elapsed:.2f}s")
                return all_tasks

            # Buscar demais páginas em paralelo
            pages_to_fetch = list(range(2, max_pages + 1))
            tasks = [fetch_page(session, page) for page in pages_to_fetch]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            for result in results:
                if isinstance(result, Exception):
                    continue
                if not result["success"] or result.get("empty"):
                    continue
                data = result["data"]
                if data and "_embedded" in data and "tasks" in data["_embedded"]:
                    tasks_list = data["_embedded"]["tasks"]
                    all_tasks.extend(tasks_list)
                    logger.info(f"Tasks página {result['page']}: {len(tasks_list)}")

        elapsed = time.time() - start_time
        logger.info(f"get_all_tasks_async: CONCLUÍDO - {len(all_tasks)} tasks em {elapsed:.2f}s")
        return all_tasks

    async def get_leads_batch_async(self, lead_ids: List[int]) -> List[Dict]:
        """
        Busca múltiplos leads por ID em paralelo.

        Args:
            lead_ids: Lista de IDs de leads a buscar

        Returns:
            Lista com os leads encontrados
        """
        if not lead_ids:
            return []

        start_time = time.time()
        logger.info(f"get_leads_batch_async: Buscando {len(lead_ids)} leads")

        rate_limiter = get_async_rate_limiter()
        leads = []

        async def fetch_lead(session: aiohttp.ClientSession, lead_id: int) -> Optional[Dict]:
            """Busca um lead individual"""
            await rate_limiter.wait()
            url = f"{self.base_url}/leads/{lead_id}"
            try:
                async with session.get(url) as response:
                    if response.status == 200:
                        return await response.json()
                    return None
            except Exception as e:
                logger.warning(f"Lead {lead_id}: Erro {str(e)}")
                return None

        connector = aiohttp.TCPConnector(limit=15, limit_per_host=10)
        timeout = aiohttp.ClientTimeout(total=30, connect=10)

        async with aiohttp.ClientSession(
            headers=self.headers,
            connector=connector,
            timeout=timeout
        ) as session:
            tasks = [fetch_lead(session, lid) for lid in lead_ids]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            for result in results:
                if isinstance(result, Exception):
                    continue
                if result:
                    leads.append(result)

        elapsed = time.time() - start_time
        logger.info(f"get_leads_batch_async: CONCLUÍDO - {len(leads)} leads em {elapsed:.2f}s")
        return leads

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


# Instância singleton para ser compartilhada entre módulos
_kommo_api_instance = None

def get_kommo_api() -> KommoAPI:
    """Retorna instância singleton de KommoAPI"""
    global _kommo_api_instance
    if _kommo_api_instance is None:
        _kommo_api_instance = KommoAPI()
    return _kommo_api_instance