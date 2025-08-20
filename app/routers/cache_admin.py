from fastapi import APIRouter, HTTPException
from typing import Dict, Any
import redis
import logging
from config import settings

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/cache", tags=["Cache Admin"])

def get_redis_client():
    """Obtém cliente Redis"""
    try:
        redis_client = redis.from_url(
            settings.REDIS_URL, 
            decode_responses=False,
            socket_timeout=5,
            socket_connect_timeout=5
        )
        # Testar conexão
        redis_client.ping()
        return redis_client
    except Exception as e:
        logger.error(f"Erro ao conectar ao Redis: {e}")
        return None

@router.get("/info")
async def get_cache_info() -> Dict[str, Any]:
    """Retorna informações sobre o cache Redis"""
    try:
        redis_client = get_redis_client()
        if not redis_client:
            raise HTTPException(status_code=503, detail="Redis não está disponível")
        
        # Obter informações do Redis
        info = redis_client.info()
        
        # Contar chaves por padrão
        kommo_keys = len(redis_client.keys("kommo:*"))
        all_keys = len(redis_client.keys("*"))
        
        # Informações de memória
        memory_info = {
            "used_memory": info.get("used_memory_human", "N/A"),
            "used_memory_peak": info.get("used_memory_peak_human", "N/A"),
            "connected_clients": info.get("connected_clients", 0)
        }
        
        return {
            "status": "connected",
            "redis_version": info.get("redis_version", "unknown"),
            "total_keys": all_keys,
            "kommo_keys": kommo_keys,
            "memory_info": memory_info,
            "cache_ttl": settings.CACHE_TTL,
            "redis_url": settings.REDIS_URL.split("@")[-1] if "@" in settings.REDIS_URL else settings.REDIS_URL
        }
        
    except Exception as e:
        logger.error(f"Erro ao obter informações do cache: {e}")
        raise HTTPException(status_code=500, detail=f"Erro ao acessar cache: {str(e)}")

@router.delete("/flush")
async def flush_cache() -> Dict[str, Any]:
    """Limpa todo o cache Redis (CUIDADO: Remove todas as chaves!)"""
    try:
        redis_client = get_redis_client()
        if not redis_client:
            raise HTTPException(status_code=503, detail="Redis não está disponível")
        
        # Contar chaves antes de limpar
        keys_before = len(redis_client.keys("*"))
        
        # Flush do database atual
        redis_client.flushdb()
        
        logger.info(f"Cache Redis limpo: {keys_before} chaves removidas")
        
        return {
            "status": "success",
            "message": f"Cache limpo com sucesso. {keys_before} chaves removidas.",
            "keys_removed": keys_before
        }
        
    except Exception as e:
        logger.error(f"Erro ao limpar cache: {e}")
        raise HTTPException(status_code=500, detail=f"Erro ao limpar cache: {str(e)}")

@router.delete("/flush/kommo")
async def flush_kommo_cache() -> Dict[str, Any]:
    """Limpa apenas as chaves do cache relacionadas ao Kommo"""
    try:
        redis_client = get_redis_client()
        if not redis_client:
            raise HTTPException(status_code=503, detail="Redis não está disponível")
        
        # Buscar chaves do Kommo
        kommo_keys = redis_client.keys("kommo:*")
        
        if kommo_keys:
            # Deletar chaves do Kommo
            redis_client.delete(*kommo_keys)
            keys_removed = len(kommo_keys)
        else:
            keys_removed = 0
        
        logger.info(f"Cache Kommo limpo: {keys_removed} chaves removidas")
        
        return {
            "status": "success",
            "message": f"Cache Kommo limpo com sucesso. {keys_removed} chaves removidas.",
            "keys_removed": keys_removed
        }
        
    except Exception as e:
        logger.error(f"Erro ao limpar cache Kommo: {e}")
        raise HTTPException(status_code=500, detail=f"Erro ao limpar cache Kommo: {str(e)}")

@router.get("/keys")
async def get_cache_keys(pattern: str = "*", limit: int = 100) -> Dict[str, Any]:
    """Lista chaves do cache com padrão opcional"""
    try:
        redis_client = get_redis_client()
        if not redis_client:
            raise HTTPException(status_code=503, detail="Redis não está disponível")
        
        # Buscar chaves com padrão
        all_keys = redis_client.keys(pattern)
        
        # Limitar resultado
        keys_limited = all_keys[:limit]
        
        # Obter informações básicas sobre as chaves
        key_info = []
        for key in keys_limited:
            try:
                key_str = key.decode('utf-8') if isinstance(key, bytes) else str(key)
                ttl = redis_client.ttl(key)
                key_type = redis_client.type(key).decode('utf-8')
                
                key_info.append({
                    "key": key_str,
                    "type": key_type,
                    "ttl": ttl if ttl > 0 else None
                })
            except Exception:
                # Se houver erro ao processar uma chave, pular
                continue
        
        return {
            "pattern": pattern,
            "total_found": len(all_keys),
            "returned": len(key_info),
            "limit": limit,
            "keys": key_info
        }
        
    except Exception as e:
        logger.error(f"Erro ao listar chaves do cache: {e}")
        raise HTTPException(status_code=500, detail=f"Erro ao listar chaves: {str(e)}")

@router.delete("/key/{key_name}")
async def delete_cache_key(key_name: str) -> Dict[str, str]:
    """Deleta uma chave específica do cache"""
    try:
        redis_client = get_redis_client()
        if not redis_client:
            raise HTTPException(status_code=503, detail="Redis não está disponível")
        
        # Verificar se a chave existe
        exists = redis_client.exists(key_name)
        
        if exists:
            redis_client.delete(key_name)
            logger.info(f"Chave deletada: {key_name}")
            return {
                "status": "success",
                "message": f"Chave '{key_name}' deletada com sucesso."
            }
        else:
            return {
                "status": "not_found",
                "message": f"Chave '{key_name}' não encontrada."
            }
        
    except Exception as e:
        logger.error(f"Erro ao deletar chave {key_name}: {e}")
        raise HTTPException(status_code=500, detail=f"Erro ao deletar chave: {str(e)}")

@router.get("/stats")
async def get_cache_stats() -> Dict[str, Any]:
    """Retorna estatísticas detalhadas do cache"""
    try:
        redis_client = get_redis_client()
        if not redis_client:
            raise HTTPException(status_code=503, detail="Redis não está disponível")
        
        # Estatísticas por padrão de chave
        patterns = {
            "kommo:leads:*": "Leads",
            "kommo:users:*": "Usuários", 
            "kommo:pipelines:*": "Pipelines",
            "kommo:custom_fields:*": "Campos Personalizados",
            "kommo:*": "Total Kommo"
        }
        
        stats = {}
        for pattern, description in patterns.items():
            keys = redis_client.keys(pattern)
            stats[description] = len(keys)
        
        return {
            "cache_stats": stats,
            "ttl_seconds": settings.CACHE_TTL,
            "ttl_minutes": settings.CACHE_TTL // 60,
            "redis_version": redis_client.info().get("redis_version", "unknown")
        }
        
    except Exception as e:
        logger.error(f"Erro ao obter estatísticas do cache: {e}")
        raise HTTPException(status_code=500, detail=f"Erro ao obter estatísticas: {str(e)}")