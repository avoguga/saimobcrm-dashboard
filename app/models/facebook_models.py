"""
Modelos MongoDB para dados do Facebook Ads
"""

from typing import Optional, List, Dict, Any
from datetime import datetime, date
from typing import TYPE_CHECKING
from pydantic import BaseModel, Field
from bson import ObjectId
from pymongo import IndexModel
import motor.motor_asyncio
from config import MONGODB_URL, MONGODB_DATABASE

# Cliente MongoDB com timeouts aumentados para sync longa
mongodb_client = motor.motor_asyncio.AsyncIOMotorClient(
    MONGODB_URL,
    serverSelectionTimeoutMS=60000,  # 60s (timeout para seleção de servidor)
    socketTimeoutMS=120000,           # 120s (timeout para operações de socket)
    connectTimeoutMS=30000,           # 30s (timeout para conexão inicial)
    maxPoolSize=50,                   # Pool maior para operações concorrentes
    minPoolSize=10,                   # Manter conexões mínimas abertas
    maxIdleTimeMS=300000,             # 5min (tempo máximo de conexões ociosas)
    waitQueueTimeoutMS=60000          # 60s (timeout na fila de espera)
)
db = mongodb_client[MONGODB_DATABASE]

class PyObjectId(ObjectId):
    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def validate(cls, v):
        if not ObjectId.is_valid(v):
            raise ValueError("Invalid ObjectId")
        return ObjectId(v)

    @classmethod
    def __get_pydantic_json_schema__(cls, field_schema):
        field_schema.update(type="string")
        return field_schema

class FacebookMetrics(BaseModel):
    """Métricas do Facebook para um período específico"""
    date: str = Field(..., description="Data das métricas YYYY-MM-DD")
    leads: int = Field(default=0, description="Total de leads")
    spend: float = Field(default=0.0, description="Valor gasto")
    impressions: int = Field(default=0, description="Impressões")
    clicks: int = Field(default=0, description="Cliques")
    reach: int = Field(default=0, description="Alcance")
    cpc: float = Field(default=0.0, description="Custo por clique")
    cpm: float = Field(default=0.0, description="Custo por mil impressões")
    ctr: float = Field(default=0.0, description="Taxa de cliques")
    cpp: float = Field(default=0.0, description="Custo por compra")
    video_views: int = Field(default=0, description="Visualizações de vídeo")
    actions_like: int = Field(default=0, description="Curtidas")
    link_clicks: int = Field(default=0, description="Cliques no link")

class FacebookAd(BaseModel):
    """Modelo para um anúncio do Facebook"""
    id: Optional[PyObjectId] = Field(default_factory=PyObjectId, alias="_id")
    ad_id: str = Field(..., description="ID do anúncio no Facebook")
    name: str = Field(..., description="Nome do anúncio")
    status: str = Field(..., description="Status do anúncio")
    adset_id: str = Field(..., description="ID do conjunto de anúncios")
    campaign_id: str = Field(..., description="ID da campanha")
    account_id: str = Field(..., description="ID da conta de anúncios")
    
    # Métricas históricas por data
    metrics: Dict[str, FacebookMetrics] = Field(default_factory=dict, description="Métricas por data (YYYY-MM-DD)")
    
    # Metadados
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    last_sync: Optional[datetime] = Field(default=None, description="Última sincronização com Facebook")

    class Config:
        allow_population_by_field_name = True
        arbitrary_types_allowed = True
        json_encoders = {ObjectId: str}

class FacebookAdSet(BaseModel):
    """Modelo para um conjunto de anúncios do Facebook"""
    id: Optional[PyObjectId] = Field(default_factory=PyObjectId, alias="_id")
    adset_id: str = Field(..., description="ID do conjunto de anúncios no Facebook")
    name: str = Field(..., description="Nome do conjunto de anúncios")
    status: str = Field(..., description="Status do conjunto de anúncios")
    campaign_id: str = Field(..., description="ID da campanha")
    account_id: str = Field(..., description="ID da conta de anúncios")
    
    # Configurações do AdSet
    daily_budget: Optional[float] = Field(default=None, description="Orçamento diário")
    lifetime_budget: Optional[float] = Field(default=None, description="Orçamento total")
    targeting: Optional[Dict[str, Any]] = Field(default=None, description="Configurações de segmentação")
    
    # Lista de anúncios deste AdSet
    ads: List[str] = Field(default_factory=list, description="Lista de IDs dos anúncios")
    
    # Métricas históricas por data
    metrics: Dict[str, FacebookMetrics] = Field(default_factory=dict, description="Métricas por data (YYYY-MM-DD)")
    
    # Metadados
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    last_sync: Optional[datetime] = Field(default=None, description="Última sincronização com Facebook")

    class Config:
        allow_population_by_field_name = True
        arbitrary_types_allowed = True
        json_encoders = {ObjectId: str}

class FacebookCampaign(BaseModel):
    """Modelo para uma campanha do Facebook"""
    id: Optional[PyObjectId] = Field(default_factory=PyObjectId, alias="_id")
    campaign_id: str = Field(..., description="ID da campanha no Facebook")
    name: str = Field(..., description="Nome da campanha")
    status: str = Field(..., description="Status da campanha")
    objective: str = Field(..., description="Objetivo da campanha")
    account_id: str = Field(..., description="ID da conta de anúncios")
    
    # Configurações da campanha
    buying_type: Optional[str] = Field(default=None, description="Tipo de compra")
    special_ad_categories: Optional[List[str]] = Field(default_factory=list, description="Categorias especiais")
    
    # Lista de conjuntos de anúncios desta campanha
    adsets: List[str] = Field(default_factory=list, description="Lista de IDs dos conjuntos de anúncios")
    
    # Métricas históricas por data  
    metrics: Dict[str, FacebookMetrics] = Field(default_factory=dict, description="Métricas por data (YYYY-MM-DD)")
    
    # Metadados
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    last_sync: Optional[datetime] = Field(default=None, description="Última sincronização com Facebook")

    class Config:
        allow_population_by_field_name = True
        arbitrary_types_allowed = True
        json_encoders = {ObjectId: str}

class SyncJob(BaseModel):
    """Modelo para controlar jobs de sincronização"""
    id: Optional[PyObjectId] = Field(default_factory=PyObjectId, alias="_id")
    job_type: str = Field(..., description="Tipo de job: campaigns, adsets, ads, metrics")
    account_id: str = Field(..., description="ID da conta de anúncios")
    status: str = Field(default="pending", description="Status: pending, running, completed, failed")
    start_date: Optional[str] = Field(default=None, description="Data de início para métricas YYYY-MM-DD")
    end_date: Optional[str] = Field(default=None, description="Data de fim para métricas YYYY-MM-DD")
    
    # Estatísticas do job
    total_items: int = Field(default=0, description="Total de itens a processar")
    processed_items: int = Field(default=0, description="Itens processados")
    failed_items: int = Field(default=0, description="Itens que falharam")
    error_message: Optional[str] = Field(default=None, description="Mensagem de erro")
    
    # Timestamps
    created_at: datetime = Field(default_factory=datetime.utcnow)
    started_at: Optional[datetime] = Field(default=None)
    completed_at: Optional[datetime] = Field(default=None)

    class Config:
        allow_population_by_field_name = True
        arbitrary_types_allowed = True
        json_encoders = {ObjectId: str}

# Collections
campaigns_collection = db.facebook_campaigns
adsets_collection = db.facebook_adsets  
ads_collection = db.facebook_ads
sync_jobs_collection = db.sync_jobs

async def create_indexes():
    """Cria índices para otimizar consultas"""
    
    # Índices para campanhas
    campaign_indexes = [
        IndexModel([("campaign_id", 1)], unique=True),
        IndexModel([("account_id", 1)]),
        IndexModel([("status", 1)]),
        IndexModel([("last_sync", 1)])
    ]
    await campaigns_collection.create_indexes(campaign_indexes)
    
    # Índices para adsets
    adset_indexes = [
        IndexModel([("adset_id", 1)], unique=True),
        IndexModel([("campaign_id", 1)]),
        IndexModel([("account_id", 1)]),
        IndexModel([("status", 1)]),
        IndexModel([("last_sync", 1)])
    ]
    await adsets_collection.create_indexes(adset_indexes)
    
    # Índices para ads
    ad_indexes = [
        IndexModel([("ad_id", 1)], unique=True),
        IndexModel([("adset_id", 1)]),
        IndexModel([("campaign_id", 1)]),
        IndexModel([("account_id", 1)]),
        IndexModel([("status", 1)]),
        IndexModel([("last_sync", 1)])
    ]
    await ads_collection.create_indexes(ad_indexes)
    
    # Índices para sync jobs
    sync_indexes = [
        IndexModel([("job_type", 1), ("account_id", 1)]),
        IndexModel([("status", 1)]),
        IndexModel([("created_at", -1)])
    ]
    await sync_jobs_collection.create_indexes(sync_indexes)

# Função para conectar ao MongoDB
async def connect_mongodb():
    """Conecta ao MongoDB e cria índices"""
    try:
        # Testar conexão
        await db.command("ping")
        print(f"OK: Conectado ao MongoDB: {MONGODB_DATABASE}")

        # Criar índices
        await create_indexes()
        print("OK: Indices MongoDB criados com sucesso")

        return True
    except Exception as e:
        print(f"ERRO ao conectar ao MongoDB: {e}")
        return False

# Função para fechar conexão
async def close_mongodb():
    """Fecha conexão com MongoDB"""
    mongodb_client.close()