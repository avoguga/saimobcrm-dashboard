"""
Modelos MongoDB para dados do Kommo (Leads e Tasks)
Otimizado para consultas rapidas no Dashboard de Vendas
"""

from typing import Optional, List, Dict, Any
from datetime import datetime
from pydantic import BaseModel, Field
from bson import ObjectId
from pymongo import IndexModel, ASCENDING, DESCENDING
import motor.motor_asyncio
from config import MONGODB_URL, MONGODB_DATABASE
import logging

logger = logging.getLogger(__name__)

# Cliente MongoDB (reutiliza conexao do facebook_models se existir)
mongodb_client = motor.motor_asyncio.AsyncIOMotorClient(
    MONGODB_URL,
    serverSelectionTimeoutMS=60000,
    socketTimeoutMS=120000,
    connectTimeoutMS=30000,
    maxPoolSize=50,
    minPoolSize=10,
    maxIdleTimeMS=300000,
    waitQueueTimeoutMS=60000
)
db = mongodb_client[MONGODB_DATABASE]

# =============================================================================
# CONSTANTES DO KOMMO
# =============================================================================

# Pipelines
PIPELINE_VENDAS = 10516987
PIPELINE_REMARKETING = 11059911

# Custom Fields IDs
CUSTOM_FIELD_CORRETOR = 837920
CUSTOM_FIELD_FONTE = 837886
CUSTOM_FIELD_PRODUTO = 857264
CUSTOM_FIELD_ANUNCIO = 837846
CUSTOM_FIELD_PUBLICO = 837844
CUSTOM_FIELD_PROPOSTA = 861100
CUSTOM_FIELD_DATA_FECHAMENTO = 858126
CUSTOM_FIELD_DATA_PROPOSTA = 882618

# Mapeamento de custom fields para nomes
CUSTOM_FIELD_NAMES = {
    CUSTOM_FIELD_CORRETOR: "corretor",
    CUSTOM_FIELD_FONTE: "fonte",
    CUSTOM_FIELD_PRODUTO: "produto",
    CUSTOM_FIELD_ANUNCIO: "anuncio",
    CUSTOM_FIELD_PUBLICO: "publico",
    CUSTOM_FIELD_PROPOSTA: "proposta",
    CUSTOM_FIELD_DATA_FECHAMENTO: "data_fechamento",
    CUSTOM_FIELD_DATA_PROPOSTA: "data_proposta",
}

# =============================================================================
# MODELOS PYDANTIC
# =============================================================================

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


class KommoLeadCustomFields(BaseModel):
    """Campos customizados extraidos e processados do lead"""
    corretor: Optional[str] = None
    fonte: Optional[str] = None
    produto: Optional[str] = None
    anuncio: Optional[str] = None
    publico: Optional[str] = None
    proposta: Optional[float] = None
    data_fechamento: Optional[datetime] = None
    data_proposta: Optional[datetime] = None

    class Config:
        arbitrary_types_allowed = True


class KommoLead(BaseModel):
    """
    Modelo MongoDB para um Lead do Kommo.
    Campos customizados sao extraidos e indexados para consultas rapidas.
    """
    id: Optional[PyObjectId] = Field(default_factory=PyObjectId, alias="_id")

    # Identificadores
    lead_id: int = Field(..., description="ID do lead no Kommo")

    # Dados basicos
    name: str = Field(default="", description="Nome do lead")
    price: float = Field(default=0.0, description="Valor do lead")

    # Pipeline e Status
    pipeline_id: int = Field(..., description="ID do pipeline")
    status_id: int = Field(..., description="ID do status/etapa")

    # Responsavel
    responsible_user_id: Optional[int] = Field(default=None, description="ID do usuario responsavel")

    # Timestamps (Unix)
    created_at: int = Field(..., description="Data de criacao (Unix timestamp)")
    updated_at: int = Field(..., description="Data de atualizacao (Unix timestamp)")
    closed_at: Optional[int] = Field(default=None, description="Data de fechamento (Unix timestamp)")

    # Custom fields processados (indexados para queries rapidas)
    custom_fields: KommoLeadCustomFields = Field(default_factory=KommoLeadCustomFields)

    # Dados brutos do Kommo (para referencia)
    raw_custom_fields: Optional[List[Dict[str, Any]]] = Field(default=None, description="Custom fields originais")
    tags: Optional[List[Dict[str, Any]]] = Field(default=None, description="Tags do lead")
    contacts: Optional[List[Dict[str, Any]]] = Field(default=None, description="Contatos associados")

    # Metadados de sincronizacao
    synced_at: datetime = Field(default_factory=datetime.utcnow, description="Ultima sincronizacao")
    source: str = Field(default="api", description="Origem: api, webhook, sync")

    # Flag para soft delete
    is_deleted: bool = Field(default=False, description="Se o lead foi deletado no Kommo")

    class Config:
        allow_population_by_field_name = True
        arbitrary_types_allowed = True
        json_encoders = {ObjectId: str, datetime: lambda v: v.isoformat()}


class KommoTask(BaseModel):
    """
    Modelo MongoDB para uma Task/Reuniao do Kommo.
    """
    id: Optional[PyObjectId] = Field(default_factory=PyObjectId, alias="_id")

    # Identificadores
    task_id: int = Field(..., description="ID da task no Kommo")

    # Dados basicos
    text: str = Field(default="", description="Texto/descricao da task")
    task_type_id: int = Field(..., description="Tipo da task (2 = reuniao)")

    # Relacionamentos
    entity_id: Optional[int] = Field(default=None, description="ID do lead associado")
    entity_type: str = Field(default="leads", description="Tipo da entidade")
    responsible_user_id: Optional[int] = Field(default=None, description="ID do responsavel")

    # Status
    is_completed: bool = Field(default=False, description="Se a task foi concluida")
    result: Optional[Dict[str, Any]] = Field(default=None, description="Resultado da task")

    # Timestamps
    complete_till: Optional[int] = Field(default=None, description="Data limite (Unix)")
    created_at: int = Field(..., description="Data de criacao (Unix)")
    updated_at: int = Field(..., description="Data de atualizacao (Unix)")

    # Metadados
    synced_at: datetime = Field(default_factory=datetime.utcnow)
    source: str = Field(default="api")
    is_deleted: bool = Field(default=False)

    class Config:
        allow_population_by_field_name = True
        arbitrary_types_allowed = True
        json_encoders = {ObjectId: str, datetime: lambda v: v.isoformat()}


class WebhookEvent(BaseModel):
    """
    Modelo para registrar eventos de webhook recebidos.
    Util para debug e auditoria.
    """
    id: Optional[PyObjectId] = Field(default_factory=PyObjectId, alias="_id")

    event_type: str = Field(..., description="Tipo: lead_add, lead_update, lead_delete, etc")
    entity_type: str = Field(default="leads", description="Tipo da entidade")
    entity_id: Optional[int] = Field(default=None, description="ID da entidade")

    # Payload recebido
    payload: Dict[str, Any] = Field(default_factory=dict)

    # Processamento
    processed: bool = Field(default=False)
    processed_at: Optional[datetime] = Field(default=None)
    error: Optional[str] = Field(default=None)

    # Timestamp
    received_at: datetime = Field(default_factory=datetime.utcnow)

    class Config:
        allow_population_by_field_name = True
        arbitrary_types_allowed = True
        json_encoders = {ObjectId: str, datetime: lambda v: v.isoformat()}


class SyncStatus(BaseModel):
    """
    Modelo para controlar status de sincronizacao.
    """
    id: Optional[PyObjectId] = Field(default_factory=PyObjectId, alias="_id")

    sync_type: str = Field(..., description="Tipo: full, incremental")
    status: str = Field(default="pending", description="Status: pending, running, completed, failed")

    # Estatisticas
    total_leads: int = Field(default=0)
    processed_leads: int = Field(default=0)
    total_tasks: int = Field(default=0)
    processed_tasks: int = Field(default=0)
    errors: int = Field(default=0)

    # Periodo sincronizado
    start_date: Optional[datetime] = Field(default=None)
    end_date: Optional[datetime] = Field(default=None)

    # Timestamps
    started_at: Optional[datetime] = Field(default=None)
    completed_at: Optional[datetime] = Field(default=None)
    created_at: datetime = Field(default_factory=datetime.utcnow)

    # Erro
    error_message: Optional[str] = Field(default=None)

    class Config:
        allow_population_by_field_name = True
        arbitrary_types_allowed = True
        json_encoders = {ObjectId: str, datetime: lambda v: v.isoformat()}


# =============================================================================
# COLLECTIONS
# =============================================================================

leads_collection = db.kommo_leads
tasks_collection = db.kommo_tasks
webhook_events_collection = db.kommo_webhook_events
sync_status_collection = db.kommo_sync_status


# =============================================================================
# FUNCOES AUXILIARES
# =============================================================================

def extract_custom_field_value(custom_fields: List[Dict], field_id: int) -> Optional[Any]:
    """Extrai valor de um custom field pelo ID"""
    if not custom_fields:
        return None

    for field in custom_fields:
        if field and field.get("field_id") == field_id:
            values = field.get("values", [])
            if values and len(values) > 0:
                return values[0].get("value")
    return None


def parse_kommo_date(value: Any) -> Optional[datetime]:
    """Converte valor de data do Kommo para datetime"""
    if not value:
        return None

    try:
        # Se for timestamp Unix
        if isinstance(value, (int, float)):
            return datetime.fromtimestamp(value)

        # Se for string de data
        if isinstance(value, str):
            # Tenta varios formatos
            for fmt in ["%Y-%m-%d", "%d.%m.%Y", "%d/%m/%Y"]:
                try:
                    return datetime.strptime(value, fmt)
                except ValueError:
                    continue

        return None
    except Exception:
        return None


def process_lead_custom_fields(raw_custom_fields: List[Dict]) -> KommoLeadCustomFields:
    """Processa custom fields brutos e retorna objeto estruturado"""
    if not raw_custom_fields:
        return KommoLeadCustomFields()

    return KommoLeadCustomFields(
        corretor=extract_custom_field_value(raw_custom_fields, CUSTOM_FIELD_CORRETOR),
        fonte=extract_custom_field_value(raw_custom_fields, CUSTOM_FIELD_FONTE),
        produto=extract_custom_field_value(raw_custom_fields, CUSTOM_FIELD_PRODUTO),
        anuncio=extract_custom_field_value(raw_custom_fields, CUSTOM_FIELD_ANUNCIO),
        publico=extract_custom_field_value(raw_custom_fields, CUSTOM_FIELD_PUBLICO),
        proposta=extract_custom_field_value(raw_custom_fields, CUSTOM_FIELD_PROPOSTA),
        data_fechamento=parse_kommo_date(extract_custom_field_value(raw_custom_fields, CUSTOM_FIELD_DATA_FECHAMENTO)),
        data_proposta=parse_kommo_date(extract_custom_field_value(raw_custom_fields, CUSTOM_FIELD_DATA_PROPOSTA)),
    )


def kommo_lead_to_model(lead_data: Dict, source: str = "api") -> Dict:
    """
    Converte dados brutos do lead da API Kommo para o formato do modelo MongoDB.
    Retorna dict pronto para upsert.
    """
    raw_custom_fields = lead_data.get("custom_fields_values", [])
    custom_fields = process_lead_custom_fields(raw_custom_fields)

    return {
        "lead_id": lead_data["id"],
        "name": lead_data.get("name", ""),
        "price": float(lead_data.get("price", 0) or 0),
        "pipeline_id": lead_data.get("pipeline_id", 0),
        "status_id": lead_data.get("status_id", 0),
        "responsible_user_id": lead_data.get("responsible_user_id"),
        "created_at": lead_data.get("created_at", 0),
        "updated_at": lead_data.get("updated_at", 0),
        "closed_at": lead_data.get("closed_at"),
        "custom_fields": custom_fields.dict(),
        "raw_custom_fields": raw_custom_fields,
        "tags": lead_data.get("_embedded", {}).get("tags", []),
        "contacts": lead_data.get("_embedded", {}).get("contacts", []),
        "synced_at": datetime.utcnow(),
        "source": source,
        "is_deleted": False
    }


def kommo_task_to_model(task_data: Dict, source: str = "api") -> Dict:
    """
    Converte dados brutos da task da API Kommo para o formato do modelo MongoDB.
    """
    return {
        "task_id": task_data["id"],
        "text": task_data.get("text", ""),
        "task_type_id": task_data.get("task_type_id", 0),
        "entity_id": task_data.get("entity_id"),
        "entity_type": task_data.get("entity_type", "leads"),
        "responsible_user_id": task_data.get("responsible_user_id"),
        "is_completed": task_data.get("is_completed", False),
        "result": task_data.get("result"),
        "complete_till": task_data.get("complete_till"),
        "created_at": task_data.get("created_at", 0),
        "updated_at": task_data.get("updated_at", 0),
        "synced_at": datetime.utcnow(),
        "source": source,
        "is_deleted": False
    }


# =============================================================================
# INDICES
# =============================================================================

async def create_kommo_indexes():
    """Cria indices otimizados para consultas do dashboard"""

    logger.info("Criando indices MongoDB para Kommo...")

    # Indices para leads
    lead_indexes = [
        # Indice unico pelo ID do Kommo
        IndexModel([("lead_id", ASCENDING)], unique=True, name="lead_id_unique"),

        # Indice principal para consultas do dashboard
        IndexModel(
            [("pipeline_id", ASCENDING), ("status_id", ASCENDING), ("created_at", DESCENDING)],
            name="pipeline_status_created"
        ),

        # Indice para filtro por corretor (mais usado)
        IndexModel(
            [("custom_fields.corretor", ASCENDING), ("pipeline_id", ASCENDING), ("created_at", DESCENDING)],
            name="corretor_pipeline_created"
        ),

        # Indice para filtro por fonte
        IndexModel(
            [("custom_fields.fonte", ASCENDING), ("pipeline_id", ASCENDING), ("created_at", DESCENDING)],
            name="fonte_pipeline_created"
        ),

        # Indice para filtro por produto
        IndexModel(
            [("custom_fields.produto", ASCENDING), ("pipeline_id", ASCENDING), ("created_at", DESCENDING)],
            name="produto_pipeline_created"
        ),

        # Indice para busca por updated_at (sync incremental)
        IndexModel([("updated_at", DESCENDING)], name="updated_at_desc"),

        # Indice para busca por synced_at
        IndexModel([("synced_at", DESCENDING)], name="synced_at_desc"),

        # Indice para leads nao deletados
        IndexModel([("is_deleted", ASCENDING)], name="is_deleted"),
    ]

    try:
        await leads_collection.create_indexes(lead_indexes)
        logger.info(f"Indices de leads criados: {len(lead_indexes)}")
    except Exception as e:
        logger.error(f"Erro ao criar indices de leads: {e}")

    # Indices para tasks
    task_indexes = [
        # Indice unico pelo ID do Kommo
        IndexModel([("task_id", ASCENDING)], unique=True, name="task_id_unique"),

        # Indice para reunioes (task_type_id = 2)
        IndexModel(
            [("task_type_id", ASCENDING), ("is_completed", ASCENDING), ("complete_till", DESCENDING)],
            name="task_type_completed_till"
        ),

        # Indice para buscar tasks por lead
        IndexModel([("entity_id", ASCENDING), ("entity_type", ASCENDING)], name="entity_id_type"),

        # Indice para responsavel
        IndexModel([("responsible_user_id", ASCENDING)], name="responsible_user"),

        # Indice para sync incremental
        IndexModel([("updated_at", DESCENDING)], name="task_updated_at_desc"),
    ]

    try:
        await tasks_collection.create_indexes(task_indexes)
        logger.info(f"Indices de tasks criados: {len(task_indexes)}")
    except Exception as e:
        logger.error(f"Erro ao criar indices de tasks: {e}")

    # Indices para webhook events
    webhook_indexes = [
        IndexModel([("received_at", DESCENDING)], name="webhook_received_at"),
        IndexModel([("event_type", ASCENDING), ("processed", ASCENDING)], name="event_type_processed"),
        IndexModel([("entity_id", ASCENDING)], name="webhook_entity_id"),
    ]

    try:
        await webhook_events_collection.create_indexes(webhook_indexes)
        logger.info(f"Indices de webhook events criados: {len(webhook_indexes)}")
    except Exception as e:
        logger.error(f"Erro ao criar indices de webhook events: {e}")

    # Indices para sync status
    sync_indexes = [
        IndexModel([("sync_type", ASCENDING), ("created_at", DESCENDING)], name="sync_type_created"),
        IndexModel([("status", ASCENDING)], name="sync_status"),
    ]

    try:
        await sync_status_collection.create_indexes(sync_indexes)
        logger.info(f"Indices de sync status criados: {len(sync_indexes)}")
    except Exception as e:
        logger.error(f"Erro ao criar indices de sync status: {e}")

    logger.info("Indices MongoDB para Kommo criados com sucesso!")


async def connect_kommo_mongodb():
    """Conecta ao MongoDB e cria indices para Kommo"""
    try:
        # Testar conexao
        await db.command("ping")
        logger.info(f"Conectado ao MongoDB: {MONGODB_DATABASE}")

        # Criar indices
        await create_kommo_indexes()

        return True
    except Exception as e:
        logger.error(f"Erro ao conectar ao MongoDB (Kommo): {e}")
        return False


async def get_leads_stats():
    """Retorna estatisticas dos leads no MongoDB"""
    try:
        total = await leads_collection.count_documents({"is_deleted": False})
        vendas = await leads_collection.count_documents({"pipeline_id": PIPELINE_VENDAS, "is_deleted": False})
        remarketing = await leads_collection.count_documents({"pipeline_id": PIPELINE_REMARKETING, "is_deleted": False})

        # Ultimo sync
        last_sync = await sync_status_collection.find_one(
            {"status": "completed"},
            sort=[("completed_at", DESCENDING)]
        )

        return {
            "total_leads": total,
            "leads_vendas": vendas,
            "leads_remarketing": remarketing,
            "last_sync": last_sync.get("completed_at").isoformat() if last_sync and last_sync.get("completed_at") else None
        }
    except Exception as e:
        logger.error(f"Erro ao obter estatisticas: {e}")
        return {"error": str(e)}
