"""
Dashboard Otimizado v2 - Consultas diretas ao MongoDB
Performance esperada: < 500ms vs 5-10s dos endpoints antigos
"""

from fastapi import APIRouter, HTTPException, Query
from typing import Optional, List, Dict, Any
import logging
from datetime import datetime, timedelta
import time

from app.models.kommo_models import (
    leads_collection,
    tasks_collection,
    PIPELINE_VENDAS,
    PIPELINE_REMARKETING,
    CUSTOM_FIELD_CORRETOR,
    CUSTOM_FIELD_FONTE,
    CUSTOM_FIELD_PRODUTO,
    CUSTOM_FIELD_ANUNCIO,
    CUSTOM_FIELD_PUBLICO,
    CUSTOM_FIELD_PROPOSTA,
    CUSTOM_FIELD_DATA_FECHAMENTO,
    CUSTOM_FIELD_DATA_PROPOSTA,
    get_leads_stats,
)

router = APIRouter()
logger = logging.getLogger(__name__)

# Status IDs importantes
STATUS_VENDA_FINAL = 142  # Closed - won
STATUS_CONTRATO_ASSINADO = 80689759  # Contrato Assinado
STATUS_PERDIDO = 143  # Closed - lost

# Mapeamento de status_id para nomes de etapas (compativel com V1)
STATUS_MAP = {
    # Pipeline Vendas
    80689711: "Qualificação",
    80689715: "Reunião Marcada",
    80689719: "Reunião Realizada",
    80689723: "Proposta",
    80689727: "Contrato Enviado",
    80689759: "Contrato Assinado",
    142: "Venda ganha",
    143: "Perdido",
    # Pipeline Remarketing
    83103919: "Novo Lead",
    83103923: "Qualificação",
    83103927: "Reunião Marcada",
    83103931: "Reunião Realizada",
    83103935: "Proposta",
    83103939: "Contrato Enviado",
}

def get_etapa_name(status_id: int) -> str:
    """Retorna o nome da etapa baseado no status_id"""
    return STATUS_MAP.get(status_id, f"Status {status_id}")


def build_leads_query(
    pipeline_ids: List[int] = None,
    start_timestamp: int = None,
    end_timestamp: int = None,
    corretor: str = None,
    fonte: str = None,
    produto: str = None,
    status_ids: List[int] = None,
    exclude_deleted: bool = True
) -> Dict:
    """
    Constroi query MongoDB para leads com filtros.
    """
    query = {}

    # Excluir deletados
    if exclude_deleted:
        query["is_deleted"] = False

    # Filtro por pipeline
    if pipeline_ids:
        if len(pipeline_ids) == 1:
            query["pipeline_id"] = pipeline_ids[0]
        else:
            query["pipeline_id"] = {"$in": pipeline_ids}

    # Filtro por periodo (created_at)
    if start_timestamp or end_timestamp:
        query["created_at"] = {}
        if start_timestamp:
            query["created_at"]["$gte"] = start_timestamp
        if end_timestamp:
            query["created_at"]["$lte"] = end_timestamp

    # Filtro por corretor (suporta multiplos separados por virgula)
    if corretor and corretor.strip():
        if "," in corretor:
            corretores = [c.strip() for c in corretor.split(",")]
            query["custom_fields.corretor"] = {"$in": corretores}
        else:
            query["custom_fields.corretor"] = corretor.strip()

    # Filtro por fonte (suporta multiplos separados por virgula)
    if fonte and fonte.strip():
        if "," in fonte:
            fontes = [f.strip() for f in fonte.split(",")]
            query["custom_fields.fonte"] = {"$in": fontes}
        else:
            query["custom_fields.fonte"] = fonte.strip()

    # Filtro por produto
    if produto and produto.strip():
        query["custom_fields.produto"] = produto.strip()

    # Filtro por status
    if status_ids:
        if len(status_ids) == 1:
            query["status_id"] = status_ids[0]
        else:
            query["status_id"] = {"$in": status_ids}

    return query


@router.get("/sales-complete")
async def get_sales_complete_v2(
    days: int = Query(90, description="Periodo em dias para analise"),
    corretor: Optional[str] = Query(None, description="Nome do corretor para filtrar dados"),
    fonte: Optional[str] = Query(None, description="Fonte para filtrar dados"),
    produto: Optional[str] = Query(None, description="Produto para filtrar dados"),
    start_date: Optional[str] = Query(None, description="Data de inicio (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="Data de fim (YYYY-MM-DD)"),
):
    """
    Dashboard de vendas OTIMIZADO - consulta MongoDB diretamente.

    Retorna os mesmos dados que /dashboard/sales-complete, mas muito mais rapido.
    """
    start_time = time.time()
    logger.info(f"[V2] Iniciando sales-complete: days={days}, corretor={corretor}, fonte={fonte}")

    try:
        # Calcular periodo
        if start_date and end_date:
            try:
                start_dt = datetime.strptime(start_date, '%Y-%m-%d')
                end_dt = datetime.strptime(end_date, '%Y-%m-%d').replace(hour=23, minute=59, second=59)
                start_timestamp = int(start_dt.timestamp())
                end_timestamp = int(end_dt.timestamp())
            except ValueError:
                raise HTTPException(status_code=400, detail="Formato de data invalido. Use YYYY-MM-DD")
        else:
            end_timestamp = int(time.time())
            start_timestamp = end_timestamp - (days * 24 * 60 * 60)

        # Query base para leads
        base_query = build_leads_query(
            pipeline_ids=[PIPELINE_VENDAS, PIPELINE_REMARKETING],
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp,
            corretor=corretor,
            fonte=fonte,
            produto=produto
        )

        # ===== AGREGACOES MONGODB =====

        # 1. Contagem total de leads
        total_leads = await leads_collection.count_documents(base_query)

        # 2. Leads por status (ativos, ganhos, perdidos)
        pipeline_status = [
            {"$match": base_query},
            {"$group": {
                "_id": "$status_id",
                "count": {"$sum": 1},
                "total_price": {"$sum": "$price"}
            }}
        ]
        status_counts = {}
        async for doc in leads_collection.aggregate(pipeline_status):
            status_counts[doc["_id"]] = {
                "count": doc["count"],
                "total_price": doc.get("total_price", 0)
            }

        # Calcular metricas
        won_count = status_counts.get(STATUS_VENDA_FINAL, {}).get("count", 0)
        won_count += status_counts.get(STATUS_CONTRATO_ASSINADO, {}).get("count", 0)
        lost_count = status_counts.get(STATUS_PERDIDO, {}).get("count", 0)
        active_count = total_leads - won_count - lost_count

        total_revenue = status_counts.get(STATUS_VENDA_FINAL, {}).get("total_price", 0)
        total_revenue += status_counts.get(STATUS_CONTRATO_ASSINADO, {}).get("total_price", 0)

        # 3. Leads por corretor
        pipeline_corretor = [
            {"$match": base_query},
            {"$group": {
                "_id": "$custom_fields.corretor",
                "total": {"$sum": 1},
                "won": {"$sum": {"$cond": [{"$in": ["$status_id", [STATUS_VENDA_FINAL, STATUS_CONTRATO_ASSINADO]]}, 1, 0]}},
                "lost": {"$sum": {"$cond": [{"$eq": ["$status_id", STATUS_PERDIDO]}, 1, 0]}},
            }}
        ]

        leads_by_user = []
        async for doc in leads_collection.aggregate(pipeline_corretor):
            corretor_name = doc["_id"] or "Sem corretor"
            total = doc["total"]
            won = doc["won"]
            lost = doc["lost"]
            active = total - won - lost

            leads_by_user.append({
                "name": corretor_name,
                "value": total,
                "active": active,
                "lost": lost,
                "sales": won,
                "meetings": 0,  # Sera preenchido depois
                "meetingsHeld": 0
            })

        # 4. Reunioes por corretor (buscar do MongoDB de tasks)
        # Primeiro, buscar lead_ids dos leads filtrados
        lead_ids_cursor = leads_collection.find(base_query, {"lead_id": 1})
        lead_ids = set()
        async for doc in lead_ids_cursor:
            lead_ids.add(doc["lead_id"])

        # Buscar reunioes completadas desses leads
        meetings_query = {
            "task_type_id": 2,  # Reuniao
            "is_completed": True,
            "entity_id": {"$in": list(lead_ids)},
            "is_deleted": False
        }

        if start_timestamp and end_timestamp:
            meetings_query["complete_till"] = {
                "$gte": start_timestamp,
                "$lte": end_timestamp
            }

        # Agregacao de reunioes por lead -> corretor
        # Precisamos fazer join com leads para pegar o corretor
        pipeline_meetings = [
            {"$match": meetings_query},
            {"$lookup": {
                "from": "kommo_leads",
                "localField": "entity_id",
                "foreignField": "lead_id",
                "as": "lead"
            }},
            {"$unwind": "$lead"},
            {"$group": {
                "_id": "$lead.custom_fields.corretor",
                "count": {"$sum": 1}
            }}
        ]

        meetings_by_corretor = {}
        async for doc in tasks_collection.aggregate(pipeline_meetings):
            corretor_name = doc["_id"] or "Sem corretor"
            meetings_by_corretor[corretor_name] = doc["count"]

        # Atualizar reunioes no leads_by_user
        total_meetings = 0
        for user in leads_by_user:
            meetings = meetings_by_corretor.get(user["name"], 0)
            user["meetings"] = meetings
            user["meetingsHeld"] = meetings
            total_meetings += meetings

        # 5. Leads por estagio (status)
        pipeline_stage = [
            {"$match": base_query},
            {"$group": {
                "_id": "$status_id",
                "count": {"$sum": 1}
            }},
            {"$sort": {"count": -1}}
        ]

        leads_by_stage = []
        async for doc in leads_collection.aggregate(pipeline_stage):
            status_id = doc["_id"]
            leads_by_stage.append({
                "name": get_etapa_name(status_id),
                "value": doc["count"]
            })

        # 6. Leads por fonte
        pipeline_fonte = [
            {"$match": base_query},
            {"$group": {
                "_id": "$custom_fields.fonte",
                "count": {"$sum": 1}
            }},
            {"$sort": {"count": -1}}
        ]

        leads_by_source = []
        async for doc in leads_collection.aggregate(pipeline_fonte):
            fonte_name = doc["_id"] or "Fonte Desconhecida"
            leads_by_source.append({
                "name": fonte_name,
                "value": doc["count"]
            })

        # 7. Calcular metricas de conversao
        total_closed = won_count + lost_count
        win_rate = (won_count / total_closed * 100) if total_closed > 0 else 0
        conversion_rate_sales = (won_count / total_leads * 100) if total_leads > 0 else 0
        conversion_rate_meetings = (total_meetings / total_leads * 100) if total_leads > 0 else 0
        conversion_rate_prospects = (active_count / total_leads * 100) if total_leads > 0 else 0
        average_deal_size = (total_revenue / won_count) if won_count > 0 else 0

        # 8. Tempo medio de ciclo (leads ganhos)
        cycle_pipeline = [
            {"$match": {
                **base_query,
                "status_id": {"$in": [STATUS_VENDA_FINAL, STATUS_CONTRATO_ASSINADO]},
                "closed_at": {"$exists": True, "$ne": None}
            }},
            {"$project": {
                "cycle_days": {
                    "$divide": [
                        {"$subtract": ["$closed_at", "$created_at"]},
                        86400  # segundos por dia
                    ]
                }
            }},
            {"$group": {
                "_id": None,
                "avg_cycle": {"$avg": "$cycle_days"}
            }}
        ]

        lead_cycle_time = 0
        async for doc in leads_collection.aggregate(cycle_pipeline):
            lead_cycle_time = doc.get("avg_cycle", 0) or 0

        # Tempo de execucao
        elapsed = time.time() - start_time
        logger.info(f"[V2] sales-complete concluido em {elapsed:.3f}s - {total_leads} leads")

        return {
            "totalLeads": total_leads,
            "leadsByUser": sorted(leads_by_user, key=lambda x: x["value"], reverse=True),
            "leadsByStage": leads_by_stage,
            "leadsBySource": leads_by_source,
            "conversionRates": {
                "meetings": round(conversion_rate_meetings, 1),
                "prospects": round(conversion_rate_prospects, 1),
                "sales": round(conversion_rate_sales, 1)
            },
            "leadCycleTime": round(lead_cycle_time, 1),
            "winRate": round(win_rate, 1),
            "averageDealSize": round(average_deal_size, 2),
            "salesbotRecovery": 0,
            "salesTrend": [],
            "analyticsOverview": {
                "leads": {
                    "total": total_leads,
                    "active": active_count,
                    "lost": lost_count,
                    "won": won_count
                }
            },
            "_metadata": {
                "version": "v2_mongodb",
                "period_days": days,
                "corretor_filter": corretor,
                "fonte_filter": fonte,
                "generated_at": datetime.now().isoformat(),
                "elapsed_ms": round(elapsed * 1000, 2),
                "source": "mongodb"
            }
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[V2] Erro em sales-complete: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/detailed-tables")
async def get_detailed_tables_v2(
    corretor: Optional[str] = Query(None, description="Nome do corretor"),
    fonte: Optional[str] = Query(None, description="Fonte"),
    start_date: Optional[str] = Query(None, description="Data inicio (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="Data fim (YYYY-MM-DD)"),
    days: int = Query(30, description="Periodo em dias"),
    limit: int = Query(500, description="Limite de registros"),
):
    """
    Tabelas detalhadas OTIMIZADAS - consulta MongoDB diretamente.

    Retorna:
    - leadsDetalhes: todos os leads (nao organicos)
    - organicosDetalhes: leads organicos
    - reunioesDetalhes: reunioes realizadas
    - vendasDetalhes: vendas fechadas
    - propostasDetalhes: propostas enviadas
    """
    start_time = time.time()
    logger.info(f"[V2] Iniciando detailed-tables: corretor={corretor}, fonte={fonte}")

    try:
        # Calcular periodo
        if start_date and end_date:
            try:
                start_dt = datetime.strptime(start_date, '%Y-%m-%d')
                end_dt = datetime.strptime(end_date, '%Y-%m-%d').replace(hour=23, minute=59, second=59)
                start_timestamp = int(start_dt.timestamp())
                end_timestamp = int(end_dt.timestamp())
            except ValueError:
                raise HTTPException(status_code=400, detail="Formato de data invalido")
        else:
            end_timestamp = int(time.time())
            start_timestamp = end_timestamp - (days * 24 * 60 * 60)

        # Funcao auxiliar para formatar lead (compativel com V1)
        def format_lead_detail(lead: Dict, tipo: str = "lead") -> Dict:
            cf = lead.get("custom_fields", {})

            # Formatar data de criacao
            created_at = lead.get("created_at", 0)
            data_criacao = datetime.fromtimestamp(created_at).strftime("%d/%m/%Y") if created_at else "N/A"

            # Determinar funil
            pipeline_id = lead.get("pipeline_id")
            if pipeline_id == PIPELINE_VENDAS:
                funil = "Funil de Vendas"
            elif pipeline_id == PIPELINE_REMARKETING:
                funil = "Remarketing"
            else:
                funil = "Não atribuído"

            # Corretor
            corretor = cf.get("corretor") or "Não atribuído"

            # Data da proposta
            data_proposta = "N/A"
            if cf.get("data_proposta"):
                try:
                    dt = cf["data_proposta"]
                    if isinstance(dt, datetime):
                        data_proposta = dt.strftime("%d/%m/%Y")
                except:
                    pass

            # Determinar status baseado no status_id
            status_id = lead.get("status_id")
            if status_id == STATUS_VENDA_FINAL or status_id == STATUS_CONTRATO_ASSINADO:
                status = "Ganho"
            elif status_id == STATUS_PERDIDO:
                status = "Perdido"
            else:
                status = "Ativo"

            # Obter nome real da etapa
            etapa = get_etapa_name(status_id)

            # Base comum para todos os tipos
            detail = {
                "id": lead.get("lead_id"),
                "Data de Criação": data_criacao,
                "Nome do Lead": lead.get("name", ""),
                "Corretor": corretor,
                "Fonte": cf.get("fonte") or "N/A",
                "Anúncio": cf.get("anuncio") or "N/A",
                "Público": cf.get("publico") or "N/A",
                "Produto": cf.get("produto") or "N/A",
                "Data da Proposta": data_proposta,
                "Funil": funil,
                "Etapa": etapa,
                "Status": status,
            }

            return detail

        def format_venda_detail(lead: Dict) -> Dict:
            """Formata lead de venda com campos especificos"""
            detail = format_lead_detail(lead, tipo="venda")

            # Adicionar campos especificos de venda
            price = lead.get("price", 0) or 0
            valor_formatado = f"R$ {price:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')
            detail["Valor da Venda"] = valor_formatado

            # Data da venda (closed_at ou data_fechamento)
            cf = lead.get("custom_fields", {})
            closed_at = lead.get("closed_at")
            data_fechamento = cf.get("data_fechamento")

            if data_fechamento and isinstance(data_fechamento, datetime):
                detail["Data da Venda"] = data_fechamento.strftime("%d/%m/%Y")
            elif closed_at:
                detail["Data da Venda"] = datetime.fromtimestamp(closed_at).strftime("%d/%m/%Y")
            else:
                detail["Data da Venda"] = detail["Data de Criação"]

            return detail

        # Fontes organicas
        FONTES_ORGANICAS = [
            "Orgânico", "Site", "Redes Sociais", "Canal Pro",
            "Escritório Patacho", "Cliente", "Grupo Zap", "Celular do Plantão"
        ]

        # Query base
        base_query = build_leads_query(
            pipeline_ids=[PIPELINE_VENDAS, PIPELINE_REMARKETING],
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp,
            corretor=corretor,
            fonte=fonte
        )

        # ===== 1. LEADS DETALHES (nao organicos) =====
        leads_query = {**base_query, "custom_fields.fonte": {"$nin": FONTES_ORGANICAS}}
        leads_cursor = leads_collection.find(leads_query).sort("created_at", -1).limit(limit)

        leads_detalhes = []
        async for lead in leads_cursor:
            leads_detalhes.append(format_lead_detail(lead, tipo="lead"))

        # ===== 2. ORGANICOS DETALHES =====
        organicos_query = {**base_query, "custom_fields.fonte": {"$in": FONTES_ORGANICAS}}
        organicos_cursor = leads_collection.find(organicos_query).sort("created_at", -1).limit(limit)

        organicos_detalhes = []
        async for lead in organicos_cursor:
            organicos_detalhes.append(format_lead_detail(lead, tipo="lead"))

        # ===== 3. VENDAS DETALHES =====
        vendas_query = {
            **base_query,
            "status_id": {"$in": [STATUS_VENDA_FINAL, STATUS_CONTRATO_ASSINADO]}
        }
        # Para vendas, usamos closed_at ou data_fechamento (remover filtro por created_at se existir)
        vendas_query.pop("created_at", None)  # Remover filtro por created_at de forma segura

        vendas_cursor = leads_collection.find(vendas_query).sort("closed_at", -1).limit(limit)

        vendas_detalhes = []
        total_vendas_valor = 0
        async for lead in vendas_cursor:
            # Verificar se a venda esta no periodo (por closed_at ou data_fechamento)
            closed_at = lead.get("closed_at")
            cf = lead.get("custom_fields", {})
            data_fechamento = cf.get("data_fechamento")

            # Determinar data da venda
            if data_fechamento and isinstance(data_fechamento, datetime):
                venda_timestamp = int(data_fechamento.timestamp())
            elif closed_at:
                venda_timestamp = closed_at
            else:
                venda_timestamp = lead.get("created_at", 0)

            # Filtrar por periodo
            if venda_timestamp < start_timestamp or venda_timestamp > end_timestamp:
                continue

            detail = format_venda_detail(lead)
            vendas_detalhes.append(detail)
            total_vendas_valor += lead.get("price", 0) or 0

        # ===== 4. PROPOSTAS DETALHES =====
        # Propostas sao leads com custom_fields.proposta = true
        propostas_query = {
            **base_query,
            "raw_custom_fields": {
                "$elemMatch": {
                    "field_id": CUSTOM_FIELD_PROPOSTA,
                    "values.0.value": {"$in": [True, "true", "1", 1]}
                }
            }
        }

        propostas_cursor = leads_collection.find(propostas_query).sort("created_at", -1).limit(limit)

        propostas_detalhes = []
        async for lead in propostas_cursor:
            detail = format_lead_detail(lead, tipo="proposta")
            # Adicionar valor para propostas (campo esperado pelo frontend)
            price = lead.get("price", 0) or 0
            valor_formatado = f"R$ {price:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')
            detail["Valor da Proposta"] = valor_formatado  # Frontend espera este nome
            propostas_detalhes.append(detail)

        # ===== 5. REUNIOES DETALHES =====
        # Buscar lead_ids primeiro
        all_leads_query = build_leads_query(
            pipeline_ids=[PIPELINE_VENDAS, PIPELINE_REMARKETING],
            corretor=corretor,
            fonte=fonte
        )
        lead_ids_cursor = leads_collection.find(all_leads_query, {"lead_id": 1, "custom_fields": 1, "name": 1, "pipeline_id": 1, "status_id": 1})

        leads_map = {}
        async for doc in lead_ids_cursor:
            leads_map[doc["lead_id"]] = doc

        # Buscar reunioes
        meetings_query = {
            "task_type_id": 2,
            "is_completed": True,
            "entity_id": {"$in": list(leads_map.keys())},
            "is_deleted": False,
            "complete_till": {
                "$gte": start_timestamp,
                "$lte": end_timestamp
            }
        }

        meetings_cursor = tasks_collection.find(meetings_query).sort("complete_till", -1).limit(limit)

        reunioes_detalhes = []
        reunioes_organicas_detalhes = []

        async for task in meetings_cursor:
            lead_id = task.get("entity_id")
            lead = leads_map.get(lead_id)

            if not lead:
                continue

            cf = lead.get("custom_fields", {})
            fonte_lead = cf.get("fonte") or "N/A"

            # Determinar funil
            pipeline_id = lead.get("pipeline_id")
            if pipeline_id == PIPELINE_VENDAS:
                funil = "Funil de Vendas"
            elif pipeline_id == PIPELINE_REMARKETING:
                funil = "Remarketing"
            else:
                funil = "Não atribuído"

            # Data da proposta
            data_proposta = "N/A"
            if cf.get("data_proposta"):
                try:
                    dt = cf["data_proposta"]
                    if isinstance(dt, datetime):
                        data_proposta = dt.strftime("%d/%m/%Y")
                except:
                    pass

            # Obter nome real da etapa
            status_id = lead.get("status_id", 0)
            etapa = get_etapa_name(status_id)

            # Formato compativel com V1
            detail = {
                "id": lead_id,
                "Data da Reunião": datetime.fromtimestamp(task.get("complete_till", 0)).strftime("%d/%m/%Y"),
                "Nome do Lead": lead.get("name", ""),
                "Corretor": cf.get("corretor") or "Não atribuído",
                "Fonte": fonte_lead,
                "Anúncio": cf.get("anuncio") or "N/A",
                "Público": cf.get("publico") or "N/A",
                "Produto": cf.get("produto") or "N/A",
                "Data da Proposta": data_proposta,
                "Funil": funil,
                "Etapa": etapa,
                "Status": "Realizada"
            }

            # Separar organicas e nao organicas
            if fonte_lead in FONTES_ORGANICAS:
                reunioes_organicas_detalhes.append(detail)
            else:
                reunioes_detalhes.append(detail)

        # Tempo de execucao
        elapsed = time.time() - start_time
        logger.info(f"[V2] detailed-tables concluido em {elapsed:.3f}s")

        return {
            "leadsDetalhes": leads_detalhes,
            "organicosDetalhes": organicos_detalhes,
            "reunioesDetalhes": reunioes_detalhes,
            "reunioesOrganicasDetalhes": reunioes_organicas_detalhes,
            "vendasDetalhes": vendas_detalhes,
            "propostasDetalhes": propostas_detalhes,
            "summary": {
                "total_leads": len(leads_detalhes) + len(organicos_detalhes),  # Total de TODOS os leads
                "total_organicos": len(organicos_detalhes),
                "total_reunioes": len(reunioes_detalhes) + len(reunioes_organicas_detalhes),
                "total_vendas": len(vendas_detalhes),
                "total_propostas": len(propostas_detalhes),
                "valor_total_vendas": total_vendas_valor
            },
            "_metadata": {
                "version": "v2_mongodb",
                "elapsed_ms": round(elapsed * 1000, 2),
                "source": "mongodb",
                "generated_at": datetime.now().isoformat()
            }
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[V2] Erro em detailed-tables: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/stats")
async def get_mongodb_stats():
    """
    Retorna estatisticas do MongoDB para monitoramento.
    """
    stats = await get_leads_stats()
    return stats


@router.get("/health")
async def health_check():
    """
    Verifica se o MongoDB esta acessivel.
    """
    try:
        # Tentar uma operacao simples
        count = await leads_collection.count_documents({})
        return {
            "status": "healthy",
            "mongodb": "connected",
            "leads_count": count
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "mongodb": "error",
            "error": str(e)
        }
