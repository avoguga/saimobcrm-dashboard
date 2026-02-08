"""
Processador de Webhooks do Kommo
Processa eventos recebidos e atualiza MongoDB em background
"""

import asyncio
import logging
import re
from datetime import datetime
from typing import Dict, Any, Optional, List
from concurrent.futures import ThreadPoolExecutor

from app.models.kommo_models import (
    leads_collection,
    tasks_collection,
    webhook_events_collection,
    kommo_lead_to_model,
    kommo_task_to_model,
    PIPELINE_VENDAS,
    PIPELINE_REMARKETING,
)
from app.services.kommo_api import get_kommo_api

logger = logging.getLogger(__name__)


def normalize_webhook_task(task_data: Dict) -> Dict:
    """
    Normaliza dados de task do webhook para o formato esperado pelo modelo.

    Webhook envia:
    - task_type -> task_type_id
    - element_id -> entity_id
    - element_type -> entity_type (2 = leads)
    - status -> is_completed (1 = completed)
    - complete_till (string) -> complete_till (timestamp)
    """
    normalized = task_data.copy()

    # task_type -> task_type_id
    if "task_type" in normalized and "task_type_id" not in normalized:
        try:
            normalized["task_type_id"] = int(normalized["task_type"])
        except (ValueError, TypeError):
            normalized["task_type_id"] = 0

    # element_id -> entity_id
    if "element_id" in normalized and "entity_id" not in normalized:
        try:
            normalized["entity_id"] = int(normalized["element_id"])
        except (ValueError, TypeError):
            normalized["entity_id"] = None

    # element_type -> entity_type (2 = leads)
    if "element_type" in normalized:
        element_type = normalized.get("element_type")
        if element_type == "2" or element_type == 2:
            normalized["entity_type"] = "leads"

    # status -> is_completed (1 = completed, 0 = pending)
    if "status" in normalized:
        status = normalized.get("status")
        normalized["is_completed"] = status == "1" or status == 1

    # complete_till string -> timestamp
    if "complete_till" in normalized and isinstance(normalized["complete_till"], str):
        try:
            # Formato: "2026-02-09 02:59:00"
            dt = datetime.strptime(normalized["complete_till"], "%Y-%m-%d %H:%M:%S")
            normalized["complete_till"] = int(dt.timestamp())
        except (ValueError, TypeError):
            pass

    # complete_before pode ser o timestamp já
    if "complete_before" in normalized and "complete_till" not in normalized:
        try:
            normalized["complete_till"] = int(normalized["complete_before"])
        except (ValueError, TypeError):
            pass

    return normalized


def normalize_phone(phone: str) -> str:
    """
    Normaliza numero de telefone removendo caracteres especiais.
    Ex: '+55 31 98624-0685' -> '5531986240685'
    """
    if not phone:
        return ""
    # Remove tudo que nao for digito
    normalized = re.sub(r'\D', '', str(phone))
    return normalized


def normalize_name(name: str) -> str:
    """
    Normaliza nome para comparacao (lowercase, sem espacos extras).
    """
    if not name:
        return ""
    # Lowercase, remove espacos extras
    return ' '.join(name.lower().strip().split())


def extract_phones_from_contacts(contacts: List[Dict]) -> List[str]:
    """
    Extrai todos os telefones dos contatos de um lead.
    Retorna lista de telefones normalizados.
    """
    phones = []
    if not contacts:
        return phones

    for contact in contacts:
        # Contatos podem ter custom_fields_values com telefones
        custom_fields = contact.get("custom_fields_values", [])
        for field in custom_fields:
            field_code = field.get("field_code", "")
            # Telefones geralmente tem field_code "PHONE"
            if field_code == "PHONE" or "phone" in field_code.lower():
                values = field.get("values", [])
                for v in values:
                    phone_value = v.get("value", "")
                    if phone_value:
                        normalized = normalize_phone(phone_value)
                        if normalized and len(normalized) >= 8:  # Telefone valido
                            phones.append(normalized)

    return phones

# Pool de threads para operacoes sync
_executor = ThreadPoolExecutor(max_workers=4)


def parse_kommo_webhook_payload(flat_payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Converte payload flat do Kommo para formato nested.

    Kommo envia form-data no formato:
        leads[add][0][id] = 123
        leads[add][0][name] = "Nome"
        leads[update][0][id] = 456

    Converte para:
        {
            "leads": {
                "add": [{"id": "123", "name": "Nome"}],
                "update": [{"id": "456"}]
            }
        }
    """
    # Se ja esta no formato correto (JSON), retorna direto
    if "leads" in flat_payload and isinstance(flat_payload.get("leads"), dict):
        return flat_payload
    if "tasks" in flat_payload and isinstance(flat_payload.get("tasks"), dict):
        return flat_payload

    result = {}

    # Regex para parsear chaves como: leads[add][0][id] ou task[add][0][id]
    pattern = re.compile(r'^(\w+)\[(\w+)\]\[(\d+)\]\[(\w+)\]$')

    for key, value in flat_payload.items():
        match = pattern.match(key)
        if match:
            entity_type = match.group(1)  # leads, task
            action = match.group(2)        # add, update, delete, status, responsible
            index = int(match.group(3))    # 0, 1, 2...
            field = match.group(4)         # id, name, price, etc

            # Normalizar entity_type (task -> tasks)
            if entity_type == "task":
                entity_type = "tasks"

            # Criar estrutura se nao existir
            if entity_type not in result:
                result[entity_type] = {}
            if action not in result[entity_type]:
                result[entity_type][action] = []

            # Expandir lista se necessario
            while len(result[entity_type][action]) <= index:
                result[entity_type][action].append({})

            # Converter valores numericos
            if field == "id" and value:
                try:
                    value = int(value)
                except (ValueError, TypeError):
                    pass
            elif field in ["price", "pipeline_id", "status_id", "responsible_user_id", "task_type_id"]:
                try:
                    value = int(value) if value else None
                except (ValueError, TypeError):
                    pass

            # Atribuir valor
            result[entity_type][action][index][field] = value

    # Log para debug
    if result:
        logger.info(f"Payload parseado: {list(result.keys())}")
        for entity, actions in result.items():
            for action, items in actions.items():
                logger.info(f"  {entity}.{action}: {len(items)} itens")

    return result


class WebhookProcessor:
    """
    Processador de webhooks do Kommo.
    Recebe eventos e atualiza MongoDB de forma assincrona.
    """

    def __init__(self):
        self.kommo_api = get_kommo_api()
        self._processing_queue = asyncio.Queue()
        self._is_processing = False

    async def find_duplicate_leads(
        self,
        name: str,
        contacts: List[Dict],
        current_lead_id: int
    ) -> List[Dict]:
        """
        Busca leads duplicados pelo nome ou telefone.
        Retorna lista de leads que podem ser duplicados.
        """
        duplicates = []

        # Extrair telefones do novo lead
        phones = extract_phones_from_contacts(contacts)
        normalized_name = normalize_name(name)

        # Se nao tem nome nem telefone, nao da pra verificar duplicatas
        if not normalized_name and not phones:
            return duplicates

        # Construir query para buscar duplicatas
        # Busca por nome exato OU por telefone
        or_conditions = []

        if normalized_name:
            # Busca por nome (case-insensitive)
            or_conditions.append({
                "name": {"$regex": f"^{re.escape(normalized_name)}$", "$options": "i"}
            })

        if phones:
            # Busca por telefones normalizados
            # Verifica se algum telefone do lead existente corresponde
            for phone in phones:
                # Busca parcial - telefone pode estar com ou sem DDI
                if len(phone) >= 10:
                    # Se tem mais de 10 digitos, busca pelos ultimos 10 (DDD + numero)
                    phone_suffix = phone[-10:]
                    or_conditions.append({
                        "normalized_phones": {"$regex": f"{phone_suffix}$"}
                    })
                else:
                    or_conditions.append({
                        "normalized_phones": phone
                    })

        if not or_conditions:
            return duplicates

        # Buscar leads existentes que nao sejam o proprio lead
        query = {
            "$and": [
                {"lead_id": {"$ne": current_lead_id}},
                {"is_deleted": False},
                {"$or": or_conditions}
            ]
        }

        try:
            cursor = leads_collection.find(query, {"lead_id": 1, "name": 1, "price": 1, "normalized_phones": 1})
            async for lead in cursor:
                duplicates.append({
                    "lead_id": lead.get("lead_id"),
                    "name": lead.get("name"),
                    "price": lead.get("price")
                })
        except Exception as e:
            logger.error(f"Erro ao buscar leads duplicados: {e}")

        return duplicates

    async def log_webhook_event(
        self,
        event_type: str,
        entity_type: str,
        entity_id: Optional[int],
        payload: Dict[str, Any]
    ) -> str:
        """
        Registra evento de webhook no MongoDB para auditoria.
        """
        event = {
            "event_type": event_type,
            "entity_type": entity_type,
            "entity_id": entity_id,
            "payload": payload,
            "processed": False,
            "received_at": datetime.utcnow(),
        }

        result = await webhook_events_collection.insert_one(event)
        return str(result.inserted_id)

    async def mark_event_processed(self, event_id: str, error: Optional[str] = None):
        """Marca evento como processado"""
        from bson import ObjectId
        await webhook_events_collection.update_one(
            {"_id": ObjectId(event_id)},
            {
                "$set": {
                    "processed": True,
                    "processed_at": datetime.utcnow(),
                    "error": error
                }
            }
        )

    async def process_lead_add(self, lead_data: Dict) -> Dict:
        """
        Processa evento de lead adicionado.
        O Kommo envia dados basicos, precisamos buscar dados completos.
        Verifica se ja existe um lead com o mesmo nome ou telefone.
        """
        lead_id = lead_data.get("id")
        if not lead_id:
            return {"success": False, "error": "ID do lead nao fornecido"}

        logger.info(f"Processando lead ADD: {lead_id}")

        try:
            # Buscar dados completos do lead (com custom fields)
            full_lead = self.kommo_api.get_lead(lead_id)

            if not full_lead or full_lead.get("_error"):
                # Se nao conseguir buscar, usar dados do webhook
                logger.warning(f"Nao foi possivel buscar lead {lead_id}, usando dados do webhook")
                full_lead = lead_data

            # Verificar se e de um pipeline que nos interessa
            pipeline_id = full_lead.get("pipeline_id")
            if pipeline_id not in [PIPELINE_VENDAS, PIPELINE_REMARKETING]:
                logger.info(f"Lead {lead_id} ignorado - pipeline {pipeline_id} nao monitorado")
                return {"success": True, "action": "ignored", "reason": "pipeline_not_monitored"}

            # Converter e salvar
            model_data = kommo_lead_to_model(full_lead, source="webhook_add")

            # Extrair contatos e telefones normalizados
            contacts = full_lead.get("_embedded", {}).get("contacts", [])
            normalized_phones = extract_phones_from_contacts(contacts)
            model_data["normalized_phones"] = normalized_phones

            # Verificar se ja existe lead duplicado (mesmo nome ou telefone)
            lead_name = full_lead.get("name", "")
            duplicates = await self.find_duplicate_leads(lead_name, contacts, lead_id)

            if duplicates:
                model_data["possible_duplicates"] = duplicates
                model_data["is_possible_duplicate"] = True
                duplicate_ids = [d["lead_id"] for d in duplicates]
                logger.warning(
                    f"Lead {lead_id} ({lead_name}) pode ser duplicado de: {duplicate_ids}"
                )

                # Atualizar os leads existentes para marcar que tem duplicata tambem
                for dup in duplicates:
                    await leads_collection.update_one(
                        {"lead_id": dup["lead_id"]},
                        {
                            "$addToSet": {
                                "possible_duplicates": {
                                    "lead_id": lead_id,
                                    "name": lead_name,
                                    "price": model_data.get("price", 0)
                                }
                            },
                            "$set": {"is_possible_duplicate": True}
                        }
                    )
            else:
                model_data["is_possible_duplicate"] = False
                model_data["possible_duplicates"] = []

            result = await leads_collection.update_one(
                {"lead_id": lead_id},
                {"$set": model_data},
                upsert=True
            )

            action = "inserted" if result.upserted_id else "updated"
            logger.info(f"Lead {lead_id} {action} via webhook ADD")

            return {
                "success": True,
                "action": action,
                "lead_id": lead_id,
                "is_duplicate": len(duplicates) > 0,
                "duplicate_of": [d["lead_id"] for d in duplicates] if duplicates else []
            }

        except Exception as e:
            logger.error(f"Erro ao processar lead ADD {lead_id}: {e}")
            return {"success": False, "error": str(e)}

    async def process_lead_update(self, lead_data: Dict) -> Dict:
        """
        Processa evento de lead atualizado.
        """
        lead_id = lead_data.get("id")
        if not lead_id:
            return {"success": False, "error": "ID do lead nao fornecido"}

        logger.info(f"Processando lead UPDATE: {lead_id}")

        try:
            # Buscar dados completos atualizados
            full_lead = self.kommo_api.get_lead(lead_id)

            if not full_lead or full_lead.get("_error"):
                logger.warning(f"Nao foi possivel buscar lead {lead_id} para update")
                return {"success": False, "error": "Nao foi possivel buscar dados do lead"}

            # Verificar pipeline
            pipeline_id = full_lead.get("pipeline_id")
            if pipeline_id not in [PIPELINE_VENDAS, PIPELINE_REMARKETING]:
                # Lead foi movido para outro pipeline - marcar como deletado
                logger.info(f"Lead {lead_id} movido para pipeline {pipeline_id} - removendo")
                await leads_collection.update_one(
                    {"lead_id": lead_id},
                    {
                        "$set": {
                            "is_deleted": True,
                            "synced_at": datetime.utcnow(),
                            "source": "webhook_update_pipeline_changed"
                        }
                    }
                )
                return {"success": True, "action": "removed", "reason": "pipeline_changed"}

            # Converter e salvar
            model_data = kommo_lead_to_model(full_lead, source="webhook_update")

            result = await leads_collection.update_one(
                {"lead_id": lead_id},
                {"$set": model_data},
                upsert=True
            )

            action = "updated" if result.modified_count > 0 else "unchanged"
            logger.info(f"Lead {lead_id} {action} via webhook UPDATE")

            return {"success": True, "action": action, "lead_id": lead_id}

        except Exception as e:
            logger.error(f"Erro ao processar lead UPDATE {lead_id}: {e}")
            return {"success": False, "error": str(e)}

    async def process_lead_delete(self, lead_data: Dict) -> Dict:
        """
        Processa evento de lead deletado.
        Faz soft delete no MongoDB.
        """
        lead_id = lead_data.get("id")
        if not lead_id:
            return {"success": False, "error": "ID do lead nao fornecido"}

        logger.info(f"Processando lead DELETE: {lead_id}")

        try:
            result = await leads_collection.update_one(
                {"lead_id": lead_id},
                {
                    "$set": {
                        "is_deleted": True,
                        "synced_at": datetime.utcnow(),
                        "source": "webhook_delete"
                    }
                }
            )

            if result.modified_count > 0:
                logger.info(f"Lead {lead_id} marcado como deletado via webhook")
                return {"success": True, "action": "deleted", "lead_id": lead_id}
            else:
                logger.warning(f"Lead {lead_id} nao encontrado para deletar")
                return {"success": True, "action": "not_found", "lead_id": lead_id}

        except Exception as e:
            logger.error(f"Erro ao processar lead DELETE {lead_id}: {e}")
            return {"success": False, "error": str(e)}

    async def process_lead_status_change(self, lead_data: Dict) -> Dict:
        """
        Processa evento de mudanca de status do lead.
        """
        # Status change e tratado igual a update
        return await self.process_lead_update(lead_data)

    async def process_lead_responsible_change(self, lead_data: Dict) -> Dict:
        """
        Processa evento de mudanca de responsavel.
        """
        # Responsible change e tratado igual a update
        return await self.process_lead_update(lead_data)

    async def process_task_add(self, task_data: Dict) -> Dict:
        """
        Processa evento de task adicionada.
        """
        task_id = task_data.get("id")
        if not task_id:
            return {"success": False, "error": "ID da task nao fornecido"}

        logger.info(f"Processando task ADD: {task_id}")

        try:
            # Normalizar dados do webhook para o formato do modelo
            normalized_data = normalize_webhook_task(task_data)

            # Verificar se e reuniao (task_type_id = 2)
            task_type_id = normalized_data.get("task_type_id")
            if task_type_id != 2:
                logger.info(f"Task {task_id} ignorada - tipo {task_type_id} (nao e reuniao)")
                return {"success": True, "action": "ignored", "reason": "not_meeting"}

            model_data = kommo_task_to_model(normalized_data, source="webhook_add")

            result = await tasks_collection.update_one(
                {"task_id": task_id},
                {"$set": model_data},
                upsert=True
            )

            action = "inserted" if result.upserted_id else "updated"
            logger.info(f"Task {task_id} {action} via webhook ADD")

            return {"success": True, "action": action, "task_id": task_id}

        except Exception as e:
            logger.error(f"Erro ao processar task ADD {task_id}: {e}")
            return {"success": False, "error": str(e)}

    async def process_task_update(self, task_data: Dict) -> Dict:
        """
        Processa evento de task atualizada.
        """
        task_id = task_data.get("id")
        if not task_id:
            return {"success": False, "error": "ID da task nao fornecido"}

        logger.info(f"Processando task UPDATE: {task_id}")

        try:
            # Normalizar dados do webhook para o formato do modelo
            normalized_data = normalize_webhook_task(task_data)
            model_data = kommo_task_to_model(normalized_data, source="webhook_update")

            result = await tasks_collection.update_one(
                {"task_id": task_id},
                {"$set": model_data},
                upsert=True
            )

            action = "updated" if result.modified_count > 0 else "unchanged"
            logger.info(f"Task {task_id} {action} via webhook UPDATE")

            return {"success": True, "action": action, "task_id": task_id}

        except Exception as e:
            logger.error(f"Erro ao processar task UPDATE {task_id}: {e}")
            return {"success": False, "error": str(e)}

    async def process_task_delete(self, task_data: Dict) -> Dict:
        """
        Processa evento de task deletada.
        """
        task_id = task_data.get("id")
        if not task_id:
            return {"success": False, "error": "ID da task nao fornecido"}

        logger.info(f"Processando task DELETE: {task_id}")

        try:
            result = await tasks_collection.update_one(
                {"task_id": task_id},
                {
                    "$set": {
                        "is_deleted": True,
                        "synced_at": datetime.utcnow(),
                        "source": "webhook_delete"
                    }
                }
            )

            if result.modified_count > 0:
                return {"success": True, "action": "deleted", "task_id": task_id}
            else:
                return {"success": True, "action": "not_found", "task_id": task_id}

        except Exception as e:
            logger.error(f"Erro ao processar task DELETE {task_id}: {e}")
            return {"success": False, "error": str(e)}

    async def process_webhook_payload(self, payload: Dict[str, Any]) -> Dict:
        """
        Processa payload completo do webhook do Kommo.
        O Kommo envia dados no formato:
        {
            "leads": {
                "add": [...],
                "update": [...],
                "delete": [...],
                "status": [...],
                "responsible": [...]
            },
            "tasks": {
                "add": [...],
                "update": [...],
                "delete": [...]
            },
            "account": {...}
        }
        """
        results = {
            "leads": {"add": [], "update": [], "delete": [], "status": [], "responsible": []},
            "tasks": {"add": [], "update": [], "delete": []},
            "total_processed": 0,
            "errors": 0
        }

        # Processar leads
        leads_data = payload.get("leads", {})

        # Lead ADD
        for lead in leads_data.get("add", []):
            event_id = await self.log_webhook_event("lead_add", "leads", lead.get("id"), lead)
            result = await self.process_lead_add(lead)
            results["leads"]["add"].append(result)
            results["total_processed"] += 1
            if not result.get("success"):
                results["errors"] += 1
            await self.mark_event_processed(event_id, result.get("error"))

        # Lead UPDATE
        for lead in leads_data.get("update", []):
            event_id = await self.log_webhook_event("lead_update", "leads", lead.get("id"), lead)
            result = await self.process_lead_update(lead)
            results["leads"]["update"].append(result)
            results["total_processed"] += 1
            if not result.get("success"):
                results["errors"] += 1
            await self.mark_event_processed(event_id, result.get("error"))

        # Lead DELETE
        for lead in leads_data.get("delete", []):
            event_id = await self.log_webhook_event("lead_delete", "leads", lead.get("id"), lead)
            result = await self.process_lead_delete(lead)
            results["leads"]["delete"].append(result)
            results["total_processed"] += 1
            if not result.get("success"):
                results["errors"] += 1
            await self.mark_event_processed(event_id, result.get("error"))

        # Lead STATUS change
        for lead in leads_data.get("status", []):
            event_id = await self.log_webhook_event("lead_status", "leads", lead.get("id"), lead)
            result = await self.process_lead_status_change(lead)
            results["leads"]["status"].append(result)
            results["total_processed"] += 1
            if not result.get("success"):
                results["errors"] += 1
            await self.mark_event_processed(event_id, result.get("error"))

        # Lead RESPONSIBLE change
        for lead in leads_data.get("responsible", []):
            event_id = await self.log_webhook_event("lead_responsible", "leads", lead.get("id"), lead)
            result = await self.process_lead_responsible_change(lead)
            results["leads"]["responsible"].append(result)
            results["total_processed"] += 1
            if not result.get("success"):
                results["errors"] += 1
            await self.mark_event_processed(event_id, result.get("error"))

        # Processar tasks
        tasks_data = payload.get("tasks", {})

        # Task ADD
        for task in tasks_data.get("add", []):
            event_id = await self.log_webhook_event("task_add", "tasks", task.get("id"), task)
            result = await self.process_task_add(task)
            results["tasks"]["add"].append(result)
            results["total_processed"] += 1
            if not result.get("success"):
                results["errors"] += 1
            await self.mark_event_processed(event_id, result.get("error"))

        # Task UPDATE
        for task in tasks_data.get("update", []):
            event_id = await self.log_webhook_event("task_update", "tasks", task.get("id"), task)
            result = await self.process_task_update(task)
            results["tasks"]["update"].append(result)
            results["total_processed"] += 1
            if not result.get("success"):
                results["errors"] += 1
            await self.mark_event_processed(event_id, result.get("error"))

        # Task DELETE
        for task in tasks_data.get("delete", []):
            event_id = await self.log_webhook_event("task_delete", "tasks", task.get("id"), task)
            result = await self.process_task_delete(task)
            results["tasks"]["delete"].append(result)
            results["total_processed"] += 1
            if not result.get("success"):
                results["errors"] += 1
            await self.mark_event_processed(event_id, result.get("error"))

        logger.info(f"Webhook processado: {results['total_processed']} eventos, {results['errors']} erros")

        return results

    async def process_in_background(self, payload: Dict[str, Any]):
        """
        Processa webhook em background para responder rapidamente.
        Kommo requer resposta em ate 2 segundos.
        """
        try:
            # Parsear payload do formato flat do Kommo para formato nested
            parsed_payload = parse_kommo_webhook_payload(payload)

            if not parsed_payload:
                logger.warning(f"Payload vazio apos parse. Original keys: {list(payload.keys())[:10]}")
                return

            await self.process_webhook_payload(parsed_payload)
        except Exception as e:
            logger.error(f"Erro no processamento em background: {e}")


# Instancia singleton
_webhook_processor: Optional[WebhookProcessor] = None


def get_webhook_processor() -> WebhookProcessor:
    """Retorna instancia singleton do processador de webhooks"""
    global _webhook_processor
    if _webhook_processor is None:
        _webhook_processor = WebhookProcessor()
    return _webhook_processor
