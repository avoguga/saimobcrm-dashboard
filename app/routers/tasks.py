from fastapi import APIRouter, HTTPException, Query
from typing import Optional, List
import logging
from datetime import datetime
from app.services.kommo_api import KommoAPI

router = APIRouter()
logger = logging.getLogger(__name__)

# Instanciar API uma vez
kommo_api = KommoAPI()

@router.get("/")
async def get_tasks(
    page: Optional[int] = Query(1, description="Número da página"),
    limit: Optional[int] = Query(50, description="Número de entidades retornadas (máximo 250)"),
    responsible_user_id: Optional[List[int]] = Query(None, description="Filtrar por ID do usuário responsável"),
    is_completed: Optional[int] = Query(None, description="Filtrar por status da tarefa (1=completa, 0=aberta)"),
    task_type: Optional[List[int]] = Query(None, description="Filtrar por tipo de tarefa"),
    entity_type: Optional[str] = Query(None, description="Filtrar por tipo de entidade vinculada"),
    entity_id: Optional[List[int]] = Query(None, description="Filtrar por ID da entidade vinculada"),
    task_id: Optional[List[int]] = Query(None, description="Filtrar por ID da tarefa"),
    updated_at_from: Optional[int] = Query(None, description="Filtrar por data de edição (timestamp início)"),
    updated_at_to: Optional[int] = Query(None, description="Filtrar por data de edição (timestamp fim)"),
    order_complete_till: Optional[str] = Query(None, description="Ordenar por prazo (asc/desc)"),
    order_created_at: Optional[str] = Query(None, description="Ordenar por data de criação (asc/desc)"),
    order_id: Optional[str] = Query(None, description="Ordenar por ID (asc/desc)")
):
    """
    Endpoint para listar tarefas do Kommo CRM
    
    Retorna uma lista de tarefas com base nos filtros aplicados.
    Suporta todos os filtros e ordenações disponíveis na API v4 do Kommo.
    
    Tipos de tarefa comuns:
    - 1: Ligação
    - 2: Reunião
    - 3: Email
    """
    try:
        logger.info(f"Buscando tarefas - página: {page}, limit: {limit}")
        
        # Construir parâmetros da consulta
        params = {
            "page": page,
            "limit": min(limit, 250)  # Garantir limite máximo de 250
        }
        
        # Aplicar filtros opcionais
        if responsible_user_id:
            if len(responsible_user_id) == 1:
                params["filter[responsible_user_id]"] = responsible_user_id[0]
            else:
                for i, user_id in enumerate(responsible_user_id):
                    params[f"filter[responsible_user_id][{i}]"] = user_id
        
        if is_completed is not None:
            params["filter[is_completed]"] = is_completed
        
        if task_type:
            if len(task_type) == 1:
                params["filter[task_type_id]"] = task_type[0]
            else:
                for i, type_id in enumerate(task_type):
                    params[f"filter[task_type_id][{i}]"] = type_id
        
        if entity_type:
            params["filter[entity_type]"] = entity_type
        
        if entity_id:
            if len(entity_id) == 1:
                params["filter[entity_id]"] = entity_id[0]
            else:
                for i, ent_id in enumerate(entity_id):
                    params[f"filter[entity_id][{i}]"] = ent_id
        
        if task_id:
            if len(task_id) == 1:
                params["filter[id]"] = task_id[0]
            else:
                for i, t_id in enumerate(task_id):
                    params[f"filter[id][{i}]"] = t_id
        
        if updated_at_from:
            params["filter[updated_at][from]"] = updated_at_from
        
        if updated_at_to:
            params["filter[updated_at][to]"] = updated_at_to
        
        # Aplicar ordenações
        if order_complete_till in ["asc", "desc"]:
            params["order[complete_till]"] = order_complete_till
        
        if order_created_at in ["asc", "desc"]:
            params["order[created_at]"] = order_created_at
        
        if order_id in ["asc", "desc"]:
            params["order[id]"] = order_id
        
        # Fazer requisição para API
        response = kommo_api.get_tasks(params)
        
        if not response:
            logger.warning("Resposta vazia da API Kommo para tarefas")
            return {
                "tasks": [],
                "total": 0,
                "message": "Nenhuma tarefa encontrada"
            }
        
        # Verificar se há erro na resposta
        if response.get("_error"):
            error_msg = response.get("_error_message", "Erro desconhecido")
            logger.error(f"Erro da API Kommo: {error_msg}")
            raise HTTPException(status_code=500, detail=f"Erro da API Kommo: {error_msg}")
        
        # Processar dados das tarefas
        tasks_data = []
        total_tasks = 0
        
        if "_embedded" in response and "tasks" in response["_embedded"]:
            raw_tasks = response["_embedded"]["tasks"]
            total_tasks = response.get("_total_items", len(raw_tasks))
            
            for task in raw_tasks:
                if not task:
                    continue
                
                # Processar dados da tarefa
                task_data = {
                    "id": task.get("id"),
                    "created_by": task.get("created_by"),
                    "updated_by": task.get("updated_by"),
                    "created_at": task.get("created_at"),
                    "updated_at": task.get("updated_at"),
                    "responsible_user_id": task.get("responsible_user_id"),
                    "group_id": task.get("group_id"),
                    "entity_id": task.get("entity_id"),
                    "entity_type": task.get("entity_type"),
                    "is_completed": task.get("is_completed", False),
                    "task_type_id": task.get("task_type_id"),
                    "text": task.get("text", ""),
                    "duration": task.get("duration"),
                    "complete_till": task.get("complete_till"),
                    "account_id": task.get("account_id")
                }
                
                # Processar resultado da tarefa
                result = task.get("result")
                if result:
                    task_data["result"] = {
                        "text": result.get("text", "")
                    }
                else:
                    task_data["result"] = None
                
                # Adicionar datas formatadas para facilitar visualização
                if task_data["created_at"]:
                    task_data["created_at_formatted"] = datetime.fromtimestamp(task_data["created_at"]).strftime("%d/%m/%Y %H:%M")
                
                if task_data["updated_at"]:
                    task_data["updated_at_formatted"] = datetime.fromtimestamp(task_data["updated_at"]).strftime("%d/%m/%Y %H:%M")
                
                if task_data["complete_till"]:
                    task_data["complete_till_formatted"] = datetime.fromtimestamp(task_data["complete_till"]).strftime("%d/%m/%Y %H:%M")
                
                # Mapear tipo de tarefa para nome legível
                task_type_names = {
                    1: "Ligação",
                    2: "Reunião", 
                    3: "Email",
                    4: "Lembrete"
                }
                task_data["task_type_name"] = task_type_names.get(task_data["task_type_id"], "Outro")
                
                # Status da tarefa
                task_data["status"] = "Completa" if task_data["is_completed"] else "Aberta"
                
                tasks_data.append(task_data)
        
        # Informações de paginação
        page_info = response.get("_page", {})
        
        # Montar resposta
        result = {
            "tasks": tasks_data,
            "pagination": {
                "total_items": total_tasks,
                "current_page": page,
                "items_per_page": limit,
                "total_pages": (total_tasks + limit - 1) // limit if total_tasks > 0 else 0
            },
            "filters_applied": {
                "responsible_user_id": responsible_user_id,
                "is_completed": is_completed,
                "task_type": task_type,
                "entity_type": entity_type,
                "entity_id": entity_id,
                "task_id": task_id,
                "updated_at_range": {
                    "from": updated_at_from,
                    "to": updated_at_to
                } if updated_at_from or updated_at_to else None
            },
            "ordering": {
                "complete_till": order_complete_till,
                "created_at": order_created_at,
                "id": order_id
            },
            "_metadata": {
                "generated_at": datetime.now().isoformat(),
                "api_version": "v4",
                "data_source": "kommo_api"
            }
        }
        
        logger.info(f"Retornando {len(tasks_data)} tarefas de {total_tasks} total")
        return result
        
    except HTTPException:
        # Re-raise HTTPExceptions
        raise
    except Exception as e:
        logger.error(f"Erro ao buscar tarefas: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")


@router.get("/summary")
async def get_tasks_summary(
    responsible_user_id: Optional[int] = Query(None, description="ID do usuário responsável"),
    days: int = Query(30, description="Período em dias para análise")
):
    """
    Endpoint para obter resumo de tarefas por usuário/período
    
    Retorna estatísticas agregadas das tarefas:
    - Total de tarefas
    - Tarefas completas vs abertas
    - Distribuição por tipo
    - Tarefas em atraso
    """
    try:
        logger.info(f"Gerando resumo de tarefas para {days} dias")
        
        # Calcular período
        import time
        end_time = int(time.time())
        start_time = end_time - (days * 24 * 60 * 60)
        
        # Parâmetros base
        params = {
            "filter[created_at][from]": start_time,
            "filter[created_at][to]": end_time,
            "limit": 250
        }
        
        if responsible_user_id:
            params["filter[responsible_user_id]"] = responsible_user_id
        
        # Buscar tarefas
        response = kommo_api.get_tasks(params)
        
        if not response or "_embedded" not in response:
            return {
                "summary": {
                    "total_tasks": 0,
                    "completed_tasks": 0,
                    "open_tasks": 0,
                    "overdue_tasks": 0
                },
                "by_type": {},
                "period_days": days,
                "responsible_user_id": responsible_user_id
            }
        
        tasks = response["_embedded"].get("tasks", [])
        
        # Calcular estatísticas
        total_tasks = len(tasks)
        completed_tasks = 0
        open_tasks = 0
        overdue_tasks = 0
        type_distribution = {}
        
        current_time = time.time()
        
        for task in tasks:
            if not task:
                continue
            
            # Status
            if task.get("is_completed"):
                completed_tasks += 1
            else:
                open_tasks += 1
                
                # Verificar se está em atraso
                complete_till = task.get("complete_till")
                if complete_till and complete_till < current_time:
                    overdue_tasks += 1
            
            # Tipo de tarefa
            task_type_id = task.get("task_type_id")
            task_type_names = {
                1: "Ligação",
                2: "Reunião", 
                3: "Email",
                4: "Lembrete"
            }
            task_type_name = task_type_names.get(task_type_id, f"Tipo {task_type_id}")
            
            if task_type_name not in type_distribution:
                type_distribution[task_type_name] = {"total": 0, "completed": 0, "open": 0}
            
            type_distribution[task_type_name]["total"] += 1
            if task.get("is_completed"):
                type_distribution[task_type_name]["completed"] += 1
            else:
                type_distribution[task_type_name]["open"] += 1
        
        # Calcular taxas
        completion_rate = (completed_tasks / total_tasks * 100) if total_tasks > 0 else 0
        overdue_rate = (overdue_tasks / open_tasks * 100) if open_tasks > 0 else 0
        
        result = {
            "summary": {
                "total_tasks": total_tasks,
                "completed_tasks": completed_tasks,
                "open_tasks": open_tasks,
                "overdue_tasks": overdue_tasks,
                "completion_rate": round(completion_rate, 1),
                "overdue_rate": round(overdue_rate, 1)
            },
            "by_type": type_distribution,
            "period": {
                "days": days,
                "start_date": datetime.fromtimestamp(start_time).strftime("%Y-%m-%d"),
                "end_date": datetime.fromtimestamp(end_time).strftime("%Y-%m-%d")
            },
            "responsible_user_id": responsible_user_id,
            "_metadata": {
                "generated_at": datetime.now().isoformat(),
                "api_version": "v4"
            }
        }
        
        logger.info(f"Resumo gerado: {total_tasks} tarefas, {completion_rate:.1f}% completas")
        return result
        
    except Exception as e:
        logger.error(f"Erro ao gerar resumo de tarefas: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")


@router.get("/by-user")
async def get_tasks_by_user(
    days: int = Query(30, description="Período em dias para análise"),
    is_completed: Optional[int] = Query(None, description="Filtrar por status (1=completa, 0=aberta)")
):
    """
    Endpoint para obter tarefas agrupadas por usuário responsável
    
    Útil para análise de produtividade da equipe
    """
    try:
        logger.info(f"Buscando tarefas por usuário para {days} dias")
        
        # Calcular período
        import time
        end_time = int(time.time())
        start_time = end_time - (days * 24 * 60 * 60)
        
        # Parâmetros
        params = {
            "filter[created_at][from]": start_time,
            "filter[created_at][to]": end_time,
            "limit": 250
        }
        
        if is_completed is not None:
            params["filter[is_completed]"] = is_completed
        
        # Buscar tarefas
        response = kommo_api.get_tasks(params)
        
        if not response or "_embedded" not in response:
            return {
                "tasks_by_user": [],
                "period_days": days,
                "is_completed_filter": is_completed
            }
        
        tasks = response["_embedded"].get("tasks", [])
        
        # Buscar dados de usuários para mapear IDs para nomes
        users_response = kommo_api.get_users()
        users_map = {}
        
        if users_response and "_embedded" in users_response:
            for user in users_response["_embedded"].get("users", []):
                users_map[user["id"]] = user["name"]
        
        # Agrupar por usuário
        user_tasks = {}
        
        for task in tasks:
            if not task:
                continue
            
            user_id = task.get("responsible_user_id")
            if not user_id:
                continue
            
            user_name = users_map.get(user_id, f"Usuário {user_id}")
            
            if user_name not in user_tasks:
                user_tasks[user_name] = {
                    "user_id": user_id,
                    "user_name": user_name,
                    "total_tasks": 0,
                    "completed_tasks": 0,
                    "open_tasks": 0,
                    "overdue_tasks": 0,
                    "by_type": {}
                }
            
            user_data = user_tasks[user_name]
            user_data["total_tasks"] += 1
            
            # Status
            if task.get("is_completed"):
                user_data["completed_tasks"] += 1
            else:
                user_data["open_tasks"] += 1
                
                # Verificar atraso
                complete_till = task.get("complete_till")
                if complete_till and complete_till < time.time():
                    user_data["overdue_tasks"] += 1
            
            # Tipo
            task_type_id = task.get("task_type_id")
            task_type_names = {1: "Ligação", 2: "Reunião", 3: "Email", 4: "Lembrete"}
            task_type_name = task_type_names.get(task_type_id, f"Tipo {task_type_id}")
            
            if task_type_name not in user_data["by_type"]:
                user_data["by_type"][task_type_name] = 0
            user_data["by_type"][task_type_name] += 1
        
        # Calcular taxas para cada usuário
        for user_data in user_tasks.values():
            total = user_data["total_tasks"]
            user_data["completion_rate"] = (user_data["completed_tasks"] / total * 100) if total > 0 else 0
            user_data["overdue_rate"] = (user_data["overdue_tasks"] / user_data["open_tasks"] * 100) if user_data["open_tasks"] > 0 else 0
        
        # Ordenar por total de tarefas
        sorted_users = sorted(user_tasks.values(), key=lambda x: x["total_tasks"], reverse=True)
        
        result = {
            "tasks_by_user": sorted_users,
            "period": {
                "days": days,
                "start_date": datetime.fromtimestamp(start_time).strftime("%Y-%m-%d"),
                "end_date": datetime.fromtimestamp(end_time).strftime("%Y-%m-%d")
            },
            "filters": {
                "is_completed": is_completed
            },
            "summary": {
                "total_users": len(sorted_users),
                "total_tasks": sum(u["total_tasks"] for u in sorted_users)
            },
            "_metadata": {
                "generated_at": datetime.now().isoformat()
            }
        }
        
        logger.info(f"Retornando tarefas para {len(sorted_users)} usuários")
        return result
        
    except Exception as e:
        logger.error(f"Erro ao buscar tarefas por usuário: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")