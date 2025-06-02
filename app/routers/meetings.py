from fastapi import APIRouter, Query, HTTPException
from typing import Dict, Optional
from datetime import datetime, timedelta
from app.services.kommo_api import KommoAPI

router = APIRouter(prefix="/meetings", tags=["Meetings"])

@router.get("/")
async def get_all_meetings(
    include_completed: bool = Query(True, description="Incluir reuniões concluídas"),
    days: int = Query(30, description="Período em dias para filtrar")
):
    """Retorna todas as reuniões/tarefas de reunião"""
    try:
        api = KommoAPI()
        
        # Parâmetros base para buscar reuniões
        params = {
            'filter[task_type]': 2,  # Tipo reunião
            'limit': 250
        }
        
        if not include_completed:
            params['filter[is_completed]'] = 0  # Apenas não concluídas
        
        tasks_response = api.get_tasks(params)
        
        if not tasks_response or '_embedded' not in tasks_response:
            return {"meetings": [], "total": 0, "message": "Nenhuma reunião encontrada"}
        
        tasks = tasks_response.get('_embedded', {}).get('tasks', [])
        
        # Filtrar por período se necessário
        if days > 0:
            cutoff_timestamp = datetime.now().timestamp() - (days * 24 * 60 * 60)
            filtered_tasks = []
            
            for task in tasks:
                task_date = task.get('complete_till', task.get('created_at', 0))
                if task_date >= cutoff_timestamp:
                    filtered_tasks.append(task)
            
            tasks = filtered_tasks
        
        # Buscar informações dos usuários para enriquecer os dados
        users_response = api.get_users()
        users_dict = {}
        if users_response and '_embedded' in users_response:
            users = users_response.get('_embedded', {}).get('users', [])
            users_dict = {user['id']: user['name'] for user in users}
        
        # Enriquecer dados das reuniões
        enriched_meetings = []
        for task in tasks:
            user_id = task.get('responsible_user_id')
            user_name = users_dict.get(user_id, f"Usuário {user_id}") if user_id else "Não atribuído"
            
            meeting = {
                "id": task.get("id"),
                "text": task.get("text", ""),
                "complete_till": task.get("complete_till"),
                "is_completed": task.get("is_completed", False),
                "completed_at": task.get("completed_at"),
                "created_at": task.get("created_at"),
                "responsible_user_id": user_id,
                "responsible_user_name": user_name,
                "entity_id": task.get("entity_id"),
                "entity_type": task.get("entity_type")
            }
            enriched_meetings.append(meeting)
        
        # Ordenar por data de vencimento
        enriched_meetings.sort(key=lambda x: x.get("complete_till", 0), reverse=True)
        
        return {
            "meetings": enriched_meetings,
            "total": len(enriched_meetings),
            "period_days": days,
            "include_completed": include_completed
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao buscar reuniões: {str(e)}")

@router.get("/types")
async def get_meeting_types():
    """Retorna os tipos de reunião disponíveis"""
    try:
        # Tipos de reunião comuns no CRM
        meeting_types = [
            {
                "id": 1,
                "name": "Primeira reunião",
                "description": "Reunião inicial com o lead",
                "color": "#4CAF50"
            },
            {
                "id": 2,
                "name": "Apresentação de proposta",
                "description": "Apresentação da proposta comercial",
                "color": "#2196F3"
            },
            {
                "id": 3,
                "name": "Negociação",
                "description": "Reunião de negociação de termos",
                "color": "#FF9800"
            },
            {
                "id": 4,
                "name": "Fechamento",
                "description": "Reunião para fechamento do negócio",
                "color": "#9C27B0"
            },
            {
                "id": 5,
                "name": "Follow-up",
                "description": "Reunião de acompanhamento",
                "color": "#607D8B"
            },
            {
                "id": 6,
                "name": "Reunião técnica",
                "description": "Reunião para discussões técnicas",
                "color": "#795548"
            },
            {
                "id": 7,
                "name": "Revisão de contrato",
                "description": "Reunião para revisão de termos contratuais",
                "color": "#E91E63"
            },
            {
                "id": 8,
                "name": "Kickoff do projeto",
                "description": "Reunião de início do projeto",
                "color": "#00BCD4"
            },
            {
                "id": 9,
                "name": "Reunião de status",
                "description": "Reunião de acompanhamento de status",
                "color": "#8BC34A"
            },
            {
                "id": 10,
                "name": "Reunião de encerramento",
                "description": "Reunião final do projeto/venda",
                "color": "#FFC107"
            }
        ]
        
        return {
            "meeting_types": meeting_types,
            "total_types": len(meeting_types)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao obter tipos de reunião: {str(e)}")

@router.get("/stats")
async def get_meeting_stats(
    days: int = Query(30, description="Período em dias para análise")
):
    """Retorna estatísticas das reuniões"""
    try:
        api = KommoAPI()
        
        # Buscar todas as reuniões
        params = {
            'filter[task_type]': 2,  # Tipo reunião
            'limit': 500
        }
        
        tasks_response = api.get_tasks(params)
        
        if not tasks_response or '_embedded' not in tasks_response:
            return {
                "total_meetings": 0,
                "scheduled_meetings": 0,
                "completed_meetings": 0,
                "overdue_meetings": 0,
                "completion_rate": 0,
                "period_days": days
            }
        
        tasks = tasks_response.get('_embedded', {}).get('tasks', [])
        
        # Filtrar por período
        cutoff_timestamp = datetime.now().timestamp() - (days * 24 * 60 * 60)
        current_timestamp = datetime.now().timestamp()
        
        total_meetings = 0
        scheduled_meetings = 0
        completed_meetings = 0
        overdue_meetings = 0
        
        # Estatísticas por usuário
        user_stats = {}
        
        # Buscar usuários
        users_response = api.get_users()
        users_dict = {}
        if users_response and '_embedded' in users_response:
            users = users_response.get('_embedded', {}).get('users', [])
            users_dict = {user['id']: user['name'] for user in users}
        
        for task in tasks:
            task_date = task.get('complete_till', task.get('created_at', 0))
            
            # Filtrar por período
            if task_date < cutoff_timestamp:
                continue
            
            total_meetings += 1
            user_id = task.get('responsible_user_id')
            
            # Inicializar estatísticas do usuário
            if user_id and user_id not in user_stats:
                user_stats[user_id] = {
                    "user_name": users_dict.get(user_id, f"Usuário {user_id}"),
                    "total": 0,
                    "completed": 0,
                    "scheduled": 0,
                    "overdue": 0
                }
            
            if user_id:
                user_stats[user_id]["total"] += 1
            
            if task.get('is_completed'):
                completed_meetings += 1
                if user_id:
                    user_stats[user_id]["completed"] += 1
            else:
                scheduled_meetings += 1
                if user_id:
                    user_stats[user_id]["scheduled"] += 1
                
                # Verificar se está atrasada
                complete_till = task.get('complete_till', 0)
                if complete_till < current_timestamp:
                    overdue_meetings += 1
                    if user_id:
                        user_stats[user_id]["overdue"] += 1
        
        # Calcular taxa de conclusão
        completion_rate = (completed_meetings / total_meetings * 100) if total_meetings > 0 else 0
        
        # Converter stats de usuários para lista
        user_stats_list = []
        for user_id, stats in user_stats.items():
            stats["completion_rate"] = (stats["completed"] / stats["total"] * 100) if stats["total"] > 0 else 0
            user_stats_list.append(stats)
        
        # Ordenar por total de reuniões
        user_stats_list.sort(key=lambda x: x["total"], reverse=True)
        
        return {
            "period_days": days,
            "summary": {
                "total_meetings": total_meetings,
                "scheduled_meetings": scheduled_meetings,
                "completed_meetings": completed_meetings,
                "overdue_meetings": overdue_meetings,
                "completion_rate": round(completion_rate, 2)
            },
            "user_stats": user_stats_list,
            "top_performer": user_stats_list[0]["user_name"] if user_stats_list else None
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao obter estatísticas de reuniões: {str(e)}")

@router.get("/scheduled-by-user")
async def get_scheduled_meetings_by_user():
    """
    Obtém quantidade de reuniões agendadas por corretor
    """
    try:
        api = KommoAPI()
        
        # Buscar usuários
        users_response = api.get_users()
        if not users_response or '_embedded' not in users_response:
            return {"error": "Não foi possível obter usuários", "meetings_scheduled_by_user": {}, "total_scheduled": 0}
        
        users = users_response.get('_embedded', {}).get('users', [])
        users_dict = {user['id']: user['name'] for user in users}
        
        # Buscar tarefas do tipo reunião (task_type_id = 2)
        params = {
            'filter[task_type]': 2,  # Tipo reunião
            'filter[is_completed]': 0,  # Não concluídas
            'limit': 250
        }
        
        tasks_response = api.get_tasks(params)
        meetings_by_user = {}
        
        if tasks_response and '_embedded' in tasks_response:
            tasks = tasks_response.get('_embedded', {}).get('tasks', [])
            
            for task in tasks:
                user_id = task.get('responsible_user_id')
                if user_id:
                    user_name = users_dict.get(user_id, f"Usuário {user_id}")
                    meetings_by_user[user_name] = meetings_by_user.get(user_name, 0) + 1
        
        return {
            "meetings_scheduled_by_user": meetings_by_user,
            "total_scheduled": sum(meetings_by_user.values())
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao buscar reuniões agendadas: {str(e)}")

@router.get("/completed-by-user")
async def get_completed_meetings_by_user(
    days: int = Query(30, description="Número de dias para análise")
):
    """
    Obtém quantidade de reuniões realizadas por corretor
    """
    try:
        api = KommoAPI()
        
        # Buscar usuários
        users_response = api.get_users()
        if not users_response or '_embedded' not in users_response:
            return {"error": "Não foi possível obter usuários", "meetings_completed_by_user": {}, "total_completed": 0, "period_days": days}
        
        users = users_response.get('_embedded', {}).get('users', [])
        users_dict = {user['id']: user['name'] for user in users}
        
        # Buscar tarefas do tipo reunião concluídas
        params = {
            'filter[task_type]': 2,  # Tipo reunião
            'filter[is_completed]': 1,  # Concluídas
            'limit': 250
        }
        
        tasks_response = api.get_tasks(params)
        meetings_by_user = {}
        
        if tasks_response and '_embedded' in tasks_response:
            tasks = tasks_response.get('_embedded', {}).get('tasks', [])
            
            # Filtrar por período se necessário
            cutoff_date = datetime.now() - timedelta(days=days)
            
            for task in tasks:
                # Verificar se a tarefa está no período desejado
                if task.get('completed_at'):
                    completed_date = datetime.fromtimestamp(task['completed_at'])
                    if completed_date < cutoff_date:
                        continue
                
                user_id = task.get('responsible_user_id')
                if user_id:
                    user_name = users_dict.get(user_id, f"Usuário {user_id}")
                    meetings_by_user[user_name] = meetings_by_user.get(user_name, 0) + 1
        
        return {
            "meetings_completed_by_user": meetings_by_user,
            "total_completed": sum(meetings_by_user.values()),
            "period_days": days
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao buscar reuniões realizadas: {str(e)}")