#!/usr/bin/env python3
"""
Script de teste para verificar o fluxo de dados do dashboard
Analisa passo a passo como os dados são coletados e processados
"""

import json
import logging
from datetime import datetime
from app.services.kommo_api import KommoAPI

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# IDs importantes
STATUS_PROPOSTA = 80689735
STATUS_CONTRATO_ASSINADO = 80689759
STATUS_VENDA_FINAL = 142
PIPELINE_VENDAS = 10516987
CUSTOM_FIELD_DATA_FECHAMENTO = 858126
CUSTOM_FIELD_FONTE = 837886
CUSTOM_FIELD_CORRETOR = 837920

def test_dashboard_flow():
    """Testa o fluxo completo do dashboard"""
    
    # Inicializar API
    api = KommoAPI()
    
    # Estrutura para armazenar logs de análise
    analysis_log = {
        "timestamp": datetime.now().isoformat(),
        "leads": {
            "total_fetched": 0,
            "from_sales_pipeline": 0,
            "with_corretor": 0,
            "without_corretor": 0,
            "by_status": {}
        },
        "tasks": {
            "total_fetched": 0,
            "completed_meetings": 0,
            "linked_to_leads": 0,
            "from_sales_pipeline": 0
        },
        "results": {
            "reunioes": [],
            "propostas": [],
            "vendas": []
        },
        "errors": [],
        "debug_samples": {
            "sample_lead": None,
            "sample_task": None,
            "sample_custom_fields": []
        }
    }
    
    try:
        # PASSO 1: Buscar todos os leads
        logger.info("=== PASSO 1: Buscando todos os leads ===")
        leads_params = {
            "limit": 250,
            "with": "contacts,tags,custom_fields_values"
        }
        
        leads_response = api.get_leads(leads_params)
        
        if not leads_response or '_embedded' not in leads_response:
            analysis_log["errors"].append("Nenhum lead encontrado na resposta da API")
            logger.error("Nenhum lead encontrado")
        else:
            all_leads = leads_response['_embedded'].get('leads', [])
            analysis_log["leads"]["total_fetched"] = len(all_leads)
            logger.info(f"Total de leads encontrados: {len(all_leads)}")
            
            # Analisar leads
            for i, lead in enumerate(all_leads):
                if not lead:
                    continue
                    
                # Salvar um exemplo de lead
                if i == 0:
                    analysis_log["debug_samples"]["sample_lead"] = {
                        "id": lead.get("id"),
                        "name": lead.get("name"),
                        "pipeline_id": lead.get("pipeline_id"),
                        "status_id": lead.get("status_id"),
                        "has_custom_fields": bool(lead.get("custom_fields_values"))
                    }
                
                # Verificar pipeline
                pipeline_id = lead.get('pipeline_id')
                if pipeline_id == PIPELINE_VENDAS:
                    analysis_log["leads"]["from_sales_pipeline"] += 1
                
                # Contar por status
                status_id = lead.get('status_id')
                status_key = str(status_id)
                analysis_log["leads"]["by_status"][status_key] = analysis_log["leads"]["by_status"].get(status_key, 0) + 1
                
                # Analisar custom fields
                custom_fields = lead.get("custom_fields_values", [])
                corretor_found = False
                fonte_found = False
                data_fechamento_found = False
                
                if custom_fields:
                    for field in custom_fields:
                        if not field:
                            continue
                        field_id = field.get("field_id")
                        values = field.get("values", [])
                        
                        if field_id == CUSTOM_FIELD_CORRETOR and values:
                            corretor_found = True
                            if i < 3:  # Primeiros 3 exemplos
                                analysis_log["debug_samples"]["sample_custom_fields"].append({
                                    "lead_id": lead.get("id"),
                                    "field": "corretor",
                                    "value": values[0].get("value")
                                })
                        elif field_id == CUSTOM_FIELD_FONTE and values:
                            fonte_found = True
                        elif field_id == CUSTOM_FIELD_DATA_FECHAMENTO and values:
                            data_fechamento_found = True
                
                if corretor_found:
                    analysis_log["leads"]["with_corretor"] += 1
                else:
                    analysis_log["leads"]["without_corretor"] += 1
        
        # PASSO 2: Buscar tarefas de reunião
        logger.info("\n=== PASSO 2: Buscando tarefas de reunião ===")
        tasks_params = {
            'filter[task_type]': 2,  # Tipo reunião
            'filter[is_completed]': 1,  # Apenas concluídas
            'limit': 250
        }
        
        tasks_response = api.get_tasks(tasks_params)
        
        if not tasks_response or '_embedded' not in tasks_response:
            analysis_log["errors"].append("Nenhuma tarefa encontrada na resposta da API")
            logger.error("Nenhuma tarefa encontrada")
        else:
            all_tasks = tasks_response['_embedded'].get('tasks', [])
            analysis_log["tasks"]["total_fetched"] = len(all_tasks)
            analysis_log["tasks"]["completed_meetings"] = len(all_tasks)
            logger.info(f"Total de tarefas de reunião concluídas: {len(all_tasks)}")
            
            # Salvar exemplo de tarefa
            if all_tasks:
                analysis_log["debug_samples"]["sample_task"] = {
                    "id": all_tasks[0].get("id"),
                    "entity_type": all_tasks[0].get("entity_type"),
                    "entity_id": all_tasks[0].get("entity_id"),
                    "is_completed": all_tasks[0].get("is_completed"),
                    "task_type_id": all_tasks[0].get("task_type_id"),
                    "complete_till": all_tasks[0].get("complete_till")
                }
            
            # Criar mapa de leads
            leads_map = {lead.get("id"): lead for lead in all_leads if lead}
            
            # Processar tarefas
            for task in all_tasks:
                if not task or task.get('entity_type') != 'leads':
                    continue
                
                lead_id = task.get('entity_id')
                lead = leads_map.get(lead_id)
                
                if lead:
                    analysis_log["tasks"]["linked_to_leads"] += 1
                    
                    if lead.get('pipeline_id') == PIPELINE_VENDAS:
                        analysis_log["tasks"]["from_sales_pipeline"] += 1
        
        # PASSO 3: Contar resultados finais esperados
        logger.info("\n=== PASSO 3: Analisando resultados esperados ===")
        
        # Reuniões esperadas
        logger.info(f"Reuniões esperadas (tarefas tipo 2 concluídas do Funil de Vendas): {analysis_log['tasks']['from_sales_pipeline']}")
        
        # Propostas esperadas
        propostas_count = 0
        vendas_count = 0
        
        for lead in all_leads:
            if not lead or lead.get('pipeline_id') != PIPELINE_VENDAS:
                continue
                
            status_id = lead.get('status_id')
            
            if status_id == STATUS_PROPOSTA:
                propostas_count += 1
            elif status_id in [STATUS_CONTRATO_ASSINADO, STATUS_VENDA_FINAL]:
                vendas_count += 1
        
        logger.info(f"Propostas esperadas (status {STATUS_PROPOSTA} no Funil de Vendas): {propostas_count}")
        logger.info(f"Vendas esperadas (status {STATUS_CONTRATO_ASSINADO} ou {STATUS_VENDA_FINAL} no Funil de Vendas): {vendas_count}")
        
        # Adicionar ao log
        analysis_log["results"]["expected_counts"] = {
            "reunioes": analysis_log['tasks']['from_sales_pipeline'],
            "propostas": propostas_count,
            "vendas": vendas_count
        }
        
        # PASSO 4: Análise de problemas potenciais
        logger.info("\n=== PASSO 4: Análise de problemas potenciais ===")
        
        if analysis_log["leads"]["total_fetched"] == 0:
            analysis_log["errors"].append("CRÍTICO: Nenhum lead foi retornado pela API")
        
        if analysis_log["leads"]["from_sales_pipeline"] == 0:
            analysis_log["errors"].append(f"CRÍTICO: Nenhum lead do Funil de Vendas (pipeline {PIPELINE_VENDAS})")
        
        if analysis_log["leads"]["with_corretor"] == 0:
            analysis_log["errors"].append("AVISO: Nenhum lead tem corretor definido no custom field")
        
        if analysis_log["tasks"]["total_fetched"] == 0:
            analysis_log["errors"].append("AVISO: Nenhuma tarefa de reunião concluída encontrada")
        
        # Listar todos os pipelines encontrados
        pipelines_found = {}
        for lead in all_leads:
            if lead:
                pipeline_id = lead.get('pipeline_id')
                if pipeline_id:
                    pipelines_found[pipeline_id] = pipelines_found.get(pipeline_id, 0) + 1
        
        analysis_log["debug_info"] = {
            "pipelines_found": pipelines_found,
            "status_distribution": analysis_log["leads"]["by_status"]
        }
        
    except Exception as e:
        logger.error(f"Erro durante execução: {str(e)}")
        analysis_log["errors"].append(f"Erro de execução: {str(e)}")
    
    # Salvar log de análise
    with open('dashboard_analysis_log.json', 'w', encoding='utf-8') as f:
        json.dump(analysis_log, f, indent=2, ensure_ascii=False)
    
    logger.info("\n=== ANÁLISE COMPLETA ===")
    logger.info(f"Log salvo em: dashboard_analysis_log.json")
    logger.info(f"Total de erros encontrados: {len(analysis_log['errors'])}")
    
    # Resumo final
    print("\n" + "="*50)
    print("RESUMO DA ANÁLISE")
    print("="*50)
    print(f"Leads totais: {analysis_log['leads']['total_fetched']}")
    print(f"Leads do Funil de Vendas: {analysis_log['leads']['from_sales_pipeline']}")
    print(f"Leads com corretor: {analysis_log['leads']['with_corretor']}")
    print(f"Tarefas de reunião concluídas: {analysis_log['tasks']['completed_meetings']}")
    print(f"Reuniões do Funil de Vendas: {analysis_log['tasks']['from_sales_pipeline']}")
    print(f"\nResultados esperados:")
    if "expected_counts" in analysis_log["results"]:
        print(f"  - Reuniões: {analysis_log['results']['expected_counts']['reunioes']}")
        print(f"  - Propostas: {analysis_log['results']['expected_counts']['propostas']}")
        print(f"  - Vendas: {analysis_log['results']['expected_counts']['vendas']}")
    print(f"\nErros encontrados: {len(analysis_log['errors'])}")
    if analysis_log['errors']:
        for error in analysis_log['errors']:
            print(f"  - {error}")

if __name__ == "__main__":
    test_dashboard_flow()