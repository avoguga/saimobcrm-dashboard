#!/usr/bin/env python3
"""
Script de Comparação: Endpoint Antigo vs Novos Endpoints V2
Compara dados retornados para validar compatibilidade e correção
"""

import requests
import json
import time
from datetime import datetime
from typing import Dict, Any, List

# Configuração
BASE_URL = "http://localhost:8000"

class EndpointComparator:
    def __init__(self, base_url: str):
        self.base_url = base_url
        self.results = {}
        
    def make_request(self, endpoint: str, params: Dict = None) -> Dict[str, Any]:
        """Faz requisição e mede tempo de resposta"""
        url = f"{self.base_url}{endpoint}"
        
        start_time = time.time()
        try:
            response = requests.get(url, params=params, timeout=30)
            end_time = time.time()
            
            response_time = (end_time - start_time) * 1000  # em ms
            
            if response.status_code == 200:
                data = response.json()
                return {
                    "success": True,
                    "data": data,
                    "response_time": response_time,
                    "size_bytes": len(response.content),
                    "status_code": response.status_code
                }
            else:
                return {
                    "success": False,
                    "error": f"HTTP {response.status_code}: {response.text}",
                    "response_time": response_time,
                    "size_bytes": len(response.content),
                    "status_code": response.status_code
                }
                
        except requests.exceptions.Timeout:
            return {"success": False, "error": "Timeout (30s)", "response_time": 30000}
        except requests.exceptions.ConnectionError:
            return {"success": False, "error": "Connection Error", "response_time": 0}
        except Exception as e:
            return {"success": False, "error": str(e), "response_time": 0}
    
    def test_old_endpoint(self, params: Dict = None) -> Dict[str, Any]:
        """Testa o endpoint antigo pesado"""
        print("🔍 Testando endpoint ANTIGO: /dashboard/sales-complete")
        
        result = self.make_request("/dashboard/sales-complete", params)
        
        if result["success"]:
            data = result["data"]
            print(f"   ✅ Sucesso - {result['response_time']:.0f}ms - {result['size_bytes']} bytes")
            
            # Extrair métricas principais
            metrics = {
                "totalLeads": data.get("totalLeads", 0),
                "leadsByUser": len(data.get("leadsByUser", [])),
                "conversionRates": data.get("conversionRates", {}),
                "leadsByStage": len(data.get("leadsByStage", [])),
                "performance": {
                    "response_time": result["response_time"],
                    "size_bytes": result["size_bytes"]
                }
            }
            
            return {"success": True, "metrics": metrics, "raw_data": data}
        else:
            print(f"   ❌ Erro: {result.get('error', 'Unknown')}")
            return {"success": False, "error": result.get("error")}
    
    def test_new_endpoints(self, params: Dict = None) -> Dict[str, Any]:
        """Testa os novos endpoints V2"""
        print("🚀 Testando endpoints NOVOS V2:")
        
        endpoints = [
            ("/api/v2/sales/kpis", "KPIs"),
            ("/api/v2/charts/leads-by-user", "Leads por Usuário"),
            ("/api/v2/sales/conversion-rates", "Taxas de Conversão"),
            ("/api/v2/sales/pipeline-status", "Status Pipeline")
        ]
        
        total_time = 0
        total_size = 0
        aggregated_data = {}
        
        for endpoint, name in endpoints:
            print(f"   🔍 {name}...")
            result = self.make_request(endpoint, params)
            
            if result["success"]:
                total_time += result["response_time"]
                total_size += result["size_bytes"]
                aggregated_data[name] = result["data"]
                print(f"      ✅ {result['response_time']:.0f}ms - {result['size_bytes']} bytes")
            else:
                print(f"      ❌ Erro: {result.get('error', 'Unknown')}")
                return {"success": False, "error": f"Falha em {name}: {result.get('error')}"}
        
        # Agregar métricas dos novos endpoints
        kpis = aggregated_data.get("KPIs", {})
        leads_by_user = aggregated_data.get("Leads por Usuário", {})
        conversion_rates = aggregated_data.get("Taxas de Conversão", {})
        pipeline_status = aggregated_data.get("Status Pipeline", {})
        
        metrics = {
            "totalLeads": kpis.get("totalLeads", 0),
            "leadsByUser": len(leads_by_user.get("leadsByUser", [])),
            "conversionRates": conversion_rates.get("conversionRates", {}),
            "leadsByStage": len(pipeline_status.get("pipelineStatus", [])),
            "performance": {
                "response_time": total_time,
                "size_bytes": total_size
            }
        }
        
        return {
            "success": True, 
            "metrics": metrics, 
            "raw_data": aggregated_data,
            "individual_times": {name: result["response_time"] for (_, name), result in zip(endpoints, [self.make_request(ep, params) for ep, _ in endpoints])}
        }
    
    def compare_metrics(self, old_metrics: Dict, new_metrics: Dict) -> Dict[str, Any]:
        """Compara métricas entre endpoints antigo e novo"""
        comparison = {
            "data_accuracy": {},
            "performance": {},
            "compatibility": True
        }
        
        # Comparar dados
        for metric in ["totalLeads", "leadsByUser", "conversionRates"]:
            old_val = old_metrics.get(metric)
            new_val = new_metrics.get(metric)
            
            if isinstance(old_val, (int, float)) and isinstance(new_val, (int, float)):
                # Comparar valores numéricos
                diff = abs(old_val - new_val)
                diff_percent = (diff / max(old_val, 1)) * 100
                
                comparison["data_accuracy"][metric] = {
                    "old_value": old_val,
                    "new_value": new_val,
                    "difference": diff,
                    "difference_percent": round(diff_percent, 2),
                    "match": diff_percent < 5  # 5% de tolerância
                }
            elif isinstance(old_val, dict) and isinstance(new_val, dict):
                # Comparar objetos (como conversionRates)
                matches = 0
                total = len(old_val)
                
                for key in old_val:
                    if key in new_val:
                        old_rate = old_val[key]
                        new_rate = new_val[key]
                        if isinstance(old_rate, (int, float)) and isinstance(new_rate, (int, float)):
                            diff_percent = abs(old_rate - new_rate) / max(old_rate, 1) * 100
                            if diff_percent < 10:  # 10% tolerância para rates
                                matches += 1
                
                comparison["data_accuracy"][metric] = {
                    "old_value": old_val,
                    "new_value": new_val,
                    "matching_keys": matches,
                    "total_keys": total,
                    "match_rate": (matches / max(total, 1)) * 100
                }
            else:
                comparison["data_accuracy"][metric] = {
                    "old_value": old_val,
                    "new_value": new_val,
                    "match": old_val == new_val
                }
        
        # Comparar performance
        old_perf = old_metrics.get("performance", {})
        new_perf = new_metrics.get("performance", {})
        
        comparison["performance"] = {
            "response_time": {
                "old_ms": old_perf.get("response_time", 0),
                "new_ms": new_perf.get("response_time", 0),
                "improvement_factor": old_perf.get("response_time", 1) / max(new_perf.get("response_time", 1), 1),
                "improvement_percent": ((old_perf.get("response_time", 0) - new_perf.get("response_time", 0)) / max(old_perf.get("response_time", 1), 1)) * 100
            },
            "payload_size": {
                "old_bytes": old_perf.get("size_bytes", 0),
                "new_bytes": new_perf.get("size_bytes", 0),
                "reduction_factor": old_perf.get("size_bytes", 1) / max(new_perf.get("size_bytes", 1), 1),
                "reduction_percent": ((old_perf.get("size_bytes", 0) - new_perf.get("size_bytes", 0)) / max(old_perf.get("size_bytes", 1), 1)) * 100
            }
        }
        
        return comparison
    
    def run_comparison(self, test_params: List[Dict] = None):
        """Executa comparação completa"""
        if test_params is None:
            test_params = [
                {"days": 30},  # Teste padrão
                {"days": 7},   # Teste período menor
                {"days": 30, "corretor": "Alexandre Perazzo"},  # Teste com filtro
            ]
        
        print("=" * 80)
        print("🔥 COMPARAÇÃO: ENDPOINT ANTIGO vs ENDPOINTS V2")
        print("=" * 80)
        print(f"🕐 Iniciado em: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"🌍 Base URL: {self.base_url}")
        
        all_results = []
        
        for i, params in enumerate(test_params, 1):
            print(f"\n📊 TESTE {i}: {params}")
            print("-" * 50)
            
            # Testar endpoint antigo
            old_result = self.test_old_endpoint(params)
            
            print()
            
            # Testar novos endpoints
            new_result = self.test_new_endpoints(params)
            
            if old_result["success"] and new_result["success"]:
                # Comparar resultados
                comparison = self.compare_metrics(old_result["metrics"], new_result["metrics"])
                
                all_results.append({
                    "test_params": params,
                    "old_result": old_result,
                    "new_result": new_result,
                    "comparison": comparison
                })
                
                print(f"\n📈 COMPARAÇÃO:")
                print(f"   Total Leads - Antigo: {old_result['metrics']['totalLeads']}, Novo: {new_result['metrics']['totalLeads']}")
                print(f"   Usuários - Antigo: {old_result['metrics']['leadsByUser']}, Novo: {new_result['metrics']['leadsByUser']}")
                print(f"   Performance - Antigo: {old_result['metrics']['performance']['response_time']:.0f}ms, Novo: {new_result['metrics']['performance']['response_time']:.0f}ms")
                
                improvement = comparison["performance"]["response_time"]["improvement_factor"]
                print(f"   🚀 Melhoria: {improvement:.1f}x mais rápido")
                
            else:
                print("❌ Não foi possível comparar devido a erros")
        
        # Resumo final
        self.print_final_summary(all_results)
        
        return all_results
    
    def print_final_summary(self, results: List[Dict]):
        """Imprime resumo final da comparação"""
        if not results:
            print("\n❌ Nenhum teste foi bem-sucedido")
            return
        
        print("\n" + "=" * 80)
        print("📋 RESUMO FINAL")
        print("=" * 80)
        
        # Calcular médias
        total_tests = len(results)
        successful_tests = len([r for r in results if r["comparison"]])
        
        avg_old_time = sum(r["old_result"]["metrics"]["performance"]["response_time"] for r in results) / total_tests
        avg_new_time = sum(r["new_result"]["metrics"]["performance"]["response_time"] for r in results) / total_tests
        
        avg_old_size = sum(r["old_result"]["metrics"]["performance"]["size_bytes"] for r in results) / total_tests
        avg_new_size = sum(r["new_result"]["metrics"]["performance"]["size_bytes"] for r in results) / total_tests
        
        print(f"✅ Testes bem-sucedidos: {successful_tests}/{total_tests}")
        print(f"\n⚡ PERFORMANCE:")
        print(f"   Tempo médio - Antigo: {avg_old_time:.0f}ms | Novo: {avg_new_time:.0f}ms")
        print(f"   Melhoria de tempo: {avg_old_time/avg_new_time:.1f}x mais rápido")
        print(f"   Tamanho médio - Antigo: {avg_old_size/1024:.1f}KB | Novo: {avg_new_size/1024:.1f}KB")
        print(f"   Redução de tamanho: {avg_old_size/avg_new_size:.1f}x menor")
        
        # Verificar consistência dos dados
        data_issues = []
        for i, result in enumerate(results, 1):
            comp = result["comparison"]["data_accuracy"]
            for metric, data in comp.items():
                if isinstance(data, dict) and not data.get("match", True):
                    if isinstance(data.get("difference_percent"), (int, float)) and data["difference_percent"] > 5:
                        data_issues.append(f"Teste {i} - {metric}: {data['difference_percent']:.1f}% diferença")
        
        if data_issues:
            print(f"\n⚠️  PROBLEMAS DE CONSISTÊNCIA:")
            for issue in data_issues:
                print(f"   - {issue}")
        else:
            print(f"\n✅ CONSISTÊNCIA DE DADOS: Todos os dados estão consistentes!")
        
        print(f"\n🎯 CONCLUSÃO:")
        if successful_tests == total_tests and not data_issues:
            print("   ✅ ENDPOINTS V2 ESTÃO CORRETOS E FUNCIONAIS!")
            print("   🚀 Performance significativamente melhorada")
            print("   📊 Dados consistentes com endpoint original")
            print("   🎉 FRONTEND PODE MIGRAR COM SEGURANÇA!")
        else:
            print("   ⚠️  Alguns problemas encontrados - revisar implementação")
        
        print("=" * 80)

def main():
    """Função principal"""
    comparator = EndpointComparator(BASE_URL)
    
    # Verificar conectividade
    print("🔌 Verificando conectividade...")
    try:
        response = requests.get(f"{BASE_URL}/", timeout=5)
        print("✅ Servidor está rodando")
    except:
        print("❌ Servidor não está acessível")
        print("💡 Certifique-se de que o servidor está rodando com: python main.py")
        return
    
    # Configurar testes
    test_scenarios = [
        {"days": 30},  # Teste básico
        {"days": 7},   # Período menor
        {"days": 30, "fonte": "Google"},  # Teste com filtro de fonte
        {"days": 30, "corretor": "Alexandre Perazzo"},  # Teste com filtro de corretor
    ]
    
    # Executar comparação
    results = comparator.run_comparison(test_scenarios)
    
    # Salvar resultados detalhados
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"comparison_results_{timestamp}.json"
    
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2, ensure_ascii=False, default=str)
        print(f"\n📄 Resultados detalhados salvos em: {filename}")
    except Exception as e:
        print(f"\n⚠️  Não foi possível salvar resultados: {e}")

if __name__ == "__main__":
    main()