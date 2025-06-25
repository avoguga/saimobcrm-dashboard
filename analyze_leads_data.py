#!/usr/bin/env python3
import json
from collections import defaultdict, Counter

def analyze_leads_data():
    # Load the JSON data
    with open('/mnt/c/Users/avogu/Documents/saimobc/leads_para_claude_analisar.json', 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    leads = data['_embedded']['leads']
    print(f"Total leads found: {len(leads)}")
    print("=" * 50)
    
    # Expected status mappings
    expected_statuses = {
        142: "Closed - won / Venda ganha (sales)",
        80689759: "Contrato Assinado (sales)",
        80689735: "Proposta (proposals)",
        80689731: "Reunião Realizada (meetings)",
        80689727: "Agendamentos Futuros (meetings)",
        80689723: "Agendamento (meetings)",
        80689719: "Atendimento Realizado (meetings)"
    }
    
    # Expected pipeline mappings
    expected_pipelines = {
        10516987: "FUNIL DE VENDAS",
        10929979: "LEADS ANTIGOS",
        11059911: "REMARKETING",
        11145843: "PÓS-VENDA"
    }
    
    # Count by status_id
    status_counts = Counter()
    pipeline_counts = Counter()
    sales_leads = []
    
    for lead in leads:
        status_id = lead.get('status_id')
        pipeline_id = lead.get('pipeline_id')
        
        status_counts[status_id] += 1
        pipeline_counts[pipeline_id] += 1
        
        # Check for sales leads (status 142 or 80689759)
        if status_id in [142, 80689759]:
            sales_leads.append({
                'id': lead.get('id'),
                'name': lead.get('name'),
                'pipeline_id': pipeline_id,
                'status_id': status_id,
                'price': lead.get('price'),
                'created_at': lead.get('created_at'),
                'updated_at': lead.get('updated_at'),
                'closed_at': lead.get('closed_at'),
                'custom_fields': lead.get('custom_fields_values', [])
            })
    
    print("STATUS ID ANALYSIS:")
    print("-" * 30)
    print("Expected status IDs and their counts:")
    for status_id, description in expected_statuses.items():
        count = status_counts.get(status_id, 0)
        print(f"  Status {status_id} ({description}): {count}")
    
    print(f"\nActual status IDs found in data:")
    for status_id, count in status_counts.most_common():
        expected_desc = expected_statuses.get(status_id, "Unknown status")
        print(f"  Status {status_id}: {count} leads ({expected_desc})")
    
    print("\n" + "=" * 50)
    print("PIPELINE ID ANALYSIS:")
    print("-" * 30)
    print("Expected pipeline IDs and their counts:")
    for pipeline_id, description in expected_pipelines.items():
        count = pipeline_counts.get(pipeline_id, 0)
        print(f"  Pipeline {pipeline_id} ({description}): {count}")
    
    print(f"\nActual pipeline IDs found in data:")
    for pipeline_id, count in pipeline_counts.most_common():
        expected_desc = expected_pipelines.get(pipeline_id, "Unknown pipeline")
        print(f"  Pipeline {pipeline_id}: {count} leads ({expected_desc})")
    
    print("\n" + "=" * 50)
    print("SALES LEADS ANALYSIS (Status 142 or 80689759):")
    print("-" * 50)
    if sales_leads:
        print(f"Found {len(sales_leads)} sales leads:")
        for lead in sales_leads:
            print(f"\n  Lead ID: {lead['id']}")
            print(f"  Name: {lead['name']}")
            print(f"  Pipeline ID: {lead['pipeline_id']} ({expected_pipelines.get(lead['pipeline_id'], 'Unknown')})")
            print(f"  Status ID: {lead['status_id']} ({expected_statuses.get(lead['status_id'], 'Unknown')})")
            print(f"  Price: {lead['price']}")
            print(f"  Created: {lead['created_at']}")
            print(f"  Updated: {lead['updated_at']}")
            print(f"  Closed: {lead['closed_at']}")
            
            if lead['custom_fields']:
                print("  Custom Fields:")
                for field in lead['custom_fields']:
                    field_name = field.get('field_name', 'Unknown')
                    values = [v.get('value', '') for v in field.get('values', [])]
                    print(f"    {field_name}: {', '.join(str(v) for v in values)}")
    else:
        print("No sales leads found with status 142 or 80689759")
    
    print("\n" + "=" * 50)
    print("WHY DETAILED-TABLE ENDPOINT MIGHT BE MISSING SALES DATA:")
    print("-" * 50)
    
    # Analysis based on findings
    actual_statuses = set(status_counts.keys())
    expected_sales_statuses = {142, 80689759}
    expected_all_statuses = set(expected_statuses.keys())
    
    missing_sales_statuses = expected_sales_statuses - actual_statuses
    missing_all_statuses = expected_all_statuses - actual_statuses
    
    if missing_sales_statuses:
        print(f"1. MISSING SALES STATUS IDs: {missing_sales_statuses}")
        print("   - The data contains no leads with the expected sales status IDs")
        print("   - This suggests either:")
        print("     a) No sales have been completed yet")
        print("     b) The status IDs have changed in the CRM system")
        print("     c) Sales data is in a different pipeline or filtered out")
    
    if len(pipeline_counts) < len(expected_pipelines):
        missing_pipelines = set(expected_pipelines.keys()) - set(pipeline_counts.keys())
        print(f"\n2. MISSING PIPELINE DATA: {missing_pipelines}")
        print("   - Some expected pipelines are not present in this dataset")
        print("   - Sales might be tracked in different pipelines")
    
    print(f"\n3. DATA DISTRIBUTION:")
    print(f"   - Most leads ({status_counts[83825679]} out of {len(leads)}) have status_id 83825679")
    print(f"   - Most leads ({pipeline_counts[10929979]} out of {len(leads)}) are in pipeline 10929979 (LEADS ANTIGOS)")
    print("   - This suggests the dataset might be filtered to show only old/archived leads")
    
    print(f"\n4. RECOMMENDATIONS:")
    print("   - Check if the API call is filtering by pipeline or status")
    print("   - Verify the correct status IDs for sales in your CRM system")
    print("   - Consider fetching data from all pipelines")
    print("   - Check if there's a separate endpoint for closed/won deals")

if __name__ == "__main__":
    analyze_leads_data()