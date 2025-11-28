from fastapi import APIRouter, HTTPException
from app.services.kommo_api import get_kommo_api

router = APIRouter(prefix="/users", tags=["Users"])
api = get_kommo_api()

@router.get("/")
async def get_all_users():
    """Retorna todos os usuários/corretores"""
    try:
        users_response = api.get_users()
        
        # Verificar se há erro na resposta
        if users_response.get('_error'):
            raise HTTPException(status_code=500, detail=f"Erro na API de usuários: {users_response.get('_error_message')}")
        
        # Extrair lista de usuários da resposta da API
        users = []
        if '_embedded' in users_response and 'users' in users_response['_embedded']:
            users = users_response['_embedded']['users']
        elif isinstance(users_response, list):
            users = users_response
        else:
            # Se não encontrar a estrutura esperada, retornar a resposta completa
            users = users_response
        
        return {"users": users}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/performance")
async def get_users_performance():
    """Retorna métricas de performance dos usuários"""
    try:
        users_response = api.get_users()
        leads_response = api.get_leads()
        
        # Verificar se há erro na resposta
        if users_response.get('_error'):
            raise HTTPException(status_code=500, detail=f"Erro na API de usuários: {users_response.get('_error_message')}")
        
        if leads_response.get('_error'):
            raise HTTPException(status_code=500, detail=f"Erro na API de leads: {leads_response.get('_error_message')}")
        
        # Extrair lista de usuários da resposta da API
        users = []
        if '_embedded' in users_response and 'users' in users_response['_embedded']:
            users = users_response['_embedded']['users']
        elif isinstance(users_response, list):
            users = users_response
        
        # Extrair lista de leads da resposta da API
        leads = []
        if '_embedded' in leads_response and 'leads' in leads_response['_embedded']:
            leads = leads_response['_embedded']['leads']
        elif isinstance(leads_response, list):
            leads = leads_response
        
        # Calcular métricas de performance por usuário
        performance_data = []
        
        for user in users:
            if not isinstance(user, dict):
                continue
                
            user_id = user.get('id')
            user_name = user.get('name', 'Unknown')
            
            # Contar leads por usuário
            user_leads = [lead for lead in leads if isinstance(lead, dict) and lead.get('responsible_user_id') == user_id]
            total_leads = len(user_leads)
            
            # Contar leads por status (assumindo que há um campo 'status_id' ou 'pipeline_id')
            qualified_leads = len([lead for lead in user_leads if lead.get('status_id') in [142, 143]])  # IDs de exemplo
            converted_leads = len([lead for lead in user_leads if lead.get('status_id') == 142])  # ID de exemplo
            
            # Calcular taxa de conversão
            conversion_rate = (converted_leads / total_leads * 100) if total_leads > 0 else 0
            
            performance_data.append({
                "user_id": user_id,
                "user_name": user_name,
                "total_leads": total_leads,
                "qualified_leads": qualified_leads,
                "converted_leads": converted_leads,
                "conversion_rate": round(conversion_rate, 2)
            })
        
        # Ordenar por total de leads (decrescente)
        performance_data.sort(key=lambda x: x['total_leads'], reverse=True)
        
        return {"performance": performance_data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))