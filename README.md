# Kommo Dashboard API

API backend para integração com o Kommo CRM, desenvolvida com FastAPI para fornecer dados para dashboards de marketing e vendas.

## 📋 Descrição

Esta API fornece endpoints para acessar dados do Kommo CRM, permitindo a visualização de métricas importantes como:

- Número total de leads
- Leads por fonte (ex.: Olx, Tráfego Pago, Panfletagem)
- Leads por tags (ex.: Maceió / Milagres)
- Leads por etapa do funil de vendas
- Leads por corretor
- E outras métricas relevantes para dashboards de marketing e vendas

## 🚀 Instalação

### Pré-requisitos

- Python 3.8 ou superior
- pip (gerenciador de pacotes Python)

### Passos para instalação

1. Clone o repositório:
```bash
git clone https://github.com/avoguga/kommo-dashboard.git
cd kommo-dashboard
```

2. Crie e ative um ambiente virtual:
```bash
python -m venv venv
# No Windows
venv\Scripts\activate
# No Linux/MacOS
source venv/bin/activate
```

3. Instale as dependências:
```bash
pip install -r requirements.txt
```

4. Crie um arquivo `.env` na raiz do projeto e adicione suas credenciais:
```
KOMMO_SUBDOMAIN=seu-subdominio
KOMMO_TOKEN=seu-token-long-lived
```

## ⚙️ Configuração

1. Obtenha um token de longa duração (Long-lived Token) no Kommo:
   - Acesse sua conta Kommo
   - Vá para Settings → Integrations → Create Integration (ou use uma existente)
   - Na aba "Keys and scopes", clique em "Generate long-lived token"
   - Escolha o período de validade e copie o token gerado

2. Verifique se o token tem as permissões necessárias para acessar leads, pipelines, usuários e outras entidades do Kommo

## 🏃‍♂️ Executando o projeto

Execute o servidor de desenvolvimento:
```bash
python main.py
```

A API estará disponível em `http://localhost:8000`

A documentação interativa da API (Swagger) estará disponível em `http://localhost:8000/docs`

## 🔌 Principais Endpoints

### Leads

- `GET /leads/` - Lista de leads com paginação
- `GET /leads/count` - Número total de leads
- `GET /leads/by-source` - Leads agrupados por fonte
- `GET /leads/by-tag` - Leads agrupados por tag
- `GET /leads/by-user` - Leads agrupados por usuário responsável
- `GET /leads/by-stage` - Leads agrupados por etapa do pipeline

### Pipelines

- `GET /pipelines/` - Lista de todos os pipelines
- `GET /pipelines/{pipeline_id}/statuses` - Estágios de um pipeline específico

### Tags

- `GET /tags/` - Lista de todas as tags disponíveis

### Usuários

- `GET /users/` - Lista de todos os usuários/corretores

## 📝 Exemplos de uso

### Obter o número total de leads
```bash
curl -X 'GET' 'http://localhost:8000/leads/count' -H 'accept: application/json'
```

### Obter leads agrupados por fonte
```bash
curl -X 'GET' 'http://localhost:8000/leads/by-source' -H 'accept: application/json'
```

## 🔧 Tecnologias utilizadas

- [FastAPI](https://fastapi.tiangolo.com/) - Framework web moderno e rápido para construção de APIs
- [Uvicorn](https://www.uvicorn.org/) - Servidor ASGI de alta performance
- [Python-dotenv](https://github.com/theskumar/python-dotenv) - Carregamento de variáveis de ambiente
- [Requests](https://docs.python-requests.org/en/latest/) - Biblioteca para fazer requisições HTTP

## 📁 Estrutura do projeto

```
kommo-dashboard/
│
├── .env                   # Variáveis de ambiente (não versionado)
├── .gitignore             # Configuração de arquivos a serem ignorados pelo Git
├── main.py                # Arquivo principal da aplicação
├── config.py              # Configurações e carregamento das variáveis de ambiente
├── requirements.txt       # Dependências do projeto
│
└── app/
    ├── routers/
    │   ├── leads.py       # Rotas para operações com leads
    │   ├── tags.py        # Rotas para operações com tags
    │   ├── pipelines.py   # Rotas para pipelines e estágios
    │   └── users.py       # Rotas para usuários/corretores
    │
    └── services/
        └── kommo_api.py   # Serviço para consumir a API do Kommo
```

