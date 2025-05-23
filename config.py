import os
from dotenv import load_dotenv

# Carrega as variáveis do arquivo .env
load_dotenv()

# Configurações da API Kommo
KOMMO_SUBDOMAIN = os.getenv("KOMMO_SUBDOMAIN")
KOMMO_TOKEN = os.getenv("KOMMO_TOKEN")
KOMMO_API_URL = f"https://{KOMMO_SUBDOMAIN}.kommo.com/api/v4"

# Configurações da aplicação
DEBUG = True