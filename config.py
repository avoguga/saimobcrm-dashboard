import os
from dotenv import load_dotenv
from pydantic_settings import BaseSettings

# Carrega as variáveis do arquivo .env
load_dotenv()

class Settings(BaseSettings):
    # Configurações da API Kommo
    KOMMO_SUBDOMAIN: str = os.getenv("KOMMO_SUBDOMAIN", "")
    KOMMO_TOKEN: str = os.getenv("KOMMO_TOKEN", "")
    KOMMO_API_URL: str = ""
    
    # Configurações da API Facebook
    FACEBOOK_ACCESS_TOKEN: str = os.getenv("FACEBOOK_ACCESS_TOKEN", "")
    FACEBOOK_API_VERSION: str = os.getenv("FACEBOOK_API_VERSION", "v23.0")
    FACEBOOK_AD_ACCOUNT_ID: str = os.getenv("FACEBOOK_AD_ACCOUNT_ID", "act_1051414772388438")
    
    # Configurações da aplicação
    DEBUG: bool = True
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        if self.KOMMO_SUBDOMAIN:
            self.KOMMO_API_URL = f"https://{self.KOMMO_SUBDOMAIN}.kommo.com/api/v4"

settings = Settings()

# Retrocompatibilidade
KOMMO_SUBDOMAIN = settings.KOMMO_SUBDOMAIN
KOMMO_TOKEN = settings.KOMMO_TOKEN
KOMMO_API_URL = settings.KOMMO_API_URL
DEBUG = settings.DEBUG