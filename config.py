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
    FACEBOOK_APP_ID: str = os.getenv("FACEBOOK_APP_ID", "")
    FACEBOOK_APP_SECRET: str = os.getenv("FACEBOOK_APP_SECRET", "")
    FACEBOOK_API_VERSION: str = os.getenv("FACEBOOK_API_VERSION", "v23.0")
    DEFAULT_FACEBOOK_AD_ACCOUNT: str = os.getenv("DEFAULT_FACEBOOK_AD_ACCOUNT", "act_1051414772388438")
    
    # Configurações da aplicação
    DEBUG: bool = True
    
    # Configurações do Redis Cache
    REDIS_URL: str = os.getenv("REDIS_URL", "redis://default:HqdjcMSpTbf0WgU1mzP583cCESRqTB4hjo4mMqudnYBhMIo1xDvV5rTsIZfHVjsl@167.88.39.225:3457/0")
    CACHE_TTL: int = int(os.getenv("CACHE_TTL", "600"))  # 10 minutos
    
    # Configurações do MongoDB
    MONGODB_URL: str = os.getenv("MONGODB_URL", "mongodb://root:YZFA3AmTjK0oZKQaGa5UtOrLI6wD1YSa78K3aDLLCv6LwFAMiDRGXtMlovjmrM1y@167.88.39.225:2342/?directConnection=true")
    MONGODB_DATABASE: str = os.getenv("MONGODB_DATABASE", "saimobc_facebook")
    
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
REDIS_URL = settings.REDIS_URL
CACHE_TTL = settings.CACHE_TTL
FACEBOOK_ACCESS_TOKEN = settings.FACEBOOK_ACCESS_TOKEN
FACEBOOK_APP_ID = settings.FACEBOOK_APP_ID
FACEBOOK_APP_SECRET = settings.FACEBOOK_APP_SECRET
FACEBOOK_API_VERSION = settings.FACEBOOK_API_VERSION
DEFAULT_FACEBOOK_AD_ACCOUNT = settings.DEFAULT_FACEBOOK_AD_ACCOUNT
MONGODB_URL = settings.MONGODB_URL
MONGODB_DATABASE = settings.MONGODB_DATABASE