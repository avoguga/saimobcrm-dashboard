from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
from app.routers import leads, tags, pipelines, users, custom_fields, sources, dashboard, cache_admin, facebook
import config

app = FastAPI(
    title="Kommo Dashboard API",
    description="API para o dashboard de análise de dados do Kommo",
    version="1.0.0"
)

# Configuração de CORS para permitir acesso do frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Em produção, use apenas as origens necessárias
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Incluir rotas
app.include_router(leads.router)
app.include_router(tags.router)
app.include_router(pipelines.router)
app.include_router(users.router)
app.include_router(custom_fields.router)
app.include_router(sources.router)
app.include_router(dashboard.router, prefix="/dashboard", tags=["Dashboard Completo"])
app.include_router(cache_admin.router, tags=["Cache Admin"])
app.include_router(facebook.router, prefix="/facebook", tags=["Facebook Analytics"])

@app.on_event("startup")
async def startup_event():
    from app.services.scheduler import facebook_scheduler
    facebook_scheduler.start_scheduler()

@app.get("/", tags=["Root"])
async def root():
    return {"message": "Bem-vindo à API do Dashboard Kommo"}

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=config.DEBUG)