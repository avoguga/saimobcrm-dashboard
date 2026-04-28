"""
Login mockado simples.

Credenciais aceitas (hardcoded):
  admin / saimob2026
  saimob / saimob

Retorna um token bearer fixo (sem JWT - apenas para liberar acesso ao
dashboard). O token nao expira; logout e tratado no frontend removendo do
localStorage.
"""

from fastapi import APIRouter, HTTPException, Header
from pydantic import BaseModel
from typing import Optional
import hashlib
import secrets

router = APIRouter()

# Lista de usuarios mockados. (usuario, senha em texto puro - login mock).
MOCK_USERS = {
    "dashsaimoveis": {"password": "S@imoveis2026", "name": "SA Imóveis", "role": "admin"},
}

# Token "secreto" para gerar tokens determinísticos por usuário (mock).
_TOKEN_SALT = "saimob-mock-auth-2026"


def _make_token(username: str) -> str:
    """Gera um token determinístico para o usuário (não é JWT, é mock)."""
    raw = f"{username}:{_TOKEN_SALT}".encode("utf-8")
    digest = hashlib.sha256(raw).hexdigest()
    return f"saimob.{digest[:48]}"


# Pré-calcula o índice token -> usuário para validação O(1).
_TOKEN_INDEX = {_make_token(u): u for u in MOCK_USERS}


class LoginRequest(BaseModel):
    username: str
    password: str


class LoginResponse(BaseModel):
    token: str
    user: dict


@router.post("/login", response_model=LoginResponse)
async def login(payload: LoginRequest):
    user = MOCK_USERS.get(payload.username.strip().lower())
    if not user or not secrets.compare_digest(user["password"], payload.password):
        raise HTTPException(status_code=401, detail="Usuario ou senha invalidos")

    token = _make_token(payload.username.strip().lower())
    return LoginResponse(
        token=token,
        user={
            "username": payload.username.strip().lower(),
            "name": user["name"],
            "role": user["role"],
        },
    )


@router.get("/me")
async def me(authorization: Optional[str] = Header(None)):
    """Valida token e devolve os dados do usuario."""
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="Token ausente")

    token = authorization.split(" ", 1)[1].strip()
    username = _TOKEN_INDEX.get(token)
    if not username:
        raise HTTPException(status_code=401, detail="Token invalido")

    user = MOCK_USERS[username]
    return {
        "username": username,
        "name": user["name"],
        "role": user["role"],
    }
