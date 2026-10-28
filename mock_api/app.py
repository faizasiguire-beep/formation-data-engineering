"""API "Référentiel marchands" de NexaPay — simulateur réaliste pour le TP1.

Reproduit volontairement les contraintes d'une vraie API métier :
  * Authentification OAuth2 (flux client_credentials) avec jeton à durée de vie courte (120 s)
  * Pagination (page / page_size, plafonnée à 100)
  * Limite de débit (HTTP 429 + en-tête Retry-After)
  * Pannes aléatoires (HTTP 503) — paramétrables via NEXA_API_CHAOS_RATE
  * Filtre incrémental `updated_since`

Lancement (depuis la racine du projet, environnement activé) :
    uvicorn mock_api.app:app --port 8000
Documentation interactive : http://127.0.0.1:8000/docs
"""
from __future__ import annotations

import json
import os
import random
import secrets
import time
from collections import defaultdict, deque
from datetime import datetime

from fastapi import Depends, FastAPI, Form, Header, HTTPException, Query
from fastapi.responses import JSONResponse

from fil_rouge.config import SOURCES_DIR

CLIENT_ID = os.getenv("NEXA_API_CLIENT_ID", "etudiant-de")
CLIENT_SECRET = os.getenv("NEXA_API_CLIENT_SECRET", "change-me-s3cret")
CHAOS_RATE = float(os.getenv("NEXA_API_CHAOS_RATE", "0.08"))
RATE_LIMIT = int(os.getenv("NEXA_API_RATE_LIMIT", "8"))      # requêtes par fenêtre
RATE_WINDOW_S = 2.0
TOKEN_TTL_S = 120

app = FastAPI(title="NexaPay — API Référentiel marchands (simulée)", version="1.0")

_tokens: dict[str, float] = {}                       # token -> expiration (epoch)
_calls: dict[str, deque] = defaultdict(deque)       # token -> horodatages des appels


def _load_merchants() -> list[dict]:
    path = SOURCES_DIR / "api" / "merchants.json"
    if not path.exists():
        raise RuntimeError("merchants.json introuvable : lancez `python -m fil_rouge.generate_data`")
    return json.loads(path.read_text(encoding="utf-8"))


MERCHANTS = _load_merchants()


@app.get("/health")
def health() -> dict:
    return {"status": "ok", "merchants": len(MERCHANTS), "time": datetime.utcnow().isoformat()}


@app.post("/oauth/token")
def token(grant_type: str = Form(...), client_id: str = Form(...), client_secret: str = Form(...)) -> dict:
    if grant_type != "client_credentials":
        raise HTTPException(400, detail={"error": "unsupported_grant_type"})
    if not (secrets.compare_digest(client_id, CLIENT_ID) and secrets.compare_digest(client_secret, CLIENT_SECRET)):
        raise HTTPException(401, detail={"error": "invalid_client"})
    tok = secrets.token_urlsafe(32)
    _tokens[tok] = time.time() + TOKEN_TTL_S
    return {"access_token": tok, "token_type": "Bearer", "expires_in": TOKEN_TTL_S, "scope": "merchants:read"}


def _auth(authorization: str | None = Header(default=None)) -> str:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(401, detail={"error": "missing_token"}, headers={"WWW-Authenticate": "Bearer"})
    tok = authorization.removeprefix("Bearer ").strip()
    exp = _tokens.get(tok)
    if exp is None or exp < time.time():
        raise HTTPException(401, detail={"error": "invalid_or_expired_token"}, headers={"WWW-Authenticate": "Bearer"})
    return tok


def _rate_limit(tok: str = Depends(_auth)) -> str:
    now = time.time()
    window = _calls[tok]
    while window and now - window[0] > RATE_WINDOW_S:
        window.popleft()
    if len(window) >= RATE_LIMIT:
        retry_after = max(1, int(RATE_WINDOW_S - (now - window[0])) + 1)
        raise HTTPException(429, detail={"error": "rate_limited"}, headers={"Retry-After": str(retry_after)})
    window.append(now)
    return tok


@app.get("/v1/merchants")
def list_merchants(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=100),
    updated_since: datetime | None = Query(None, description="ISO 8601 — extraction incrémentale"),
    _tok: str = Depends(_rate_limit),
):
    if random.random() < CHAOS_RATE:
        return JSONResponse(status_code=503, content={"error": "service_unavailable"})
    items = MERCHANTS
    if updated_since is not None:
        ref = updated_since.replace(tzinfo=None)
        items = [m for m in items if datetime.fromisoformat(m["updated_at"]).replace(tzinfo=None) > ref]
    total = len(items)
    total_pages = max(1, -(-total // page_size))
    start = (page - 1) * page_size
    return {
        "data": items[start:start + page_size],
        "pagination": {
            "page": page,
            "page_size": page_size,
            "total_items": total,
            "total_pages": total_pages,
            "next_page": page + 1 if page < total_pages else None,
        },
    }


@app.get("/v1/merchants/{merchant_id}")
def get_merchant(merchant_id: str, _tok: str = Depends(_rate_limit)):
    for m in MERCHANTS:
        if m["merchant_id"] == merchant_id:
            return m
    raise HTTPException(404, detail={"error": "merchant_not_found"})
