"""Client de l'API « Référentiel marchands » : OAuth2, pagination, 429, 503.

Repris du TP1 (version complétée en TP) : le budget de tentatives ne borne que les PANNES (5xx, réseau) ;
un 429 attend Retry-After sans consommer de tentative, avec un garde-fou de durée globale.
"""
from __future__ import annotations

import logging
import random
import time
from typing import Iterator

import requests

logger = logging.getLogger("projet.api")


class ApiError(RuntimeError):
    pass


class NexaApiClient:
    RETRYABLE = {500, 502, 503, 504}

    def __init__(self, base_url: str, client_id: str, client_secret: str,
                 timeout: tuple[float, float] = (3.05, 15.0), max_retries: int = 6, backoff_base: float = 0.5,
                 max_wait_s: float = 300.0):
        if not client_id or not client_secret:
            raise ValueError("Identifiants API absents : renseignez .env ou la Connection Airflow")
        self.base_url = base_url.rstrip("/")
        self._client_id, self._client_secret = client_id, client_secret
        self.timeout = timeout
        self.max_retries = max_retries
        self.backoff_base = backoff_base
        self.max_wait_s = max_wait_s
        self.session = requests.Session()
        self._token: str | None = None
        self._token_exp: float = 0.0
        self.stats = {"requests": 0, "retries": 0, "rate_limited": 0, "token_refresh": 0}

    def _fetch_token(self) -> None:
        resp = self.session.post(
            f"{self.base_url}/oauth/token",
            data={"grant_type": "client_credentials",
                  "client_id": self._client_id, "client_secret": self._client_secret},
            timeout=self.timeout,
        )
        if resp.status_code != 200:
            raise ApiError(f"Échec d'authentification OAuth2 (HTTP {resp.status_code})")   # jamais le secret
        payload = resp.json()
        self._token = payload["access_token"]
        ttl = float(payload.get("expires_in", 60))
        self._token_exp = time.time() + ttl - max(5.0, ttl * 0.1)
        self.stats["token_refresh"] += 1

    def _auth_header(self) -> dict[str, str]:
        if self._token is None or time.time() >= self._token_exp:
            self._fetch_token()
        return {"Authorization": f"Bearer {self._token}"}

    def _backoff(self, attempt: int) -> float:
        return min(30, self.backoff_base * 2 ** attempt) + random.uniform(0, 0.5)

    def get(self, path: str, params: dict | None = None) -> dict:
        url = f"{self.base_url}{path}"
        attempt = 0
        reauth_done = False
        deadline = time.monotonic() + self.max_wait_s
        while attempt <= self.max_retries:
            if time.monotonic() >= deadline:
                raise ApiError(f"GET {path} : durée maximale de {self.max_wait_s:.0f} s dépassée")
            self.stats["requests"] += 1
            try:
                resp = self.session.get(url, params=params, headers=self._auth_header(), timeout=self.timeout)
            except (requests.ConnectionError, requests.Timeout) as exc:
                if attempt == self.max_retries:
                    raise ApiError(f"GET {path} : réseau indisponible après {attempt + 1} tentatives") from exc
                delay = self._backoff(attempt)
                logger.warning("GET %s : %s, nouvel essai dans %.1f s", path, type(exc).__name__, delay)
                self.stats["retries"] += 1
                attempt += 1
                time.sleep(delay)
                continue
            if resp.status_code == 200:
                return resp.json()
            if resp.status_code == 401 and not reauth_done:
                self._token, reauth_done = None, True
                continue
            if resp.status_code == 429:
                wait = float(resp.headers.get("Retry-After", 1))
                self.stats["rate_limited"] += 1
                if time.monotonic() + wait > deadline:
                    raise ApiError(f"GET {path} : quota (429) toujours atteint après {self.max_wait_s:.0f} s")
                logger.warning("GET %s : 429 (quota), attente de %.0f s", path, wait)
                time.sleep(wait)
                continue
            if resp.status_code in self.RETRYABLE:
                if attempt == self.max_retries:
                    break
                delay = self._backoff(attempt)
                logger.warning("GET %s : %d, nouvel essai dans %.1f s", path, resp.status_code, delay)
                self.stats["retries"] += 1
                attempt += 1
                time.sleep(delay)
                continue
            raise ApiError(f"GET {path} : HTTP {resp.status_code} non récupérable")
        raise ApiError(f"GET {path} : échec après {self.max_retries + 1} tentatives")

    def iter_pages(self, path: str, params: dict | None = None, page_size: int = 100) -> Iterator[list[dict]]:
        page: int | None = 1
        while page is not None:
            body = self.get(path, {**(params or {}), "page": page, "page_size": page_size})
            yield body.get("data", [])
            page = body.get("pagination", {}).get("next_page")
