import hashlib
import hmac
import os
import secrets
import sqlite3
import time
from contextlib import contextmanager
from pathlib import Path

from fastapi import APIRouter, HTTPException, Request, Response
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from abassurance.shared.request_limits import RequestLimits

COOKIE = "abassurance_session"
TTL = 8 * 3600

router = APIRouter(prefix="/auth", tags=["auth"])


@contextmanager
def database():
    path = Path(os.environ.get("AUTH_DB_PATH", "/var/lib/abassurance/auth.sqlite"))
    path.parent.mkdir(parents=True, exist_ok=True)

    db = sqlite3.connect(path, timeout=10)
    db.row_factory = sqlite3.Row

    try:
        db.executescript("""
            CREATE TABLE IF NOT EXISTS users (
                username TEXT PRIMARY KEY,
                password TEXT NOT NULL,
                role TEXT NOT NULL CHECK(role IN ('admin', 'user')),
                source TEXT,
                source_id TEXT,
                CHECK(role = 'admin' OR (
                    source IN ('abassurance', 'assureplus')
                    AND source_id IS NOT NULL
                ))
            );

            CREATE TABLE IF NOT EXISTS sessions (
                token TEXT PRIMARY KEY,
                username TEXT NOT NULL,
                expires REAL NOT NULL
            );

            CREATE TABLE IF NOT EXISTS attempts (
                key TEXT PRIMARY KEY,
                failures INTEGER NOT NULL,
                expires REAL NOT NULL
            );
        """)

        yield db
    finally:
        db.close()


def password_hash(password, salt=None):
    salt = salt or secrets.token_hex(16)

    digest = hashlib.scrypt(password.encode(), salt=bytes.fromhex(salt), n=16384, r=8, p=1).hex()

    return f"{salt}:{digest}"


DUMMY_PASSWORD = password_hash("invalid-account", "00" * 16)


def create_user(username, password, role, source=None, source_id=None):
    if role not in ("admin", "user") or not username.strip() or not 12 <= len(password) <= 1024:
        raise ValueError("Invalid account or password (minimum 12 characters).")

    if role == "user" and (source not in ("abassurance", "assureplus") or not source_id):
        raise ValueError("A user requires a source and client identifier.")

    with database() as db:
        db.execute(
            "INSERT INTO users VALUES (?, ?, ?, ?, ?)", (username, password_hash(password), role, source, source_id)
        )
        db.commit()


def public_user(row):
    return {key: row[key] for key in ("username", "role", "source", "source_id")}


def current_user(request):
    token = request.cookies.get(COOKIE, "")
    token_hash = hashlib.sha256(token.encode()).hexdigest()

    with database() as db:
        row = db.execute(
            """
            SELECT u.*
            FROM sessions s
            JOIN users u USING (username)
            WHERE s.token = ? AND s.expires > ?
            """,
            (token_hash, time.time()),
        ).fetchone()

    return public_user(row) if row else None


async def protect(request: Request, call_next):
    path = request.url.path

    if not hasattr(request.app.state, "request_limits"):
        request.app.state.request_limits = RequestLimits()
    limits: RequestLimits = request.app.state.request_limits

    if path != "/auth/status":
        # Do not trust arbitrary X-Forwarded-For headers supplied by callers.
        peer = request.client.host if request.client else "unknown"
        retry = limits.retry_after(f"peer:{peer}", 300)
        if not retry and path == "/auth/login":
            retry = limits.retry_after("login:global", 60) or limits.retry_after(f"login:{peer}", 20)
        if retry:
            return JSONResponse(
                status_code=429,
                content={"detail": "Too many requests. Try again later."},
                headers={"Retry-After": str(retry), "Cache-Control": "no-store"},
            )

    public = path in ("/auth/login", "/auth/me", "/auth/status")

    if request.method not in ("GET", "HEAD", "OPTIONS"):
        if request.headers.get("X-Requested-With") != "AbAssurance":
            return JSONResponse(status_code=403, content={"detail": "Unauthorized request."})

    if not public:
        user = current_user(request)

        if not user:
            return JSONResponse(status_code=401, content={"detail": "Authentication required."})

        request.state.user = user

        if user["role"] != "admin" and not (path.startswith("/me/data/") or path == "/auth/logout"):
            return JSONResponse(status_code=403, content={"detail": "Administrator access required."})
        if path != "/auth/logout":
            retry = limits.retry_after(f"account:{user['username']}", 120)
            if retry:
                return JSONResponse(
                    status_code=429,
                    content={"detail": "Too many requests. Try again later."},
                    headers={"Retry-After": str(retry), "Cache-Control": "no-store"},
                )

    response = await call_next(request)
    response.headers["Cache-Control"] = "no-store"

    return response


@router.get("/status", include_in_schema=False)
def status():
    return {"status": "ok"}


class Credentials(BaseModel):
    username: str = Field(min_length=1, max_length=100)
    password: str = Field(min_length=1, max_length=1024)


@router.post("/login", summary="Se connecter", description="Vérifie les identifiants et crée un cookie de session.")
def login(credentials: Credentials, request: Request, response: Response):
    now = time.time()

    client_host = request.client.host if request.client else "unknown"
    key = hashlib.sha256(f"{client_host}:{credentials.username}".encode()).hexdigest()

    with database() as db:
        db.execute("DELETE FROM attempts WHERE expires <= ?", (now,))

        attempt = db.execute("SELECT failures FROM attempts WHERE key = ?", (key,)).fetchone()

        if attempt and attempt["failures"] >= 10:
            raise HTTPException(status_code=429, detail="Too many login attempts. Try again in 15 minutes.")

        row = db.execute("SELECT * FROM users WHERE username = ?", (credentials.username,)).fetchone()

        encoded = row["password"] if row else DUMMY_PASSWORD
        salt = encoded.split(":")[0]

        valid = hmac.compare_digest(encoded, password_hash(credentials.password, salt))

        if not row or not valid:
            db.execute(
                """
                INSERT INTO attempts VALUES (?, 1, ?)
                ON CONFLICT(key) DO UPDATE SET failures = failures + 1
                """,
                (key, now + 900),
            )
            db.commit()

            raise HTTPException(status_code=401, detail="Invalid credentials.")

        db.execute("DELETE FROM attempts WHERE key = ?", (key,))

        previous_token = request.cookies.get(COOKIE, "")
        previous_hash = hashlib.sha256(previous_token.encode()).hexdigest()

        db.execute("DELETE FROM sessions WHERE token = ? OR expires <= ?", (previous_hash, now))

        token = secrets.token_urlsafe(32)
        token_hash = hashlib.sha256(token.encode()).hexdigest()

        db.execute("INSERT INTO sessions VALUES (?, ?, ?)", (token_hash, row["username"], now + TTL))
        db.commit()

    response.set_cookie(
        COOKIE,
        token,
        httponly=True,
        samesite="strict",
        secure=os.environ.get("AUTH_COOKIE_SECURE") == "true",
        max_age=TTL,
        path="/",
    )

    return public_user(row)


@router.get(
    "/me",
    summary="Voir sa session",
    description="Renvoie le compte connecté et son rôle. Répond 401 sans session valide.",
)
def me(request: Request):
    user = current_user(request)

    if not user:
        raise HTTPException(status_code=401, detail="Authentication required.")

    return user


@router.post("/logout", summary="Se déconnecter", description="Supprime la session et son cookie.")
def logout(request: Request, response: Response):
    token = request.cookies.get(COOKIE, "")
    token_hash = hashlib.sha256(token.encode()).hexdigest()

    with database() as db:
        db.execute("DELETE FROM sessions WHERE token = ?", (token_hash,))
        db.commit()

    response.delete_cookie(COOKIE, path="/")

    return {"ok": True}
