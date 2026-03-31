"""
auth.py — Google OAuth (combined login + gmail.send) + decorators

Single OAuth 2.0 flow:
  Scopes: openid  email  https://www.googleapis.com/auth/gmail.send
  Callback: GET /auth/callback
  On success: store token in Supabase settings, set session['user']

Decorators:
  @login_required  — redirect /login if no session
  @csrf_protect    — 403 if csrf_token mismatch on POST
"""

import os
import json
import secrets
import functools
from urllib.parse import urlencode

import requests
from flask import (
    Blueprint, redirect, request, session, url_for,
    render_template, abort, current_app,
)

import db

auth_bp = Blueprint("auth", __name__)

GOOGLE_AUTH_URL = "https://accounts.google.com/o/oauth2/v2/auth"
GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"
SCOPES = [
    "openid",
    "email",
    "profile",
    "https://www.googleapis.com/auth/gmail.send",
    "https://www.googleapis.com/auth/gmail.readonly",
]

# ── Decorators ─────────────────────────────────────────────────────────────────

def login_required(f):
    @functools.wraps(f)
    def decorated(*args, **kwargs):
        if not session.get("user"):
            return redirect(url_for("auth.login"))
        return f(*args, **kwargs)
    return decorated

def csrf_protect(f):
    @functools.wraps(f)
    def decorated(*args, **kwargs):
        if request.method == "POST":
            token = request.form.get("csrf_token")
            if not token or token != session.get("csrf_token"):
                abort(403)
        return f(*args, **kwargs)
    return decorated

def get_csrf_token() -> str:
    if "csrf_token" not in session:
        session["csrf_token"] = secrets.token_hex(32)
    return session["csrf_token"]

# ── Routes ─────────────────────────────────────────────────────────────────────

@auth_bp.route("/login")
def login():
    if session.get("user"):
        return redirect(url_for("index"))
    return render_template("login.html", csrf_token=get_csrf_token())

@auth_bp.route("/auth/google")
def google_auth():
    state = secrets.token_hex(16)
    session["oauth_state"] = state
    params = {
        "client_id": os.environ["GOOGLE_CLIENT_ID"],
        "redirect_uri": _callback_url(),
        "response_type": "code",
        "scope": " ".join(SCOPES),
        "state": state,
        "access_type": "offline",
        "prompt": "consent",  # always request refresh_token
    }
    return redirect(f"{GOOGLE_AUTH_URL}?{urlencode(params)}")

@auth_bp.route("/auth/callback")
def google_callback():
    if request.args.get("state") != session.pop("oauth_state", None):
        abort(400)

    code = request.args.get("code")
    if not code:
        abort(400)

    # Exchange code for token
    token_resp = requests.post(GOOGLE_TOKEN_URL, data={
        "code": code,
        "client_id": os.environ["GOOGLE_CLIENT_ID"],
        "client_secret": os.environ["GOOGLE_CLIENT_SECRET"],
        "redirect_uri": _callback_url(),
        "grant_type": "authorization_code",
    }, timeout=10)
    token_resp.raise_for_status()
    token_data = token_resp.json()

    # Get user info from id_token (simple JWT decode — no sig verify needed, we just issued it)
    import base64
    id_token = token_data.get("id_token", "")
    payload_b64 = id_token.split(".")[1] if id_token else ""
    # Pad base64
    payload_b64 += "=" * (-len(payload_b64) % 4)
    try:
        user_info = json.loads(base64.urlsafe_b64decode(payload_b64))
    except Exception:
        user_info = {}

    email = user_info.get("email", "")
    allowed = [e.strip().lower() for e in os.environ.get("ALLOWED_EMAIL", "").split(",") if e.strip()]
    if email.lower() not in allowed:
        return render_template("login.html", error="이 계정은 승인되지 않았습니다", csrf_token=get_csrf_token()), 403

    # Store token in Supabase settings
    db.set_setting("gmail_token", json.dumps(token_data))

    session["user"] = {
        "email": email,
        "name": user_info.get("name", email),
    }
    return redirect(url_for("index"))

@auth_bp.route("/auth/logout")
def logout():
    session.clear()
    return redirect(url_for("auth.login"))

# ── Helpers ────────────────────────────────────────────────────────────────────

def _callback_url() -> str:
    # In production (Vercel), use HTTPS. Locally, use http://localhost:5000
    base = os.environ.get("APP_BASE_URL", request.host_url.rstrip("/"))
    return f"{base}/auth/callback"
