"""
gmail.py — Gmail API send + token management

Token lifecycle:
  Stored in Supabase settings table (key='gmail_token')
  Loaded and refreshed here; refreshed token written back to DB.

Error handling:
  RefreshError   → re-raise (caller redirects to /auth/google)
  HttpError 429  → raise (caller shows quota error)
  HttpError 400  → raise (bad email address)
  HttpError 5xx  → raise (transient; caller may retry)
  Network errors → retry once, then raise
"""

import os
import json
import base64
import socket
import email.mime.text
import email.mime.multipart

import httplib2
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from google.auth.exceptions import RefreshError

import db

GMAIL_SCOPES = ["https://www.googleapis.com/auth/gmail.send"]


def get_service():
    """
    Build and return an authenticated Gmail API service object.
    Refreshes token if expired and writes updated token back to DB.
    Raises RefreshError if token cannot be refreshed.
    """
    token_json = db.get_setting("gmail_token")
    if not token_json:
        raise RefreshError("Gmail 토큰이 없습니다. /auth/google 로그인이 필요합니다.")

    token_data = json.loads(token_json)

    creds = Credentials(
        token=token_data.get("access_token"),
        refresh_token=token_data.get("refresh_token"),
        token_uri="https://oauth2.googleapis.com/token",
        client_id=os.environ["GOOGLE_CLIENT_ID"],
        client_secret=os.environ["GOOGLE_CLIENT_SECRET"],
        scopes=GMAIL_SCOPES,
    )

    # Refresh if expired
    if creds.expired and creds.refresh_token:
        creds.refresh(Request())
        # Write updated token back to DB
        updated = {
            **token_data,
            "access_token": creds.token,
            "expires_in": 3600,
        }
        db.set_setting("gmail_token", json.dumps(updated))

    return build("gmail", "v1", credentials=creds)


def send_email(service, to: str, subject: str, body: str) -> str:
    """
    Send a plain-text email via the Gmail API.
    Returns the Gmail message ID on success.
    Retries once on network errors.
    Raises HttpError on API errors (caller must handle).
    """
    msg = email.mime.text.MIMEText(body, "plain", "utf-8")
    msg["To"] = to
    msg["Subject"] = subject
    raw = base64.urlsafe_b64encode(msg.as_bytes()).decode()

    def _execute():
        return service.users().messages().send(
            userId="me", body={"raw": raw}
        ).execute()

    try:
        result = _execute()
    except (httplib2.ServerNotFoundError, socket.timeout, ConnectionError):
        # Retry once on network errors
        try:
            result = _execute()
        except (httplib2.ServerNotFoundError, socket.timeout, ConnectionError) as e:
            raise ConnectionError(f"네트워크 오류: {e}") from e

    return result["id"]


def has_token() -> bool:
    """Quick check — does a Gmail token exist in DB?"""
    return db.get_setting("gmail_token") is not None
