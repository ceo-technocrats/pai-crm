"""
gmail.py — Gmail API send + read + sync

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
import re
import json
import base64
import socket
import email.mime.text
import email.mime.multipart
import email.utils
from datetime import datetime, timezone, timedelta

import httplib2
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from google.auth.exceptions import RefreshError

import db

GMAIL_SCOPES = [
    "https://www.googleapis.com/auth/gmail.send",
    "https://www.googleapis.com/auth/gmail.readonly",
]


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


def send_email(service, to: str, subject: str, body: str, attachments: list = None) -> str:
    """
    Send an email via the Gmail API, optionally with attachments.
    attachments: list of dicts with keys: filename, mimetype, data (base64 string)
    Returns the Gmail message ID on success.
    Retries once on network errors.
    Raises HttpError on API errors (caller must handle).
    """
    # Detect HTML content
    import re
    is_html = bool(re.search(r'<[a-z][\s\S]*>', body, re.IGNORECASE)) if body else False
    content_type = "html" if is_html else "plain"

    if attachments:
        import email.mime.multipart
        import email.mime.base
        import email.encoders
        msg = email.mime.multipart.MIMEMultipart()
        msg["To"] = to
        msg["Subject"] = subject
        msg.attach(email.mime.text.MIMEText(body, content_type, "utf-8"))
        for att in attachments:
            maintype, subtype = att["mimetype"].split("/", 1) if "/" in att["mimetype"] else ("application", "octet-stream")
            part = email.mime.base.MIMEBase(maintype, subtype)
            part.set_payload(base64.b64decode(att["data"]))
            email.encoders.encode_base64(part)
            part.add_header("Content-Disposition", "attachment", filename=att["filename"])
            msg.attach(part)
    else:
        msg = email.mime.text.MIMEText(body, content_type, "utf-8")
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


# ── Gmail Read ────────────────────────────────────────────────────────────────

def list_messages(service, query: str, max_results: int = 50) -> list[str]:
    """Return list of Gmail message IDs matching query."""
    try:
        resp = service.users().messages().list(
            userId="me", q=query, maxResults=max_results
        ).execute()
    except (httplib2.ServerNotFoundError, socket.timeout, ConnectionError):
        resp = service.users().messages().list(
            userId="me", q=query, maxResults=max_results
        ).execute()
    return [m["id"] for m in resp.get("messages", [])]


def get_message_headers(service, message_id: str) -> dict:
    """Fetch message metadata (no body). Returns parsed headers dict."""
    resp = service.users().messages().get(
        userId="me", id=message_id,
        format="metadata",
        metadataHeaders=["From", "To", "Subject", "Date"],
    ).execute()

    headers = {}
    for h in resp.get("payload", {}).get("headers", []):
        headers[h["name"].lower()] = h["value"]

    # Parse From email
    from_raw = headers.get("from", "")
    _, from_email = email.utils.parseaddr(from_raw)

    # Parse To emails (can be comma-separated)
    to_raw = headers.get("to", "")
    to_emails = [email.utils.parseaddr(a)[1] for a in to_raw.split(",")]
    to_emails = [e for e in to_emails if e]

    # Parse date
    date_str = headers.get("date", "")
    try:
        dt = email.utils.parsedate_to_datetime(date_str)
    except Exception:
        dt = datetime.now(timezone.utc)

    return {
        "gmail_id": message_id,
        "from_email": from_email.lower(),
        "to_emails": [e.lower() for e in to_emails],
        "subject": headers.get("subject", "(제목 없음)"),
        "date": dt,
    }


def build_contact_queries(contact_emails: list[str], since: datetime) -> list[str]:
    """Build Gmail search queries batched to stay under length limits."""
    date_str = since.strftime("%Y/%m/%d")
    queries = []
    batch = []
    batch_len = 0

    for addr in contact_emails:
        # Each email adds "from:x OR to:x " = ~2*(len+5) chars
        addition = len(addr) * 2 + 20
        if batch and batch_len + addition > 1200:
            q = _build_query(batch, date_str)
            queries.append(q)
            batch = []
            batch_len = 0
        batch.append(addr)
        batch_len += addition

    if batch:
        queries.append(_build_query(batch, date_str))

    return queries


def _build_query(emails: list[str], date_str: str) -> str:
    parts = []
    for e in emails:
        parts.append(f"from:{e}")
        parts.append(f"to:{e}")
    return f"({' OR '.join(parts)}) after:{date_str}"


# ── Sync ──────────────────────────────────────────────────────────────────────

def sync_inbox() -> dict:
    """
    Sync Gmail inbox with CRM contacts.
    Matches emails from/to contacts, logs to outreach_log, auto-advances status.
    Returns {"synced": N, "checked": N}.
    """
    # Load last sync time (default: 7 days ago)
    last_sync_str = db.get_setting("last_gmail_sync")
    if last_sync_str:
        try:
            since = datetime.fromisoformat(last_sync_str)
        except Exception:
            since = datetime.now(timezone.utc) - timedelta(days=7)
    else:
        since = datetime.now(timezone.utc) - timedelta(days=7)

    service = get_service()
    email_map = db.get_all_contact_emails()  # {email_lower: contact_id}
    if not email_map:
        return {"synced": 0, "checked": 0, "reason": "no_contacts_with_email"}

    user_email = os.environ.get("ALLOWED_EMAIL", "").strip().lower()
    queries = build_contact_queries(list(email_map.keys()), since)

    synced = 0
    checked = 0

    for query in queries:
        message_ids = list_messages(service, query, max_results=50)
        if not message_ids:
            continue

        # Batch dedup
        existing = db.bulk_check_gmail_ids(message_ids)
        new_ids = [mid for mid in message_ids if mid not in existing]
        checked += len(message_ids)

        for mid in new_ids:
            try:
                headers = get_message_headers(service, mid)
            except Exception:
                continue

            # Determine direction and contact
            if headers["from_email"] == user_email:
                direction = "outbound"
                contact_id = None
                for to_addr in headers["to_emails"]:
                    if to_addr in email_map:
                        contact_id = email_map[to_addr]
                        break
            else:
                direction = "inbound"
                contact_id = email_map.get(headers["from_email"])

            if not contact_id:
                continue

            inserted = db.log_outreach_if_new(
                gmail_message_id=mid,
                contact_id=contact_id,
                channel="email",
                direction=direction,
                subject=headers["subject"],
                body=None,
                notes="[자동 동기화]",
                logged_at=headers["date"],
            )

            if inserted:
                synced += 1
                # Auto-advance: 연락함 → 답변옴 on inbound
                if direction == "inbound":
                    try:
                        contact = db.get_contact(contact_id)
                        if contact and contact["status"] == "연락함":
                            db.update_contact_status(contact_id, "답변옴")
                    except Exception:
                        pass  # non-fatal

                    # Global reply suppression: pause ALL active campaign enrollments
                    try:
                        db.mark_contact_enrollments_replied(contact_id)
                    except Exception:
                        pass  # non-fatal

    # Update last sync timestamp
    db.set_setting("last_gmail_sync", datetime.now(timezone.utc).isoformat())

    return {"synced": synced, "checked": checked}
