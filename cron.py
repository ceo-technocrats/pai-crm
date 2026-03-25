"""
cron.py — Vercel Cron handlers

POST /cron/send
  Auth: Authorization: Bearer <CRON_SECRET>
  Picks ≤100 pending queue rows (SELECT FOR UPDATE SKIP LOCKED)
  Sends each via Gmail API, logs result, advances contact status on success.
  Returns: {"sent": N, "failed": N, "remaining": N}

POST /cron/sync-inbox
  Auth: Authorization: Bearer <CRON_SECRET>
  Syncs Gmail inbox — matches emails from/to CRM contacts, logs to outreach_log.
  Returns: {"synced": N, "checked": N}
"""

import os

from flask import Blueprint, request, jsonify
from googleapiclient.errors import HttpError
from google.auth.exceptions import RefreshError

import db
import gmail

cron_bp = Blueprint("cron", __name__)

DAILY_LIMIT = 2000


@cron_bp.route("/cron/send", methods=["POST"])
def cron_send():
    # Authenticate
    secret = os.environ.get("CRON_SECRET", "")
    auth_header = request.headers.get("Authorization", "")
    if not secret or auth_header != f"Bearer {secret}":
        return jsonify({"error": "Unauthorized"}), 401

    sent = 0
    failed = 0

    # Pre-flight: Gmail token must exist and be refreshable
    try:
        service = gmail.get_service()
    except RefreshError:
        return jsonify({"error": "Gmail token missing or expired. Re-authenticate at /auth/google."}), 503

    rows = db.get_pending_queue_batch(100)
    if not rows:
        return jsonify({"sent": 0, "failed": 0, "remaining": 0})

    for row in rows:
        # Check daily send limit before each send
        today_count = db.sends_today()
        if today_count >= DAILY_LIMIT:
            # Mark this row and all remaining pending rows in this job as skipped
            job_id = row["job_id"]
            db.mark_remaining_skipped(job_id, "daily_limit")
            break

        contact = db.get_contact(row["contact_id"])
        if not contact:
            db.mark_queue_failed(row["id"], "contact_not_found")
            failed += 1
            continue

        to_email = contact.get("email", "").strip()
        if not to_email:
            db.mark_queue_failed(row["id"], "no_email_address")
            failed += 1
            continue

        # Build email from template
        template_id = row.get("template_id")
        if template_id:
            with db.db_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT subject, body FROM templates WHERE id = %s", (template_id,))
                    tmpl = cur.fetchone()
            if tmpl:
                subject = db.fill_template_vars(tmpl[0], contact)
                body = db.fill_template_vars(tmpl[1], contact)
            else:
                subject = "PAI 소개"
                body = ""
        else:
            subject = "PAI 소개"
            body = ""

        try:
            gmail_id = gmail.send_email(service, to_email, subject, body)
        except HttpError as e:
            status_code = int(e.resp.status)
            if status_code == 400:
                # Bad email address — bounce
                db.mark_queue_failed(row["id"], f"HttpError 400: {e}")
                db.log_outreach(
                    contact_id=row["contact_id"],
                    channel="email",
                    direction="outbound",
                    subject=subject,
                    body=body,
                    gmail_message_id=None,
                    notes=f"반송: 이메일 주소 오류 (400)",
                )
                failed += 1
            else:
                db.mark_queue_failed(row["id"], f"HttpError {status_code}: {e}")
                failed += 1
            continue
        except (ConnectionError, Exception) as e:
            db.mark_queue_failed(row["id"], str(e))
            failed += 1
            continue

        # Success
        db.mark_queue_sent(row["id"], gmail_id)
        db.log_outreach(
            contact_id=row["contact_id"],
            channel="email",
            direction="outbound",
            subject=subject,
            body=body,
            gmail_message_id=gmail_id,
        )

        # Advance contact status only if currently 미연락
        if contact.get("status") == "미연락":
            try:
                db.update_contact_status(row["contact_id"], "연락함")
            except Exception:
                pass  # Status advancement failure is non-fatal

        sent += 1

    # Count remaining pending rows
    remaining = db.get_queue_status(rows[0]["job_id"])["pending"] if rows else 0

    return jsonify({"sent": sent, "failed": failed, "remaining": remaining})


@cron_bp.route("/cron/sync-inbox", methods=["POST"])
def cron_sync_inbox():
    """Sync Gmail inbox with CRM contacts."""
    secret = os.environ.get("CRON_SECRET", "")
    auth_header = request.headers.get("Authorization", "")
    if not secret or auth_header != f"Bearer {secret}":
        return jsonify({"error": "Unauthorized"}), 401

    try:
        result = gmail.sync_inbox()
        return jsonify(result)
    except RefreshError:
        return jsonify({"error": "Gmail token expired. Re-authenticate at /auth/google."}), 503
    except Exception as e:
        return jsonify({"error": str(e)}), 500
