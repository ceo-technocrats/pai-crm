"""
app.py — PAI CRM main Flask application

Routes:
  GET  /                    → dashboard (index)
  GET  /contacts            → contact list (filter, search, pagination)
  GET  /contacts/<id>       → contact detail
  POST /contacts/<id>/status     → update status
  POST /contacts/<id>/notes      → update notes
  POST /contacts/<id>/log        → log manual activity
  POST /contacts/<id>/tags/add   → add tag
  POST /contacts/<id>/tags/<tid>/remove → remove tag
  POST /contacts/<id>/email      → send single email
  GET  /pipeline            → Kanban view
  GET  /bulk-send           → bulk send form
  POST /bulk-send           → enqueue bulk send
  GET  /bulk-send/<job_id>/status → SSE progress stream
  POST /bulk-send/<job_id>/retry  → retry failed rows
  GET  /analytics           → analytics
  GET  /export.csv          → export contacts as CSV
  POST /import              → import CSV
  GET  /settings            → settings page
  POST /settings/template   → save template
"""

import os
import io
import csv
import uuid
import json
import time
from datetime import date

from flask import (
    Flask, render_template, request, redirect, url_for,
    session, flash, Response, stream_with_context, abort, jsonify,
)
from googleapiclient.errors import HttpError
from google.auth.exceptions import RefreshError

import db
import gmail
import importer
from auth import auth_bp, login_required, csrf_protect, get_csrf_token
from cron import cron_bp


def create_app():
    app = Flask(__name__)
    app.secret_key = os.environ["SECRET_KEY"]

    app.register_blueprint(auth_bp)
    app.register_blueprint(cron_bp)

    # ── Dashboard ──────────────────────────────────────────────────────────────

    @app.route("/")
    @login_required
    def index():
        raw = db.dashboard_data()
        gmail_ok = gmail.has_token()
        pipeline_list = [
            {"status": s, "count": raw["pipeline"].get(s, 0)}
            for s in db.STATUSES
        ]
        total = sum(p["count"] for p in pipeline_list)
        data = {
            "total": total,
            "overdue": len(raw["overdue"]),
            "due_today": len(raw["due_today"]),
            "sends_today": raw["sends_today"],
            "pipeline": pipeline_list,
            "recent_emails": raw.get("recent_emails", []),
        }
        return render_template(
            "index.html",
            data=data,
            gmail_ok=gmail_ok,
            csrf_token=get_csrf_token(),
        )

    # ── Contact list ───────────────────────────────────────────────────────────

    @app.route("/contacts")
    @login_required
    def contacts():
        page = int(request.args.get("page", 1))
        filters = {
            "region":  request.args.get("region", ""),
            "party":   request.args.get("party", ""),
            "council": request.args.get("council", ""),
            "status":  request.args.get("status", ""),
            "tag":     request.args.get("tag", ""),
            "search":  request.args.get("q", ""),
            "today_targets": request.args.get("today_targets") == "1",
        }
        rows, total = db.list_contacts(page=page, per_page=50, **filters)
        regions = db.distinct_regions()
        parties = db.distinct_parties()
        tags = db.all_tags()
        return render_template(
            "contacts.html",
            contacts=rows,
            total=total,
            page=page,
            per_page=50,
            filters=filters,
            regions=regions,
            parties=parties,
            tags=tags,
            csrf_token=get_csrf_token(),
        )

    # ── Contact detail ─────────────────────────────────────────────────────────

    @app.route("/contacts/<int:cid>")
    @login_required
    def contact_detail(cid):
        contact = db.get_contact(cid)
        if not contact:
            abort(404)
        log = db.get_outreach_log(cid)
        contact_tags = db.get_contact_tags(cid)
        all_tags = db.all_tags()
        template = db.get_default_template()
        preview_subject = db.fill_template_vars(template["subject"], contact) if template else ""
        preview_body = db.fill_template_vars(template["body"], contact) if template else ""
        return render_template(
            "contact.html",
            contact=contact,
            log=log,
            contact_tags=contact_tags,
            all_tags=all_tags,
            preview_subject=preview_subject,
            preview_body=preview_body,
            csrf_token=get_csrf_token(),
            statuses=["미연락", "연락함", "답변옴", "데모예약", "클로즈"],
        )

    # ── Add / Edit contact ────────────────────────────────────────────────────

    @app.route("/contacts/new", methods=["GET", "POST"])
    @login_required
    def new_contact():
        if request.method == "POST":
            csrf_protect_check()
            data = _contact_form_data()
            cid = db.create_contact(data)
            flash("연락처가 추가되었습니다.", "success")
            return redirect(url_for("contact_detail", cid=cid))
        return render_template(
            "contact_form.html",
            contact=None,
            csrf_token=get_csrf_token(),
        )

    @app.route("/contacts/<int:cid>/edit", methods=["GET", "POST"])
    @login_required
    def edit_contact(cid):
        contact = db.get_contact(cid)
        if not contact:
            abort(404)
        if request.method == "POST":
            csrf_protect_check()
            data = _contact_form_data()
            db.update_contact(cid, data)
            flash("연락처가 수정되었습니다.", "success")
            return redirect(url_for("contact_detail", cid=cid))
        return render_template(
            "contact_form.html",
            contact=contact,
            csrf_token=get_csrf_token(),
        )

    def _contact_form_data() -> dict:
        return {
            "region": request.form.get("region", "").strip(),
            "council": request.form.get("council", "").strip(),
            "name": request.form.get("name", "").strip(),
            "party": request.form.get("party", "").strip() or None,
            "district": request.form.get("district", "").strip() or None,
            "term": request.form.get("term", "").strip() or None,
            "email": request.form.get("email", "").strip() or None,
            "phone_office": request.form.get("phone_office", "").strip() or None,
            "phone_mobile": request.form.get("phone_mobile", "").strip() or None,
            "fax": request.form.get("fax", "").strip() or None,
            "notes": request.form.get("notes", "").strip() or None,
        }

    def csrf_protect_check():
        token = request.form.get("csrf_token")
        if not token or token != session.get("csrf_token"):
            abort(403)

    # ── Status update ──────────────────────────────────────────────────────────

    @app.route("/contacts/<int:cid>/status", methods=["POST"])
    @login_required
    @csrf_protect
    def update_status(cid):
        target = request.form.get("status", "")
        close_outcome = request.form.get("close_outcome") or None
        follow_up_date = request.form.get("follow_up_date") or None
        try:
            db.update_contact_status(cid, target, close_outcome, follow_up_date)
            flash("상태가 업데이트되었습니다.", "success")
        except ValueError as e:
            flash(str(e), "error")
        return redirect(url_for("contact_detail", cid=cid))

    # ── Status update (API — for Kanban drag-and-drop) ───────────────────────

    @app.route("/api/contacts/<int:cid>/status", methods=["POST"])
    @login_required
    def api_update_status(cid):
        data = request.get_json(force=True)
        target = data.get("status", "")
        try:
            db.update_contact_status(cid, target)
            return jsonify({"ok": True})
        except (ValueError, KeyError) as e:
            return jsonify({"ok": False, "error": str(e)}), 400

    # ── Notes update ───────────────────────────────────────────────────────────

    @app.route("/contacts/<int:cid>/notes", methods=["POST"])
    @login_required
    @csrf_protect
    def update_notes(cid):
        notes = request.form.get("notes", "")
        db.update_contact_notes(cid, notes)
        flash("메모가 저장되었습니다.", "success")
        return redirect(url_for("contact_detail", cid=cid))

    # ── Manual activity log ────────────────────────────────────────────────────

    @app.route("/contacts/<int:cid>/log", methods=["POST"])
    @login_required
    @csrf_protect
    def log_activity(cid):
        channel   = request.form.get("channel", "other")
        direction = request.form.get("direction", "outbound")
        subject   = request.form.get("subject", "")
        body      = request.form.get("body", "")
        notes     = request.form.get("notes", "")
        db.log_outreach(
            contact_id=cid,
            channel=channel,
            direction=direction,
            subject=subject,
            body=body,
            notes=notes,
        )
        flash("활동이 기록되었습니다.", "success")
        return redirect(url_for("contact_detail", cid=cid))

    # ── Tags ───────────────────────────────────────────────────────────────────

    @app.route("/contacts/<int:cid>/tags/add", methods=["POST"])
    @login_required
    @csrf_protect
    def add_tag(cid):
        name = request.form.get("tag_name", "").strip()
        if name:
            db.add_tag_to_contact(cid, name)
        return redirect(url_for("contact_detail", cid=cid))

    @app.route("/contacts/<int:cid>/tags/<int:tid>/remove", methods=["POST"])
    @login_required
    @csrf_protect
    def remove_tag(cid, tid):
        db.remove_tag_from_contact(cid, tid)
        return redirect(url_for("contact_detail", cid=cid))

    # ── Single email send ──────────────────────────────────────────────────────

    @app.route("/contacts/<int:cid>/email", methods=["POST"])
    @login_required
    @csrf_protect
    def send_single_email(cid):
        contact = db.get_contact(cid)
        if not contact:
            abort(404)

        subject = request.form.get("subject", "").strip()
        body    = request.form.get("body", "").strip()
        to_email = contact.get("email", "").strip()

        if not to_email:
            flash("이메일 주소가 없습니다.", "error")
            return redirect(url_for("contact_detail", cid=cid))

        try:
            service = gmail.get_service()
            gmail_id = gmail.send_email(service, to_email, subject, body)
        except RefreshError:
            flash("Gmail 인증이 필요합니다. 설정에서 재인증하세요.", "error")
            return redirect(url_for("contact_detail", cid=cid))
        except HttpError as e:
            flash(f"이메일 전송 실패: {e}", "error")
            return redirect(url_for("contact_detail", cid=cid))

        db.log_outreach(
            contact_id=cid,
            channel="email",
            direction="outbound",
            subject=subject,
            body=body,
            gmail_message_id=gmail_id,
        )
        if contact.get("status") == "미연락":
            try:
                db.update_contact_status(cid, "연락함")
            except Exception:
                pass

        flash("이메일이 전송되었습니다.", "success")
        return redirect(url_for("contact_detail", cid=cid))

    # ── Pipeline (Kanban) ──────────────────────────────────────────────────────

    @app.route("/pipeline")
    @login_required
    def pipeline():
        raw = db.pipeline_data()
        columns = [
            {"status": s, "contacts": raw[s]["rows"], "overflow": raw[s]["overflow"]}
            for s in db.STATUSES
        ]
        return render_template("pipeline.html", columns=columns, today=str(date.today()), csrf_token=get_csrf_token())

    # ── Bulk send ──────────────────────────────────────────────────────────────

    @app.route("/bulk-send")
    @login_required
    def bulk_send():
        regions = db.distinct_regions()
        parties = db.distinct_parties()
        tags = db.all_tags()
        template = db.get_default_template()
        gmail_ok = gmail.has_token()
        return render_template(
            "bulk_send.html",
            regions=regions,
            parties=parties,
            tags=tags,
            template=template,
            gmail_ok=gmail_ok,
            csrf_token=get_csrf_token(),
        )

    @app.route("/bulk-send", methods=["POST"])
    @login_required
    @csrf_protect
    def bulk_send_post():
        if not gmail.has_token():
            flash("Gmail 인증이 필요합니다.", "error")
            return redirect(url_for("bulk_send"))

        region  = request.form.get("region", "")
        party   = request.form.get("party", "")
        status  = request.form.get("status", "미연락")
        tag     = request.form.get("tag", "")

        # Save template if provided
        subject = request.form.get("subject", "").strip()
        body    = request.form.get("body", "").strip()
        if subject and body:
            db.save_template("발송용", subject, body)
            template = db.get_default_template()
            template_id = template["id"] if template else None
        else:
            template = db.get_default_template()
            template_id = template["id"] if template else None

        job_id = str(uuid.uuid4())
        inserted, skipped = db.enqueue_bulk_send(
            job_id=job_id,
            template_id=template_id,
            region=region,
            party=party,
            status=status,
            tag=tag,
        )

        if inserted == 0:
            flash(f"발송 대상이 없습니다. ({skipped}명 제외됨)", "warning")
            return redirect(url_for("bulk_send"))

        flash(f"{inserted}명에게 발송이 예약되었습니다. ({skipped}명 제외)", "success")
        return redirect(url_for("bulk_send_status", job_id=job_id))

    @app.route("/bulk-send/<job_id>/status")
    @login_required
    def bulk_send_status(job_id):
        status = db.get_queue_status(job_id)
        if not status:
            abort(404)
        return render_template(
            "bulk_send.html",
            job_id=job_id,
            job_status=status,
            regions=db.distinct_regions(),
            parties=db.distinct_parties(),
            tags=db.all_tags(),
            template=db.get_default_template(),
            gmail_ok=gmail.has_token(),
            csrf_token=get_csrf_token(),
        )

    @app.route("/bulk-send/<job_id>/retry", methods=["POST"])
    @login_required
    @csrf_protect
    def bulk_send_retry(job_id):
        count = db.retry_failed_queue(job_id)
        flash(f"{count}건을 재시도 대기열에 넣었습니다.", "success")
        return redirect(url_for("bulk_send_status", job_id=job_id))

    # ── Analytics ──────────────────────────────────────────────────────────────

    @app.route("/analytics")
    @login_required
    def analytics():
        data = db.analytics_data()
        return render_template("analytics.html", data=data)

    # ── CSV Export ─────────────────────────────────────────────────────────────

    @app.route("/export.csv")
    @login_required
    def export_csv():
        status_filter = request.args.get("status", "")
        region_filter = request.args.get("region", "")

        def generate():
            output = io.StringIO()
            writer = csv.writer(output)
            writer.writerow([
                "id", "region", "council", "name", "party", "district",
                "term", "email", "phone_office", "phone_mobile", "fax",
                "status", "close_outcome", "follow_up_date", "notes",
                "last_contact_at", "created_at",
            ])
            yield output.getvalue()
            output.truncate(0)
            output.seek(0)

            for row in db.export_contacts(status_filter, region_filter):
                writer.writerow([
                    row.get("id"), row.get("region"), row.get("council"),
                    row.get("name"), row.get("party"), row.get("district"),
                    row.get("term"), row.get("email"), row.get("phone_office"),
                    row.get("phone_mobile"), row.get("fax"), row.get("status"),
                    row.get("close_outcome"), row.get("follow_up_date"),
                    row.get("notes"), row.get("last_contact_at"), row.get("created_at"),
                ])
                yield output.getvalue()
                output.truncate(0)
                output.seek(0)

        return Response(
            stream_with_context(generate()),
            mimetype="text/csv",
            headers={"Content-Disposition": "attachment; filename=contacts.csv"},
        )

    # ── CSV Import ─────────────────────────────────────────────────────────────

    @app.route("/import", methods=["POST"])
    @login_required
    @csrf_protect
    def import_csv():
        f = request.files.get("csvfile")
        if not f:
            flash("파일을 선택하세요.", "error")
            return redirect(url_for("contacts"))
        inserted, updated, skipped = importer.import_csv(f.read())
        flash(f"가져오기 완료: 신규 {inserted}명, 업데이트 {updated}명, 건너뜀 {skipped}명", "success")
        return redirect(url_for("contacts"))

    # ── Settings ───────────────────────────────────────────────────────────────

    @app.route("/settings")
    @login_required
    def settings():
        with db.db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM templates ORDER BY is_default DESC, id")
                templates = cur.fetchall()
        gmail_ok = gmail.has_token()
        stages = db.get_pipeline_stages()
        return render_template(
            "settings.html",
            templates=templates,
            stages=stages,
            gmail_ok=gmail_ok,
            csrf_token=get_csrf_token(),
        )

    @app.route("/settings/template", methods=["POST"])
    @login_required
    @csrf_protect
    def save_template():
        name    = request.form.get("name", "").strip()
        subject = request.form.get("subject", "").strip()
        body    = request.form.get("body", "").strip()
        if not (name and subject and body):
            flash("모든 항목을 입력하세요.", "error")
            return redirect(url_for("settings"))
        db.save_template(name, subject, body)
        flash("템플릿이 저장되었습니다.", "success")
        return redirect(url_for("settings"))

    # ── Pipeline stage management ────────────────────────────────────────────

    @app.route("/settings/stages/add", methods=["POST"])
    @login_required
    def add_stage():
        csrf_protect_check()
        name = request.form.get("name", "").strip()
        if not name:
            flash("스테이지 이름을 입력하세요.", "error")
            return redirect(url_for("settings"))
        try:
            db.add_pipeline_stage(name)
            flash(f"'{name}' 스테이지가 추가되었습니다.", "success")
        except Exception:
            flash("이미 존재하는 스테이지 이름입니다.", "error")
        return redirect(url_for("settings"))

    @app.route("/settings/stages/<int:stage_id>/delete", methods=["POST"])
    @login_required
    def delete_stage(stage_id):
        csrf_protect_check()
        db.delete_pipeline_stage(stage_id)
        flash("스테이지가 삭제되었습니다.", "success")
        return redirect(url_for("settings"))

    @app.route("/settings/stages/<int:stage_id>/rename", methods=["POST"])
    @login_required
    def rename_stage(stage_id):
        csrf_protect_check()
        new_name = request.form.get("name", "").strip()
        if not new_name:
            flash("새 이름을 입력하세요.", "error")
            return redirect(url_for("settings"))
        try:
            db.rename_pipeline_stage(stage_id, new_name)
            flash("스테이지 이름이 변경되었습니다.", "success")
        except Exception:
            flash("이미 존재하는 스테이지 이름입니다.", "error")
        return redirect(url_for("settings"))

    @app.route("/settings/stages/reorder", methods=["POST"])
    @login_required
    def reorder_stages():
        csrf_protect_check()
        ids_str = request.form.get("order", "")
        try:
            ids = [int(x) for x in ids_str.split(",") if x.strip()]
            db.reorder_pipeline_stages(ids)
            flash("순서가 변경되었습니다.", "success")
        except Exception:
            flash("순서 변경에 실패했습니다.", "error")
        return redirect(url_for("settings"))

    # ── Error handlers ─────────────────────────────────────────────────────────

    @app.errorhandler(404)
    def not_found(e):
        return render_template("404.html"), 404

    @app.errorhandler(403)
    def forbidden(e):
        return render_template("403.html"), 403

    return app


app = create_app()

if __name__ == "__main__":
    app.run(debug=True)
