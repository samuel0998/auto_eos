import json
import logging
import os
from datetime import datetime

from flask import Flask, jsonify, redirect, render_template, request

from services.fclm_service import trigger_hourly_collection, start_background_scheduler
from services.pprt import FCLM_BASE, STORAGE_STATE_PATH, ensure_session_dir, session_login_init
from services.reporte_service import get_latest_metrics, save_manual_fields


def create_app() -> Flask:
    app = Flask(__name__)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    )

    @app.get("/")
    def home():
        return render_template("reporte.html")

    @app.get("/api/metrics")
    def api_metrics():
        return jsonify(get_latest_metrics())

    @app.post("/api/manual-fields")
    def api_manual_fields():
        payload = request.get_json(force=True) or {}
        result = save_manual_fields(payload)
        return jsonify(result)

    @app.post("/api/pull-now")
    def api_pull_now():
        result = trigger_hourly_collection(triggered_by="manual_button")
        return jsonify(result)

    @app.get("/fclm/session/status")
    def session_status():
        return jsonify({"session_ready": os.path.exists(STORAGE_STATE_PATH), "storage_state_path": STORAGE_STATE_PATH})

    @app.get("/fclm/session/init")
    def session_init_get():
        return render_template("session_init.html", login_url=f"{FCLM_BASE}/")

    @app.get("/fclm/session/login")
    def session_login_redirect():
        return redirect(f"{FCLM_BASE}/", code=302)

    @app.post("/fclm/session/upload")
    def session_upload():
        ensure_session_dir()
        payload = request.get_json(force=True) or {}
        storage_state = payload.get("storage_state")
        if not storage_state:
            return jsonify({"ok": False, "message": "Envie storage_state (objeto JSON)."}), 400
        with open(STORAGE_STATE_PATH, "w", encoding="utf-8") as f:
            json.dump(storage_state, f)
        return jsonify({"ok": True, "saved": True, "storage_state_path": STORAGE_STATE_PATH})

    @app.post("/fclm/session/init")
    def session_init_post():
        payload = request.get_json(silent=True) or {}
        wait_seconds = int(payload.get("wait_seconds", 45))
        headless = bool(payload.get("headless", False))
        url = payload.get("url")
        try:
            result = session_login_init(url=url, wait_seconds=wait_seconds, headless=headless)
            return jsonify({"ok": True, **result})
        except Exception as exc:
            return jsonify({"ok": False, "message": str(exc)}), 500

    @app.get("/template")
    def logs_page():
        return render_template("template.html")

    @app.get("/health")
    def health():
        return jsonify({"status": "ok", "now": datetime.utcnow().isoformat()})

    if os.getenv("DISABLE_SCHEDULER", "0") != "1":
        start_background_scheduler()

    return app


app = create_app()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")), debug=False)
