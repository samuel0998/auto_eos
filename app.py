import logging
import os
from datetime import datetime

from flask import Flask, jsonify, render_template, request

from services.fclm_service import trigger_hourly_collection, start_background_scheduler
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
