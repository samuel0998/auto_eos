from __future__ import annotations

import logging
import os
import threading
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from services.pprt import FCMLParams, build_function_rollup_url, fetch_fcml_report
from services.reporte_service import upsert_metric

logger = logging.getLogger(__name__)

WAREHOUSE_ID = os.getenv("WAREHOUSE_ID", "GIG2")
PROCESS_ID = os.getenv("PROCESS_ID", "01003027")

_scheduler_started = False


def _coerce_float(value: Optional[str]) -> Optional[float]:
    if value is None:
        return None
    txt = str(value).strip().replace("%", "").replace(",", ".")
    try:
        return float(txt)
    except ValueError:
        return None


def _coerce_int(value: Optional[str]) -> Optional[int]:
    if value is None:
        return None
    txt = "".join(ch for ch in str(value) if ch.isdigit())
    if not txt:
        return None
    return int(txt)


def _extract_metric(rows: List[Dict[str, str]], window_start: datetime, window_end: datetime, url: str, triggered_by: str) -> Dict:
    selected = rows[0] if rows else {}
    return {
        "metric_date": window_start.date().isoformat(),
        "hour_start": window_start.hour,
        "hour_end": window_end.hour,
        "report_name": "HORA HORA",
        "functions_count": _coerce_int(selected.get("Functions") or selected.get("Function") or selected.get("col_1")),
        "associates_count": _coerce_int(selected.get("Associates") or selected.get("Associate") or selected.get("col_2")),
        "dpmo": _coerce_float(selected.get("Defects") or selected.get("DPMO") or selected.get("col_3")),
        "receive_error_indicator": _coerce_float(selected.get("Receive Error Indicator") or selected.get("col_4")),
        "source_url": url,
        "raw_payload": {"rows": rows},
        "triggered_by": triggered_by,
    }


def _compute_window(now: Optional[datetime] = None) -> tuple[datetime, datetime]:
    current = now or datetime.now()
    top_of_hour = current.replace(minute=0, second=0, microsecond=0)
    start = top_of_hour - timedelta(hours=2)
    end = top_of_hour - timedelta(hours=1)
    return start, end


def trigger_hourly_collection(triggered_by: str = "scheduler") -> Dict:
    window_start, window_end = _compute_window()
    params = FCMLParams(
        warehouse_id=WAREHOUSE_ID,
        process_id=PROCESS_ID,
        start_dt=window_start,
        end_dt=window_end,
    )
    url = build_function_rollup_url(params)
    logger.info("Starting FCML collection for window %s -> %s", window_start, window_end)
    try:
        rows = fetch_fcml_report(url)
        metric = _extract_metric(rows, window_start, window_end, url, triggered_by)
        upsert_metric(metric)
        msg = f"Coleta concluída para janela {window_start:%Hh}-{window_end:%Hh}."
        logger.info(msg)
        return {"ok": True, "message": msg, "window": [window_start.isoformat(), window_end.isoformat()]}
    except Exception as exc:
        logger.exception("Falha na coleta FCML: %s", exc)
        return {"ok": False, "message": str(exc), "window": [window_start.isoformat(), window_end.isoformat()]}


def _scheduler_loop() -> None:
    while True:
        now = datetime.now()
        seconds_until_next_hour = 3600 - (now.minute * 60 + now.second)
        time.sleep(max(1, seconds_until_next_hour))
        trigger_hourly_collection(triggered_by="scheduler")


def start_background_scheduler() -> None:
    global _scheduler_started
    if _scheduler_started:
        return
    _scheduler_started = True

    thread = threading.Thread(target=_scheduler_loop, daemon=True)
    thread.start()
    logger.info("Hourly scheduler iniciado.")
