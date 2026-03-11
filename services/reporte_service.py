from __future__ import annotations

import json
import os
import sqlite3
from datetime import date, datetime
from typing import Any, Dict

DB_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres:WxmwezugggdaTwTvKsTiQrymIRkDAAvk@tramway.proxy.rlwy.net:41111/railway",
)
FALLBACK_SQLITE_PATH = os.getenv("FALLBACK_SQLITE_PATH", "/tmp/eos_metrics.db")

try:
    import psycopg2  # type: ignore
    import psycopg2.extras  # type: ignore
    HAS_PG = True
except Exception:
    HAS_PG = False


def _normalize_dt(row: Dict[str, Any]) -> Dict[str, Any]:
    if isinstance(row.get("metric_date"), (datetime, date)):
        row["metric_date"] = row["metric_date"].isoformat()
    if isinstance(row.get("collected_at"), datetime):
        row["collected_at"] = row["collected_at"].isoformat()
    return row


def get_conn():
    if HAS_PG:
        return psycopg2.connect(DB_URL)
    conn = sqlite3.connect(FALLBACK_SQLITE_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def ensure_table_exists() -> None:
    ddl_pg = """
    CREATE TABLE IF NOT EXISTS eos_metrics (
        id BIGSERIAL PRIMARY KEY,
        metric_date DATE NOT NULL,
        hour_start SMALLINT NOT NULL,
        hour_end SMALLINT NOT NULL,
        report_name TEXT NOT NULL DEFAULT 'HORA HORA',
        functions_count INTEGER,
        associates_count INTEGER,
        dpmo NUMERIC(12,2),
        receive_error_indicator NUMERIC(12,2),
        justificativa TEXT,
        apollo TEXT,
        dive_deep TEXT,
        call_outs TEXT,
        source_url TEXT,
        raw_payload JSONB,
        collected_at TIMESTAMP NOT NULL DEFAULT NOW(),
        triggered_by TEXT NOT NULL DEFAULT 'scheduler',
        UNIQUE (metric_date, hour_start, hour_end)
    );
    """
    ddl_sqlite = """
    CREATE TABLE IF NOT EXISTS eos_metrics (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        metric_date TEXT NOT NULL,
        hour_start INTEGER NOT NULL,
        hour_end INTEGER NOT NULL,
        report_name TEXT NOT NULL DEFAULT 'HORA HORA',
        functions_count INTEGER,
        associates_count INTEGER,
        dpmo REAL,
        receive_error_indicator REAL,
        justificativa TEXT,
        apollo TEXT,
        dive_deep TEXT,
        call_outs TEXT,
        source_url TEXT,
        raw_payload TEXT,
        collected_at TEXT DEFAULT CURRENT_TIMESTAMP,
        triggered_by TEXT NOT NULL DEFAULT 'scheduler',
        UNIQUE (metric_date, hour_start, hour_end)
    );
    """
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(ddl_pg if HAS_PG else ddl_sqlite)
        conn.commit()


def upsert_metric(row: Dict[str, Any]) -> None:
    ensure_table_exists()
    with get_conn() as conn:
        cur = conn.cursor()
        if HAS_PG:
            cur.execute(
                """
                INSERT INTO eos_metrics (
                    metric_date, hour_start, hour_end, report_name,
                    functions_count, associates_count, dpmo, receive_error_indicator,
                    source_url, raw_payload, triggered_by
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s)
                ON CONFLICT (metric_date, hour_start, hour_end)
                DO UPDATE SET
                    functions_count=EXCLUDED.functions_count,
                    associates_count=EXCLUDED.associates_count,
                    dpmo=EXCLUDED.dpmo,
                    receive_error_indicator=EXCLUDED.receive_error_indicator,
                    source_url=EXCLUDED.source_url,
                    raw_payload=EXCLUDED.raw_payload,
                    triggered_by=EXCLUDED.triggered_by,
                    collected_at=NOW();
                """,
                (
                    row["metric_date"], row["hour_start"], row["hour_end"], row["report_name"],
                    row.get("functions_count"), row.get("associates_count"), row.get("dpmo"), row.get("receive_error_indicator"),
                    row.get("source_url"), json.dumps(row.get("raw_payload", {}), ensure_ascii=False), row.get("triggered_by", "scheduler")
                ),
            )
        else:
            cur.execute(
                """
                INSERT INTO eos_metrics (
                    metric_date, hour_start, hour_end, report_name,
                    functions_count, associates_count, dpmo, receive_error_indicator,
                    source_url, raw_payload, triggered_by
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(metric_date, hour_start, hour_end) DO UPDATE SET
                    functions_count=excluded.functions_count,
                    associates_count=excluded.associates_count,
                    dpmo=excluded.dpmo,
                    receive_error_indicator=excluded.receive_error_indicator,
                    source_url=excluded.source_url,
                    raw_payload=excluded.raw_payload,
                    triggered_by=excluded.triggered_by,
                    collected_at=CURRENT_TIMESTAMP;
                """,
                (
                    row["metric_date"], row["hour_start"], row["hour_end"], row["report_name"],
                    row.get("functions_count"), row.get("associates_count"), row.get("dpmo"), row.get("receive_error_indicator"),
                    row.get("source_url"), json.dumps(row.get("raw_payload", {}), ensure_ascii=False), row.get("triggered_by", "scheduler")
                ),
            )
        conn.commit()


def save_manual_fields(payload: Dict[str, Any]) -> Dict[str, Any]:
    ensure_table_exists()
    required = (payload.get("metric_date"), payload.get("hour_start"), payload.get("hour_end"))
    if not all(v is not None and v != "" for v in required):
        return {"ok": False, "message": "metric_date, hour_start e hour_end são obrigatórios."}

    with get_conn() as conn:
        cur = conn.cursor()
        sql = """
        UPDATE eos_metrics
           SET justificativa = ?, apollo = ?, dive_deep = ?, call_outs = ?
         WHERE metric_date = ? AND hour_start = ? AND hour_end = ?;
        """
        params = (
            payload.get("justificativa", ""),
            payload.get("apollo", ""),
            payload.get("dive_deep", ""),
            payload.get("call_outs", ""),
            payload.get("metric_date"),
            int(payload.get("hour_start")),
            int(payload.get("hour_end")),
        )
        if HAS_PG:
            sql = sql.replace("?", "%s")
        cur.execute(sql, params)
        updated = cur.rowcount
        conn.commit()

    return {"ok": updated > 0, "updated": updated, "db_mode": "postgresql" if HAS_PG else "sqlite-fallback"}


def get_latest_metrics(limit: int = 24) -> Dict[str, Any]:
    ensure_table_exists()
    with get_conn() as conn:
        cur = conn.cursor()
        sql = """
        SELECT metric_date, hour_start, hour_end, report_name,
               functions_count, associates_count, dpmo, receive_error_indicator,
               justificativa, apollo, dive_deep, call_outs,
               source_url, collected_at, triggered_by
          FROM eos_metrics
      ORDER BY metric_date DESC, hour_start DESC
         LIMIT ?;
        """
        if HAS_PG:
            sql = sql.replace("?", "%s")
        cur.execute(sql, (limit,))
        rows_raw = cur.fetchall()

    rows = []
    for row in rows_raw:
        if isinstance(row, sqlite3.Row):
            item = dict(row)
        else:
            item = dict(zip([d[0] for d in cur.description], row))
        rows.append(_normalize_dt(item))
    return {"items": rows, "db_mode": "postgresql" if HAS_PG else "sqlite-fallback"}
