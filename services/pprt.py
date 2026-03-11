from __future__ import annotations

import json
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional

import requests
from bs4 import BeautifulSoup

FCLM_BASE = "https://fclm-portal.amazon.com"
SESSION_DIR = os.path.join(os.path.dirname(__file__), ".session")
STORAGE_STATE_PATH = os.path.join(SESSION_DIR, "storage_state.json")


@dataclass
class FCMLParams:
    warehouse_id: str
    process_id: str
    start_dt: datetime
    end_dt: datetime


def ensure_session_dir() -> None:
    os.makedirs(SESSION_DIR, exist_ok=True)


def build_function_rollup_url(params: FCMLParams) -> str:
    if not re.match(r"^[A-Z0-9]+$", params.warehouse_id):
        raise ValueError("warehouseId inválido")
    if not re.match(r"^\d{6,}$", params.process_id):
        raise ValueError("processId inválido")

    return (
        f"{FCLM_BASE}/reports/functionRollup"
        f"?warehouseId={params.warehouse_id}"
        "&spanType=Intraday"
        f"&startDate={params.start_dt.strftime('%Y-%m-%dT%H:00:00.000')}"
        f"&endDate={params.end_dt.strftime('%Y-%m-%dT%H:00:00.000')}"
        "&reportFormat=HTML"
        f"&processId={params.process_id}"
    )


def parse_fcml_table(html: str) -> List[Dict[str, str]]:
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table")
    if table is None:
        return []

    headers = [th.get_text(strip=True) for th in table.find_all("th")]
    rows: List[Dict[str, str]] = []

    for tr in table.find_all("tr"):
        tds = tr.find_all("td")
        if not tds:
            continue
        values = [td.get_text(" ", strip=True) for td in tds]
        if headers and len(headers) == len(values):
            rows.append(dict(zip(headers, values)))
        else:
            rows.append({f"col_{idx + 1}": v for idx, v in enumerate(values)})
    return rows


def _extract_midway_cookies_from_storage_state() -> requests.cookies.RequestsCookieJar:
    if not os.path.exists(STORAGE_STATE_PATH):
        raise FileNotFoundError("Sessão Midway não inicializada. Faça /fclm/session/init primeiro.")

    with open(STORAGE_STATE_PATH, "r", encoding="utf-8") as f:
        state = json.load(f)

    jar = requests.cookies.RequestsCookieJar()
    for cookie in state.get("cookies", []):
        name = cookie.get("name")
        value = cookie.get("value")
        domain = cookie.get("domain")
        path = cookie.get("path", "/")
        if name and value and domain:
            jar.set(name, value, domain=domain, path=path)
    return jar


def session_login_init(url: Optional[str] = None, wait_seconds: int = 45, headless: bool = False) -> Dict[str, str]:
    from playwright.sync_api import sync_playwright

    ensure_session_dir()
    login_url = url or f"{FCLM_BASE}/"
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        context = browser.new_context()
        page = context.new_page()
        page.goto(login_url, wait_until="domcontentloaded", timeout=120000)
        time.sleep(wait_seconds)
        context.storage_state(path=STORAGE_STATE_PATH)
        context.close()
        browser.close()

    return {
        "saved": str(os.path.exists(STORAGE_STATE_PATH)).lower(),
        "storage_state_path": STORAGE_STATE_PATH,
    }


def fetch_fcml_report(url: str, timeout: int = 30) -> List[Dict[str, str]]:
    resp = requests.get(url, timeout=timeout)
    if resp.status_code == 401 or "midway-auth.amazon.com" in resp.url:
        jar = _extract_midway_cookies_from_storage_state()
        session = requests.Session()
        session.cookies = jar
        resp = session.get(url, timeout=timeout)

    resp.raise_for_status()
    return parse_fcml_table(resp.text)
