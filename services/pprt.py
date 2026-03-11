from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List

import requests
from bs4 import BeautifulSoup


@dataclass
class FCMLParams:
    warehouse_id: str
    process_id: str
    start_dt: datetime
    end_dt: datetime


def build_function_rollup_url(params: FCMLParams) -> str:
    base_url = "https://fclm-portal.amazon.com/reports/functionRollup"
    return (
        f"{base_url}?warehouseId={params.warehouse_id}"
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


def fetch_fcml_report(url: str, timeout: int = 30) -> List[Dict[str, str]]:
    response = requests.get(url, timeout=timeout)
    response.raise_for_status()
    return parse_fcml_table(response.text)
