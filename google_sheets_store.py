"""
Persistencia opcional en Google Sheets para reservas.
Si no hay configuración válida, el sistema hace fallback a CSV local.
"""

from __future__ import annotations

import os
from typing import Dict, Iterable, List, Optional

import pandas as pd

try:
    import streamlit as st
except Exception:  # pragma: no cover
    st = None

try:
    import gspread
    from google.oauth2.service_account import Credentials
except Exception:  # pragma: no cover
    gspread = None
    Credentials = None


SHEET_RESERVAS_WEB = "reservas_web_2026"
SHEET_RESERVAS_HIST = "reservas_2026_full"


def _secret_get(name: str, default=None):
    env_val = os.environ.get(name)
    if env_val is not None:
        return env_val
    if st is None:
        return default
    try:
        if name in st.secrets:
            return st.secrets[name]
        lname = name.lower()
        if lname in st.secrets:
            return st.secrets[lname]
    except Exception:
        pass
    return default


def _to_bool(value) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def sheets_enabled() -> bool:
    return _to_bool(_secret_get("USE_GOOGLE_SHEETS", False))


def _service_account_info() -> Optional[Dict]:
    if st is not None:
        try:
            if "gcp_service_account" in st.secrets:
                return dict(st.secrets["gcp_service_account"])
        except Exception:
            pass

    raw = _secret_get("GOOGLE_SERVICE_ACCOUNT_JSON", None)
    if raw:
        try:
            import json
            return json.loads(raw)
        except Exception:
            return None
    return None


def _open_spreadsheet():
    if not sheets_enabled() or gspread is None or Credentials is None:
        return None

    spreadsheet_id = _secret_get("GOOGLE_SHEETS_SPREADSHEET_ID", None)
    if not spreadsheet_id:
        return None

    info = _service_account_info()
    if not info:
        return None

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    client = gspread.authorize(creds)
    return client.open_by_key(str(spreadsheet_id).strip())


def _ensure_worksheet(spreadsheet, title: str, headers: Optional[List[str]] = None):
    try:
        ws = spreadsheet.worksheet(title)
    except Exception:
        cols = max(26, len(headers or []))
        ws = spreadsheet.add_worksheet(title=title, rows=1000, cols=cols)

    if headers:
        first = ws.row_values(1)
        if not first:
            ws.update("A1", [headers])
    return ws


def _normalize_df(df: pd.DataFrame, headers: Optional[Iterable[str]]) -> pd.DataFrame:
    if headers is None:
        return df
    cols = list(headers)
    out = df.copy()
    for c in cols:
        if c not in out.columns:
            out[c] = ""
    return out[cols]


def read_sheet_df(sheet_name: str, headers: Optional[List[str]] = None) -> pd.DataFrame:
    spreadsheet = _open_spreadsheet()
    if spreadsheet is None:
        return pd.DataFrame(columns=headers or [])

    ws = _ensure_worksheet(spreadsheet, sheet_name, headers=headers)
    values = ws.get_all_values()
    if not values:
        return pd.DataFrame(columns=headers or [])

    raw_headers = values[0]
    rows = values[1:]
    if not rows:
        return pd.DataFrame(columns=headers or raw_headers)

    df = pd.DataFrame(rows, columns=raw_headers)
    return _normalize_df(df, headers)


def write_sheet_df(sheet_name: str, df: pd.DataFrame, headers: Optional[List[str]] = None) -> bool:
    spreadsheet = _open_spreadsheet()
    if spreadsheet is None:
        return False

    target = _normalize_df(df.copy(), headers)
    if headers is None:
        headers = list(target.columns)

    ws = _ensure_worksheet(spreadsheet, sheet_name, headers=headers)
    ws.clear()
    ws.update("A1", [headers])

    if target.empty:
        return True

    rows = target.fillna("").astype(str).values.tolist()
    chunk = 500
    for i in range(0, len(rows), chunk):
        ws.append_rows(rows[i : i + chunk], value_input_option="USER_ENTERED")
    return True


def upsert_sheet_row(
    sheet_name: str,
    row: Dict,
    key_col: str = "ID_RESERVA",
    headers: Optional[List[str]] = None,
) -> bool:
    spreadsheet = _open_spreadsheet()
    if spreadsheet is None:
        return False

    if headers is None:
        headers = list(row.keys())

    ws = _ensure_worksheet(spreadsheet, sheet_name, headers=headers)

    key_value = str(row.get(key_col, "")).strip()
    if not key_value:
        return False

    try:
        key_col_idx = headers.index(key_col) + 1
    except Exception:
        return False

    existing = ws.col_values(key_col_idx)
    row_idx = None
    for i, v in enumerate(existing[1:], start=2):
        if str(v).strip() == key_value:
            row_idx = i
            break

    out_row = [str(row.get(c, "")) for c in headers]
    if row_idx is None:
        ws.append_row(out_row, value_input_option="USER_ENTERED")
    else:
        ws.update(f"A{row_idx}", [out_row])
    return True


def update_sheet_fields_by_id(
    sheet_name: str,
    id_value: str,
    updates: Dict,
    key_col: str = "ID_RESERVA",
) -> bool:
    if not updates:
        return False
    spreadsheet = _open_spreadsheet()
    if spreadsheet is None:
        return False
    ws = _ensure_worksheet(spreadsheet, sheet_name, headers=None)
    headers = ws.row_values(1)
    if not headers or key_col not in headers:
        return False

    id_idx = headers.index(key_col) + 1
    rows = ws.col_values(id_idx)
    target_row = None
    id_norm = str(id_value).strip()
    for i, v in enumerate(rows[1:], start=2):
        if str(v).strip() == id_norm:
            target_row = i
            break
    if target_row is None:
        return False

    current = ws.row_values(target_row)
    if len(current) < len(headers):
        current = current + [""] * (len(headers) - len(current))
    for k, v in updates.items():
        if k in headers:
            current[headers.index(k)] = "" if v is None else str(v)
    ws.update(f"A{target_row}", [current])
    return True

