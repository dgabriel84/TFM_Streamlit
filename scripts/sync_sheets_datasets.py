#!/usr/bin/env python3
"""
Sincroniza datasets locales a Google Sheets.

Uso:
  export USE_GOOGLE_SHEETS=true
  export GOOGLE_SHEETS_SPREADSHEET_ID=...
  export GOOGLE_SERVICE_ACCOUNT_JSON='{"type":"service_account",...}'
  python scripts/sync_sheets_datasets.py
"""

import os
import sys

import pandas as pd

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from google_sheets_store import (  # noqa: E402
    SHEET_RESERVAS_HIST,
    SHEET_RESERVAS_WEB,
    sheets_enabled,
    write_sheet_df,
)


def main():
    if not sheets_enabled():
        print("Google Sheets no está habilitado. Define USE_GOOGLE_SHEETS=true.")
        return 1

    path_hist = os.path.join(ROOT, "reservas_2026_full.csv")
    path_web = os.path.join(ROOT, "reservas_web_2026.csv")

    if not os.path.exists(path_hist) or not os.path.exists(path_web):
        print("No se encuentran los CSV base en el proyecto.")
        return 1

    print("Leyendo CSV local histórico...")
    df_hist = pd.read_csv(path_hist, on_bad_lines="skip")
    print(f"Histórico: {len(df_hist)} filas")

    print("Leyendo CSV local web...")
    df_web = pd.read_csv(path_web, on_bad_lines="skip")
    print(f"Web: {len(df_web)} filas")

    print("Escribiendo hoja histórica...")
    ok_hist = write_sheet_df(SHEET_RESERVAS_HIST, df_hist, headers=list(df_hist.columns))
    print(f"Histórico sincronizado: {ok_hist}")

    print("Escribiendo hoja web...")
    ok_web = write_sheet_df(SHEET_RESERVAS_WEB, df_web, headers=list(df_web.columns))
    print(f"Web sincronizado: {ok_web}")

    if ok_hist and ok_web:
        print("Sincronización completada.")
        return 0
    print("Sincronización incompleta. Revisa credenciales/permisos.")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())

