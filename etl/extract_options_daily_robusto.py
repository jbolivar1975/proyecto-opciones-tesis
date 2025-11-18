# etl/extract_options_daily_robusto.py

import os
import time
from datetime import datetime

import pandas as pd
import yfinance as yf

# ============= CONFIGURACIÓN =============

TICKERS = [
    "AAPL", "MSFT", "NVDA", "GOOGL", "AMZN",
    "META", "TSLA", "JPM", "UNH", "XOM"
]

BASE_DIR = os.path.join("data", "options")
DAILY_DIR = os.path.join(BASE_DIR, "daily")
os.makedirs(DAILY_DIR, exist_ok=True)

MAX_EXPIRIES = 3        # nº máximo de vencimientos por ticker
MAX_RETRIES = 3         # nº máximo de reintentos por llamada
SLEEP_BETWEEN_TICKERS = 1.0   # seg de espera entre tickers
SLEEP_BETWEEN_EXPIRIES = 0.5  # seg entre vencimientos


def safe_get_options_list(ticker_str):
    """Obtiene la lista de vencimientos para un ticker con reintentos."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            t = yf.Ticker(ticker_str)
            opts = t.options
            if not opts:
                print(f"[WARN] {ticker_str}: sin vencimientos devueltos.")
            return t, opts
        except Exception as e:
            print(f"[ERROR] {ticker_str}: fallo obteniendo options list (intento {attempt}/{MAX_RETRIES}) -> {e}")
            if attempt == MAX_RETRIES:
                print(f"[ERROR] {ticker_str}: se agotaron los intentos para la lista de vencimientos.")
                return None, []
            sleep_time = 2 * attempt
            print(f"[INFO] Esperando {sleep_time} seg antes de reintentar...")
            time.sleep(sleep_time)
    return None, []


def safe_download_chain(ticker_obj, expiry_str, ticker_name):
    """Descarga la option chain (calls + puts) de un vencimiento concreto."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            chain = ticker_obj.option_chain(expiry_str)
            calls = chain.calls.copy()
            puts = chain.puts.copy()

            calls["type"] = "call"
            puts["type"] = "put"

            df = pd.concat([calls, puts], ignore_index=True)
            df["expiry"] = pd.to_datetime(expiry_str)
            df["ticker"] = ticker_name
            return df
        except Exception as e:
            print(f"[ERROR] {ticker_name} {expiry_str}: fallo descargando chain (intento {attempt}/{MAX_RETRIES}) -> {e}")
            if attempt == MAX_RETRIES:
                print(f"[ERROR] {ticker_name} {expiry_str}: agotados los intentos, se omite este vencimiento.")
                return pd.DataFrame()
            sleep_time = 2 * attempt
            print(f"[INFO] Esperando {sleep_time} seg antes de reintentar este vencimiento...")
            time.sleep(sleep_time)
    return pd.DataFrame()


if __name__ == "__main__":
    today = datetime.utcnow().strftime("%Y-%m-%d")
    print(f"[INFO] Fecha de captura (UTC): {today}")

    all_snapshots = []

    for tk in TICKERS:
        print(f"\n[INFO] Procesando ticker: {tk}")
        ticker_obj, expiries = safe_get_options_list(tk)

        if ticker_obj is None or not expiries:
            print(f"[WARN] {tk}: no se pudo obtener lista de vencimientos.")
            time.sleep(SLEEP_BETWEEN_TICKERS)
            continue

        expiries = expiries[:MAX_EXPIRIES]
        print(f"[INFO] {tk}: {len(expiries)} vencimientos -> {expiries}")

        for exp in expiries:
            print(f"[INFO] {tk}: descargando vencimiento {exp} ...")
            df_chain = safe_download_chain(ticker_obj, exp, tk)
            if not df_chain.empty:
                all_snapshots.append(df_chain)
            time.sleep(SLEEP_BETWEEN_EXPIRIES)

        time.sleep(SLEEP_BETWEEN_TICKERS)

    if not all_snapshots:
        print("[WARN] No se obtuvo información de opciones en esta ejecución.")
    else:
        df_all = pd.concat(all_snapshots, ignore_index=True)
        df_all["as_of_date"] = pd.to_datetime(today)

        out_path = os.path.join(DAILY_DIR, f"options_{today}.parquet")
        df_all.to_parquet(out_path, index=False)
        print(f"\n[OK] Snapshot diario guardado en: {out_path}")
        print(f"[INFO] Filas totales: {len(df_all)}")
