#!/usr/bin/env python3
"""
fetch_stock.py

Purpose:
    Fetch latest quotes for one or more tickers from Alpha Vantage and append
    them to a Postgres table.

Usage:
    python scripts/fetch_stock.py --symbols MSFT,AAPL

Environment variables:
    - ALPHAVANTAGE_API_KEY (required)
    - STOCK_SYMBOLS (comma separated) OR use --symbols
    - STOCK_API_PROVIDER (default: ALPHAVANTAGE)
    - POSTGRES_HOST / POSTGRES_PORT / POSTGRES_DB / POSTGRES_USER / POSTGRES_PASSWORD
    - STOCK_DB_SCHEMA (default: public) / STOCK_DB_TABLE (default: stocks)
"""

import os
import sys
import time
import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional

import requests
import psycopg2
from psycopg2.extras import Json, execute_values

# Config from env
ALPHAVANTAGE_API_KEY = os.getenv("ALPHAVANTAGE_API_KEY")
API_PROVIDER = os.getenv("STOCK_API_PROVIDER", "ALPHAVANTAGE")
SYMBOLS_ENV = os.getenv("STOCK_SYMBOLS", "")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", 5432))
POSTGRES_DB = os.getenv("POSTGRES_DB", "airflow")
POSTGRES_USER = os.getenv("POSTGRES_USER", "airflow")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "airflow")
STOCK_DB_SCHEMA = os.getenv("STOCK_DB_SCHEMA", "public")
STOCK_DB_TABLE = os.getenv("STOCK_DB_TABLE", "stocks")

ALPHA_URL = "https://www.alphavantage.co/query"


def now_utc() -> datetime:
    """Return current UTC time with tzinfo."""
    return datetime.now(timezone.utc)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--symbols", default=None, help="Comma-separated symbols to fetch"
    )
    return parser.parse_args()


def fetch_alpha(symbol: str) -> Dict[str, Any]:
    """Fetch latest quote via Alpha Vantage GLOBAL_QUOTE."""
    params = {
        "function": "GLOBAL_QUOTE",
        "symbol": symbol,
        "apikey": ALPHAVANTAGE_API_KEY,
    }
    response = requests.get(ALPHA_URL, params=params, timeout=30)
    response.raise_for_status()
    return response.json()


def extract_from_alpha(symbol: str, data: Dict[str, Any]) -> Dict[str, Any]:
    """
    AlphaVantage GLOBAL_QUOTE sample:
    {"Global Quote": {
      "01. symbol": "MSFT",
      "02. open": "xxx",
      ...
    }}
    """
    try:
        g = data.get("Global Quote") or {}

        # map keys defensively
        def getf(key: str) -> Optional[str]:
            return g.get(key) or g.get(key.replace(".", "")) or None

        open_v = safe_float(getf("02. open"))
        high_v = safe_float(getf("03. high"))
        low_v = safe_float(getf("04. low"))
        price_v = safe_float(getf("05. price"))
        volume_v = safe_int(getf("06. volume"))
        latest_trading_day = getf("07. latest trading day")
        fetched_at = (
            now_utc()
            if not latest_trading_day
            else datetime.fromisoformat(latest_trading_day).replace(tzinfo=timezone.utc)
        )
        return {
            "symbol": symbol,
            "fetched_at": fetched_at,
            "open": open_v,
            "high": high_v,
            "low": low_v,
            "close": price_v,
            "volume": volume_v,
            "raw": data,
        }
    except Exception:
        # raise upstream; caller logs context
        raise


def safe_float(v: Any) -> Optional[float]:
    try:
        if v is None:
            return None
        return float(str(v).strip())
    except Exception:
        return None


def safe_int(v: Any) -> Optional[int]:
    try:
        if v is None:
            return None
        return int(float(str(v).strip()))
    except Exception:
        return None


def upsert_batch(
    conn: "psycopg2.extensions.connection", rows: Iterable[Dict[str, Any]]
) -> None:
    """Append rows to target table (no dedup)."""
    rows = list(rows)
    if not rows:
        return
    with conn.cursor() as cur:
        sql = f"""
        INSERT INTO {STOCK_DB_SCHEMA}.{STOCK_DB_TABLE}
          (symbol, fetched_at, open, high, low, close, volume, raw)
        VALUES %s
        """
        values = [
            (
                r.get("symbol"),
                r.get("fetched_at"),
                r.get("open"),
                r.get("high"),
                r.get("low"),
                r.get("close"),
                r.get("volume"),
                Json(r.get("raw")),
            )
            for r in rows
        ]
        execute_values(cur, sql, values)
    conn.commit()


def main() -> None:
    args = parse_args()

    symbols_in = args.symbols or SYMBOLS_ENV
    if not symbols_in:
        print(
            "No symbols specified. Set STOCK_SYMBOLS env or use --symbols",
            file=sys.stderr,
        )
        sys.exit(1)
    symbols = [s.strip().upper() for s in symbols_in.split(",") if s.strip()]
    if API_PROVIDER.upper() != "ALPHAVANTAGE":
        print(
            "Only ALPHAVANTAGE currently implemented. Set STOCK_API_PROVIDER=ALPHAVANTAGE",
            file=sys.stderr,
        )
        sys.exit(1)
    if not ALPHAVANTAGE_API_KEY:
        print("Missing ALPHAVANTAGE_API_KEY in env", file=sys.stderr)
        sys.exit(1)

    # connect to postgres
    conn = None
    try:
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
        )
    except Exception as e:
        print("Failed to connect to Postgres:", e, file=sys.stderr)
        sys.exit(2)

    rows_to_insert = []
    for symbol in symbols:
        try:
            data = fetch_alpha(symbol)
            parsed = extract_from_alpha(symbol, data)
            rows_to_insert.append(parsed)
            # Alpha Vantage has throttling limits; be polite between calls
            time.sleep(12)
        except requests.HTTPError as http_err:
            print(f"HTTP error fetching {symbol}: {http_err}", file=sys.stderr)
        except Exception as e:
            print(f"Error processing {symbol}: {e}", file=sys.stderr)

    try:
        upsert_batch(conn, rows_to_insert)
        print(f"Inserted {len(rows_to_insert)} rows.")
    except Exception as e:
        print("DB write error:", e, file=sys.stderr)
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    main()
