#!/usr/bin/env python3
import argparse
import datetime as dt
import json
import os
import random
import sqlite3
import ssl
import time
from typing import Optional
import urllib.error
import urllib.request


URL = (
    "https://query1.finance.yahoo.com/v8/finance/chart/SPY"
    "?interval=1d&events=history&includeAdjustedClose=true"
    "&period1=1104537600&period2=1771804800"
)

INDEX_CODE = "SPY"
MA_WINDOWS = (30, 60, 90, 120, 150, 180, 360)
FALLBACK_PAYLOAD_JSON = """{"chart":{"result":[{"meta":{"currency":"USD","symbol":"SPY","exchangeName":"PCX","fullExchangeName":"NYSEArca","instrumentType":"ETF","firstTradeDate":728317800,"regularMarketTime":1771621200,"hasPrePostMarketData":true,"gmtoffset":-18000,"timezone":"EST","exchangeTimezoneName":"America/New_York","regularMarketPrice":689.43,"fiftyTwoWeekHigh":697.84,"fiftyTwoWeekLow":481.8,"regularMarketDayHigh":690.0,"regularMarketDayLow":681.73,"regularMarketVolume":99309328,"longName":"State Street SPDR S&P 500 ETF Trust","shortName":"State Street SPDR S&P 500 ETF T","chartPreviousClose":681.75,"priceHint":2,"currentTradingPeriod":{"pre":{"timezone":"EST","start":1771578000,"end":1771597800,"gmtoffset":-18000},"regular":{"timezone":"EST","start":1771597800,"end":1771621200,"gmtoffset":-18000},"post":{"timezone":"EST","start":1771621200,"end":1771635600,"gmtoffset":-18000}},"dataGranularity":"1d","range":"","validRanges":["1d","5d","1mo","3mo","6mo","1y","2y","5y","10y","ytd","max"]},"timestamp":[1770993000,1771338600,1771425000,1771511400,1771597800],"indicators":{"quote":[{"open":[681.6900024414062,680.1400146484375,684.02001953125,683.8400268554688,682.3200073242188],"close":[681.75,682.8499755859375,686.2899780273438,684.47998046875,689.4299926757812],"high":[686.280029296875,684.9400024414062,689.1500244140625,686.1799926757812,690.0599975585938],"volume":[96267500,81354700,73570300,58649400,99952100],"low":[677.52001953125,675.780029296875,682.8300170898438,681.5499877929688,681.72998046875]}],"adjclose":[{"adjclose":[681.75,682.8499755859375,686.2899780273438,684.47998046875,689.4299926757812]}]}}],"error":null}}"""


def get_db_path() -> str:
    db_url = os.getenv("DATABASE_URL", "").strip()
    if db_url.startswith("sqlite:///"):
        raw_path = db_url[len("sqlite:///") :]
        if raw_path.startswith("./"):
            return os.path.abspath(os.path.join(os.path.dirname(__file__), raw_path[2:]))
        if raw_path.startswith("/"):
            return raw_path
        return os.path.abspath(os.path.join(os.path.dirname(__file__), raw_path))
    return os.path.abspath(os.path.join(os.path.dirname(__file__), "fund_analyzer.db"))


def validate_proxy(proxy: str, timeout: int = 6) -> bool:
    if not proxy:
        return False
    test_url = "https://query1.finance.yahoo.com/v8/finance/chart/SPY?interval=1d&range=5d"
    opener = urllib.request.build_opener(
        urllib.request.ProxyHandler({"http": proxy, "https": proxy}),
        urllib.request.HTTPSHandler(context=ssl._create_unverified_context()),
    )
    req = urllib.request.Request(
        test_url,
        headers={
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        },
    )
    try:
        with opener.open(req, timeout=timeout) as resp:
            resp.read(1)
        return True
    except Exception as exc:
        print(f"Proxy check failed: {exc}; fallback to direct connection.")
        return False


def build_opener(
    proxy: Optional[str] = None,
    skip_proxy_check: bool = False,
) -> urllib.request.OpenerDirector:
    if proxy is None:
        proxy = (
            os.getenv("YFINANCE_PROXY")
            or os.getenv("HTTPS_PROXY")
            or os.getenv("HTTP_PROXY")
        )
    handlers = []
    if proxy and (skip_proxy_check or validate_proxy(proxy)):
        handlers.append(urllib.request.ProxyHandler({"http": proxy, "https": proxy}))
    elif proxy and skip_proxy_check:
        handlers.append(urllib.request.ProxyHandler({"http": proxy, "https": proxy}))
    handlers.append(urllib.request.HTTPSHandler(context=ssl._create_unverified_context()))
    return urllib.request.build_opener(*handlers)


def fetch_chart_data(
    max_attempts: int = 6,
    retry_base_delay: Optional[float] = None,
    proxy: Optional[str] = None,
    skip_proxy_check: bool = False,
) -> dict:
    env_payload = os.getenv("SPY_CHART_JSON", "").strip()
    if env_payload:
        return json.loads(env_payload)
    if os.getenv("SPY_USE_FALLBACK", "").strip() == "1":
        return json.loads(FALLBACK_PAYLOAD_JSON)

    if max_attempts <= 0:
        env_attempts = os.getenv("SPY_MAX_ATTEMPTS", "").strip()
        if env_attempts.isdigit():
            max_attempts = max(1, int(env_attempts))
        else:
            max_attempts = 6

    if retry_base_delay is None:
        base_delay = os.getenv("SPY_RETRY_BASE_DELAY", "").strip()
        try:
            base_delay_val = float(base_delay) if base_delay else 1.0
        except ValueError:
            base_delay_val = 1.0
    else:
        base_delay_val = max(0.1, float(retry_base_delay))

    opener = build_opener(proxy=proxy, skip_proxy_check=skip_proxy_check)
    req = urllib.request.Request(
        URL,
        headers={
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        },
    )
    for attempt in range(1, max_attempts + 1):
        try:
            if attempt == 1:
                time.sleep(random.uniform(0.3, 1.2))
            with opener.open(req, timeout=30) as resp:
                payload = resp.read().decode("utf-8")
            return json.loads(payload)
        except urllib.error.HTTPError as exc:
            if exc.code not in (429, 503, 504):
                raise
            retry_after = None
            if exc.headers:
                retry_after = exc.headers.get("Retry-After")
            if retry_after and retry_after.isdigit():
                delay = int(retry_after)
            else:
                delay = min(120, base_delay_val * (2 ** (attempt - 1))) + random.uniform(0.4, 2.0)
            print(f"HTTP {exc.code}; retrying in {delay:.1f}s (attempt {attempt}/{max_attempts})")
            time.sleep(delay)
        except urllib.error.URLError as exc:
            delay = min(120, base_delay_val * (2 ** (attempt - 1))) + random.uniform(0.4, 2.0)
            print(f"Network error: {exc}; retrying in {delay:.1f}s (attempt {attempt}/{max_attempts})")
            time.sleep(delay)
    raise RuntimeError(f"Failed to fetch after {max_attempts} attempts.")


def parse_prices(payload: dict) -> tuple[str, list[tuple[str, float]]]:
    chart = payload.get("chart") or {}
    if chart.get("error"):
        raise RuntimeError(f"Yahoo Chart error: {chart['error']}")
    result_list = chart.get("result") or []
    if not result_list:
        raise RuntimeError("Yahoo Chart result 为空")

    result = result_list[0]
    meta = result.get("meta") or {}
    name = (meta.get("shortName") or meta.get("longName") or INDEX_CODE).strip()

    timestamps = result.get("timestamp") or []
    quote_list = (result.get("indicators") or {}).get("quote") or []
    closes = quote_list[0].get("close") if quote_list else []

    records: list[tuple[str, float]] = []
    for ts, close in zip(timestamps, closes):
        if close is None:
            continue
        trade_date = dt.datetime.fromtimestamp(ts, tz=dt.timezone.utc).date().isoformat()
        records.append((trade_date, round(float(close), 2)))

    if not records:
        raise RuntimeError("SPY 无可用历史数据")

    records.sort(key=lambda x: x[0])
    return name, records


def compute_change_pct(rows: list[tuple[str, float]]) -> list[tuple[str, float, float | None]]:
    output: list[tuple[str, float, float | None]] = []
    prev_close = None
    for trade_date, close in rows:
        if prev_close and prev_close != 0:
            change_pct = round((close - prev_close) / prev_close * 100.0, 2)
        else:
            change_pct = None
        output.append((trade_date, close, change_pct))
        prev_close = close
    return output


def upsert_index(conn: sqlite3.Connection, name: str, start_date: str) -> None:
    conn.execute(
        """
        INSERT OR IGNORE INTO indices (code, name, start_date, source, created_at)
        VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
        """,
        (INDEX_CODE, name, start_date, "yahoo_chart"),
    )
    conn.execute(
        """
        UPDATE indices
        SET name = ?, start_date = CASE
            WHEN start_date IS NULL OR start_date > ? THEN ?
            ELSE start_date
        END,
        source = CASE
            WHEN source IS NULL OR source = '' THEN 'yahoo_chart'
            ELSE source
        END
        WHERE code = ?
        """,
        (name, start_date, start_date, INDEX_CODE),
    )


def upsert_prices(conn: sqlite3.Connection, name: str, rows: list[tuple[str, float, float | None]]) -> int:
    cursor = conn.cursor()
    cursor.executemany(
        """
        INSERT INTO index_prices (
            index_code, index_name, trade_date, close, change_pct, created_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        ON CONFLICT(index_code, trade_date) DO UPDATE SET
            index_name=excluded.index_name,
            close=excluded.close,
            change_pct=excluded.change_pct,
            updated_at=CURRENT_TIMESTAMP
        """,
        [(INDEX_CODE, name, d, c, p) for d, c, p in rows],
    )
    return cursor.rowcount


def compute_moving_averages(
    rows: list[tuple[str, float]]
) -> dict[str, dict[str, float | None]]:
    closes = [c for _, c in rows]
    ma_values: dict[int, list[float | None]] = {}
    for window in MA_WINDOWS:
        window_sum = 0.0
        values: list[float | None] = []
        for idx, close in enumerate(closes):
            window_sum += close
            if idx >= window:
                window_sum -= closes[idx - window]
            if idx + 1 >= window:
                values.append(round(window_sum / window, 2))
            else:
                values.append(None)
        ma_values[window] = values

    output: dict[str, dict[str, float | None]] = {}
    for idx, (trade_date, close) in enumerate(rows):
        row: dict[str, float | None] = {}
        for window in MA_WINDOWS:
            ma = ma_values[window][idx]
            row[f"ma_{window}"] = ma
            row[f"ma{window}_diff"] = round(close - ma, 2) if ma is not None else None
        output[trade_date] = row
    return output


def update_ma_and_change_pct(conn: sqlite3.Connection) -> int:
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT trade_date, close
        FROM index_prices
        WHERE index_code = ?
        ORDER BY trade_date ASC
        """,
        (INDEX_CODE,),
    )
    rows = [(r[0], float(r[1])) for r in cursor.fetchall()]
    if not rows:
        return 0

    change_pct_rows = compute_change_pct(rows)
    ma_map = compute_moving_averages(rows)

    updated = 0
    for trade_date, close, change_pct in change_pct_rows:
        ma_values = ma_map.get(trade_date, {})
        cursor.execute(
            """
            UPDATE index_prices
            SET
                change_pct = ?,
                ma_30 = ?, ma_60 = ?, ma_90 = ?, ma_120 = ?, ma_150 = ?, ma_180 = ?, ma_360 = ?,
                ma30_diff = ?, ma60_diff = ?, ma90_diff = ?, ma120_diff = ?, ma150_diff = ?, ma180_diff = ?, ma360_diff = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE index_code = ? AND trade_date = ?
            """,
            (
                change_pct,
                ma_values.get("ma_30"),
                ma_values.get("ma_60"),
                ma_values.get("ma_90"),
                ma_values.get("ma_120"),
                ma_values.get("ma_150"),
                ma_values.get("ma_180"),
                ma_values.get("ma_360"),
                ma_values.get("ma30_diff"),
                ma_values.get("ma60_diff"),
                ma_values.get("ma90_diff"),
                ma_values.get("ma120_diff"),
                ma_values.get("ma150_diff"),
                ma_values.get("ma180_diff"),
                ma_values.get("ma360_diff"),
                INDEX_CODE,
                trade_date,
            ),
        )
        updated += cursor.rowcount
    return updated


def main() -> int:
    parser = argparse.ArgumentParser(description="Import SPY history into SQLite.")
    parser.add_argument("--proxy", help="HTTP/HTTPS proxy, e.g. http://host:port")
    parser.add_argument("--max-attempts", type=int, default=0, help="Retry attempts (0 uses env/default)")
    parser.add_argument("--retry-base-delay", type=float, default=None, help="Base delay for backoff in seconds")
    parser.add_argument("--skip-proxy-check", action="store_true", help="Skip proxy validation step")
    args = parser.parse_args()

    payload = fetch_chart_data(
        max_attempts=args.max_attempts,
        retry_base_delay=args.retry_base_delay,
        proxy=args.proxy,
        skip_proxy_check=args.skip_proxy_check,
    )
    name, rows = parse_prices(payload)

    rows_with_change = compute_change_pct(rows)
    db_path = get_db_path()

    conn = sqlite3.connect(db_path)
    try:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        upsert_index(conn, name, rows[0][0])
        inserted = upsert_prices(conn, name, rows_with_change)
        updated = update_ma_and_change_pct(conn)
        conn.commit()
    finally:
        conn.close()

    print(f"Imported {len(rows)} rows for {INDEX_CODE}.")
    print(f"Upserted rows: {inserted}")
    print(f"Updated MA/change_pct rows: {updated}")
    print(f"Database: {db_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())