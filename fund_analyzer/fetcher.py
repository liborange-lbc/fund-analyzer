from __future__ import annotations

import logging
import os
import sys
from contextlib import contextmanager
from datetime import date, datetime, timedelta
from typing import Iterable, Optional
from urllib.parse import urlencode

import pandas as pd
import requests
from sqlalchemy import select
from sqlalchemy.orm import Session

from .config import (
    ALPHA_VANTAGE_API_KEY,
    ALPHA_VANTAGE_SYMBOL_MAP,
    FRED_API_KEY,
    FRED_US_INDICES,
)
from .database import TZ_SHANGHAI, log_timestamp
from .models import Index, IndexPrice


def _compute_change_pct(df: pd.DataFrame) -> pd.DataFrame:
    """
    计算涨跌幅百分比：当日涨跌幅 = (当日收盘价 - 前一日收盘价) / 前一日收盘价 * 100
    DataFrame 需要包含 'close' 列，并按 trade_date 排序。
    """
    df = df.sort_values("trade_date").reset_index(drop=True)
    df["change_pct"] = df["close"].pct_change() * 100  # pct_change 计算百分比变化
    return df


def _get_last_trade_date(db: Session, index_code: str) -> Optional[date]:
    stmt = (
        select(IndexPrice.trade_date)
        .where(IndexPrice.index_code == index_code)
        .order_by(IndexPrice.trade_date.desc())
        .limit(1)
    )
    row = db.execute(stmt).scalar_one_or_none()
    return row


def _fetch_history_from_csindex(
    symbol: str,
    start: Optional[date],
    end: Optional[date],
) -> pd.DataFrame:
    """
    从中证指数官网获取指数历史收盘价。
    所有指数代码均可尝试请求，由接口返回结果判断是否有数据。

    接口示例：
    https://www.csindex.com.cn/csindex-home/perf/index-perf?indexCode=H20269&startDate=20260101&endDate=20260212
    """
    index_code = symbol.strip().upper()

    # 确定结束日期（东八区“今天”）
    today = datetime.now(TZ_SHANGHAI).date()
    if end is None:
        end = today
    elif end > today:
        end = today
    
    # 验证日期范围：开始日期不能晚于结束日期
    if start is not None and start > end:
        return pd.DataFrame()
    
    # 如果开始日期为 None，无法调用 API
    if start is None:
        return pd.DataFrame()

    def _fmt(d: Optional[date]) -> str:
        if d is None:
            return ""
        return d.strftime("%Y%m%d")

    start_str = _fmt(start)
    end_str = _fmt(end)

    url = "https://www.csindex.com.cn/csindex-home/perf/index-perf"
    params = {
        "indexCode": index_code,
        "startDate": start_str,
        "endDate": end_str,
    }
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": "https://www.csindex.com.cn",
    }
    try:
        resp = requests.get(url, params=params, headers=headers, timeout=10)
        resp.raise_for_status()
        
        # 检查响应内容类型
        content_type = resp.headers.get("Content-Type", "").lower()
        if "application/json" not in content_type:
            return pd.DataFrame()
        
        try:
            payload = resp.json()
        except ValueError:
            return pd.DataFrame()
    except Exception:
        return pd.DataFrame()

    # 检查 payload 格式
    if not isinstance(payload, dict):
        return pd.DataFrame()
    
    data = payload.get("data")
    if not data or not isinstance(data, list):
        return pd.DataFrame()

    records = []
    for item in data:
        try:
            # 确保 item 是字典
            if not isinstance(item, dict):
                continue
            
            # tradeDate 形如 '20260212'，需要用 %Y%m%d 解析
            if "tradeDate" not in item or "close" not in item:
                continue
                
            d = datetime.strptime(item["tradeDate"], "%Y%m%d").date()
            c = float(item["close"])
        except (KeyError, ValueError, TypeError):
            continue
        except Exception:
            continue
            
        if start and d < start:
            continue
        if end and d > end:
            continue
        records.append({"trade_date": d, "close": c, "pe_ratio": None})  # 中证接口不提供PE

    if not records:
        return pd.DataFrame()

    return pd.DataFrame(records)


def _fetch_history_from_yahoo_chart_api(
    symbol: str,
    start: Optional[date],
    end: Optional[date],
) -> pd.DataFrame:
    """
    通过 Yahoo Finance Chart API 直接获取历史日线收盘价。
    接口示例: https://query1.finance.yahoo.com/v8/finance/chart/QQQ?interval=1d&period1=xxx&period2=xxx
    用于 QQQ 等标的，避免 yfinance 的 YFTzMissingError / YFRateLimitError。
    """
    if start is None:
        return pd.DataFrame()
    today = datetime.now(TZ_SHANGHAI).date()
    if end is None:
        end = today
    elif end > today:
        end = today
    if start > end:
        return pd.DataFrame()

    period1 = int(datetime(start.year, start.month, start.day, tzinfo=TZ_SHANGHAI).timestamp())
    period2 = int(datetime(end.year, end.month, end.day, 23, 59, 59, tzinfo=TZ_SHANGHAI).timestamp())

    proxy = os.environ.get("YFINANCE_PROXY", "").strip()
    proxies = {"http": proxy, "https": proxy} if proxy else None

    def _try_fetch(ticker: str) -> pd.DataFrame:
        url = (
            "https://query1.finance.yahoo.com/v8/finance/chart/"
            f"{ticker}?interval=1d&period1={period1}&period2={period2}"
        )
        try:
            resp = requests.get(url, timeout=15, proxies=proxies)
            resp.raise_for_status()
            data = resp.json()
        except Exception:
            return pd.DataFrame()
        chart = data.get("chart") or {}
        result_list = chart.get("result")
        if not result_list:
            return pd.DataFrame()
        r = result_list[0]
        timestamps = r.get("timestamp")
        quote = (r.get("indicators") or {}).get("quote")
        if not quote:
            return pd.DataFrame()
        closes = quote[0].get("close")
        if not timestamps or not closes or len(timestamps) != len(closes):
            return pd.DataFrame()
        records = []
        for ts, c in zip(timestamps, closes):
            if c is None or (isinstance(c, float) and (c != c)):
                continue
            try:
                d = datetime.fromtimestamp(ts, tz=TZ_SHANGHAI).date()
            except (ValueError, OSError):
                continue
            if d < start or d > end:
                continue
            try:
                close_val = round(float(c), 2)
            except (TypeError, ValueError):
                continue
            records.append({"trade_date": d, "close": close_val, "pe_ratio": None})
        if not records:
            return pd.DataFrame()
        return pd.DataFrame(records)

    ticker = symbol.strip().upper()
    df = _try_fetch(ticker)
    if not df.empty:
        return df
    if ticker.isdigit() and len(ticker) == 6:
        df = _try_fetch(ticker + ".SS")
        if not df.empty:
            return df
        df = _try_fetch(ticker + ".SZ")
        if not df.empty:
            return df
    return pd.DataFrame()


def _fetch_history_from_yfinance(
    symbol: str,
    start: Optional[date],
    end: Optional[date],
) -> pd.DataFrame:
    """
    使用 yfinance 获取历史收盘价。中证无数据时可作备选。
    支持 A 股指数（如 000300.SS、399006.SZ）、美股等。若 symbol 为纯数字则尝试 .SS / .SZ 后缀。
    """
    # 仅当显式设置 YFINANCE_PROXY 时才走代理，避免本地未开代理时 Connection refused
    proxy = os.environ.get("YFINANCE_PROXY", "").strip()
    if proxy:
        os.environ["HTTP_PROXY"] = proxy
        os.environ["HTTPS_PROXY"] = proxy
    try:
        import yfinance as yf
        try:
            from yfinance.exceptions import YFException
        except ImportError:
            YFException = Exception
    except ImportError:
        return pd.DataFrame()
    # 抑制 yfinance 的 "Failed download" 等日志，仅保留 ERROR
    logging.getLogger("yfinance").setLevel(logging.ERROR)
    if start is None:
        return pd.DataFrame()
    today = datetime.now(TZ_SHANGHAI).date()
    if end is None:
        end = today
    elif end > today:
        end = today
    if start > end:
        return pd.DataFrame()

    start_str = start.strftime("%Y-%m-%d")
    end_str = end.strftime("%Y-%m-%d")

    _yf_logger = logging.getLogger("yfinance")

    @contextmanager
    def _quiet():
        """临时屏蔽 stdout/stderr 与 yfinance 日志，避免 Failed download / YFRateLimitError 等刷屏"""
        old_out, old_err = sys.stdout, sys.stderr
        old_level = _yf_logger.level
        try:
            with open(os.devnull, "w") as devnull:
                sys.stdout, sys.stderr = devnull, devnull
                _yf_logger.setLevel(logging.CRITICAL)
                yield
        finally:
            sys.stdout, sys.stderr = old_out, old_err
            _yf_logger.setLevel(old_level)

    def _try_download(ticker: str) -> pd.DataFrame:
        try:
            with _quiet():
                df = yf.download(
                    ticker,
                    start=start_str,
                    end=end_str,
                    progress=False,
                    auto_adjust=False,
                    threads=False,
                )
        except (YFException, Exception):
            return pd.DataFrame()
        if df is None or df.empty:
            return pd.DataFrame()
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        if "Close" not in df.columns:
            return pd.DataFrame()
        df = df[["Close"]].copy()
        df = df.rename(columns={"Close": "close"})
        df.index = pd.to_datetime(df.index)
        df["trade_date"] = df.index.date
        df = df[["trade_date", "close"]].dropna(subset=["close"])
        df["pe_ratio"] = None
        return df[["trade_date", "close", "pe_ratio"]].reset_index(drop=True)

    ticker = symbol.strip().upper()
    df = _try_download(ticker)
    if not df.empty:
        return df
    if ticker.isdigit() and len(ticker) == 6:
        df = _try_download(ticker + ".SS")
        if not df.empty:
            return df
        df = _try_download(ticker + ".SZ")
        if not df.empty:
            return df
    return pd.DataFrame()


def _fetch_history_from_fred(
    series_id: str,
    start: Optional[date],
    end: Optional[date],
) -> pd.DataFrame:
    """
    从 FRED（美联储经济数据）获取美股指数历史收盘。
    需配置 FRED_API_KEY。series_id 即 FRED 系列 ID，如 NASDAQCOM、SP500、DJIA。
    日期格式 YYYY-MM-DD；value 为字符串，"." 表示缺失。
    """
    if not FRED_API_KEY:
        return pd.DataFrame()
    if start is None:
        return pd.DataFrame()
    today = datetime.now(TZ_SHANGHAI).date()
    if end is None:
        end = today
    elif end > today:
        end = today
    if start > end:
        return pd.DataFrame()

    url = "https://api.stlouisfed.org/fred/series/observations"
    params = {
        "series_id": series_id,
        "api_key": FRED_API_KEY,
        "file_type": "json",
        "observation_start": start.strftime("%Y-%m-%d"),
        "observation_end": end.strftime("%Y-%m-%d"),
        "sort_order": "asc",
    }
    try:
        resp = requests.get(url, params=params, headers={"User-Agent": "Mozilla/5.0"}, timeout=15)
        resp.raise_for_status()
        payload = resp.json()
    except Exception:
        return pd.DataFrame()

    obs = payload.get("observations") if isinstance(payload, dict) else None
    if not isinstance(obs, list):
        return pd.DataFrame()

    records = []
    for item in obs:
        if not isinstance(item, dict):
            continue
        d_str = item.get("date")
        val = item.get("value")
        if not d_str or val is None or str(val).strip() == ".":
            continue
        try:
            d = datetime.strptime(d_str, "%Y-%m-%d").date()
            c = float(val)
        except (ValueError, TypeError):
            continue
        if start and d < start:
            continue
        if end and d > end:
            continue
        records.append({"trade_date": d, "close": c, "pe_ratio": None})

    if not records:
        return pd.DataFrame()
    return pd.DataFrame(records)


def fetch_fred_latest_observation(series_id: str) -> Optional[dict]:
    """
    从 FRED 获取指定系列的最近一条观测（日期、收盘值）。
    返回 {"trade_date": date, "close": float} 或 None。
    """
    if not FRED_API_KEY or not series_id:
        return None
    url = "https://api.stlouisfed.org/fred/series/observations"
    params = {
        "series_id": series_id,
        "api_key": FRED_API_KEY,
        "file_type": "json",
        "sort_order": "desc",
        "limit": 1,
    }
    try:
        resp = requests.get(url, params=params, headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
        resp.raise_for_status()
        payload = resp.json()
    except Exception:
        return None
    obs = payload.get("observations") if isinstance(payload, dict) else None
    if not isinstance(obs, list) or len(obs) == 0:
        return None
    item = obs[0]
    if not isinstance(item, dict):
        return None
    d_str = item.get("date")
    val = item.get("value")
    if not d_str or val is None or str(val).strip() == ".":
        return None
    try:
        d = datetime.strptime(d_str, "%Y-%m-%d").date()
        c = float(val)
    except (ValueError, TypeError):
        return None
    return {"trade_date": d, "close": round(c, 2)}


def _fetch_history_from_alphavantage(
    symbol_av: str,
    start: Optional[date],
    end: Optional[date],
) -> pd.DataFrame:
    """
    从 Alpha Vantage 获取日线收盘价（美股指数或 ETF）。
    免费档 outputsize=compact 仅返回最近约 100 个交易日，无 start/end 参数，本地按日期过滤。
    """
    if not ALPHA_VANTAGE_API_KEY or not symbol_av:
        return pd.DataFrame()
    url = "https://www.alphavantage.co/query"
    params = {
        "function": "TIME_SERIES_DAILY",
        "symbol": symbol_av,
        "outputsize": "compact",
        "apikey": ALPHA_VANTAGE_API_KEY,
        "datatype": "json",
    }
    try:
        resp = requests.get(url, params=params, headers={"User-Agent": "Mozilla/5.0"}, timeout=15)
        resp.raise_for_status()
        payload = resp.json()
    except Exception:
        return pd.DataFrame()
    if not isinstance(payload, dict):
        return pd.DataFrame()
    ts = payload.get("Time Series (Daily)")
    if not isinstance(ts, dict):
        return pd.DataFrame()
    records = []
    for d_str, o in ts.items():
        if not isinstance(o, dict):
            continue
        close_str = o.get("4. close")
        if close_str is None:
            continue
        try:
            d = datetime.strptime(d_str, "%Y-%m-%d").date()
            c = float(close_str)
        except (ValueError, TypeError):
            continue
        if start is not None and d < start:
            continue
        if end is not None and d > end:
            continue
        records.append({"trade_date": d, "close": c, "pe_ratio": None})
    if not records:
        return pd.DataFrame()
    return pd.DataFrame(records)


def fetch_alphavantage_latest(symbol_av: str) -> Optional[dict]:
    """
    从 Alpha Vantage 获取指定代码的最近一个交易日收盘。
    返回 {"trade_date": date, "close": float} 或 None。
    """
    if not ALPHA_VANTAGE_API_KEY or not symbol_av:
        return None
    url = "https://www.alphavantage.co/query"
    params = {
        "function": "TIME_SERIES_DAILY",
        "symbol": symbol_av,
        "outputsize": "compact",
        "apikey": ALPHA_VANTAGE_API_KEY,
        "datatype": "json",
    }
    try:
        resp = requests.get(url, params=params, headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
        resp.raise_for_status()
        payload = resp.json()
    except Exception:
        return None
    if not isinstance(payload, dict):
        return None
    ts = payload.get("Time Series (Daily)")
    if not isinstance(ts, dict) or len(ts) == 0:
        return None
    # 取最近一天（key 为日期字符串，降序时第一个为最新）
    dates_sorted = sorted(ts.keys(), reverse=True)
    d_str = dates_sorted[0]
    o = ts[d_str]
    if not isinstance(o, dict):
        return None
    close_str = o.get("4. close")
    if close_str is None:
        return None
    try:
        d = datetime.strptime(d_str, "%Y-%m-%d").date()
        c = float(close_str)
    except (ValueError, TypeError):
        return None
    return {"trade_date": d, "close": round(c, 2)}


def fetch_history_for_index(
    db: Session,
    index_obj: Index,
    start: Optional[date] = None,
    end: Optional[date] = None,
) -> tuple[int, Optional[str]]:
    """
    获取单个指数的历史数据（或增量数据），存入数据库并计算均线。

    返回新增/更新的记录数。
    """
    symbol = index_obj.code
    index_code = index_obj.code
    index_name = index_obj.name
    today = datetime.now(TZ_SHANGHAI).date()

    # 始终取库内最新交易日，用于增量区间与“昨日已有则跳过”
    last_date = _get_last_trade_date(db, index_code)

    # 若昨日净值已存在，不再请求外部接口
    yesterday = today - timedelta(days=1)
    if last_date is not None and last_date >= yesterday:
        return 0, None

    # 自动增量：从数据库最新日期的下一天开始
    if start is None:
        if last_date is not None:
            start = last_date + timedelta(days=1)
        elif index_obj.start_date is not None:
            start = index_obj.start_date

    # 确保结束日期不早于开始日期
    if start is not None and end is not None and start > end:
        return 0, None

    # 如果开始日期已经超过今天，无需同步
    if start is not None and start > today:
        return 0, None

    # 美股指数：优先 FRED，若无数据则尝试 Alpha Vantage（需配置 ALPHA_VANTAGE_API_KEY）
    df = pd.DataFrame()
    source = None
    if symbol in FRED_US_INDICES and FRED_API_KEY:
        df = _fetch_history_from_fred(symbol, start=start, end=end)
        if not df.empty:
            source = "fred"
    if df.empty and symbol in ALPHA_VANTAGE_SYMBOL_MAP and ALPHA_VANTAGE_API_KEY:
        av_symbol = ALPHA_VANTAGE_SYMBOL_MAP[symbol]
        df = _fetch_history_from_alphavantage(av_symbol, start=start, end=end)
        if not df.empty:
            source = "alphavantage"
    if df.empty:
        df = _fetch_history_from_csindex(symbol, start=start, end=end)
        if not df.empty:
            source = "csindex"
    if df.empty:
        df = _fetch_history_from_yahoo_chart_api(symbol, start=start, end=end)
        if not df.empty:
            source = "yahoo_chart"
    if df.empty:
        df = _fetch_history_from_yfinance(symbol, start=start, end=end)
        if not df.empty:
            source = "yfinance"
    if df.empty:
        return 0, None

    # 输出查询结果日志
    if len(df) > 0:
        date_range = f"{start.isoformat() if start else '全部'} ~ {end.isoformat() if end else '全部'}"
        print(f"{log_timestamp()} [查询] {index_code}({index_name}) | 数据源: {source} | 日期: {date_range} | 获取: {len(df)} 条")

    # 计算涨跌幅百分比
    df = _compute_change_pct(df)

    # 写入数据库：存在则更新，不存在则插入
    # 注意：查询时不计算均值，均值通过后续计算得来
    count = 0
    for _, row in df.iterrows():
        trade_date: date = row["trade_date"]
        close: float = float(row["close"])
        change_pct = round(float(row["change_pct"]), 2) if pd.notna(row.get("change_pct")) else None
        pe_ratio = round(float(row["pe_ratio"]), 2) if pd.notna(row.get("pe_ratio")) else None

        price_obj = (
            db.query(IndexPrice)
            .filter(
                IndexPrice.index_code == index_code,
                IndexPrice.trade_date == trade_date,
            )
            .one_or_none()
        )
        if price_obj is None:
            price_obj = IndexPrice(
                index_code=index_code,
                index_name=index_name,
                trade_date=trade_date,
                close=round(float(close), 2),
                change_pct=change_pct,
                pe_ratio=pe_ratio,
            )
            db.add(price_obj)

        # 更新时也同步 index_name（以防指数名称变更）
        price_obj.index_code = index_code
        price_obj.index_name = index_name
        # 所有数字保留两位小数
        price_obj.close = round(float(close), 2)
        price_obj.change_pct = change_pct
        price_obj.pe_ratio = pe_ratio
        
        # 注意：均线值和点位差不再在此处计算，通过后续的 recalculate_moving_averages_and_diffs 计算
        
        count += 1

    db.commit()
    return count, source


def fetch_history_for_indices(
    db: Session,
    indices: Iterable[Index],
    start: Optional[date] = None,
    end: Optional[date] = None,
) -> tuple[dict[int, int], dict[int, Optional[str]]]:
    """
    为多个指数获取历史/增量数据。

    返回 {index_id: affected_rows}。
    """
    result: dict[int, int] = {}
    sources: dict[int, Optional[str]] = {}
    for idx in indices:
        affected, src = fetch_history_for_index(db, idx, start=start, end=end)
        result[idx.id] = affected
        sources[idx.id] = src
    return result, sources


def backfill_all_history(db: Session, indices: Iterable[Index]) -> dict[int, int]:
    """
    首次运行时：一次性将所有历史数据拉取到当前。
    每个指数使用其配置的 start_date（如有）作为起始日期。
    """
    result: dict[int, int] = {}
    sources: dict[int, Optional[str]] = {}
    for idx in indices:
        # 全量同步时，使用指数配置的 start_date（如有）
        start = idx.start_date
        affected, src = fetch_history_for_index(db, idx, start=start, end=None)
        result[idx.id] = affected
        sources[idx.id] = src
    return result


def recalculate_moving_averages_and_diffs(
    db: Session,
    index_code: Optional[str] = None,
    verbose: bool = False,
    *,
    ma_only: bool = False,
    diff_only: bool = False,
) -> int:
    """
    重新计算所有指数的移动平均线和/或点位差（30、60、90、120、150、180、360）。
    
    参数:
        db: 数据库会话
        index_code: 如果指定，只重新计算该指数的数据；如果为None，重新计算所有指数
        verbose: 是否显示详细输出
        ma_only: 若为 True，只计算并写入均线 ma_*，不写点位差
        diff_only: 若为 True，只根据已有均线重算点位差（收盘价-均线）；与 ma_only 互斥
    
    返回:
        更新的记录数
    """
    from .models import Index
    
    # 获取需要处理的指数
    if index_code:
        indices = db.query(Index).filter(Index.code == index_code).all()
    else:
        indices = db.query(Index).all()
    
    if not indices:
        if verbose:
            print(f"{log_timestamp()} [计算] 没有找到需要处理的指数")
        return 0
    
    total_updated = 0
    
    for index_obj in indices:
        if verbose:
            print(f"{log_timestamp()} [计算] 处理指数: {index_obj.code} ({index_obj.name})")
        
        # 获取该指数的所有价格数据，按日期排序
        prices = (
            db.query(IndexPrice)
            .filter(IndexPrice.index_code == index_obj.code)
            .order_by(IndexPrice.trade_date.asc())
            .all()
        )
        
        if not prices:
            if verbose:
                print(f"{log_timestamp()} [计算]  跳过: 没有价格数据")
            continue
        
        if verbose:
            print(f"{log_timestamp()} [计算]  数据量: {len(prices)} 条 | 日期范围: {prices[0].trade_date} ~ {prices[-1].trade_date}")
        
        # 转换为DataFrame以便计算移动平均线
        data = []
        for price in prices:
            data.append({
                "trade_date": price.trade_date,
                "close": float(price.close),
                "id": price.id,
            })
        
        df = pd.DataFrame(data)
        df = df.sort_values("trade_date").reset_index(drop=True)

        MA_WINDOWS = (30, 60, 90, 120, 150, 180, 360)

        # 计算移动平均线（仅当需要更新均线时）
        if not diff_only:
            for window in MA_WINDOWS:
                df[f"ma_{window}"] = df["close"].rolling(window=window, min_periods=window).mean()

        # 更新数据库记录
        index_updated = 0
        for idx, row in df.iterrows():
            price_id = int(row["id"])
            price_obj = db.query(IndexPrice).filter(IndexPrice.id == price_id).first()

            if price_obj is None:
                continue

            # 确保 close 保留两位小数
            close_price = round(float(price_obj.close), 2)
            price_obj.close = close_price

            if not diff_only:
                # 更新均线值（保留两位小数）
                for window in MA_WINDOWS:
                    col = f"ma_{window}"
                    val = round(float(row[col]), 2) if pd.notna(row.get(col)) else None
                    setattr(price_obj, col, val)

            # 用于计算偏差的均线：来自本次计算的 df（未 diff_only）或数据库已有值（diff_only）
            if diff_only:
                ma_30 = price_obj.ma_30
                ma_60 = price_obj.ma_60
                ma_90 = price_obj.ma_90
                ma_120 = price_obj.ma_120
                ma_150 = price_obj.ma_150
                ma_180 = price_obj.ma_180
                ma_360 = price_obj.ma_360
            else:
                ma_30 = round(float(row["ma_30"]), 2) if pd.notna(row.get("ma_30")) else None
                ma_60 = round(float(row["ma_60"]), 2) if pd.notna(row.get("ma_60")) else None
                ma_90 = round(float(row["ma_90"]), 2) if pd.notna(row.get("ma_90")) else None
                ma_120 = round(float(row["ma_120"]), 2) if pd.notna(row.get("ma_120")) else None
                ma_150 = round(float(row["ma_150"]), 2) if pd.notna(row.get("ma_150")) else None
                ma_180 = round(float(row["ma_180"]), 2) if pd.notna(row.get("ma_180")) else None
                ma_360 = round(float(row["ma_360"]), 2) if pd.notna(row.get("ma_360")) else None

            # 计算点位差：当日收盘价 - 当日均线价（保留两位小数）；ma_only 时不写
            if not ma_only:
                price_obj.ma30_diff = round(close_price - ma_30, 2) if ma_30 is not None else None
                price_obj.ma60_diff = round(close_price - ma_60, 2) if ma_60 is not None else None
                price_obj.ma90_diff = round(close_price - ma_90, 2) if ma_90 is not None else None
                price_obj.ma120_diff = round(close_price - ma_120, 2) if ma_120 is not None else None
                price_obj.ma150_diff = round(close_price - ma_150, 2) if ma_150 is not None else None
                price_obj.ma180_diff = round(close_price - ma_180, 2) if ma_180 is not None else None
                price_obj.ma360_diff = round(close_price - ma_360, 2) if ma_360 is not None else None

            index_updated += 1
            total_updated += 1

        # 每处理完一个指数就提交一次，避免事务过大
        db.commit()

        if verbose:
            print(f"{log_timestamp()} [计算]  完成: 更新 {index_updated} 条记录")

    return total_updated

