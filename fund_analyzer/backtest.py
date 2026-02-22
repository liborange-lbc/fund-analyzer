from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Optional, Iterable

from .models import IndexPrice


MA_PERIODS = {30, 60, 90, 120, 150, 180, 360}


@dataclass
class BacktestTrade:
    trade_date: date
    action: str  # buy / sell
    price: float
    shares: float
    cash_after: float
    position_after: float
    reason: str


@dataclass
class BacktestPoint:
    trade_date: date
    price: float
    cash: float
    position: float
    value: float


def _get_ma_value(row: IndexPrice, period: int) -> Optional[float]:
    return getattr(row, f"ma_{period}", None)


def _get_ma_diff_value(row: IndexPrice, period: int) -> Optional[float]:
    return getattr(row, f"ma{period}_diff", None)


def _ensure_ma_period(period: int) -> int:
    if period not in MA_PERIODS:
        raise ValueError(f"ma_period 必须是 {sorted(MA_PERIODS)} 之一")
    return period


def _compute_max_drawdown(values: Iterable[float]) -> tuple[float, Optional[int], Optional[int]]:
    peak = None
    peak_idx = None
    max_dd = 0.0
    dd_start = None
    dd_end = None
    for idx, v in enumerate(values):
        if peak is None or v > peak:
            peak = v
            peak_idx = idx
        if peak and peak > 0:
            dd = (peak - v) / peak * 100
            if dd > max_dd:
                max_dd = dd
                dd_start = peak_idx
                dd_end = idx
    return max_dd, dd_start, dd_end


def backtest_dca(
    rows: list[IndexPrice],
    initial_cash: float,
    periodic_amount: float,
    frequency: str,
) -> tuple[list[BacktestPoint], list[BacktestTrade], float]:
    cash = float(initial_cash)
    position = 0.0
    trades: list[BacktestTrade] = []
    curve: list[BacktestPoint] = []

    last_week = None
    last_month = None
    total_invested = float(initial_cash)

    for idx, row in enumerate(rows):
        price = float(row.close)
        curr_week = row.trade_date.isocalendar().week
        curr_month = row.trade_date.month

        invest_today = False
        if frequency == "daily":
            invest_today = True
        elif frequency == "weekly":
            invest_today = last_week is None or curr_week != last_week
        elif frequency == "monthly":
            invest_today = last_month is None or curr_month != last_month
        else:
            raise ValueError("frequency 必须是 daily / weekly / monthly")

        if idx == 0 and cash > 0:
            shares = cash / price
            position += shares
            cash = 0.0
            trades.append(
                BacktestTrade(
                    trade_date=row.trade_date,
                    action="buy",
                    price=price,
                    shares=shares,
                    cash_after=cash,
                    position_after=position,
                    reason="initial",
                )
            )

        if invest_today and periodic_amount > 0:
            cash += periodic_amount
            total_invested += periodic_amount
            shares = cash / price
            position += shares
            cash = 0.0
            trades.append(
                BacktestTrade(
                    trade_date=row.trade_date,
                    action="buy",
                    price=price,
                    shares=shares,
                    cash_after=cash,
                    position_after=position,
                    reason=f"{frequency}_invest",
                )
            )

        value = cash + position * price
        curve.append(
            BacktestPoint(
                trade_date=row.trade_date,
                price=price,
                cash=cash,
                position=position,
                value=value,
            )
        )
        last_week = curr_week
        last_month = curr_month

    return curve, trades, total_invested


def backtest_smart_dca(
    rows: list[IndexPrice],
    initial_cash: float,
    base_amount: float,
    frequency: str,
    step_pct: float,
    max_multiplier: float,
) -> tuple[list[BacktestPoint], list[BacktestTrade], float]:
    if base_amount <= 0:
        raise ValueError("base_amount 必须大于 0")
    if step_pct <= 0:
        raise ValueError("step_pct 必须大于 0")
    if max_multiplier <= 1:
        raise ValueError("max_multiplier 必须大于 1")

    cash = float(initial_cash)
    position = 0.0
    trades: list[BacktestTrade] = []
    curve: list[BacktestPoint] = []

    last_week = None
    last_month = None
    total_invested = float(initial_cash)

    peak_price: Optional[float] = None

    for idx, row in enumerate(rows):
        price = float(row.close)
        if peak_price is None or price > peak_price:
            peak_price = price

        curr_week = row.trade_date.isocalendar().week
        curr_month = row.trade_date.month

        invest_today = False
        if frequency == "daily":
            invest_today = True
        elif frequency == "weekly":
            invest_today = last_week is None or curr_week != last_week
        elif frequency == "monthly":
            invest_today = last_month is None or curr_month != last_month
        else:
            raise ValueError("frequency 必须是 daily / weekly / monthly")

        if idx == 0 and cash > 0:
            shares = cash / price
            position += shares
            cash = 0.0
            trades.append(
                BacktestTrade(
                    trade_date=row.trade_date,
                    action="buy",
                    price=price,
                    shares=shares,
                    cash_after=cash,
                    position_after=position,
                    reason="initial",
                )
            )

        if invest_today and base_amount > 0:
            drawdown_pct = ((peak_price - price) / peak_price * 100) if peak_price else 0.0
            steps = int(drawdown_pct // step_pct) if drawdown_pct > 0 else 0
            multiplier = min(max_multiplier, 1 + steps)
            invest_amount = base_amount * multiplier
            cash += invest_amount
            total_invested += invest_amount
            shares = cash / price
            position += shares
            cash = 0.0
            trades.append(
                BacktestTrade(
                    trade_date=row.trade_date,
                    action="buy",
                    price=price,
                    shares=shares,
                    cash_after=cash,
                    position_after=position,
                    reason=f"smart_dca_dd_{drawdown_pct:.1f}%",
                )
            )

        value = cash + position * price
        curve.append(
            BacktestPoint(
                trade_date=row.trade_date,
                price=price,
                cash=cash,
                position=position,
                value=value,
            )
        )
        last_week = curr_week
        last_month = curr_month

    return curve, trades, total_invested


def backtest_mean_reversion(
    rows: list[IndexPrice],
    initial_cash: float,
    ma_period: int,
    buy_threshold_pct: float,
    sell_threshold_pct: float,
) -> tuple[list[BacktestPoint], list[BacktestTrade], float]:
    ma_period = _ensure_ma_period(ma_period)
    cash = float(initial_cash)
    position = 0.0
    trades: list[BacktestTrade] = []
    curve: list[BacktestPoint] = []
    total_invested = float(initial_cash)

    for row in rows:
        price = float(row.close)
        ma = _get_ma_value(row, ma_period)
        if ma:
            buy_line = ma * (1 - buy_threshold_pct / 100)
            sell_line = ma * (1 + sell_threshold_pct / 100)
            if position == 0 and cash > 0 and price <= buy_line:
                shares = cash / price
                position += shares
                cash = 0.0
                trades.append(
                    BacktestTrade(
                        trade_date=row.trade_date,
                        action="buy",
                        price=price,
                        shares=shares,
                        cash_after=cash,
                        position_after=position,
                        reason="below_ma",
                    )
                )
            elif position > 0 and price >= sell_line:
                cash = position * price
                trades.append(
                    BacktestTrade(
                        trade_date=row.trade_date,
                        action="sell",
                        price=price,
                        shares=position,
                        cash_after=cash,
                        position_after=0.0,
                        reason="above_ma",
                    )
                )
                position = 0.0

        value = cash + position * price
        curve.append(
            BacktestPoint(
                trade_date=row.trade_date,
                price=price,
                cash=cash,
                position=position,
                value=value,
            )
        )

    return curve, trades, total_invested


def backtest_ma_diff(
    rows: list[IndexPrice],
    initial_cash: float,
    ma_period: int,
    buy_threshold: float,
    sell_threshold: float,
) -> tuple[list[BacktestPoint], list[BacktestTrade], float]:
    ma_period = _ensure_ma_period(ma_period)
    cash = float(initial_cash)
    position = 0.0
    trades: list[BacktestTrade] = []
    curve: list[BacktestPoint] = []
    total_invested = float(initial_cash)

    for row in rows:
        price = float(row.close)
        diff_val = _get_ma_diff_value(row, ma_period)
        if diff_val is not None:
            if position == 0 and cash > 0 and diff_val <= buy_threshold:
                shares = cash / price
                position += shares
                cash = 0.0
                trades.append(
                    BacktestTrade(
                        trade_date=row.trade_date,
                        action="buy",
                        price=price,
                        shares=shares,
                        cash_after=cash,
                        position_after=position,
                        reason=f"diff<= {buy_threshold}",
                    )
                )
            elif position > 0 and diff_val >= sell_threshold:
                cash = position * price
                trades.append(
                    BacktestTrade(
                        trade_date=row.trade_date,
                        action="sell",
                        price=price,
                        shares=position,
                        cash_after=cash,
                        position_after=0.0,
                        reason=f"diff>= {sell_threshold}",
                    )
                )
                position = 0.0

        value = cash + position * price
        curve.append(
            BacktestPoint(
                trade_date=row.trade_date,
                price=price,
                cash=cash,
                position=position,
                value=value,
            )
        )

    return curve, trades, total_invested


def backtest_trend_following(
    rows: list[IndexPrice],
    initial_cash: float,
    ma_period: int,
) -> tuple[list[BacktestPoint], list[BacktestTrade], float]:
    ma_period = _ensure_ma_period(ma_period)
    cash = float(initial_cash)
    position = 0.0
    trades: list[BacktestTrade] = []
    curve: list[BacktestPoint] = []
    total_invested = float(initial_cash)

    for row in rows:
        price = float(row.close)
        ma = _get_ma_value(row, ma_period)
        if ma:
            if price >= ma and position == 0 and cash > 0:
                shares = cash / price
                position += shares
                cash = 0.0
                trades.append(
                    BacktestTrade(
                        trade_date=row.trade_date,
                        action="buy",
                        price=price,
                        shares=shares,
                        cash_after=cash,
                        position_after=position,
                        reason="above_ma",
                    )
                )
            elif price < ma and position > 0:
                cash = position * price
                trades.append(
                    BacktestTrade(
                        trade_date=row.trade_date,
                        action="sell",
                        price=price,
                        shares=position,
                        cash_after=cash,
                        position_after=0.0,
                        reason="below_ma",
                    )
                )
                position = 0.0

        value = cash + position * price
        curve.append(
            BacktestPoint(
                trade_date=row.trade_date,
                price=price,
                cash=cash,
                position=position,
                value=value,
            )
        )

    return curve, trades, total_invested


def backtest_mean_reversion_multi(
    rows: list[IndexPrice],
    initial_cash: float,
    ma_period: int,
    step_pct: float,
    order_amount: float,
) -> tuple[list[BacktestPoint], list[BacktestTrade], float]:
    ma_period = _ensure_ma_period(ma_period)
    if step_pct <= 0:
        raise ValueError("step_pct 必须大于 0")
    if order_amount <= 0:
        raise ValueError("order_amount 必须大于 0")

    cash = float(initial_cash)
    position = 0.0
    trades: list[BacktestTrade] = []
    curve: list[BacktestPoint] = []
    total_invested = float(initial_cash)

    for row in rows:
        price = float(row.close)
        ma = _get_ma_value(row, ma_period)
        if not ma:
            value = cash + position * price
            curve.append(
                BacktestPoint(
                    trade_date=row.trade_date,
                    price=price,
                    cash=cash,
                    position=position,
                    value=value,
                )
            )
            continue

        buy_line = ma * (1 - step_pct / 100)
        sell_line = ma * (1 + step_pct / 100)

        if price <= buy_line and cash >= order_amount:
            shares = order_amount / price
            cash -= order_amount
            position += shares
            trades.append(
                BacktestTrade(
                    trade_date=row.trade_date,
                    action="buy",
                    price=price,
                    shares=shares,
                    cash_after=cash,
                    position_after=position,
                    reason="step_buy",
                )
            )
        elif price >= sell_line and position > 0:
            shares = min(position, order_amount / price)
            cash += shares * price
            position -= shares
            trades.append(
                BacktestTrade(
                    trade_date=row.trade_date,
                    action="sell",
                    price=price,
                    shares=shares,
                    cash_after=cash,
                    position_after=position,
                    reason="step_sell",
                )
            )

        value = cash + position * price
        curve.append(
            BacktestPoint(
                trade_date=row.trade_date,
                price=price,
                cash=cash,
                position=position,
                value=value,
            )
        )

    return curve, trades, total_invested


def compute_metrics(curve: list[BacktestPoint], total_invested: float) -> dict:
    if not curve:
        return {}
    start = curve[0].trade_date
    end = curve[-1].trade_date
    final_position_value = curve[-1].position * curve[-1].price
    final_value = curve[-1].value
    total_profit = final_position_value - total_invested
    total_return_pct = (final_value - total_invested) / total_invested * 100 if total_invested > 0 else 0.0
    years = max(1, (end - start).days) / 365.0
    annualized_return_pct = (final_value / total_invested) ** (1 / years) * 100 - 100 if total_invested > 0 else 0.0
    values = [p.value for p in curve]
    max_dd, dd_start, dd_end = _compute_max_drawdown(values)
    return {
        "start_date": start,
        "end_date": end,
        "total_days": len(curve),
        "total_invested": round(total_invested, 2),
        "total_position_value": round(final_position_value, 2),
        "total_profit": round(total_profit, 2),
        "final_value": round(final_value, 2),
        "total_return_pct": round(total_return_pct, 2),
        "annualized_return_pct": round(annualized_return_pct, 2),
        "max_drawdown_pct": round(max_dd, 2),
        "max_drawdown_start": curve[dd_start].trade_date if dd_start is not None else None,
        "max_drawdown_end": curve[dd_end].trade_date if dd_end is not None else None,
    }
