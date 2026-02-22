from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timedelta, date

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger
from sqlalchemy import select, func
from sqlalchemy.orm import Session

from .database import SessionLocal, TZ_SHANGHAI, log_timestamp
from .models import Index, IndexPrice, LiveStrategy, Contact
from .fetcher import fetch_history_for_index, _get_last_trade_date
from .config import FRED_US_INDICES, ALPHA_VANTAGE_SYMBOL_MAP
from .emailer import send_email
from . import wechat as wechat_module

# 定时任务专用 logger，确保有输出（无 handler 时绑定 stderr）
_log = logging.getLogger("fund_analyzer.scheduler")
if not _log.handlers:
    _h = logging.StreamHandler(sys.stderr)
    _h.setFormatter(logging.Formatter("[%(asctime)s] [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
    _log.addHandler(_h)
    _log.setLevel(logging.INFO)

# 配置 APScheduler 日志：完全禁用冗余日志输出
# 禁用所有 APScheduler 相关日志，避免输出 "maximum number of running instances" 等信息
apscheduler_loggers = [
    'apscheduler',
    'apscheduler.scheduler',
    'apscheduler.executors',
    'apscheduler.executors.default',
    'apscheduler.jobstores',
    'apscheduler.jobstores.default',
]
for logger_name in apscheduler_loggers:
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.ERROR)
    logger.propagate = False
    # 添加空处理器，确保日志不会输出
    if not logger.handlers:
        logger.addHandler(logging.NullHandler())


scheduler: BackgroundScheduler | None = None


def _job_sync_to_today():
    """
    统一同步任务：每 5 秒运行一次。
    对每个指数，若最大数据日期不是今天，则从「最大日期+1」或 start_date 开始查询，直到更新到今天；
    若已是今天则跳过。
    """
    db: Session = SessionLocal()
    db.info["operator"] = "scheduler"
    updates: list[tuple[str, str, int]] = []  # (code, name, count)
    attempted_codes: list[str] = []
    attempted_seen: set[str] = set()
    try:
        indices = db.query(Index).all()
        if not indices:
            _log.info("[调度] 无指数配置，跳过同步")
            return
        today = datetime.now(TZ_SHANGHAI).date()
        _log.info("[调度] 数据同步开始，共 %d 个指数，目标截止: %s", len(indices), today)

        for index_obj in indices:
            try:
                last_date = _get_last_trade_date(db, index_obj.code)
                first_date = None
                if last_date is not None:
                    stmt = (
                        select(func.min(IndexPrice.trade_date))
                        .where(IndexPrice.index_code == index_obj.code)
                    )
                    first_date = db.execute(stmt).scalar_one_or_none()

                # 若用户把 start_date 改早，先补 start_date 到 first_date 之间的缺口
                if index_obj.start_date is not None and first_date is not None and index_obj.start_date < first_date:
                    gap_start = index_obj.start_date
                    gap_end = min(first_date - timedelta(days=1), today)
                    if gap_start <= gap_end:
                        if index_obj.code not in attempted_seen:
                            attempted_codes.append(index_obj.code)
                            attempted_seen.add(index_obj.code)
                        n, _ = fetch_history_for_index(db, index_obj, start=gap_start, end=gap_end)
                        if n and n > 0:
                            updates.append((index_obj.code, index_obj.name, n))

                # 最大数据已是今天则跳过
                last_date = _get_last_trade_date(db, index_obj.code)
                if last_date is not None and last_date >= today:
                    continue

                # 从「最大日期+1」或 start_date 拉到今天
                if last_date is None:
                    if index_obj.start_date is None:
                        if index_obj.code in FRED_US_INDICES or index_obj.code in ALPHA_VANTAGE_SYMBOL_MAP:
                            start = today - timedelta(days=365 * 5)
                        else:
                            continue
                    else:
                        start = index_obj.start_date
                else:
                    start = last_date + timedelta(days=1)
                if start > today:
                    continue
                if index_obj.code not in attempted_seen:
                    attempted_codes.append(index_obj.code)
                    attempted_seen.add(index_obj.code)
                n, _ = fetch_history_for_index(db, index_obj, start=start, end=today)
                if n and n > 0:
                    updates.append((index_obj.code, index_obj.name, n))
            except Exception as e:
                _log.warning("[调度] 同步失败 %s: %s", index_obj.code, e)
                continue

        total_rows = sum(n for _, _, n in updates)
        if updates:
            _log.info(
                "[调度] 数据同步完成，本次更新 %d 条（%d 个指数）: %s；本次尝试更新 %d 个指数: %s",
                total_rows,
                len(updates),
                [c for c, _, _ in updates],
                len(attempted_codes),
                attempted_codes,
            )
            try:
                wechat_module.notify_data_update(updates)
            except Exception as e:
                _log.warning("[调度] 微信通知失败: %s", e)
        else:
            if attempted_codes:
                _log.info(
                    "[调度] 数据同步完成，无更新；本次尝试更新 %d 个指数: %s",
                    len(attempted_codes),
                    attempted_codes,
                )
            else:
                _log.info("[调度] 数据同步完成，无更新")
    finally:
        db.close()


def _is_invest_day(freq: str, last_run_at: datetime | None, today: date) -> bool:
    if freq == "daily":
        return True
    if last_run_at is None:
        return True
    last_date = last_run_at.date()
    if freq == "weekly":
        return today.isocalendar().week != last_date.isocalendar().week
    if freq == "monthly":
        return today.month != last_date.month or today.year != last_date.year
    return False


def _evaluate_live_signal(
    db: Session,
    strategy: LiveStrategy,
    params: dict,
    latest_row: IndexPrice,
) -> tuple[str | None, str | None, str]:
    price = float(latest_row.close)
    action = None
    detail = None
    debug_lines = [
        f"date={latest_row.trade_date}",
        f"price={price}",
        f"strategy={strategy.strategy}",
        f"params={params}",
    ]
    strat = strategy.strategy

    if strat == "dca":
        freq = params.get("frequency", "monthly")
        if _is_invest_day(freq, strategy.last_run_at, latest_row.trade_date):
            action = "buy"
            detail = f"dca {freq}"
        debug_lines.append(f"frequency={freq}")
    elif strat == "smart_dca":
        freq = params.get("frequency", "monthly")
        step_pct = float(params.get("step_pct", 3))
        max_mul = float(params.get("max_multiplier", 3))
        if _is_invest_day(freq, strategy.last_run_at, latest_row.trade_date):
            # 基于历史峰值计算回撤
            peak = db.execute(
                select(func.max(IndexPrice.close)).where(
                    IndexPrice.index_code == strategy.index_code,
                    IndexPrice.trade_date <= latest_row.trade_date,
                )
            ).scalar_one_or_none()
            peak = float(peak) if peak else price
            drawdown = ((peak - price) / peak * 100) if peak else 0.0
            steps = int(drawdown // step_pct) if drawdown > 0 else 0
            multiplier = min(max_mul, 1 + steps)
            action = "buy"
            detail = f"smart_dca dd={drawdown:.1f}% x{multiplier:.1f}"
        debug_lines.append(f"frequency={freq} step_pct={step_pct} max_multiplier={max_mul}")
    elif strat == "mean_reversion":
        ma_period = int(params.get("ma_period", 60))
        buy_th = float(params.get("buy_threshold_pct", 3))
        sell_th = float(params.get("sell_threshold_pct", 3))
        ma_val = getattr(latest_row, f"ma_{ma_period}", None)
        if ma_val:
            buy_line = ma_val * (1 - buy_th / 100)
            sell_line = ma_val * (1 + sell_th / 100)
            if price <= buy_line:
                action = "buy"
                detail = f"below_ma{ma_period}"
            elif price >= sell_line:
                action = "sell"
                detail = f"above_ma{ma_period}"
            debug_lines.append(f"ma{ma_period}={ma_val} buy_line={buy_line} sell_line={sell_line}")
    elif strat == "mean_reversion_multi":
        ma_period = int(params.get("ma_period", 60))
        step_pct = float(params.get("step_pct", 3))
        ma_val = getattr(latest_row, f"ma_{ma_period}", None)
        if ma_val:
            buy_line = ma_val * (1 - step_pct / 100)
            sell_line = ma_val * (1 + step_pct / 100)
            if price <= buy_line:
                action = "buy"
                detail = f"step_buy_ma{ma_period}"
            elif price >= sell_line:
                action = "sell"
                detail = f"step_sell_ma{ma_period}"
            debug_lines.append(f"ma{ma_period}={ma_val} buy_line={buy_line} sell_line={sell_line}")
    elif strat == "ma_diff":
        ma_period = int(params.get("ma_period", 60))
        buy_diff = float(params.get("buy_diff", -100))
        sell_diff = float(params.get("sell_diff", 100))
        diff_val = getattr(latest_row, f"ma{ma_period}_diff", None)
        if diff_val is not None:
            if diff_val <= buy_diff:
                action = "buy"
                detail = f"diff<= {buy_diff}"
            elif diff_val >= sell_diff:
                action = "sell"
                detail = f"diff>= {sell_diff}"
            debug_lines.append(f"ma{ma_period}_diff={diff_val} buy_diff={buy_diff} sell_diff={sell_diff}")
    elif strat == "trend_following":
        ma_period = int(params.get("ma_period", 60))
        ma_val = getattr(latest_row, f"ma_{ma_period}", None)
        if ma_val:
            if price >= ma_val:
                action = "buy"
                detail = f"above_ma{ma_period}"
            else:
                action = "sell"
                detail = f"below_ma{ma_period}"
            debug_lines.append(f"ma{ma_period}={ma_val}")

    return action, detail, "\n".join(debug_lines)


def run_live_strategies(strategy_id: int | None = None, force_email: bool = False) -> list[dict]:
    db: Session = SessionLocal()
    db.info["operator"] = "live_strategy"
    results: list[dict] = []
    try:
        q = db.query(LiveStrategy).filter(LiveStrategy.enabled.is_(True))
        if strategy_id is not None:
            q = q.filter(LiveStrategy.id == strategy_id)
        strategies = q.order_by(LiveStrategy.id.asc()).all()
        if not strategies:
            return []
        for strat in strategies:
            latest_row = (
                db.query(IndexPrice)
                .filter(IndexPrice.index_code == strat.index_code)
                .order_by(IndexPrice.trade_date.desc())
                .first()
            )
            strat.last_run_at = datetime.now(TZ_SHANGHAI).replace(tzinfo=None)
            if not latest_row:
                db.commit()
                continue
            try:
                params = json.loads(strat.params) if strat.params else {}
            except Exception:
                params = {}
            action, detail, debug = _evaluate_live_signal(db, strat, params, latest_row)
            recipients = []
            recipient_contacts = []
            if strat.email_subscribers:
                ids = [
                    int(x)
                    for x in str(strat.email_subscribers).replace(";", ",").split(",")
                    if x.strip().isdigit()
                ]
                if ids:
                    contacts = (
                        db.query(Contact)
                        .filter(Contact.id.in_(ids))
                        .all()
                    )
                    recipient_contacts = [c for c in contacts if c and c.email]
                    recipients = [c.email for c in recipient_contacts if c.email]
            if action or force_email:
                _log.info(
                    "[调度] 实盘策略运行: id=%s action=%s force_email=%s recipients=%s",
                    strat.id,
                    action or "none",
                    force_email,
                    recipients,
                )
                if (
                    strat.last_signal_date == latest_row.trade_date
                    and strat.last_signal_action == action
                    and not force_email
                ):
                    db.commit()
                    continue
                subject = f"实盘策略执行: {strat.name}" if force_email and not action else f"实盘策略信号: {strat.name}"
                body = (
                    f"指数: {strat.index_name}({strat.index_code})\n"
                    f"策略: {strat.strategy}\n"
                    f"日期: {latest_row.trade_date}\n"
                    f"动作: {action or 'none'}\n"
                    f"价格: {latest_row.close}\n"
                    f"说明: {detail or ''}\n"
                    f"\n[决策过程]\n{debug}\n"
                )
                sent_flags = []
                if recipient_contacts:
                    for c in recipient_contacts:
                        smtp_config = None
                        if c.email_provider or c.email_user or c.email_password or c.email_sender or c.email_port:
                            smtp_config = {
                                "provider": c.email_provider,
                                "port": c.email_port,
                                "use_ssl": c.email_use_ssl,
                                "username": c.email_user,
                                "password": c.email_password,
                                "sender": c.email_sender or c.email_user or c.email,
                            }
                        sent_flags.append(send_email(subject, body, [c.email], smtp_config=smtp_config))
                else:
                    sent_flags.append(send_email(subject, body, recipients))
                sent = any(sent_flags)
                if force_email and not sent:
                    _log.warning(
                        "[调度] 实盘策略邮件未发送: strategy_id=%s recipients=%s",
                        strat.id,
                        recipients,
                    )
                if action:
                    strat.last_signal_date = latest_row.trade_date
                    strat.last_signal_action = action
                    strat.last_signal_price = latest_row.close
                    strat.last_signal_detail = detail
                db.commit()
                results.append(
                    {
                        "strategy_id": strat.id,
                        "action": action or "none",
                        "sent": sent,
                        "date": latest_row.trade_date.isoformat(),
                    }
                )
            else:
                db.commit()
        return results
    finally:
        db.close()


def _job_run_live_strategies():
    try:
        run_live_strategies()
    except Exception as e:
        _log.warning("[调度] 实盘策略运行失败: %s", e)


def start_scheduler():
    """
    启动定时任务：唯一任务为「同步到今日」，每 5 分钟执行一次；
    每次对最大数据未到今天的指数从缺漏日期拉到今天。
    """
    global scheduler
    if scheduler is not None and scheduler.running:
        return

    scheduler = BackgroundScheduler(timezone="Asia/Shanghai")
    scheduler.add_job(
        _job_sync_to_today,
        IntervalTrigger(minutes=5),
        id="sync_to_today",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
        misfire_grace_time=30,
    )
    scheduler.start()
    _log.info("[调度] 定时任务已启动（每 5 分钟同步至今日）")
    scheduler.add_job(
        _job_sync_to_today,
        "date",
        run_date=datetime.now(TZ_SHANGHAI),
        id="initial_sync_to_today",
        replace_existing=True,
    )

    # 实盘策略：每天 10:00 运行一次
    scheduler.add_job(
        _job_run_live_strategies,
        CronTrigger(hour=10, minute=0),
        id="live_strategies_daily",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
        misfire_grace_time=300,
    )


def shutdown_scheduler():
    global scheduler
    if scheduler is not None:
        scheduler.shutdown(wait=False)
        scheduler = None

