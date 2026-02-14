from __future__ import annotations

import logging
import sys
from datetime import datetime, timedelta

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from sqlalchemy import select, func
from sqlalchemy.orm import Session

from .database import SessionLocal, TZ_SHANGHAI, log_timestamp
from .models import Index, IndexPrice
from .fetcher import fetch_history_for_index, _get_last_trade_date
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
                        continue
                    start = index_obj.start_date
                else:
                    start = last_date + timedelta(days=1)
                if start > today:
                    continue
                n, _ = fetch_history_for_index(db, index_obj, start=start, end=today)
                if n and n > 0:
                    updates.append((index_obj.code, index_obj.name, n))
            except Exception as e:
                _log.warning("[调度] 同步失败 %s: %s", index_obj.code, e)
                continue

        total_rows = sum(n for _, _, n in updates)
        if updates:
            _log.info("[调度] 数据同步完成，本次更新 %d 条（%d 个指数）: %s", total_rows, len(updates), [c for c, _, _ in updates])
            try:
                wechat_module.notify_data_update(updates)
            except Exception as e:
                _log.warning("[调度] 微信通知失败: %s", e)
        else:
            _log.info("[调度] 数据同步完成，无更新")
    finally:
        db.close()


def start_scheduler():
    """
    启动定时任务：唯一任务为「同步到今日」，每 5 秒执行一次；
    每次对最大数据未到今天的指数从缺漏日期拉到今天。
    """
    global scheduler
    if scheduler is not None and scheduler.running:
        return

    scheduler = BackgroundScheduler(timezone="Asia/Shanghai")
    scheduler.add_job(
        _job_sync_to_today,
        IntervalTrigger(seconds=5),
        id="sync_to_today",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
        misfire_grace_time=30,
    )
    scheduler.start()
    _log.info("[调度] 定时任务已启动（每 5 秒同步至今日）")
    scheduler.add_job(
        _job_sync_to_today,
        "date",
        run_date=datetime.now(TZ_SHANGHAI),
        id="initial_sync_to_today",
        replace_existing=True,
    )


def shutdown_scheduler():
    global scheduler
    if scheduler is not None:
        scheduler.shutdown(wait=False)
        scheduler = None

