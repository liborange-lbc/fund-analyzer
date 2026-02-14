from datetime import date, datetime
from typing import Optional

from sqlalchemy import (
    Date,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .database import Base, now_shanghai


class Index(Base):
    __tablename__ = "indices"
    __table_args__ = (
        UniqueConstraint("code", name="uq_index_code"),
        UniqueConstraint("name", name="uq_index_name"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    code: Mapped[str] = mapped_column(String(50), unique=True, nullable=False, index=True)
    name: Mapped[str] = mapped_column(String(200), unique=True, nullable=False, index=True)
    start_date: Mapped[Optional[date]] = mapped_column(Date, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=now_shanghai
    )

    prices: Mapped[list["IndexPrice"]] = relationship(
        "IndexPrice",
        primaryjoin="Index.code == IndexPrice.index_code",
        foreign_keys="IndexPrice.index_code",
        back_populates="index",
        viewonly=True,
    )


class IndexPrice(Base):
    __tablename__ = "index_prices"
    __table_args__ = (
        UniqueConstraint("index_code", "trade_date", name="uq_index_date"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    index_code: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    index_name: Mapped[str] = mapped_column(String(200), nullable=False, index=True)
    trade_date: Mapped[date] = mapped_column(Date, index=True)
    close: Mapped[float] = mapped_column(Float, nullable=False)
    change_pct: Mapped[Optional[float]] = mapped_column(Float, nullable=True)  # 当日涨跌幅（百分比）
    pe_ratio: Mapped[Optional[float]] = mapped_column(Float, nullable=True)  # 滚动市盈率

    ma_30: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    ma_60: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    ma_90: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    ma_120: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    ma_150: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    ma_180: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    ma_360: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    # 点位差：当日收盘价 - 当日均线价（30、60、90、120、150、180、360）
    ma30_diff: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    ma60_diff: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    ma90_diff: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    ma120_diff: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    ma150_diff: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    ma180_diff: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    ma360_diff: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=now_shanghai
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False,
        default=now_shanghai,
        onupdate=now_shanghai,
    )

    # 保留反向关系以便查询，但不再使用外键约束
    index: Mapped[Index] = relationship(
        "Index",
        primaryjoin="IndexPrice.index_code == Index.code",
        foreign_keys="IndexPrice.index_code",
        viewonly=True,
    )


class DbAuditLog(Base):
    """
    数据库操作审计日志：记录所有 INSERT / UPDATE / DELETE。
    """

    __tablename__ = "db_audit_logs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    table_name: Mapped[str] = mapped_column(String(128), index=True)
    operation: Mapped[str] = mapped_column(String(16), index=True)
    operator: Mapped[Optional[str]] = mapped_column(String(64), nullable=True, index=True)
    row_pk: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    detail: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=now_shanghai, index=True
    )


class SyncJobLog(Base):
    """
    行情同步任务日志：记录每次全量 / 今日同步的概要信息。
    """

    __tablename__ = "sync_job_logs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    job_type: Mapped[str] = mapped_column(String(32), index=True)  # sync_all_history / sync_today
    status: Mapped[str] = mapped_column(String(16), index=True)  # running / success / error
    started_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=now_shanghai, index=True
    )
    finished_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    total_indices: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    total_rows: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    source_summary: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    detail: Mapped[Optional[str]] = mapped_column(Text, nullable=True)


class NotificationSetting(Base):
    """
    通知开关：控制哪些事件会触发微信公众号模板消息。
    event_type 如：data_update（新数据更新）
    """
    __tablename__ = "notification_settings"
    __table_args__ = (UniqueConstraint("event_type", name="uq_notification_event_type"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    event_type: Mapped[str] = mapped_column(String(64), unique=True, nullable=False, index=True)
    enabled: Mapped[bool] = mapped_column(default=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=now_shanghai
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=now_shanghai, onupdate=now_shanghai
    )


class User(Base):
    """
    用户表：存储用户账号信息。
    """
    __tablename__ = "users"
    __table_args__ = (
        UniqueConstraint("username", name="uq_user_username"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    username: Mapped[str] = mapped_column(String(64), unique=True, nullable=False, index=True)
    hashed_password: Mapped[str] = mapped_column(String(128), nullable=False)
    role: Mapped[str] = mapped_column(String(16), nullable=False, default="user")  # admin / user
    is_active: Mapped[bool] = mapped_column(default=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=now_shanghai
    )


