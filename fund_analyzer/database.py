from datetime import datetime
from typing import Optional

import pytz
from sqlalchemy import create_engine, event, inspect, text
from sqlalchemy.orm import sessionmaker, DeclarativeBase, Session

from .config import get_database_url

# 东八区（Asia/Shanghai），所有记录时间统一使用此时区
TZ_SHANGHAI = pytz.timezone("Asia/Shanghai")


def now_shanghai() -> datetime:
    """返回当前东八区时间（naive datetime，表示上海本地时钟时间）。"""
    return datetime.now(TZ_SHANGHAI).replace(tzinfo=None)


def log_timestamp() -> str:
    """返回当前东八区时间字符串，用于日志前缀。格式: 2026-02-14 11:30:00 +0800"""
    return now_shanghai().strftime("%Y-%m-%d %H:%M:%S") + " +0800"


class Base(DeclarativeBase):
    pass


DATABASE_URL = get_database_url()

connect_args = {}
if DATABASE_URL.startswith("sqlite"):
    # SQLite 在多线程环境下需要关闭 check_same_thread
    connect_args["check_same_thread"] = False

engine = create_engine(
    DATABASE_URL,
    echo=False,
    pool_pre_ping=True,
    connect_args=connect_args,
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def get_db():
    db: Session = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def init_db():
    """初始化数据库表结构。"""
    from . import models  # noqa: F401

    Base.metadata.create_all(bind=engine)

    # 简单的 SQLite 迁移：
    if engine.url.get_backend_name() == "sqlite":
        with engine.connect() as conn:
            # 1) db_audit_logs.operator
            cols = conn.execute(text("PRAGMA table_info('db_audit_logs')")).fetchall()
            col_names = {row[1] for row in cols}  # row[1] = column name
            if "operator" not in col_names:
                conn.execute(
                    text("ALTER TABLE db_audit_logs ADD COLUMN operator VARCHAR(64)")
                )

            # 2) sync_job_logs.source_summary
            cols2 = conn.execute(text("PRAGMA table_info('sync_job_logs')")).fetchall()
            col_names2 = {row[1] for row in cols2}
            if "source_summary" not in col_names2:
                conn.execute(
                    text(
                        "ALTER TABLE sync_job_logs ADD COLUMN source_summary VARCHAR(255)"
                    )
                )

            # 3) indices 表迁移：name 改为非空，添加 start_date
            cols3 = conn.execute(text("PRAGMA table_info('indices')")).fetchall()
            col_names3 = {row[1] for row in cols3}
            if "start_date" not in col_names3:
                conn.execute(
                    text("ALTER TABLE indices ADD COLUMN start_date DATE")
                )
            if "source" not in col_names3:
                conn.execute(
                    text("ALTER TABLE indices ADD COLUMN source VARCHAR(50)")
                )
            # 注意：SQLite 不支持直接修改列的非空约束，需要重建表
            # 这里先检查，如果 name 可以为空，会在应用层校验

            # 4) index_prices 表迁移：从 index_id 改为 index_code + index_name
            cols4 = conn.execute(text("PRAGMA table_info('index_prices')")).fetchall()
            col_names4 = {row[1] for row in cols4}
            
            if "index_id" in col_names4:
                # 如果存在 index_id 列，需要迁移
                if "index_code" not in col_names4:
                    # 第一步：添加新列
                    conn.execute(
                        text("ALTER TABLE index_prices ADD COLUMN index_code VARCHAR(50)")
                    )
                    conn.execute(
                        text("ALTER TABLE index_prices ADD COLUMN index_name VARCHAR(200)")
                    )
                    # 填充新字段
                    conn.execute(
                        text("""
                            UPDATE index_prices 
                            SET index_code = (
                                SELECT code FROM indices WHERE indices.id = index_prices.index_id
                            ),
                            index_name = (
                                SELECT COALESCE(name, code) FROM indices WHERE indices.id = index_prices.index_id
                            )
                            WHERE index_code IS NULL
                        """)
                    )
                    conn.commit()
                
                # 第二步：重建表以删除 index_id 列
                # 创建新表（不包含 index_id）
                conn.execute(
                    text("""
                        CREATE TABLE index_prices_new (
                            id INTEGER NOT NULL PRIMARY KEY,
                            index_code VARCHAR(50) NOT NULL,
                            index_name VARCHAR(200) NOT NULL,
                            trade_date DATE NOT NULL,
                            close FLOAT NOT NULL,
                            ma_30 FLOAT,
                            ma_60 FLOAT,
                            ma_120 FLOAT,
                            ma_180 FLOAT,
                            ma_360 FLOAT,
                            created_at DATETIME NOT NULL DEFAULT (datetime('now')),
                            updated_at DATETIME NOT NULL DEFAULT (datetime('now'))
                        )
                    """)
                )
                # 复制数据（排除 index_id）
                conn.execute(
                    text("""
                        INSERT INTO index_prices_new 
                        (id, index_code, index_name, trade_date, close, ma_30, ma_60, ma_120, ma_180, ma_360, created_at, updated_at)
                        SELECT id, index_code, index_name, trade_date, close, ma_30, ma_60, ma_120, ma_180, ma_360, created_at, updated_at
                        FROM index_prices
                    """)
                )
                # 创建唯一约束
                conn.execute(
                    text("CREATE UNIQUE INDEX uq_index_date ON index_prices_new (index_code, trade_date)")
                )
                # 创建其他索引
                conn.execute(
                    text("CREATE INDEX ix_index_prices_new_id ON index_prices_new (id)")
                )
                conn.execute(
                    text("CREATE INDEX ix_index_prices_new_index_code ON index_prices_new (index_code)")
                )
                conn.execute(
                    text("CREATE INDEX ix_index_prices_new_trade_date ON index_prices_new (trade_date)")
                )
                # 删除旧表
                conn.execute(text("DROP TABLE index_prices"))
                # 重命名新表
                conn.execute(text("ALTER TABLE index_prices_new RENAME TO index_prices"))
                conn.commit()

            # 5) 添加基础数据字段（涨跌幅、市盈率）
            cols5 = conn.execute(text("PRAGMA table_info('index_prices')")).fetchall()
            col_names5 = {row[1] for row in cols5}
            
            if "change_pct" not in col_names5:
                conn.execute(text("ALTER TABLE index_prices ADD COLUMN change_pct FLOAT"))
            if "pe_ratio" not in col_names5:
                conn.execute(text("ALTER TABLE index_prices ADD COLUMN pe_ratio FLOAT"))
            
            # 6) 添加均线和点位差字段
            if "ma_90" not in col_names5:
                conn.execute(text("ALTER TABLE index_prices ADD COLUMN ma_90 FLOAT"))
            if "ma_150" not in col_names5:
                conn.execute(text("ALTER TABLE index_prices ADD COLUMN ma_150 FLOAT"))
            if "ma30_diff" not in col_names5:
                conn.execute(text("ALTER TABLE index_prices ADD COLUMN ma30_diff FLOAT"))
            if "ma60_diff" not in col_names5:
                conn.execute(text("ALTER TABLE index_prices ADD COLUMN ma60_diff FLOAT"))
            if "ma90_diff" not in col_names5:
                conn.execute(text("ALTER TABLE index_prices ADD COLUMN ma90_diff FLOAT"))
            if "ma120_diff" not in col_names5:
                conn.execute(text("ALTER TABLE index_prices ADD COLUMN ma120_diff FLOAT"))
            if "ma150_diff" not in col_names5:
                conn.execute(text("ALTER TABLE index_prices ADD COLUMN ma150_diff FLOAT"))
            if "ma180_diff" not in col_names5:
                conn.execute(text("ALTER TABLE index_prices ADD COLUMN ma180_diff FLOAT"))
            if "ma360_diff" not in col_names5:
                conn.execute(text("ALTER TABLE index_prices ADD COLUMN ma360_diff FLOAT"))
            conn.commit()

            # 7) live_strategies 增加 email_subscribers
            cols6 = conn.execute(text("PRAGMA table_info('live_strategies')")).fetchall()
            if cols6:
                col_names6 = {row[1] for row in cols6}
                if "email_subscribers" not in col_names6:
                    conn.execute(
                        text("ALTER TABLE live_strategies ADD COLUMN email_subscribers TEXT")
                    )
                    conn.commit()

            # 8) contacts 表
            conn.execute(
                text("""
                    CREATE TABLE IF NOT EXISTS contacts (
                        id INTEGER NOT NULL PRIMARY KEY,
                        nickname VARCHAR(64) NOT NULL,
                        email VARCHAR(255) NOT NULL,
                        created_at DATETIME NOT NULL DEFAULT (datetime('now')),
                        updated_at DATETIME NOT NULL DEFAULT (datetime('now'))
                    )
                """)
            )
            conn.execute(
                text("CREATE UNIQUE INDEX IF NOT EXISTS uq_contact_email ON contacts (email)")
            )
            cols_contacts = conn.execute(text("PRAGMA table_info('contacts')")).fetchall()
            if cols_contacts:
                col_names_contacts = {row[1] for row in cols_contacts}
                if "phone" not in col_names_contacts:
                    conn.execute(text("ALTER TABLE contacts ADD COLUMN phone VARCHAR(32)"))
                if "email_provider" not in col_names_contacts:
                    conn.execute(text("ALTER TABLE contacts ADD COLUMN email_provider VARCHAR(32)"))
                if "email_user" not in col_names_contacts:
                    conn.execute(text("ALTER TABLE contacts ADD COLUMN email_user VARCHAR(255)"))
                if "email_password" not in col_names_contacts:
                    conn.execute(text("ALTER TABLE contacts ADD COLUMN email_password VARCHAR(255)"))
                if "email_sender" not in col_names_contacts:
                    conn.execute(text("ALTER TABLE contacts ADD COLUMN email_sender VARCHAR(255)"))
                if "email_port" not in col_names_contacts:
                    conn.execute(text("ALTER TABLE contacts ADD COLUMN email_port INTEGER"))
                if "email_use_ssl" not in col_names_contacts:
                    conn.execute(text("ALTER TABLE contacts ADD COLUMN email_use_ssl BOOLEAN DEFAULT 0"))
                conn.commit()

            # 9) email_configs 表
            conn.execute(
                text("""
                    CREATE TABLE IF NOT EXISTS email_configs (
                        id INTEGER NOT NULL PRIMARY KEY,
                        host VARCHAR(255) NOT NULL,
                        port INTEGER NOT NULL DEFAULT 587,
                        username VARCHAR(255),
                        password VARCHAR(255),
                        sender VARCHAR(255) NOT NULL,
                        use_tls BOOLEAN DEFAULT 1,
                        created_at DATETIME NOT NULL DEFAULT (datetime('now')),
                        updated_at DATETIME NOT NULL DEFAULT (datetime('now'))
                    )
                """)
            )
            conn.commit()


@event.listens_for(SessionLocal, "after_flush")
def _audit_after_flush(session: Session, flush_context) -> None:
    """
    在每次 flush 后，记录本次事务中的 INSERT / UPDATE / DELETE。
    """
    # 为避免循环日志，不记录日志表本身的操作
    from .models import DbAuditLog  # 局部导入以避免循环依赖

    def _make_pk(obj) -> Optional[str]:
        state = inspect(obj)
        if not state.identity:
            return None
        return ",".join(str(v) for v in state.identity)

    def _add_log(obj, op: str) -> None:
        if isinstance(obj, DbAuditLog):
            return
        table_name = getattr(obj, "__tablename__", obj.__class__.__name__)
        pk = _make_pk(obj)
        operator = session.info.get("operator", "system")
        # 这里 detail 暂时为空，如需更详细的变更信息可扩展
        session.add(
            DbAuditLog(
                table_name=table_name,
                operation=op,
                operator=operator,
                row_pk=pk,
                detail=None,
            )
        )

    for obj in session.new:
        _add_log(obj, "INSERT")
    for obj in session.dirty:
        if session.is_modified(obj, include_collections=False):
            _add_log(obj, "UPDATE")

    for obj in session.deleted:
        _add_log(obj, "DELETE")
