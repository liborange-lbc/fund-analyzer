from datetime import date, datetime
from typing import Optional, Any

from pydantic import BaseModel


class IndexBase(BaseModel):
    code: str
    name: str
    start_date: Optional[date] = None


class IndexCreate(IndexBase):
    pass


class IndexRead(IndexBase):
    id: int
    created_at: datetime

    class Config:
        from_attributes = True


class IndexPriceRead(BaseModel):
    index_code: str
    index_name: str
    trade_date: date
    close: float
    change_pct: Optional[float] = None  # 当日涨跌幅（百分比）
    pe_ratio: Optional[float] = None  # 滚动市盈率
    ma_30: Optional[float] = None
    ma_60: Optional[float] = None
    ma_90: Optional[float] = None
    ma_120: Optional[float] = None
    ma_150: Optional[float] = None
    ma_180: Optional[float] = None
    ma_360: Optional[float] = None
    ma30_diff: Optional[float] = None
    ma60_diff: Optional[float] = None
    ma90_diff: Optional[float] = None
    ma120_diff: Optional[float] = None
    ma150_diff: Optional[float] = None
    ma180_diff: Optional[float] = None
    ma360_diff: Optional[float] = None

    class Config:
        from_attributes = True


class PriceQueryResponse(BaseModel):
    index: IndexRead
    prices: list[IndexPriceRead]


class UsIndexQuoteRead(BaseModel):
    """美股指数最新净值（来自 FRED）"""
    series_id: str
    name: str
    trade_date: date
    close: float


class TableInfo(BaseModel):
    name: str
    row_count: int


class TableDataResponse(BaseModel):
    table: str
    total: int
    offset: int
    limit: int
    columns: list[str]
    rows: list[dict[str, Any]]


class DbAuditLogRead(BaseModel):
    id: int
    table_name: str
    operation: str
    operator: Optional[str] = None
    row_pk: Optional[str] = None
    detail: Optional[str] = None
    created_at: datetime

    class Config:
        from_attributes = True


class AuditLogListResponse(BaseModel):
    total: int
    offset: int
    limit: int
    items: list[DbAuditLogRead]


class SyncJobLogRead(BaseModel):
    id: int
    job_type: str
    status: str
    started_at: datetime
    finished_at: Optional[datetime] = None
    total_indices: Optional[int] = None
    total_rows: Optional[int] = None
    source_summary: Optional[str] = None
    detail: Optional[str] = None

    class Config:
        from_attributes = True


class SyncJobLogListResponse(BaseModel):
    total: int
    offset: int
    limit: int
    items: list[SyncJobLogRead]


class TableRowUpdateRequest(BaseModel):
    primary_key_value: str
    updates: dict[str, Any]


class IndexDateStatus(BaseModel):
    """指数的日期数据状态"""
    index_id: int
    index_code: str
    index_name: str
    dates: list[str]  # 有数据的日期列表，格式：YYYY-MM-DD
    start_date: Optional[date] = None  # 指数配置的开始日期
    first_data_date: Optional[date] = None  # 实际数据的最早日期
    last_data_date: Optional[date] = None  # 实际数据的最晚日期
    prices: dict[str, IndexPriceRead] = {}  # 日期对应的价格信息，key为日期字符串 YYYY-MM-DD

    class Config:
        from_attributes = True


class CalendarDataPoint(BaseModel):
    """日历图数据点"""
    date: str  # 日期，格式：YYYY-MM-DD
    value: float  # 收盘价
    change_pct: Optional[float] = None  # 涨跌幅（百分比）

    class Config:
        from_attributes = True


class CalendarDataResponse(BaseModel):
    """日历图数据响应"""
    index: IndexRead
    year: int
    data: list[CalendarDataPoint]
    min_value: float
    max_value: float


# ========== 用户认证相关 Schema ==========

class UserCreate(BaseModel):
    """创建用户请求"""
    username: str
    password: str
    role: str = "user"  # admin / user

    class Config:
        from_attributes = True


class UserResponse(BaseModel):
    """用户信息返回（不含密码）"""
    id: int
    username: str
    role: str
    is_active: bool
    created_at: datetime

    class Config:
        from_attributes = True


class UserLogin(BaseModel):
    """登录请求"""
    username: str
    password: str


class Token(BaseModel):
    """JWT Token 返回"""
    access_token: str
    token_type: str = "bearer"


class TokenData(BaseModel):
    """Token 解析数据"""
    username: Optional[str] = None


class NotificationSettingRead(BaseModel):
    """通知开关（只读）"""
    event_type: str
    enabled: bool
    updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True


class NotificationSettingUpdate(BaseModel):
    """通知开关（更新）"""
    enabled: bool


class WechatSubscriberRead(BaseModel):
    """公众号订阅者"""
    openid: str
    nickname: Optional[str] = None


class SendCustomMessageRequest(BaseModel):
    """发送自定义消息请求"""
    openids: list[str]
    content: str