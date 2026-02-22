from datetime import date, datetime
from typing import Optional, Any

from pydantic import BaseModel


class IndexBase(BaseModel):
    code: str
    name: str
    start_date: Optional[date] = None
    source: Optional[str] = None


class IndexCreate(IndexBase):
    pass


class IndexUpdate(BaseModel):
    start_date: Optional[date] = None
    source: Optional[str] = None


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


# ========== 策略回测相关 Schema ==========

class DcaConfig(BaseModel):
    amount: float
    frequency: str  # daily / weekly / monthly


class SmartDcaConfig(BaseModel):
    base_amount: float
    frequency: str  # daily / weekly / monthly
    step_pct: float
    max_multiplier: float


class MeanReversionConfig(BaseModel):
    ma_period: int
    buy_threshold_pct: float
    sell_threshold_pct: float


class TrendFollowingConfig(BaseModel):
    ma_period: int


class MeanReversionMultiConfig(BaseModel):
    ma_period: int
    step_pct: float
    order_amount: float


class MaDiffConfig(BaseModel):
    ma_period: int
    buy_diff: float
    sell_diff: float


class BacktestRequest(BaseModel):
    index_id: int
    strategy: str  # dca / smart_dca / mean_reversion / mean_reversion_multi / ma_diff / trend_following
    initial_cash: float
    start_date: Optional[date] = None
    end_date: Optional[date] = None
    dca: Optional[DcaConfig] = None
    smart_dca: Optional[SmartDcaConfig] = None
    mean_reversion: Optional[MeanReversionConfig] = None
    mean_reversion_multi: Optional[MeanReversionMultiConfig] = None
    ma_diff: Optional[MaDiffConfig] = None
    trend_following: Optional[TrendFollowingConfig] = None


class BacktestPointRead(BaseModel):
    trade_date: date
    price: float
    cash: float
    position: float
    value: float


class BacktestTradeRead(BaseModel):
    trade_date: date
    action: str
    price: float
    shares: float
    cash_after: float
    position_after: float
    reason: str


class BacktestMetrics(BaseModel):
    start_date: date
    end_date: date
    total_days: int
    total_invested: float
    total_position_value: float
    total_profit: float
    final_value: float
    total_return_pct: float
    annualized_return_pct: float
    max_drawdown_pct: float
    max_drawdown_start: Optional[date] = None
    max_drawdown_end: Optional[date] = None


class BacktestResponse(BaseModel):
    index: IndexRead
    strategy: str
    params: dict[str, Any]
    metrics: BacktestMetrics
    equity_curve: list[BacktestPointRead]
    trades: list[BacktestTradeRead]


# ========== 实盘策略相关 Schema ==========

class LiveStrategyCreate(BaseModel):
    name: str
    index_id: int
    strategy: str
    params: dict[str, Any]
    enabled: bool = True
    email_subscribers: Optional[list[int]] = None


class LiveStrategyUpdate(BaseModel):
    name: Optional[str] = None
    params: Optional[dict[str, Any]] = None
    enabled: Optional[bool] = None
    email_subscribers: Optional[list[int]] = None


class LiveStrategyRead(BaseModel):
    id: int
    name: str
    index_id: int
    index_code: str
    index_name: str
    strategy: str
    params: Optional[dict[str, Any]] = None
    email_subscribers: Optional[list[int]] = None
    enabled: bool
    last_run_at: Optional[datetime] = None
    last_signal_date: Optional[date] = None
    last_signal_action: Optional[str] = None
    last_signal_price: Optional[float] = None
    last_signal_detail: Optional[str] = None
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class LiveStrategyFromBacktest(BaseModel):
    name: Optional[str] = None
    backtest: BacktestRequest


# ========== 联系人 / 邮箱配置 ==========

class ContactCreate(BaseModel):
    nickname: str
    email: str
    phone: Optional[str] = None
    email_provider: Optional[str] = None
    email_port: Optional[int] = None
    email_use_ssl: Optional[bool] = None
    email_user: Optional[str] = None
    email_password: Optional[str] = None
    email_sender: Optional[str] = None


class ContactUpdate(BaseModel):
    nickname: Optional[str] = None
    email: Optional[str] = None
    phone: Optional[str] = None
    email_provider: Optional[str] = None
    email_port: Optional[int] = None
    email_use_ssl: Optional[bool] = None
    email_user: Optional[str] = None
    email_password: Optional[str] = None
    email_sender: Optional[str] = None


class ContactRead(BaseModel):
    id: int
    nickname: str
    email: str
    phone: Optional[str] = None
    email_provider: Optional[str] = None
    email_port: Optional[int] = None
    email_use_ssl: Optional[bool] = None
    email_user: Optional[str] = None
    email_sender: Optional[str] = None
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class EmailConfigUpdate(BaseModel):
    host: str
    port: int
    username: Optional[str] = None
    password: Optional[str] = None
    sender: str
    use_tls: bool = True


class EmailConfigRead(BaseModel):
    host: str
    port: int
    username: Optional[str] = None
    sender: str
    use_tls: bool = True


# ========== 监控看板 ==========

class MonitorDailyChangeItem(BaseModel):
    index_code: str
    index_name: str
    close: Optional[float] = None
    change_pct: Optional[float] = None


class MonitorRiskAlertItem(BaseModel):
    index_code: str
    index_name: str
    reason: str
    change_pct: Optional[float] = None
    ma60_diff: Optional[float] = None


class MonitorStrategyTodoItem(BaseModel):
    strategy_id: int
    name: str
    index_code: str
    index_name: str
    action: str
    detail: Optional[str] = None


class MonitorDailySummary(BaseModel):
    date: Optional[str] = None
    total_indices: int
    up_count: int
    down_count: int
    flat_count: int
    top_gainers: list[MonitorDailyChangeItem]
    top_losers: list[MonitorDailyChangeItem]


class MonitorDashboard(BaseModel):
    date: Optional[str] = None
    daily_summary: MonitorDailySummary
    risk_alerts: list[MonitorRiskAlertItem]
    strategy_todos: list[MonitorStrategyTodoItem]