import os
import secrets
from dotenv import load_dotenv

load_dotenv()

# JWT 配置
SECRET_KEY = os.getenv("SECRET_KEY", secrets.token_urlsafe(32))
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", "1440"))  # 默认 24 小时


def get_database_url() -> str:
    """
    返回数据库连接字符串。

    优先使用环境变量 DATABASE_URL；
    若未设置，则默认使用当前目录下的 SQLite 文件：
    sqlite:///./fund_analyzer.db
    """
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        # 默认本地开发使用 SQLite，更易于启动；用户可在 .env 中覆盖为 MySQL 等
        db_url = "sqlite:///./fund_analyzer.db"
    return db_url


def get_scheduler_hour_minute() -> tuple[int, int]:
    """
    返回每日定时任务的时分，默认 18:00。
    可通过环境变量 SCHEDULE_HOUR, SCHEDULE_MINUTE 配置。
    """
    hour = int(os.getenv("SCHEDULE_HOUR", "18"))
    minute = int(os.getenv("SCHEDULE_MINUTE", "0"))
    return hour, minute


def get_default_indices() -> list[str]:
    """
    默认跟踪的指数代码。
    用户可通过接口动态添加删除。
    """
    codes = os.getenv(
        "DEFAULT_INDICES",
        "",  # 默认不自动添加任何指数，完全由用户自行配置
    )
    return [c.strip() for c in codes.split(",") if c.strip()]


# 微信公众号通知（模板消息，需服务号）
WECHAT_APPID = os.getenv("WECHAT_APPID", "").strip()
WECHAT_APPSECRET = os.getenv("WECHAT_APPSECRET", "").strip()
# 用于「新数据更新」的模板 ID（在公众平台 功能 - 模板消息 中申请）
WECHAT_TEMPLATE_ID_DATA_UPDATE = os.getenv("WECHAT_TEMPLATE_ID_DATA_UPDATE", "").strip()
# 用于「管理员自定义消息」的模板 ID（建议模板含：first、keyword1=内容、keyword2=时间）
WECHAT_TEMPLATE_ID_CUSTOM = os.getenv("WECHAT_TEMPLATE_ID_CUSTOM", "").strip()
# 未在 DB 中配置时，是否默认开启「新数据更新」通知（true/false）
WECHAT_NOTIFY_DATA_UPDATE_DEFAULT = os.getenv("WECHAT_NOTIFY_DATA_UPDATE", "false").lower() in ("1", "true", "yes")

# 美股指数数据源：FRED（美联储经济数据），需免费 API Key：https://fred.stlouisfed.org/docs/api/api_key.html
FRED_API_KEY = os.getenv("FRED_API_KEY", "").strip()
# 支持的 FRED 系列 ID（指数代码与 FRED series_id 一致时可拉取历史并参与同步）
FRED_US_INDICES = ["NASDAQCOM", "SP500", "DJIA"]  # 纳斯达克综合、标普500、道琼斯工业
FRED_US_INDEX_NAMES = {
    "NASDAQCOM": "纳斯达克综合指数",
    "SP500": "标普500",
    "DJIA": "道琼斯工业平均指数",
}

# 美股指数备选数据源：Alpha Vantage（免费 Key：https://www.alphavantage.co/support/#api-key，免费档约 100 条/日）
ALPHA_VANTAGE_API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY", "").strip()
# 本系统指数代码 -> Alpha Vantage 代码。IXIC=纳斯达克综合指数；若 IXIC 无数据可改为 "QQQ"（纳斯达克100 ETF，数值为 ETF 价格）
ALPHA_VANTAGE_SYMBOL_MAP = {
    "NASDAQCOM": "IXIC",
    "SP500": "SPY",
    "DJIA": "DIA",
}


# 邮件通知配置（用于实盘策略）
EMAIL_HOST = os.getenv("EMAIL_HOST", "").strip()
EMAIL_PORT = int(os.getenv("EMAIL_PORT", "587"))
EMAIL_USER = os.getenv("EMAIL_USER", "").strip()
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD", "").strip()
EMAIL_FROM = os.getenv("EMAIL_FROM", EMAIL_USER).strip()
EMAIL_TO = [x.strip() for x in os.getenv("EMAIL_TO", "").split(",") if x.strip()]
EMAIL_USE_TLS = os.getenv("EMAIL_USE_TLS", "true").lower() in ("1", "true", "yes")

