from __future__ import annotations

import logging
import sys
import smtplib
from email.message import EmailMessage
from typing import Iterable

from .config import (
    EMAIL_HOST,
    EMAIL_PORT,
    EMAIL_USER,
    EMAIL_PASSWORD,
    EMAIL_FROM,
    EMAIL_TO,
    EMAIL_USE_TLS,
)

_log = logging.getLogger("fund_analyzer.emailer")
if not _log.handlers:
    _h = logging.StreamHandler(sys.stderr)
    _h.setFormatter(logging.Formatter("[%(asctime)s] [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
    _log.addHandler(_h)
    _log.setLevel(logging.INFO)


def _load_email_config() -> dict:
    try:
        from .database import SessionLocal
        from .models import EmailConfig
    except Exception:
        return {}
    db = SessionLocal()
    try:
        cfg = db.query(EmailConfig).order_by(EmailConfig.id.desc()).first()
        if not cfg:
            return {}
        return {
            "host": cfg.host,
            "port": cfg.port,
            "username": cfg.username or "",
            "password": cfg.password or "",
            "sender": cfg.sender,
            "use_tls": bool(cfg.use_tls),
        }
    finally:
        db.close()


def _get_recipients(extra: Iterable[str] | None = None) -> list[str]:
    recipients = list(EMAIL_TO)
    if extra:
        for r in extra:
            if r and r not in recipients:
                recipients.append(r)
    return recipients


def _provider_defaults(provider: str | None) -> dict:
    if not provider:
        return {}
    mapping = {
        "163": {"host": "smtp.163.com", "port": 587, "use_tls": True, "use_ssl": False},
        "qq": {"host": "smtp.qq.com", "port": 587, "use_tls": True, "use_ssl": False},
        "gmail": {"host": "smtp.gmail.com", "port": 587, "use_tls": True, "use_ssl": False},
    }
    return mapping.get(provider.lower(), {})


def _resolve_smtp_config(smtp_config: dict | None) -> dict:
    cfg = _load_email_config()
    if smtp_config:
        provider_cfg = _provider_defaults(smtp_config.get("provider"))
        host = smtp_config.get("host") or provider_cfg.get("host") or cfg.get("host", EMAIL_HOST)
        port = smtp_config.get("port") or provider_cfg.get("port") or cfg.get("port", EMAIL_PORT)
        username = smtp_config.get("username") or cfg.get("username", EMAIL_USER)
        password = smtp_config.get("password") or cfg.get("password", EMAIL_PASSWORD)
        sender = smtp_config.get("sender") or cfg.get("sender", EMAIL_FROM)
        use_ssl = (
            smtp_config.get("use_ssl")
            if smtp_config.get("use_ssl") is not None
            else provider_cfg.get("use_ssl", False)
        )
        use_tls = (
            smtp_config.get("use_tls")
            if smtp_config.get("use_tls") is not None
            else provider_cfg.get("use_tls", cfg.get("use_tls", EMAIL_USE_TLS))
        )
    else:
        host = cfg.get("host", EMAIL_HOST)
        port = cfg.get("port", EMAIL_PORT)
        username = cfg.get("username", EMAIL_USER)
        password = cfg.get("password", EMAIL_PASSWORD)
        sender = cfg.get("sender", EMAIL_FROM)
        use_ssl = False
        use_tls = cfg.get("use_tls", EMAIL_USE_TLS)

    if port == 465:
        use_ssl = True
        use_tls = False

    return {
        "host": host,
        "port": port,
        "username": username,
        "password": password,
        "sender": sender,
        "use_tls": use_tls,
        "use_ssl": use_ssl,
    }


def send_email(
    subject: str,
    content: str,
    to_list: Iterable[str] | None = None,
    smtp_config: dict | None = None,
) -> bool:
    safe_config = None
    if smtp_config:
        safe_config = dict(smtp_config)
        if "password" in safe_config and safe_config["password"]:
            safe_config["password"] = "***"
    _log.info("[邮件] 发送参数: to_list=%s smtp_config=%s", list(to_list or []), safe_config)
    resolved = _resolve_smtp_config(smtp_config)
    host = resolved["host"]
    port = resolved["port"]
    username = resolved["username"]
    password = resolved["password"]
    sender = resolved["sender"]
    use_tls = resolved["use_tls"]
    use_ssl = resolved["use_ssl"]

    _log.info(
        "[邮件] 发送请求: host=%s port=%s from=%s tls=%s ssl=%s login=%s",
        host or "<empty>",
        port,
        sender or "<empty>",
        use_tls,
        use_ssl,
        bool(username and password),
    )
    if not host or not sender:
        _log.warning("[邮件] 未配置 EMAIL_HOST 或 EMAIL_FROM")
        return False
    recipients = _get_recipients(to_list)
    if not recipients:
        _log.warning("[邮件] 无收件人，EMAIL_TO 与订阅列表均为空")
        return False

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = ", ".join(recipients)
    msg.set_content(content)

    try:
        server = smtplib.SMTP_SSL(host, port, timeout=15) if use_ssl else smtplib.SMTP(host, port, timeout=15)
        if use_tls and not use_ssl:
            server.starttls()
        if username and password:
            server.login(username, password)
        server.send_message(msg)
        _log.info("[邮件] 发送成功：to=%s subject=%s", recipients, subject)
        return True
    except Exception as exc:
        _log.exception("[邮件] 发送失败：%s", exc)
        return False
    finally:
        try:
            if "server" in locals():
                server.quit()
        except Exception:
            pass


def test_smtp_connection(smtp_config: dict | None = None) -> tuple[bool, str]:
    resolved = _resolve_smtp_config(smtp_config)
    host = resolved["host"]
    port = resolved["port"]
    username = resolved["username"]
    password = resolved["password"]
    use_tls = resolved["use_tls"]
    use_ssl = resolved["use_ssl"]

    if not host:
        return False, "缺少 SMTP Host"
    try:
        server = smtplib.SMTP_SSL(host, port, timeout=10) if use_ssl else smtplib.SMTP(host, port, timeout=10)
        if use_tls and not use_ssl:
            server.starttls()
        if username and password:
            server.login(username, password)
        server.quit()
        return True, "连接成功"
    except Exception as exc:
        _log.exception("[邮件] 连接测试失败：%s", exc)
        return False, str(exc)
