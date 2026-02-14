"""
微信公众号模板消息通知。
需在公众平台配置 AppID、AppSecret，并申请模板消息获取 template_id。
仅服务号支持模板消息；订阅者需先关注公众号。
"""
from __future__ import annotations

import time
from typing import Optional

import requests

from .config import (
    WECHAT_APPID,
    WECHAT_APPSECRET,
    WECHAT_TEMPLATE_ID_DATA_UPDATE,
    WECHAT_TEMPLATE_ID_CUSTOM,
)
from .database import log_timestamp

# access_token 缓存，避免频繁请求
_token_cache: tuple[str, float] = ("", 0.0)
TOKEN_EXPIRE_SECONDS = 7000  # 微信 7200，提前 200 秒刷新


def _get_access_token() -> Optional[str]:
    """获取公众号 access_token（带缓存）。"""
    global _token_cache
    now = time.time()
    if _token_cache[0] and (now - _token_cache[1]) < TOKEN_EXPIRE_SECONDS:
        return _token_cache[0]
    if not WECHAT_APPID or not WECHAT_APPSECRET:
        return None
    url = "https://api.weixin.qq.com/cgi-bin/token"
    params = {
        "grant_type": "client_credential",
        "appid": WECHAT_APPID,
        "secret": WECHAT_APPSECRET,
    }
    try:
        resp = requests.get(url, params=params, timeout=10)
        data = resp.json()
        token = data.get("access_token")
        if token:
            _token_cache = (token, now)
            return token
    except Exception:
        pass
    return None


def _get_subscriber_openids(access_token: str) -> list[str]:
    """获取公众号关注者 openid 列表（分页拉取）。"""
    openids: list[str] = []
    next_openid = ""
    url = "https://api.weixin.qq.com/cgi-bin/user/get"
    try:
        while True:
            resp = requests.get(
                url,
                params={"access_token": access_token, "next_openid": next_openid},
                timeout=10,
            )
            data = resp.json()
            if data.get("errcode"):
                break
            data_list = data.get("data") or {}
            ids = data_list.get("openid") or []
            openids.extend(ids)
            next_openid = data.get("next_openid", "")
            if not next_openid or len(ids) == 0:
                break
    except Exception:
        pass
    return openids


def get_subscriber_list() -> list[dict]:
    """
    获取公众号订阅者列表，每项为 {"openid": str, "nickname": str | None}。
    nickname 通过批量拉取用户信息获取，失败或未配置时仅返回 openid。
    """
    token = _get_access_token()
    if not token:
        return []
    openids = _get_subscriber_openids(token)
    if not openids:
        return []
    # 可选：拉取昵称（逐个请求，数量多时较慢）
    result: list[dict] = []
    url = "https://api.weixin.qq.com/cgi-bin/user/info"
    for o in openids:
        nickname = None
        try:
            r = requests.get(
                url,
                params={"access_token": token, "openid": o, "lang": "zh_CN"},
                timeout=5,
            )
            d = r.json()
            if not d.get("errcode"):
                nickname = (d.get("nickname") or "").strip() or None
        except Exception:
            pass
        result.append({"openid": o, "nickname": nickname})
    return result


def send_custom_message(openids: list[str], content: str) -> tuple[int, int]:
    """
    向指定 openid 列表发送自定义模板消息。
    使用 WECHAT_TEMPLATE_ID_CUSTOM 模板，keyword1=内容，keyword2=时间。
    返回 (成功数, 失败数)。
    """
    if not openids or not content or not content.strip():
        return 0, 0
    if not WECHAT_APPID or not WECHAT_APPSECRET or not WECHAT_TEMPLATE_ID_CUSTOM:
        raise ValueError("未配置公众号或未配置自定义消息模板 ID（WECHAT_TEMPLATE_ID_CUSTOM）")
    token = _get_access_token()
    if not token:
        raise ValueError("获取 access_token 失败")
    time_str = log_timestamp()
    data = {
        "first": {"value": "管理员通知", "color": "#173177"},
        "keyword1": {"value": content.strip()[:200], "color": "#173177"},
        "keyword2": {"value": time_str, "color": "#173177"},
        "remark": {"value": "", "color": "#999999"},
    }
    success = 0
    for o in openids:
        if _send_template_message(token, o, WECHAT_TEMPLATE_ID_CUSTOM, data):
            success += 1
    return success, len(openids) - success


def _send_template_message(
    access_token: str,
    openid: str,
    template_id: str,
    data: dict,
    url: Optional[str] = None,
) -> bool:
    """
    发送模板消息。
    data 格式: {"first": {"value": "..."}, "keyword1": {"value": "..."}, ...}
    """
    post_url = f"https://api.weixin.qq.com/cgi-bin/message/template/send?access_token={access_token}"
    body = {
        "touser": openid,
        "template_id": template_id,
        "data": data,
    }
    if url:
        body["url"] = url
    try:
        resp = requests.post(post_url, json=body, timeout=10)
        result = resp.json()
        return result.get("errcode") == 0
    except Exception:
        return False


# 新数据更新模板建议格式（公众平台申请时选类似字段）：
# first: 基金指数数据更新提醒
# keyword1: 新数据更新
# keyword2: 更新指数与条数摘要
# keyword3: 时间
# remark: 点击查看详情
def notify_data_update(
    updates: list[tuple[str, str, int]],
    *,
    base_url: Optional[str] = None,
) -> bool:
    """
    向所有订阅者发送「新数据更新」模板消息。
    updates: [(index_code, index_name, row_count), ...]
    若未配置公众号或未开启该事件，返回 False。
    """
    if not updates:
        return False
    if not WECHAT_APPID or not WECHAT_APPSECRET or not WECHAT_TEMPLATE_ID_DATA_UPDATE:
        return False

    from .database import SessionLocal
    from .models import NotificationSetting

    db = SessionLocal()
    try:
        setting = db.query(NotificationSetting).filter(
            NotificationSetting.event_type == "data_update"
        ).one_or_none()
        if setting is None:
            from .config import WECHAT_NOTIFY_DATA_UPDATE_DEFAULT
            if not WECHAT_NOTIFY_DATA_UPDATE_DEFAULT:
                return False
        elif not setting.enabled:
            return False
    finally:
        db.close()

    token = _get_access_token()
    if not token:
        print(f"{log_timestamp()} [微信] 获取 access_token 失败")
        return False
    openids = _get_subscriber_openids(token)
    if not openids:
        print(f"{log_timestamp()} [微信] 无订阅者 openid")
        return True  # 视为发送逻辑已执行

    # 摘要：最多展示前 5 个指数及总条数
    total_rows = sum(u[2] for u in updates)
    parts = [f"{code}({name})+{cnt}条" for code, name, cnt in updates[:5]]
    if len(updates) > 5:
        parts.append(f"等共{len(updates)}个指数")
    summary = "；".join(parts) + f"，共 {total_rows} 条"
    time_str = log_timestamp()

    data = {
        "first": {"value": "基金指数数据更新提醒", "color": "#173177"},
        "keyword1": {"value": "新数据更新", "color": "#173177"},
        "keyword2": {"value": summary, "color": "#173177"},
        "keyword3": {"value": time_str, "color": "#173177"},
        "remark": {"value": "点击查看详情" if base_url else "请登录系统查看", "color": "#999999"},
    }
    success = 0
    for openid in openids:
        if _send_template_message(
            token, openid, WECHAT_TEMPLATE_ID_DATA_UPDATE, data, url=base_url
        ):
            success += 1
    print(f"{log_timestamp()} [微信] 新数据更新通知已发送: {success}/{len(openids)} 人")
    return success > 0
