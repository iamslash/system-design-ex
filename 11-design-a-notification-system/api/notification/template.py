"""Notification template engine with variable substitution.

템플릿에 정의된 제목/본문에서 {변수명} 을 실제 값으로 치환한다.
새로운 템플릿은 TEMPLATES 딕셔너리에 추가하면 된다.
"""

from __future__ import annotations

from typing import Any

# 채널별 알림 템플릿 정의
# 각 템플릿은 title 과 body 를 가지며, {변수명} 형식의 플레이스홀더를 지원한다.
TEMPLATES: dict[str, dict[str, str]] = {
    "welcome": {
        "title": "Welcome, {name}!",
        "body": "Hi {name}, welcome to our service. We are glad to have you!",
    },
    "payment": {
        "title": "Payment Received",
        "body": "Hi {name}, your payment of {amount} has been processed successfully.",
    },
    "shipping": {
        "title": "Order Shipped",
        "body": "Hi {name}, your order #{order_id} has been shipped. Tracking: {tracking}",
    },
    "default": {
        "title": "Notification",
        "body": "You have a new notification.",
    },
}


def render_template(template_name: str, params: dict[str, Any]) -> dict[str, str]:
    """템플릿 이름과 파라미터로 제목/본문을 렌더링한다.

    Args:
        template_name: TEMPLATES 에 정의된 템플릿 키.
        params: 플레이스홀더를 치환할 값 딕셔너리.

    Returns:
        {"title": "...", "body": "..."} 형태의 렌더링 결과.

    Examples:
        >>> render_template("welcome", {"name": "Alice"})
        {'title': 'Welcome, Alice!', 'body': 'Hi Alice, welcome to our service. We are glad to have you!'}
    """
    tmpl = TEMPLATES.get(template_name, TEMPLATES["default"])
    title = tmpl["title"].format_map(_SafeDict(params))
    body = tmpl["body"].format_map(_SafeDict(params))
    return {"title": title, "body": body}


class _SafeDict(dict):
    """누락된 키를 {key} 그대로 남기는 dict.

    format_map 에서 KeyError 를 방지한다.
    """

    def __missing__(self, key: str) -> str:
        return "{" + key + "}"
