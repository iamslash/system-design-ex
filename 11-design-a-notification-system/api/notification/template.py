"""Notification template engine with variable substitution.

Replaces {variable_name} placeholders in template titles and bodies with actual values.
New templates can be added to the TEMPLATES dictionary.
"""

from __future__ import annotations

from typing import Any

# Notification template definitions per channel.
# Each template has a title and body, supporting {variable_name} format placeholders.
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
    """Render a template title and body using the given parameters.

    Args:
        template_name: Key of the template defined in TEMPLATES.
        params: Dictionary of values to substitute for placeholders.

    Returns:
        Rendered result as {"title": "...", "body": "..."}.

    Examples:
        >>> render_template("welcome", {"name": "Alice"})
        {'title': 'Welcome, Alice!', 'body': 'Hi Alice, welcome to our service. We are glad to have you!'}
    """
    tmpl = TEMPLATES.get(template_name, TEMPLATES["default"])
    title = tmpl["title"].format_map(_SafeDict(params))
    body = tmpl["body"].format_map(_SafeDict(params))
    return {"title": title, "body": body}


class _SafeDict(dict):
    """A dict subclass that leaves missing keys as {key} instead of raising KeyError.

    Prevents KeyError when used with format_map.
    """

    def __missing__(self, key: str) -> str:
        return "{" + key + "}"
