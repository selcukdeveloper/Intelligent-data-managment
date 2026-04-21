from __future__ import annotations

from functools import wraps
from typing import Callable

from flask import redirect, request, session, url_for, abort


USERS: dict[str, dict] = {
    "tier1": {"password": "tier1", "role": "tier1"},
    "tier2": {"password": "tier2", "role": "tier2"},
    "admin": {"password": "admin", "role": "admin"},
}

ROLES = ("tier1", "tier2", "admin")


GENERAL_QUERY_KEYS: set[str] = {
    "top_products_by_revenue",
    "monthly_revenue_trend",
    "top_customers",
    "revenue_by_category",
    "profit_by_state",
}


def can_run_custom(role: str | None) -> bool:
    return role in ("tier2", "admin")


def can_ingest(role: str | None) -> bool:
    return role == "admin"


def can_edit_queries(role: str | None) -> bool:
    return role == "admin"


def visible_query_keys(role: str | None) -> set[str] | None:
    if role == "tier1":
        return GENERAL_QUERY_KEYS
    return None


def check_credentials(username: str, password: str) -> str | None:
    user = USERS.get(username)
    if user is None:
        return None
    if user["password"] != password:
        return None
    return user["role"]


def current_user() -> dict | None:
    username = session.get("username")
    role = session.get("role")
    if not username or role not in ROLES:
        return None
    return {"username": username, "role": role}


def login_required(view: Callable) -> Callable:
    @wraps(view)
    def wrapper(*args, **kwargs):
        if current_user() is None:
            return redirect(url_for("login", next=request.path))
        return view(*args, **kwargs)
    return wrapper


def role_required(*allowed_roles: str) -> Callable:
    def decorator(view: Callable) -> Callable:
        @wraps(view)
        def wrapper(*args, **kwargs):
            user = current_user()
            if user is None:
                return redirect(url_for("login", next=request.path))
            if user["role"] not in allowed_roles:
                abort(403)
            return view(*args, **kwargs)
        return wrapper
    return decorator
