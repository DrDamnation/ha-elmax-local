"""Test AuthManager."""
from __future__ import annotations

import base64
import json
import time

import pytest
from aioresponses import aioresponses

from custom_components.elmax_local.auth import AuthManager, ElmaxAuthError


def _make_jwt(exp_offset: int = 3600) -> str:
    header = base64.urlsafe_b64encode(b'{"alg":"HS256"}').rstrip(b"=").decode()
    payload = base64.urlsafe_b64encode(
        json.dumps({"exp": int(time.time()) + exp_offset, "sub": "test"}).encode()
    ).rstrip(b"=").decode()
    return f"{header}.{payload}.fakesig"


@pytest.fixture
def auth(hass):
    return AuthManager(hass, "1.2.3.4", "000000")


def test_parse_exp_valid(auth):
    exp = auth._parse_exp(_make_jwt(3600))
    assert abs(exp - (time.time() + 3600)) < 5


def test_parse_exp_malformed(auth):
    exp = auth._parse_exp("not.a.jwt")
    assert abs(exp - (time.time() + 3000)) < 5


def test_parse_exp_missing_claim(auth):
    header = base64.urlsafe_b64encode(b'{"alg":"HS256"}').rstrip(b"=").decode()
    payload = base64.urlsafe_b64encode(b'{"sub":"test"}').rstrip(b"=").decode()
    exp = auth._parse_exp(f"{header}.{payload}.sig")
    assert abs(exp - (time.time() + 3000)) < 5


async def test_login_success(auth):
    jwt = _make_jwt(3600)
    with aioresponses() as m:
        m.post("https://1.2.3.4/api/v2/login", payload={"token": f"JWT {jwt}"})
        token = await auth.async_get_token()
        assert token == jwt
        assert auth._expiry > time.time() + 3500
    await auth.async_close()


async def test_token_cached(auth):
    jwt = _make_jwt(3600)
    with aioresponses() as m:
        m.post("https://1.2.3.4/api/v2/login", payload={"token": f"JWT {jwt}"})
        t1 = await auth.async_get_token()
        t2 = await auth.async_get_token()
        assert t1 == t2
    await auth.async_close()


async def test_refresh_within_margin(auth):
    jwt_old = _make_jwt(3600)
    jwt_new = _make_jwt(3600)
    with aioresponses() as m:
        m.post("https://1.2.3.4/api/v2/login", payload={"token": f"JWT {jwt_old}"})
        m.post("https://1.2.3.4/api/v2/refresh", payload={"token": f"JWT {jwt_new}"})
        await auth.async_get_token()
        auth._expiry = time.time() + 300  # within REFRESH_MARGIN
        token = await auth.async_get_token()
        assert token == jwt_new
    await auth.async_close()


async def test_handle_401_invalidates(auth):
    jwt = _make_jwt(3600)
    with aioresponses() as m:
        m.post("https://1.2.3.4/api/v2/login", payload={"token": f"JWT {jwt}"})
        await auth.async_get_token()
        await auth.async_handle_401()
        assert auth._token is None
        assert auth._expiry == 0
    await auth.async_close()


async def test_login_403_triggers_backoff(auth):
    with aioresponses() as m:
        m.post("https://1.2.3.4/api/v2/login", status=403,
               payload={"message": "Forbidden"})
        with pytest.raises(ElmaxAuthError):
            await auth.async_get_token()
        assert auth._login_fail_count == 1
        assert auth._blocked_until > time.time()
    await auth.async_close()
