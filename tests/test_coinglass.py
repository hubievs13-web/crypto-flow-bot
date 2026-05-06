"""Tests for the Coinglass cross-exchange liquidation client."""

from __future__ import annotations

import httpx
import pytest

from crypto_flow_bot.data.coinglass import CoinglassClient, base_coin_from_symbol


def test_base_coin_from_symbol():
    assert base_coin_from_symbol("BTCUSDT") == "BTC"
    assert base_coin_from_symbol("ETHUSDT") == "ETH"
    assert base_coin_from_symbol("SOLUSDT") == "SOL"
    assert base_coin_from_symbol("BTCUSD") == "BTC"
    assert base_coin_from_symbol("BTCUSDC") == "BTC"
    # Unknown suffix -> passthrough.
    assert base_coin_from_symbol("WEIRD") == "WEIRD"


def _mock_transport(handler):
    return httpx.MockTransport(handler)


def _make_client(handler) -> CoinglassClient:
    http = httpx.AsyncClient(
        base_url="https://open-api-v4.coinglass.com",
        transport=_mock_transport(handler),
        timeout=5.0,
    )
    return CoinglassClient(api_key="dummy", http=http)


@pytest.mark.asyncio
async def test_aggregated_liquidations_happy_path():
    def handler(request: httpx.Request) -> httpx.Response:
        # Sanity-check the auth header and query params.
        assert request.headers.get("CG-API-KEY") == "dummy"
        assert "exchange_list" in request.url.params
        assert request.url.params["symbol"] == "BTC"
        return httpx.Response(
            200,
            json={
                "code": "0",
                "msg": "success",
                "data": [
                    {"time": 1, "long_liquidation_usd": "100", "short_liquidation_usd": "200"},
                    {"time": 2, "long_liquidation_usd": "1234567.89", "short_liquidation_usd": "987654.32"},
                ],
            },
        )

    client = _make_client(handler)
    out = await client.aggregated_liquidations("BTC")
    assert out is not None
    long_usd, short_usd = out
    assert abs(long_usd - 1_234_567.89) < 1e-3
    assert abs(short_usd - 987_654.32) < 1e-3
    await client.aclose()


@pytest.mark.asyncio
async def test_aggregated_liquidations_returns_none_on_api_error():
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"code": "30001", "msg": "rate limit", "data": []})

    client = _make_client(handler)
    assert await client.aggregated_liquidations("BTC") is None
    await client.aclose()


@pytest.mark.asyncio
async def test_aggregated_liquidations_returns_none_on_http_error():
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(401, json={"code": "401", "msg": "Unauthorized"})

    client = _make_client(handler)
    assert await client.aggregated_liquidations("BTC") is None
    await client.aclose()


@pytest.mark.asyncio
async def test_aggregated_liquidations_returns_none_on_empty_data():
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"code": "0", "msg": "success", "data": []})

    client = _make_client(handler)
    assert await client.aggregated_liquidations("BTC") is None
    await client.aclose()


def test_from_env_returns_none_without_key(monkeypatch):
    monkeypatch.delenv("COINGLASS_API_KEY", raising=False)
    assert CoinglassClient.from_env() is None


def test_from_env_returns_client_with_key(monkeypatch):
    monkeypatch.setenv("COINGLASS_API_KEY", "abc")
    c = CoinglassClient.from_env()
    assert c is not None
