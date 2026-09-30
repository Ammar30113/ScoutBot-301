from unittest.mock import Mock

import pytest

from core.cache import TTLCache
from data import price_router
from data.alpaca_provider import AlpacaProvider
from data.marketstack_provider import MarketstackProvider
from data.twelvedata_provider import TwelveDataProvider


def response(payload):
    result = Mock(status_code=200)
    result.json.return_value = payload
    return result


def test_alpaca_requests_recent_daily_history_explicitly(monkeypatch):
    provider = AlpacaProvider()
    provider.api_key, provider.api_secret = "test", "test"
    provider._rate_limit_until, provider._disabled = 0, False
    get = Mock(return_value=response({"bars": [dict(o=10, h=11, l=9, c=10, v=100, t="2024-07-03T04:00:00Z")]}))
    monkeypatch.setattr("data.alpaca_provider.requests.get", get)
    assert len(provider.get_aggregates("SPY", limit=260)) == 1
    params = get.call_args.kwargs["params"]
    assert params["start"] < params["end"]
    assert params["sort"] == "desc"
    assert params["limit"] == 260
    assert params["adjustment"] == "split"


def test_marketstack_https_v2_and_cache_respects_requested_length(monkeypatch):
    provider = MarketstackProvider()
    provider.api_key = "test"
    provider.cache = TTLCache()
    provider.cache.set("ms:1day:SPY:60", [{"close": 1}])
    get = Mock(return_value=response({"data": [dict(open=10, high=11, low=9, close=10, volume=100,
                      date="2024-07-03T00:00:00Z", split_factor=1)]}))
    monkeypatch.setattr("data.marketstack_provider.requests.get", get)
    assert provider.get_aggregates("SPY", limit=260)[0]["close"] == 10
    assert get.call_args.args[0] == "https://api.marketstack.com/v2/eod"


def test_twelve_explicit_timezone_adjustment_and_wrong_currency_rejected(monkeypatch):
    provider = TwelveDataProvider()
    provider.api_key = "test"
    provider.cache = TTLCache()
    get = Mock(return_value=response({"meta": {"currency": "CAD"}, "values": [dict(datetime="2024-07-03", open="10", high="11", low="9", close="10", volume="100")]}))
    monkeypatch.setattr("data.twelvedata_provider.requests.get", get)
    assert provider.get_aggregates("SPY") == []
    assert get.call_args.kwargs["params"]["adjust"] == "splits"
    assert get.call_args.kwargs["params"]["timezone"] == "America/New_York"


def test_router_does_not_fallback_or_merge_after_provider_failure(monkeypatch):
    td, ms = TwelveDataProvider(), MarketstackProvider()
    td.get_aggregates = Mock(return_value=[])
    ms.get_aggregates = Mock(side_effect=AssertionError("must not fall back"))
    monkeypatch.setattr(price_router.settings, "paper_data_provider", "twelvedata")
    monkeypatch.setattr(price_router, "cache", TTLCache())
    router = price_router.PriceRouter()
    router.providers = [td, ms]
    with pytest.raises(RuntimeError):
        router.get_daily_aggregates("SPY", 260)
    ms.get_aggregates.assert_not_called()
