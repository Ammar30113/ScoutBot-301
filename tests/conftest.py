"""Tests use explicit provider fixtures and must never call external services."""
import pytest


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    def denied(*args, **kwargs):
        raise AssertionError("Unexpected network access in offline test")
    monkeypatch.setattr("requests.sessions.Session.request", denied)
    monkeypatch.setattr("httpx.Client.send", denied)
