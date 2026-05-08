from sentiment import gpt_provider


def test_gpt_sentiment_returns_neutral_without_api_key(monkeypatch):
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.setattr(gpt_provider, "_client", None)
    monkeypatch.setattr(gpt_provider, "_missing_key_warned", False)

    assert gpt_provider.get_gpt_sentiment("AAPL") == 0.0
