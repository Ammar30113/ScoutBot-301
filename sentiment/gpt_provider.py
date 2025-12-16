import os
import logging
from openai import OpenAI
from openai import APIError, AuthenticationError, PermissionDeniedError

log = logging.getLogger(__name__)

# Primary model comes from env, default to a cheap/allowed model
PRIMARY_MODEL = os.getenv("OPENAI_MODEL", "gpt-3.5-turbo-16k")

# Fallback models MUST be restricted to those allowed in my project
FALLBACK_MODELS = [
    "gpt-4o-2024-05-13",
    "gpt-5",
]

client = OpenAI()


def get_gpt_sentiment(symbol: str, news: list[str] | None = None) -> float:
    """
    Query GPT for a sentiment score in [-1, 1] for a stock symbol.
    Uses PRIMARY_MODEL then allowed fallbacks; handles permission errors
    and other failures gracefully, returning 0.0 if everything fails.
    """
    models_to_try = [PRIMARY_MODEL] + FALLBACK_MODELS

    news_block = ""
    if news:
        limited_news = [item.strip() for item in news if item.strip()][:5]
        if limited_news:
            formatted_news = "\n".join(f"- {item}" for item in limited_news)
            news_block = (
                "\nUse the following curated Twitter headlines as supplemental context only if helpful. "
                "They are pre-filtered to the requested ticker:\n"
                f"{formatted_news}\n"
            )

    prompt = (
        f"Provide a sentiment score between -1 and 1 for the stock symbol {symbol} "
        f"based only on medium to long-term market perception and news, not on intraday price action. "
        f"{news_block}"
        f"Return ONLY the number, no text."
    )

    for model_name in models_to_try:
        try:
            log.info(f"sentiment.gpt_provider | Trying model {model_name} for {symbol}")

            response = client.chat.completions.create(
                model=model_name,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=5,
                temperature=0,
            )

            raw = response.choices[0].message.content.strip()

            try:
                value = float(raw)
                # Clamp to [-1, 1]
                value = max(-1.0, min(1.0, value))
                log.info(f"GPT sentiment for {symbol} = {value:.4f} (model={model_name})")
                return value
            except ValueError:
                log.warning(
                    f"sentiment.gpt_provider | Invalid GPT sentiment output '{raw}' "
                    f"for {symbol} (model={model_name})"
                )
                return 0.0

        except PermissionDeniedError as e:
            # Missing scope / model forbidden for this project
            log.warning(
                f"sentiment.gpt_provider | Skipping model {model_name} due to permission error: {e}. "
                f"Trying next fallback."
            )
            continue

        except AuthenticationError as e:
            # Bad API key or invalid auth; nothing else to do
            log.error(f"sentiment.gpt_provider | Authentication error with model {model_name}: {e}")
            return 0.0

        except APIError as e:
            # Transient errors; try next fallback if any
            log.warning(f"sentiment.gpt_provider | API error on model {model_name}: {e}. Trying next fallback.")
            continue

        except Exception as e:
            log.error(f"sentiment.gpt_provider | Unexpected error for model {model_name}: {e}")
            continue

    log.warning(f"sentiment.gpt_provider | All GPT models failed for {symbol}; returning neutral 0.0.")
    return 0.0
