"""Diagnostic script for API key validation across data providers."""

import os
import requests


def check_env_vars() -> None:
    keys = ["MASSIVE_API_KEY", "FINNHUB_API_KEY"]
    print("🔍 Checking environment variables...\n")
    for key in keys:
        value = os.getenv(key)
        if value:
            print(f"✅ {key} detected ({len(value)} chars)")
        else:
            print(f"❌ {key} missing or not loaded")


def check_massive() -> None:
    key = os.getenv("MASSIVE_API_KEY")
    if not key:
        print("\n[Massive] ❌ Missing key, skipping test.")
        return
    url = "https://api.massive.com/v1/reference/markets"
    headers = {"Authorization": f"Bearer {key}"}
    try:
        response = requests.get(url, headers=headers, timeout=10)
        print(f"\n[Massive] Response: {response.status_code} → {response.reason}")
        if response.status_code == 200:
            print("✅ Massive API reachable and key is valid.")
        elif response.status_code == 401:
            print("🚫 Invalid Massive API key.")
        else:
            print(f"⚠️ Massive returned unexpected status: {response.status_code}")
    except Exception as exc:
        print(f"🔥 Massive connection error: {exc}")


def check_finnhub() -> None:
    key = os.getenv("FINNHUB_API_KEY")
    if not key:
        print("\n[Finnhub] ❌ Missing key, skipping test.")
        return
    url = f"https://finnhub.io/api/v1/quote?symbol=AAPL&token={key}"
    try:
        response = requests.get(url, timeout=10)
        print(f"\n[Finnhub] Response: {response.status_code} → {response.reason}")
        if response.status_code == 200:
            print("✅ Finnhub API reachable and key is valid.")
        elif response.status_code == 403:
            print("🚫 Finnhub key forbidden — expired or rate limited.")
        else:
            print(f"⚠️ Finnhub returned: {response.status_code}")
    except Exception as exc:
        print(f"🔥 Finnhub connection error: {exc}")


def main() -> None:
    check_env_vars()
    check_massive()
    check_finnhub()


if __name__ == "__main__":
    main()
