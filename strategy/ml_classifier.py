from __future__ import annotations

from pathlib import Path
from typing import Dict, Iterable, List

import joblib
import numpy as np
import pandas as pd
from ta.volatility import AverageTrueRange
from xgboost import XGBClassifier

from core.logger import get_logger

logger = get_logger(__name__)

MODEL_PATH = Path("models/microcap_model.pkl")
FEATURE_COLUMNS = [
    "return_5d",
    "return_10d",
    "return_20d",
    "volatility_20d",
    "relative_volume",
    "atr",
    "atr_pct",
    "finnhub_sentiment",
    "newsapi_sentiment",
    "etf_relative_strength",
    "liquidity_bucket",
]


class MLClassifier:
    def __init__(self, model_path: Path = MODEL_PATH) -> None:
        self.model_path = model_path
        self.model = self._load_or_train_model()

    def _load_or_train_model(self) -> XGBClassifier:
        self.model_path.parent.mkdir(parents=True, exist_ok=True)
        if self.model_path.exists():
            try:
                return joblib.load(self.model_path)
            except Exception as exc:  # pragma: no cover - defensive log
                logger.warning(
                    "Existing ML model %s is unreadable (%s); retraining placeholder",
                    self.model_path,
                    exc,
                )
        else:
            logger.warning("ML model missing; training placeholder classifier")
        model = self._train_placeholder_model()
        joblib.dump(model, self.model_path)
        return model

    def _train_placeholder_model(self) -> XGBClassifier:
        rng = np.random.default_rng(42)
        samples = 2000
        X = rng.normal(size=(samples, len(FEATURE_COLUMNS)))
        weights = rng.uniform(-1, 1, size=len(FEATURE_COLUMNS))
        logits = X @ weights + rng.normal(scale=0.25, size=samples)
        y = (logits > 0).astype(int)
        model = XGBClassifier(
            max_depth=3,
            learning_rate=0.1,
            n_estimators=50,
            subsample=0.8,
            colsample_bytree=0.8,
            eval_metric="logloss",
            use_label_encoder=False,
        )
        model.fit(X, y)
        return model

    def predict(self, features: Dict[str, float]) -> float:
        vector = np.array([[features.get(col, 0.0) for col in FEATURE_COLUMNS]])
        proba = self.model.predict_proba(vector)[0][1]
        return float(np.clip(proba, 0.0, 1.0))


def build_features(
    price_frame: pd.DataFrame,
    etf_frame: pd.DataFrame,
    finnhub_sentiment: float,
    newsapi_sentiment: float,
    liquidity_hint: float,
) -> Dict[str, float]:
    if price_frame.empty:
        return {col: 0.0 for col in FEATURE_COLUMNS}

    close = price_frame["close"].astype(float)
    volume = price_frame["volume"].astype(float).replace(0, np.nan)
    returns = close.pct_change()

    def _return(period: int) -> float:
        if len(close) <= period:
            return 0.0
        start = close.iloc[-period - 1]
        if start == 0:
            return 0.0
        return float(close.iloc[-1] / start - 1)

    atr_series = AverageTrueRange(
        high=price_frame["high"], low=price_frame["low"], close=close, window=14, fillna=True
    ).average_true_range()
    atr_value = float(atr_series.iloc[-1]) if not atr_series.empty else 0.0
    atr_percent = atr_value / close.iloc[-1] if close.iloc[-1] else 0.0

    rel_volume = float(volume.iloc[-1] / volume.rolling(window=20, min_periods=1).mean().iloc[-1]) if len(volume) else 0.0
    volatility = float(returns.rolling(window=20, min_periods=1).std().iloc[-1])

    etf_strength = 0.0
    if not etf_frame.empty:
        etf_close = etf_frame["close"].astype(float)
        if len(etf_close) and etf_close.iloc[-1] != 0:
            etf_strength = float(close.iloc[-1] / etf_close.iloc[-1] - 1)

    avg_dollar_volume = float((close * volume).rolling(window=20, min_periods=1).mean().iloc[-1]) if len(close) else 0.0
    liquidity_bucket = _bucketize_liquidity(avg_dollar_volume, liquidity_hint)

    features = {
        "return_5d": _return(5),
        "return_10d": _return(10),
        "return_20d": _return(20),
        "volatility_20d": volatility,
        "relative_volume": rel_volume,
        "atr": atr_value,
        "atr_pct": atr_percent,
        "finnhub_sentiment": finnhub_sentiment,
        "newsapi_sentiment": newsapi_sentiment,
        "etf_relative_strength": etf_strength,
        "liquidity_bucket": liquidity_bucket,
    }
    return features


def _bucketize_liquidity(avg_dollar_volume: float, liquidity_hint: float) -> float:
    if avg_dollar_volume < 2_000_000:
        bucket = 0
    elif avg_dollar_volume < 10_000_000:
        bucket = 1
    else:
        bucket = 2
    return float(bucket + liquidity_hint)
