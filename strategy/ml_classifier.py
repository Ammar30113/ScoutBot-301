from __future__ import annotations

import os
from pathlib import Path
from typing import Dict, Iterable, List, Tuple
from collections import defaultdict

import joblib
import numpy as np
import pandas as pd
from ta.momentum import RSIIndicator
from ta.trend import MACD
from xgboost import XGBClassifier

from core.logger import get_logger
from core.config import get_settings
from data.price_router import PriceRouter
from strategy.technicals import atr_bands, compute_atr

logger = get_logger(__name__)
settings = get_settings()

MODEL_PATH = Path("models/momentum_sentiment_model.pkl")
FEATURE_COLUMNS = [
    "rsi",
    "macd",
    "macd_sig",
    "macd_hist",
    "vwap_diff",
    "slope",
    "vol_ratio",
    "atr",
    "atr_band_position",
]

price_router = PriceRouter()
LOG_SAMPLE_LIMIT = 5
_warn_counts: dict[str, int] = defaultdict(int)


class MLClassifier:
    def __init__(self, model_path: Path = MODEL_PATH) -> None:
        self.model_path = model_path
        self.synthetic = False
        self.model = self._load_or_train_model()

    def _load_or_train_model(self) -> XGBClassifier:
        self.model_path.parent.mkdir(parents=True, exist_ok=True)
        self.synthetic = False
        if self.model_path.exists():
            try:
                model = joblib.load(self.model_path)
                if hasattr(model, "n_features_in_") and int(model.n_features_in_) != len(FEATURE_COLUMNS):
                    raise ValueError("Stale model feature shape; retraining")
                # sanity check predict_proba shape
                _ = model.predict_proba(np.zeros((1, len(FEATURE_COLUMNS))))
                self.synthetic = bool(getattr(model, "synthetic", False))
                logger.info("Loaded existing ML model successfully.")
                return model
            except Exception as exc:  # pragma: no cover - defensive log
                logger.warning("Existing ML model %s invalid; retraining (%s)", self.model_path, exc)
                try:
                    os.remove(self.model_path)
                except Exception:
                    pass
        model = self._train_model()
        joblib.dump(model, self.model_path)
        return model

    def _train_model(self) -> XGBClassifier:
        """
        Real intraday ML training:
        - pulls intraday 5-minute bars via price_router
        - extracts features: momentum slope, volume expansion, RSI, MACD, VWAP diff
        - target: next bar return >= +0.1%
        """

        symbols = ["AAPL", "NVDA", "MSFT", "TSLA", "AMZN", "META", "AMD", "QQQ", "SPY", "SMH"]
        frames: List[pd.DataFrame] = []

        for symbol in symbols:
            try:
                bars = price_router.get_aggregates(symbol, window=600)
                df = PriceRouter.aggregates_to_dataframe(bars)
            except Exception as exc:  # pragma: no cover - network guard
                logger.warning("Training data unavailable for %s: %s", symbol, exc)
                continue
            if df is None or df.empty or len(df) < 50:
                continue

            df = df.copy()
            df["ret1"] = df["close"].pct_change().shift(-1)
            df["target"] = (df["ret1"] >= 0.001).astype(int)

            rsi = RSIIndicator(df["close"].astype(float), window=14).rsi()
            macd_indicator = MACD(df["close"].astype(float))
            macd_line = macd_indicator.macd()
            macd_sig = macd_indicator.macd_signal()
            macd_hist = macd_indicator.macd_diff()
            vwap = _compute_vwap(df)
            atr = compute_atr(df, window=14)
            mid, _, _, _ = atr_bands(df, multiplier=1.5, window=14)

            df["rsi"] = rsi
            df["macd"] = macd_line
            df["macd_sig"] = macd_sig
            df["macd_hist"] = macd_hist
            df["vwap_diff"] = df["close"].astype(float) - vwap
            df["slope"] = df["close"].astype(float).diff().rolling(5).mean()
            df["vol_ratio"] = (
                df["volume"].astype(float).rolling(5).mean() / df["volume"].astype(float).rolling(20).mean()
            )
            df["atr"] = atr
            if mid is not None and atr is not None:
                df["atr_band_position"] = (df["close"].astype(float) - mid) / atr
            else:
                df["atr_band_position"] = 0.0

            df.replace([np.inf, -np.inf], np.nan, inplace=True)
            df = df.dropna(subset=FEATURE_COLUMNS + ["target"])
            if not df.empty:
                frames.append(df)

        if not frames:
            logger.warning("No intraday training data available; training fallback synthetic model.")
            self.synthetic = True
            rng = np.random.default_rng(42)
            samples = 200
            X = rng.normal(size=(samples, len(FEATURE_COLUMNS)))
            y = (rng.random(size=samples) > 0.5).astype(int)

            model = XGBClassifier(
                n_estimators=50,
                max_depth=3,
                learning_rate=0.1,
                subsample=0.8,
                colsample_bytree=0.8,
                eval_metric="logloss",
            )
            model.fit(X, y)
            model.synthetic = True
            return model

        full = pd.concat(frames, ignore_index=True)
        X = full[FEATURE_COLUMNS]
        y = full["target"]

        self.synthetic = False
        model = XGBClassifier(
            n_estimators=200,
            max_depth=5,
            learning_rate=0.05,
            subsample=0.8,
            colsample_bytree=0.8,
            eval_metric="logloss",
        )
        model.fit(X, y)
        model.synthetic = False
        return model

    def predict(self, features: Dict[str, float], crash_mode: bool = False) -> float:
        vector = np.array([[features.get(col, 0.0) for col in FEATURE_COLUMNS]])
        if crash_mode:
            # weight ATR-band and MACD-hist higher during crash
            macd_idx = FEATURE_COLUMNS.index("macd_hist")
            atr_band_idx = FEATURE_COLUMNS.index("atr_band_position")
            vector[0, macd_idx] *= 1.3
            vector[0, atr_band_idx] *= 1.3
        proba = self.model.predict_proba(vector)[0][1]
        return float(np.clip(proba, 0.0, 1.0))


def build_features(price_frame: pd.DataFrame) -> Dict[str, float]:
    if price_frame.empty or len(price_frame) < 20:
        return {col: 0.0 for col in FEATURE_COLUMNS}

    df = price_frame.copy()
    close = df["close"].astype(float)
    volume = df["volume"].astype(float)

    rsi_val = float(RSIIndicator(close, window=14).rsi().iloc[-1])
    macd_indicator = MACD(close)
    macd_line = macd_indicator.macd().iloc[-1]
    macd_sig = macd_indicator.macd_signal().iloc[-1]
    macd_hist = macd_indicator.macd_diff().iloc[-1]
    vwap_series = _compute_vwap(df)
    vwap = vwap_series.iloc[-1] if not vwap_series.empty else np.nan
    vwap = vwap if np.isfinite(vwap) else float(close.iloc[-1])
    slope = float(close.diff().rolling(5).mean().iloc[-1])
    vol_ratio = float((volume.rolling(5).mean() / volume.rolling(20).mean()).iloc[-1])
    atr_series = compute_atr(df, window=14)
    atr_val = float(atr_series.iloc[-1]) if len(atr_series) else 0.0
    mid, _, _, _ = atr_bands(df, multiplier=1.5, window=14)
    mid_val = float(mid.iloc[-1]) if mid is not None and len(mid) else float(close.iloc[-1])
    atr_band_position = (float(close.iloc[-1]) - mid_val) / atr_val if atr_val else 0.0

    return {
        "rsi": rsi_val,
        "macd": float(macd_line),
        "macd_sig": float(macd_sig),
        "macd_hist": float(macd_hist),
        "vwap_diff": float(close.iloc[-1] - vwap),
        "slope": slope,
        "vol_ratio": vol_ratio if np.isfinite(vol_ratio) else 0.0,
        "atr": atr_val if np.isfinite(atr_val) else 0.0,
        "atr_band_position": atr_band_position if np.isfinite(atr_band_position) else 0.0,
    }


def _compute_vwap(df: pd.DataFrame) -> pd.Series:
    price = df["close"].astype(float)
    volume = df["volume"].astype(float)
    cumulative_volume = volume.cumsum().replace(0, np.nan)
    dollar_volume = (price * volume).cumsum()
    return dollar_volume / cumulative_volume


def _heuristic_prob(features: Dict[str, float]) -> float:
    rsi = float(features.get("rsi", 50.0) or 50.0)
    rsi_score = 1.0 - min(abs(rsi - 55.0) / 25.0, 1.0)
    macd = float(features.get("macd", 0.0) or 0.0)
    macd_hist = float(features.get("macd_hist", 0.0) or 0.0)
    vol_ratio = float(features.get("vol_ratio", 1.0) or 1.0)
    slope = float(features.get("slope", 0.0) or 0.0)

    vol_score = min(max((vol_ratio - 0.8) / 0.7, 0.0), 1.0)
    macd_score = 1.0 if macd > 0 else 0.0
    macd_hist_score = 1.0 if macd_hist > 0 else 0.0
    slope_score = 1.0 if slope > 0 else 0.0

    if not np.isfinite(rsi_score):
        rsi_score = 0.0
    if not np.isfinite(vol_score):
        vol_score = 0.0
    score = 0.3 * rsi_score + 0.2 * macd_score + 0.2 * macd_hist_score + 0.2 * vol_score + 0.1 * slope_score
    if not np.isfinite(score):
        score = 0.0
    return float(np.clip(0.15 + 0.7 * score, 0.0, 1.0))


_ml_classifier: MLClassifier | None = None
_synthetic_warned = False


def get_classifier() -> MLClassifier:
    global _ml_classifier
    if _ml_classifier is None:
        _ml_classifier = MLClassifier()
    return _ml_classifier


def generate_predictions(universe: Iterable[str], crash_mode: bool = False) -> List[Tuple[str, float, Dict[str, float]]]:
    predictions: List[Tuple[str, float, Dict[str, float]]] = []
    classifier = get_classifier()
    use_heuristic = False
    if classifier.synthetic and not settings.allow_synthetic_ml:
        global _synthetic_warned
        if not settings.allow_fallback_ml:
            if not _synthetic_warned:
                logger.warning(
                    "Synthetic ML model in use; ML signals disabled. Set ALLOW_SYNTHETIC_ML=true to override."
                )
                _synthetic_warned = True
            return predictions
        use_heuristic = True
        if not _synthetic_warned:
            logger.warning(
                "Synthetic ML model in use; heuristic fallback enabled. Set ALLOW_FALLBACK_ML=false to disable."
            )
            _synthetic_warned = True
    for symbol in universe:
        try:
            bars = price_router.get_aggregates(symbol, window=120)
        except Exception as exc:  # pragma: no cover - network guard
            _warn_counts[symbol] += 1
            count = _warn_counts[symbol]
            if count <= LOG_SAMPLE_LIMIT:
                logger.warning("Aggregates unavailable for %s: %s", symbol, exc)
            elif count == LOG_SAMPLE_LIMIT + 1:
                logger.info("Aggregates unavailable for %s (suppressing repeats; %s occurrences)", symbol, count)
            continue
        price_frame = PriceRouter.aggregates_to_dataframe(bars)
        if price_frame.empty:
            logger.warning("No price data for %s", symbol)
            continue

        features = build_features(price_frame)
        if crash_mode:
            features = {k: (0.0 if v is None or not np.isfinite(v) else v) for k, v in features.items()}
        if use_heuristic:
            prob = _heuristic_prob(features)
            logger.info("Heuristic ML probability for %s -> %.3f", symbol, prob)
        else:
            prob = classifier.predict(features, crash_mode=crash_mode)
            logger.info("ML probability for %s -> %.3f", symbol, prob)
        predictions.append((symbol, prob, features))
    return predictions
