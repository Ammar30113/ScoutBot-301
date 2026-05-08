from pathlib import Path

from strategy import ml_classifier


class _SyntheticClassifier:
    synthetic = True


class _DummyModel:
    synthetic = True

    def predict_proba(self, _vector):
        return [[0.0, 0.5]]


def test_synthetic_predictions_disabled_without_explicit_fallback(monkeypatch):
    monkeypatch.setattr(ml_classifier, "get_classifier", lambda: _SyntheticClassifier())
    monkeypatch.setattr(ml_classifier.settings, "allow_synthetic_ml", False)
    monkeypatch.setattr(ml_classifier.settings, "allow_fallback_ml", False)

    assert ml_classifier.generate_predictions(["AAPL"]) == []


def test_invalid_model_does_not_persist_synthetic_replacement(tmp_path, monkeypatch):
    model_path = tmp_path / "bad_model.pkl"
    model_path.write_bytes(b"")

    def fake_train_synthetic(self):
        self.synthetic = True
        return _DummyModel()

    monkeypatch.setattr(ml_classifier.settings, "train_ml_on_startup", False)
    monkeypatch.setattr(ml_classifier.MLClassifier, "_train_synthetic_model", fake_train_synthetic)

    classifier = ml_classifier.MLClassifier(model_path=Path(model_path))

    assert classifier.synthetic is True
    assert model_path.read_bytes() == b""
