import numpy as np

from sklearn.ensemble import (
    VotingClassifier,
    BaggingClassifier,
    StackingClassifier
)

from future_prediction_all_phase2.single_model_library import create_single_model


# ======================================================
# SOFT VOTING ENSEMBLE
# ======================================================

def build_soft_voting(cfg, Xs, Xns, y):
    """
    Soft voting ensemble builder.

    cfg example:
    {
      "type": "ensemble",
      "method": "soft_voting",
      "sampling": ["none"],
      "models": {
        "logistic_regression": {
          "scaling": true,
          "params": {...}
        },
        "random_forest": {
          "scaling": false,
          "params": {...}
        }
      }
    }
    """

    estimators = []

    for model_name, rule in cfg["models"].items():
        model = create_single_model(
            model_name=model_name,
            params=rule.get("params", {})
        )

        X_use = Xs if rule["scaling"] else Xns
        model.fit(X_use, y)

        estimators.append((model_name, model))

    voter = VotingClassifier(
        estimators=estimators,
        voting="soft"
    )

    # IMPORTANT: final fit on scaled data
    voter.fit(Xs, y)

    return voter


# ======================================================
# BAGGING ENSEMBLE
# ======================================================

def build_bagging(cfg, Xs, Xns, y):
    """
    Bagging ensemble builder.
    """

    base_model_name = cfg["base_model"]

    base_model = create_single_model(
        model_name=base_model_name,
        params=cfg.get("base_params", {})
    )

    X_use = Xs if cfg.get("scaling", False) else Xns

    model = BaggingClassifier(
        estimator=base_model,
        n_estimators=cfg["params"]["n_estimators"],
        random_state=42,
        n_jobs=-1
    )

    model.fit(X_use, y)

    return model


# ======================================================
# STACKING ENSEMBLE
# ======================================================

def build_stacking(cfg, Xs, Xns, y):
    """
    Stacking ensemble builder.
    """

    estimators = []

    for model_name, rule in cfg["base_models"].items():
        model = create_single_model(
            model_name=model_name,
            params=rule.get("params", {})
        )

        X_use = Xs if rule["scaling"] else Xns
        model.fit(X_use, y)

        estimators.append((model_name, model))

    meta_name, meta_cfg = list(cfg["meta_model"].items())[0]

    meta_model = create_single_model(
        model_name=meta_name,
        params=meta_cfg.get("params", {})
    )

    stack = StackingClassifier(
        estimators=estimators,
        final_estimator=meta_model,
        n_jobs=-1
    )

    stack.fit(Xs, y)

    return stack


# ======================================================
# WEIGHTED ENSEMBLE (CLASS-WISE)
# ======================================================

class WeightedEnsembleClassifier:
    """
    Custom weighted ensemble with class-wise weights.

    Supports different scaling strategies per model.
    """

    def __init__(self, models_cfg, weightage_cfg):
        self.models_cfg = models_cfg
        self.weightage_cfg = weightage_cfg
        self.models_ = {}

    def fit(self, Xs, Xns, y):
        for model_name, rule in self.models_cfg.items():
            model = create_single_model(
                model_name=model_name,
                params=rule.get("params", {})
            )

            X_use = Xs if rule["scaling"] else Xns
            model.fit(X_use, y)

            self.models_[model_name] = {
                "model": model,
                "scaling": rule["scaling"]
            }

        # validate weights
        for model_name, weights in self.weightage_cfg.items():
            if "class_0" not in weights:
                raise ValueError(f"class_0 missing for {model_name}")
            if "class_1" not in weights:
                raise ValueError(f"class_1 missing for {model_name}")

        return self

    def predict_proba(self, X):
        """
        X must be tuple: (Xs, Xns)
        """

        Xs, Xns = X

        prob_0_sum = np.zeros(len(Xs))
        prob_1_sum = np.zeros(len(Xs))

        w0_sum = 0.0
        w1_sum = 0.0

        for model_name, info in self.models_.items():
            model = info["model"]
            scaling = info["scaling"]

            X_use = Xs if scaling else Xns
            probs = model.predict_proba(X_use)

            p0 = probs[:, 0]
            p1 = probs[:, 1]

            w0 = self.weightage_cfg[model_name]["class_0"]
            w1 = self.weightage_cfg[model_name]["class_1"]

            prob_0_sum += p0 * w0
            prob_1_sum += p1 * w1

            w0_sum += w0
            w1_sum += w1

        prob_0 = prob_0_sum / w0_sum
        prob_1 = prob_1_sum / w1_sum

        total = prob_0 + prob_1
        prob_0 /= total
        prob_1 /= total

        return np.column_stack((prob_0, prob_1))

    def predict(self, X):
        return (self.predict_proba(X)[:, 1] >= 0.5).astype(int)


# ======================================================
# WEIGHTED ENSEMBLE BUILDER
# ======================================================

def build_weighted_ensemble(cfg, Xs, Xns, y):
    """
    Builder for class-wise weighted ensemble.
    """

    model = WeightedEnsembleClassifier(
        models_cfg=cfg["models"],
        weightage_cfg=cfg["weightage"]
    )

    model.fit(Xs, Xns, y)

    return model
