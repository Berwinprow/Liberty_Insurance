import numpy as np

from sklearn.ensemble import (
    VotingClassifier,
    BaggingClassifier,
    StackingClassifier
)
from xgboost import XGBClassifier

from ml_pipeline_phase_wise.model_library import MODEL_GROUPS


# ======================================================
# FLATTEN MODEL REGISTRY
# ======================================================
MODEL_REGISTRY = {}
for group in MODEL_GROUPS:
    MODEL_REGISTRY.update(group)



# ======================================================
# SOFT VOTING
# ======================================================
def build_soft_voting(cfg, Xs, Xns, y):
    estimators = []

    for model_name, rule in cfg["models"].items():
        model = MODEL_REGISTRY[model_name]
        model.set_params(**rule.get("params", {}))
        X = Xs if rule["scaling"] else Xns
        model.fit(X, y)
        estimators.append((model_name, model))

    voter = VotingClassifier(
        estimators=estimators,
        voting="soft"
    )

    voter.fit(Xs, y)  # ✅ REQUIRED
    return voter

# ======================================================
# BAGGING
# ======================================================
def build_bagging(cfg, Xs, Xns, y):
    base = MODEL_REGISTRY[cfg["base_model"]]
    X = Xs if cfg["scaling"] else Xns

    model = BaggingClassifier(
        estimator=base,
        n_estimators=cfg["params"]["n_estimators"],  # ✅ FIX
        random_state=42,
        n_jobs=-1
    )

    model.fit(X, y)
    return model

# ======================================================
# STACKING
# ======================================================
def build_stacking(cfg, Xs, Xns, y):
    estimators = []

    for name, rule in cfg["base_models"].items():
        model = MODEL_REGISTRY[name]
        model.set_params(**rule.get("params", {}))
        X = Xs if rule["scaling"] else Xns
        model.fit(X, y)
        estimators.append((name, model))

    meta_name, meta_cfg = list(cfg["meta_model"].items())[0]
    meta_model = MODEL_REGISTRY[meta_name]
    meta_model.set_params(**meta_cfg["params"])

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
    Custom weighted ensemble with class-wise weights
    """

    def __init__(self, models_cfg, weightage_cfg):
        self.models_cfg = models_cfg
        self.weightage_cfg = weightage_cfg
        self.models_ = {}

    def fit(self, Xs, Xns, y):
        for model_name, rule in self.models_cfg.items():
            model = MODEL_REGISTRY[model_name]
            model.set_params(**rule.get("params", {}))

            X = Xs if rule["scaling"] else Xns
            model.fit(X, y)

            self.models_[model_name] = {
                "model": model,
                "scaling": rule["scaling"]
            }

        # validation
        for model_name, weights in self.weightage_cfg.items():
            assert "class_0" in weights, f"class_0 missing for {model_name}"
            assert "class_1" in weights, f"class_1 missing for {model_name}"

        return self

    def predict_proba(self, X):
        # X is tuple: (Xs, Xns)
        Xs, Xns = X

        prob_0_sum = np.zeros(len(Xs))
        prob_1_sum = np.zeros(len(Xs))

        w0_sum = 0.0
        w1_sum = 0.0

        for name, info in self.models_.items():
            model = info["model"]
            scaling = info["scaling"]

            X_use = Xs if scaling else Xns
            probs = model.predict_proba(X_use)

            p0 = probs[:, 0]
            p1 = probs[:, 1]

            w0 = self.weightage_cfg[name]["class_0"]
            w1 = self.weightage_cfg[name]["class_1"]

            prob_0_sum += p0 * w0
            prob_1_sum += p1 * w1

            w0_sum += w0
            w1_sum += w1

        # normalize per class
        prob_0 = prob_0_sum / w0_sum
        prob_1 = prob_1_sum / w1_sum

        # final normalization to ensure p0 + p1 = 1
        total = prob_0 + prob_1
        prob_0 /= total
        prob_1 /= total

        return np.column_stack((prob_0, prob_1))

    def predict(self, X):
        return (self.predict_proba(X)[:, 1] >= 0.5).astype(int)


# ======================================================
# BUILDER FUNCTION
# ======================================================
def build_weighted_ensemble(cfg, Xs, Xns, y):
    model = WeightedEnsembleClassifier(
        models_cfg=cfg["models"],
        weightage_cfg=cfg["weightage"]
    )
    model.fit(Xs, Xns, y)
    return model