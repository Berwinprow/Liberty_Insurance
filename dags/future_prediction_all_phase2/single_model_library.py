"""
Single model factory for Future Prediction Phase-2.

Supports:
- Classical ML models
- Deep Learning models (RNN, LSTM, GRU)
"""

import logging

from sklearn.linear_model import LogisticRegression, SGDClassifier
from sklearn.tree import DecisionTreeClassifier
from sklearn.ensemble import (
    RandomForestClassifier,
    AdaBoostClassifier,
    GradientBoostingClassifier,
)
from sklearn.naive_bayes import GaussianNB
from sklearn.neighbors import KNeighborsClassifier

from xgboost import XGBClassifier
from catboost import CatBoostClassifier
from lightgbm import LGBMClassifier

from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense, Dropout, SimpleRNN, LSTM, GRU
from tensorflow.keras.optimizers import Adam

logger = logging.getLogger(__name__)


# ======================================================
# DL MODEL BUILDERS
# ======================================================
def build_rnn(input_shape, params):
    model = Sequential(
        [
            SimpleRNN(
                units=params.get("units", 32),
                activation=params.get("activation", "relu"),
                input_shape=input_shape,
            ),
            Dropout(params.get("dropout", 0.2)),
            Dense(params.get("dense_units", 16), activation="relu"),
            Dropout(params.get("dropout", 0.2)),
            Dense(1, activation="sigmoid"),
        ]
    )

    model.compile(
        optimizer=Adam(
            learning_rate=params.get("learning_rate", 0.001)
        ),
        loss="binary_crossentropy",
        metrics=["accuracy"],
    )

    return model


def build_lstm(input_shape, params):
    model = Sequential(
        [
            LSTM(
                units=params.get("units", 32),
                activation=params.get("activation", "relu"),
                input_shape=input_shape,
            ),
            Dropout(params.get("dropout", 0.2)),
            Dense(params.get("dense_units", 16), activation="relu"),
            Dropout(params.get("dropout", 0.2)),
            Dense(1, activation="sigmoid"),
        ]
    )

    model.compile(
        optimizer=Adam(
            learning_rate=params.get("learning_rate", 0.001)
        ),
        loss="binary_crossentropy",
        metrics=["accuracy"],
    )

    return model


def build_gru(input_shape, params):
    model = Sequential(
        [
            GRU(
                units=params.get("units", 32),
                activation=params.get("activation", "relu"),
                input_shape=input_shape,
            ),
            Dropout(params.get("dropout", 0.2)),
            Dense(params.get("dense_units", 16), activation="relu"),
            Dropout(params.get("dropout", 0.2)),
            Dense(1, activation="sigmoid"),
        ]
    )

    model.compile(
        optimizer=Adam(
            learning_rate=params.get("learning_rate", 0.001)
        ),
        loss="binary_crossentropy",
        metrics=["accuracy"],
    )

    return model


DL_MODEL_BUILDERS = {
    "rnn": build_rnn,
    "lstm": build_lstm,
    "gru": build_gru,
}


# ======================================================
# SINGLE MODEL FACTORY (ML + DL)
# ======================================================
def create_single_model(
    model_name,
    params,
    input_shape=None,
):
    """
    Unified single model factory.

    Parameters
    ----------
    model_name : str
        Name of the model.
    params : dict
        Model parameters.
    input_shape : tuple, optional
        Required only for DL models.

    Returns
    -------
    model
        Instantiated ML or DL model.
    """

    model_name = model_name.lower()

    # ---------- DL MODELS ----------
    if model_name in DL_MODEL_BUILDERS:
        if input_shape is None:
            raise ValueError(
                f"input_shape is required for DL model: {model_name}"
            )

        return DL_MODEL_BUILDERS[model_name](
            input_shape,
            params,
        )

    # ---------- ML MODELS ----------
    if model_name == "logistic_regression":
        return LogisticRegression(**params)

    if model_name == "random_forest":
        return RandomForestClassifier(**params)

    if model_name == "decision_tree":
        return DecisionTreeClassifier(**params)

    if model_name == "xgboost":
        return XGBClassifier(
            use_label_encoder=False,
            eval_metric="logloss",
            **params,
        )

    if model_name == "catboost":
        return CatBoostClassifier(**params)

    if model_name == "lightgbm":
        return LGBMClassifier(**params)

    if model_name == "adaboost":
        return AdaBoostClassifier(**params)

    if model_name == "gradient_boosting":
        return GradientBoostingClassifier(**params)

    if model_name == "naive_bayes":
        return GaussianNB(**params)

    if model_name == "sgd":
        return SGDClassifier(**params)

    if model_name == "knn":
        return KNeighborsClassifier(**params)

    raise ValueError(f"Unsupported model: {model_name}")
