# =========================
# Scikit-learn models
# =========================
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import (
    RandomForestClassifier,
    AdaBoostClassifier,
    GradientBoostingClassifier
)
from sklearn.tree import DecisionTreeClassifier
from sklearn.naive_bayes import GaussianNB
from sklearn.svm import SVC
from sklearn.linear_model import SGDClassifier
from sklearn.neighbors import KNeighborsClassifier

# =========================
# Gradient boosting libraries
# =========================
from xgboost import XGBClassifier
from catboost import CatBoostClassifier
from lightgbm import LGBMClassifier

# =========================
# Deep Learning (Keras / TensorFlow)
# =========================
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense, Dropout, SimpleRNN, LSTM, GRU


def create_model(model_cls, **kwargs):
    model = model_cls(**kwargs)
    model.used_params = kwargs
    return model


MODEL_FIXED_PARAMS = {

    "decision_tree": [
        {"random_state": 42, "max_depth": 5},
        {"random_state": 42, "max_depth": 10},
        {"random_state": 42, "min_samples_split": 10},
        {"random_state": 42, "min_samples_leaf": 5},
        {"random_state": 42, "criterion": "entropy"},
        {"random_state": 42, "max_leaf_nodes": 20},
        {"random_state": 42, "splitter": "random"},
        {"random_state": 42, "max_depth": 8, "min_samples_split": 20, "min_samples_leaf": 10, "max_leaf_nodes": 50, "ccp_alpha": 0.01},
        {"random_state": 42, "max_depth": 20, "min_samples_split": 25, "min_samples_leaf": 8, "criterion": "entropy", "max_leaf_nodes": 100, "ccp_alpha": 0.002}
       ],

    "random_forest": [
        {"random_state": 42, "max_depth": 10},
        {"n_estimators": 100, "max_depth": 8, "min_samples_split": 20, "min_samples_leaf": 10, "max_leaf_nodes": 50, "class_weight": {0: 10, 1: 1}, "random_state": 42},
        {"random_state": 42, "max_depth": 16},
        {"n_estimators": 100, "max_depth": 8, "min_samples_split": 20, "min_samples_leaf": 10, "max_leaf_nodes": 50, "class_weight": {0: 5, 1: 1}, "random_state": 42},
        {"n_estimators": 50, "max_depth": 6, "min_samples_split": 50, "min_samples_leaf": 20, "max_leaf_nodes": 30, "class_weight": {0: 5, 1: 1}, "random_state": 42},
        {"n_estimators": 150, "max_depth": 12, "min_samples_split": 20, "random_state": 42}
      ],

    "xgboost": [
        {"max_depth": 6, "learning_rate": 0.1, "n_estimators": 100, "random_state": 42},
        {"max_depth": 6, "learning_rate": 0.1, "n_estimators": 100, "subsample": 0.8, "colsample_bytree": 0.8, "reg_alpha": 0.1, "reg_lambda": 1.0, "random_state": 42},
        {"max_depth": 6, "learning_rate": 0.1, "n_estimators": 150, "subsample": 0.8, "random_state": 42},
        {"max_depth": 5, "learning_rate": 0.05, "n_estimators": 200, "random_state": 42},
        {"max_depth": 4, "learning_rate": 0.1, "n_estimators": 100, "random_state": 42},
        {"n_estimators": 100, "random_state": 42},
        {"max_depth": 6, "n_estimators": 100, "random_state": 42}
    ],

    "catboost": [
        {"depth": 6, "learning_rate": 0.1, "iterations": 100, "random_seed": 42, "verbose": 0},
        {"depth": 10, "learning_rate": 0.1, "iterations": 500, "random_seed": 42, "verbose": 0},
        {"depth": 6, "learning_rate": 0.01, "iterations": 1000, "random_seed": 42, "verbose": 0},
        {"depth": 8, "learning_rate": 0.05, "iterations": 500, "l2_leaf_reg": 5, "bagging_temperature": 0.5, "random_seed": 42, "verbose": 0},
        {"depth": 6, "learning_rate": 0.1, "iterations": 300, "auto_class_weights": "Balanced", "subsample": 0.8, "random_seed": 42, "verbose": 0},
        {"depth": 10, "learning_rate": 0.03, "iterations": 1000, "l2_leaf_reg": 7, "bagging_temperature": 0.3, "random_strength": 1.5, "auto_class_weights": "Balanced", "subsample": 0.9, "random_seed": 42, "verbose": 0},
        {"depth": 8, "learning_rate": 0.04, "iterations": 800, "l2_leaf_reg": 6, "bagging_temperature": 0.4, "auto_class_weights": "Balanced", "subsample": 0.85, "random_strength": 1.3, "loss_function": "Logloss", "early_stopping_rounds": 50, "random_seed": 42, "verbose": 0},
        {"depth": 5, "iterations": 300, "learning_rate": 0.1, "random_seed": 42, "verbose": 0}
    ],

    "lightgbm": [
        {"max_depth": 6, "learning_rate": 0.1, "n_estimators": 100, "random_state": 42},
        {"max_depth": 8, "learning_rate": 0.05, "n_estimators": 300, "num_leaves": 31, "min_child_samples": 20, "subsample": 0.8, "colsample_bytree": 0.8, "reg_alpha": 0.1, "reg_lambda": 1.0, "random_state": 42},
        {"max_depth": 10, "learning_rate": 0.03, "n_estimators": 500, "num_leaves": 63, "min_child_samples": 15, "subsample": 0.85, "colsample_bytree": 0.85, "reg_alpha": 0.2, "reg_lambda": 1.5, "random_state": 42},
        {"learning_rate": 0.1, "n_estimators": 100, "random_state": 42},
        {"max_depth": 4, "learning_rate": 0.1, "n_estimators": 150, "random_state": 42}
    ],

    "adaboost": [
    {"n_estimators": 100, "learning_rate": 0.1},
    {"estimator": DecisionTreeClassifier(max_depth=6), "learning_rate": 0.1, "n_estimators": 100, "random_state": 42}
    ],

    "gradient_boosting": [
        {"max_depth": 5, "learning_rate": 0.05, "n_estimators": 200, "subsample": 0.8, "random_state": 42},
        {"max_depth": 5, "learning_rate": 0.05, "n_estimators": 200, "subsample": 0.8, "random_state": 42, "min_samples_split": 10, "min_samples_leaf": 5, "max_features": "sqrt", "validation_fraction": 0.1, "n_iter_no_change": 10, "tol": 1e-4},
        {"max_depth": 6, "learning_rate": 0.1, "n_estimators": 100, "random_state": 42}
    ],

    "logistic_regression": [
        {"max_iter": 1500, "random_state": 42},
        {"max_iter": 2000, "solver": "liblinear", "C": 0.5, "penalty": "l2", "random_state": 42},
        {"max_iter": 3000, "solver": "saga", "C": 0.2, "penalty": "l1", "random_state": 42}
    ],
    "naive_bayes": [
        {},
        {"var_smoothing": 1e-9},
        {"var_smoothing": 1e-8}
      ],

    "sgd": [
        {
        "loss": "log_loss",   # probability support
        "alpha": 1e-4,
        "max_iter": 1000,
        "tol": 1e-3
        }
    ],

    "knn": [
        {"n_neighbors": 5}
    ]
}



# ======================================================
# DL GRID PARAMS (SAME STYLE)
# ======================================================
DL_MODEL_PARAM_GRIDS = {
    "rnn": [{"epochs": [13], "batch_size": [256]}],
    "lstm": [{"epochs": [14], "batch_size": [256]}],
    "gru": [{"epochs": [15], "batch_size": [256]}]
}


# ======================================================
# DL MODEL BUILDERS
# ======================================================
def build_rnn(input_shape):
    return Sequential([
        SimpleRNN(32, input_shape=input_shape, activation="relu"),
        Dropout(0.2),
        Dense(16, activation="relu"),
        Dropout(0.2),
        Dense(1, activation="sigmoid")
    ])


def build_lstm(input_shape):
    return Sequential([
        LSTM(32, input_shape=input_shape, activation="relu"),
        Dropout(0.2),
        Dense(16, activation="relu"),
        Dropout(0.2),
        Dense(1, activation="sigmoid")
    ])


def build_gru(input_shape):
    return Sequential([
        GRU(32, input_shape=input_shape, activation="relu"),
        Dropout(0.2),
        Dense(16, activation="relu"),
        Dropout(0.2),
        Dense(1, activation="sigmoid")
    ])


DL_MODEL_BUILDERS = {
    "rnn": build_rnn,
    "lstm": build_lstm,
    "gru": build_gru
}

# ======================================================
# MODEL GROUPS (ONLY PARAMS NOT IN GRID)
# - ONLY random_state and n_jobs
# - NO overlap with grid params
# ======================================================
MODEL_GROUPS = [
    {
        "decision_tree": create_model(
            DecisionTreeClassifier
        ),

        "random_forest": create_model(
            RandomForestClassifier
        ),

        "xgboost": create_model(
            XGBClassifier
        ),

        "catboost": create_model(
            CatBoostClassifier
            # n_jobs / thread_count not passed
        ),

        "adaboost": create_model(
            AdaBoostClassifier
        ),

        "lightgbm": create_model(
            LGBMClassifier
        ),

        "gradient_boosting": create_model(
            GradientBoostingClassifier
        )
      },
      {
        "logistic_regression": create_model(
            LogisticRegression
            # n_jobs not supported reliably → omitted
        ),


        "naive_bayes": create_model(
            GaussianNB
            # n_jobs not supported → omitted
        ),

        "sgd": create_model(
            SGDClassifier
        ),


        "knn": create_model(
            KNeighborsClassifier
        )
    }
]
