MODEL_PARAM_GRIDS = {

    "decision_tree": [
        {
            "max_depth": [5, 10, 40],
            "min_samples_split": [2, 10, 20]
        }
        # {
        #     "max_depth": [5, 10, 40],
        #     "min_samples_split": [2, 10, 20],
        #     "min_samples_leaf": [1, 5, 10]
        # },
        # {
        #     "max_depth": [5, 10, 40],
        #     "min_samples_split": [2, 10, 20],
        #     "min_samples_leaf": [1, 5, 10],
        #     "max_features": ["sqrt", "log2", None]
        # },
        # {
        #     "max_depth": [5, 10, 40],
        #     "min_samples_split": [2, 10, 20],
        #     "min_samples_leaf": [1, 5, 10],
        #     "max_features": ["sqrt", "log2", None],
        #     "criterion": ["gini", "entropy", "log_loss"]
        # }
    ],

    "random_forest": [
        {
            "n_estimators": [50, 100, 150],
            "max_depth": [8, 16, 25]
        },
        {
            "n_estimators": [50, 100, 150],
            "max_depth": [8, 16, 25],
            "min_samples_split": [10, 20, 50]
        },
        {
            "n_estimators": [50, 100, 150],
            "max_depth": [8, 16, 25],
            "min_samples_split": [10, 20, 50],
            "min_samples_leaf": [5, 10, 20]
        },
        {
            "n_estimators": [50, 100, 150],
            "max_depth": [8, 16, 25],
            "min_samples_split": [10, 20, 50],
            "min_samples_leaf": [5, 10, 20],
            "max_features": ["sqrt", "log2", None]
        }
    ],

    "xgboost": [
        {
            "max_depth": [5, 6],
            "n_estimators": [100, 150]
          }
        # {
        #     "max_depth": [5, 6, 8],
        #     "n_estimators": [100, 150, 200],
        #     "learning_rate": [0.05, 0.1, 0.2]
        # },
        # {
        #     "max_depth": [5, 6, 8],
        #     "n_estimators": [100, 150, 200],
        #     "learning_rate": [0.05, 0.1, 0.2],
        #     "subsample": [0.7, 0.8, 1.0]
        # },
        # {
        #     "max_depth": [5, 6, 8],
        #     "n_estimators": [100, 150, 200],
        #     "learning_rate": [0.05, 0.1, 0.2],
        #     "subsample": [0.7, 0.8, 1.0],
        #     "colsample_bytree": [0.7, 0.8, 1.0]
        # }
    ],

    "catboost": [
        {
            "depth": [6, 8, 10],
            "iterations": [300, 800, 1200]
        },
        {
            "depth": [6, 8, 10],
            "iterations": [300, 800, 1200],
            "learning_rate": [0.01, 0.03, 0.1]
        },
        {
            "depth": [6, 8, 10],
            "iterations": [300, 800, 1200],
            "learning_rate": [0.01, 0.03, 0.1],
            "l2_leaf_reg": [5, 7, 9]
        },
        {
            "depth": [6, 8, 10],
            "iterations": [300, 800, 1200],
            "learning_rate": [0.01, 0.03, 0.1],
            "l2_leaf_reg": [5, 7, 9],
            "subsample": [0.8, 0.85, 0.9]
        },
        {
            "depth": [6, 8, 10],
            "iterations": [300, 800, 1200],
            "learning_rate": [0.01, 0.03, 0.1],
            "l2_leaf_reg": [5, 7, 9],
            "subsample": [0.8, 0.85, 0.9],
            "bagging_temperature": [0.3, 0.5, 0.7]
        }
    ],

    "adaboost": [
        {
            "n_estimators": [50, 100, 200],
            "learning_rate": [0.05, 0.1, 0.2]
        },
        {
            "n_estimators": [50, 100, 200],
            "learning_rate": [0.05, 0.1, 0.2],
            "estimator__max_depth": [4, 6, 8]
        },
        {
            "n_estimators": [50, 100, 200],
            "learning_rate": [0.05, 0.1, 0.2],
            "estimator__max_depth": [4, 6, 8],
            "estimator__min_samples_leaf": [1, 5, 10]
        }
    ],

    "lightgbm": [
        {
            "max_depth": [4, 6, 8],
            "n_estimators": [100, 200, 300]
        },
        {
            "max_depth": [4, 6, 8],
            "n_estimators": [100, 200, 300],
            "learning_rate": [0.03, 0.05, 0.1]
        },
        {
            "max_depth": [4, 6, 8],
            "n_estimators": [100, 200, 300],
            "learning_rate": [0.03, 0.05, 0.1],
            "num_leaves": [15, 31, 63]
        },
        {
            "max_depth": [4, 6, 8],
            "n_estimators": [100, 200, 300],
            "learning_rate": [0.03, 0.05, 0.1],
            "num_leaves": [15, 31, 63],
            "min_child_samples": [10, 20, 40]
        }
    ],

    "gradient_boosting": [
        {
            "max_depth": [5],
            "learning_rate": [0.05],
            "n_estimators": [200],
            "subsample": [0.8]
        },
        {
            "max_depth": [5],
            "learning_rate": [0.05],
            "n_estimators": [200],
            "subsample": [0.8],
            "min_samples_split": [10],
            "min_samples_leaf": [5],
            "max_features": ["sqrt"],
            "warm_start": [False],
            "validation_fraction": [0.1],
            "n_iter_no_change": [10],
            "tol": [1e-4]
        },
        {
            "max_depth": [6],
            "learning_rate": [0.1],
            "n_estimators": [100]
        }
    ],

    "logistic_regression": [
        {"C": [0.01, 0.1, 1.0], "max_iter": [200, 500, 1000]},
        {"C": [0.01, 0.1, 1.0], "max_iter": [200, 500, 1000], "penalty": ["l2", "elasticnet", "none"]},
        {"C": [0.01, 0.1, 1.0], "max_iter": [200, 500, 1000], "penalty": ["l2", "elasticnet", "none"], "solver": ["saga", "lbfgs", "liblinear"]},
        {"C": [0.01, 0.1, 1.0], "max_iter": [200, 500, 1000], "penalty": ["l2", "elasticnet", "none"], "solver": ["saga", "lbfgs", "liblinear"], "tol": [1e-4, 1e-3, 1e-2]}
    ],

    "svc": [
        {"C": [0.1, 1, 10], "kernel": ["rbf", "linear", "poly"]},
        {"C": [0.1, 1, 10], "kernel": ["rbf", "linear", "poly"], "gamma": ["scale", "auto", 0.1]},
        {"C": [0.1, 1, 10], "kernel": ["rbf", "linear", "poly"], "gamma": ["scale", "auto", 0.1], "degree": [2, 3, 4]},
        {"C": [0.1, 1, 10], "kernel": ["rbf", "linear", "poly"], "gamma": ["scale", "auto", 0.1], "degree": [2, 3, 4], "tol": [1e-4, 1e-3, 1e-2]}
    ],

    "sgd": [
        {
        "loss": ["log_loss"],
        "alpha": [1e-4]
        }
    ],

    "naive_bayes": [
        {"var_smoothing": [1e-9, 1e-8, 1e-7]}
        # {"var_smoothing": [1e-9, 1e-8, 1e-7], "fit_prior": [True, False, True], "priors": [None, None, None]},
        # {"var_smoothing": [1e-9, 1e-8, 1e-7], "fit_prior": [True, False, True], "priors": [None, None, None], "epsilon": [1e-9, 1e-8, 1e-7]},
        # {"var_smoothing": [1e-9, 1e-8, 1e-7], "fit_prior": [True, False, True], "priors": [None, None, None], "epsilon": [1e-9, 1e-8, 1e-7], "copy": [True, False, True]}
    ],

    "knn": [
        {
            "n_neighbors": [10]
        }
    ]
}


# ======================================================
# DL GRID PARAMS (SAME STYLE)
# ======================================================
DL_MODEL_PARAM_GRIDS = {
    "rnn": [{"epochs": [15], "batch_size": [256]}],
    "lstm": [{"epochs": [10], "batch_size": [256]}],
    "gru": [{"epochs": [10], "batch_size": [256]}]
}
