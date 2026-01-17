"""
Policy Churn ML Pipeline - Future Prediction System (REFACTORED)
=================================================================
Supports 3 prediction modes:
1. Future Prediction (Open customers)
2. Year-wise Prediction (Time-based split)
3. Cross Validation (Stratified K-Fold)

Key Changes:
- No column dropping (keep all columns)
- Extended metrics tracking
- Algorithm parameters from config
- Inline label encoding with unseen category handling
- Cross-validation with StratifiedKFold
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sklearn.metrics import make_scorer
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
import numpy as np
import json
import warnings
warnings.filterwarnings('ignore')

from sklearn.model_selection import train_test_split, StratifiedKFold, cross_validate
from sklearn.preprocessing import LabelEncoder, StandardScaler
from sklearn.metrics import (
    accuracy_score, precision_score, recall_score, f1_score, 
    confusion_matrix, roc_auc_score, log_loss, classification_report
)
from imblearn.over_sampling import SMOTE, RandomOverSampler
from imblearn.under_sampling import RandomUnderSampler
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.tree import DecisionTreeClassifier
from sklearn.naive_bayes import GaussianNB

from schema_table_config import get_schema
import random

# Set random seed for reproducibility
random.seed(42)
np.random.seed(42)

# =====================================================================
# CONFIGURATION
# =====================================================================

DAGS_DIR = Path(__file__).resolve().parent
JSON_PATH = str(DAGS_DIR / "config" / "schema_config.json")
FEATURE_PATH = str(DAGS_DIR / "config" / "selected_columns.json")
MODEL_CONFIG_PATH = str(DAGS_DIR / "config" / "model_training_config1.json")

SOURCE_SCHEMA = get_schema("bi_dwh", JSON_PATH)
CONNECTION_ID = "postgres_cloud_prochurn"
SOURCE_TABLE = "final_policy_feature_manclean"

# Results tables for different prediction modes
RESULTS_TABLE_FUTURE = "model_results_future_prediction2"
RESULTS_TABLE_YEARWISE = "model_results_yearwise_prediction2"
RESULTS_TABLE_CV = "model_results_cross_validation2"

# =====================================================================
# DATABASE SETUP
# =====================================================================

def create_results_tables():
    """Create all results tables with extended metrics - ALL LOWERCASE"""
    hook = PostgresHook(postgres_conn_id=CONNECTION_ID)
    
    # Table 1: Future Prediction Results - LOWERCASE COLUMNS
    future_table = f"""
    CREATE TABLE IF NOT EXISTS pip_bi_dwh.{RESULTS_TABLE_FUTURE} (
        id SERIAL PRIMARY KEY,
        run_timestamp TIMESTAMP,
        feature VARCHAR(50),
        data_splitting VARCHAR(20),
        sampling_method VARCHAR(50),
        model_name VARCHAR(100),
        parameter TEXT,

        train_accuracy FLOAT,
        train_accuracy_class1 FLOAT,
        train_accuracy_class0 FLOAT,
        train_logloss FLOAT,
        train_roc FLOAT,
        train_precision_class1 FLOAT,
        train_precision_class0 FLOAT,
        train_recall_class1 FLOAT,
        train_recall_class0 FLOAT,
        train_f1_class1 FLOAT,
        train_f1_class0 FLOAT,
        train_truepositive INTEGER,
        train_truenegative INTEGER,
        train_falsepositive INTEGER,
        train_falsenegative INTEGER,

        predicted_renewed INTEGER,
        predicted_not_renewed INTEGER,
        total_open_customers INTEGER,

        train_size INTEGER,
        open_customers_size INTEGER,

        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );"""

    # Table 2: Year-wise Prediction Results - LOWERCASE COLUMNS
    yearwise_table = f"""
    CREATE TABLE IF NOT EXISTS pip_bi_dwh.{RESULTS_TABLE_YEARWISE} (
        id SERIAL PRIMARY KEY,
        run_timestamp TIMESTAMP,
        feature VARCHAR(50),
        data_splitting VARCHAR(50),
        sampling_method VARCHAR(50),
        model_name VARCHAR(100),
        parameter TEXT,
        test_year INTEGER,
        test_months TEXT,
        
        -- Training metrics
        train_accuracy FLOAT,
        train_accuracy_class1 FLOAT,
        train_accuracy_class0 FLOAT,
        train_logloss FLOAT,
        train_roc FLOAT,
        train_precision_class1 FLOAT,
        train_precision_class0 FLOAT,
        train_recall_class1 FLOAT,
        train_recall_class0 FLOAT,
        train_f1_class1 FLOAT,
        train_f1_class0 FLOAT,
        train_truepositive INTEGER,
        train_truenegative INTEGER,
        train_falsepositive INTEGER,
        train_falsenegative INTEGER,
        
        -- Test metrics
        test_accuracy FLOAT,
        test_accuracy_class1 FLOAT,
        test_accuracy_class0 FLOAT,
        test_logloss FLOAT,
        test_roc FLOAT,
        test_precision_class1 FLOAT,
        test_precision_class0 FLOAT,
        test_recall_class1 FLOAT,
        test_recall_class0 FLOAT,
        test_f1_class1 FLOAT,
        test_f1_class0 FLOAT,
        test_truepositive INTEGER,
        test_truenegative INTEGER,
        test_falsepositive INTEGER,
        test_falsenegative INTEGER,
        
        -- Data sizes
        train_size INTEGER,
        test_size INTEGER,
        
        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    
    # Table 3: Cross Validation Results - WITH PER-CLASS CV METRICS
    cv_table = f"""
    CREATE TABLE IF NOT EXISTS pip_bi_dwh.{RESULTS_TABLE_CV} (
        id SERIAL PRIMARY KEY,
        run_timestamp TIMESTAMP,
        feature VARCHAR(50),
        data_splitting VARCHAR(20),
        sampling_method VARCHAR(50),
        model_name VARCHAR(100),
        parameter TEXT,
        
        -- Cross-validation MEAN metrics (overall)
        cv_mean_train_accuracy FLOAT,
        cv_mean_train_roc_auc FLOAT,
        cv_mean_train_log_loss FLOAT,
        cv_mean_test_accuracy FLOAT,
        cv_mean_test_roc_auc FLOAT,
        cv_mean_test_log_loss FLOAT,
        
        -- Cross-validation MEAN metrics (per-class) - NEW
        cv_mean_train_precision_class1 FLOAT,
        cv_mean_train_precision_class0 FLOAT,
        cv_mean_train_recall_class1 FLOAT,
        cv_mean_train_recall_class0 FLOAT,
        cv_mean_train_f1_class1 FLOAT,
        cv_mean_train_f1_class0 FLOAT,
        
        cv_mean_test_precision_class1 FLOAT,
        cv_mean_test_precision_class0 FLOAT,
        cv_mean_test_recall_class1 FLOAT,
        cv_mean_test_recall_class0 FLOAT,
        cv_mean_test_f1_class1 FLOAT,
        cv_mean_test_f1_class0 FLOAT,
        
        -- Final test metrics (from holdout set)
        test_accuracy FLOAT,
        test_accuracy_class1 FLOAT,
        test_accuracy_class0 FLOAT,
        test_logloss FLOAT,
        test_roc FLOAT,
        test_precision_class1 FLOAT,
        test_precision_class0 FLOAT,
        test_recall_class1 FLOAT,
        test_recall_class0 FLOAT,
        test_f1_class1 FLOAT,
        test_f1_class0 FLOAT,
        test_truepositive INTEGER,
        test_truenegative INTEGER,
        test_falsepositive INTEGER,
        test_falsenegative INTEGER,
        
        -- Data sizes
        train_size INTEGER,
        test_size INTEGER,
        
        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    
    try:
        hook.run(future_table)
        hook.run(yearwise_table)
        hook.run(cv_table)
        print("✅ All results tables created successfully")
    except Exception as e:
        print(f"⚠️ Table creation: {e}")
# =====================================================================
# DATA LOADING FUNCTIONS
# =====================================================================

def load_data_full_with_null_handling():
    """Load data with null handling - KEEP ALL DATA"""
    hook = PostgresHook(postgres_conn_id=CONNECTION_ID)
    engine = hook.get_sqlalchemy_engine()
    
    query = f"SELECT * FROM {SOURCE_SCHEMA}.{SOURCE_TABLE}"
    df = pd.read_sql(query, engine)
    
    print(f"📥 Loaded {len(df)} rows from database")
    
    # Null handling
    for col in df.columns:
        if df[col].dtype == "object":
            df[col] = df[col].fillna("unknown")
        else:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    
    print("✅ Null handling completed (Full dataset)")
    return df

def load_data_renewed_only():
    """Load data - FILTER RENEWED/NOT RENEWED ONLY"""
    hook = PostgresHook(postgres_conn_id=CONNECTION_ID)
    engine = hook.get_sqlalchemy_engine()
    
    query = f"SELECT * FROM {SOURCE_SCHEMA}.{SOURCE_TABLE}"
    df = pd.read_sql(query, engine)
    
    print(f"📥 Loaded {len(df)} rows from database")
    
    # Null handling
    for col in df.columns:
        if df[col].dtype == "object":
            df[col] = df[col].fillna("unknown")
        else:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    
    # Filter only Renewed and Not Renewed
    df = df[df['policy_status'].isin(['Renewed', 'Not Renewed'])]
    
    # Encode target: Not Renewed = 1, Renewed = 0
    df['policy_status'] = df['policy_status'].apply(
        lambda x: 1 if x == 'Not Renewed' else 0
    )
    
    print(f"✅ Filtered to {len(df)} rows (Renewed/Not Renewed only)")
    print(f"   Target distribution: {df['policy_status'].value_counts().to_dict()}")
    
    return df

# =====================================================================
# FEATURE SELECTION
# =====================================================================

def select_features(df, feature_set_name):
    """Select features based on feature set configuration"""
    with open(FEATURE_PATH, 'r') as f:
        feature_config = json.load(f)
    
    selected_cols = feature_config.get(feature_set_name, [])
    
    # Ensure policy_status is included
    if 'policy_status' not in selected_cols:
        selected_cols.append('policy_status')
    
    # Filter existing columns
    existing_cols = [c for c in selected_cols if c in df.columns]
    
    df_subset = df[existing_cols].copy()
    
    print(f"📊 Feature Set '{feature_set_name}': {len(existing_cols)} columns selected")
    
    return df_subset

# =====================================================================
# PREPROCESSING FUNCTIONS
# =====================================================================

def handle_date_columns(df):
    """Handle date columns: extract features and drop originals"""
    date_cols = ['policy_start_date', 'policy_end_date']
    
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
            df[f'{col}_year'] = df[col].dt.year
            df[f'{col}_month'] = df[col].dt.month
            df[f'{col}_day'] = df[col].dt.day
    
    # Store original date columns before dropping
    date_data = {}
    for col in date_cols:
        if col in df.columns:
            date_data[col] = df[col].copy()
    
    # Drop date columns
    df.drop(columns=[c for c in date_cols if c in df.columns], inplace=True, errors='ignore')
    
    print("✅ Date columns processed")
    
    return df, date_data

def apply_inline_label_encoding(X_train, X_test):
    """
    Apply inline label encoding with unseen category handling
    """
    X_train_enc = X_train.copy()
    X_test_enc = X_test.copy()
    
    for column in X_train.columns:
        if X_train[column].dtype == 'object':
            # Initialize and fit the LabelEncoder on the training data
            label_encoder = LabelEncoder()
            X_train_enc[column] = label_encoder.fit_transform(X_train[column].astype(str))
            
            # Create a mapping dictionary from the LabelEncoder
            mapping_dict = {label: i for i, label in enumerate(label_encoder.classes_)}
            
            # Track the next unique integer for unseen values in the test set
            next_unique_value = [max(mapping_dict.values()) + 1]
            
            # Encode the test data
            def encode_test_value(value):
                if value in mapping_dict:
                    return mapping_dict[value]
                else:
                    # Update the mapping_dict with a new unique value for unseen categories
                    mapping_dict[value] = next_unique_value[0]
                    next_unique_value[0] += 1
                    return mapping_dict[value]
            
            # Apply the encoding to the test set
            X_test_enc[column] = X_test[column].apply(encode_test_value)
    
    print("✅ Inline label encoding applied")
    return X_train_enc, X_test_enc

def apply_sampling(X_train, y_train, method="none"):
    """Apply sampling methods"""
    if method == "none":
        return X_train, y_train
    
    if method.lower() in ["smote", "SMOTE"]:
        sampler = SMOTE(random_state=42)
    elif method.lower() in ["oversample", "oversampling", "over"]:
        sampler = RandomOverSampler(random_state=42)
    elif method.lower() in ["undersample", "undersampling", "under"]:
        sampler = RandomUnderSampler(random_state=42)
    else:
        print(f"⚠️ Unknown sampling method: {method}, using none")
        return X_train, y_train
    
    X_res, y_res = sampler.fit_resample(X_train, y_train)
    print(f"✅ Sampling ({method}): {y_res.value_counts().to_dict()}")
    
    return X_res, y_res

def apply_scaling(X_train, X_test, scaling=True):
    """Apply standard scaling to numeric features"""
    if not scaling:
        return X_train, X_test
    
    numeric_cols = X_train.select_dtypes(include=["int64", "float64"]).columns
    
    if len(numeric_cols) == 0:
        print("⚠️ No numeric columns to scale")
        return X_train, X_test
    
    scaler = StandardScaler()
    
    X_train_scaled = X_train.copy()
    X_test_scaled = X_test.copy()
    
    X_train_scaled[numeric_cols] = scaler.fit_transform(X_train[numeric_cols])
    X_test_scaled[numeric_cols] = scaler.transform(X_test[numeric_cols])
    
    print(f"✅ Scaling applied on {len(numeric_cols)} columns")
    
    return X_train_scaled, X_test_scaled

# =====================================================================
# MODEL FUNCTIONS
# =====================================================================
def get_model_instance(algorithm_name, params=None):
    """
    Get model instance with parameters from config
    FIXED: Creates models conditionally to avoid parameter conflicts
    """
    if params is None:
        params = {}
    
    # Normalize algorithm name
    algo = algorithm_name.lower().strip()
    
    # Create the appropriate model instance
    if algo == "naive_bayes":
        # GaussianNB only accepts: priors, var_smoothing
        valid_params = {}
        if 'priors' in params:
            valid_params['priors'] = params['priors']
        if 'var_smoothing' in params:
            valid_params['var_smoothing'] = params['var_smoothing']
        return GaussianNB(**valid_params)
    
    elif algo == "decision_tree":
        return DecisionTreeClassifier(random_state=42, **params)
    
    elif algo == "random_forest":
        return RandomForestClassifier(random_state=42, **params)
    
    else:
        raise ValueError(f"Unknown algorithm: {algorithm_name}")


def requires_scaling(algorithm_name):
    """Determine if algorithm requires feature scaling"""
    scaling_required = ["logistic_regression", "naive_bayes"]
    return algorithm_name.lower() in scaling_required
# def get_model_instance(algorithm_name, params=None):
#     """Get model instance with parameters from config"""
#     if params is None:
#         params = {}
    
#     models = {
#         "naive_bayes": GaussianNB(**params),
#         "decision_tree": DecisionTreeClassifier(random_state=42, **params),
#         "random_forest": RandomForestClassifier(random_state=42, **params),
#         # "logistic_regression": LogisticRegression(max_iter=1000, random_state=42, **params),
#         # "gradient_boost": GradientBoostingClassifier(random_state=42, **params),
#     }
    
#     return models.get(algorithm_name.lower())

# def requires_scaling(algorithm_name):
#     """Determine if algorithm requires feature scaling"""
#     scaling_required = ["logistic_regression", "naive_bayes"]
#     return algorithm_name.lower() in scaling_required

def calculate_extended_metrics(y_true, y_pred, y_prob):
    """Calculate extended metrics including per-class metrics"""
    # Confusion matrix
    tn, fp, fn, tp = confusion_matrix(y_true, y_pred).ravel()
    
    metrics = {
        'accuracy': accuracy_score(y_true, y_pred),
        'accuracy_class1': recall_score(y_true, y_pred, pos_label=1, zero_division=0),
        'accuracy_class0': recall_score(y_true, y_pred, pos_label=0, zero_division=0),
        'logloss': log_loss(y_true, y_prob),
        'roc': roc_auc_score(y_true, y_prob),
        'precision_class1': precision_score(y_true, y_pred, pos_label=1, zero_division=0),
        'precision_class0': precision_score(y_true, y_pred, pos_label=0, zero_division=0),
        'recall_class1': recall_score(y_true, y_pred, pos_label=1, zero_division=0),
        'recall_class0': recall_score(y_true, y_pred, pos_label=0, zero_division=0),
        'f1_class1': f1_score(y_true, y_pred, pos_label=1, zero_division=0),
        'f1_class0': f1_score(y_true, y_pred, pos_label=0, zero_division=0),
        'truepositive': int(tp),
        'truenegative': int(tn),
        'falsepositive': int(fp),
        'falsenegative': int(fn),
    }
    
    return metrics

# =====================================================================
# PREDICTION MODE 1: FUTURE PREDICTION (OPEN CUSTOMERS)
# =====================================================================

def run_future_prediction(config_entry):
    """Mode 1: Future Prediction - Train on Renewed/Not Renewed, Predict on Open"""
    print("\n" + "="*70)
    print("🔮 MODE 1: FUTURE PREDICTION (OPEN CUSTOMERS)")
    print("="*70)
    
    algorithm = config_entry['algorithm']
    feature_set = config_entry['set']
    sampling = config_entry['sampling']
    split_ratio = config_entry.get('split', '80/20')
    params = config_entry.get('params', {})
    
    print(f"Algorithm: {algorithm}")
    print(f"Feature Set: {feature_set}")
    print(f"Sampling: {sampling}")
    print(f"Parameters: {params}")
    
    # Load data
    df_full = load_data_full_with_null_handling()
    df_train = load_data_renewed_only()
    
    # Select features
    df_full = select_features(df_full, feature_set)
    df_train = select_features(df_train, feature_set)
    
    # Handle dates
    df_full, _ = handle_date_columns(df_full)
    df_train, _ = handle_date_columns(df_train)
    
    # Split open customers
    open_customers = df_full[df_full['policy_status'] == 'Open'].copy()
    
    print(f"\n📊 Data Split:")
    print(f"   Training data: {len(df_train)}")
    print(f"   Open customers: {len(open_customers)}")
    
    # Prepare data
    X_train = df_train.drop(columns=['policy_status'])
    y_train = df_train['policy_status']
    X_open = open_customers.drop(columns=['policy_status'])
    
    # Encoding
    X_train_enc, X_open_enc = apply_inline_label_encoding(X_train, X_open)
    
    # Sampling
    X_train_samp, y_train_samp = apply_sampling(X_train_enc, y_train, sampling)
    
    # Scaling
    scaling = requires_scaling(algorithm)
    X_train_final, X_open_final = apply_scaling(X_train_samp, X_open_enc, scaling)
    
    # Train model
    model = get_model_instance(algorithm, params)
    print(f"\n🔄 Training {algorithm}...")
    model.fit(X_train_final, y_train_samp)
    
    # Training metrics
    y_train_pred = model.predict(X_train_final)
    y_train_prob = model.predict_proba(X_train_final)[:, 1]
    train_metrics = calculate_extended_metrics(y_train_samp, y_train_pred, y_train_prob)
    
    print(f"\n📊 Training Metrics:")
    print(f"   Accuracy: {train_metrics['accuracy']:.4f}")
    print(f"   ROC-AUC: {train_metrics['roc']:.4f}")
    
    # Predict on open customers
    y_open_pred = model.predict(X_open_final)
    predicted_renewed = (y_open_pred == 0).sum()
    predicted_not_renewed = (y_open_pred == 1).sum()
    
    print(f"\n🔮 Predictions on Open Customers:")
    print(f"   Predicted Renewed: {predicted_renewed}")
    print(f"   Predicted Not Renewed: {predicted_not_renewed}")
    
    # Save results
    result = {
        'run_timestamp': datetime.now(),
        'feature': feature_set,
        'data_splitting': split_ratio,
        'sampling_method': sampling,
        'model_name': algorithm,
        'parameter': str(params),
        
        
        'train_accuracy': train_metrics['accuracy'],
        'train_accuracy_class1': train_metrics['accuracy_class1'],
        'train_accuracy_class0': train_metrics['accuracy_class0'],
        'train_logloss': train_metrics['logloss'],
        'train_roc': train_metrics['roc'],
        'train_precision_class1': train_metrics['precision_class1'],
        'train_precision_class0': train_metrics['precision_class0'],
        'train_recall_class1': train_metrics['recall_class1'],
        'train_recall_class0': train_metrics['recall_class0'],
        'train_f1_class1': train_metrics['f1_class1'],
        'train_f1_class0': train_metrics['f1_class0'],
        'train_truepositive': train_metrics['truepositive'],
        'train_truenegative': train_metrics['truenegative'],
        'train_falsepositive': train_metrics['falsepositive'],
        'train_falsenegative': train_metrics['falsenegative'],
        
        'predicted_renewed': int(predicted_renewed),
        'predicted_not_renewed': int(predicted_not_renewed),
        'total_open_customers': len(open_customers),
        'train_size': len(X_train_final),
        'open_customers_size': len(X_open_final),
        'timestamp': datetime.now()
    }
    
    save_to_db(result, RESULTS_TABLE_FUTURE)
    print("✅ Results saved to database")
    
    return result

# =====================================================================
# PREDICTION MODE 2: YEAR-WISE PREDICTION
# =====================================================================

def run_yearwise_prediction(config_entry):
    """Mode 2: Year-wise Prediction - Time-based split"""
    print("\n" + "="*70)
    print("📅 MODE 2: YEAR-WISE PREDICTION (TIME-BASED SPLIT)")
    print("="*70)
    
    algorithm = config_entry['algorithm']
    feature_set = config_entry['set']
    sampling = config_entry['sampling']
    months_filter = config_entry['years_split']
    params = config_entry.get('params', {})
    
    # Parse months
    try:
        num_months = int(months_filter.replace('months', '').replace('month', ''))
    except:
        num_months = 4
    
    print(f"Algorithm: {algorithm}")
    print(f"Feature Set: {feature_set}")
    print(f"Test Period: Last {num_months} months of 2024")
    
    # Load data
    df = load_data_renewed_only()
    df = select_features(df, feature_set)
    
    # Keep dates for splitting
    df_with_dates = df.copy()
    if 'policy_end_date' in df_with_dates.columns:
        df_with_dates['policy_end_date'] = pd.to_datetime(df_with_dates['policy_end_date'], errors='coerce')
    else:
        print("❌ policy_end_date not found!")
        return None
    
    # Split data
    test_year = 2024
    test_months = list(range(13 - num_months, 13))
    
    test_data = df_with_dates[
        (df_with_dates['policy_end_date'].dt.year == test_year) &
        (df_with_dates['policy_end_date'].dt.month.isin(test_months))
    ].copy()
    
    train_data = df_with_dates[~df_with_dates.index.isin(test_data.index)].copy()
    
    print(f"\n📊 Data Split:")
    print(f"   Training: {len(train_data)}")
    print(f"   Test: {len(test_data)}")
    
    # Process dates
    train_data, _ = handle_date_columns(train_data)
    test_data, _ = handle_date_columns(test_data)
    
    # Prepare data
    X_train = train_data.drop(columns=['policy_status'])
    y_train = train_data['policy_status']
    X_test = test_data.drop(columns=['policy_status'])
    y_test = test_data['policy_status']
    
    # Encoding
    X_train_enc, X_test_enc = apply_inline_label_encoding(X_train, X_test)
    
    # Sampling
    X_train_samp, y_train_samp = apply_sampling(X_train_enc, y_train, sampling)
    
    # Scaling
    scaling = requires_scaling(algorithm)
    X_train_final, X_test_final = apply_scaling(X_train_samp, X_test_enc, scaling)
    
    # Train model
    model = get_model_instance(algorithm, params)
    print(f"\n🔄 Training {algorithm}...")
    model.fit(X_train_final, y_train_samp)
    
    # Training metrics
    y_train_pred = model.predict(X_train_final)
    y_train_prob = model.predict_proba(X_train_final)[:, 1]
    train_metrics = calculate_extended_metrics(y_train_samp, y_train_pred, y_train_prob)
    
    # Test metrics
    y_test_pred = model.predict(X_test_final)
    y_test_prob = model.predict_proba(X_test_final)[:, 1]
    test_metrics = calculate_extended_metrics(y_test, y_test_pred, y_test_prob)
    
    print(f"\n📊 Test Metrics:")
    print(f"   Accuracy: {test_metrics['accuracy']:.4f}")
    print(f"   ROC-AUC: {test_metrics['roc']:.4f}")
    
    # Save results
    result = {
       'run_timestamp': datetime.now(),
        'feature': feature_set,
        'data_splitting': months_filter,
        'sampling_method': sampling,
        'model_name': algorithm,
        'parameter': str(params),
        
        'test_year': test_year,
        'test_months': str(test_months),
        
        'train_accuracy': train_metrics['accuracy'],
        'train_accuracy_class1': train_metrics['accuracy_class1'],
        'train_accuracy_class0': train_metrics['accuracy_class0'],
        'train_logloss': train_metrics['logloss'],
        'train_roc': train_metrics['roc'],
        'train_precision_class1': train_metrics['precision_class1'],
        'train_precision_class0': train_metrics['precision_class0'],
        'train_recall_class1': train_metrics['recall_class1'],
        'train_recall_class0': train_metrics['recall_class0'],
        'train_f1_class1': train_metrics['f1_class1'],
        'train_f1_class0': train_metrics['f1_class0'],
        'train_truepositive': train_metrics['truepositive'],
        'train_truenegative': train_metrics['truenegative'],
        'train_falsepositive': train_metrics['falsepositive'],
        'train_falsenegative': train_metrics['falsenegative'],
        
        'test_accuracy': test_metrics['accuracy'],
        'test_accuracy_class1': test_metrics['accuracy_class1'],
        'test_accuracy_class0': test_metrics['accuracy_class0'],
        'test_logloss': test_metrics['logloss'],
        'test_roc': test_metrics['roc'],
        'test_precision_class1': test_metrics['precision_class1'],
        'test_precision_class0': test_metrics['precision_class0'],
        'test_recall_class1': test_metrics['recall_class1'],
        'test_recall_class0': test_metrics['recall_class0'],
        'test_f1_class1': test_metrics['f1_class1'],
        'test_f1_class0': test_metrics['f1_class0'],
        'test_truepositive': test_metrics['truepositive'],
        'test_truenegative': test_metrics['truenegative'],
        'test_falsepositive': test_metrics['falsepositive'],
        'test_falsenegative': test_metrics['falsenegative'],
        
        'train_size': len(X_train_final),
        'test_size': len(X_test_final),
        'timestamp': datetime.now()
    }
    
    save_to_db(result, RESULTS_TABLE_YEARWISE)
    print("✅ Results saved to database")
    
    return result

# =====================================================================
# PREDICTION MODE 3: CROSS VALIDATION WITH STRATIFIED K-FOLD
# =====================================================================

# =====================================================================
# COMPLETE run_cross_validation FUNCTION
# Add this to your code after run_yearwise_prediction function
# =====================================================================

def run_cross_validation(config_entry):
    """Mode 3: Cross Validation with per-class CV metrics"""
    print("\n" + "="*70)
    print("🔀 MODE 3: CROSS VALIDATION (STRATIFIED K-FOLD)")
    print("="*70)
    
    algorithm = config_entry['algorithm']
    feature_set = config_entry['set']
    split_ratio = config_entry.get('split', '80/20')
    sampling = config_entry['sampling']
    params = config_entry.get('params', {})
    
    # Parse split ratio
    try:
        train_pct, test_pct = split_ratio.split('/')
        test_size = int(test_pct) / 100
    except:
        test_size = 0.2
    
    print(f"Algorithm: {algorithm}")
    print(f"Feature Set: {feature_set}")
    print(f"Split Ratio: {split_ratio}")
    print(f"Sampling: {sampling}")
    print(f"Parameters: {params}")
    
    # Load data
    df = load_data_renewed_only()
    df = select_features(df, feature_set)
    df, _ = handle_date_columns(df)
    
    # Prepare data
    X = df.drop(columns=['policy_status'])
    y = df['policy_status']
    
    # Train-test split
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=test_size, stratify=y, random_state=42
    )
    
    print(f"\n📊 Data Split:")
    print(f"   Training: {len(X_train)}")
    print(f"   Testing: {len(X_test)}")
    
    # Encoding
    X_train_enc, X_test_enc = apply_inline_label_encoding(X_train, X_test)
    
    # Sampling
    X_train_samp, y_train_samp = apply_sampling(X_train_enc, y_train, sampling)
    
    # Scaling
    scaling = requires_scaling(algorithm)
    X_train_final, X_test_final = apply_scaling(X_train_samp, X_test_enc, scaling)
    
    # Get model
    model = get_model_instance(algorithm, params)
    
    # Stratified K-Fold Cross-Validation with per-class metrics
    print(f"\n🔄 Performing 5-Fold Cross-Validation...")
    cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
    
    # Import make_scorer if not already imported
    from sklearn.metrics import make_scorer
    
    # Define scoring metrics including per-class
    scoring = {
        'accuracy': 'accuracy',
        'roc_auc': 'roc_auc',
        'neg_log_loss': 'neg_log_loss',
        'precision_class1': make_scorer(precision_score, pos_label=1, zero_division=0),
        'precision_class0': make_scorer(precision_score, pos_label=0, zero_division=0),
        'recall_class1': make_scorer(recall_score, pos_label=1, zero_division=0),
        'recall_class0': make_scorer(recall_score, pos_label=0, zero_division=0),
        'f1_class1': make_scorer(f1_score, pos_label=1, zero_division=0),
        'f1_class0': make_scorer(f1_score, pos_label=0, zero_division=0),
    }
    
    cv_results = cross_validate(
        model, X_train_final, y_train_samp, cv=cv,
        scoring=scoring,
        return_train_score=True
    )
    
    # Calculate mean CV metrics (overall)
    cv_train_accuracy = cv_results['train_accuracy'].mean()
    cv_train_roc = cv_results['train_roc_auc'].mean()
    cv_train_logloss = -cv_results['train_neg_log_loss'].mean()
    cv_test_accuracy = cv_results['test_accuracy'].mean()
    cv_test_roc = cv_results['test_roc_auc'].mean()
    cv_test_logloss = -cv_results['test_neg_log_loss'].mean()
    
    # Calculate mean CV metrics (per-class)
    cv_train_precision_class1 = cv_results['train_precision_class1'].mean()
    cv_train_precision_class0 = cv_results['train_precision_class0'].mean()
    cv_train_recall_class1 = cv_results['train_recall_class1'].mean()
    cv_train_recall_class0 = cv_results['train_recall_class0'].mean()
    cv_train_f1_class1 = cv_results['train_f1_class1'].mean()
    cv_train_f1_class0 = cv_results['train_f1_class0'].mean()
    
    cv_test_precision_class1 = cv_results['test_precision_class1'].mean()
    cv_test_precision_class0 = cv_results['test_precision_class0'].mean()
    cv_test_recall_class1 = cv_results['test_recall_class1'].mean()
    cv_test_recall_class0 = cv_results['test_recall_class0'].mean()
    cv_test_f1_class1 = cv_results['test_f1_class1'].mean()
    cv_test_f1_class0 = cv_results['test_f1_class0'].mean()
    
    print(f"\n📊 Cross-Validation Results:")
    print(f"   Mean Train Accuracy: {cv_train_accuracy:.4f}")
    print(f"   Mean Train ROC AUC: {cv_train_roc:.4f}")
    print(f"   Mean Train Log Loss: {cv_train_logloss:.4f}")
    print(f"   Mean Test Accuracy: {cv_test_accuracy:.4f}")
    print(f"   Mean Test ROC AUC: {cv_test_roc:.4f}")
    print(f"   Mean Test Log Loss: {cv_test_logloss:.4f}")
    print(f"\n   Per-Class CV Metrics:")
    print(f"   Test Precision (Class 1): {cv_test_precision_class1:.4f}")
    print(f"   Test Precision (Class 0): {cv_test_precision_class0:.4f}")
    print(f"   Test Recall (Class 1): {cv_test_recall_class1:.4f}")
    print(f"   Test Recall (Class 0): {cv_test_recall_class0:.4f}")
    print(f"   Test F1 (Class 1): {cv_test_f1_class1:.4f}")
    print(f"   Test F1 (Class 0): {cv_test_f1_class0:.4f}")
    
    # Train on entire training set
    print(f"\n🔄 Training {algorithm} on full training set...")
    model.fit(X_train_final, y_train_samp)
    
    # Test predictions on holdout set
    y_test_pred = model.predict(X_test_final)
    y_test_prob = model.predict_proba(X_test_final)[:, 1]
    
    # Test metrics
    test_metrics = calculate_extended_metrics(y_test, y_test_pred, y_test_prob)
    
    print(f"\n📊 Final Test Metrics (Holdout Set):")
    print(f"   Accuracy: {test_metrics['accuracy']:.4f}")
    print(f"   ROC-AUC: {test_metrics['roc']:.4f}")
    print(f"   Log Loss: {test_metrics['logloss']:.4f}")
    
    # Classification report
    report = classification_report(y_test, y_test_pred)
    print(f"\nClassification Report:\n{report}")
    
    # Save results - ALL LOWERCASE KEYS
    result = {
        'run_timestamp': datetime.now(),
        'feature': feature_set,
        'data_splitting': split_ratio,
        'sampling_method': sampling,
        'model_name': algorithm,
        'parameter': str(params),
        
        # Cross-validation metrics (overall)
        'cv_mean_train_accuracy': cv_train_accuracy,
        'cv_mean_train_roc_auc': cv_train_roc,
        'cv_mean_train_log_loss': cv_train_logloss,
        'cv_mean_test_accuracy': cv_test_accuracy,
        'cv_mean_test_roc_auc': cv_test_roc,
        'cv_mean_test_log_loss': cv_test_logloss,
        
        # Cross-validation metrics (per-class)
        'cv_mean_train_precision_class1': cv_train_precision_class1,
        'cv_mean_train_precision_class0': cv_train_precision_class0,
        'cv_mean_train_recall_class1': cv_train_recall_class1,
        'cv_mean_train_recall_class0': cv_train_recall_class0,
        'cv_mean_train_f1_class1': cv_train_f1_class1,
        'cv_mean_train_f1_class0': cv_train_f1_class0,
        
        'cv_mean_test_precision_class1': cv_test_precision_class1,
        'cv_mean_test_precision_class0': cv_test_precision_class0,
        'cv_mean_test_recall_class1': cv_test_recall_class1,
        'cv_mean_test_recall_class0': cv_test_recall_class0,
        'cv_mean_test_f1_class1': cv_test_f1_class1,
        'cv_mean_test_f1_class0': cv_test_f1_class0,
        
        # Final test metrics (from holdout set)
        'test_accuracy': test_metrics['accuracy'],
        'test_accuracy_class1': test_metrics['accuracy_class1'],
        'test_accuracy_class0': test_metrics['accuracy_class0'],
        'test_logloss': test_metrics['logloss'],
        'test_roc': test_metrics['roc'],
        'test_precision_class1': test_metrics['precision_class1'],
        'test_precision_class0': test_metrics['precision_class0'],
        'test_recall_class1': test_metrics['recall_class1'],
        'test_recall_class0': test_metrics['recall_class0'],
        'test_f1_class1': test_metrics['f1_class1'],
        'test_f1_class0': test_metrics['f1_class0'],
        'test_truepositive': test_metrics['truepositive'],
        'test_truenegative': test_metrics['truenegative'],
        'test_falsepositive': test_metrics['falsepositive'],
        'test_falsenegative': test_metrics['falsenegative'],
        
        'train_size': len(X_train_final),
        'test_size': len(X_test_final),
        'timestamp': datetime.now()
    }
    
    save_to_db(result, RESULTS_TABLE_CV)
    print("✅ Results saved to database")
    
    return result

# =====================================================================
# ALSO ENSURE YOU HAVE THESE IMPORTS AT THE TOP OF YOUR FILE
# =====================================================================

"""
Add these imports if not already present:

from sklearn.model_selection import train_test_split, StratifiedKFold, cross_validate
from sklearn.metrics import (
    accuracy_score, precision_score, recall_score, f1_score, 
    confusion_matrix, roc_auc_score, log_loss, classification_report,
    make_scorer
)
"""

# =====================================================================
# DATABASE HELPER
# =====================================================================

def save_to_db(result_dict, table_name):
    """Save results to database"""
    df = pd.DataFrame([result_dict])
    
    hook = PostgresHook(postgres_conn_id=CONNECTION_ID)
    engine = hook.get_sqlalchemy_engine()
    
    df.to_sql(
        table_name,
        engine,
        schema=SOURCE_SCHEMA,
        if_exists='append',
        index=False
    )

# =====================================================================
# MAIN ORCHESTRATION
# =====================================================================

def run_all_predictions(**context):
    """Main function to run all prediction modes"""
    print("\n" + "="*70)
    print("🚀 STARTING ML PREDICTION PIPELINE")
    print("="*70)
    
    # Create tables
    create_results_tables()
    
    # Load model training configuration
    with open(MODEL_CONFIG_PATH, 'r') as f:
        config_data = json.load(f)
    
    # Extract common config and experiments
    common_config = config_data.get('common_config', {})
    experiments = config_data.get('experiments', [])
    
    years_split = common_config.get('years_split', '4months')
    
    print(f"\n📋 Common Config:")
    print(f"   Years Split: {years_split}")
    print(f"\n📋 Loaded {len(experiments)} experiment configurations")
    
    results_summary = {
        'future_prediction': [],
        'yearwise_prediction': [],
        'cross_validation': []
    }
    
    for idx, config in enumerate(experiments, 1):
        # Add years_split to each config
        config['years_split'] = years_split
        
        print(f"\n{'='*70}")
        print(f"📌 Experiment {idx}/{len(experiments)}")
        print(f"{'='*70}")
        
        try:
            # Mode 1: Future Prediction
            result_future = run_future_prediction(config)
            results_summary['future_prediction'].append(result_future)
            
            # Mode 2: Year-wise Prediction
            result_yearwise = run_yearwise_prediction(config)
            results_summary['yearwise_prediction'].append(result_yearwise)
            
            # Mode 3: Cross Validation
            result_cv = run_cross_validation(config)
            results_summary['cross_validation'].append(result_cv)
            
        except Exception as e:
            print(f"❌ Error in experiment {idx}: {e}")
            import traceback
            traceback.print_exc()
            continue
    
    print("\n" + "="*70)
    print("✅ ALL PREDICTIONS COMPLETED!")
    print("="*70)
    print(f"\nResults Summary:")
    print(f"  Future Predictions: {len(results_summary['future_prediction'])} completed")
    print(f"  Year-wise Predictions: {len(results_summary['yearwise_prediction'])} completed")
    print(f"  Cross Validations: {len(results_summary['cross_validation'])} completed")
    
    return results_summary

# =====================================================================
# AIRFLOW DAG DEFINITION
# =====================================================================

default_args = {
    'owner': 'data_science_team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'ml_pipeline_future_prediction_3modes_refactored1',
    default_args=default_args,
    description='ML Pipeline - 3 Modes with Extended Metrics and K-Fold CV',
    schedule_interval=None,
    catchup=False,
    tags=['machine_learning', 'future_prediction', 'policy_churn', 'refactored'],
)

prediction_task = PythonOperator(
    task_id='run_all_predictions',
    python_callable=run_all_predictions,
    provide_context=True,
    dag=dag,
    execution_timeout=timedelta(hours=6),
)

prediction_task