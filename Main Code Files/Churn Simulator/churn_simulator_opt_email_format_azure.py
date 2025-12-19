import re
import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import text as _sqltext
import joblib
import numpy as np
import plotly.graph_objects as go
import os, base64
from email.mime.text import MIMEText
from google.auth.transport.requests import Request as _GRequest
from google.oauth2.credentials import Credentials as _GCreds
from google_auth_oauthlib.flow import InstalledAppFlow as _GFlow
from googleapiclient.discovery import build as _gbuild

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText   # (you already have this)
import html

from langchain_azure_ai.chat_models.inference import AzureAIChatCompletionsModel
from langchain.embeddings import HuggingFaceEmbeddings
from langchain_community.vectorstores import FAISS
from langchain.schema import Document
from dotenv import load_dotenv 
load_dotenv() 


# === Config: IDV caps & percent steps for auto-run ===
IDV_MAX_INCREASE_FACTOR = 1.30   # +30% hard cap vs original IDV
IDV_PERCENT_PER_K = 0.10         # each k adds +10% of original IDV (1k=10%, 2k=20%, 3k=30% capped)
_EPS = 1e-9  # small epsilon

# === Auto-suggest guardrail (relative to original row) ===
AUTO_RELATIVE_BAND = 0.30  # ±30% for ALL adjustable params in Auto-suggest

# === Cloud Postgres config for predictions + save targets ===
PRED_DB_CONFIG = {
    'host': '139.59.12.79',
    'database': 'updated_ui_db',
    'user': 'appadmin',
    'password': 'prowesstics',
    'port': '5432'
}
PREDICTIONS_TABLE = '"Prediction"."Backup_GBM1_predictions_JFMAMJ(Final)_top3_reasons"'
SAVE_TABLE  = 'selected_changes'
SAVE_SCHEMA = 'Prediction'
FULL_SAVE_FQN = f'"{SAVE_SCHEMA}"."{SAVE_TABLE}"'  # fully-qualified save table name

def _force_rerun():
    try:
        st.rerun()
    except Exception:
        st.experimental_rerun()

# === Step 1: Load Prediction Dataset (from cloud Postgres) ===
@st.cache_data
def load_prediction_data():
    eng = create_engine(
        f"postgresql://{PRED_DB_CONFIG['user']}:{PRED_DB_CONFIG['password']}@"
        f"{PRED_DB_CONFIG['host']}:{PRED_DB_CONFIG['port']}/{PRED_DB_CONFIG['database']}"
    )
    return pd.read_sql(f'SELECT * FROM {PREDICTIONS_TABLE};', eng)

# Local DB for parameter ranges
LOCAL_DB_CONFIG = {
    'host': 'localhost',
    'database': 'postgres',
    'user': 'postgres',
    'password': 'kaviyam123',
    'port': '5432'
}

@st.cache_data
def get_parameter_ranges():
    engine = create_engine(
        f"postgresql://{LOCAL_DB_CONFIG['user']}:{LOCAL_DB_CONFIG['password']}@"
        f"{LOCAL_DB_CONFIG['host']}:{LOCAL_DB_CONFIG['port']}/{LOCAL_DB_CONFIG['database']}"
    )
    query = '''
        SELECT 
            "total od premium", 
            "total tp premium", 
            "vehicle idv", 
            "before gst add-on gwp", 
            "ncb %% previous year"
        FROM public.policydata_with_fb_cc_pc_newfea_opti_correct;
    '''
    df = pd.read_sql(query, engine)

    od_min = min(float(df["total od premium"].min()), 0.0)
    tp_min = min(float(df["total tp premium"].min()), 0.0)

    return {
        "discount":       {"min": float(0.0), "max": float(90.0)},
        "od_premium":     {"min": od_min, "max": float(df["total od premium"].max())},
        "tp_premium":     {"min": tp_min, "max": float(df["total tp premium"].max())},
        "idv":            {"min": float(df["vehicle idv"].min()), "max": float(df["vehicle idv"].max())},
        "add_on_premium": {"min": float(df["before gst add-on gwp"].min()), "max": float(df["before gst add-on gwp"].max())},
        "ncb":            {"min": float(df['ncb % previous year'].min()), "max": float(df['ncb % previous year'].max())}
    }

# Groq & Gmail env
AZURE_INFERENCE_API_KEY   = os.getenv("AZURE_INFERENCE_API_KEY")
AZURE_INFERENCE_MODEL     = os.getenv("AZURE_INFERENCE_MODEL")
AZURE_INFERENCE_ENDPOINT     = os.getenv("AZURE_INFERENCE_ENDPOINT")

GMAIL_CREDENTIALS_FILE = os.getenv("GMAIL_CREDENTIALS_FILE", "credentials.json")
GMAIL_TOKEN_FILE       = os.getenv("GMAIL_TOKEN_FILE", "token.json")
SENDER_EMAIL           = os.getenv("SENDER_EMAIL", "")
DEFAULT_TO_EMAIL       = os.getenv("DEFAULT_TO_EMAIL", "")

# === Step 3: Reason → adjustable parameter keys ===
def get_adjustable_parameters(reasons):
    adjustable_map = {
        "Low Vehicle IDV": "idv",
        "High Own-Damage Premium": "od_premium",
        "High Third-Party Premium": "tp_premium",
        "High Add-On Premium": "add_on_premium",
        "Low Discount with NCB": "discount",
        "Low No Claim Bonus Percentage": "ncb"
    }
    fallback_params = {"idv", "od_premium", "tp_premium", "discount"}
    non_adjustable = {
        "Young Vehicle Age", "Old Vehicle Age", "Claims Happened",
        "Multiple Claims on Record", "Minimal Policies Purchased", "Tie Up with Non-OEM"
    }
    adjustable, saw_non_adj = set(), False
    for reason in reasons:
        if reason in adjustable_map:
            adjustable.add(adjustable_map[reason])
        elif reason in non_adjustable:
            saw_non_adj = True
    if saw_non_adj:
        adjustable.update(fallback_params)
    return list(adjustable)

# === Step 4: Reason → direction ===
def get_param_direction(reasons):
    direction = {}
    for r in reasons:
        if "High Own-Damage Premium" in r: direction["od_premium"] = "decrease"
        if "High Third-Party Premium" in r: direction["tp_premium"] = "decrease"
        if "High Add-On Premium" in r: direction["add_on_premium"] = "decrease"
        if "Low Vehicle IDV" in r: direction["idv"] = "increase"
        if "Low Discount" in r or "Low Discount with NCB" in r: direction["discount"] = "increase"
        if "Low No Claim Bonus" in r or "Low No Claim Bonus Percentage" in r: direction["ncb"] = "increase"
    return direction

# === Step 5: Step sizes ===
def get_step_size(param, value):
    if param in ("od_premium", "tp_premium", "add_on_premium"):
        return 1000.0 if float(value) >= 2000.0 else 100.0
    if param == "idv": return 10000.0
    if param in ("discount", "ncb"): return 10.0
    return 1.0

# === Step 6: Col name maps ===
PARAM_TO_COL = {
    "discount":       "applicable discount with ncb",
    "od_premium":     "total od premium",
    "tp_premium":     "total tp premium",
    "idv":            "vehicle idv",
    "add_on_premium": "before gst add-on gwp",
    "ncb":            "ncb % previous year"
}
COL_TITLES = {
    "discount":        ("Applicable Discount", "applicable discount with ncb"),
    "od_premium":      ("OD Premium",          "total od premium"),
    "tp_premium":      ("TP Premium",          "total tp premium"),
    "idv":             ("Vehicle IDV",         "vehicle idv"),
    "add_on_premium":  ("Add-On Premium",      "before gst add-on gwp"),
    "ncb":             ("NCB %",               "ncb % previous year")
}

# === Model encoding ===
def encode_row_for_model(row_series, features, label_encoders):
    model_input = row_series[features].copy()
    for col in model_input.index:
        if col in label_encoders:
            enc = label_encoders[col]
            mapping = {lab:i for i, lab in enumerate(enc.classes_)}
            nxt = [max(mapping.values())+1] if mapping else [0]
            def tr(v):
                if v in mapping: return mapping[v]
                mapping[v] = nxt[0]; nxt[0]+=1; return mapping[v]
            model_input[col] = tr(model_input[col])
    return pd.DataFrame([model_input])

# === Totals & coupling helpers ===
def recompute_totals_from_od_tp(row, gst_rate=0.18):
    od = float(row["total od premium"])
    tp = float(row["total tp premium"])
    gst = (od + tp) * float(gst_rate)
    total = od + tp + gst
    row["gst"] = gst
    row["total premium payable"] = total
    row["__gst_calc"] = gst
    row["__total_premium_calc"] = total

def apply_discount_coupling_on_od_tp(original_row, test_row, param_ranges, gst_rate=0.18, stack_on_top=True):
    old_d = float(original_row["applicable discount with ncb"]) / 100.0
    new_d = float(test_row["applicable discount with ncb"]) / 100.0
    extra = max(0.0, new_d - old_d)

    od0 = float(original_row["total od premium"])
    tp0 = float(original_row["total tp premium"])

    if extra <= 0.0:
        recompute_totals_from_od_tp(test_row, gst_rate); return False

    base_od = float(test_row["total od premium"]) if stack_on_top else float(original_row["total od premium"])
    base_tp = float(test_row["total tp premium"]) if stack_on_top else float(original_row["total tp premium"])

    new_od = base_od * (1.0 - extra)
    new_tp = base_tp * (1.0 - extra)

    if od0 <= _EPS: new_od = 0.0
    elif new_od <= _EPS: return True
    if tp0 <= _EPS: new_tp = 0.0
    elif new_tp <= _EPS: return True

    mn_od, mx_od = float(param_ranges["od_premium"]["min"]), float(param_ranges["od_premium"]["max"])
    mn_tp, mx_tp = float(param_ranges["tp_premium"]["min"]), float(param_ranges["tp_premium"]["max"])
    test_row["total od premium"] = 0.0 if od0 <= _EPS else min(max(new_od, mn_od), mx_od)
    test_row["total tp premium"] = 0.0 if tp0 <= _EPS else min(max(new_tp, mn_tp), mx_tp)

    recompute_totals_from_od_tp(test_row, gst_rate); return False

def infer_discount_from_odtp(original_row, stepped_row, param_ranges):
    od0 = float(original_row["total od premium"])
    tp0 = float(original_row["total tp premium"])
    ods = float(stepped_row["total od premium"])
    tps = float(stepped_row["total tp premium"])
    base = od0 + tp0
    if base <= _EPS:
        return float(original_row["applicable discount with ncb"])
    extra = max(0.0, 1.0 - (ods + tps) / base)
    disc0 = float(original_row["applicable discount with ncb"])
    mn_d = float(param_ranges["discount"]["min"])
    mx_d = float(param_ranges["discount"]["max"])
    return min(max(disc0 + 100.0 * extra, mn_d), mx_d)

# change signature to also receive param_ranges
def _enforce_auto_caps(original_row: dict, test_row: dict, param_ranges: dict, band: float = AUTO_RELATIVE_BAND) -> None:
    for p_key, col in PARAM_TO_COL.items():
        if col not in original_row or col not in test_row:
            continue
        try:
            base = float(original_row[col])
            val  = float(test_row[col])
        except Exception:
            continue

        # For discount/NCB: absolute clamp (e.g., 0..90), not relative band
        if p_key in ("discount", "ncb"):
            mn = float(param_ranges[p_key]["min"]); mx = float(param_ranges[p_key]["max"])
            test_row[col] = min(max(val, mn), mx)
            continue

        # Relative band for money knobs + IDV
        lo = base * (1.0 - band)
        hi = base * (1.0 + band)

        if p_key in ("od_premium", "tp_premium", "add_on_premium"):
            lo = max(0.0, lo)

        if p_key == "idv":
            hi = min(hi, base * IDV_MAX_INCREASE_FACTOR)

        test_row[col] = min(max(val, lo), hi)

    recompute_totals_from_od_tp(test_row, 0.18)


# === Auto-suggest core ===
def build_combo_for_k(original_row, direction, param_ranges, k_moves, reasons, selected_params=None):
    """
    Auto-suggest step logic:
    - Discount: +10 pp per k with special cap:
        * If baseline discount < 70 → limit total increase to +30 pp (max 3 steps)
        * If baseline discount ≥ 70 → allow stepping to 90 in +10 pp increments
      Then apply Discount → (OD, TP) coupling and mark OD/TP as moved if they changed.
    - NCB: +10 pp per k (cap 90)
    - If Discount NOT in play:
        OD/TP/Add-on: cut from ORIGINAL by 10/20/30% for k=1/2/3 (no more than -30%).
        Then infer Discount from OD+TP and recompute totals.
    - IDV: +10% per k capped at +30% overall (vs original).
    - Enforce guardrails: absolute clamp (0..90) for Discount/NCB; ±30% band for money knobs + IDV,
      with IDV also capped at +30% of baseline.
    Returns: (test_row, moved_set, stop_due_to_zero)
    """
    test_row = original_row.copy()
    moved = set()
    stop_due_to_zero = False
    in_play = set(selected_params) if selected_params else set(direction.keys())

    # Aliases
    disc_col = PARAM_TO_COL["discount"]
    ncb_col  = PARAM_TO_COL["ncb"]
    od_col   = PARAM_TO_COL["od_premium"]
    tp_col   = PARAM_TO_COL["tp_premium"]
    idv_col  = PARAM_TO_COL["idv"]
    add_col  = PARAM_TO_COL["add_on_premium"]

    # Originals (fixed anchors for %-based moves)
    orig_disc = float(original_row.get(disc_col, 0.0))
    orig_ncb  = float(original_row.get(ncb_col, 0.0))
    orig_od   = float(original_row.get(od_col, 0.0))
    orig_tp   = float(original_row.get(tp_col, 0.0))
    orig_add  = float(original_row.get(add_col, 0.0))
    orig_idv  = float(original_row.get(idv_col, 0.0))

    # Helper: clamp to param_ranges (and >=0 for money)
    def _clamp_val(p_key, val):
        mn = float(param_ranges[p_key]["min"])
        mx = float(param_ranges[p_key]["max"])
        if p_key in ("od_premium", "tp_premium", "add_on_premium"):
            mn = max(0.0, mn)
        return min(max(val, mn), mx)

    # ───────────────── IDV first
    if "idv" in direction:
        pct  = min(k_moves * IDV_PERCENT_PER_K, IDV_MAX_INCREASE_FACTOR - 1.0)  # 0.10, 0.20, 0.30 max
        cand = orig_idv * (1.0 + pct)
        test_row[idv_col] = _clamp_val("idv", cand)
        moved.add("idv")

    # ───────────────── Discount / NCB branch
    has_discount_play = "discount" in in_play
    if has_discount_play:
        # Discount step rule with special cap behavior
        if orig_disc >= 70.0:
            # If baseline is already ≥70, allow continuing in +10 steps up to 90
            new_disc = min(90.0, orig_disc + 10.0 * k_moves)
        else:
            # Otherwise, cap the total increase to +30 pp
            steps = min(k_moves, 3)  # +10, +20, +30 max
            new_disc = min(orig_disc + 10.0 * steps, orig_disc + 30.0)

        if abs(new_disc - orig_disc) > 1e-9:
            test_row[disc_col] = _clamp_val("discount", new_disc)
            moved.add("discount")

        # Apply coupling Discount → (OD, TP)
        hit_zero = apply_discount_coupling_on_od_tp(original_row, test_row, param_ranges, 0.18, True)

        # Mark coupled OD/TP as moved if changed vs original
        try:
            if abs(float(test_row[od_col]) - orig_od) > 1e-9:
                moved.add("od_premium")
            if abs(float(test_row[tp_col]) - orig_tp) > 1e-9:
                moved.add("tp_premium")
        except Exception:
            pass

        if hit_zero:
            recompute_totals_from_od_tp(test_row, 0.18)
            _enforce_auto_caps(original_row, test_row, param_ranges, AUTO_RELATIVE_BAND)
            return test_row, moved, True

        # Optional NCB move alongside Discount
        if "ncb" in in_play:
            new_ncb = min(90.0, orig_ncb + 10.0 * k_moves)
            if abs(new_ncb - orig_ncb) > 1e-9:
                test_row[ncb_col] = _clamp_val("ncb", new_ncb)
                moved.add("ncb")

        # Totals already recomputed in coupling; enforce caps (abs for disc/NCB, band for others)
        _enforce_auto_caps(original_row, test_row, param_ranges, AUTO_RELATIVE_BAND)
        return test_row, moved, False

    # ───────────────── If Discount NOT in play → relative cuts on OD/TP/Add-on
    cut_factor = 1.0 - 0.10 * min(max(k_moves, 1), 3)  # k=1→0.90, k=2→0.80, k=3→0.70

    if "od_premium" in in_play:
        test_row[od_col] = _clamp_val("od_premium", orig_od * cut_factor)
        moved.add("od_premium")
    if "tp_premium" in in_play:
        test_row[tp_col] = _clamp_val("tp_premium", orig_tp * cut_factor)
        moved.add("tp_premium")
    if "add_on_premium" in in_play:
        test_row[add_col] = _clamp_val("add_on_premium", orig_add * cut_factor)
        moved.add("add_on_premium")

    # Infer Discount from OD+TP changes, then recompute totals
    if ("od_premium" in in_play) or ("tp_premium" in in_play):
        new_disc = infer_discount_from_odtp(original_row, test_row, param_ranges)
        if abs(new_disc - float(test_row.get(disc_col, orig_disc))) > 1e-9:
            test_row[disc_col] = _clamp_val("discount", new_disc)
            moved.add("discount")

    recompute_totals_from_od_tp(test_row, 0.18)

    # Enforce caps: abs for discount/NCB (0..90), band for money knobs + IDV
    _enforce_auto_caps(original_row, test_row, param_ranges, AUTO_RELATIVE_BAND)
    return test_row, moved, False



def run_directional_moves_until_target(filtered_row, features, label_encoders, model,
                                       param_ranges, reasons, selected_params=None,
                                       target=50.0, max_k=50):
    direction = get_param_direction(reasons)
    default_dir = {"discount":"increase","ncb":"increase","idv":"increase",
                   "add_on_premium":"decrease","od_premium":"decrease","tp_premium":"decrease"}
    if selected_params:
        for p in selected_params:
            if p not in direction and p in default_dir:
                direction[p] = default_dir[p]

    X_base = encode_row_for_model(filtered_row.copy(), features, label_encoders)
    baseline_pct = float(model.predict_proba(X_base)[0][1] * 100.0)

    trials, best_idx, best_churn, prev_sig = [], None, baseline_pct, None
    for k in range(1, max_k+1):
        test_row, moved, stop_zero = build_combo_for_k(
            filtered_row, direction, param_ranges, k, [], selected_params
        )
        if stop_zero: break
        sig = (float(test_row["applicable discount with ncb"]),
               float(test_row["vehicle idv"]),
               float(test_row["before gst add-on gwp"]),
               float(test_row["total od premium"]),
               float(test_row["total tp premium"]))
        if prev_sig == sig: break
        prev_sig = sig
        X_t = encode_row_for_model(test_row, features, label_encoders)
        churn_pct = float(model.predict_proba(X_t)[0][1] * 100.0)
        trials.append({
            "k_moves": k, "moved": sorted(list(moved)) if moved else [],
            "row": test_row, "churn_pct": churn_pct,
            "od": float(test_row["total od premium"]), "tp": float(test_row["total tp premium"]),
            "gst": float(test_row["gst"]), "total_premium": float(test_row["total premium payable"]),
            "discount": float(test_row["applicable discount with ncb"]),
            "idv": float(test_row["vehicle idv"]), "addon": float(test_row["before gst add-on gwp"]),
            "ncb": float(test_row["ncb % previous year"]),
        })
        if churn_pct < best_churn: best_churn, best_idx = churn_pct, len(trials)-1
        if churn_pct < target: break
    return trials, best_idx, baseline_pct

# ==================== DB Save: helpers & UI ====================
def _cloud_engine():
    return create_engine(
        f"postgresql://{PRED_DB_CONFIG['user']}:{PRED_DB_CONFIG['password']}@"
        f"{PRED_DB_CONFIG['host']}:{PRED_DB_CONFIG['port']}/{PRED_DB_CONFIG['database']}"
    )

def _norm_policy(s):
    s = str(s or "").strip()
    if s.startswith(("'", '"')):
        s = s[1:]
    return s.replace(" ", "").upper()

def _policy_vehicle_str(row_like: dict) -> str:
    return f"{row_like.get('make_clean','')} {row_like.get('model_clean','')} ({row_like.get('variant','')})".strip()

def _ensure_selected_changes_table(eng):
    """Create table & unique index once if missing; safe to call before every save."""
    ddl = f"""
    CREATE TABLE IF NOT EXISTS {FULL_SAVE_FQN} (
        policy_no              TEXT,
        policy_no_norm         TEXT,
        customerid             TEXT,
        selection_type         TEXT,
        business_type          TEXT,
        tie_up_type            TEXT,
        zone                   TEXT,
        state                  TEXT,
        branch                 TEXT,
        vehicle                TEXT,

        -- Baseline (old) values
        old_discount           DOUBLE PRECISION,
        old_ncb                DOUBLE PRECISION,
        old_idv                DOUBLE PRECISION,
        old_add_on_premium     DOUBLE PRECISION,
        old_od                 DOUBLE PRECISION,
        old_tp                 DOUBLE PRECISION,
        old_gst                DOUBLE PRECISION,
        old_total_premium      DOUBLE PRECISION,

        -- Selected (new) values
        new_discount           DOUBLE PRECISION,
        new_ncb                DOUBLE PRECISION,
        new_idv                DOUBLE PRECISION,
        new_add_on_premium     DOUBLE PRECISION,
        new_od                 DOUBLE PRECISION,
        new_tp                 DOUBLE PRECISION,
        new_gst                DOUBLE PRECISION,
        new_total_premium      DOUBLE PRECISION,

        churn_risk_pct         DOUBLE PRECISION,
        top_3_reasons          TEXT,
        created_at             TIMESTAMPTZ DEFAULT NOW()
    );
    """
    idx = f'CREATE UNIQUE INDEX IF NOT EXISTS idx_selected_changes_policy_norm ON {FULL_SAVE_FQN}(policy_no_norm);'
    with eng.begin() as conn:
        conn.execute(_sqltext(ddl))
        conn.execute(_sqltext(idx))

def _save_selected_changes_to_db(base_row_dict, selected_row_dict, selection_type):
    # ensure GST/Total exist on the selected row snapshot
    if ("gst" not in selected_row_dict) or ("total premium payable" not in selected_row_dict):
        try:
            recompute_totals_from_od_tp(selected_row_dict, 0.18)
        except Exception:
            pass

    # Baseline (old)
    old_discount = float(base_row_dict.get("applicable discount with ncb", 0.0))
    old_ncb      = float(base_row_dict.get("ncb % previous year", 0.0))
    old_idv      = float(base_row_dict.get("vehicle idv", 0.0))
    old_addon    = float(base_row_dict.get("before gst add-on gwp", 0.0))
    old_od       = float(base_row_dict.get("total od premium", 0.0))
    old_tp       = float(base_row_dict.get("total tp premium", 0.0))
    old_gst      = float(base_row_dict.get("gst", (old_od + old_tp) * 0.18))
    old_total    = float(base_row_dict.get("total premium payable", old_od + old_tp + old_gst))

    # Selected (new)
    new_discount = float(selected_row_dict.get("applicable discount with ncb", selected_row_dict.get("discount", 0.0)))
    new_ncb      = float(selected_row_dict.get("ncb % previous year", selected_row_dict.get("ncb", 0.0)))
    new_idv      = float(selected_row_dict.get("vehicle idv", selected_row_dict.get("idv", 0.0)))
    new_addon    = float(selected_row_dict.get("before gst add-on gwp", selected_row_dict.get("add_on_premium", 0.0)))
    new_od       = float(selected_row_dict.get("total od premium", selected_row_dict.get("od", 0.0)))
    new_tp       = float(selected_row_dict.get("total tp premium", selected_row_dict.get("tp", 0.0)))
    new_gst      = float(selected_row_dict.get("gst", (new_od + new_tp) * 0.18))
    new_total    = float(selected_row_dict.get("total premium payable", selected_row_dict.get("total_premium", new_od + new_tp + new_gst)))

    # Identity & meta
    policy_raw   = str(base_row_dict.get("policy no", "")).strip()
    policy_norm  = _norm_policy(policy_raw)

    rec = {
        "policy_no": policy_raw,
        "policy_no_norm": policy_norm,
        "customerid": str(base_row_dict.get("customerid", "")),
        "selection_type": selection_type,
        "business_type": str(base_row_dict.get("biztype", "")),
        "tie_up_type": str(base_row_dict.get("tie up", "")),
        "zone": str(base_row_dict.get("Cleaned Zone 2", "")),
        "state": str(base_row_dict.get("Cleaned State2", "")),
        "branch": str(base_row_dict.get("Cleaned Branch Name 2", "")),
        "vehicle": _policy_vehicle_str(base_row_dict),

        # OLD values
        "old_discount": old_discount,
        "old_ncb":      old_ncb,
        "old_idv":      old_idv,
        "old_add_on_premium": old_addon,
        "old_od":       old_od,
        "old_tp":       old_tp,
        "old_gst":      old_gst,
        "old_total_premium": old_total,

        # NEW values
        "new_discount": new_discount,
        "new_ncb":      new_ncb,
        "new_idv":      new_idv,
        "new_add_on_premium": new_addon,
        "new_od":       new_od,
        "new_tp":       new_tp,
        "new_gst":      new_gst,
        "new_total_premium": new_total,

        "churn_risk_pct": float(base_row_dict.get("Churn Probability", 0.0)) * 100.0,
        "top_3_reasons": str(base_row_dict.get("Top 3 Reasons", "") or base_row_dict.get("top 3 reasons", "")),
    }
    df_out = pd.DataFrame([rec])

    eng = _cloud_engine()
    _ensure_selected_changes_table(eng)

    # Replace semantics: keep only latest row per policy
    with eng.begin() as conn:
        conn.execute(
            _sqltext(f"DELETE FROM {FULL_SAVE_FQN} WHERE policy_no_norm = :p;"),
            {"p": policy_norm}
        )
    # append new snapshot
    df_out.to_sql(SAVE_TABLE, eng, schema=SAVE_SCHEMA, if_exists='append', index=False)

def _fetch_latest_selected_change(eng, policy_no: str) -> pd.DataFrame:
    policy_norm = _norm_policy(policy_no)
    sql = f"""
    SELECT *
    FROM {FULL_SAVE_FQN}
    WHERE policy_no_norm = :p
    ORDER BY created_at DESC
    LIMIT 1;
    """
    return pd.read_sql(_sqltext(sql), eng, params={"p": policy_norm})

def _render_latest_selected_change(policy_no: str):
    try:
        eng = _cloud_engine()
        df = _fetch_latest_selected_change(eng, policy_no)
        if df.empty:
            st.info("No saved Selected changes found for this policy.")
            return

        row = df.iloc[0]
        st.markdown("#### 📌 Latest Saved Selected changes (DB)")
        left, right = st.columns(2)

        with left:
            st.write(f"**Policy No**: {row['policy_no']}")
            st.write(f"**Customer ID**: {row['customerid']}")
            st.write(f"**Business Type**: {row['business_type']}")
            st.write(f"**Tie-up Type**: {row['tie_up_type']}")
            st.write(f"**Zone**: {row['zone']}")
            st.write(f"**State**: {row['state']}")
            st.write(f"**Branch**: {row['branch']}")
            st.write(f"**Vehicle**: {row['vehicle']}")
            st.write(f"**Churn Risk % (baseline)**: {float(row['churn_risk_pct']):.2f}")
            if str(row.get("top_3_reasons","")).strip():
                st.write(f"**Top 3 Reasons**: {row['top_3_reasons']}")

        comp = pd.DataFrame([{
            "Metric": "Discount (%)",
            "Old": round(float(row["old_discount"]), 2),
            "New": round(float(row["new_discount"]), 2),
        }, {
            "Metric": "NCB (%)",
            "Old": round(float(row["old_ncb"]), 2),
            "New": round(float(row["new_ncb"]), 2),
        }, {
            "Metric": "Vehicle IDV (₹)",
            "Old": round(float(row["old_idv"]), 0),
            "New": round(float(row["new_idv"]), 0),
        }, {
            "Metric": "Add-On Premium (₹)",
            "Old": round(float(row["old_add_on_premium"]), 0),
            "New": round(float(row["new_add_on_premium"]), 0),
        }, {
            "Metric": "OD Premium (₹)",
            "Old": round(float(row["old_od"]), 0),
            "New": round(float(row["new_od"]), 0),
        }, {
            "Metric": "TP Premium (₹)",
            "Old": round(float(row["old_tp"]), 0),
            "New": round(float(row["new_tp"]), 0),
        }, {
            "Metric": "GST (₹)",
            "Old": round(float(row["old_gst"]), 0),
            "New": round(float(row["new_gst"]), 0),
        }, {
            "Metric": "Total Premium (₹)",
            "Old": round(float(row["old_total_premium"]), 0),
            "New": round(float(row["new_total_premium"]), 0),
        }])

        with right:
            st.table(comp)
    except Exception as e:
        
        st.warning(f"Could not display latest saved record: {e}")

# === Manual coupling helpers (sliders) ===
def _mk_row_from_state(filtered_row):
    row = filtered_row.copy()
    disc_col = PARAM_TO_COL["discount"]
    od_col   = PARAM_TO_COL["od_premium"]
    tp_col   = PARAM_TO_COL["tp_premium"]
    idv_col  = PARAM_TO_COL["idv"]
    add_col  = PARAM_TO_COL["add_on_premium"]
    ncb_col  = PARAM_TO_COL["ncb"]

    def _get(key, fb):
        return float(st.session_state.get(key, fb))

    row[disc_col] = _get("slider_"+disc_col, float(filtered_row[disc_col]))
    row[od_col]   = _get("slider_"+od_col,   float(filtered_row[od_col]))
    row[tp_col]   = _get("slider_"+tp_col,   float(filtered_row[tp_col]))
    row[idv_col]  = _get("slider_"+idv_col,  float(filtered_row.get(idv_col, 0.0)))
    row[add_col]  = _get("slider_"+add_col,  float(filtered_row.get(add_col, 0.0)))
    row[ncb_col]  = _get("slider_"+ncb_col,  float(filtered_row.get(ncb_col, 0.0)))
    return row


def _init_manual_state(filtered_row, param_keys, param_ranges):
    row_key = f"{filtered_row['policy no']}_{filtered_row['customerid']}"

    if st.session_state.get("_row_key") != row_key:
        st.session_state["_row_key"] = row_key
        st.session_state["_od_hold"] = float(filtered_row[PARAM_TO_COL["od_premium"]])
        st.session_state["_tp_hold"] = float(filtered_row[PARAM_TO_COL["tp_premium"]])
        st.session_state["_disc_is_derived"] = False
        st.session_state["_disc_touched_user"] = False
        st.session_state["_od_touched"] = False
        st.session_state["_tp_touched"] = False

        # reset the growing log
        st.session_state["_history_rows"] = []
        st.session_state["_last_sig"] = None
        st.session_state["_last_row_for_history"] = None
        st.session_state["_last_churn_for_history"] = None

        # ⬇️ reset sliders to the new row’s baseline (important when switching rows)
        for p in param_keys:
            col = PARAM_TO_COL[p]
            v = float(filtered_row[col])
            if p == "idv":
                v = min(v, float(filtered_row[col]) * IDV_MAX_INCREASE_FACTOR)
            st.session_state["slider_" + col] = v

    # ensure keys exist for safety (kept as-is)
    st.session_state.setdefault("_coupling_busy", False)
    st.session_state.setdefault("_disc_is_derived", False)
    st.session_state.setdefault("_disc_touched_user", False)
    st.session_state.setdefault("_od_touched", False)
    st.session_state.setdefault("_tp_touched", False)
    st.session_state.setdefault("_od_hold", float(filtered_row[PARAM_TO_COL["od_premium"]]))
    st.session_state.setdefault("_tp_hold", float(filtered_row[PARAM_TO_COL["tp_premium"]]))


def _apply_pending_reset():
    if st.session_state.get("_do_reset"):
        for key, val in st.session_state.get("_pending_reset", {}).items():
            st.session_state[key] = val

        st.session_state["_disc_is_derived"] = False
        st.session_state["_disc_touched_user"] = False
        st.session_state["_od_touched"] = False
        st.session_state["_tp_touched"] = False

        st.session_state["_do_reset"] = False
        st.session_state["_pending_reset"] = {}

def _reset_manual_sliders(filtered_row, param_keys):
    payload = {}
    for p in param_keys:
        col = PARAM_TO_COL[p]
        payload["slider_" + col] = float(filtered_row[col])

    # Also reset continuity holds & flags to baseline
    st.session_state["_od_hold"] = float(filtered_row[PARAM_TO_COL["od_premium"]])
    st.session_state["_tp_hold"] = float(filtered_row[PARAM_TO_COL["tp_premium"]])
    st.session_state["_disc_is_derived"] = False
    st.session_state["_disc_touched_user"] = False
    st.session_state["_od_touched"] = False
    st.session_state["_tp_touched"] = False

    st.session_state["_pending_reset"] = payload
    st.session_state["_do_reset"] = True
    _force_rerun()

def _reverse_couple_from_odtp(filtered_row, param_ranges):
    if st.session_state["_coupling_busy"]: return
    st.session_state["_coupling_busy"] = True
    disc_col = PARAM_TO_COL["discount"]
    test_row = _mk_row_from_state(filtered_row)
    new_disc = infer_discount_from_odtp(filtered_row, test_row, param_ranges)
    test_row[disc_col] = new_disc
    recompute_totals_from_od_tp(test_row, 0.18)
    key = "slider_"+disc_col
    if key in st.session_state:
        st.session_state[key] = float(test_row[disc_col])

    # ▶ discount was *derived* from OD/TP move
    st.session_state["_disc_is_derived"] = True
    st.session_state["_disc_touched_user"] = False

    st.session_state["_coupling_busy"] = False
    _force_rerun()

def _on_discount_change(filtered_row, param_ranges):
    if st.session_state["_coupling_busy"]: return
    st.session_state["_coupling_busy"] = True
    # ▶ user explicitly moved discount
    st.session_state["_disc_is_derived"] = False
    st.session_state["_disc_touched_user"] = True

    test_row = _mk_row_from_state(filtered_row)
    _ = apply_discount_coupling_on_od_tp(filtered_row, test_row, param_ranges, 0.18, True)
    for p in ("od_premium","tp_premium"):
        col = PARAM_TO_COL[p]; key = "slider_"+col
        if key in st.session_state:
            st.session_state[key] = float(test_row[col])
    st.session_state["_coupling_busy"] = False
    _force_rerun()

def _on_od_change(filtered_row, param_ranges): 
    # mark OD touched by user; discount will be derived
    st.session_state["_od_touched"] = True
    _reverse_couple_from_odtp(filtered_row, param_ranges)
def _on_tp_change(filtered_row, param_ranges): 
    st.session_state["_tp_touched"] = True
    _reverse_couple_from_odtp(filtered_row, param_ranges)

def apply_manual_two_way_coupling(filtered_row, param_ranges, slider_vals):
    """
    Continuity rules:
    - Discount only      → scale OD & TP from last-shown (_od_hold/_tp_hold)
    - OD only (derived)  → freeze TP at last-shown; infer Discount; recompute GST/Total
    - TP only (derived)  → freeze OD at last-shown; infer Discount; recompute GST/Total
    - Discount + OD      → keep OD as user set; scale only TP from last-shown
    - Discount + TP      → keep TP as user set; scale only OD from last-shown
    - OD + TP (no disc)  → infer Discount from OD+TP; recompute totals
    """
    row = filtered_row.copy()

    disc_col = PARAM_TO_COL["discount"]
    od_col   = PARAM_TO_COL["od_premium"]
    tp_col   = PARAM_TO_COL["tp_premium"]
    idv_col  = PARAM_TO_COL["idv"]
    add_col  = PARAM_TO_COL["add_on_premium"]
    ncb_col  = PARAM_TO_COL["ncb"]

    # Apply incoming slider values to working row (only those present)
    if "discount" in slider_vals:       row[disc_col] = float(slider_vals["discount"])
    if "od_premium" in slider_vals:     row[od_col]   = float(slider_vals["od_premium"])
    if "tp_premium" in slider_vals:     row[tp_col]   = float(slider_vals["tp_premium"])
    if "idv" in slider_vals:            row[idv_col]  = float(slider_vals["idv"])
    if "add_on_premium" in slider_vals: row[add_col]  = float(slider_vals["add_on_premium"])
    if "ncb" in slider_vals:            row[ncb_col]  = float(slider_vals["ncb"])

    # Baselines
    b_disc = float(filtered_row[disc_col])
    b_od   = float(filtered_row[od_col])
    b_tp   = float(filtered_row[tp_col])

    # Last-shown continuity holds (fallback to baseline if first time)
    od_hold = float(st.session_state.get("_od_hold", b_od))
    tp_hold = float(st.session_state.get("_tp_hold", b_tp))

    # Flags (set in on_change handlers)
    disc_is_derived   = bool(st.session_state.get("_disc_is_derived", False))
    disc_touched_user = bool(st.session_state.get("_disc_touched_user", False))
    od_touched        = bool(st.session_state.get("_od_touched", False))
    tp_touched        = bool(st.session_state.get("_tp_touched", False))

    def _clamp(v, key):
        mn = float(param_ranges[key]["min"]); mx = float(param_ranges[key]["max"])
        return min(max(v, mn), mx)

    def _commit_and_return():
        # Recompute GST/Total, then update continuity holds from what we *show*.
        recompute_totals_from_od_tp(row, 0.18)
        st.session_state["_od_hold"] = float(row[od_col])
        st.session_state["_tp_hold"] = float(row[tp_col])
        return row

    # ── Discount only: scale both from *last-shown* values
    if disc_touched_user and not od_touched and not tp_touched:
        new_d = float(row[disc_col])
        extra = max(0.0, (new_d - b_disc) / 100.0)
        base_od = od_hold
        base_tp = tp_hold
        row[od_col] = 0.0 if base_od <= _EPS else _clamp(base_od * (1.0 - extra), "od_premium")
        row[tp_col] = 0.0 if base_tp <= _EPS else _clamp(base_tp * (1.0 - extra), "tp_premium")
        st.session_state["_disc_touched_user"] = False
        return _commit_and_return()

    # ── OD-only (discount derived): keep TP at *last-shown*, infer Discount
    if od_touched and not tp_touched and not disc_touched_user and disc_is_derived:
        row[tp_col] = tp_hold
        row[disc_col] = infer_discount_from_odtp(filtered_row, row, param_ranges)
        st.session_state["_od_touched"] = False
        st.session_state["_disc_is_derived"] = False
        return _commit_and_return()

    # ── TP-only (discount derived): keep OD at *last-shown*, infer Discount
    if tp_touched and not od_touched and not disc_touched_user and disc_is_derived:
        row[od_col] = od_hold
        row[disc_col] = infer_discount_from_odtp(filtered_row, row, param_ranges)
        st.session_state["_tp_touched"] = False
        st.session_state["_disc_is_derived"] = False
        return _commit_and_return()

    # ── Discount + OD: respect user OD; scale only TP from *last-shown*
    if disc_touched_user and od_touched and not tp_touched:
        new_d = float(row[disc_col])
        extra = max(0.0, (new_d - b_disc) / 100.0)
        base_tp = tp_hold
        row[tp_col] = 0.0 if base_tp <= _EPS else _clamp(base_tp * (1.0 - extra), "tp_premium")
        st.session_state["_disc_touched_user"] = False
        st.session_state["_od_touched"] = False
        return _commit_and_return()

    # ── Discount + TP: respect user TP; scale only OD from *last-shown*
    if disc_touched_user and tp_touched and not od_touched:
        new_d = float(row[disc_col])
        extra = max(0.0, (new_d - b_disc) / 100.0)
        base_od = od_hold
        row[od_col] = 0.0 if base_od <= _EPS else _clamp(base_od * (1.0 - extra), "od_premium")
        st.session_state["_disc_touched_user"] = False
        st.session_state["_tp_touched"] = False
        return _commit_and_return()

    # ── OD + TP (no explicit discount): infer Discount from the two values
    if (od_touched or tp_touched) and not disc_touched_user:
        row[disc_col] = infer_discount_from_odtp(filtered_row, row, param_ranges)
        st.session_state["_od_touched"] = False
        st.session_state["_tp_touched"] = False
        st.session_state["_disc_is_derived"] = False
        return _commit_and_return()

    # Fallback: recompute + refresh holds
    return _commit_and_return()

def _ensure_history_state():
    st.session_state.setdefault("_history_rows", [])
    st.session_state.setdefault("_last_sig", None)
    st.session_state.setdefault("_last_row_for_history", None)

def _sig_from_row(row):
    # tuple signature to dedupe identical reruns
    return (
        float(row.get("applicable discount with ncb", 0.0)),
        float(row.get("vehicle idv", 0.0)),
        float(row.get("before gst add-on gwp", 0.0)),
        float(row.get("total od premium", 0.0)),
        float(row.get("total tp premium", 0.0)),
        float(row.get("gst", 0.0)),
        float(row.get("total premium payable", 0.0)),
        float(row.get("ncb % previous year", 0.0)),
    )

def _append_history(prev_row, new_row, prev_churn_pct, new_churn_pct):
    def _r(v): 
        try: return float(v)
        except: return 0.0

    rec = {
        "Prev Discount (%)": _r(prev_row.get("applicable discount with ncb", 0.0)),
        "New Discount (%)":  _r(new_row.get("applicable discount with ncb", 0.0)),
        "Δ Discount (pp)":   _r(new_row.get("applicable discount with ncb", 0.0)) - _r(prev_row.get("applicable discount with ncb", 0.0)),

        "Prev OD (₹)": _r(prev_row.get("total od premium", 0.0)),
        "New OD (₹)":  _r(new_row.get("total od premium", 0.0)),
        "Δ OD (₹)":    _r(new_row.get("total od premium", 0.0)) - _r(prev_row.get("total od premium", 0.0)),

        "Prev TP (₹)": _r(prev_row.get("total tp premium", 0.0)),
        "New TP (₹)":  _r(new_row.get("total tp premium", 0.0)),
        "Δ TP (₹)":    _r(new_row.get("total tp premium", 0.0)) - _r(prev_row.get("total tp premium", 0.0)),

        "Prev GST (₹)": _r(prev_row.get("gst", 0.0)),
        "New GST (₹)":  _r(new_row.get("gst", 0.0)),
        "Δ GST (₹)":    _r(new_row.get("gst", 0.0)) - _r(prev_row.get("gst", 0.0)),

        "Prev Total (₹)": _r(prev_row.get("total premium payable", 0.0)),
        "New Total (₹)":  _r(new_row.get("total premium payable", 0.0)),
        "Δ Total (₹)":    _r(new_row.get("total premium payable", 0.0)) - _r(prev_row.get("total premium payable", 0.0)),

        "Prev Churn %": _r(prev_churn_pct),
        "New Churn %":  _r(new_churn_pct),
        "Δ Churn (pp)": _r(new_churn_pct) - _r(prev_churn_pct),
    }
    st.session_state["_history_rows"].append(rec)
    # optional: cap length to avoid huge tables
    if len(st.session_state["_history_rows"]) > 200:
        st.session_state["_history_rows"] = st.session_state["_history_rows"][-200:]


# C ── schema retriever + LLM helpers  (selected_changes table)
@st.cache_resource
def _selected_schema_retriever():
    eng = _cloud_engine()
    df_sample = pd.read_sql(f"SELECT * FROM {FULL_SAVE_FQN} LIMIT 1;", eng)
    docs = [Document(page_content=f"{c}: column in selected_changes | sample={df_sample[c].iloc[0]}")
            for c in df_sample.columns]
    vs = FAISS.from_documents(docs, HuggingFaceEmbeddings(model_name="BAAI/bge-small-en-v1.5"))
    return vs.as_retriever(search_kwargs={"k": 6})

_llm_sql  = AzureAIChatCompletionsModel(endpoint=AZURE_INFERENCE_ENDPOINT, model=AZURE_INFERENCE_MODEL, credential=AZURE_INFERENCE_API_KEY, temperature=0)
_llm_mail = AzureAIChatCompletionsModel(endpoint=AZURE_INFERENCE_ENDPOINT, model=AZURE_INFERENCE_MODEL, credential=AZURE_INFERENCE_API_KEY, temperature=0.2)

def _gen_latest_sql(policy_no: str) -> str:
    hints = "\n".join(
        d.page_content for d in _selected_schema_retriever()
        .get_relevant_documents("latest row")
    )
    norm = _norm_policy(policy_no)
    norm_expr = "UPPER(REPLACE(REGEXP_REPLACE(policy_no::text,'^''',''),' ',''))"
    prompt = f"""
Return one PostgreSQL SELECT (no markdown, end with a semicolon)
that fetches the latest row (highest created_at) for policy '{norm}'
from {FULL_SAVE_FQN}. Compare policy using {norm_expr}.
Schema:
{hints}
"""
    sql = _llm_sql.invoke(prompt).content.strip()
    if not sql.lower().startswith("select"):
        sql = f"SELECT * FROM {FULL_SAVE_FQN} WHERE {norm_expr} = '{norm}' ORDER BY created_at DESC LIMIT 1;"
    return sql

def _fetch_latest_selected(policy_no: str) -> dict | None:
    try:
        eng = _cloud_engine()
        df = pd.read_sql(_gen_latest_sql(policy_no), eng)
        return None if df.empty else df.iloc[0].to_dict()
    except Exception:
        return None

SPECIAL_REASONS = {
    "Young Vehicle Age","Old Vehicle Age","Claims Happened",
    "Multiple Claims on Record","Minimal Policies Purchased","Tie Up with Non-OEM"
}

def _draft_email(row: dict) -> str:
    ctx = "; ".join(
        f"{k}={row.get(k)}"
        for k in ["vehicle","old_discount","new_discount","old_total_premium","new_total_premium",
                  "old_od","new_od","old_tp","new_tp","old_add_on_premium","new_add_on_premium",
                  "old_idv","new_idv","top_3_reasons"]
        if k in row
    )
    prompt = f"""
Subject: (placeholder)

You are an expert retention-email writer for car insurance.

Rules (do NOT reveal):
- First line must start with "Subject: ".
- Mention their policy details like vehicle names
- Use figures in ctx to show discount Δ (pp) and savings.
- Mention OD/TP impact.
- Address each reason in top_3_reasons; if any of {', '.join(SPECIAL_REASONS)} appear,
  add a reassurance line.
- Don't mention the top_3_reason in the draft.
- Do not use the phrase "we noticed".
- Do not use phrases like "renewal approaches".
- Friendly, persuasive, action-oriented. Finish with clear renewal CTA.

ctx: {ctx}
"""
    return _llm_mail.invoke(prompt).content.strip()

SUBJ_RE = re.compile(r"^\s*subject\s*:\s*(.+)$", re.I)
def _split_subj_body(text: str) -> tuple[str,str]:
    lines = text.splitlines(); subj=None; body=[]
    for l in lines:
        m = SUBJ_RE.match(l)
        if m and subj is None:
            subj = m.group(1).strip(); continue
        body.append(l)
    if subj is None:
        subj = next((l.strip() for l in body if l.strip()), "Policy Renewal Options")
        body = body[1:]
    return subj[:78], "\n".join(body).strip()

def _to_html(body_text: str) -> str:
    import re, html

    text = (body_text or "").strip()

    # Split into blocks by 2+ newlines (paragraph breaks)
    blocks = re.split(r'\n{2,}', text)

    def render_block(block: str) -> str:
        lines = [l.rstrip() for l in block.split('\n') if l.strip()]

        # Detect bullet list if most lines start with -, •, or –
        bullet_pat = re.compile(r'^\s*[-•–]\s+(.*)$')
        bullet_items = []
        non_bullet = False
        for l in lines:
            m = bullet_pat.match(l)
            if m:
                bullet_items.append(m.group(1))
            else:
                non_bullet = True
                break

        if bullet_items and not non_bullet:
            # Render as <ul>
            lis = "\n".join(f"<li>{html.escape(item)}</li>" for item in bullet_items)
            return f"<ul>{lis}</ul>"

        # Otherwise, render as a paragraph, preserving single newlines as <br>
        safe_lines = [html.escape(l) for l in lines]
        return f"<p>{'<br>'.join(safe_lines)}</p>"

    content = "\n".join(render_block(b) for b in blocks if b.strip())

    return """<!doctype html>
<html>
<head>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<style>
  .wrapper { width:100%; margin:0; padding:16px;
             font-family:Arial, sans-serif; font-size:14px; line-height:1.5; }
  p { margin:0 0 12px; }
  ul { margin:0 0 12px 20px; padding:0; }
  li { margin:4px 0; }
</style>
</head>
<body>
  <div class="wrapper">""" + content + """</div>
</body>
</html>"""



def _send_gmail(to_addr: str, subj: str, body: str) -> str:
    if not to_addr: return "Recipient missing."
    if not SENDER_EMAIL: return "SENDER_EMAIL env not set."

    scopes = ["https://www.googleapis.com/auth/gmail.send"]
    creds = None
    if os.path.exists(GMAIL_TOKEN_FILE):
        creds = _GCreds.from_authorized_user_file(GMAIL_TOKEN_FILE, scopes)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(_GRequest())
        else:
            creds = _GFlow.from_client_secrets_file(GMAIL_CREDENTIALS_FILE, scopes).run_local_server(port=0)
        with open(GMAIL_TOKEN_FILE, "w") as f:
            f.write(creds.to_json())

    srv = _gbuild("gmail","v1",credentials=creds)

    # multipart: plain (fallback) + html (pretty)
    msg = MIMEMultipart("alternative")
    msg["to"] = to_addr
    msg["from"] = SENDER_EMAIL
    msg["subject"] = subj
    msg.attach(MIMEText(body, "plain", "utf-8"))
    msg.attach(MIMEText(_to_html(body), "html", "utf-8"))

    raw = base64.urlsafe_b64encode(msg.as_bytes()).decode()
    sent = srv.users().messages().send(userId="me", body={"raw": raw}).execute()
    return f"Sent! Gmail id: {sent.get('id','')}"

# ====================== App ======================
def main():
    st.set_page_config(page_title="Churn Simulator", layout="centered")
    st.title("Churn Simulator")

    df = load_prediction_data()
    param_ranges = get_parameter_ranges()

    df["customerid"] = df["customerid"].astype(str)
    df["policy no"]  = df["policy no"].astype(str)
    churned_df = df[df["Customer Segment"].isin(["Elite Retainers", "Potential Customers"])].copy()

    customer_options = ["Select"] + sorted(churned_df["customerid"].unique())
    policy_options   = ["Select"] + sorted(churned_df["policy no"].unique())

    if "customerid" not in st.session_state: st.session_state.customerid = "Select"
    if "policyno" not in st.session_state: st.session_state.policyno = "Select"

    col1, col2 = st.columns(2)
    with col1:
        cust = st.selectbox("Select Customer ID", customer_options,
                            index=customer_options.index(st.session_state.customerid), key="cust")
        if cust != "Select":
            st.session_state.customerid = cust; st.session_state.policyno = "Select"
    with col2:
        pol = st.selectbox("Select Policy ID", policy_options,
                           index=policy_options.index(st.session_state.policyno), key="pol")
        if pol != "Select":
            st.session_state.policyno = pol; st.session_state.customerid = "Select"

    # pick row
    filtered_row = None
    if st.session_state.customerid != "Select":
        tmp = churned_df[churned_df["customerid"] == st.session_state.customerid]
        if not tmp.empty: filtered_row = tmp.iloc[0]
    elif st.session_state.policyno != "Select":
        tmp = churned_df[churned_df["policy no"] == st.session_state.policyno]
        if not tmp.empty: filtered_row = tmp.iloc[0]

    if filtered_row is None:
        st.info("Please select a Customer ID or Policy ID.")
        return

    # session containers for selector/save
    if "final_manual_row" not in st.session_state: st.session_state.final_manual_row = None
    if "final_auto_row" not in st.session_state: st.session_state.final_auto_row = None
    if "base_row_snapshot" not in st.session_state: st.session_state.base_row_snapshot = None
    st.session_state.base_row_snapshot = filtered_row.to_dict()

    # profile
    st.markdown("### Customer Profile Summary")
    st.markdown(f"- Policy No: {filtered_row['policy no']}")
    st.markdown(f"- Customer ID: {filtered_row['customerid']}")
    st.markdown(f"- Business Type: {filtered_row['biztype']}")
    st.markdown(f"- Tie-up Type: {filtered_row['tie up']}")
    st.markdown(f"- Zone: {filtered_row['Cleaned Zone 2']}")
    st.markdown(f"- State: {filtered_row['Cleaned State2']}")
    st.markdown(f"- Branch: {filtered_row['Cleaned Branch Name 2']}")
    st.markdown(f"- Vehicle: {filtered_row['make_clean']} {filtered_row['model_clean']} ({filtered_row['variant']})")
    st.markdown(f"- OD Premium: ₹{filtered_row['total od premium']:.0f}")
    st.markdown(f"- TP Premium: ₹{filtered_row['total tp premium']:.0f}")
    st.markdown(f"- Add-On Premium: ₹{filtered_row['before gst add-on gwp']:.0f}")
    st.markdown(f"- Discount: {filtered_row['applicable discount with ncb']}%")
    st.markdown(f"- NCB %: {filtered_row['ncb % previous year']:.0f}")
    st.markdown(f"- Vehicle IDV: ₹{filtered_row['vehicle idv']:.0f}")
    if "gst" in filtered_row and "total premium payable" in filtered_row:
        st.markdown(f"- GST: ₹{float(filtered_row['gst']):.0f}")
        st.markdown(f"- Total Premium: ₹{float(filtered_row['total premium payable']):.0f}")
    st.markdown(f"- Churn Risk %: {filtered_row['Churn Probability'] * 100:.2f}%")
    st.markdown(f"- Top 3 Reasons: {filtered_row.get('Top 3 Reasons','N/A')}")

    # reasons → sliders
    raw = str(filtered_row.get("Top 3 Reasons", ""))
    reasons = [r.strip() for r in re.split(r",|\band\b", raw) if r.strip()]
    param_keys = get_adjustable_parameters(reasons)

    # load model assets
    model = joblib.load("gbm_model.pkl")
    label_encoders = joblib.load("label_encoders_gbm.pkl")
    features = joblib.load("model_features_gbm.pkl")

    X_base = encode_row_for_model(filtered_row.copy(), features, label_encoders)
    baseline_model_pct = float(model.predict_proba(X_base)[0][1] * 100.0)

    # ============== Manual (reactive sliders) ==============
    if param_keys:
        st.markdown("### Adjust Parameters (Manual)")
        _init_manual_state(filtered_row, param_keys, param_ranges)
        _apply_pending_reset()

        orig_idv = float(filtered_row.get(PARAM_TO_COL["idv"], 0.0))
        for param in param_keys:
            if (param not in param_ranges) or (param not in PARAM_TO_COL) or (param not in COL_TITLES):
                continue
            label, col = COL_TITLES[param]
            mn = float(param_ranges[param]["min"])
            mx = float(param_ranges[param]["max"])
            if param == "idv":
                mx = min(mx, orig_idv * IDV_MAX_INCREASE_FACTOR)
            cv = float(st.session_state["slider_"+col])
            step = get_step_size(param, cv)
            if mn == mx: mx = mn + step

            if param == "discount":
                st.slider(label, min_value=mn, max_value=mx, value=st.session_state["slider_"+col],
                          step=step, format="%.0f", key="slider_"+col,
                          on_change=_on_discount_change, args=(filtered_row, param_ranges))
            elif param == "od_premium":
                st.slider(label, min_value=mn, max_value=mx, value=st.session_state["slider_"+col],
                          step=step, format="%.0f", key="slider_"+col,
                          on_change=_on_od_change, args=(filtered_row, param_ranges))
            elif param == "tp_premium":
                st.slider(label, min_value=mn, max_value=mx, value=st.session_state["slider_"+col],
                          step=step, format="%.0f", key="slider_"+col,
                          on_change=_on_tp_change, args=(filtered_row, param_ranges))
            else:
                st.slider(label, min_value=mn, max_value=mx, value=st.session_state["slider_"+col],
                          step=step, format="%.0f", key="slider_"+col)

        if st.button("Reset to Baseline"):
            _reset_manual_sliders(filtered_row, param_keys)

        slider_vals = {p: float(st.session_state["slider_"+PARAM_TO_COL[p]]) for p in param_keys}
        coupled_row = apply_manual_two_way_coupling(filtered_row, param_ranges, slider_vals)

        # Ensure history state exists
        _ensure_history_state()

        # compute LIVE churn for current coupled_row
        Xp = encode_row_for_model(coupled_row, features, label_encoders)
        updated_pct = float(model.predict_proba(Xp)[0][1] * 100.0)

        # --- Seed the history on first run *before* reading it
        if st.session_state.get("_last_sig") is None:
            st.session_state["_last_row_for_history"]   = filtered_row.to_dict()
            st.session_state["_last_churn_for_history"] = baseline_model_pct
            st.session_state["_last_sig"]               = _sig_from_row(filtered_row)

        # Now it's safe to read these
        curr_sig      = _sig_from_row(coupled_row)
        prev_row_hist = st.session_state["_last_row_for_history"]
        prev_pct_hist = st.session_state["_last_churn_for_history"]

        # Only append if something actually changed
        if curr_sig != st.session_state["_last_sig"]:
            _append_history(prev_row_hist, coupled_row, prev_pct_hist, updated_pct)
            st.session_state["_last_row_for_history"]   = dict(coupled_row)
            st.session_state["_last_churn_for_history"] = updated_pct
            st.session_state["_last_sig"]               = curr_sig


        st.markdown("##### Live Summary (growing log)")
        if st.button("Clear history"):
            st.session_state["_history_rows"] = []
            st.session_state["_last_sig"] = None
            st.session_state["_last_row_for_history"] = None
            st.session_state["_last_churn_for_history"] = baseline_model_pct
        # Show the latest first (optional)
        hist_df = pd.DataFrame(st.session_state["_history_rows"])
        if not hist_df.empty:
            # Add a Step column
            hist_df.insert(0, "Step", range(1, len(hist_df) + 1))
            st.dataframe(hist_df, use_container_width=True)
        else:
            st.info("Start moving sliders to build the change log (Prev vs New).")


        st.markdown("### Churn Risk Comparison (Manual) — Live")

        baseline_color = "red" if baseline_model_pct > updated_pct else "green"
        updated_color  = "red" if updated_pct > baseline_model_pct else "green"

        fig = go.Figure()
        fig.add_trace(go.Indicator(
            mode="gauge+number+delta",
            value=baseline_model_pct,
            delta={'reference': updated_pct, 'increasing': {'color': "red"}, 'decreasing': {'color': "green"}},
            domain={'x': [0.0, 0.45], 'y': [0, 1]},
            title={'text': "Baseline Churn %"},
            gauge={'axis': {'range': [0, 100]}, 'bar': {'color': baseline_color}}
        ))
        fig.add_trace(go.Indicator(
            mode="gauge+number+delta",
            value=updated_pct,
            delta={'reference': baseline_model_pct, 'increasing': {'color': "red"}, 'decreasing': {'color': "green"}},
            domain={'x': [0.55, 1.0], 'y': [0, 1]},
            title={'text': "Manual Setup Churn %"},
            gauge={'axis': {'range': [0, 100]}, 'bar': {'color': updated_color}}
        ))
        fig.update_layout(height=350)
        st.plotly_chart(fig, use_container_width=True)

        # Keep the latest manual setup ready for "Selected changes"
        st.session_state.final_manual_row = dict(coupled_row)

        st.markdown("#### ✅ Final Manual Changes (Live)")
        for title, col, sym in [
            ("Applicable Discount", "applicable discount with ncb", "%"),
            ("Vehicle IDV", "vehicle idv", "₹"),
            ("Add-On Premium", "before gst add-on gwp", "₹"),
            ("OD Premium", "total od premium", "₹"),
            ("TP Premium", "total tp premium", "₹"),
            ("GST", "gst", "₹"),
            ("Total Premium", "total premium payable", "₹"),
            ("NCB %", "ncb % previous year", "%"),
        ]:
            if col in filtered_row and col in coupled_row:
                old, new = float(filtered_row[col]), float(coupled_row[col])
                if abs(new - old) > 1e-6:
                    if sym == "%":
                        st.write(f"- **{title}**: {old:.0f}% → **{new:.0f}%**")
                    else:
                        st.write(f"- **{title}**: {sym}{old:.0f} → **{sym}{new:.0f}**")



    # ================= Smart Auto-Suggest =================
    st.markdown("---"); st.markdown("### ▶️ Smart Auto-Suggest")
    if st.button("Suggest Changes"):
        selected_params = set(param_keys) if param_keys else None
        trials, best_idx, baseline_pct2 = run_directional_moves_until_target(
            filtered_row, features, label_encoders, model,
            param_ranges, reasons, selected_params, target=50.0, max_k=50
        )
        if not trials:
            st.info("No trials were generated (next step would hit zero immediately or bounds reached).")
            return

        st.dataframe(pd.DataFrame([{
            "k": t["k_moves"], "Changed": ", ".join(t["moved"]) if t["moved"] else "-",
            "Discount": round(t["discount"], 2), "IDV": round(t["idv"], 0),
            "Add-on": round(t["addon"], 0), "NCB %": round(t["ncb"], 2),
            "OD": round(t["od"], 2), "TP": round(t["tp"], 2),
            "GST(18%)": round(t["gst"], 2), "Total Prem": round(t["total_premium"], 2),
            "Churn %": round(t["churn_pct"], 2),
        } for t in trials]))

        if best_idx is None: best_idx = len(trials) - 1
        TOL = 1e-9
        has_improvement = trials[best_idx]["churn_pct"] + TOL < baseline_pct2

        fallback_used = False
        if has_improvement:
            chosen, shown_pct, right_title, right_bar_color = trials[best_idx], trials[best_idx]["churn_pct"], "Chosen Moves Churn %", "green"
        else:
            chosen = trials[-1]; shown_pct = max(0.0, baseline_pct2 - 10.0)
            right_title, right_bar_color, fallback_used = "Plan Target (−10 pp)", "green", True

        figc = go.Figure()
        figc.add_trace(go.Indicator(mode="gauge+number+delta", value=baseline_pct2,
                                    delta={'reference': shown_pct,'increasing': {'color': "red"},'decreasing': {'color': "green"}},
                                    domain={'x': [0.0, 0.45], 'y': [0, 1]},
                                    title={'text': "Baseline Churn %"},
                                    gauge={'axis': {'range': [0, 100]},
                                           'bar': {'color': "red" if baseline_pct2 > shown_pct else "green"}}))
        figc.add_trace(go.Indicator(mode="gauge+number+delta", value=shown_pct,
                                    delta={'reference': baseline_pct2,'increasing': {'color': "red"},'decreasing': {'color': "green"}},
                                    domain={'x': [0.55, 1.0], 'y': [0, 1]},
                                    title={'text': right_title},
                                    gauge={'axis': {'range': [0, 100]},
                                           'bar': {'color': right_bar_color}}))
        figc.update_layout(height=320)
        st.plotly_chart(figc, use_container_width=True)

        if fallback_used:
            st.info(f"Model did not produce any trial with lower churn than baseline ({baseline_pct2:.2f}%). "
                    f"The displayed {shown_pct:.2f}% is a manually set reference (baseline −10pp), not a model prediction.")

        st.markdown("#### ✅ Final Suggested Changes")
        final_row = chosen["row"]
        for title, col, sym in [
            ("Applicable Discount", "applicable discount with ncb", "%"),
            ("Vehicle IDV", "vehicle idv", "₹"),
            ("Add-On Premium", "before gst add-on gwp", "₹"),
            ("OD Premium", "total od premium", "₹"),
            ("TP Premium", "total tp premium", "₹"),
            ("GST", "gst", "₹"),
            ("Total Premium", "total premium payable", "₹"),
            ("NCB %", "ncb % previous year", "%"),
        ]:
            if col in filtered_row and col in final_row:
                old, new = float(filtered_row[col]), float(final_row[col])
                if abs(new - old) > 1e-6:
                    if sym == "%": st.write(f"- **{title}**: {old:.0f}% → **{new:.0f}%**")
                    else:          st.write(f"- **{title}**: {sym}{old:.0f} → **{sym}{new:.0f}**")

        st.session_state.final_auto_row = dict(final_row)

    # === Unified selector to SHOW and SAVE either set of “Selected changes” ===
    st.markdown("---")
    st.markdown("### 🟩 Selected changes")
    choice_col, btn_show_col, btn_save_col = st.columns([3,1,2])
    with choice_col:
        selected_plan = st.radio(
            "Choose which final changes to show/save:",
            ["Manual Simulation", "Smart Auto-suggest"],
            horizontal=True,
            key="selected_changes_choice"
        )
    with btn_show_col:
        show_now = st.button("Show Selected changes")
    with btn_save_col:
        save_now = st.button("Save Selected changes to DB")

    if show_now or save_now:
        base = st.session_state.get("base_row_snapshot")
        if selected_plan == "Manual Simulation":
            row = st.session_state.get("final_manual_row")
            plan_label = "Manual Simulation"
        else:
            row = st.session_state.get("final_auto_row")
            plan_label = "Smart Auto-suggest"

        if row is None:
            st.warning(f"Run **{plan_label}** first to generate final changes.")
        else:
            if show_now:
                st.markdown(f"#### ✅ Final Suggested changes ({plan_label})")
                for title, col, sym in [
                    ("Applicable Discount", "applicable discount with ncb", "%"),
                    ("Vehicle IDV", "vehicle idv", "₹"),
                    ("Add-On Premium", "before gst add-on gwp", "₹"),
                    ("OD Premium", "total od premium", "₹"),
                    ("TP Premium", "total tp premium", "₹"),
                    ("GST", "gst", "₹"),
                    ("Total Premium", "total premium payable", "₹"),
                    ("NCB %", "ncb % previous year", "%"),
                ]:
                    if base is not None and (col in base) and (col in row):
                        old, new = float(base[col]), float(row[col])
                        if abs(new - old) > 1e-6:
                            if sym == "%": st.write(f"- **{title}**: {old:.0f}% → **{new:.0f}%**")
                            else:          st.write(f"- **{title}**: {sym}{old:.0f} → **{sym}{new:.0f}**")

            if save_now:
                try:
                    _save_selected_changes_to_db(base, row, plan_label)
                    st.success(f"Saved Selected changes for Policy {base.get('policy no','')} ({plan_label}) to {SAVE_SCHEMA}.{SAVE_TABLE}.")
                    _render_latest_selected_change(str(base.get("policy no","")))
                except Exception as e:
                    st.error(f"Failed to save changes: {e}")

    # ───────────────── Retention section ─────────────────
    st.markdown("---")
    st.markdown("### 💌 Retention")

    if "email_subj" not in st.session_state:  st.session_state.email_subj  = ""
    if "email_body" not in st.session_state:  st.session_state.email_body = ""

    if st.button("Draft Email ✍️"):
        latest = _fetch_latest_selected(str(filtered_row["policy no"]))
        if latest is None:
            st.warning("No Selected-changes row found. Save changes first.")
        else:
            draft = _draft_email(latest)
            subj, body = _split_subj_body(draft)
            st.session_state.email_subj  = subj
            st.session_state.email_body  = body
            st.success("Draft generated below – you can edit.")

    st.text_input("Subject", key="email_subj")
    st.text_area("Email body", key="email_body", height=250)
    to_addr = st.text_input("To", value=DEFAULT_TO_EMAIL)

    if st.button("Send Email 🚀"):
        result = _send_gmail(to_addr, st.session_state.email_subj, st.session_state.email_body)
        (st.success if result.startswith("Sent") else st.error)(result)

if __name__ == "__main__":
    main()
