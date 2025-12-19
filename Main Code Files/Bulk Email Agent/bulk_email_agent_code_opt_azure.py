# =========================
# Bulk Email Agent (Streamlit) — single source table
# =========================
import os, re, base64, time, uuid
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine
from sqlalchemy import text as _sqltext
import joblib
from dotenv import load_dotenv
load_dotenv()
from datetime import datetime
from zoneinfo import ZoneInfo 
from sqlalchemy import create_engine, text as _sqltext, event


# Display timezone for timestamptz (kept in UTC in DB)
LOCAL_TZ = os.getenv("LOCAL_TZ", "Asia/Kolkata")

def _to_local_tz_formatted(series, tz_name: str, fmt: str = "%Y-%m-%d %H:%M:%S %Z"):
    """
    Parse -> assume/convert to UTC -> convert to display tz -> format as string.
    Works whether input is tz-naive or tz-aware.
    """
    s = pd.to_datetime(series, errors="coerce", utc=True)  # naive -> UTC; aware -> converted to UTC
    return s.dt.tz_convert(tz_name).dt.strftime(fmt)

# --------------------------
# CONFIG
# --------------------------
PRED_DB_CONFIG = {
    'host': '139.59.12.79',
    'database': 'updated_ui_db',
    'user': 'appadmin',
    'password': 'prowesstics',
    'port': '5432'
}
BULK_SOURCE_FQN   = '"Prediction"."sample_bulk_email_data"'  # <- sole source
BULK_CHANGES_FQN  = '"Prediction"."bulk_selected_changes"'
BULK_DRAFTS_FQN   = '"Prediction"."bulk_email_drafts"'
# 👇 alias used by your helper block (selected_changes)
FULL_SAVE_FQN     = BULK_CHANGES_FQN

# Model files (must match columns in sample_bulk_email_data)
MODEL_FILE   = "gbm_model.pkl"
LE_FILE      = "label_encoders_gbm.pkl"
FEATS_FILE   = "model_features_gbm.pkl"

# Gmail env
SENDER_EMAIL           = os.getenv("SENDER_EMAIL", "")
GMAIL_CREDENTIALS_FILE = os.getenv("GMAIL_CREDENTIALS_FILE", "credentials.json")
GMAIL_TOKEN_FILE       = os.getenv("GMAIL_TOKEN_FILE", "token.json")
DEFAULT_TO_EMAIL       = os.getenv("DEFAULT_TO_EMAIL", "")

# LLM for drafting
from langchain_azure_ai.chat_models.inference import AzureAIChatCompletionsModel
AZURE_INFERENCE_API_KEY   = os.getenv("AZURE_INFERENCE_API_KEY")
AZURE_INFERENCE_MODEL     = os.getenv("AZURE_INFERENCE_MODEL")
AZURE_INFERENCE_ENDPOINT     = os.getenv("AZURE_INFERENCE_ENDPOINT")

# Gmail libs
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText as _MIMEText
from google.auth.transport.requests import Request as _GRequest
from google.oauth2.credentials import Credentials as _GCreds
from google_auth_oauthlib.flow import InstalledAppFlow as _GFlow
from googleapiclient.discovery import build as _gbuild

# --------------------------
# Core constants & mappings
# --------------------------
_EPS = 1e-9
IDV_MAX_INCREASE_FACTOR = 1.30
IDV_PERCENT_PER_K = 0.10
AUTO_RELATIVE_BAND = 0.30

PARAM_TO_COL = {
    "discount":       "applicable discount with ncb",
    "od_premium":     "total od premium",
    "tp_premium":     "total tp premium",
    "idv":            "vehicle idv",
    "add_on_premium": "before gst add-on gwp",
    "ncb":            "ncb % previous year"
}

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

def recompute_totals_from_od_tp(row, gst_rate=0.18):
    od = float(row.get("total od premium", 0.0))
    tp = float(row.get("total tp premium", 0.0))
    gst = (od + tp) * float(gst_rate)
    total = od + tp + gst
    row["gst"] = gst
    row["total premium payable"] = total

# -------- DB-FREE GUARDRAILS --------
def get_parameter_ranges():
    return {
        "discount":       {"min": 0.0, "max": 90.0},
        "ncb":            {"min": 0.0, "max": 90.0},
        "od_premium":     {"min": 0.0, "max": float("inf")},
        "tp_premium":     {"min": 0.0, "max": float("inf")},
        "add_on_premium": {"min": 0.0, "max": float("inf")},
        "idv":            {"min": 0.0, "max": float("inf")},
    }

# === Model encode
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

# === Reason→params
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

# === Coupling & caps
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

def _enforce_auto_caps(original_row: dict, test_row: dict, param_ranges: dict, band: float = AUTO_RELATIVE_BAND) -> None:
    for p_key, col in PARAM_TO_COL.items():
        if col not in original_row or col not in test_row:
            continue
        try:
            base = float(original_row[col]); val = float(test_row[col])
        except Exception:
            continue
        if p_key in ("discount", "ncb"):
            mn = float(param_ranges[p_key]["min"]); mx = float(param_ranges[p_key]["max"])
            test_row[col] = min(max(val, mn), mx); continue
        lo = base * (1.0 - band); hi = base * (1.0 + band)
        if p_key in ("od_premium", "tp_premium", "add_on_premium"): lo = max(0.0, lo)
        if p_key == "idv": hi = min(hi, base * IDV_MAX_INCREASE_FACTOR)
        test_row[col] = min(max(val, lo), hi)
    recompute_totals_from_od_tp(test_row, 0.18)

# === Auto-suggest core
def build_combo_for_k(original_row, direction, param_ranges, k_moves, reasons, selected_params=None):
    test_row = original_row.copy()
    moved = set()
    stop_due_to_zero = False
    in_play = set(selected_params) if selected_params else set(direction.keys())

    disc_col = PARAM_TO_COL["discount"]; ncb_col = PARAM_TO_COL["ncb"]
    od_col = PARAM_TO_COL["od_premium"]; tp_col = PARAM_TO_COL["tp_premium"]
    idv_col = PARAM_TO_COL["idv"]; add_col = PARAM_TO_COL["add_on_premium"]

    orig_disc = float(original_row.get(disc_col, 0.0))
    orig_ncb  = float(original_row.get(ncb_col, 0.0))
    orig_od   = float(original_row.get(od_col, 0.0))
    orig_tp   = float(original_row.get(tp_col, 0.0))
    orig_add  = float(original_row.get(add_col, 0.0))
    orig_idv  = float(original_row.get(idv_col, 0.0))

    def _clamp_val(p_key, val):
        mn = float(param_ranges[p_key]["min"])
        mx = float(param_ranges[p_key]["max"])
        if p_key in ("od_premium", "tp_premium", "add_on_premium"):
            mn = max(0.0, mn)
        return min(max(val, mn), mx)

    if "idv" in direction:
        pct  = min(k_moves * IDV_PERCENT_PER_K, IDV_MAX_INCREASE_FACTOR - 1.0)
        cand = orig_idv * (1.0 + pct)
        test_row[idv_col] = _clamp_val("idv", cand); moved.add("idv")

    has_discount_play = "discount" in in_play
    if has_discount_play:
        if orig_disc >= 70.0:
            new_disc = min(90.0, orig_disc + 10.0 * k_moves)
        else:
            steps = min(k_moves, 3)
            new_disc = min(orig_disc + 10.0 * steps, orig_disc + 30.0)
        if abs(new_disc - orig_disc) > 1e-9:
            test_row[disc_col] = _clamp_val("discount", new_disc); moved.add("discount")

        hit_zero = apply_discount_coupling_on_od_tp(original_row, test_row, param_ranges, 0.18, True)
        try:
            if abs(float(test_row[od_col]) - orig_od) > 1e-9: moved.add("od_premium")
            if abs(float(test_row[tp_col]) - orig_tp) > 1e-9: moved.add("tp_premium")
        except Exception:
            pass
        if hit_zero:
            recompute_totals_from_od_tp(test_row, 0.18)
            _enforce_auto_caps(original_row, test_row, param_ranges, AUTO_RELATIVE_BAND)
            return test_row, moved, True

        if "ncb" in in_play:
            new_ncb = min(90.0, orig_ncb + 10.0 * k_moves)
            if abs(new_ncb - orig_ncb) > 1e-9:
                test_row[ncb_col] = _clamp_val("ncb", new_ncb); moved.add("ncb")

        _enforce_auto_caps(original_row, test_row, param_ranges, AUTO_RELATIVE_BAND)
        return test_row, moved, False

    cut_factor = 1.0 - 0.10 * min(max(k_moves, 1), 3)
    if "od_premium" in in_play:
        test_row[od_col] = _clamp_val("od_premium", orig_od * cut_factor); moved.add("od_premium")
    if "tp_premium" in in_play:
        test_row[tp_col] = _clamp_val("tp_premium", orig_tp * cut_factor); moved.add("tp_premium")
    if "add_on_premium" in in_play:
        test_row[add_col] = _clamp_val("add_on_premium", orig_add * cut_factor); moved.add("add_on_premium")

    if ("od_premium" in in_play) or ("tp_premium" in in_play):
        new_disc = infer_discount_from_odtp(original_row, test_row, param_ranges)
        if abs(new_disc - float(test_row.get(disc_col, orig_disc))) > 1e-9:
            test_row[disc_col] = _clamp_val("discount", new_disc); moved.add("discount")

    recompute_totals_from_od_tp(test_row, 0.18)
    _enforce_auto_caps(original_row, test_row, param_ranges, AUTO_RELATIVE_BAND)
    return test_row, moved, False

def run_directional_moves_until_target(row, features, label_encoders, model,
                                       param_ranges, reasons, selected_params=None,
                                       target=50.0, max_k=50):
    direction = get_param_direction(reasons)
    default_dir = {"discount":"increase","ncb":"increase","idv":"increase",
                   "add_on_premium":"decrease","od_premium":"decrease","tp_premium":"decrease"}
    if selected_params:
        for p in selected_params:
            if p not in direction and p in default_dir:
                direction[p] = default_dir[p]

    X_base = encode_row_for_model(row.copy(), features, label_encoders)
    baseline_pct = float(model.predict_proba(X_base)[0][1] * 100.0)

    trials, best_idx, best_churn, prev_sig = [], None, baseline_pct, None
    for k in range(1, max_k+1):
        test_row, moved, stop_zero = build_combo_for_k(
            row, direction, param_ranges, k, [], selected_params
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
        trials.append({"k_moves": k, "moved": sorted(list(moved)) if moved else [],
                       "row": test_row, "churn_pct": churn_pct,
                       "od": float(test_row["total od premium"]), "tp": float(test_row["total tp premium"]),
                       "gst": float(test_row["gst"]), "total_premium": float(test_row["total premium payable"]),
                       "discount": float(test_row["applicable discount with ncb"]),
                       "idv": float(test_row["vehicle idv"]), "addon": float(test_row["before gst add-on gwp"]),
                       "ncb": float(test_row["ncb % previous year"])})
        if churn_pct < best_churn: best_churn, best_idx = churn_pct, len(trials)-1
        if churn_pct < target: break
    return trials, best_idx, baseline_pct

# --------------------------
# C ── schema retriever + LLM helpers  (selected_changes table)
# --------------------------
from langchain_community.vectorstores import FAISS
from langchain_community.embeddings import HuggingFaceEmbeddings
from langchain.docstore.document import Document

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

# --------------------------
# Drafting & Gmail
# --------------------------
SPECIAL_REASONS = {
    "Young Vehicle Age","Old Vehicle Age","Claims Happened",
    "Multiple Claims on Record","Minimal Policies Purchased","Tie Up with Non-OEM"
}

def _to_html(body_text: str) -> str:
    import html
    text = (body_text or "").strip()
    blocks = re.split(r'\n{2,}', text)
    def render_block(block: str) -> str:
        lines = [l.rstrip() for l in block.split('\n') if l.strip()]
        bullet_pat = re.compile(r'^\s*[-•–]\s+(.*)$')
        bullet_items = []
        non_bullet = False
        for l in lines:
            m = bullet_pat.match(l)
            if m: bullet_items.append(m.group(1))
            else: non_bullet = True; break
        if bullet_items and not non_bullet:
            lis = "\n".join(f"<li>{html.escape(item)}</li>" for item in bullet_items)
            return f"<ul>{lis}</ul>"
        safe_lines = [html.escape(l) for l in lines]
        return f"<p>{'<br>'.join(safe_lines)}</p>"
    content = "\n".join(render_block(b) for b in blocks if b.strip())
    return f"""<!doctype html>
<html><head><meta name="viewport" content="width=device-width, initial-scale=1"/>
<style>
.wrapper {{ width:100%; margin:0; padding:16px; font-family:Arial,sans-serif; font-size:14px; line-height:1.5; }}
p {{ margin:0 0 12px; }} ul {{ margin:0 0 12px 20px; padding:0; }} li {{ margin:4px 0; }}
</style></head><body><div class="wrapper">{content}</div></body></html>"""

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
    msg = MIMEMultipart("alternative")
    msg["to"] = to_addr
    msg["from"] = SENDER_EMAIL
    msg["subject"] = subj
    msg.attach(_MIMEText(body, "plain", "utf-8"))
    msg.attach(_MIMEText(_to_html(body), "html", "utf-8"))
    raw = base64.urlsafe_b64encode(msg.as_bytes()).decode()
    sent = srv.users().messages().send(userId="me", body={"raw": raw}).execute()
    return f"Sent! Gmail id: {sent.get('id','')}"

# --------------------------
# DB ensure + helpers
# --------------------------

# NEW: de-dupe helper (keeps latest row per policy_no_norm using created_at)
def _dedupe_latest_per_policy(eng):
    """
    Keep only the latest row per policy_no_norm in both tables.
    Uses created_at DESC (and ctid as a tie-breaker).
    Safe to run repeatedly.
    """
    with eng.begin() as c:
        # Ensure created_at exists for ranking
        c.execute(_sqltext(f'''
            UPDATE {BULK_CHANGES_FQN} SET created_at = NOW() WHERE created_at IS NULL;
        '''))
        c.execute(_sqltext(f'''
            UPDATE {BULK_DRAFTS_FQN} SET created_at = NOW() WHERE created_at IS NULL;
        '''))

        # Remove duplicates in bulk_selected_changes
        c.execute(_sqltext(f'''
            DELETE FROM {BULK_CHANGES_FQN} t
            USING (
              SELECT ctid,
                     row_number() OVER (
                       PARTITION BY policy_no_norm
                       ORDER BY created_at DESC, ctid DESC
                     ) AS rn
              FROM {BULK_CHANGES_FQN}
            ) d
            WHERE t.ctid = d.ctid
              AND d.rn > 1;
        '''))

        # Remove duplicates in bulk_email_drafts
        c.execute(_sqltext(f'''
            DELETE FROM {BULK_DRAFTS_FQN} t
            USING (
              SELECT ctid,
                     row_number() OVER (
                       PARTITION BY policy_no_norm
                       ORDER BY created_at DESC, ctid DESC
                     ) AS rn
              FROM {BULK_DRAFTS_FQN}
            ) d
            WHERE t.ctid = d.ctid
              AND d.rn > 1;
        '''))

def _ensure_bulk_tables(eng):
    ddl_changes = f"""
    CREATE TABLE IF NOT EXISTS {BULK_CHANGES_FQN} (
        policy_no              TEXT,
        policy_no_norm         TEXT,
        customerid             TEXT,
        segment                TEXT,
        batch_id               TEXT,
        vehicle                TEXT,
        old_discount           DOUBLE PRECISION,
        old_ncb                DOUBLE PRECISION,
        old_idv                DOUBLE PRECISION,
        old_add_on_premium     DOUBLE PRECISION,
        old_od                 DOUBLE PRECISION,
        old_tp                 DOUBLE PRECISION,
        old_gst                DOUBLE PRECISION,
        old_total_premium      DOUBLE PRECISION,
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
    ddl_drafts = f"""
    CREATE TABLE IF NOT EXISTS {BULK_DRAFTS_FQN} (
        policy_no          TEXT,
        policy_no_norm     TEXT,
        to_email           TEXT,
        subject            TEXT,
        body_text          TEXT,
        body_html          TEXT,
        segment            TEXT,
        batch_id           TEXT,
        status             TEXT DEFAULT 'drafted',
        gmail_message_id   TEXT,
        error_text         TEXT,
        created_at         TIMESTAMPTZ DEFAULT NOW(),
        sent_at            TIMESTAMPTZ
    );
    """
    # 1) Create tables first (no changes here)
    with eng.begin() as c:
        c.execute(_sqltext(ddl_changes))
        c.execute(_sqltext(ddl_drafts))

    # 2) De-duplicate BEFORE creating unique indexes (NEW)
    _dedupe_latest_per_policy(eng)

    # 3) Non-unique/supporting indexes + drop old per-batch uniques
    with eng.begin() as c:
        c.execute(_sqltext(f'CREATE INDEX IF NOT EXISTS idx_bulk_changes_pol_norm ON {BULK_CHANGES_FQN}(policy_no_norm);'))
        c.execute(_sqltext(f'CREATE INDEX IF NOT EXISTS idx_bulk_drafts_pol_norm  ON {BULK_DRAFTS_FQN}(policy_no_norm);'))
        c.execute(_sqltext(f'CREATE INDEX IF NOT EXISTS idx_bulk_drafts_status    ON {BULK_DRAFTS_FQN}(status);'))
        c.execute(_sqltext(f'CREATE INDEX IF NOT EXISTS idx_bulk_changes_created  ON {BULK_CHANGES_FQN}(created_at);'))
        c.execute(_sqltext(f'CREATE INDEX IF NOT EXISTS idx_bulk_drafts_created   ON {BULK_DRAFTS_FQN}(created_at);'))
        c.execute(_sqltext('DROP INDEX IF EXISTS uq_bulk_changes_pol_batch;'))
        c.execute(_sqltext('DROP INDEX IF EXISTS uq_bulk_drafts_pol_batch;'))

    # 4) Unique indexes (ONE row per policy) — same as your intent, now safe after dedupe
    with eng.begin() as c:
        c.execute(_sqltext(f'CREATE UNIQUE INDEX IF NOT EXISTS uq_bulk_changes_pol ON {BULK_CHANGES_FQN}(policy_no_norm);'))
        c.execute(_sqltext(f'CREATE UNIQUE INDEX IF NOT EXISTS uq_bulk_drafts_pol  ON {BULK_DRAFTS_FQN}(policy_no_norm);'))

def _save_bulk_selected_change(eng, base_row: dict, final_row: dict, segment: str, batch_id: str):
    if ("gst" not in base_row) or ("total premium payable" not in base_row):
        recompute_totals_from_od_tp(base_row, 0.18)
    if ("gst" not in final_row) or ("total premium payable" not in final_row):
        recompute_totals_from_od_tp(final_row, 0.18)

    rec = {
        "policy_no": str(base_row.get("policy no","")),
        "policy_no_norm": _norm_policy(base_row.get("policy no","")),
        "customerid": str(base_row.get("customerid","")),
        "segment": segment,
        "batch_id": batch_id,
        "vehicle": _policy_vehicle_str(base_row),
        "old_discount": float(base_row.get("applicable discount with ncb", 0.0)),
        "old_ncb":      float(base_row.get("ncb % previous year", 0.0)),
        "old_idv":      float(base_row.get("vehicle idv", 0.0)),
        "old_add_on_premium": float(base_row.get("before gst add-on gwp", 0.0)),
        "old_od":       float(base_row.get("total od premium", 0.0)),
        "old_tp":       float(base_row.get("total tp premium", 0.0)),
        "old_gst":      float(base_row.get("gst", 0.0)),
        "old_total_premium": float(base_row.get("total premium payable", 0.0)),
        "new_discount": float(final_row.get("applicable discount with ncb", final_row.get("discount", 0.0))),
        "new_ncb":      float(final_row.get("ncb % previous year", final_row.get("ncb", 0.0))),
        "new_idv":      float(final_row.get("vehicle idv", final_row.get("idv", 0.0))),
        "new_add_on_premium": float(final_row.get("before gst add-on gwp", final_row.get("add_on_premium", 0.0))),
        "new_od":       float(final_row.get("total od premium", final_row.get("od", 0.0))),
        "new_tp":       float(final_row.get("total tp premium", final_row.get("tp", 0.0))),
        "new_gst":      float(final_row.get("gst", (float(final_row.get("od",0))+float(final_row.get("tp",0)))*0.18)),
        "new_total_premium": float(final_row.get("total premium payable", final_row.get("total_premium", 0.0))),
        "churn_risk_pct": float(base_row.get("Churn Probability", 0.0)) * 100.0,
        "top_3_reasons": str(base_row.get("Top 3 Reasons","") or base_row.get("top 3 reasons","")),
    }

    with eng.begin() as c:
        c.execute(_sqltext(f"""
            INSERT INTO {BULK_CHANGES_FQN} (
                policy_no, policy_no_norm, customerid, segment, batch_id, vehicle,
                old_discount, old_ncb, old_idv, old_add_on_premium, old_od, old_tp, old_gst, old_total_premium,
                new_discount, new_ncb, new_idv, new_add_on_premium, new_od, new_tp, new_gst, new_total_premium,
                churn_risk_pct, top_3_reasons, created_at
            )
            VALUES (
                :policy_no, :policy_no_norm, :customerid, :segment, :batch_id, :vehicle,
                :old_discount, :old_ncb, :old_idv, :old_add_on_premium, :old_od, :old_tp, :old_gst, :old_total_premium,
                :new_discount, :new_ncb, :new_idv, :new_add_on_premium, :new_od, :new_tp, :new_gst, :new_total_premium,
                :churn_risk_pct, :top_3_reasons, NOW()
            )
            ON CONFLICT (policy_no_norm) DO UPDATE
            SET
                customerid          = EXCLUDED.customerid,
                segment             = EXCLUDED.segment,
                batch_id            = EXCLUDED.batch_id,
                vehicle             = EXCLUDED.vehicle,
                old_discount        = EXCLUDED.old_discount,
                old_ncb             = EXCLUDED.old_ncb,
                old_idv             = EXCLUDED.old_idv,
                old_add_on_premium  = EXCLUDED.old_add_on_premium,
                old_od              = EXCLUDED.old_od,
                old_tp              = EXCLUDED.old_tp,
                old_gst             = EXCLUDED.old_gst,
                old_total_premium   = EXCLUDED.old_total_premium,
                new_discount        = EXCLUDED.new_discount,
                new_ncb             = EXCLUDED.new_ncb,
                new_idv             = EXCLUDED.new_idv,
                new_add_on_premium  = EXCLUDED.new_add_on_premium,
                new_od              = EXCLUDED.new_od,
                new_tp              = EXCLUDED.new_tp,
                new_gst             = EXCLUDED.new_gst,
                new_total_premium   = EXCLUDED.new_total_premium,
                churn_risk_pct      = EXCLUDED.churn_risk_pct,
                top_3_reasons       = EXCLUDED.top_3_reasons,
                created_at          = EXCLUDED.created_at
            WHERE EXCLUDED.created_at >= {BULK_CHANGES_FQN}.created_at;
        """), rec)
    return rec

def _upsert_bulk_draft(eng, rec: dict):
    with eng.begin() as c:
        c.execute(_sqltext(f"""
            INSERT INTO {BULK_DRAFTS_FQN} (
                policy_no, policy_no_norm, to_email, subject, body_text, body_html,
                segment, batch_id, status, created_at
            )
            VALUES (
                :policy_no, :policy_no_norm, :to_email, :subject, :body_text, :body_html,
                :segment, :batch_id, 'drafted', NOW()
            )
            ON CONFLICT (policy_no_norm) DO UPDATE
            SET
                to_email         = EXCLUDED.to_email,
                subject          = EXCLUDED.subject,
                body_text        = EXCLUDED.body_text,
                body_html        = EXCLUDED.body_html,
                segment          = EXCLUDED.segment,
                batch_id         = EXCLUDED.batch_id,
                status           = 'drafted',
                error_text       = NULL,
                gmail_message_id = NULL,
                created_at       = EXCLUDED.created_at
            WHERE EXCLUDED.created_at >= {BULK_DRAFTS_FQN}.created_at;
        """), rec)

def _mark_sent(eng, policy_no_norm: str, msg_id: str):
    with eng.begin() as c:
        c.execute(_sqltext(f"""
            UPDATE {BULK_DRAFTS_FQN}
               SET status='sent', gmail_message_id=:m, sent_at=NOW()
             WHERE policy_no_norm=:p;
        """), {"m": msg_id, "p": policy_no_norm})

def _mark_failed(eng, policy_no_norm: str, err: str):
    with eng.begin() as c:
        c.execute(_sqltext(f"""
            UPDATE {BULK_DRAFTS_FQN}
               SET status='failed', error_text=:e
             WHERE policy_no_norm=:p;
        """), {"e": err[:500], "p": policy_no_norm})

# --------------------------
# Data loading
# --------------------------
@st.cache_data
def load_bulk_source():
    eng = _cloud_engine()
    return pd.read_sql(f"SELECT * FROM {BULK_SOURCE_FQN};", eng)

# --------------------------
# Review queries/UI
# --------------------------
def _fetch_review_df(eng, segment: str, _batch_id_ignored: str, status_filter: list[str] | None, policy_search: str | None):
    where = ["d.segment = :s"]
    params = {"s": segment}
    if status_filter and len(status_filter) < 3:
        where.append("d.status = ANY(:st)"); params["st"] = status_filter
    if policy_search and policy_search.strip():
        where.append("(d.policy_no ILIKE :q OR d.policy_no_norm ILIKE :q)")
        params["q"] = f"%{policy_search.strip()}%"
    where_sql = " AND ".join(where)
    sql = f"""
    SELECT
    d.policy_no, d.to_email, d.status, d.subject, d.body_text,
    d.created_at, d.sent_at, d.gmail_message_id,
    c.old_discount, c.new_discount, c.old_od, c.new_od, c.old_tp, c.new_tp,
    c.old_total_premium, c.new_total_premium
    FROM {BULK_DRAFTS_FQN} d
    LEFT JOIN {BULK_CHANGES_FQN} c
    ON c.policy_no_norm = d.policy_no_norm
    WHERE {where_sql}
    ORDER BY d.created_at DESC;
    """
    df = pd.read_sql(_sqltext(sql), eng, params=params)
    if df.empty: return df

    # --- Convert timestamptz columns for display ---
    try:
        fmt = "%Y-%m-%d %H:%M:%S %Z"
        if "created_at" in df.columns:
            df["created_at"] = _to_local_tz_formatted(df["created_at"], LOCAL_TZ, fmt)
        if "sent_at" in df.columns:
            df["sent_at"] = _to_local_tz_formatted(df["sent_at"], LOCAL_TZ, fmt)
    except Exception:
        pass

    df["Δ Discount (pp)"] = (df["new_discount"] - df["old_discount"]).round(2)
    df["Δ OD (₹)"]        = (df["new_od"] - df["old_od"]).round(0)
    df["Δ TP (₹)"]        = (df["new_tp"] - df["old_tp"]).round(0)
    df["Δ Total (₹)"]     = (df["new_total_premium"] - df["old_total_premium"]).round(0)
    cols = [
    "policy_no", "to_email", "status", "subject", "body_text", "created_at", "sent_at",
    "Δ Discount (pp)", "Δ OD (₹)", "Δ TP (₹)", "Δ Total (₹)",
    "old_total_premium", "new_total_premium", "gmail_message_id"
    ]
    return df[cols]

def review_ui(segment: str, batch_id: str):
    st.markdown("### 📬 Review Drafted Emails")
    eng = _cloud_engine()
    _ensure_bulk_tables(eng)

    c1, c2, c3 = st.columns([1,1,2])
    with c1:
        status_filter = st.multiselect("Status", ["drafted","sent","failed"], default=["drafted","sent","failed"])
    with c2:
        policy_search = st.text_input("Search Policy No")
    with c3:
        st.write("")
        if st.button("Refresh"):
            st.rerun()

    df = _fetch_review_df(eng, segment, batch_id, status_filter, policy_search)
    if df.empty:
        st.info("No drafts found for current filters."); return

    st.dataframe(df, use_container_width=True)
    csv = df.to_csv(index=False).encode("utf-8-sig")
    st.download_button("Download CSV", csv, file_name=f"bulk_drafts_{segment}_{batch_id}.csv", mime="text/csv")

def send_all_drafts_ui(segment: str, batch_id: str):
    st.markdown("### ✉️ Send Emails")
    eng = _cloud_engine()
    df = pd.read_sql(_sqltext(f"""
        SELECT policy_no, policy_no_norm, to_email, subject, body_text
        FROM {BULK_DRAFTS_FQN}
        WHERE segment=:s AND status='drafted';
    """), eng, params={"s": segment})

    if df.empty:
        st.info("No drafted emails to send."); return

    if st.button(f"Send All ({len(df)})"):
        sent, failed = 0, 0
        prog = st.progress(0.0)
        for i, r in df.iterrows():
            try:
                res = _send_gmail(r["to_email"], r["subject"], r["body_text"])
                if res.startswith("Sent"):
                    msg_id = res.split(":")[-1].strip()
                    _mark_sent(eng, r["policy_no_norm"], msg_id); sent += 1
                else:
                    _mark_failed(eng, r["policy_no_norm"], res); failed += 1
            except Exception as e:
                _mark_failed(eng, r["policy_no_norm"], str(e)); failed += 1
            prog.progress((i+1)/len(df))
        st.success(f"Done. Sent: {sent}, Failed: {failed}")

# --------------------------
# BULK PIPELINE
# --------------------------
@st.cache_resource
def _load_model_assets():
    model = joblib.load(MODEL_FILE)
    label_encoders = joblib.load(LE_FILE)
    features = joblib.load(FEATS_FILE)
    return model, label_encoders, features

def _auto_suggest_for_row(base_row: pd.Series, model, label_encoders, features, param_ranges):
    raw = str(base_row.get("Top 3 Reasons",""))
    reasons = [r.strip() for r in re.split(r",|\band\b", raw) if r.strip()]
    selected_params = get_adjustable_parameters(reasons) or None

    trials, best_idx, _ = run_directional_moves_until_target(
        base_row, features, label_encoders, model,
        param_ranges, reasons, selected_params, target=50.0, max_k=50
    )
    if not trials:
        return None
    if best_idx is None:
        best_idx = len(trials) - 1
    return trials[best_idx]["row"]

def process_segment(segment_value: str, batch_id: str):
    st.write(f"**Batch ID:** `{batch_id}`")
    eng = _cloud_engine()
    _ensure_bulk_tables(eng)

    # Flow cards
    st.markdown("### 🔄 Processing Flow")
    c1, c2, c3 = st.columns(3)
    step1 = c1.container(border=True); step1.markdown("**1) Auto-Suggest**")
    step2 = c2.container(border=True); step2.markdown("**2) Save Selected Changes**")
    step3 = c3.container(border=True); step3.markdown("**3) Draft Emails**")

    # Load single source (has Mail ID + all model columns)
    src = load_bulk_source()
    param_ranges = get_parameter_ranges()
    model, label_encoders, features = _load_model_assets()

    # Filter by segment (expects exact labels in data)
    subset = src[src["Customer Segment"].astype(str).str.strip() == segment_value].copy()
    if subset.empty:
        st.warning("No rows for selected segment."); return

    # Step 1: Auto-suggest
    n = len(subset)
    p1 = step1.progress(0.0, text="Starting …")
    auto_rows = []
    for i, (_, row) in enumerate(subset.iterrows(), start=1):
        try:
            final_row = _auto_suggest_for_row(row, model, label_encoders, features, param_ranges)
            auto_rows.append((row, final_row))
        except Exception:
            auto_rows.append((row, None))
        p1.progress(i/n, text=f"Auto-suggested {i} / {n}")
    step1.success(f"Auto-suggest completed for {len(auto_rows)} rows")

    # Step 2: Save selected changes
    saved_context_rows = []
    p2 = step2.progress(0.0, text="Saving …")
    for j, (base_row, final_row) in enumerate(auto_rows, start=1):
        try:
            if final_row is None:
                final_row = base_row.to_dict()
                recompute_totals_from_od_tp(final_row, 0.18)
            ctx = _save_bulk_selected_change(eng, base_row.to_dict(), dict(final_row), segment_value, batch_id)
            saved_context_rows.append(ctx)
        except Exception:
            pass
        p2.progress(j/len(auto_rows), text=f"Saved {j} / {len(auto_rows)}")
    step2.success(f"Saved selected changes: {len(saved_context_rows)}")

    # Step 3: Draft emails (store full body_text + body_html)
    p3 = step3.progress(0.0, text="Drafting …")
    drafted = 0
    for k, ctx in enumerate(saved_context_rows, start=1):
        try:
            policy = ctx["policy_no"]
            # Mail ID pulled directly from subset (same single table)
            to_email_series = subset.loc[subset["policy no"].astype(str)==str(policy), "Mail ID"]
            to_email = (to_email_series.iloc[0] if not to_email_series.empty else DEFAULT_TO_EMAIL) or DEFAULT_TO_EMAIL

            # 👉 fetch the latest selected_changes row for this policy via LLM SQL (your style)
            latest_row = _fetch_latest_selected(policy)
            row_for_draft = latest_row if latest_row is not None else dict(ctx)

            draft = _draft_email(row_for_draft)
            subj, body_text = _split_subj_body(draft)
            body_html = _to_html(body_text)
            _upsert_bulk_draft(eng, {
                "policy_no": policy,
                "policy_no_norm": ctx["policy_no_norm"],
                "to_email": to_email,
                "subject": subj,
                "body_text": body_text,
                "body_html": body_html,
                "segment": segment_value,
                "batch_id": batch_id,
            })
            drafted += 1
        except Exception as e:
            _upsert_bulk_draft(eng, {
                "policy_no": ctx.get("policy_no",""),
                "policy_no_norm": ctx.get("policy_no_norm",""),
                "to_email": DEFAULT_TO_EMAIL,
                "subject": f"[DRAFT ERROR] {ctx.get('policy_no','')}",
                "body_text": str(e),
                "body_html": _to_html(str(e)),
                "segment": segment_value,
                "batch_id": batch_id,
            })
        p3.progress(k/len(saved_context_rows), text=f"Drafted {k} / {len(saved_context_rows)}")
    step3.success(f"Drafted emails: {drafted}/{len(saved_context_rows)}")

    st.success("Pipeline finished ✔️")

# --------------------------
# Streamlit Page
# --------------------------
def main():
    st.set_page_config(page_title="Bulk Email Agent", layout="wide")
    st.title("Bulk Email Agent")

    # Segment filter — must match values present in sample_bulk_email_data
    src = load_bulk_source()
    all_segments = sorted(set(src["Customer Segment"].astype(str).str.strip()))
    segment = st.selectbox("Customer Segment", all_segments)

    # Batch id
    if "batch_id" not in st.session_state:
        st.session_state["batch_id"] = f"batch_{time.strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"
    c1, c2 = st.columns([1,3])
    with c1:
        st.text_input("Batch ID", value=st.session_state["batch_id"], disabled=True)
    with c2:
        st.caption("Each run groups results by a batch id for easy review & sending.")

    if st.button("Process Segment"):
        process_segment(segment, st.session_state["batch_id"])

    st.markdown("---")
    review_ui(segment, st.session_state["batch_id"])

    st.markdown("---")
    send_all_drafts_ui(segment, st.session_state["batch_id"])

if __name__ == "__main__":
    main()
