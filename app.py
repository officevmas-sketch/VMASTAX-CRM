import sqlite3
import hashlib
import os
from datetime import date, datetime, timedelta
from pathlib import Path
from urllib.parse import quote_plus

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

APP_TITLE = "VMAS Tax Solutions CRM"
DB_PATH = Path("vmas_crm.db")
ASSET_DIR = Path(__file__).parent / "assets"
LOGO_PATH = ASSET_DIR / "vmas_logo.png"

STATUS_OPTIONS = ["New Lead", "Contacted", "Documents Pending", "In Progress", "Filed/Completed", "Payment Pending", "Closed", "Lost"]
PRIORITY_OPTIONS = ["High", "Medium", "Low"]
SERVICE_OPTIONS = ["ITR Filing", "GST Return", "GST Registration", "Accounting", "ROC Filing", "Tax Consultation", "Other"]
DEFAULT_COLUMNS = [
    "client_name", "mobile", "email", "pan_number", "gstin", "tan", "msme_number", "other_registration_number",
    "service", "financial_year", "lead_source", "assigned_to", "managed_by", "status", "priority",
    "fee_amount", "amount_received", "balance_amount", "next_followup_date", "remarks"
]

st.set_page_config(page_title=APP_TITLE, page_icon=str(LOGO_PATH) if LOGO_PATH.exists() else "💼", layout="wide")

PREMIUM_CSS = """
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap');
:root { --gold:#B8860B; --gold2:#F4C542; --navy:#17324D; --sky:#EAF3FF; --bg:#F4F6FA; --panel:#FFFFFF; --text:#1F2937; --muted:#64748B; --green:#15803D; --red:#DC2626; --orange:#D97706; }
html, body, [class*="css"] { font-family:'Inter', sans-serif; color:var(--text); }
.stApp { background: var(--bg); }
.block-container { padding-top: 1rem; max-width: 1560px; }
[data-testid="stSidebar"] { background: linear-gradient(180deg, #FFFFFF 0%, #FFF8E6 48%, #F3F7FB 100%); border-right: 1px solid #E5E7EB; box-shadow: 8px 0 28px rgba(15,23,42,.08); }
[data-testid="stSidebar"] * { color:#17324D !important; }
[data-testid="stSidebar"] .stImage { background:#FFFFFF; border-radius:18px; padding:10px; box-shadow:0 8px 22px rgba(15,23,42,.08); border:1px solid #F1E3B8; margin-bottom:10px; }
[data-testid="stSidebar"] .stRadio [role="radiogroup"] label { background:#FFFFFF; border:1px solid #E5E7EB; border-radius:12px; padding:7px 10px; margin:5px 0; box-shadow:0 3px 12px rgba(15,23,42,.04); }
[data-testid="stSidebar"] .stRadio [role="radiogroup"] label:hover { border-color:#B8860B; background:#FFF7DC; }
[data-testid="stSidebar"] input, [data-testid="stSidebar"] textarea { color:#111827 !important; background:#FFFFFF !important; }
[data-testid="stSidebar"] .stButton>button { background:linear-gradient(135deg,#B8860B,#F4C542) !important; color:#17324D !important; }
.powerbi-header { display:flex; justify-content:space-between; align-items:center; gap:18px; padding:18px 22px; border-radius:18px; background:#FFFFFF; border:1px solid #E5E7EB; box-shadow:0 6px 22px rgba(15,23,42,.06); margin-bottom:16px; }
.powerbi-title { font-size:1.55rem; font-weight:800; color:#17324D; margin:0; }
.powerbi-subtitle { color:#64748B; font-size:.9rem; margin-top:4px; }
.powerbi-badge { padding:8px 12px; border-radius:999px; background:#FFF7DC; color:#7A5200; border:1px solid rgba(184,134,11,.28); font-weight:700; font-size:.8rem; white-space:nowrap; }
.filter-strip { display:flex; flex-wrap:wrap; gap:8px; margin: -4px 0 14px 0; }
.filter-pill { padding:7px 11px; border-radius:999px; background:#FFFFFF; border:1px solid #E5E7EB; color:#475569; box-shadow:0 4px 14px rgba(15,23,42,.04); font-size:.78rem; }
.kpi-grid { display:grid; grid-template-columns: repeat(6, minmax(150px, 1fr)); gap:14px; margin: 10px 0 18px 0; align-items:stretch; }
.kpi-card { background:#FFFFFF; border:1px solid #E5E7EB; border-top:5px solid var(--gold); border-radius:18px; box-shadow:0 8px 20px rgba(15,23,42,.06); padding:16px 16px; min-height:128px; aspect-ratio: 1.18 / 1; transition:.22s ease; overflow:hidden; display:flex; flex-direction:column; justify-content:space-between; }
.kpi-card:hover { transform:translateY(-3px); box-shadow:0 12px 28px rgba(15,23,42,.10); }
.kpi-label { color:#64748B; font-size:.72rem; font-weight:800; text-transform:uppercase; letter-spacing:.08em; line-height:1.25; min-height:2.1em; word-break:normal; }
.kpi-value { font-size:clamp(1.25rem,1.7vw,1.8rem); font-weight:800; color:#17324D; margin-top:10px; line-height:1.08; overflow-wrap:anywhere; }
.kpi-note { color:#64748B; font-size:.78rem; line-height:1.35; margin-top:7px; }
.dashboard-grid-2 { display:grid; grid-template-columns: 1.25fr .75fr; gap:14px; margin: 12px 0; }
.dashboard-grid-3 { display:grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap:14px; margin:12px 0; }
.tile { background:#FFFFFF; border:1px solid #E5E7EB; border-radius:16px; box-shadow:0 8px 22px rgba(15,23,42,.06); padding:14px; min-height: 100px; }
.tile-title { font-size:.86rem; text-transform:uppercase; letter-spacing:.08em; color:#475569; font-weight:800; margin:0 0 8px 0; }
.insight-box { padding:12px 14px; border-radius:14px; background:linear-gradient(135deg,#EAF3FF,#FFFFFF); border:1px solid #D6E8FF; margin:8px 0; color:#17324D; font-size:.88rem; }
.kanban { padding: 14px; border-radius: 16px; background: #FFFFFF; border:1px solid #E5E7EB; min-height:145px; box-shadow: 0 8px 22px rgba(15,23,42,.06); }
.kanban h4 { color:#17324D; margin:0 0 8px 0; }
.chip { display:inline-block; padding:6px 10px; border-radius:999px; background:#F8FAFC; border:1px solid #CBD5E1; color:#334155; margin:3px; font-size:.78rem; }
.stButton>button, .stDownloadButton>button { border-radius:12px !important; background:linear-gradient(135deg,#17324D,#285679) !important; color:#FFFFFF !important; border:0 !important; font-weight:800 !important; box-shadow:0 8px 20px rgba(23,50,77,.16); }
.followup-table { width:100%; border-collapse:separate; border-spacing:0 10px; }
.followup-table th { text-align:left; font-size:.74rem; text-transform:uppercase; letter-spacing:.08em; color:#64748B; padding:10px 12px; background:#F8FAFC; }
.followup-table td { background:#FFFFFF; border-top:1px solid #E5E7EB; border-bottom:1px solid #E5E7EB; padding:12px; color:#17324D; vertical-align:middle; }
.followup-table td:first-child, .followup-table th:first-child { border-left:1px solid #E5E7EB; border-radius:14px 0 0 14px; }
.followup-table td:last-child, .followup-table th:last-child { border-right:1px solid #E5E7EB; border-radius:0 14px 14px 0; }
.whatsapp-btn { display:inline-block; padding:9px 13px; border-radius:12px; background:linear-gradient(135deg,#16A34A,#22C55E); color:#FFFFFF !important; font-weight:800; text-decoration:none !important; box-shadow:0 8px 18px rgba(22,163,74,.18); white-space:nowrap; }
.whatsapp-btn:hover { transform:translateY(-1px); filter:brightness(1.03); }
.stTabs [data-baseweb="tab-list"] { gap: 8px; border-bottom:1px solid #E5E7EB; }
.stTabs [data-baseweb="tab"] { background:#FFFFFF; border:1px solid #E5E7EB; border-bottom:0; border-radius:12px 12px 0 0; padding:10px 16px; color:#17324D; font-weight:700; }
.stTabs [aria-selected="true"] { background:#FFF7DC !important; color:#7A5200 !important; border-color:#F4C542 !important; }
[data-testid="stDataFrame"] { background:#FFFFFF; border-radius:14px; }
@media (max-width: 1100px) { .kpi-grid { grid-template-columns: repeat(3, minmax(150px, 1fr)); } .dashboard-grid-2, .dashboard-grid-3 { grid-template-columns: 1fr; } .powerbi-header { flex-direction:column; align-items:flex-start; } }
@media (max-width: 640px) { .kpi-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); } .kpi-card { aspect-ratio:auto; min-height:120px; } }
</style>
"""
st.markdown(PREMIUM_CSS, unsafe_allow_html=True)


def get_conn():
    return sqlite3.connect(DB_PATH, check_same_thread=False)


def init_db():
    conn = get_conn()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS clients (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            client_name TEXT NOT NULL, mobile TEXT, email TEXT,
            pan_number TEXT, gstin TEXT, tan TEXT, msme_number TEXT, other_registration_number TEXT,
            service TEXT, financial_year TEXT,
            lead_source TEXT, assigned_to TEXT, managed_by TEXT, status TEXT DEFAULT 'New Lead', priority TEXT DEFAULT 'Medium',
            fee_amount REAL DEFAULT 0, amount_received REAL DEFAULT 0, balance_amount REAL DEFAULT 0,
            next_followup_date TEXT, remarks TEXT, created_at TEXT, updated_at TEXT
        )
    """)
    # Auto-migrate older local databases created by previous app versions
    existing_cols = [row[1] for row in conn.execute("PRAGMA table_info(clients)").fetchall()]
    for col in ["pan_number", "gstin", "tan", "msme_number", "other_registration_number", "managed_by"]:
        if col not in existing_cols:
            conn.execute(f"ALTER TABLE clients ADD COLUMN {col} TEXT")
    conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_client ON clients(client_name, mobile, email, service, financial_year)")
    conn.commit()
    return conn


def clean_text(v):
    if pd.isna(v): return ""
    v = str(v).strip()
    return "" if v.lower() in ["nan", "none", "nat"] else v


def load_data(conn):
    df = pd.read_sql_query("SELECT * FROM clients ORDER BY updated_at DESC, id DESC", conn)
    if df.empty: return df
    for col in ["fee_amount", "amount_received", "balance_amount"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    df["next_followup_dt"] = pd.to_datetime(df["next_followup_date"], errors="coerce")
    return df


def normalize_import(df):
    df = df.copy()
    df.columns = [str(c).strip().lower().replace(" ", "_").replace("/", "_") for c in df.columns]
    mapping = {
        "client_name": ["client_name", "client", "name", "customer", "party", "assessee"],
        "mobile": ["mobile", "phone", "contact", "contact_no", "mobile_no"],
        "email": ["email", "mail", "email_id"],
        "pan_number": ["pan_number", "pan", "pan_no", "pan_number"],
        "gstin": ["gstin", "gst_number", "gst_no", "gst", "gst_registration_number"],
        "tan": ["tan", "tan_number", "tan_no"],
        "msme_number": ["msme_number", "msme", "udyam", "udyam_registration", "udyam_number"],
        "other_registration_number": ["other_registration_number", "other_registration", "registration_number", "reg_no", "cin", "shop_act", "iec"],
        "service": ["service", "work", "type", "return", "nature"],
        "financial_year": ["fy", "financial_year", "year", "assessment_year"],
        "assigned_to": ["assigned_to", "assigned", "owner", "allocated_to"],
        "managed_by": ["managed_by", "manager", "relationship_manager", "client_manager", "handled_by"],
        "status": ["status", "stage", "filing_status", "work_status"],
        "fee_amount": ["fee", "fees", "amount", "professional_fees", "billing"],
        "amount_received": ["received", "amount_received", "paid", "collection"],
        "balance_amount": ["balance", "pending", "outstanding"],
        "next_followup_date": ["followup", "follow_up", "next_followup"],
        "remarks": ["remarks", "remark", "notes", "comments"],
    }
    out = pd.DataFrame()
    for target in DEFAULT_COLUMNS:
        out[target] = ""
        for cand in mapping.get(target, [target]):
            hits = [c for c in df.columns if c == cand or cand in c]
            if hits:
                out[target] = df[hits[0]]
                break
    if out["client_name"].astype(str).str.strip().eq("").all():
        obj_cols = df.select_dtypes(include="object").columns
        if len(obj_cols): out["client_name"] = df[obj_cols[0]]
    out["client_name"] = out["client_name"].apply(clean_text)
    out = out[out["client_name"].ne("")]
    out["status"] = out["status"].apply(clean_text).replace("", "New Lead")
    out["priority"] = out["priority"].apply(clean_text).replace("", "Medium")
    out["service"] = out["service"].apply(clean_text).replace("", "ITR Filing")
    out["financial_year"] = out["financial_year"].apply(clean_text).replace("", "FY 2025-26")
    for col in ["fee_amount", "amount_received", "balance_amount"]:
        out[col] = pd.to_numeric(out[col], errors="coerce").fillna(0)
    out["balance_amount"] = out.apply(lambda r: r["fee_amount"] - r["amount_received"] if r["balance_amount"] == 0 else r["balance_amount"], axis=1)
    return out


def insert_rows(conn, df):
    now = datetime.now().isoformat(timespec="seconds")
    records = []
    for _, r in df.iterrows():
        name = clean_text(r.get("client_name", ""))
        if not name: continue
        fee = float(r.get("fee_amount", 0) or 0); rec = float(r.get("amount_received", 0) or 0)
        bal = float(r.get("balance_amount", fee-rec) or 0)
        if bal == 0 and fee > 0: bal = fee - rec
        records.append((name, clean_text(r.get("mobile", "")), clean_text(r.get("email", "")),
                        clean_text(r.get("pan_number", "")), clean_text(r.get("gstin", "")), clean_text(r.get("tan", "")),
                        clean_text(r.get("msme_number", "")), clean_text(r.get("other_registration_number", "")),
                        clean_text(r.get("service", "")) or "ITR Filing",
                        clean_text(r.get("financial_year", "")) or "FY 2025-26", clean_text(r.get("lead_source", "")), clean_text(r.get("assigned_to", "")), clean_text(r.get("managed_by", "")),
                        clean_text(r.get("status", "")) or "New Lead", clean_text(r.get("priority", "")) or "Medium", fee, rec, bal,
                        clean_text(r.get("next_followup_date", "")), clean_text(r.get("remarks", "")), now, now))
    if not records: return 0
    before = conn.total_changes
    conn.executemany("""
        INSERT OR IGNORE INTO clients
        (client_name,mobile,email,pan_number,gstin,tan,msme_number,other_registration_number,service,financial_year,lead_source,assigned_to,managed_by,status,priority,fee_amount,amount_received,balance_amount,next_followup_date,remarks,created_at,updated_at)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, records)
    conn.commit()
    return conn.total_changes - before



def get_duplicate_report(conn, df):
    """Return rows that would duplicate existing CRM records or duplicate within uploaded file."""
    if df is None or df.empty:
        return pd.DataFrame(), df

    work = df.copy().reset_index(drop=True)
    work["_row_no"] = work.index + 2  # Excel row number assuming header is row 1
    for c in ["client_name", "mobile", "email", "pan_number", "gstin", "tan", "msme_number", "service", "financial_year"]:
        if c not in work.columns:
            work[c] = ""
        work[c] = work[c].apply(clean_text)

    key_cols = ["client_name", "mobile", "email", "service", "financial_year"]
    work["_key"] = work[key_cols].fillna("").agg("|".join, axis=1).str.upper()

    duplicate_rows = []

    # Duplicates inside the uploaded Excel itself
    internal_dups = work[work.duplicated("_key", keep=False) & work["client_name"].ne("")].copy()
    for _, r in internal_dups.iterrows():
        duplicate_rows.append({
            "Excel Row": int(r["_row_no"]),
            "Client Name": r.get("client_name", ""),
            "Mobile": r.get("mobile", ""),
            "Email": r.get("email", ""),
            "PAN": r.get("pan_number", ""),
            "GSTIN": r.get("gstin", ""),
            "TAN": r.get("tan", ""),
            "MSME": r.get("msme_number", ""),
            "Service": r.get("service", ""),
            "Financial Year": r.get("financial_year", ""),
            "Duplicate Reason": "Duplicate row within uploaded file",
        })

    # Existing database duplicates
    existing = load_data(conn)
    existing_keys = set()
    identifier_map = {}
    if not existing.empty:
        for c in ["client_name", "mobile", "email", "pan_number", "gstin", "tan", "msme_number", "service", "financial_year"]:
            if c not in existing.columns:
                existing[c] = ""
            existing[c] = existing[c].apply(clean_text)
        existing["_key"] = existing[key_cols].fillna("").agg("|".join, axis=1).str.upper()
        existing_keys = set(existing["_key"].tolist())
        for _, ex in existing.iterrows():
            for col, label in [("pan_number", "PAN"), ("gstin", "GSTIN"), ("tan", "TAN"), ("msme_number", "MSME"), ("email", "Email"), ("mobile", "Mobile")]:
                val = clean_text(ex.get(col, "")).upper()
                if val:
                    identifier_map.setdefault((col, val), []).append(clean_text(ex.get("client_name", "")))

    duplicate_indices = set(internal_dups.index.tolist())
    for idx, r in work.iterrows():
        reasons = []
        if r["_key"] in existing_keys:
            reasons.append("Same client/mobile/email/service/FY already exists")
        for col, label in [("pan_number", "PAN"), ("gstin", "GSTIN"), ("tan", "TAN"), ("msme_number", "MSME"), ("email", "Email"), ("mobile", "Mobile")]:
            val = clean_text(r.get(col, "")).upper()
            if val and (col, val) in identifier_map:
                matched_names = ", ".join(sorted(set(identifier_map[(col, val)]))[:3])
                reasons.append(f"{label} already exists for {matched_names}")
        if reasons:
            duplicate_indices.add(idx)
            duplicate_rows.append({
                "Excel Row": int(r["_row_no"]),
                "Client Name": r.get("client_name", ""),
                "Mobile": r.get("mobile", ""),
                "Email": r.get("email", ""),
                "PAN": r.get("pan_number", ""),
                "GSTIN": r.get("gstin", ""),
                "TAN": r.get("tan", ""),
                "MSME": r.get("msme_number", ""),
                "Service": r.get("service", ""),
                "Financial Year": r.get("financial_year", ""),
                "Duplicate Reason": "; ".join(reasons),
            })

    dup_df = pd.DataFrame(duplicate_rows).drop_duplicates() if duplicate_rows else pd.DataFrame()
    clean_to_insert = work.drop(index=list(duplicate_indices), errors="ignore").drop(columns=["_row_no", "_key"], errors="ignore")
    return dup_df, clean_to_insert


def import_rows_with_duplicate_check(conn, df):
    duplicate_df, rows_to_insert = get_duplicate_report(conn, df)
    inserted = insert_rows(conn, rows_to_insert)
    return inserted, duplicate_df, len(rows_to_insert)

def update_record(conn, row_id, data):
    data["balance_amount"] = float(data.get("fee_amount", 0)) - float(data.get("amount_received", 0))
    data["updated_at"] = datetime.now().isoformat(timespec="seconds")
    conn.execute(f"UPDATE clients SET {', '.join([k+'=?' for k in data])} WHERE id=?", list(data.values()) + [row_id])
    conn.commit()


def delete_record(conn, row_id):
    conn.execute("DELETE FROM clients WHERE id=?", (row_id,)); conn.commit()


def amount_scale():
    unit = globals().get("amount_unit", "Absolute")
    scales = {"Absolute": (1, ""), "Thousands": (1_000, "K"), "Lakhs": (100_000, "L"), "Crores": (10_000_000, "Cr")}
    return scales.get(unit, (1, ""))

def money(v):
    scale, suffix = amount_scale()
    val = float(v or 0) / scale
    if suffix:
        return f"₹{val:,.2f} {suffix}"
    return f"₹{float(v or 0):,.0f}"

def scaled_amount(v):
    scale, _ = amount_scale()
    return float(v or 0) / scale

def amount_axis_title(label="Amount"):
    unit = globals().get("amount_unit", "Absolute")
    return f"{label} ({unit})"

def kpi_html(label, value, note=""):
    return f"<div class='kpi-card'><div class='kpi-label'>{label}</div><div class='kpi-value'>{value}</div><div class='kpi-note'>{note}</div></div>"


# -------------------------
# Login, Roles & Rights
# -------------------------
def hash_password(password: str, salt: bytes | None = None) -> str:
    """Create a salted password hash suitable for a small internal Streamlit CRM."""
    salt = salt or os.urandom(16)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, 120_000)
    return salt.hex() + ":" + digest.hex()


def verify_password(password: str, stored_hash: str) -> bool:
    try:
        salt_hex, digest_hex = stored_hash.split(":", 1)
        salt = bytes.fromhex(salt_hex)
        check = hash_password(password, salt).split(":", 1)[1]
        return check == digest_hex
    except Exception:
        return False


PERMISSION_LABELS = {
    "can_dashboard": "Executive Dashboard",
    "can_clients": "Client Workspace",
    "can_pipeline": "Lead Pipeline",
    "can_followups": "Follow-ups",
    "can_add_edit": "Add / Edit Client",
    "can_import_export": "Import / Export",
    "can_manage_users": "User Management",
}

PAGE_PERMISSION = {
    "Executive Dashboard": "can_dashboard",
    "Client Workspace": "can_clients",
    "Lead Pipeline": "can_pipeline",
    "Follow-ups": "can_followups",
    "Add / Edit Client": "can_add_edit",
    "Import / Export": "can_import_export",
    "User Management": "can_manage_users",
}


def init_auth_db(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT NOT NULL UNIQUE,
            full_name TEXT,
            password_hash TEXT NOT NULL,
            role TEXT DEFAULT 'User',
            is_active INTEGER DEFAULT 1,
            can_dashboard INTEGER DEFAULT 1,
            can_clients INTEGER DEFAULT 1,
            can_pipeline INTEGER DEFAULT 1,
            can_followups INTEGER DEFAULT 1,
            can_add_edit INTEGER DEFAULT 0,
            can_import_export INTEGER DEFAULT 0,
            can_manage_users INTEGER DEFAULT 0,
            created_at TEXT,
            updated_at TEXT
        )
    """)
    existing_cols = [row[1] for row in conn.execute("PRAGMA table_info(users)").fetchall()]
    for col in PERMISSION_LABELS:
        if col not in existing_cols:
            default = 1 if col in ["can_dashboard", "can_clients", "can_pipeline", "can_followups"] else 0
            conn.execute(f"ALTER TABLE users ADD COLUMN {col} INTEGER DEFAULT {default}")
    if "must_change_password" not in existing_cols:
        conn.execute("ALTER TABLE users ADD COLUMN must_change_password INTEGER DEFAULT 1")
    if conn.execute("SELECT COUNT(*) FROM users").fetchone()[0] == 0:
        now = datetime.now().isoformat(timespec="seconds")
        conn.execute("""
            INSERT INTO users
            (username, full_name, password_hash, role, is_active, can_dashboard, can_clients, can_pipeline, can_followups, can_add_edit, can_import_export, can_manage_users, must_change_password, created_at, updated_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, ("admin", "VMAS Admin", hash_password("admin123"), "Admin", 1, 1, 1, 1, 1, 1, 1, 1, 1, now, now))
    conn.commit()


def get_user_by_username(conn, username):
    row = conn.execute("SELECT * FROM users WHERE lower(username)=lower(?)", (username.strip(),)).fetchone()
    if not row:
        return None
    cols = [c[1] for c in conn.execute("PRAGMA table_info(users)").fetchall()]
    return dict(zip(cols, row))


def load_users(conn):
    return pd.read_sql_query("SELECT id, username, full_name, role, is_active, must_change_password, can_dashboard, can_clients, can_pipeline, can_followups, can_add_edit, can_import_export, can_manage_users, created_at, updated_at FROM users ORDER BY role, username", conn)


def current_user():
    return st.session_state.get("user")


def has_perm(page_or_perm):
    user = current_user()
    if not user:
        return False
    if user.get("role") == "Admin":
        return True
    perm = PAGE_PERMISSION.get(page_or_perm, page_or_perm)
    return bool(user.get(perm, 0))



def change_password(conn, user_id, new_password, must_change_password=0):
    now = datetime.now().isoformat(timespec="seconds")
    conn.execute(
        "UPDATE users SET password_hash=?, must_change_password=?, updated_at=? WHERE id=?",
        (hash_password(new_password), int(must_change_password), now, int(user_id))
    )
    conn.commit()


def force_password_change_screen(conn):
    user = current_user()
    st.markdown(PREMIUM_CSS, unsafe_allow_html=True)
    left, mid, right = st.columns([1, 1.15, 1])
    with mid:
        if LOGO_PATH.exists():
            st.image(str(LOGO_PATH), width=140)
        st.markdown("### Change Password")
        st.info("For security, please change your password before continuing.")
        with st.form("force_change_password_form"):
            current_password = st.text_input("Current Password", type="password")
            new_password = st.text_input("New Password", type="password")
            confirm_password = st.text_input("Confirm New Password", type="password")
            submitted = st.form_submit_button("Update Password")
        if submitted:
            db_user = get_user_by_username(conn, user.get("username", ""))
            if not db_user or not verify_password(current_password, db_user["password_hash"]):
                st.error("Current password is incorrect.")
            elif len(new_password) < 6:
                st.error("New password must be at least 6 characters.")
            elif new_password != confirm_password:
                st.error("New password and confirmation do not match.")
            elif verify_password(new_password, db_user["password_hash"]):
                st.error("New password cannot be the same as the current password.")
            else:
                change_password(conn, user["id"], new_password, must_change_password=0)
                refreshed = get_user_by_username(conn, user.get("username", ""))
                refreshed.pop("password_hash", None)
                st.session_state["user"] = refreshed
                st.success("Password changed successfully.")
                st.rerun()


def render_change_password_panel(conn):
    user = current_user()
    with st.expander("Change Password", expanded=False):
        with st.form("self_change_password_form"):
            current_password = st.text_input("Current Password", type="password", key="self_current_pwd")
            new_password = st.text_input("New Password", type="password", key="self_new_pwd")
            confirm_password = st.text_input("Confirm New Password", type="password", key="self_confirm_pwd")
            submitted = st.form_submit_button("Change Password")
        if submitted:
            db_user = get_user_by_username(conn, user.get("username", ""))
            if not db_user or not verify_password(current_password, db_user["password_hash"]):
                st.error("Current password is incorrect.")
            elif len(new_password) < 6:
                st.error("New password must be at least 6 characters.")
            elif new_password != confirm_password:
                st.error("New password and confirmation do not match.")
            elif verify_password(new_password, db_user["password_hash"]):
                st.error("New password cannot be the same as the current password.")
            else:
                change_password(conn, user["id"], new_password, must_change_password=0)
                refreshed = get_user_by_username(conn, user.get("username", ""))
                refreshed.pop("password_hash", None)
                st.session_state["user"] = refreshed
                st.success("Password changed successfully.")

def login_screen(conn):
    st.markdown(PREMIUM_CSS, unsafe_allow_html=True)
    left, mid, right = st.columns([1, 1.2, 1])
    with mid:
        if LOGO_PATH.exists():
            st.image(str(LOGO_PATH), width=140)
        st.markdown("### VMAS CRM Login")
        st.caption("Default first login: admin / admin123. Please change the password after deployment.")
        with st.form("login_form"):
            username = st.text_input("Username")
            password = st.text_input("Password", type="password")
            submitted = st.form_submit_button("Login")
        if submitted:
            user = get_user_by_username(conn, username)
            if not user or not user.get("is_active") or not verify_password(password, user["password_hash"]):
                st.error("Invalid username/password or inactive user.")
            else:
                # Do not store password hash in session.
                user.pop("password_hash", None)
                st.session_state["user"] = user
                st.success("Login successful.")
                st.rerun()


def create_user(conn, username, full_name, password, role, permissions):
    now = datetime.now().isoformat(timespec="seconds")
    vals = {k: 1 if k in permissions else 0 for k in PERMISSION_LABELS}
    if role == "Admin":
        vals = {k: 1 for k in PERMISSION_LABELS}
    conn.execute("""
        INSERT INTO users
        (username, full_name, password_hash, role, is_active, can_dashboard, can_clients, can_pipeline, can_followups, can_add_edit, can_import_export, can_manage_users, must_change_password, created_at, updated_at)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (username.strip(), full_name.strip(), hash_password(password), role, 1, vals["can_dashboard"], vals["can_clients"], vals["can_pipeline"], vals["can_followups"], vals["can_add_edit"], vals["can_import_export"], vals["can_manage_users"], 1, now, now))
    conn.commit()


def update_user(conn, user_id, full_name, role, is_active, permissions, new_password=""):
    now = datetime.now().isoformat(timespec="seconds")
    vals = {k: 1 if k in permissions else 0 for k in PERMISSION_LABELS}
    if role == "Admin":
        vals = {k: 1 for k in PERMISSION_LABELS}
    if new_password:
        conn.execute("""
            UPDATE users SET full_name=?, role=?, is_active=?, password_hash=?, must_change_password=1, can_dashboard=?, can_clients=?, can_pipeline=?, can_followups=?, can_add_edit=?, can_import_export=?, can_manage_users=?, updated_at=? WHERE id=?
        """, (full_name, role, int(is_active), hash_password(new_password), vals["can_dashboard"], vals["can_clients"], vals["can_pipeline"], vals["can_followups"], vals["can_add_edit"], vals["can_import_export"], vals["can_manage_users"], now, user_id))
    else:
        conn.execute("""
            UPDATE users SET full_name=?, role=?, is_active=?, can_dashboard=?, can_clients=?, can_pipeline=?, can_followups=?, can_add_edit=?, can_import_export=?, can_manage_users=?, updated_at=? WHERE id=?
        """, (full_name, role, int(is_active), vals["can_dashboard"], vals["can_clients"], vals["can_pipeline"], vals["can_followups"], vals["can_add_edit"], vals["can_import_export"], vals["can_manage_users"], now, user_id))
    conn.commit()




def render_followup_action_table(dataframe, max_rows=50, show_action=True):
    """Render follow-up rows with an in-row WhatsApp reminder button."""
    if dataframe is None or dataframe.empty:
        st.info("No follow-ups found for the selected filters.")
        return
    cols = ["client_name", "mobile", "service", "assigned_to", "managed_by", "status", "priority", "next_followup_date", "balance_amount"]
    view = dataframe.copy().head(max_rows)
    for col in cols:
        if col not in view.columns:
            view[col] = ""
    html = ["<table class='followup-table'>"]
    html.append("<thead><tr><th>Client</th><th>Mobile</th><th>Service</th><th>Assigned To</th><th>Managed By</th><th>Status</th><th>Priority</th><th>Follow-up Date</th><th>Outstanding</th><th>Action</th></tr></thead><tbody>")
    for _, r in view.iterrows():
        mobile_digits = ''.join(ch for ch in str(r.get('mobile', '')) if ch.isdigit())
        msg = quote_plus(f"Dear {r.get('client_name','')}, this is a reminder from VMAS Tax Solutions regarding your {r.get('service','')} work. Kindly share pending details/payment to proceed.")
        action = ""
        if mobile_digits:
            action = f"<a class='whatsapp-btn' target='_blank' href='https://wa.me/91{mobile_digits[-10:]}?text={msg}'>Send Reminder</a>"
        else:
            action = "<span style='color:#DC2626;font-weight:700;'>Mobile missing</span>"
        html.append(
            "<tr>"
            f"<td><b>{clean_text(r.get('client_name',''))}</b></td>"
            f"<td>{clean_text(r.get('mobile',''))}</td>"
            f"<td>{clean_text(r.get('service',''))}</td>"
            f"<td>{clean_text(r.get('assigned_to',''))}</td>"
            f"<td>{clean_text(r.get('managed_by',''))}</td>"
            f"<td>{clean_text(r.get('status',''))}</td>"
            f"<td>{clean_text(r.get('priority',''))}</td>"
            f"<td>{clean_text(r.get('next_followup_date',''))}</td>"
            f"<td><b>{money(r.get('balance_amount',0))}</b></td>"
            f"<td>{action}</td>"
            "</tr>"
        )
    html.append("</tbody></table>")
    st.markdown("".join(html), unsafe_allow_html=True)

conn = init_db()
init_auth_db(conn)
if "user" not in st.session_state:
    login_screen(conn)
    st.stop()

if int(st.session_state.get("user", {}).get("must_change_password", 0) or 0) == 1:
    force_password_change_screen(conn)
    st.stop()

df_all = load_data(conn)

with st.sidebar:
    if LOGO_PATH.exists(): st.image(str(LOGO_PATH), width=150)
    st.markdown("### VMAS Tax Solutions")
    st.caption("Professional Services Guaranteed")
    user = current_user()
    st.success(f"Logged in: {user.get('full_name') or user.get('username')} ({user.get('role')})")
    all_pages = ["Executive Dashboard", "Client Workspace", "Lead Pipeline", "Follow-ups", "Add / Edit Client", "Import / Export", "User Management"]
    allowed_pages = [p for p in all_pages if has_perm(p)]
    if not allowed_pages:
        st.error("No page rights assigned. Contact admin.")
        st.stop()
    page = st.radio("Navigation", allowed_pages, label_visibility="collapsed")
    render_change_password_panel(conn)
    if st.button("Logout"):
        st.session_state.pop("user", None)
        st.rerun()
    st.divider()
    st.markdown("#### Smart Filters")
    status_filter = st.multiselect("Status", STATUS_OPTIONS)
    service_filter = st.multiselect("Service", SERVICE_OPTIONS)
    priority_filter = st.multiselect("Priority", PRIORITY_OPTIONS)
    assigned_values = sorted([x for x in df_all.get("assigned_to", pd.Series(dtype=str)).dropna().astype(str).unique() if x.strip()]) if not df_all.empty else []
    managed_values = sorted([x for x in df_all.get("managed_by", pd.Series(dtype=str)).dropna().astype(str).unique() if x.strip()]) if not df_all.empty else []
    assigned_filter = st.multiselect("Assigned To", assigned_values)
    managed_filter = st.multiselect("Managed By", managed_values)
    search_text = st.text_input("Search", placeholder="Client / mobile / email")
    st.divider()
    st.markdown("#### Amount Display")
    amount_unit = st.radio(
        "View amounts as",
        ["Absolute", "Thousands", "Lakhs", "Crores"],
        horizontal=False,
        index=0,
        help="This changes KPI and chart amount labels only; stored CRM values remain absolute rupees."
    )


def apply_filters(df):
    if df.empty: return df
    out = df.copy()
    if status_filter: out = out[out["status"].isin(status_filter)]
    if service_filter: out = out[out["service"].isin(service_filter)]
    if priority_filter: out = out[out["priority"].isin(priority_filter)]
    if assigned_filter: out = out[out["assigned_to"].isin(assigned_filter)]
    if managed_filter and "managed_by" in out.columns: out = out[out["managed_by"].isin(managed_filter)]
    if search_text:
        mask = out[["client_name", "mobile", "email", "pan_number", "gstin", "tan", "msme_number", "other_registration_number", "service", "assigned_to", "managed_by"]].fillna("").astype(str).agg(" ".join, axis=1).str.contains(search_text, case=False, na=False)
        out = out[mask]
    return out

filtered = apply_filters(df_all)

st.markdown(f"""
<div style='margin-top:30px;'>
    <div class='powerbi-header'>
        <div>
            <div class='powerbi-title'>VMAS CRM Analytics Dashboard</div>
        </div>
        <div class='powerbi-badge'>
            Amount View: {amount_unit}
        </div>
    </div>
</div>
""", unsafe_allow_html=True)
active_filters = []
if status_filter: active_filters.append('Status: ' + ', '.join(status_filter))
if service_filter: active_filters.append('Service: ' + ', '.join(service_filter))
if priority_filter: active_filters.append('Priority: ' + ', '.join(priority_filter))
if assigned_filter: active_filters.append('Assigned: ' + ', '.join(assigned_filter))
if managed_filter: active_filters.append('Managed: ' + ', '.join(managed_filter))
if search_text: active_filters.append('Search: ' + search_text)
if active_filters:
    st.markdown("<div class='filter-strip'>" + ''.join([f"<span class='filter-pill'>{x}</span>" for x in active_filters]) + "</div>", unsafe_allow_html=True)
st.write("")

if filtered.empty:
    st.info("No CRM records yet. Add a client or import your Excel worklist from the Import / Export page.")

if page == "Executive Dashboard" and not filtered.empty:
    total = len(filtered)
    open_cases = int((~filtered["status"].isin(["Filed/Completed", "Closed", "Lost"])).sum())
    completed = int(filtered["status"].isin(["Filed/Completed", "Closed"]).sum())
    fees = filtered["fee_amount"].sum()
    received = filtered["amount_received"].sum()
    outstanding = filtered["balance_amount"].sum()
    collection_eff = received / fees * 100 if fees else 0
    avg_client_value = fees / total if total else 0
    today = pd.Timestamp(date.today())
    due_count = int((filtered["next_followup_dt"].notna() & (filtered["next_followup_dt"] <= today)).sum())

    kpi_items = [
        ("Clients", total, "Total filtered clients"),
        ("Open Work", open_cases, "Cases needing action"),
        ("Closure %", f"{completed/max(total,1):.0%}", f"{completed} completed"),
        ("Total Fees", money(fees), "Billed / expected"),
        ("Collection", money(received), f"{collection_eff:.1f}% efficiency"),
        ("Outstanding", money(outstanding), f"{due_count} follow-ups due"),
    ]
    kpi_cards = "".join(kpi_html(label, value, note) for label, value, note in kpi_items)
    st.markdown(f"<div class='kpi-grid'>{kpi_cards}</div>", unsafe_allow_html=True)

    dash_tab1, dash_tab2, dash_tab3, dash_tab4 = st.tabs(["Executive Summary", "Revenue & Recovery", "Compliance Workload", "Follow-up Control"])

    with dash_tab1:
        c1, c2 = st.columns([1.25, .75])
        with c1:
            st.markdown("<div class='tile'><div class='tile-title'>Work Status Funnel</div>", unsafe_allow_html=True)
            status_counts = filtered.groupby("status", dropna=False).size().reset_index(name="count")
            fig = px.bar(status_counts, x="count", y="status", orientation="h", text="count")
            fig.update_layout(template="plotly_white", height=360, margin=dict(t=10,l=5,r=5,b=5), xaxis_title="Clients", yaxis_title="")
            fig.update_traces(
                marker_color="#0F766E",
                marker_line_color="#14B8A6",
                marker_line_width=1.5,
                opacity=0.92,
                textposition="outside",
                textfont=dict(color="#17324D", size=13)
            )
            fig.update_xaxes(
                showgrid=True,
                gridcolor="#EEF2F7",
                zeroline=False,
                tickfont=dict(color="#64748B")
            )
            fig.update_yaxes(
                tickfont=dict(color="#64748B", size=13)
            )
            st.plotly_chart(fig, use_container_width=True)
            st.markdown("</div>", unsafe_allow_html=True)
        with c2:
            st.markdown("<div class='tile'><div class='tile-title'>Collection Efficiency</div>", unsafe_allow_html=True)
            fig = go.Figure(go.Indicator(
                mode="gauge+number+delta",
                value=collection_eff,
                number={"suffix":"%"},
                delta={"reference":80, "suffix":"% target"},
                gauge={"axis":{"range":[0,100]}, "bar":{"color":"#B8860B"}, "steps":[{"range":[0,50],"color":"#FEE2E2"},{"range":[50,80],"color":"#FEF3C7"},{"range":[80,100],"color":"#DCFCE7"}]}
            ))
            fig.update_layout(template="plotly_white", height=360, margin=dict(t=10,l=5,r=5,b=5))
            st.plotly_chart(fig, use_container_width=True)
            st.markdown("</div>", unsafe_allow_html=True)

        service = filtered.groupby("service").agg(clients=("id","count"), fees=("fee_amount","sum"), received=("amount_received","sum"), outstanding=("balance_amount","sum")).reset_index()
        service["fees_display"] = service["fees"].apply(scaled_amount)
        service["outstanding_display"] = service["outstanding"].apply(scaled_amount)
        c3, c4, c5 = st.columns(3)
        with c3:
            st.markdown("<div class='tile'><div class='tile-title'>Service Portfolio</div>", unsafe_allow_html=True)
            fig = px.treemap(service, path=["service"], values="fees_display", color="outstanding_display")
            fig.update_layout(template="plotly_white", height=320, margin=dict(t=5,l=5,r=5,b=5))
            st.plotly_chart(fig, use_container_width=True)
            st.markdown("</div>", unsafe_allow_html=True)
        with c4:
            st.markdown("<div class='tile'><div class='tile-title'>Priority Mix</div>", unsafe_allow_html=True)
            pr = filtered.groupby("priority").size().reset_index(name="count")
            fig = px.pie(pr, names="priority", values="count", hole=.58)
            fig.update_layout(template="plotly_white", height=320, margin=dict(t=5,l=5,r=5,b=5), showlegend=True)
            st.plotly_chart(fig, use_container_width=True)
            st.markdown("</div>", unsafe_allow_html=True)
        with c5:
            st.markdown("<div class='tile'><div class='tile-title'>Smart Insights</div>", unsafe_allow_html=True)
            top_service = service.sort_values("fees", ascending=False).head(1)
            top_service_name = top_service["service"].iloc[0] if not top_service.empty else "NA"
            st.markdown(f"<div class='insight-box'>Top revenue service: <b>{top_service_name}</b>.</div>", unsafe_allow_html=True)
            st.markdown(f"<div class='insight-box'>Average client value: <b>{money(avg_client_value)}</b>.</div>", unsafe_allow_html=True)
            st.markdown(f"<div class='insight-box'>Collection gap: <b>{money(outstanding)}</b> pending.</div>", unsafe_allow_html=True)
            if due_count:
                st.markdown(f"<div class='insight-box'>Action required: <b>{due_count}</b> follow-ups are due/overdue.</div>", unsafe_allow_html=True)
            st.markdown("</div>", unsafe_allow_html=True)

    with dash_tab2:
        temp = filtered.copy()
        temp["fee_display"] = temp["fee_amount"].apply(scaled_amount)
        temp["received_display"] = temp["amount_received"].apply(scaled_amount)
        temp["outstanding_display"] = temp["balance_amount"].apply(scaled_amount)
        a, b = st.columns([1.1, .9])
        with a:
            st.markdown("<div class='tile'><div class='tile-title'>Fees vs Collection by Service</div>", unsafe_allow_html=True)
            svc = temp.groupby("service")[["fee_display","received_display","outstanding_display"]].sum().reset_index()
            fig = go.Figure()
            fig.add_bar(x=svc["service"], y=svc["fee_display"], name="Fees")
            fig.add_bar(x=svc["service"], y=svc["received_display"], name="Received")
            fig.add_bar(x=svc["service"], y=svc["outstanding_display"], name="Outstanding")
            fig.update_layout(template="plotly_white", barmode="group", height=390, margin=dict(t=15,l=5,r=5,b=5), yaxis_title=amount_axis_title("Amount"))
            st.plotly_chart(fig, use_container_width=True)
            st.markdown("</div>", unsafe_allow_html=True)
        with b:
            st.markdown("<div class='tile'><div class='tile-title'>Top Outstanding Clients</div>", unsafe_allow_html=True)
            top_out = temp.sort_values("balance_amount", ascending=False).head(10).copy()
            fig = px.bar(top_out, x="outstanding_display", y="client_name", orientation="h", text="outstanding_display")
            fig.update_layout(template="plotly_white", height=390, margin=dict(t=15,l=5,r=5,b=5), xaxis_title=amount_axis_title("Outstanding"), yaxis_title="")
            fig.update_traces(marker_color="#DC2626")
            st.plotly_chart(fig, use_container_width=True)
            st.markdown("</div>", unsafe_allow_html=True)

    with dash_tab3:
        left, right = st.columns(2)
        with left:
            st.markdown("<div class='tile'><div class='tile-title'>Client Count by Service</div>", unsafe_allow_html=True)
            svc_count = filtered.groupby("service").size().reset_index(name="clients").sort_values("clients", ascending=False)
            fig = px.bar(svc_count, x="service", y="clients", text="clients")
            fig.update_layout(template="plotly_white", height=360, margin=dict(t=15,l=5,r=5,b=5), xaxis_title="", yaxis_title="Clients")
            fig.update_traces(marker_color="#17324D")
            st.plotly_chart(fig, use_container_width=True)
            st.markdown("</div>", unsafe_allow_html=True)
        with right:
            st.markdown("<div class='tile'><div class='tile-title'>Status by Service Matrix</div>", unsafe_allow_html=True)
            matrix = filtered.pivot_table(index="service", columns="status", values="id", aggfunc="count", fill_value=0)
            st.dataframe(matrix, use_container_width=True)
            st.markdown("</div>", unsafe_allow_html=True)

    with dash_tab4:
        temp = filtered.copy()
        today = pd.Timestamp(date.today())
        due = temp[temp["next_followup_dt"].notna() & (temp["next_followup_dt"] <= today)].sort_values("next_followup_dt")
        upcoming = temp[temp["next_followup_dt"].notna() & (temp["next_followup_dt"] > today)].sort_values("next_followup_dt")
        a,b = st.columns([.9,1.1])
        with a:
            st.markdown("<div class='tile'><div class='tile-title'>Follow-up Summary</div>", unsafe_allow_html=True)
            st.metric("Due / Overdue", len(due))
            st.metric("Upcoming", len(upcoming))
            st.metric("Outstanding Linked", money(due["balance_amount"].sum() if not due.empty else 0))
            st.markdown("</div>", unsafe_allow_html=True)
        with b:
            st.markdown("<div class='tile'><div class='tile-title'>Due Follow-ups Control Table</div>", unsafe_allow_html=True)
            st.dataframe(due[["client_name","mobile","service","assigned_to","managed_by","status","priority","next_followup_date","balance_amount","remarks"]].head(15), use_container_width=True, hide_index=True)
            st.markdown("</div>", unsafe_allow_html=True)

elif page == "Client Workspace":
    st.subheader("Client Workspace")
    if not filtered.empty:
        view_cols = ["client_name","mobile","email","pan_number","gstin","tan","msme_number","other_registration_number","service","financial_year","assigned_to","managed_by","status","priority","fee_amount","amount_received","balance_amount","next_followup_date","remarks"]
        st.dataframe(filtered[view_cols], use_container_width=True, hide_index=True)
        st.download_button("Download Filtered CRM Data", filtered.to_csv(index=False).encode(), "vmas_filtered_crm.csv", "text/csv")

elif page == "Lead Pipeline" and not filtered.empty:
    st.subheader("Lead Pipeline Board")
    cols = st.columns(4)
    pipeline = ["New Lead", "Contacted", "Documents Pending", "In Progress"]
    for i, stage in enumerate(pipeline):
        with cols[i]:
            subset = filtered[filtered["status"] == stage].head(8)
            st.markdown(f"<div class='kanban'><h4>{stage}</h4>", unsafe_allow_html=True)
            if subset.empty: st.caption("No records")
            for _, r in subset.iterrows():
                st.markdown(f"<span class='chip'>{r['client_name']} • {r['service']}</span>", unsafe_allow_html=True)
            st.markdown("</div>", unsafe_allow_html=True)
    st.write("")
    cols = st.columns(4)
    pipeline2 = ["Payment Pending", "Filed/Completed", "Closed", "Lost"]
    for i, stage in enumerate(pipeline2):
        with cols[i]:
            subset = filtered[filtered["status"] == stage].head(8)
            st.markdown(f"<div class='kanban'><h4>{stage}</h4>", unsafe_allow_html=True)
            if subset.empty: st.caption("No records")
            for _, r in subset.iterrows():
                st.markdown(f"<span class='chip'>{r['client_name']} • {money(r['balance_amount'])}</span>", unsafe_allow_html=True)
            st.markdown("</div>", unsafe_allow_html=True)

elif page == "Follow-ups" and not filtered.empty:
    st.subheader("Follow-up Center")
    temp = filtered.copy()
    today = pd.Timestamp(date.today())
    due = temp[temp["next_followup_dt"].notna() & (temp["next_followup_dt"] <= today)].sort_values("next_followup_dt")
    upcoming = temp[temp["next_followup_dt"].notna() & (temp["next_followup_dt"] > today)].sort_values("next_followup_dt")
    tab1, tab2 = st.tabs(["Due / Overdue", "Upcoming"])
    with tab1:
        st.caption("Each row now has its own Send Reminder button. The button opens WhatsApp with a ready message for that client.")
        render_followup_action_table(due, max_rows=100)
    with tab2:
        st.caption("Upcoming follow-ups with client-wise action button.")
        render_followup_action_table(upcoming, max_rows=100)

elif page == "Add / Edit Client":
    tab_add, tab_edit = st.tabs(["Add New Client", "Update Existing"])
    with tab_add:
        with st.form("add_client", clear_on_submit=True):
            c1,c2,c3 = st.columns(3)
            client_name = c1.text_input("Client Name *")
            mobile = c2.text_input("Mobile")
            email = c3.text_input("Email")
            st.markdown("#### Registration Details")
            r1, r2, r3 = st.columns(3)
            pan_number = r1.text_input("PAN Number")
            gstin = r2.text_input("GSTIN")
            tan = r3.text_input("TAN")
            msme_number = r1.text_input("MSME / Udyam Number")
            other_registration_number = r2.text_input("Other Registration Number")
            st.markdown("#### Service & Commercial Details")
            service = c1.selectbox("Service", SERVICE_OPTIONS)
            financial_year = c2.text_input("Financial Year", value="FY 2025-26")
            lead_source = c3.text_input("Lead Source", value="Referral")
            assigned_to = c1.text_input("Assigned To")
            managed_by = c2.text_input("Managed By")
            status = c2.selectbox("Status", STATUS_OPTIONS)
            priority = c3.selectbox("Priority", PRIORITY_OPTIONS, index=1)
            fee_amount = c1.number_input("Fee Amount", min_value=0.0, step=500.0)
            amount_received = c2.number_input("Amount Received", min_value=0.0, step=500.0)
            next_followup_date = c3.date_input("Next Follow-up", value=date.today()+timedelta(days=2))
            remarks = st.text_area("Remarks")
            if st.form_submit_button("Save Client"):
                if not client_name.strip(): st.error("Client name is mandatory.")
                else:
                    insert_rows(conn, pd.DataFrame([locals() | {"balance_amount": fee_amount-amount_received, "next_followup_date": next_followup_date.isoformat()}]))
                    st.success("Client saved successfully."); st.rerun()
    with tab_edit:
        if df_all.empty: st.info("No records available.")
        else:
            selected_id = st.selectbox("Select Client", df_all["id"].tolist(), format_func=lambda x: f"{df_all.loc[df_all['id']==x,'client_name'].iloc[0]} | ID {x}")
            row = df_all[df_all["id"] == selected_id].iloc[0]
            with st.form("edit_client"):
                c1,c2,c3 = st.columns(3)
                edit = {
                    "client_name": c1.text_input("Client Name", row["client_name"]),
                    "mobile": c2.text_input("Mobile", str(row.get("mobile", ""))),
                    "email": c3.text_input("Email", str(row.get("email", ""))),
                    "pan_number": c1.text_input("PAN Number", str(row.get("pan_number", ""))),
                    "gstin": c2.text_input("GSTIN", str(row.get("gstin", ""))),
                    "tan": c3.text_input("TAN", str(row.get("tan", ""))),
                    "msme_number": c1.text_input("MSME / Udyam Number", str(row.get("msme_number", ""))),
                    "other_registration_number": c2.text_input("Other Registration Number", str(row.get("other_registration_number", ""))),
                    "service": c1.selectbox("Service", SERVICE_OPTIONS, index=SERVICE_OPTIONS.index(row["service"]) if row["service"] in SERVICE_OPTIONS else 0),
                    "financial_year": c2.text_input("Financial Year", str(row.get("financial_year", ""))),
                    "lead_source": c3.text_input("Lead Source", str(row.get("lead_source", ""))),
                    "assigned_to": c1.text_input("Assigned To", str(row.get("assigned_to", ""))),
                    "managed_by": c2.text_input("Managed By", str(row.get("managed_by", ""))),
                    "status": c2.selectbox("Status", STATUS_OPTIONS, index=STATUS_OPTIONS.index(row["status"]) if row["status"] in STATUS_OPTIONS else 0),
                    "priority": c3.selectbox("Priority", PRIORITY_OPTIONS, index=PRIORITY_OPTIONS.index(row["priority"]) if row["priority"] in PRIORITY_OPTIONS else 1),
                    "fee_amount": c1.number_input("Fee Amount", min_value=0.0, value=float(row["fee_amount"]), step=500.0),
                    "amount_received": c2.number_input("Amount Received", min_value=0.0, value=float(row["amount_received"]), step=500.0),
                    "next_followup_date": c3.date_input("Next Follow-up", value=pd.to_datetime(row["next_followup_date"], errors="coerce").date() if pd.notna(pd.to_datetime(row["next_followup_date"], errors="coerce")) else date.today()).isoformat(),
                    "remarks": st.text_area("Remarks", str(row.get("remarks", ""))),
                }
                save, delete = st.columns(2)
                if save.form_submit_button("Update Client"):
                    update_record(conn, selected_id, edit); st.success("Updated."); st.rerun()
                if delete.form_submit_button("Delete Client"):
                    delete_record(conn, selected_id); st.warning("Deleted."); st.rerun()

elif page == "User Management":
    if not has_perm("User Management"):
        st.error("You do not have rights to manage users.")
        st.stop()
    st.subheader("User Management")
    st.caption("Admin can create users, activate/deactivate users, reset passwords and assign page rights.")
    tab_create, tab_manage = st.tabs(["Create User", "Manage Users"])
    with tab_create:
        with st.form("create_user_form"):
            c1, c2, c3 = st.columns(3)
            username = c1.text_input("Username *")
            full_name = c2.text_input("Full Name")
            role = c3.selectbox("Role", ["User", "Admin"])
            password = c1.text_input("Password *", type="password")
            confirm_password = c2.text_input("Confirm Password *", type="password")
            st.markdown("#### Assign Rights")
            default_perms = ["can_dashboard", "can_clients", "can_pipeline", "can_followups"]
            permissions = st.multiselect(
                "Page Rights",
                list(PERMISSION_LABELS.keys()),
                default=list(PERMISSION_LABELS.keys()) if role == "Admin" else default_perms,
                format_func=lambda x: PERMISSION_LABELS[x],
                disabled=(role == "Admin")
            )
            if st.form_submit_button("Create User"):
                if not username.strip() or not password:
                    st.error("Username and password are mandatory.")
                elif password != confirm_password:
                    st.error("Password and confirm password do not match.")
                elif get_user_by_username(conn, username):
                    st.error("Username already exists.")
                else:
                    create_user(conn, username, full_name, password, role, permissions)
                    st.success("User created successfully.")
                    st.rerun()
    with tab_manage:
        users_df = load_users(conn)
        st.dataframe(users_df.drop(columns=[]), use_container_width=True, hide_index=True)
        if not users_df.empty:
            selected_user_id = st.selectbox("Select user to update", users_df["id"].tolist(), format_func=lambda x: f"{users_df.loc[users_df['id']==x,'username'].iloc[0]} | {users_df.loc[users_df['id']==x,'role'].iloc[0]}")
            row = users_df[users_df["id"] == selected_user_id].iloc[0]
            with st.form("update_user_form"):
                c1, c2, c3 = st.columns(3)
                full_name = c1.text_input("Full Name", str(row.get("full_name", "")))
                role = c2.selectbox("Role", ["User", "Admin"], index=1 if row.get("role") == "Admin" else 0)
                is_active = c3.checkbox("Active", value=bool(row.get("is_active", 1)))
                existing_perms = [k for k in PERMISSION_LABELS if int(row.get(k, 0)) == 1]
                permissions = st.multiselect(
                    "Page Rights",
                    list(PERMISSION_LABELS.keys()),
                    default=list(PERMISSION_LABELS.keys()) if role == "Admin" else existing_perms,
                    format_func=lambda x: PERMISSION_LABELS[x],
                    disabled=(role == "Admin")
                )
                new_password = st.text_input("New Password (leave blank to keep existing)", type="password")
                if st.form_submit_button("Update User"):
                    update_user(conn, int(selected_user_id), full_name, role, is_active, permissions, new_password)
                    st.success("User updated successfully. Rights will apply on next login/refresh.")
                    st.rerun()


elif page == "Import / Export":
    st.subheader("Import / Export")
    tab1, tab2 = st.tabs(["Import Excel", "Export Data"])
    with tab1:
        upload = st.file_uploader("Upload Excel worklist", type=["xlsx", "xls"])
        if upload:
            xls = pd.ExcelFile(upload)
            sheet = st.selectbox("Select sheet", xls.sheet_names)
            preview = pd.read_excel(upload, sheet_name=sheet)
            st.dataframe(preview.head(25), use_container_width=True)
            if st.button("Import Sheet"):
                clean = normalize_import(preview)
                inserted, duplicates, eligible_rows = import_rows_with_duplicate_check(conn, clean)
                skipped = len(duplicates) if duplicates is not None else 0
                st.success(f"Import completed. New rows inserted: {inserted}")
                st.info(f"Valid rows found: {len(clean)} | Eligible rows imported: {eligible_rows} | Duplicate rows skipped: {skipped}")
                if duplicates is not None and not duplicates.empty:
                    st.warning("Duplicate clients were found and skipped. Please review the duplicate report below.")
                    st.dataframe(duplicates, use_container_width=True)
                    csv = duplicates.to_csv(index=False).encode("utf-8")
                    st.download_button("Download Duplicate Report", csv, "duplicate_clients_report.csv", "text/csv")
                else:
                    st.success("No duplicates found in this import.")
    with tab2:
        data = load_data(conn)
        if data.empty: st.info("No data to export.")
        else:
            out = Path("vmas_crm_export.xlsx")
            with pd.ExcelWriter(out, engine="openpyxl") as writer:
                data.drop(columns=["next_followup_dt"], errors="ignore").to_excel(writer, sheet_name="CRM Data", index=False)
                data.groupby("status").size().reset_index(name="count").to_excel(writer, sheet_name="Status Summary", index=False)
                data.groupby("service").agg(clients=("id","count"), fees=("fee_amount","sum"), received=("amount_received","sum"), outstanding=("balance_amount","sum")).reset_index().to_excel(writer, sheet_name="Service Summary", index=False)
            st.download_button("Download Excel Export", out.read_bytes(), "vmas_crm_export.xlsx")
