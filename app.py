import sqlite3
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
    "service", "financial_year", "lead_source", "assigned_to", "status", "priority",
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
[data-testid="stSidebar"] { background: linear-gradient(180deg, #17324D 0%, #244866 100%); border-right: 0; box-shadow: 8px 0 28px rgba(15,23,42,.16); }
[data-testid="stSidebar"] * { color:#FFFFFF !important; }
[data-testid="stSidebar"] .stRadio [role="radiogroup"] label { background:rgba(255,255,255,.08); border-radius:12px; padding:4px 8px; margin:3px 0; }
[data-testid="stSidebar"] input, [data-testid="stSidebar"] textarea { color:#111827 !important; background:#FFFFFF !important; }
.powerbi-header { display:flex; justify-content:space-between; align-items:center; gap:18px; padding:18px 22px; border-radius:18px; background:#FFFFFF; border:1px solid #E5E7EB; box-shadow:0 6px 22px rgba(15,23,42,.06); margin-bottom:16px; }
.powerbi-title { font-size:1.55rem; font-weight:800; color:#17324D; margin:0; }
.powerbi-subtitle { color:#64748B; font-size:.9rem; margin-top:4px; }
.powerbi-badge { padding:8px 12px; border-radius:999px; background:#FFF7DC; color:#7A5200; border:1px solid rgba(184,134,11,.28); font-weight:700; font-size:.8rem; white-space:nowrap; }
.filter-strip { display:flex; flex-wrap:wrap; gap:8px; margin: -4px 0 14px 0; }
.filter-pill { padding:7px 11px; border-radius:999px; background:#FFFFFF; border:1px solid #E5E7EB; color:#475569; box-shadow:0 4px 14px rgba(15,23,42,.04); font-size:.78rem; }
.kpi-grid { display:grid; grid-template-columns: repeat(6, minmax(145px, 1fr)); gap:12px; margin: 8px 0 14px 0; }
.kpi-card { background:#FFFFFF; border:1px solid #E5E7EB; border-left:5px solid var(--gold); border-radius:16px; box-shadow:0 8px 20px rgba(15,23,42,.06); padding:15px 16px; min-height:112px; transition:.22s ease; overflow:hidden; }
.kpi-card:hover { transform:translateY(-3px); box-shadow:0 12px 28px rgba(15,23,42,.10); }
.kpi-label { color:#64748B; font-size:.72rem; font-weight:800; text-transform:uppercase; letter-spacing:.08em; line-height:1.25; }
.kpi-value { font-size:clamp(1.15rem,1.6vw,1.65rem); font-weight:800; color:#17324D; margin-top:9px; line-height:1.12; overflow-wrap:anywhere; }
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
.stTabs [data-baseweb="tab-list"] { gap: 8px; border-bottom:1px solid #E5E7EB; }
.stTabs [data-baseweb="tab"] { background:#FFFFFF; border:1px solid #E5E7EB; border-bottom:0; border-radius:12px 12px 0 0; padding:10px 16px; color:#17324D; font-weight:700; }
.stTabs [aria-selected="true"] { background:#FFF7DC !important; color:#7A5200 !important; border-color:#F4C542 !important; }
[data-testid="stDataFrame"] { background:#FFFFFF; border-radius:14px; }
@media (max-width: 1100px) { .kpi-grid { grid-template-columns: repeat(3, minmax(0, 1fr)); } .dashboard-grid-2, .dashboard-grid-3 { grid-template-columns: 1fr; } .powerbi-header { flex-direction:column; align-items:flex-start; } }
@media (max-width: 640px) { .kpi-grid { grid-template-columns: 1fr; } }
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
            lead_source TEXT, assigned_to TEXT, status TEXT DEFAULT 'New Lead', priority TEXT DEFAULT 'Medium',
            fee_amount REAL DEFAULT 0, amount_received REAL DEFAULT 0, balance_amount REAL DEFAULT 0,
            next_followup_date TEXT, remarks TEXT, created_at TEXT, updated_at TEXT
        )
    """)
    # Auto-migrate older local databases created by previous app versions
    existing_cols = [row[1] for row in conn.execute("PRAGMA table_info(clients)").fetchall()]
    for col in ["pan_number", "gstin", "tan", "msme_number", "other_registration_number"]:
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
                        clean_text(r.get("financial_year", "")) or "FY 2025-26", clean_text(r.get("lead_source", "")), clean_text(r.get("assigned_to", "")),
                        clean_text(r.get("status", "")) or "New Lead", clean_text(r.get("priority", "")) or "Medium", fee, rec, bal,
                        clean_text(r.get("next_followup_date", "")), clean_text(r.get("remarks", "")), now, now))
    if not records: return 0
    before = conn.total_changes
    conn.executemany("""
        INSERT OR IGNORE INTO clients
        (client_name,mobile,email,pan_number,gstin,tan,msme_number,other_registration_number,service,financial_year,lead_source,assigned_to,status,priority,fee_amount,amount_received,balance_amount,next_followup_date,remarks,created_at,updated_at)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, records)
    conn.commit()
    return conn.total_changes - before


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

def kpi(label, value, note=""):
    st.markdown(f"<div class='kpi-card'><div class='kpi-label'>{label}</div><div class='kpi-value'>{value}</div><div class='kpi-note'>{note}</div></div>", unsafe_allow_html=True)

conn = init_db()
df_all = load_data(conn)

with st.sidebar:
    if LOGO_PATH.exists(): st.image(str(LOGO_PATH), width=150)
    st.markdown("### VMAS Tax Solutions")
    st.caption("Professional Services Guaranteed")
    page = st.radio("Navigation", ["Executive Dashboard", "Client Workspace", "Lead Pipeline", "Follow-ups", "Add / Edit Client", "Import / Export"], label_visibility="collapsed")
    st.divider()
    st.markdown("#### Smart Filters")
    status_filter = st.multiselect("Status", STATUS_OPTIONS)
    service_filter = st.multiselect("Service", SERVICE_OPTIONS)
    priority_filter = st.multiselect("Priority", PRIORITY_OPTIONS)
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
    if search_text:
        mask = out[["client_name", "mobile", "email", "pan_number", "gstin", "tan", "msme_number", "other_registration_number", "service"]].fillna("").astype(str).agg(" ".join, axis=1).str.contains(search_text, case=False, na=False)
        out = out[mask]
    return out

filtered = apply_filters(df_all)

st.markdown(f"""
<div class='powerbi-header'>
  <div>
    <div class='powerbi-title'>VMAS CRM Analytics Dashboard</div>
    <div class='powerbi-subtitle'>Power BI style view for clients, filings, collections, follow-ups and lead pipeline</div>
  </div>
  <div class='powerbi-badge'>Amount View: {amount_unit}</div>
</div>
""", unsafe_allow_html=True)
active_filters = []
if status_filter: active_filters.append('Status: ' + ', '.join(status_filter))
if service_filter: active_filters.append('Service: ' + ', '.join(service_filter))
if priority_filter: active_filters.append('Priority: ' + ', '.join(priority_filter))
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
    st.markdown("<div class='kpi-grid'>", unsafe_allow_html=True)
    for label, value, note in kpi_items:
        kpi(label, value, note)
    st.markdown("</div>", unsafe_allow_html=True)

    dash_tab1, dash_tab2, dash_tab3, dash_tab4 = st.tabs(["Executive Summary", "Revenue & Recovery", "Compliance Workload", "Follow-up Control"])

    with dash_tab1:
        c1, c2 = st.columns([1.25, .75])
        with c1:
            st.markdown("<div class='tile'><div class='tile-title'>Work Status Funnel</div>", unsafe_allow_html=True)
            status_counts = filtered.groupby("status", dropna=False).size().reset_index(name="count")
            fig = px.bar(status_counts, x="count", y="status", orientation="h", text="count")
            fig.update_layout(template="plotly_white", height=360, margin=dict(t=10,l=5,r=5,b=5), xaxis_title="Clients", yaxis_title="")
            fig.update_traces(marker_color="#17324D", textposition="outside")
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
            st.dataframe(due[["client_name","mobile","service","status","priority","next_followup_date","balance_amount","remarks"]].head(15), use_container_width=True, hide_index=True)
            st.markdown("</div>", unsafe_allow_html=True)

elif page == "Client Workspace":
    st.subheader("Client Workspace")
    if not filtered.empty:
        view_cols = ["client_name","mobile","email","pan_number","gstin","tan","msme_number","other_registration_number","service","financial_year","status","priority","fee_amount","amount_received","balance_amount","next_followup_date","remarks"]
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
        st.dataframe(due[["client_name","mobile","service","status","priority","next_followup_date","balance_amount","remarks"]], use_container_width=True, hide_index=True)
        for _, r in due.head(10).iterrows():
            mobile = ''.join(ch for ch in str(r['mobile']) if ch.isdigit())
            msg = quote_plus(f"Dear {r['client_name']}, this is a reminder from VMAS Tax Solutions regarding your {r['service']} work. Kindly share pending details/payment to proceed.")
            if mobile:
                st.markdown(f"[{r['client_name']} - Send WhatsApp reminder](https://wa.me/91{mobile[-10:]}?text={msg})")
    with tab2:
        st.dataframe(upcoming[["client_name","mobile","service","status","priority","next_followup_date","balance_amount","remarks"]], use_container_width=True, hide_index=True)

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
                inserted = insert_rows(conn, clean)
                st.success(f"Import completed. Valid rows: {len(clean)} | New rows inserted: {inserted}"); st.rerun()
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
