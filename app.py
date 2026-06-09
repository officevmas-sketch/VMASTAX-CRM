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
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700;800&display=swap');
:root { --gold:#C99A18; --gold2:#FFE29A; --cream:#FFF8E7; --panel:#FFFFFF; --soft:#F7F3EA; --text:#1F2937; --muted:#6B7280; --blue:#2563EB; }
html, body, [class*="css"] { font-family: 'Inter', sans-serif; color: var(--text); }
.stApp { background: linear-gradient(135deg, #FFFDF7 0%, #FFF7DA 45%, #F4FAFF 100%); color: var(--text); }
[data-testid="stSidebar"] { background: linear-gradient(180deg, #FFFFFF 0%, #FFF5D6 100%); border-right: 1px solid rgba(201,154,24,.25); box-shadow: 6px 0 22px rgba(31,41,55,.06); }
[data-testid="stSidebar"] * { color: #1F2937 !important; }
.block-container { padding-top: 1.2rem; }
.hero-card { padding: 28px 30px; border-radius: 26px; background: linear-gradient(135deg, rgba(255,255,255,.96), rgba(255,239,184,.78)); border: 1px solid rgba(201,154,24,.28); box-shadow: 0 18px 55px rgba(201,154,24,.13); position: relative; overflow: hidden; }
.hero-card:before { content: ''; position:absolute; top:-90px; right:-70px; width:260px; height:260px; background: radial-gradient(circle, rgba(255,226,154,.75), transparent 68%); animation: pulse 4s ease-in-out infinite; }
.hero-card:after { content: ''; position:absolute; bottom:-120px; left:-80px; width:260px; height:260px; background: radial-gradient(circle, rgba(37,99,235,.12), transparent 70%); animation: floatGlow 5s ease-in-out infinite; }
@keyframes pulse { 0%,100% { transform:scale(.92); opacity:.65;} 50% { transform:scale(1.08); opacity:1;} }
@keyframes floatGlow { 0%,100% { transform:translateY(0);} 50% { transform:translateY(-14px);} }
.hero-title { font-size: 2.2rem; font-weight: 800; margin-bottom: 4px; background: linear-gradient(90deg, #111827, #B8860B, #2563EB); -webkit-background-clip: text; color: transparent; position:relative; z-index:1; }
.hero-sub { color:#475569; font-size:1rem; position:relative; z-index:1; }
.kpi-card { padding: 22px; border-radius: 22px; background: rgba(255,255,255,.94); border: 1px solid rgba(201,154,24,.22); box-shadow: 0 10px 30px rgba(31,41,55,.08); transition: transform .25s ease, box-shadow .25s ease, border .25s ease; min-height: 118px; }
.kpi-card:hover { transform: translateY(-5px); border: 1px solid rgba(201,154,24,.65); box-shadow: 0 16px 38px rgba(201,154,24,.16); }
.kpi-label { color:#64748B; font-size:.82rem; text-transform:uppercase; letter-spacing:.08em; }
.kpi-value { font-size:1.85rem; font-weight:800; color:#B8860B; margin-top:8px; }
.kpi-note { color:#475569; font-size:.85rem; margin-top:4px; }
.section-card { padding: 20px; border-radius: 22px; background: rgba(255,255,255,.95); border:1px solid rgba(201,154,24,.18); box-shadow: 0 12px 30px rgba(31,41,55,.08); }
.kanban { padding: 14px; border-radius: 18px; background: rgba(255,255,255,.90); border:1px solid rgba(201,154,24,.20); min-height:145px; box-shadow: 0 8px 22px rgba(31,41,55,.06); }
.kanban h4 { color:#B8860B; margin:0 0 8px 0; }
.chip { display:inline-block; padding:6px 10px; border-radius:999px; border:1px solid rgba(201,154,24,.38); background:rgba(255,226,154,.55); color:#7A5200; margin:3px; font-size:.78rem; }
.stButton>button, .stDownloadButton>button { border-radius: 14px !important; background: linear-gradient(135deg, #F6C453, #FFE29A) !important; color: #111827 !important; border: 0 !important; font-weight: 800 !important; box-shadow:0 8px 20px rgba(201,154,24,.18); }
.stButton>button:hover, .stDownloadButton>button:hover { transform: translateY(-2px); box-shadow:0 12px 24px rgba(201,154,24,.24); }
.stTabs [data-baseweb="tab-list"] { gap: 8px; }
.stTabs [data-baseweb="tab"] { background:rgba(255,255,255,.75); border:1px solid rgba(201,154,24,.16); border-radius:14px; padding:10px 16px; color:#7A5200; }
.stTabs [aria-selected="true"] { background:linear-gradient(135deg,#F6C453,#FFE29A) !important; color:#111827 !important; font-weight:800; }
[data-testid="stMetricValue"] { color: #B8860B; }
[data-testid="stMetricLabel"] { color: #475569; }
hr { border-color: rgba(201,154,24,.22); }
input, textarea, select { background-color: #FFFFFF !important; color: #111827 !important; }
[data-testid="stDataFrame"] { background:#FFFFFF; border-radius:18px; }
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


def money(v): return f"₹{v:,.0f}"

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

st.markdown("""
<div class='hero-card'>
  <div class='hero-title'>VMAS Bright CRM Command Center</div>
  <div class='hero-sub'>Leads • ITR/GST Worklist • Follow-ups • Collections • Client Relationship Tracking</div>
</div>
""", unsafe_allow_html=True)
st.write("")

if filtered.empty:
    st.info("No CRM records yet. Add a client or import your Excel worklist from the Import / Export page.")

if page == "Executive Dashboard" and not filtered.empty:
    total = len(filtered); open_cases = int((~filtered["status"].isin(["Filed/Completed", "Closed", "Lost"])).sum())
    completed = int(filtered["status"].isin(["Filed/Completed", "Closed"]).sum())
    fees = filtered["fee_amount"].sum(); received = filtered["amount_received"].sum(); outstanding = filtered["balance_amount"].sum()
    today = pd.Timestamp(date.today())
    due_count = int((filtered["next_followup_dt"].notna() & (filtered["next_followup_dt"] <= today)).sum())
    c1,c2,c3,c4,c5,c6 = st.columns(6)
    with c1: kpi("Total Clients", total, "Active database")
    with c2: kpi("Open Cases", open_cases, "Need action")
    with c3: kpi("Completed", completed, f"{completed/max(total,1):.0%} closure")
    with c4: kpi("Fees", money(fees), "Billed / expected")
    with c5: kpi("Received", money(received), "Collection")
    with c6: kpi("Outstanding", money(outstanding), f"{due_count} follow-ups due")
    st.write("")
    left, right = st.columns([1.2, 1])
    with left:
        st.markdown("<div class='section-card'>", unsafe_allow_html=True)
        status_counts = filtered.groupby("status", dropna=False).size().reset_index(name="count")
        fig = px.bar(status_counts, x="status", y="count", title="Work Status Funnel", text="count")
        fig.update_layout(template="plotly_dark", paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
        st.plotly_chart(fig, use_container_width=True)
        st.markdown("</div>", unsafe_allow_html=True)
    with right:
        st.markdown("<div class='section-card'>", unsafe_allow_html=True)
        fig = go.Figure(go.Indicator(mode="gauge+number", value=(received/fees*100 if fees else 0), title={"text":"Collection Efficiency %"}, gauge={"axis":{"range":[None,100]}, "bar":{"color":"#D4AF37"}}))
        fig.update_layout(template="plotly_dark", paper_bgcolor="rgba(0,0,0,0)", height=380)
        st.plotly_chart(fig, use_container_width=True)
        st.markdown("</div>", unsafe_allow_html=True)
    a,b = st.columns(2)
    with a:
        service = filtered.groupby("service").agg(clients=("id","count"), fees=("fee_amount","sum"), outstanding=("balance_amount","sum")).reset_index().sort_values("fees", ascending=False)
        fig = px.treemap(service, path=["service"], values="fees", color="outstanding", title="Service Portfolio by Fees & Outstanding")
        fig.update_layout(template="plotly_dark", paper_bgcolor="rgba(0,0,0,0)")
        st.plotly_chart(fig, use_container_width=True)
    with b:
        pr = filtered.groupby("priority").size().reset_index(name="count")
        fig = px.pie(pr, names="priority", values="count", hole=.55, title="Priority Mix")
        fig.update_layout(template="plotly_dark", paper_bgcolor="rgba(0,0,0,0)")
        st.plotly_chart(fig, use_container_width=True)

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
