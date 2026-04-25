import sqlite3
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

APP_TITLE = "VMAS TAX SOLUTIONS - CRM"
DB_PATH = Path("vmas_crm.db")
DEFAULT_COLUMNS = [
    "client_name", "mobile", "email", "service", "financial_year", "lead_source",
    "assigned_to", "status", "priority", "fee_amount", "amount_received",
    "balance_amount", "next_followup_date", "remarks"
]
STATUS_OPTIONS = ["New Lead", "Contacted", "Documents Pending", "In Progress", "Filed/Completed", "Payment Pending", "Closed", "Lost"]
PRIORITY_OPTIONS = ["High", "Medium", "Low"]
SERVICE_OPTIONS = ["ITR Filing", "GST Return", "GST Registration", "Accounting", "ROC Filing", "Tax Consultation", "Other"]

st.set_page_config(page_title=APP_TITLE, page_icon="📊", layout="wide")


def get_conn():
    return sqlite3.connect(DB_PATH, check_same_thread=False)


def init_db():
    conn = get_conn()
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS clients (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            client_name TEXT NOT NULL,
            mobile TEXT,
            email TEXT,
            service TEXT,
            financial_year TEXT,
            lead_source TEXT,
            assigned_to TEXT,
            status TEXT DEFAULT 'New Lead',
            priority TEXT DEFAULT 'Medium',
            fee_amount REAL DEFAULT 0,
            amount_received REAL DEFAULT 0,
            balance_amount REAL DEFAULT 0,
            next_followup_date TEXT,
            remarks TEXT,
            created_at TEXT,
            updated_at TEXT
        )
        """
    )
    conn.commit()
    return conn


def load_data(conn):
    df = pd.read_sql_query("SELECT * FROM clients ORDER BY updated_at DESC, id DESC", conn)
    if df.empty:
        return df
    for col in ["fee_amount", "amount_received", "balance_amount"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    return df


def normalize_import(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip().lower().replace(" ", "_").replace("/", "_") for c in df.columns]

    mapping_candidates = {
        "client_name": ["client", "name", "customer", "party_name", "assessee", "client_name"],
        "mobile": ["mobile", "phone", "contact", "contact_no", "mobile_no"],
        "email": ["email", "mail", "email_id"],
        "service": ["service", "work", "type", "return_type", "nature_of_work"],
        "financial_year": ["fy", "financial_year", "year", "assessment_year"],
        "status": ["status", "stage", "filing_status", "work_status"],
        "fee_amount": ["fee", "fees", "amount", "professional_fees", "billing"],
        "amount_received": ["received", "amount_received", "paid", "collection"],
        "balance_amount": ["balance", "pending", "outstanding", "balance_amount"],
        "next_followup_date": ["followup", "next_followup", "follow_up_date", "next_followup_date"],
        "remarks": ["remarks", "remark", "notes", "comments"],
    }

    out = pd.DataFrame()
    for target in DEFAULT_COLUMNS:
        out[target] = ""
        for candidate in mapping_candidates.get(target, [target]):
            matches = [c for c in df.columns if c == candidate or candidate in c]
            if matches:
                out[target] = df[matches[0]]
                break

    if out["client_name"].astype(str).str.strip().eq("").all():
        first_text_col = df.select_dtypes(include="object").columns[:1]
        if len(first_text_col):
            out["client_name"] = df[first_text_col[0]]

    out["client_name"] = out["client_name"].astype(str).str.strip()
    out = out[out["client_name"].ne("") & out["client_name"].ne("nan")]
    out["status"] = out["status"].replace("", "New Lead").fillna("New Lead")
    out["priority"] = out["priority"].replace("", "Medium").fillna("Medium")
    out["service"] = out["service"].replace("", "ITR Filing").fillna("ITR Filing")
    out["financial_year"] = out["financial_year"].replace("", "FY 2025-26").fillna("FY 2025-26")
    for col in ["fee_amount", "amount_received", "balance_amount"]:
        out[col] = pd.to_numeric(out[col], errors="coerce").fillna(0)
    out["balance_amount"] = out.apply(lambda r: r["fee_amount"] - r["amount_received"] if r["balance_amount"] == 0 else r["balance_amount"], axis=1)
    return out


def insert_rows(conn, df):
    now = datetime.now().isoformat(timespec="seconds")
    records = []
    for _, row in df.iterrows():
        balance = float(row.get("balance_amount", 0) or 0)
        fee = float(row.get("fee_amount", 0) or 0)
        received = float(row.get("amount_received", 0) or 0)
        if balance == 0 and fee > 0:
            balance = fee - received
        records.append((
            row.get("client_name", ""), row.get("mobile", ""), row.get("email", ""), row.get("service", ""),
            row.get("financial_year", ""), row.get("lead_source", ""), row.get("assigned_to", ""),
            row.get("status", "New Lead"), row.get("priority", "Medium"), fee, received, balance,
            str(row.get("next_followup_date", "") or ""), row.get("remarks", ""), now, now
        ))
    conn.executemany(
        """
        INSERT INTO clients
        (client_name, mobile, email, service, financial_year, lead_source, assigned_to, status, priority,
         fee_amount, amount_received, balance_amount, next_followup_date, remarks, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        records,
    )
    conn.commit()


def update_record(conn, row_id, data):
    data["balance_amount"] = float(data.get("fee_amount", 0)) - float(data.get("amount_received", 0))
    data["updated_at"] = datetime.now().isoformat(timespec="seconds")
    assignments = ", ".join([f"{k}=?" for k in data])
    conn.execute(f"UPDATE clients SET {assignments} WHERE id=?", list(data.values()) + [row_id])
    conn.commit()


def delete_record(conn, row_id):
    conn.execute("DELETE FROM clients WHERE id=?", (row_id,))
    conn.commit()


conn = init_db()
st.title(APP_TITLE)
st.caption("Python based CRM for leads, filing worklist, client follow-ups and collections")

with st.sidebar:
    st.header("CRM Menu")
    page = st.radio("Go to", ["Dashboard", "Clients", "Add / Update", "Import Excel", "Export"], label_visibility="collapsed")
    st.divider()
    st.subheader("Quick Filters")
    df_all = load_data(conn)
    status_filter = st.multiselect("Status", STATUS_OPTIONS, default=[])
    service_filter = st.multiselect("Service", SERVICE_OPTIONS, default=[])
    search_text = st.text_input("Search client/mobile/email")


def apply_filters(df):
    if df.empty:
        return df
    out = df.copy()
    if status_filter:
        out = out[out["status"].isin(status_filter)]
    if service_filter:
        out = out[out["service"].isin(service_filter)]
    if search_text:
        mask = out[["client_name", "mobile", "email"]].fillna("").astype(str).agg(" ".join, axis=1).str.contains(search_text, case=False, na=False)
        out = out[mask]
    return out

filtered = apply_filters(df_all)

if page == "Dashboard":
    st.subheader("Dashboard")
    if filtered.empty:
        st.info("No CRM data available. Import Excel or add a client record.")
    else:
        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("Total Clients", len(filtered))
        c2.metric("Open Cases", int((~filtered["status"].isin(["Filed/Completed", "Closed", "Lost"])).sum()))
        c3.metric("Completed", int(filtered["status"].isin(["Filed/Completed", "Closed"]).sum()))
        c4.metric("Fees", f"₹{filtered['fee_amount'].sum():,.0f}")
        c5.metric("Outstanding", f"₹{filtered['balance_amount'].sum():,.0f}")

        left, right = st.columns(2)
        with left:
            status_counts = filtered.groupby("status", dropna=False).size().reset_index(name="count")
            st.plotly_chart(px.pie(status_counts, names="status", values="count", title="Status Mix"), use_container_width=True)
        with right:
            service_counts = filtered.groupby("service", dropna=False).size().reset_index(name="count").sort_values("count", ascending=False)
            st.plotly_chart(px.bar(service_counts, x="service", y="count", title="Service-wise Clients"), use_container_width=True)

        st.markdown("#### Follow-ups Due")
        temp = filtered.copy()
        temp["next_followup_date_parsed"] = pd.to_datetime(temp["next_followup_date"], errors="coerce")
        due = temp[temp["next_followup_date_parsed"].dt.date <= date.today()].sort_values("next_followup_date_parsed")
        st.dataframe(due[["client_name", "mobile", "service", "status", "priority", "next_followup_date", "balance_amount", "remarks"]], use_container_width=True, hide_index=True)

elif page == "Clients":
    st.subheader("Client CRM List")
    st.dataframe(filtered, use_container_width=True, hide_index=True)

elif page == "Add / Update":
    st.subheader("Add New Client")
    with st.form("add_client"):
        col1, col2, col3 = st.columns(3)
        client_name = col1.text_input("Client Name *")
        mobile = col2.text_input("Mobile")
        email = col3.text_input("Email")
        service = col1.selectbox("Service", SERVICE_OPTIONS)
        financial_year = col2.text_input("Financial Year", value="FY 2025-26")
        lead_source = col3.text_input("Lead Source")
        assigned_to = col1.text_input("Assigned To")
        status = col2.selectbox("Status", STATUS_OPTIONS)
        priority = col3.selectbox("Priority", PRIORITY_OPTIONS, index=1)
        fee_amount = col1.number_input("Fee Amount", min_value=0.0, step=500.0)
        amount_received = col2.number_input("Amount Received", min_value=0.0, step=500.0)
        next_followup_date = col3.date_input("Next Follow-up", value=date.today() + timedelta(days=2))
        remarks = st.text_area("Remarks")
        submitted = st.form_submit_button("Save Client")
        if submitted:
            if not client_name.strip():
                st.error("Client name is mandatory.")
            else:
                insert_rows(conn, pd.DataFrame([{
                    "client_name": client_name, "mobile": mobile, "email": email, "service": service,
                    "financial_year": financial_year, "lead_source": lead_source, "assigned_to": assigned_to,
                    "status": status, "priority": priority, "fee_amount": fee_amount,
                    "amount_received": amount_received, "balance_amount": fee_amount - amount_received,
                    "next_followup_date": next_followup_date.isoformat(), "remarks": remarks
                }]))
                st.success("Client saved successfully.")
                st.rerun()

    st.divider()
    st.subheader("Update / Delete Existing Record")
    if df_all.empty:
        st.info("No existing records to update.")
    else:
        selected_id = st.selectbox("Select Record ID", df_all["id"].tolist(), format_func=lambda x: f"{x} - {df_all.loc[df_all['id']==x, 'client_name'].iloc[0]}")
        row = df_all[df_all["id"] == selected_id].iloc[0]
        with st.form("edit_client"):
            col1, col2, col3 = st.columns(3)
            edit_data = {
                "client_name": col1.text_input("Client Name", row["client_name"]),
                "mobile": col2.text_input("Mobile", str(row.get("mobile", ""))),
                "email": col3.text_input("Email", str(row.get("email", ""))),
                "service": col1.selectbox("Service", SERVICE_OPTIONS, index=SERVICE_OPTIONS.index(row["service"]) if row["service"] in SERVICE_OPTIONS else 0),
                "financial_year": col2.text_input("Financial Year", str(row.get("financial_year", ""))),
                "lead_source": col3.text_input("Lead Source", str(row.get("lead_source", ""))),
                "assigned_to": col1.text_input("Assigned To", str(row.get("assigned_to", ""))),
                "status": col2.selectbox("Status", STATUS_OPTIONS, index=STATUS_OPTIONS.index(row["status"]) if row["status"] in STATUS_OPTIONS else 0),
                "priority": col3.selectbox("Priority", PRIORITY_OPTIONS, index=PRIORITY_OPTIONS.index(row["priority"]) if row["priority"] in PRIORITY_OPTIONS else 1),
                "fee_amount": col1.number_input("Fee Amount", min_value=0.0, value=float(row["fee_amount"]), step=500.0),
                "amount_received": col2.number_input("Amount Received", min_value=0.0, value=float(row["amount_received"]), step=500.0),
                "next_followup_date": col3.date_input("Next Follow-up", value=pd.to_datetime(row["next_followup_date"], errors="coerce").date() if pd.notna(pd.to_datetime(row["next_followup_date"], errors="coerce")) else date.today()).isoformat(),
                "remarks": st.text_area("Remarks", str(row.get("remarks", ""))),
            }
            col_save, col_delete = st.columns(2)
            if col_save.form_submit_button("Update Record"):
                update_record(conn, selected_id, edit_data)
                st.success("Record updated.")
                st.rerun()
            if col_delete.form_submit_button("Delete Record"):
                delete_record(conn, selected_id)
                st.warning("Record deleted.")
                st.rerun()

elif page == "Import Excel":
    st.subheader("Import Excel Worklist")
    upload = st.file_uploader("Upload Excel file", type=["xlsx", "xls"])
    if upload:
        xls = pd.ExcelFile(upload)
        sheet = st.selectbox("Select sheet", xls.sheet_names)
        preview = pd.read_excel(upload, sheet_name=sheet)
        st.write("Preview")
        st.dataframe(preview.head(20), use_container_width=True)
        if st.button("Import selected sheet into CRM"):
            clean = normalize_import(preview)
            insert_rows(conn, clean)
            st.success(f"Imported {len(clean)} records.")
            st.rerun()

elif page == "Export":
    st.subheader("Export CRM")
    data = load_data(conn)
    if data.empty:
        st.info("No data to export.")
    else:
        output = Path("vmas_crm_export.xlsx")
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            data.to_excel(writer, sheet_name="CRM Data", index=False)
            data.groupby("status").size().reset_index(name="count").to_excel(writer, sheet_name="Status Summary", index=False)
            data.groupby("service").agg(clients=("id", "count"), fees=("fee_amount", "sum"), received=("amount_received", "sum"), outstanding=("balance_amount", "sum")).reset_index().to_excel(writer, sheet_name="Service Summary", index=False)
        st.download_button("Download Excel Export", data=output.read_bytes(), file_name="vmas_crm_export.xlsx")
