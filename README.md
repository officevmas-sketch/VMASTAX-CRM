# VMAS Python CRM Software

A local Python/Streamlit CRM for tax consultancy worklists.

## Features
- Client master and searchable CRM table
- Lead / filing / payment / follow-up status tracking
- Dashboard KPIs and charts
- Add / update client records
- Import from Excel and export to Excel
- SQLite database for local storage

## How to run
```bash
pip install -r requirements.txt
streamlit run app.py
```

## Suggested workflow
1. Place your Excel worklist in the same folder.
2. Run the app.
3. Use **Import Excel** from the sidebar.
4. Manage clients, update statuses and export latest CRM.


## Interactive VMAS Branding Added
- VMAS Tax logo included in `assets/vmas_logo.png`
- Animated premium black-gold header
- Animated KPI cards
- Dashboard tabs for Overview, Follow-ups, Collections and Priority
- Kanban-style status board
