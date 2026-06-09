# VMAS Bright CRM

Run locally:

```bash
pip install -r requirements.txt
streamlit run app.py
```

## Updated Registration Fields
The Add / Edit Client screen and Excel import now support:
- PAN Number
- GSTIN
- TAN
- MSME / Udyam Number
- Other Registration Number

The app automatically migrates older local SQLite databases by adding these columns on startup.

## Import Template
Use `templates/VMAS_Client_Import_Template_Updated.xlsx` for client upload.
