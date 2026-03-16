
# Excel Reconciliation App v5

Version 5 was rebuilt from the real workbook layouts you uploaded.

## What changed
- Reconciliation parsing is now **header-anchored**
- Merchant statement parsing is now **header-anchored**
- The app handles the merchant statement's **summary section** and **transaction section**
- The matching engine still uses the Feb 9 logic:
  1. same-day exact match on normalized auth code + amount
  2. next-day exact match on normalized auth code + amount

## Intended scope
This version is designed for other date ranges of the **same two export formats**:
- Credit Card Transaction Detail (Online)
- Merchant Statement

## Run
```bash
pip install -r requirements.txt
streamlit run app.py
```
