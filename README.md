# Excel Reconciliation App

This Streamlit app compares:
- a reconciliation workbook
- a merchant statement workbook

It automatically:
- matches transactions
- shows unmatched items
- calculates true net settlement
- flags duplicates and declines
- exports a clean Excel reconciliation report

## Run locally

```bash
pip install -r requirements.txt
streamlit run app.py
```

## Input files

Upload two `.xlsx` files:
1. Reconciliation workbook
2. Merchant statement workbook

## Output

The app produces:
- on-screen summary metrics
- matched transactions table
- unmatched transactions tables
- exceptions table
- downloadable Excel report
