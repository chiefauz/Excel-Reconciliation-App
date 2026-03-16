
# Excel Reconciliation App v4

This version fixes the underlying design problem:

The Feb 9 report logic and the workbook-layout parsing logic are now separate.

## Core logic preserved from the Feb 9 report
The matching engine uses the same sequence:
1. same-day exact match on normalized auth code + amount
2. next-day exact match on normalized auth code + amount

It does **not** use fuzzy matching.

## What changed
- detects reconciliation header rows dynamically
- detects statement transaction sections dynamically
- maps uploaded files into a canonical structure first
- then applies the exact same matching logic used for the Feb 9 workbook
- supports date ranges by classifying merchant unmatched items as:
  - within reconciliation dates
  - outside reconciliation date range

## Install and run
```bash
pip install -r requirements.txt
streamlit run app.py
```
