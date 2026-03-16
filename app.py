
import io
import re
from dataclasses import dataclass
from typing import Optional, List, Dict
import numpy as np
import pandas as pd
import streamlit as st

APP_TITLE = "Excel Reconciliation App"

@dataclass
class SummaryMetrics:
    gross_sales: float
    bank_charges: float
    vat: float
    net_settlement: float
    rec_total_count: int
    rec_total_amount: float
    rec_scope_count: int
    rec_scope_amount: float
    stmt_total_count: int
    stmt_total_amount: float
    matched_count: int
    matched_amount: float
    same_day_count: int
    same_day_amount: float
    next_day_count: int
    next_day_amount: float
    rec_unmatched_scope_count: int
    rec_unmatched_scope_amount: float
    rec_unmatched_out_scope_count: int
    rec_unmatched_out_scope_amount: float
    stmt_unmatched_same_date_count: int
    stmt_unmatched_same_date_amount: float
    stmt_unmatched_other_date_count: int
    stmt_unmatched_other_date_amount: float
    duplicate_count: int
    decline_count: int

def parse_amount_text(v):
    if pd.isna(v):
        return np.nan
    s = str(v).strip().upper().replace(",", "")
    m = re.search(r"(-?\d+(?:\.\d+)?)", s)
    return abs(float(m.group(1))) if m else np.nan

def normalize_brand(v: str) -> str:
    if pd.isna(v):
        return ""
    s = str(v).strip().upper()
    return {
        "V": "VISA", "VISA": "VISA",
        "M": "MASTERCARD", "MC": "MASTERCARD", "MASTERCARD": "MASTERCARD",
        "A": "AMEX", "AMEX": "AMEX", "AMERICAN EXPRESS": "AMEX",
    }.get(s, s or "")

def extract_last4(v):
    if pd.isna(v):
        return ""
    s = str(v)
    m = re.search(r"(\d{4})\s*$", s)
    if m:
        return m.group(1)
    digits = re.findall(r"\d", s)
    return "".join(digits[-4:]) if len(digits) >= 4 else ""

def normalize_auth(v):
    if pd.isna(v):
        return ""
    s = re.sub(r"[^A-Z0-9]", "", str(v).strip().upper())
    if s in {"", "NAN", "NONE", "NULL", "-", "NA"}:
        return ""
    stripped = s.lstrip("0")
    return stripped if stripped else s

def safe_float(v, default=0.0):
    try:
        if pd.isna(v):
            return default
        return float(v)
    except Exception:
        return default

def find_best_column(df: pd.DataFrame, candidates: List[int], scorer) -> Optional[int]:
    best_col = None
    best_score = -1.0
    for c in candidates:
        if c not in df.columns:
            continue
        sample = df[c].dropna().head(200).astype(str)
        if sample.empty:
            continue
        score = scorer(sample)
        if score > best_score:
            best_score = score
            best_col = c
    return best_col

def score_datetime(series: pd.Series) -> float:
    return float(pd.to_datetime(series, errors="coerce").notna().mean())

def score_amount(series: pd.Series) -> float:
    return float(series.map(parse_amount_text).notna().mean())

def score_brand(series: pd.Series) -> float:
    s = series.astype(str).str.upper().str.strip()
    return float(s.isin(["V","VISA","M","MC","MASTERCARD","A","AMEX","AMERICAN EXPRESS"]).mean())

def score_last4(series: pd.Series) -> float:
    return float((series.map(extract_last4).str.len() == 4).mean())

def score_auth(series: pd.Series) -> float:
    return float((series.map(normalize_auth) != "").mean())

def parse_statement_summary(raw: pd.DataFrame) -> dict:
    gross_sales = bank_charges = vat = 0.0
    for _, row in raw.head(20).iterrows():
        txt = " | ".join([str(x) for x in row.tolist() if pd.notna(x)]).upper()
        nums = re.findall(r"\d+(?:\.\d+)?", txt)
        if "BANK CHARGES" in txt and nums:
            bank_charges = float(nums[-1])
        if "GROSS SALES" in txt and nums:
            gross_sales = float(nums[-1])
        if ("VALUE ADDED TAX" in txt or txt.strip().startswith("VAT")) and nums:
            vat = float(nums[-1])
    return {
        "gross_sales": gross_sales,
        "bank_charges": bank_charges,
        "vat": vat,
        "net_settlement": gross_sales - bank_charges - vat,
    }

def detect_reconciliation_columns(df: pd.DataFrame) -> Dict[str, Optional[int]]:
    return {
        "datetime": find_best_column(df, [1,0,2,3,4], score_datetime),
        "seq": 3 if 3 in df.columns else None,
        "node": 7 if 7 in df.columns else None,
        "user": 11 if 11 in df.columns else None,
        "amount": find_best_column(df, [20,19,18,21,22,17,16,23,24], score_amount),
        "brand": find_best_column(df, [27,26,28,25,29,30], score_brand),
        "last4": find_best_column(df, [32,31,33,34,30,29,35], score_last4),
        "processor_ref": 38 if 38 in df.columns else None,
        "auth": find_best_column(df, [40,39,41,38,42,37], score_auth),
        "auth_status": 42 if 42 in df.columns else None,
        "response": 47 if 47 in df.columns else None,
    }

def detect_statement_columns(df: pd.DataFrame) -> Dict[str, Optional[int]]:
    return {
        "purchase_date": find_best_column(df, [0,1,2], score_datetime),
        "brand": find_best_column(df, [1,2,3], score_brand),
        "card_masked": find_best_column(df, [3,2,4,5], score_last4),
        "auth": find_best_column(df, [4,5,6], score_auth),
        "trans_type": 7 if 7 in df.columns else (6 if 6 in df.columns else None),
        "amount": find_best_column(df, [9,8,10,7,11], score_amount),
    }

def load_reconciliation(uploaded_file) -> pd.DataFrame:
    raw = pd.read_excel(uploaded_file, sheet_name=0, header=None, engine="openpyxl")
    df = raw.iloc[7:].copy()
    df = df[df.notna().any(axis=1)].copy()
    cols = detect_reconciliation_columns(df)
    if cols["datetime"] is None:
        raise ValueError("Could not locate the reconciliation transaction datetime column.")
    if cols["amount"] is None:
        raise ValueError("Could not locate the reconciliation amount column. This export layout differs from the original one.")
    if cols["brand"] is None:
        raise ValueError("Could not locate the reconciliation card brand column.")
    if cols["auth"] is None:
        raise ValueError("Could not locate the reconciliation authorization code column.")

    dt = pd.to_datetime(df[cols["datetime"]], errors="coerce")
    df = df[dt.notna()].copy()
    dt = pd.to_datetime(df[cols["datetime"]], errors="coerce")
    out = pd.DataFrame({
        "Source_Row": df.index + 1,
        "DateTime": dt,
        "Date": dt.dt.date,
        "Time": dt.dt.time,
        "Seq": df[cols["seq"]].astype(str) if cols["seq"] in df.columns else "",
        "Node": df[cols["node"]].astype(str) if cols["node"] in df.columns else "",
        "User": df[cols["user"]].astype(str) if cols["user"] in df.columns else "",
        "Amount": df[cols["amount"]].map(parse_amount_text),
        "Amount_Raw": df[cols["amount"]].astype(str),
        "Card_Brand": df[cols["brand"]].map(normalize_brand),
        "Last4": df[cols["last4"]].map(extract_last4) if cols["last4"] in df.columns else "",
        "Auth_Code": df[cols["auth"]].astype(str),
        "Auth_Norm": df[cols["auth"]].map(normalize_auth),
        "Authorization_Status": df[cols["auth_status"]].astype(str) if cols["auth_status"] in df.columns else "",
        "Processor_Ref": df[cols["processor_ref"]].astype(str) if cols["processor_ref"] in df.columns else "",
        "Response": df[cols["response"]].fillna("").astype(str) if cols["response"] in df.columns else "",
    })
    out = out[out["Amount"].notna()].copy()
    out["In_Statement_Scope"] = np.where(out["Card_Brand"].isin(["VISA","MASTERCARD"]), "Y", "N")
    status_text = (out["Authorization_Status"].fillna("") + " " + out["Response"].fillna("")).str.upper()
    out["Is_Decline"] = status_text.str.contains("DECLIN|DENIED|ERROR|FAILED|REJECT", na=False)
    out["Duplicate_Key"] = (
        out["DateTime"].astype(str) + "|" + out["Card_Brand"].astype(str) + "|" +
        out["Last4"].astype(str) + "|" + out["Auth_Norm"].astype(str) + "|" +
        out["Amount"].round(2).astype(str)
    )
    out["Is_Duplicate"] = out.duplicated("Duplicate_Key", keep=False)
    return out.reset_index(drop=True)

def load_statement(uploaded_file):
    raw = pd.read_excel(uploaded_file, sheet_name=0, header=None, engine="openpyxl")
    summary = parse_statement_summary(raw)
    df = raw.iloc[7:].copy()
    df = df[df.notna().any(axis=1)].copy()
    cols = detect_statement_columns(df)
    if cols["purchase_date"] is None:
        raise ValueError("Could not locate the merchant statement purchase date column.")
    if cols["amount"] is None:
        raise ValueError("Could not locate the merchant statement amount column.")
    if cols["brand"] is None:
        raise ValueError("Could not locate the merchant statement card brand column.")
    if cols["auth"] is None:
        raise ValueError("Could not locate the merchant statement authorization code column.")

    purchase_date = pd.to_datetime(df[cols["purchase_date"]], errors="coerce")
    df = df[purchase_date.notna()].copy()
    purchase_date = pd.to_datetime(df[cols["purchase_date"]], errors="coerce")
    trans_type = df[cols["trans_type"]].astype(str).str.strip() if cols["trans_type"] in df.columns else "PURCHASE"

    out = pd.DataFrame({
        "Source_Row": df.index + 1,
        "Purchase_Date": purchase_date.dt.date,
        "Card_Brand": df[cols["brand"]].map(normalize_brand),
        "Last4": df[cols["card_masked"]].map(extract_last4) if cols["card_masked"] in df.columns else "",
        "Card_Number_Masked": df[cols["card_masked"]].astype(str) if cols["card_masked"] in df.columns else "",
        "Auth_Code": df[cols["auth"]].astype(str),
        "Auth_Norm": df[cols["auth"]].map(normalize_auth),
        "Amount": df[cols["amount"]].map(parse_amount_text),
        "Trans_Type": trans_type,
    })
    out = out[out["Amount"].notna()].copy()
    out = out[out["Trans_Type"].str.upper().eq("PURCHASE") | out["Trans_Type"].eq("")].copy()
    out["Duplicate_Key"] = (
        out["Purchase_Date"].astype(str) + "|" + out["Card_Brand"].astype(str) + "|" +
        out["Last4"].astype(str) + "|" + out["Auth_Norm"].astype(str) + "|" +
        out["Amount"].round(2).astype(str)
    )
    out["Is_Duplicate"] = out.duplicated("Duplicate_Key", keep=False)
    return out.reset_index(drop=True), summary

def match_transactions(rec: pd.DataFrame, stmt: pd.DataFrame):
    rec = rec.copy(); stmt = stmt.copy()
    rec["Matched"] = False; stmt["Matched"] = False
    matches = []

    for rec_idx, r in rec[(rec["In_Statement_Scope"] == "Y") & (~rec["Is_Decline"])].iterrows():
        possible = stmt[
            (~stmt["Matched"]) &
            (stmt["Purchase_Date"] == r["Date"]) &
            (stmt["Auth_Norm"] == r["Auth_Norm"]) &
            (stmt["Amount"].round(2) == round(r["Amount"], 2))
        ]
        if not possible.empty:
            stmt_idx = possible.index[0]
            rec.loc[rec_idx, "Matched"] = True
            stmt.loc[stmt_idx, "Matched"] = True
            matches.append({
                "Match_Type": "Same-day auth match",
                "Rec_Source_Row": int(r["Source_Row"]),
                "Merchant_Source_Row": int(stmt.loc[stmt_idx, "Source_Row"]),
                "Rec_DateTime": r["DateTime"],
                "Merchant_Purchase_Date": stmt.loc[stmt_idx, "Purchase_Date"],
                "Amount": float(r["Amount"]),
                "Card_Brand": r["Card_Brand"],
                "Last4": r["Last4"],
                "Rec_Auth_Code": r["Auth_Code"],
                "Merchant_Auth_Code": stmt.loc[stmt_idx, "Auth_Code"],
            })

    for rec_idx, r in rec[(rec["In_Statement_Scope"] == "Y") & (~rec["Is_Decline"]) & (~rec["Matched"])].iterrows():
        next_date = (pd.Timestamp(r["Date"]) + pd.Timedelta(days=1)).date()
        possible = stmt[
            (~stmt["Matched"]) &
            (stmt["Purchase_Date"] == next_date) &
            (stmt["Auth_Norm"] == r["Auth_Norm"]) &
            (stmt["Amount"].round(2) == round(r["Amount"], 2))
        ]
        if not possible.empty:
            stmt_idx = possible.index[0]
            rec.loc[rec_idx, "Matched"] = True
            stmt.loc[stmt_idx, "Matched"] = True
            matches.append({
                "Match_Type": "Next-day auth match",
                "Rec_Source_Row": int(r["Source_Row"]),
                "Merchant_Source_Row": int(stmt.loc[stmt_idx, "Source_Row"]),
                "Rec_DateTime": r["DateTime"],
                "Merchant_Purchase_Date": stmt.loc[stmt_idx, "Purchase_Date"],
                "Amount": float(r["Amount"]),
                "Card_Brand": r["Card_Brand"],
                "Last4": r["Last4"],
                "Rec_Auth_Code": r["Auth_Code"],
                "Merchant_Auth_Code": stmt.loc[stmt_idx, "Auth_Code"],
            })

    matched = pd.DataFrame(matches)
    rec_unmatched = rec[~rec["Matched"]].copy()
    rec_unmatched["Exception_Type"] = np.where(
        rec_unmatched["In_Statement_Scope"] == "Y",
        "In-scope but no match on provided statement",
        "Out of statement scope (non-Visa/MC)",
    )
    stmt_unmatched = stmt[~stmt["Matched"]].copy()
    base_date = rec["Date"].min() if len(rec) else None
    stmt_unmatched["Exception_Type"] = np.where(
        stmt_unmatched["Purchase_Date"] == base_date,
        "No match in provided online log",
        "Outside reconciliation date",
    )
    return matched, rec_unmatched, stmt_unmatched

def build_summary(rec, stmt, merchant_summary, matched, rec_unmatched, stmt_unmatched):
    rec_scope = rec[rec["In_Statement_Scope"] == "Y"]
    same_day = matched[matched["Match_Type"].eq("Same-day auth match")] if len(matched) else matched
    next_day = matched[matched["Match_Type"].eq("Next-day auth match")] if len(matched) else matched
    rec_scope_unmatched = rec_unmatched[rec_unmatched["In_Statement_Scope"] == "Y"]
    rec_out_scope_unmatched = rec_unmatched[rec_unmatched["In_Statement_Scope"] == "N"]
    base_date = rec["Date"].min() if len(rec) else None
    stmt_same = stmt_unmatched[stmt_unmatched["Purchase_Date"] == base_date]
    stmt_other = stmt_unmatched[stmt_unmatched["Purchase_Date"] != base_date]
    return SummaryMetrics(
        gross_sales=safe_float(merchant_summary.get("gross_sales")),
        bank_charges=safe_float(merchant_summary.get("bank_charges")),
        vat=safe_float(merchant_summary.get("vat")),
        net_settlement=safe_float(merchant_summary.get("net_settlement")),
        rec_total_count=int(len(rec)),
        rec_total_amount=float(rec["Amount"].fillna(0).sum()),
        rec_scope_count=int(len(rec_scope)),
        rec_scope_amount=float(rec_scope["Amount"].fillna(0).sum()),
        stmt_total_count=int(len(stmt)),
        stmt_total_amount=float(stmt["Amount"].fillna(0).sum()),
        matched_count=int(len(matched)),
        matched_amount=float(matched["Amount"].fillna(0).sum() if len(matched) else 0),
        same_day_count=int(len(same_day)),
        same_day_amount=float(same_day["Amount"].fillna(0).sum() if len(same_day) else 0),
        next_day_count=int(len(next_day)),
        next_day_amount=float(next_day["Amount"].fillna(0).sum() if len(next_day) else 0),
        rec_unmatched_scope_count=int(len(rec_scope_unmatched)),
        rec_unmatched_scope_amount=float(rec_scope_unmatched["Amount"].fillna(0).sum()),
        rec_unmatched_out_scope_count=int(len(rec_out_scope_unmatched)),
        rec_unmatched_out_scope_amount=float(rec_out_scope_unmatched["Amount"].fillna(0).sum()),
        stmt_unmatched_same_date_count=int(len(stmt_same)),
        stmt_unmatched_same_date_amount=float(stmt_same["Amount"].fillna(0).sum()),
        stmt_unmatched_other_date_count=int(len(stmt_other)),
        stmt_unmatched_other_date_amount=float(stmt_other["Amount"].fillna(0).sum()),
        duplicate_count=int(rec["Is_Duplicate"].sum() + stmt["Is_Duplicate"].sum()),
        decline_count=int(rec["Is_Decline"].sum()),
    )

def create_excel_report(rec, stmt, merchant_summary, matched, rec_unmatched, stmt_unmatched):
    output = io.BytesIO()
    summary = build_summary(rec, stmt, merchant_summary, matched, rec_unmatched, stmt_unmatched)
    summary_df = pd.DataFrame([
        ["Clean Reconciliation Report", None, None],
        ["True Net Settlement", summary.net_settlement, None],
        ["Matched", summary.matched_count, summary.matched_amount],
        ["Same-day matches", summary.same_day_count, summary.same_day_amount],
        ["Next-day matches", summary.next_day_count, summary.next_day_amount],
        ["Rec unmatched in scope", summary.rec_unmatched_scope_count, summary.rec_unmatched_scope_amount],
        ["Rec unmatched out of scope", summary.rec_unmatched_out_scope_count, summary.rec_unmatched_out_scope_amount],
        ["Stmt unmatched same date", summary.stmt_unmatched_same_date_count, summary.stmt_unmatched_same_date_amount],
        ["Stmt unmatched other date", summary.stmt_unmatched_other_date_count, summary.stmt_unmatched_other_date_amount],
        ["Declines", summary.decline_count, None],
        ["Duplicates", summary.duplicate_count, None],
    ])
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        summary_df.to_excel(writer, index=False, header=False, sheet_name="Summary")
        rec.to_excel(writer, index=False, sheet_name="Reconciliation_Clean")
        stmt.to_excel(writer, index=False, sheet_name="Merchant_Clean")
        matched.to_excel(writer, index=False, sheet_name="Matched")
        rec_unmatched.to_excel(writer, index=False, sheet_name="Rec_Unmatched")
        stmt_unmatched.to_excel(writer, index=False, sheet_name="Merchant_Unmatched")
    output.seek(0)
    return output.getvalue()

def main():
    st.set_page_config(page_title=APP_TITLE, layout="wide")
    st.title(APP_TITLE)
    c1, c2 = st.columns(2)
    with c1:
        rec_file = st.file_uploader("Reconciliation workbook (.xlsx)", type=["xlsx"], key="rec")
    with c2:
        stmt_file = st.file_uploader("Merchant statement workbook (.xlsx)", type=["xlsx"], key="stmt")
    if rec_file and stmt_file:
        try:
            rec = load_reconciliation(rec_file)
            stmt, merchant_summary = load_statement(stmt_file)
            matched, rec_unmatched, stmt_unmatched = match_transactions(rec, stmt)
            summary = build_summary(rec, stmt, merchant_summary, matched, rec_unmatched, stmt_unmatched)
            a, b, c, d = st.columns(4)
            a.metric("True net settlement", f"{summary.net_settlement:,.2f}")
            b.metric("Matched", f"{summary.matched_count} / {summary.matched_amount:,.2f}")
            c.metric("Rec unmatched in scope", f"{summary.rec_unmatched_scope_count} / {summary.rec_unmatched_scope_amount:,.2f}")
            d.metric("Stmt unmatched same date", f"{summary.stmt_unmatched_same_date_count} / {summary.stmt_unmatched_same_date_amount:,.2f}")
            with st.expander("Detected reconciliation layout"):
                raw = pd.read_excel(rec_file, sheet_name=0, header=None, engine="openpyxl")
                st.json(detect_reconciliation_columns(raw.iloc[7:].copy()))
            with st.expander("Detected statement layout"):
                raw = pd.read_excel(stmt_file, sheet_name=0, header=None, engine="openpyxl")
                st.json(detect_statement_columns(raw.iloc[7:].copy()))
            st.dataframe(matched, use_container_width=True)
            excel_bytes = create_excel_report(rec, stmt, merchant_summary, matched, rec_unmatched, stmt_unmatched)
            st.download_button(
                "Download reconciliation report",
                data=excel_bytes,
                file_name="reconciliation_report.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
        except Exception as exc:
            st.error(f"Could not process the workbooks: {exc}")
            st.exception(exc)
    else:
        st.info("Upload both files to continue.")

if __name__ == "__main__":
    main()
