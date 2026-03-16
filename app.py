
import io
import re
from dataclasses import dataclass
from typing import Optional, List, Dict, Tuple

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


# ----------------------------
# Normalization helpers
# ----------------------------
def parse_amount_text(v):
    if pd.isna(v):
        return np.nan
    if isinstance(v, (int, float, np.integer, np.floating)):
        return abs(float(v))
    s = str(v).strip().upper().replace(",", "")
    m = re.search(r"(-?\d+(?:\.\d+)?)", s)
    return abs(float(m.group(1))) if m else np.nan


def normalize_brand(v: str) -> str:
    if pd.isna(v):
        return ""
    s = str(v).strip().upper()
    mapping = {
        "V": "VISA",
        "VISA": "VISA",
        "M": "MASTERCARD",
        "MC": "MASTERCARD",
        "MASTERCARD": "MASTERCARD",
        "MASTERCARD": "MASTERCARD",
        "A": "AMEX",
        "AMEX": "AMEX",
        "AMERICAN EXPRESS": "AMEX",
        "OTHER": "OTHER",
    }
    return mapping.get(s, s or "")


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
    s = str(v).strip().upper()
    s = re.sub(r"[^A-Z0-9]", "", s)
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


def is_probably_datetime(series: pd.Series) -> float:
    return float(pd.to_datetime(series, errors="coerce").notna().mean())


def is_probably_amount(series: pd.Series) -> float:
    return float(series.map(parse_amount_text).notna().mean())


def is_probably_brand(series: pd.Series) -> float:
    s = series.astype(str).str.upper().str.strip()
    valid = s.isin(["V", "VISA", "M", "MC", "MASTERCARD", "MASTERCARD", "A", "AMEX", "AMERICAN EXPRESS", "OTHER"])
    return float(valid.mean())


def is_probably_last4(series: pd.Series) -> float:
    return float((series.map(extract_last4).str.len() == 4).mean())


def is_probably_auth(series: pd.Series) -> float:
    return float((series.map(normalize_auth) != "").mean())


def normalize_header_text(v) -> str:
    if pd.isna(v):
        return ""
    return re.sub(r"\s+", " ", str(v).replace("\n", " ").strip()).lower()


def index_to_excel_col(idx: int) -> str:
    # 0 -> A
    out = ""
    idx += 1
    while idx:
        idx, rem = divmod(idx - 1, 26)
        out = chr(65 + rem) + out
    return out


# ----------------------------
# Header / layout detection
# ----------------------------
def detect_reconciliation_header_row(raw: pd.DataFrame) -> int:
    best_row = 5
    best_score = -1
    for i in range(min(25, len(raw))):
        vals = [normalize_header_text(x) for x in raw.iloc[i].tolist()]
        rowtxt = " | ".join(vals)
        score = 0
        if "date/time" in rowtxt:
            score += 3
        if "payment/refund total" in rowtxt:
            score += 3
        if "card type" in rowtxt:
            score += 2
        if "card number" in rowtxt:
            score += 2
        if "authorization status" in rowtxt:
            score += 2
        if "response" in rowtxt:
            score += 1
        if score > best_score:
            best_score = score
            best_row = i
    return best_row


def detect_statement_start_row(raw: pd.DataFrame) -> int:
    # Usually detailed lines begin after the "Settlement Date / Posting Date" section.
    for i in range(min(40, len(raw))):
        rowtxt = " | ".join([normalize_header_text(x) for x in raw.iloc[i].tolist()])
        if "settlement date:" in rowtxt and "posting date:" in rowtxt:
            return i + 1
    # fallback
    for i in range(min(25, len(raw))):
        score = is_probably_datetime(raw.iloc[i:, 0].head(20).astype(str)) if 0 in raw.columns else 0
        if score > 0.7:
            return i
    return 7


def find_named_columns(header_row: pd.Series, synonym_map: Dict[str, List[str]]) -> Dict[str, Optional[int]]:
    headers = [normalize_header_text(v) for v in header_row.tolist()]
    found = {}
    for field, names in synonym_map.items():
        idx = None
        for i, h in enumerate(headers):
            if any(name in h for name in names):
                idx = i
                break
        found[field] = idx
    return found


def find_best_column(df: pd.DataFrame, candidates: List[int], scorer) -> Optional[int]:
    best_col = None
    best_score = -1.0
    for c in candidates:
        if c not in df.columns:
            continue
        sample = df[c].dropna().head(200)
        if sample.empty:
            continue
        score = scorer(sample.astype(str))
        if score > best_score:
            best_score = score
            best_col = c
    return best_col


def detect_reconciliation_columns(raw: pd.DataFrame) -> Tuple[int, Dict[str, Optional[int]]]:
    header_row_idx = detect_reconciliation_header_row(raw)
    header_found = find_named_columns(
        raw.iloc[header_row_idx],
        {
            "datetime": ["date/time", "datetime"],
            "seq": ["seq"],
            "node": ["node"],
            "user": ["user"],
            "amount": ["payment/refund total", "amount", "total"],
            "brand": ["card type"],
            "last4": ["card number"],
            "auth": ["auth", "authorization"],
            "auth_status": ["authorization status"],
            "response": ["response"],
        },
    )

    data = raw.iloc[header_row_idx + 1 :].copy()
    data = data[data.notna().any(axis=1)].copy()

    cols = {
        "datetime": header_found["datetime"] if header_found["datetime"] is not None else find_best_column(data, list(data.columns[:8]), is_probably_datetime),
        "seq": header_found["seq"] if header_found["seq"] is not None else find_best_column(data, list(data.columns[:10]), is_probably_auth),
        "node": header_found["node"],
        "user": header_found["user"],
        "amount": header_found["amount"] if header_found["amount"] is not None else find_best_column(data, list(data.columns), is_probably_amount),
        "brand": header_found["brand"] if header_found["brand"] is not None else find_best_column(data, list(data.columns), is_probably_brand),
        "last4": header_found["last4"] if header_found["last4"] is not None else find_best_column(data, list(data.columns), is_probably_last4),
        "auth": find_best_column(data, [c for c in data.columns if c != header_found.get("amount")], is_probably_auth),
        "auth_status": header_found["auth_status"],
        "response": header_found["response"],
    }
    # If header identified auth column by name, prefer it
    if header_found.get("auth") is not None:
        cols["auth"] = header_found["auth"]
    return header_row_idx, cols


def detect_statement_columns(raw: pd.DataFrame) -> Tuple[int, Dict[str, Optional[int]]]:
    start_row_idx = detect_statement_start_row(raw)
    data = raw.iloc[start_row_idx:].copy()
    data = data[data.notna().any(axis=1)].copy()

    cols = {
        "purchase_date": find_best_column(data, list(data.columns[:4]), is_probably_datetime),
        "brand": find_best_column(data, list(data.columns[:5]), is_probably_brand),
        "card_masked": find_best_column(data, list(data.columns[:7]), is_probably_last4),
        "auth": find_best_column(data, list(data.columns[:8]), is_probably_auth),
        "trans_type": None,
        "amount": find_best_column(data, list(data.columns), is_probably_amount),
    }

    # Try to identify a transaction type column by looking for Purchase
    for c in data.columns:
        sample = data[c].astype(str).str.strip().str.upper().head(200)
        if (sample == "PURCHASE").mean() > 0.25:
            cols["trans_type"] = c
            break

    return start_row_idx, cols


# ----------------------------
# Parsing
# ----------------------------
def parse_statement_summary(raw: pd.DataFrame) -> dict:
    gross_sales = bank_charges = vat = 0.0
    for _, row in raw.head(20).iterrows():
        rowtxt = " | ".join([str(x) for x in row.tolist() if pd.notna(x)]).upper()
        nums = re.findall(r"\d+(?:\.\d+)?", rowtxt)
        if "BANK CHARGES" in rowtxt and nums:
            bank_charges = float(nums[-1])
        if "GROSS SALES" in rowtxt and nums:
            gross_sales = float(nums[-1])
        if ("VALUE ADDED TAX" in rowtxt or rowtxt.strip().startswith("VAT")) and nums:
            vat = float(nums[-1])
    return {
        "gross_sales": gross_sales,
        "bank_charges": bank_charges,
        "vat": vat,
        "net_settlement": gross_sales - bank_charges - vat,
    }


def build_mapping_display(cols: Dict[str, Optional[int]]) -> Dict[str, Optional[str]]:
    out = {}
    for k, v in cols.items():
        out[k] = None if v is None else f"{index_to_excel_col(v)} ({v})"
    return out


def load_reconciliation(uploaded_file) -> Tuple[pd.DataFrame, Dict]:
    raw = pd.read_excel(uploaded_file, sheet_name=0, header=None, engine="openpyxl")
    header_row_idx, cols = detect_reconciliation_columns(raw)

    if cols["datetime"] is None:
        raise ValueError("Could not locate the reconciliation date/time column.")
    if cols["amount"] is None:
        raise ValueError("Could not locate the reconciliation amount column.")
    if cols["brand"] is None:
        raise ValueError("Could not locate the reconciliation card brand column.")
    if cols["auth"] is None:
        raise ValueError("Could not locate the reconciliation authorization code column.")

    df = raw.iloc[header_row_idx + 1 :].copy()
    df = df[df.notna().any(axis=1)].copy()

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
        "Response": df[cols["response"]].astype(str) if cols["response"] in df.columns else "",
    })

    out = out[out["Amount"].notna()].copy()

    status_text = (out["Authorization_Status"].fillna("") + " " + out["Response"].fillna("")).str.upper()
    out["Is_Decline"] = status_text.str.contains("DECLIN|DENIED|ERROR|FAILED|REJECT", na=False)
    out["In_Statement_Scope"] = np.where(out["Card_Brand"].isin(["VISA", "MASTERCARD"]), "Y", "N")
    out["Duplicate_Key"] = (
        out["DateTime"].astype(str)
        + "|" + out["Card_Brand"].astype(str)
        + "|" + out["Last4"].astype(str)
        + "|" + out["Auth_Norm"].astype(str)
        + "|" + out["Amount"].round(2).astype(str)
    )
    out["Is_Duplicate"] = out.duplicated("Duplicate_Key", keep=False)

    meta = {
        "header_row_idx": header_row_idx,
        "detected_columns": cols,
        "display_columns": build_mapping_display(cols),
    }
    return out.reset_index(drop=True), meta


def load_statement(uploaded_file) -> Tuple[pd.DataFrame, dict, Dict]:
    raw = pd.read_excel(uploaded_file, sheet_name=0, header=None, engine="openpyxl")
    summary = parse_statement_summary(raw)
    start_row_idx, cols = detect_statement_columns(raw)

    if cols["purchase_date"] is None:
        raise ValueError("Could not locate the merchant statement purchase date column.")
    if cols["amount"] is None:
        raise ValueError("Could not locate the merchant statement amount column.")
    if cols["brand"] is None:
        raise ValueError("Could not locate the merchant statement card brand column.")
    if cols["auth"] is None:
        raise ValueError("Could not locate the merchant statement authorization code column.")

    df = raw.iloc[start_row_idx:].copy()
    df = df[df.notna().any(axis=1)].copy()

    purchase_date = pd.to_datetime(df[cols["purchase_date"]], errors="coerce")
    df = df[purchase_date.notna()].copy()
    purchase_date = pd.to_datetime(df[cols["purchase_date"]], errors="coerce")

    trans_type = df[cols["trans_type"]].astype(str).str.strip() if cols["trans_type"] in df.columns else pd.Series(["Purchase"] * len(df), index=df.index)

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
        out["Purchase_Date"].astype(str)
        + "|" + out["Card_Brand"].astype(str)
        + "|" + out["Last4"].astype(str)
        + "|" + out["Auth_Norm"].astype(str)
        + "|" + out["Amount"].round(2).astype(str)
    )
    out["Is_Duplicate"] = out.duplicated("Duplicate_Key", keep=False)

    meta = {
        "start_row_idx": start_row_idx,
        "detected_columns": cols,
        "display_columns": build_mapping_display(cols),
    }
    return out.reset_index(drop=True), summary, meta


# ----------------------------
# Matching logic
# ----------------------------
def match_transactions(rec: pd.DataFrame, stmt: pd.DataFrame):
    """
    This intentionally follows the same core logic used for the Feb 9 report:
    1. same-day exact match on normalized auth code + amount
    2. next-day exact match on normalized auth code + amount
    No fuzzy matching is applied.
    """
    rec = rec.copy()
    stmt = stmt.copy()
    rec["Matched"] = False
    stmt["Matched"] = False

    matches = []
    eligible_rec = rec[(rec["In_Statement_Scope"] == "Y") & (~rec["Is_Decline"])].copy()

    # Pass 1: same-day exact auth + amount
    for rec_idx, r in eligible_rec.iterrows():
        possible = stmt[
            (~stmt["Matched"])
            & (stmt["Purchase_Date"] == r["Date"])
            & (stmt["Auth_Norm"] == r["Auth_Norm"])
            & (stmt["Amount"].round(2) == round(r["Amount"], 2))
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
                "Rec_Seq": r["Seq"],
                "Rec_Node": r["Node"],
                "Rec_User": r["User"],
                "Rec_Status": r["Authorization_Status"],
                "Notes": "Matched on purchase date + normalized auth code + amount.",
            })

    # Pass 2: next-day exact auth + amount
    for rec_idx, r in rec[(rec["In_Statement_Scope"] == "Y") & (~rec["Is_Decline"]) & (~rec["Matched"])].iterrows():
        next_date = (pd.Timestamp(r["Date"]) + pd.Timedelta(days=1)).date()
        possible = stmt[
            (~stmt["Matched"])
            & (stmt["Purchase_Date"] == next_date)
            & (stmt["Auth_Norm"] == r["Auth_Norm"])
            & (stmt["Amount"].round(2) == round(r["Amount"], 2))
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
                "Rec_Seq": r["Seq"],
                "Rec_Node": r["Node"],
                "Rec_User": r["User"],
                "Rec_Status": r["Authorization_Status"],
                "Notes": "Matched on next-day purchase date + normalized auth code + amount.",
            })

    matched = pd.DataFrame(matches)

    rec_unmatched = rec[~rec["Matched"]].copy()
    rec_unmatched["Exception_Type"] = np.where(
        rec_unmatched["In_Statement_Scope"] == "Y",
        "In-scope but no match on provided statement",
        "Out of statement scope (non-Visa/MC)",
    )
    rec_unmatched["Notes"] = np.where(
        rec_unmatched["In_Statement_Scope"] == "Y",
        "Visa/MC transaction in online log with no matching statement line.",
        "Non-Visa/MC card brand; merchant statement is labeled VISA/MC.",
    )
    rec_unmatched["Y/N Scope"] = rec_unmatched["In_Statement_Scope"]

    stmt_unmatched = stmt[~stmt["Matched"]].copy()
    rec_dates = set(rec["Date"].dropna().tolist())
    stmt_unmatched["Exception_Type"] = np.where(
        stmt_unmatched["Purchase_Date"].isin(rec_dates),
        "No match in provided online log",
        "Outside reconciliation date range",
    )
    stmt_unmatched["Notes"] = np.where(
        stmt_unmatched["Purchase_Date"].isin(rec_dates),
        "Statement transaction has no matching record in the provided online log.",
        "Statement line is outside the reconciliation workbook date range.",
    )

    return matched, rec_unmatched, stmt_unmatched


def build_summary(rec, stmt, merchant_summary, matched, rec_unmatched, stmt_unmatched):
    rec_scope = rec[rec["In_Statement_Scope"] == "Y"]
    same_day = matched[matched["Match_Type"].eq("Same-day auth match")] if len(matched) else matched
    next_day = matched[matched["Match_Type"].eq("Next-day auth match")] if len(matched) else matched
    rec_scope_unmatched = rec_unmatched[rec_unmatched["In_Statement_Scope"] == "Y"]
    rec_out_scope_unmatched = rec_unmatched[rec_unmatched["In_Statement_Scope"] == "N"]
    rec_dates = set(rec["Date"].dropna().tolist())
    stmt_same = stmt_unmatched[stmt_unmatched["Purchase_Date"].isin(rec_dates)]
    stmt_other = stmt_unmatched[~stmt_unmatched["Purchase_Date"].isin(rec_dates)]

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


# ----------------------------
# Report export
# ----------------------------
def create_excel_report(rec, stmt, merchant_summary, matched, rec_unmatched, stmt_unmatched, rec_meta, stmt_meta):
    output = io.BytesIO()
    summary = build_summary(rec, stmt, merchant_summary, matched, rec_unmatched, stmt_unmatched)

    summary_rows = [
        ["Clean Reconciliation Report", None, None, None, None, None],
        ["Generated by Streamlit app", None, None, None, None, None],
        [None, None, None, None, None, None],
        ["Source Totals", "Count", "Amount", None, "Statement Settlement", "Amount"],
        ["Reconciliation file (all brands)", summary.rec_total_count, summary.rec_total_amount, None, "Gross Sales (VISA/MC)", summary.gross_sales],
        ["Reconciliation file (VISA/MC scope)", summary.rec_scope_count, summary.rec_scope_amount, None, "Bank Charges", summary.bank_charges],
        ["Merchant statement transactions", summary.stmt_total_count, summary.stmt_total_amount, None, "VAT", summary.vat],
        [None, None, None, None, "True Net Settlement", summary.net_settlement],
        [None, None, None, None, None, None],
        ["Automatic Matching Results", "Count", "Amount", None, "Exceptions", "Count / Amount"],
        ["Matched transactions", summary.matched_count, summary.matched_amount, None, "Rec unmatched in scope", f"{summary.rec_unmatched_scope_count} / {summary.rec_unmatched_scope_amount:,.2f}"],
        ["Same-day matches", summary.same_day_count, summary.same_day_amount, None, "Rec unmatched out of scope", f"{summary.rec_unmatched_out_scope_count} / {summary.rec_unmatched_out_scope_amount:,.2f}"],
        ["Next-day matches", summary.next_day_count, summary.next_day_amount, None, "Stmt unmatched within rec dates", f"{summary.stmt_unmatched_same_date_count} / {summary.stmt_unmatched_same_date_amount:,.2f}"],
        [None, None, None, None, "Stmt unmatched outside rec dates", f"{summary.stmt_unmatched_other_date_count} / {summary.stmt_unmatched_other_date_amount:,.2f}"],
        [None, None, None, None, "Declines found", summary.decline_count],
        [None, None, None, None, "Duplicates found", summary.duplicate_count],
    ]
    summary_df = pd.DataFrame(summary_rows)

    statement_summary_df = pd.DataFrame([
        ["Merchant Statement Summary", None],
        [None, None],
        ["Metric", "Amount"],
        ["Bank Charges", summary.bank_charges],
        ["Gross Sales: VISA/MC", summary.gross_sales],
        ["Total Value Added Taxes", summary.vat],
        ["True Net Settlement", summary.net_settlement],
    ])

    mapping_df = pd.DataFrame({
        "Workbook": ["Reconciliation"] * len(rec_meta["display_columns"]) + ["Merchant Statement"] * len(stmt_meta["display_columns"]),
        "Field": list(rec_meta["display_columns"].keys()) + list(stmt_meta["display_columns"].keys()),
        "Detected Column": list(rec_meta["display_columns"].values()) + list(stmt_meta["display_columns"].values()),
    })

    exceptions_df = pd.DataFrame([
        ["Exceptions Review", None, None, None],
        [None, None, None, None],
        ["Check", "Count", "Amount", "Comment"],
        ["Reconciliation declines", int(rec["Is_Decline"].sum()), float(rec.loc[rec["Is_Decline"], "Amount"].fillna(0).sum()), "No declines found." if int(rec["Is_Decline"].sum()) == 0 else "Review declined items."],
        ["Reconciliation exact duplicates", int(rec["Is_Duplicate"].sum()), float(rec.loc[rec["Is_Duplicate"], "Amount"].fillna(0).sum()), "No exact duplicates found." if int(rec["Is_Duplicate"].sum()) == 0 else "Review duplicate online-log items."],
        ["Merchant exact duplicates", int(stmt["Is_Duplicate"].sum()), float(stmt.loc[stmt["Is_Duplicate"], "Amount"].fillna(0).sum()), "No exact duplicates found." if int(stmt["Is_Duplicate"].sum()) == 0 else "Review duplicate statement items."],
        ["Rec non-Visa/MC items", int((rec["In_Statement_Scope"] == "N").sum()), float(rec.loc[rec["In_Statement_Scope"] == "N", "Amount"].fillna(0).sum()), "These items are outside the VISA/MC statement scope (AMEX / Other)."],
    ])

    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        summary_df.to_excel(writer, index=False, header=False, sheet_name="Summary")
        statement_summary_df.to_excel(writer, index=False, header=False, sheet_name="Statement_Summary")
        mapping_df.to_excel(writer, index=False, sheet_name="Detected_Mapping")
        rec.to_excel(writer, index=False, sheet_name="Reconciliation_Clean")
        stmt.to_excel(writer, index=False, sheet_name="Merchant_Clean")
        matched.to_excel(writer, index=False, sheet_name="Matched")
        rec_unmatched.to_excel(writer, index=False, sheet_name="Rec_Unmatched")
        stmt_unmatched.to_excel(writer, index=False, sheet_name="Merchant_Unmatched")
        exceptions_df.to_excel(writer, index=False, header=False, sheet_name="Exceptions")

        wb = writer.book
        for ws in wb.worksheets:
            ws.freeze_panes = "A2"
            for col_cells in ws.columns:
                letter = col_cells[0].column_letter
                max_len = 0
                for cell in col_cells:
                    value = "" if cell.value is None else str(cell.value)
                    max_len = max(max_len, len(value))
                ws.column_dimensions[letter].width = min(max(max_len + 2, 12), 38)

    output.seek(0)
    return output.getvalue()


# ----------------------------
# UI
# ----------------------------
def main():
    st.set_page_config(page_title=APP_TITLE, layout="wide")
    st.title(APP_TITLE)
    st.write(
        "This version keeps the same Feb 9 matching logic, but separates it from workbook layout detection "
        "so it can be applied to other reconciliation and merchant statement pairs."
    )

    col1, col2 = st.columns(2)
    with col1:
        rec_file = st.file_uploader("Reconciliation workbook (.xlsx)", type=["xlsx"], key="rec")
    with col2:
        stmt_file = st.file_uploader("Merchant statement workbook (.xlsx)", type=["xlsx"], key="stmt")

    if rec_file and stmt_file:
        try:
            rec, rec_meta = load_reconciliation(rec_file)
            stmt, merchant_summary, stmt_meta = load_statement(stmt_file)
            matched, rec_unmatched, stmt_unmatched = match_transactions(rec, stmt)
            summary = build_summary(rec, stmt, merchant_summary, matched, rec_unmatched, stmt_unmatched)

            a, b, c, d = st.columns(4)
            a.metric("True net settlement", f"{summary.net_settlement:,.2f}")
            b.metric("Matched", f"{summary.matched_count} / {summary.matched_amount:,.2f}")
            c.metric("Rec unmatched in scope", f"{summary.rec_unmatched_scope_count} / {summary.rec_unmatched_scope_amount:,.2f}")
            d.metric("Stmt unmatched within rec dates", f"{summary.stmt_unmatched_same_date_count} / {summary.stmt_unmatched_same_date_amount:,.2f}")

            with st.expander("Detected reconciliation layout", expanded=False):
                st.write({"header_row": rec_meta["header_row_idx"] + 1, "columns": rec_meta["display_columns"]})
            with st.expander("Detected merchant statement layout", expanded=False):
                st.write({"data_start_row": stmt_meta["start_row_idx"] + 1, "columns": stmt_meta["display_columns"]})

            st.subheader("Matched transactions")
            st.dataframe(matched, use_container_width=True)

            left, right = st.columns(2)
            with left:
                st.subheader("Reconciliation unmatched")
                st.dataframe(rec_unmatched, use_container_width=True)
            with right:
                st.subheader("Merchant unmatched")
                st.dataframe(stmt_unmatched, use_container_width=True)

            excel_bytes = create_excel_report(rec, stmt, merchant_summary, matched, rec_unmatched, stmt_unmatched, rec_meta, stmt_meta)
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
