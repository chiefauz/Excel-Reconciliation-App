
import io
import re
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import streamlit as st


APP_TITLE = "Excel Reconciliation App v5"


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
    stmt_unmatched_within_rec_count: int
    stmt_unmatched_within_rec_amount: float
    stmt_unmatched_outside_rec_count: int
    stmt_unmatched_outside_rec_amount: float
    duplicate_count: int
    decline_count: int


# -----------------------------
# Basic normalization helpers
# -----------------------------
def normalize_text(v) -> str:
    if pd.isna(v):
        return ""
    return re.sub(r"\s+", " ", str(v).replace("\n", " ").strip()).lower()


def parse_amount_text(v):
    if pd.isna(v):
        return np.nan
    if isinstance(v, (int, float, np.integer, np.floating)):
        return abs(float(v))
    s = str(v).strip().upper().replace(",", "")
    m = re.search(r"(-?\d+(?:\.\d+)?)", s)
    if not m:
        return np.nan
    return abs(float(m.group(1)))


def normalize_brand(v: str) -> str:
    s = normalize_text(v).upper()
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
    return mapping.get(s, s if s else "")


def extract_last4(v) -> str:
    if pd.isna(v):
        return ""
    s = str(v)
    m = re.search(r"(\d{4})\s*$", s)
    if m:
        return m.group(1)
    digits = re.findall(r"\d", s)
    return "".join(digits[-4:]) if len(digits) >= 4 else ""


def normalize_auth(v) -> str:
    if pd.isna(v):
        return ""
    s = re.sub(r"[^A-Z0-9]", "", str(v).strip().upper())
    if s in {"", "NAN", "NONE", "NULL", "-", "NA"}:
        return ""
    stripped = s.lstrip("0")
    return stripped if stripped else s


def excel_col(idx: int) -> str:
    n = idx + 1
    out = ""
    while n:
        n, rem = divmod(n - 1, 26)
        out = chr(65 + rem) + out
    return out


# -----------------------------
# Scoring helpers
# -----------------------------
def score_datetime(series: pd.Series) -> float:
    return float(pd.to_datetime(series, errors="coerce").notna().mean())


def score_amount(series: pd.Series) -> float:
    return float(series.map(parse_amount_text).notna().mean())


def score_brand(series: pd.Series) -> float:
    s = series.astype(str).str.upper().str.strip()
    valid = s.isin(["V", "VISA", "M", "MC", "MASTERCARD", "MASTERCARD", "A", "AMEX", "AMERICAN EXPRESS", "OTHER"])
    return float(valid.mean())


def score_last4(series: pd.Series) -> float:
    vals = series.map(extract_last4)
    return float((vals.str.len() == 4).mean())


def score_auth(series: pd.Series) -> float:
    vals = series.map(normalize_auth)
    return float((vals != "").mean())


def score_status(series: pd.Series) -> float:
    s = series.astype(str).str.upper()
    return float(s.str.contains("APPROV|DECLIN|DENIED|FAILED|ERROR|REJECT|\[0\]", regex=True, na=False).mean())


def best_column_in_window(df: pd.DataFrame, anchor: int, scorer, radius: int = 6, exclude: Optional[List[int]] = None) -> Optional[int]:
    exclude = exclude or []
    candidates = []
    for c in df.columns:
        if c in exclude:
            continue
        if abs(int(c) - int(anchor)) <= radius:
            sample = df[c].dropna().head(250)
            if sample.empty:
                continue
            score = scorer(sample.astype(str))
            candidates.append((score, c))
    if not candidates:
        return None
    candidates.sort(reverse=True)
    return int(candidates[0][1])


def best_column_anywhere(df: pd.DataFrame, scorer, exclude: Optional[List[int]] = None) -> Optional[int]:
    exclude = exclude or []
    candidates = []
    for c in df.columns:
        if c in exclude:
            continue
        sample = df[c].dropna().head(250)
        if sample.empty:
            continue
        score = scorer(sample.astype(str))
        candidates.append((score, c))
    if not candidates:
        return None
    candidates.sort(reverse=True)
    return int(candidates[0][1])


# -----------------------------
# Reconciliation parsing
# -----------------------------
REC_HEADER_NAMES = {
    "datetime": ["date/time", "datetime"],
    "seq": ["seq"],
    "node": ["node"],
    "user": ["user"],
    "amount": ["payment/refund total"],
    "card_type": ["card type"],
    "card_number": ["card number"],
    "auth_status": ["authorization status"],
    "response": ["response"],
}


def detect_reconciliation_header_row(raw: pd.DataFrame) -> int:
    best_row = 0
    best_score = -1
    for i in range(min(30, len(raw))):
        vals = [normalize_text(x) for x in raw.iloc[i].tolist()]
        rowtxt = " | ".join(vals)
        score = 0
        if "date/time" in rowtxt:
            score += 4
        if "payment/refund total" in rowtxt:
            score += 4
        if "card type" in rowtxt:
            score += 3
        if "card number" in rowtxt:
            score += 3
        if "authorization status" in rowtxt:
            score += 2
        if "response" in rowtxt:
            score += 1
        if score > best_score:
            best_score = score
            best_row = i
    return best_row


def header_positions(row: pd.Series, names_map: Dict[str, List[str]]) -> Dict[str, Optional[int]]:
    headers = [normalize_text(x) for x in row.tolist()]
    out = {}
    for field, names in names_map.items():
        idx = None
        for i, h in enumerate(headers):
            if any(name in h for name in names):
                idx = i
                break
        out[field] = idx
    return out


def load_reconciliation(uploaded_file) -> Tuple[pd.DataFrame, Dict]:
    raw = pd.read_excel(uploaded_file, sheet_name=0, header=None, engine="openpyxl")
    header_row = detect_reconciliation_header_row(raw)
    hdr = header_positions(raw.iloc[header_row], REC_HEADER_NAMES)

    data = raw.iloc[header_row + 1 :].copy()
    data = data[data.notna().any(axis=1)].copy()

    # Use header anchors, but choose actual data columns near those anchors.
    datetime_col = best_column_in_window(data, hdr["datetime"] if hdr["datetime"] is not None else 1, score_datetime, radius=3)
    seq_col = best_column_in_window(data, hdr["seq"] if hdr["seq"] is not None else 3, score_auth, radius=2)
    node_col = best_column_in_window(data, hdr["node"] if hdr["node"] is not None else 7, lambda s: float((s.astype(str).str.len().between(2, 6)).mean()), radius=2)
    user_col = best_column_in_window(data, hdr["user"] if hdr["user"] is not None else 11, lambda s: float((s.astype(str).str.len().between(2, 8)).mean()), radius=2)
    amount_col = best_column_in_window(data, hdr["amount"] if hdr["amount"] is not None else 21, score_amount, radius=4)
    brand_col = best_column_in_window(data, hdr["card_type"] if hdr["card_type"] is not None else 31, score_brand, radius=5)
    card_col = best_column_in_window(data, hdr["card_number"] if hdr["card_number"] is not None else 33, score_last4, radius=5)

    # Auth code is usually just before the status columns, and not the long processor reference.
    auth_candidates = []
    for c in data.columns:
        if amount_col is not None and c == amount_col:
            continue
        if abs(int(c) - int(hdr["auth_status"] if hdr["auth_status"] is not None else 45)) <= 8:
            sample = data[c].dropna().head(250).astype(str)
            if sample.empty:
                continue
            score = score_auth(sample)
            avg_len = sample.map(lambda x: len(normalize_auth(x))).mean() if len(sample) else 0
            # Prefer short auth codes, not processor refs
            bonus = 0.2 if avg_len <= 8 else 0.0
            penalty = -0.4 if avg_len > 10 else 0.0
            auth_candidates.append((score + bonus + penalty, int(c)))
    auth_col = sorted(auth_candidates, reverse=True)[0][1] if auth_candidates else best_column_anywhere(data, score_auth)

    auth_status_col = best_column_in_window(data, hdr["auth_status"] if hdr["auth_status"] is not None else 45, score_status, radius=4, exclude=[auth_col] if auth_col is not None else [])
    response_col = best_column_in_window(data, hdr["response"] if hdr["response"] is not None else 47, score_status, radius=4, exclude=[auth_col, auth_status_col] if auth_col is not None and auth_status_col is not None else [])

    mapping = {
        "datetime": datetime_col,
        "seq": seq_col,
        "node": node_col,
        "user": user_col,
        "amount": amount_col,
        "brand": brand_col,
        "last4": card_col,
        "auth": auth_col,
        "auth_status": auth_status_col,
        "response": response_col,
    }

    required = ["datetime", "amount", "brand", "last4", "auth"]
    missing = [k for k in required if mapping[k] is None]
    if missing:
        raise ValueError(f"Could not locate reconciliation fields: {', '.join(missing)}")

    dt = pd.to_datetime(data[mapping["datetime"]], errors="coerce")
    data = data[dt.notna()].copy()
    dt = pd.to_datetime(data[mapping["datetime"]], errors="coerce")

    out = pd.DataFrame({
        "Source_Row": data.index + 1,
        "DateTime": dt,
        "Date": dt.dt.date,
        "Time": dt.dt.time,
        "Seq": data[mapping["seq"]].astype(str) if mapping["seq"] is not None else "",
        "Node": data[mapping["node"]].astype(str) if mapping["node"] is not None else "",
        "User": data[mapping["user"]].astype(str) if mapping["user"] is not None else "",
        "Amount": data[mapping["amount"]].map(parse_amount_text),
        "Amount_Raw": data[mapping["amount"]].astype(str),
        "Card_Brand": data[mapping["brand"]].map(normalize_brand),
        "Last4": data[mapping["last4"]].map(extract_last4),
        "Auth_Code": data[mapping["auth"]].astype(str),
        "Auth_Norm": data[mapping["auth"]].map(normalize_auth),
        "Authorization_Status": data[mapping["auth_status"]].astype(str) if mapping["auth_status"] is not None else "",
        "Response": data[mapping["response"]].astype(str) if mapping["response"] is not None else "",
    })

    out = out[out["Amount"].notna()].copy()
    status_text = (out["Authorization_Status"].fillna("") + " " + out["Response"].fillna("")).str.upper()
    # Treat blank status as non-decline; declines only when explicit.
    out["Is_Decline"] = status_text.str.contains("DECLIN|DENIED|FAILED|ERROR|REJECT", regex=True, na=False)
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
        "header_row": int(header_row) + 1,
        "mapping": {k: (None if v is None else f"{excel_col(v)} ({v})") for k, v in mapping.items()},
    }
    return out.reset_index(drop=True), meta


# -----------------------------
# Statement parsing
# -----------------------------
def parse_statement_summary(raw: pd.DataFrame) -> dict:
    gross_sales = 0.0
    bank_charges = 0.0
    vat = 0.0

    for _, row in raw.iterrows():
        rowtxt = " | ".join([str(x) for x in row.tolist() if pd.notna(x)]).upper()
        nums = re.findall(r"\d+(?:\.\d+)?", rowtxt)
        if not nums:
            continue
        amount = float(nums[-1])
        if "GROSS SALES" in rowtxt:
            gross_sales += amount
        elif "BANK CHARGES" in rowtxt:
            bank_charges += amount
        elif "VALUE ADDED TAX" in rowtxt or "VAT" in rowtxt:
            vat += amount

    return {
        "gross_sales": gross_sales,
        "bank_charges": bank_charges,
        "vat": vat,
        "net_settlement": gross_sales - bank_charges - vat,
    }


def detect_statement_header_row(raw: pd.DataFrame) -> int:
    best_row = 0
    best_score = -1
    for i in range(min(40, len(raw))):
        vals = [normalize_text(x) for x in raw.iloc[i].tolist()]
        rowtxt = " | ".join(vals)
        score = 0
        if "purchase date" in rowtxt:
            score += 4
        if "card type" in rowtxt:
            score += 3
        if "card number" in rowtxt:
            score += 3
        if "auth. code" in rowtxt or "auth code" in rowtxt:
            score += 3
        if "trans. type" in rowtxt or "trans type" in rowtxt:
            score += 2
        if "amount" in rowtxt:
            score += 3
        if score > best_score:
            best_score = score
            best_row = i
    return best_row


STMT_HEADER_NAMES = {
    "purchase_date": ["purchase date"],
    "brand": ["card type"],
    "card_masked": ["card number"],
    "auth": ["auth. code", "auth code"],
    "trans_type": ["trans. type", "trans type"],
    "amount": ["amount"],
}


def load_statement(uploaded_file) -> Tuple[pd.DataFrame, dict, Dict]:
    raw = pd.read_excel(uploaded_file, sheet_name=0, header=None, engine="openpyxl")
    summary = parse_statement_summary(raw)
    header_row = detect_statement_header_row(raw)
    hdr = header_positions(raw.iloc[header_row], STMT_HEADER_NAMES)

    required = ["purchase_date", "brand", "card_masked", "auth", "amount"]
    missing = [k for k in required if hdr.get(k) is None]
    if missing:
        raise ValueError(f"Could not locate statement fields: {', '.join(missing)}")

    data = raw.iloc[header_row + 1 :].copy()
    data = data[data.notna().any(axis=1)].copy()

    purchase_date = pd.to_datetime(data[hdr["purchase_date"]], errors="coerce")
    data = data[purchase_date.notna()].copy()
    purchase_date = pd.to_datetime(data[hdr["purchase_date"]], errors="coerce")

    trans_type = data[hdr["trans_type"]].astype(str).str.strip() if hdr.get("trans_type") is not None else pd.Series(["Purchase"] * len(data), index=data.index)

    out = pd.DataFrame({
        "Source_Row": data.index + 1,
        "Purchase_Date": purchase_date.dt.date,
        "Card_Brand": data[hdr["brand"]].map(normalize_brand),
        "Last4": data[hdr["card_masked"]].map(extract_last4),
        "Card_Number_Masked": data[hdr["card_masked"]].astype(str),
        "Auth_Code": data[hdr["auth"]].astype(str),
        "Auth_Norm": data[hdr["auth"]].map(normalize_auth),
        "Amount": data[hdr["amount"]].map(parse_amount_text),
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
        "header_row": int(header_row) + 1,
        "mapping": {k: (None if v is None else f"{excel_col(v)} ({v})") for k, v in hdr.items()},
    }
    return out.reset_index(drop=True), summary, meta


# -----------------------------
# Matching logic
# -----------------------------
def match_transactions(rec: pd.DataFrame, stmt: pd.DataFrame):
    # Preserve the Feb 9 logic exactly:
    # same-day auth+amount, then next-day auth+amount. No fuzzy matching.
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
                "Notes": "Matched on same purchase date + normalized auth code + amount.",
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
        "Non-Visa/MC card brand; merchant statement is VISA/MC only.",
    )

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


def build_summary(rec, stmt, merchant_summary, matched, rec_unmatched, stmt_unmatched) -> SummaryMetrics:
    rec_scope = rec[rec["In_Statement_Scope"] == "Y"]
    same_day = matched[matched["Match_Type"].eq("Same-day auth match")] if len(matched) else matched
    next_day = matched[matched["Match_Type"].eq("Next-day auth match")] if len(matched) else matched
    rec_scope_unmatched = rec_unmatched[rec_unmatched["In_Statement_Scope"] == "Y"]
    rec_out_scope_unmatched = rec_unmatched[rec_unmatched["In_Statement_Scope"] == "N"]
    rec_dates = set(rec["Date"].dropna().tolist())
    stmt_within = stmt_unmatched[stmt_unmatched["Purchase_Date"].isin(rec_dates)]
    stmt_outside = stmt_unmatched[~stmt_unmatched["Purchase_Date"].isin(rec_dates)]

    return SummaryMetrics(
        gross_sales=float(merchant_summary.get("gross_sales", 0.0)),
        bank_charges=float(merchant_summary.get("bank_charges", 0.0)),
        vat=float(merchant_summary.get("vat", 0.0)),
        net_settlement=float(merchant_summary.get("net_settlement", 0.0)),
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
        stmt_unmatched_within_rec_count=int(len(stmt_within)),
        stmt_unmatched_within_rec_amount=float(stmt_within["Amount"].fillna(0).sum()),
        stmt_unmatched_outside_rec_count=int(len(stmt_outside)),
        stmt_unmatched_outside_rec_amount=float(stmt_outside["Amount"].fillna(0).sum()),
        duplicate_count=int(rec["Is_Duplicate"].sum() + stmt["Is_Duplicate"].sum()),
        decline_count=int(rec["Is_Decline"].sum()),
    )


# -----------------------------
# Export
# -----------------------------
def create_excel_report(rec, stmt, merchant_summary, matched, rec_unmatched, stmt_unmatched, rec_meta, stmt_meta):
    output = io.BytesIO()
    summary = build_summary(rec, stmt, merchant_summary, matched, rec_unmatched, stmt_unmatched)

    summary_rows = [
        ["Clean Reconciliation Report", None, None, None, None, None],
        ["Generated by Streamlit app v5", None, None, None, None, None],
        [None, None, None, None, None, None],
        ["Source Totals", "Count", "Amount", None, "Statement Settlement", "Amount"],
        ["Reconciliation file (all brands)", summary.rec_total_count, summary.rec_total_amount, None, "Gross Sales (VISA/MC)", summary.gross_sales],
        ["Reconciliation file (VISA/MC scope)", summary.rec_scope_count, summary.rec_scope_amount, None, "Bank Charges", summary.bank_charges],
        ["Merchant statement transactions", summary.stmt_total_count, summary.stmt_total_amount, None, "VAT", summary.vat],
        [None, None, None, None, "True Net Settlement", summary.net_settlement],
        [None, None, None, None, None, None],
        ["Matching Results", "Count", "Amount", None, "Exceptions", "Count / Amount"],
        ["Matched transactions", summary.matched_count, summary.matched_amount, None, "Rec unmatched in scope", f"{summary.rec_unmatched_scope_count} / {summary.rec_unmatched_scope_amount:,.2f}"],
        ["Same-day matches", summary.same_day_count, summary.same_day_amount, None, "Rec unmatched out of scope", f"{summary.rec_unmatched_out_scope_count} / {summary.rec_unmatched_out_scope_amount:,.2f}"],
        ["Next-day matches", summary.next_day_count, summary.next_day_amount, None, "Stmt unmatched within rec dates", f"{summary.stmt_unmatched_within_rec_count} / {summary.stmt_unmatched_within_rec_amount:,.2f}"],
        [None, None, None, None, "Stmt unmatched outside rec dates", f"{summary.stmt_unmatched_outside_rec_count} / {summary.stmt_unmatched_outside_rec_amount:,.2f}"],
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
        "Workbook": ["Reconciliation"] * len(rec_meta["mapping"]) + ["Merchant Statement"] * len(stmt_meta["mapping"]),
        "Field": list(rec_meta["mapping"].keys()) + list(stmt_meta["mapping"].keys()),
        "Detected Column": list(rec_meta["mapping"].values()) + list(stmt_meta["mapping"].values()),
    })

    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        summary_df.to_excel(writer, index=False, header=False, sheet_name="Summary")
        statement_summary_df.to_excel(writer, index=False, header=False, sheet_name="Statement_Summary")
        mapping_df.to_excel(writer, index=False, sheet_name="Detected_Mapping")
        rec.to_excel(writer, index=False, sheet_name="Reconciliation_Clean")
        stmt.to_excel(writer, index=False, sheet_name="Merchant_Clean")
        matched.to_excel(writer, index=False, sheet_name="Matched")
        rec_unmatched.to_excel(writer, index=False, sheet_name="Rec_Unmatched")
        stmt_unmatched.to_excel(writer, index=False, sheet_name="Merchant_Unmatched")

        wb = writer.book
        for ws in wb.worksheets:
            ws.freeze_panes = "A2"
            for col_cells in ws.columns:
                max_len = 0
                letter = col_cells[0].column_letter
                for cell in col_cells:
                    value = "" if cell.value is None else str(cell.value)
                    max_len = max(max_len, len(value))
                ws.column_dimensions[letter].width = min(max(max_len + 2, 12), 40)

    output.seek(0)
    return output.getvalue()


# -----------------------------
# UI
# -----------------------------
def main():
    st.set_page_config(page_title=APP_TITLE, layout="wide")
    st.title(APP_TITLE)
    st.write(
        "Version 5 is built around the Feb 9 matching logic, but uses header-anchored parsing for the "
        "two workbook formats you uploaded. It is designed to work across other date ranges of the same exports."
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
            d.metric("Stmt unmatched within rec dates", f"{summary.stmt_unmatched_within_rec_count} / {summary.stmt_unmatched_within_rec_amount:,.2f}")

            with st.expander("Detected reconciliation mapping", expanded=False):
                st.write({"header_row": rec_meta["header_row"], "mapping": rec_meta["mapping"]})

            with st.expander("Detected merchant statement mapping", expanded=False):
                st.write({"header_row": stmt_meta["header_row"], "mapping": stmt_meta["mapping"]})

            st.subheader("Matched transactions")
            st.dataframe(matched, use_container_width=True)

            left, right = st.columns(2)
            with left:
                st.subheader("Reconciliation unmatched")
                st.dataframe(rec_unmatched, use_container_width=True)
            with right:
                st.subheader("Merchant unmatched")
                st.dataframe(stmt_unmatched, use_container_width=True)

            excel_bytes = create_excel_report(
                rec, stmt, merchant_summary, matched, rec_unmatched, stmt_unmatched, rec_meta, stmt_meta
            )
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
