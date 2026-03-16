
import io
import re
from dataclasses import dataclass
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd
import streamlit as st


APP_TITLE = "Excel Reconciliation App v7"


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
    return abs(float(m.group(1))) if m else np.nan


def normalize_brand(v) -> str:
    s = normalize_text(v).upper()
    return {
        "V": "VISA",
        "VISA": "VISA",
        "M": "MASTERCARD",
        "MC": "MASTERCARD",
        "MASTERCARD": "MASTERCARD",
        "A": "AMEX",
        "AMEX": "AMEX",
        "AMERICAN EXPRESS": "AMEX",
        "OTHER": "OTHER",
    }.get(s, s if s else "")


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


def compress_visible_cells(row: pd.Series) -> List[Tuple[int, object]]:
    return [(int(i), v) for i, v in row.items() if pd.notna(v) and str(v).strip() != ""]


def is_transaction_like(values: List[object]) -> bool:
    if len(values) < 6:
        return False
    dt_ok = pd.notna(pd.to_datetime(values[0], errors="coerce"))
    amt_ok = pd.notna(parse_amount_text(values[4])) if len(values) > 4 else False
    brand = normalize_brand(values[5]) if len(values) > 5 else ""
    card_ok = len(extract_last4(values[6])) == 4 if len(values) > 6 else False
    return bool(dt_ok and amt_ok and brand in {"VISA", "MASTERCARD", "AMEX", "OTHER"} and card_ok)


# -----------------------------
# Reconciliation parsing
# -----------------------------
def detect_reconciliation_header_row(raw: pd.DataFrame) -> Tuple[int, str]:
    best_row = None
    best_score = -1

    for i in range(min(40, len(raw))):
        vals = [normalize_text(x) for x in raw.iloc[i].tolist()]
        rowtxt = " | ".join(vals)
        score = 0
        if "date/time" in rowtxt: score += 5
        if "seq" in rowtxt: score += 2
        if "node" in rowtxt: score += 2
        if "user" in rowtxt: score += 2
        if "payment/refund total" in rowtxt: score += 5
        if "card type" in rowtxt: score += 4
        if "card number" in rowtxt: score += 4
        if "authorization status" in rowtxt: score += 2
        if "response" in rowtxt: score += 1

        if score > best_score:
            best_score = score
            best_row = i

    if best_row is not None and best_score >= 8:
        return best_row, "label-based"

    # Fallback: detect the first run of transaction-like rows and treat the row before as header.
    transaction_rows = []
    for i in range(min(80, len(raw))):
        cells = compress_visible_cells(raw.iloc[i])
        values = [v for _, v in cells]
        if is_transaction_like(values):
            transaction_rows.append(i)

    if transaction_rows:
        first_tx = transaction_rows[0]
        header_guess = max(0, first_tx - 1)
        return header_guess, "transaction-fallback"

    raise ValueError(
        "Could not identify the reconciliation header row. "
        "This export appears to differ from the supported Credit Card Transaction Detail layout."
    )


def load_reconciliation(uploaded_file):
    raw = pd.read_excel(uploaded_file, sheet_name=0, header=None, engine="openpyxl")
    header_row, method = detect_reconciliation_header_row(raw)
    header_cells = compress_visible_cells(raw.iloc[header_row])

    records = []
    parse_examples = []

    for ridx in range(header_row + 1, len(raw)):
        row = raw.iloc[ridx]
        cells = compress_visible_cells(row)
        values = [v for _, v in cells]

        if len(values) >= 7 and is_transaction_like(values):
            auth = values[8] if len(values) > 8 else ""
            status = values[9] if len(values) > 9 else ""
            response = values[10] if len(values) > 10 else ""
            dt = pd.to_datetime(values[0], errors="coerce")
            amount = parse_amount_text(values[4])

            records.append({
                "Source_Row": ridx + 1,
                "DateTime": dt,
                "Date": dt.date(),
                "Time": dt.time(),
                "Seq": str(values[1]) if len(values) > 1 else "",
                "Node": str(values[2]) if len(values) > 2 else "",
                "User": str(values[3]) if len(values) > 3 else "",
                "Amount": amount,
                "Amount_Raw": str(values[4]) if len(values) > 4 else "",
                "Card_Brand": normalize_brand(values[5]) if len(values) > 5 else "",
                "Last4": extract_last4(values[6]) if len(values) > 6 else "",
                "Auth_Code": str(auth),
                "Auth_Norm": normalize_auth(auth),
                "Authorization_Status": str(status),
                "Response": str(response),
            })

            if len(parse_examples) < 5:
                parse_examples.append({
                    "row": ridx + 1,
                    "visible_values": [str(v) for v in values[:11]],
                })

    out = pd.DataFrame(records)
    if out.empty:
        raise ValueError(
            "No reconciliation transactions were parsed. "
            "The workbook was opened, but no rows matched the expected transaction pattern."
        )

    status_text = (out["Authorization_Status"].fillna("") + " " + out["Response"].fillna("")).str.upper()
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

    mapping = {}
    labels = ["datetime","seq","node","user","amount","brand","last4","processor_ref","auth","auth_status","response"]
    for pos, item in enumerate(header_cells[:len(labels)]):
        mapping[labels[pos]] = f"{excel_col(item[0])} ({item[0]})"

    preview_rows = []
    for i in range(min(12, len(raw))):
        preview_rows.append([str(v) for _, v in compress_visible_cells(raw.iloc[i])[:12]])

    meta = {
        "header_row": header_row + 1,
        "detection_method": method,
        "mapping": mapping,
        "preview": preview_rows,
        "parse_examples": parse_examples,
    }
    return out.reset_index(drop=True), meta


# -----------------------------
# Statement parsing
# -----------------------------
def parse_statement_summary(raw: pd.DataFrame) -> dict:
    gross_sales = bank_charges = vat = 0.0
    for _, row in raw.iterrows():
        rowtxt = " | ".join([str(x) for x in row.tolist() if pd.notna(x)]).upper()
        nums = re.findall(r"\d+(?:\.\d+)?", rowtxt)
        if not nums:
            continue
        amt = float(nums[-1])
        if "GROSS SALES" in rowtxt:
            gross_sales += amt
        elif "BANK CHARGES" in rowtxt:
            bank_charges += amt
        elif "VALUE ADDED TAX" in rowtxt or "VAT" in rowtxt:
            vat += amt
    return {
        "gross_sales": gross_sales,
        "bank_charges": bank_charges,
        "vat": vat,
        "net_settlement": gross_sales - bank_charges - vat,
    }


def detect_statement_header_row(raw: pd.DataFrame) -> int:
    for i in range(min(60, len(raw))):
        vals = [normalize_text(x) for x in raw.iloc[i].tolist()]
        rowtxt = " | ".join(vals)
        if "purchase date" in rowtxt and "card type" in rowtxt and "card number" in rowtxt and "amount" in rowtxt:
            return i
    raise ValueError("Could not find merchant statement transaction header row.")


def load_statement(uploaded_file):
    raw = pd.read_excel(uploaded_file, sheet_name=0, header=None, engine="openpyxl")
    summary = parse_statement_summary(raw)
    header_row = detect_statement_header_row(raw)
    headers = [normalize_text(x) for x in raw.iloc[header_row].tolist()]

    def find_col(name_options):
        for i, h in enumerate(headers):
            if any(n in h for n in name_options):
                return i
        return None

    mapping_idx = {
        "purchase_date": find_col(["purchase date"]),
        "brand": find_col(["card type"]),
        "card_masked": find_col(["card number"]),
        "auth": find_col(["auth. code", "auth code"]),
        "trans_type": find_col(["trans. type", "trans type"]),
        "amount": find_col(["amount"]),
    }
    required = ["purchase_date","brand","card_masked","auth","amount"]
    missing = [k for k in required if mapping_idx[k] is None]
    if missing:
        raise ValueError(f"Could not locate statement fields: {', '.join(missing)}")

    data = raw.iloc[header_row + 1 :].copy()
    data = data[data.notna().any(axis=1)].copy()
    purchase_date = pd.to_datetime(data[mapping_idx["purchase_date"]], errors="coerce")
    data = data[purchase_date.notna()].copy()
    purchase_date = pd.to_datetime(data[mapping_idx["purchase_date"]], errors="coerce")
    trans_type = data[mapping_idx["trans_type"]].astype(str).str.strip() if mapping_idx["trans_type"] is not None else pd.Series(["Purchase"] * len(data), index=data.index)

    out = pd.DataFrame({
        "Source_Row": data.index + 1,
        "Purchase_Date": purchase_date.dt.date,
        "Card_Brand": data[mapping_idx["brand"]].map(normalize_brand),
        "Last4": data[mapping_idx["card_masked"]].map(extract_last4),
        "Card_Number_Masked": data[mapping_idx["card_masked"]].astype(str),
        "Auth_Code": data[mapping_idx["auth"]].astype(str),
        "Auth_Norm": data[mapping_idx["auth"]].map(normalize_auth),
        "Amount": data[mapping_idx["amount"]].map(parse_amount_text),
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
        "header_row": header_row + 1,
        "mapping": {k: f"{excel_col(v)} ({v})" if v is not None else None for k, v in mapping_idx.items()},
    }
    return out.reset_index(drop=True), summary, meta


# -----------------------------
# Matching logic
# -----------------------------
def match_transactions(rec: pd.DataFrame, stmt: pd.DataFrame):
    rec = rec.copy()
    stmt = stmt.copy()
    rec["Matched"] = False
    stmt["Matched"] = False
    matches = []

    eligible_rec = rec[(rec["In_Statement_Scope"] == "Y") & (~rec["Is_Decline"])].copy()

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
            })

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
            })

    matched = pd.DataFrame(matches)
    rec_unmatched = rec[~rec["Matched"]].copy()
    rec_unmatched["Exception_Type"] = np.where(
        rec_unmatched["In_Statement_Scope"] == "Y",
        "In-scope but no match on provided statement",
        "Out of statement scope (non-Visa/MC)",
    )

    stmt_unmatched = stmt[~stmt["Matched"]].copy()
    rec_dates = set(rec["Date"].dropna().tolist())
    stmt_unmatched["Exception_Type"] = np.where(
        stmt_unmatched["Purchase_Date"].isin(rec_dates),
        "No match in provided online log",
        "Outside reconciliation date range",
    )
    return matched, rec_unmatched, stmt_unmatched


def build_summary(rec, stmt, merchant_summary, matched, rec_unmatched, stmt_unmatched):
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


def create_excel_report(rec, stmt, merchant_summary, matched, rec_unmatched, stmt_unmatched, rec_meta, stmt_meta):
    output = io.BytesIO()
    summary = build_summary(rec, stmt, merchant_summary, matched, rec_unmatched, stmt_unmatched)

    summary_rows = [
        ["Clean Reconciliation Report", None, None, None, None, None],
        ["Generated by Streamlit app v7", None, None, None, None, None],
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
    mapping_df = pd.DataFrame({
        "Workbook": ["Reconciliation"] * len(rec_meta["mapping"]) + ["Merchant Statement"] * len(stmt_meta["mapping"]),
        "Field": list(rec_meta["mapping"].keys()) + list(stmt_meta["mapping"].keys()),
        "Detected Column": list(rec_meta["mapping"].values()) + list(stmt_meta["mapping"].values()),
    })

    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        summary_df.to_excel(writer, index=False, header=False, sheet_name="Summary")
        mapping_df.to_excel(writer, index=False, sheet_name="Detected_Mapping")
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
    st.write("Version 7 adds a fallback header detector for reconciliation files that do not label the header row consistently.")

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

            with st.expander("Detected reconciliation details", expanded=False):
                st.write(rec_meta)
            with st.expander("Detected merchant statement mapping", expanded=False):
                st.write(stmt_meta)

            st.subheader("Parsed reconciliation sample")
            st.dataframe(rec.head(30), use_container_width=True)

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
