
import io
import re
from dataclasses import dataclass
from typing import Dict, List, Tuple

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
    if pd.isna(v):
        return ""
    s = str(v).strip().upper()
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
    }.get(s, s or "")


def extract_last4(v):
    if pd.isna(v):
        return ""
    m = re.search(r"(\d{4})\s*$", str(v))
    if m:
        return m.group(1)
    digits = re.findall(r"\d", str(v))
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


def compress_visible_cells(row: pd.Series) -> List[Tuple[int, object]]:
    return [(int(i), v) for i, v in row.items() if pd.notna(v) and str(v).strip() != ""]


def detect_reconciliation_header_row(raw: pd.DataFrame) -> int:
    best_row = None
    best_score = -1
    for i in range(min(40, len(raw))):
        vals = [normalize_text(x) for x in raw.iloc[i].tolist()]
        rowtxt = " | ".join(vals)
        score = 0
        if "date/time" in rowtxt:
            score += 4
        if "seq" in rowtxt:
            score += 2
        if "node" in rowtxt:
            score += 2
        if "user" in rowtxt:
            score += 2
        if "payment/refund total" in rowtxt or "account / smart card" in rowtxt:
            score += 4
        if "card type" in rowtxt:
            score += 3
        if "card number" in rowtxt:
            score += 3
        if "authorization status" in rowtxt:
            score += 1
        if "response" in rowtxt:
            score += 1
        if score > best_score:
            best_score = score
            best_row = i
    if best_row is None or best_score < 8:
        raise ValueError("Could not identify the reconciliation header row reliably.")
    return best_row


def parse_statement_summary(raw: pd.DataFrame) -> dict:
    gross_sales = bank_charges = vat = 0.0
    for _, row in raw.iterrows():
        rowtxt = " | ".join([str(x) for x in row.tolist() if pd.notna(x)]).upper()
        nums = re.findall(r"\d+(?:\.\d+)?", rowtxt)
        if not nums:
            continue
        amt = float(nums[-1])
        if "BANK CHARGES" in rowtxt:
            bank_charges += amt
        elif "GROSS SALES" in rowtxt:
            gross_sales += amt
        elif "VALUE ADDED TAX" in rowtxt or "VAT" in rowtxt:
            vat += amt
    return {
        "gross_sales": gross_sales,
        "bank_charges": bank_charges,
        "vat": vat,
        "net_settlement": gross_sales - bank_charges - vat,
    }


def detect_statement_header_row(raw: pd.DataFrame) -> int:
    for i in range(min(80, len(raw))):
        vals = [normalize_text(x) for x in raw.iloc[i].tolist()]
        rowtxt = " | ".join(vals)
        if "purchase date" in rowtxt and "card type" in rowtxt and "card number" in rowtxt and "amount" in rowtxt:
            return i
    raise ValueError("Could not find the merchant statement transaction header row.")


def load_reconciliation(uploaded_file) -> pd.DataFrame:
    raw = pd.read_excel(uploaded_file, sheet_name=0, header=None, engine="openpyxl")
    header_row = detect_reconciliation_header_row(raw)

    records = []
    for ridx in range(header_row + 1, len(raw)):
        cells = compress_visible_cells(raw.iloc[ridx])
        values = [v for _, v in cells]

        if len(values) < 7:
            continue

        dt = pd.to_datetime(values[0], errors="coerce")
        amount = parse_amount_text(values[4]) if len(values) > 4 else np.nan
        brand = normalize_brand(values[5]) if len(values) > 5 else ""
        last4 = extract_last4(values[6]) if len(values) > 6 else ""

        if pd.isna(dt) or pd.isna(amount) or brand == "" or last4 == "":
            continue

        processor_ref = values[7] if len(values) > 7 else ""
        auth_code = values[8] if len(values) > 8 else ""
        auth_status = values[9] if len(values) > 9 else ""
        response = values[10] if len(values) > 10 else ""

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
            "Card_Brand": brand,
            "Last4": last4,
            "Auth_Code": str(auth_code),
            "Auth_Norm": normalize_auth(auth_code),
            "Authorization_Status": str(auth_status),
            "Processor_Ref": str(processor_ref),
            "Response": str(response),
        })

    out = pd.DataFrame(records)
    if out.empty:
        raise ValueError("No reconciliation transactions were parsed from the workbook.")

    status_text = (out["Authorization_Status"].fillna("") + " " + out["Response"].fillna("")).str.upper()
    out["Is_Decline"] = status_text.str.contains("DECLIN|DENIED|ERROR|FAILED|REJECT", na=False)
    out["In_Statement_Scope"] = np.where(
        out["Card_Brand"].isin(["VISA", "MASTERCARD"]), "Y", "N"
    )
    out["Duplicate_Key"] = (
        out["DateTime"].astype(str)
        + "|" + out["Card_Brand"].astype(str)
        + "|" + out["Last4"].astype(str)
        + "|" + out["Auth_Norm"].astype(str)
        + "|" + out["Amount"].round(2).astype(str)
    )
    out["Is_Duplicate"] = out.duplicated("Duplicate_Key", keep=False)
    return out.reset_index(drop=True)


def load_statement(uploaded_file):
    raw = pd.read_excel(uploaded_file, sheet_name=0, header=None, engine="openpyxl")
    summary = parse_statement_summary(raw)
    header_row = detect_statement_header_row(raw)
    headers = [normalize_text(x) for x in raw.iloc[header_row].tolist()]

    def find_col(options):
        for i, h in enumerate(headers):
            if any(opt in h for opt in options):
                return i
        return None

    purchase_col = find_col(["purchase date"])
    brand_col = find_col(["card type"])
    card_col = find_col(["card number"])
    auth_col = find_col(["auth. code", "auth code"])
    trans_col = find_col(["trans. type", "trans type"])
    amount_col = find_col(["amount"])

    if None in [purchase_col, brand_col, card_col, auth_col, amount_col]:
        raise ValueError("Could not locate one or more required merchant statement columns.")

    data = raw.iloc[header_row + 1 :].copy()
    data = data[data.notna().any(axis=1)].copy()
    data["Purchase_Date"] = pd.to_datetime(data[purchase_col], errors="coerce")
    data = data[data["Purchase_Date"].notna()].copy()

    out = pd.DataFrame({
        "Source_Row": data.index + 1,
        "Purchase_Date": data["Purchase_Date"].dt.date,
        "Card_Brand": data[brand_col].map(normalize_brand),
        "Last4": data[card_col].map(extract_last4),
        "Card_Number_Masked": data[card_col].astype(str),
        "Auth_Code": data[auth_col].astype(str),
        "Auth_Norm": data[auth_col].map(normalize_auth),
        "Amount": data[amount_col].map(parse_amount_text),
        "Trans_Type": data[trans_col].astype(str).str.strip() if trans_col is not None else "Purchase",
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
    return out.reset_index(drop=True), summary


def match_transactions(rec: pd.DataFrame, stmt: pd.DataFrame):
    rec = rec.copy()
    stmt = stmt.copy()
    rec["Matched"] = False
    stmt["Matched"] = False

    matches = []

    for rec_idx, r in rec[(rec["In_Statement_Scope"] == "Y") & (~rec["Is_Decline"])].iterrows():
        possible = stmt[
            (~stmt["Matched"])
            & (stmt["Purchase_Date"] == r["Date"])
            & (stmt["Auth_Norm"] == r["Auth_Norm"])
            & (stmt["Amount"].round(2) == round(r["Amount"], 2))
        ]
        if possible.empty:
            continue

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

    for rec_idx, r in rec[(rec["In_Statement_Scope"] == "Y") & (~rec["Is_Decline"]) & (~rec["Matched"])].iterrows():
        next_date = (pd.Timestamp(r["Date"]) + pd.Timedelta(days=1)).date()
        possible = stmt[
            (~stmt["Matched"])
            & (stmt["Purchase_Date"] == next_date)
            & (stmt["Auth_Norm"] == r["Auth_Norm"])
            & (stmt["Amount"].round(2) == round(r["Amount"], 2))
        ]
        if possible.empty:
            continue

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
            "Notes": "Matched on next-day settlement date + normalized auth code + amount.",
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
    base_date = rec["Date"].min() if len(rec) else None
    stmt_unmatched["Exception_Type"] = np.where(
        stmt_unmatched["Purchase_Date"] == base_date,
        "No match in provided online log",
        "Outside reconciliation date",
    )
    stmt_unmatched["Notes"] = np.where(
        stmt_unmatched["Purchase_Date"] == base_date,
        "Statement transaction has no matching record in the provided online log.",
        "Statement line is outside the reconciliation workbook date.",
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
        ["Matching Results", "Count", "Amount", None, "Exceptions", "Count / Amount"],
        ["Matched transactions", summary.matched_count, summary.matched_amount, None, f"Rec unmatched in scope", f'{summary.rec_unmatched_scope_count} / {summary.rec_unmatched_scope_amount:,.2f}'],
        ["Same-day matches", summary.same_day_count, summary.same_day_amount, None, f"Rec unmatched out of scope", f'{summary.rec_unmatched_out_scope_count} / {summary.rec_unmatched_out_scope_amount:,.2f}'],
        ["Next-day matches", summary.next_day_count, summary.next_day_amount, None, f"Stmt unmatched same date", f'{summary.stmt_unmatched_same_date_count} / {summary.stmt_unmatched_same_date_amount:,.2f}'],
        [None, None, None, None, f"Stmt unmatched other date", f'{summary.stmt_unmatched_other_date_count} / {summary.stmt_unmatched_other_date_amount:,.2f}'],
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

    exceptions_df = pd.DataFrame([
        ["Exceptions Review", None, None, None],
        [None, None, None, None],
        ["Check", "Count", "Amount", "Comment"],
        ["Reconciliation declines", int(rec["Is_Decline"].sum()), float(rec.loc[rec["Is_Decline"], "Amount"].fillna(0).sum()), "All transactions in the reconciliation file are approved." if int(rec["Is_Decline"].sum()) == 0 else "Review declined items."],
        ["Reconciliation exact duplicates", int(rec["Is_Duplicate"].sum()), float(rec.loc[rec["Is_Duplicate"], "Amount"].fillna(0).sum()), "No exact duplicates found using date + brand + last4 + normalized auth + amount." if int(rec["Is_Duplicate"].sum()) == 0 else "Review duplicate online-log items."],
        ["Merchant exact duplicates", int(stmt["Is_Duplicate"].sum()), float(stmt.loc[stmt["Is_Duplicate"], "Amount"].fillna(0).sum()), "No exact duplicates found using purchase date + brand + last4 + normalized auth + amount." if int(stmt["Is_Duplicate"].sum()) == 0 else "Review duplicate statement items."],
        ["Rec non-Visa/MC items", int((rec["In_Statement_Scope"] == "N").sum()), float(rec.loc[rec["In_Statement_Scope"] == "N", "Amount"].fillna(0).sum()), "These items are outside the VISA/MC statement scope (AMEX / Other)."],
    ])

    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        summary_df.to_excel(writer, index=False, header=False, sheet_name="Summary")
        statement_summary_df.to_excel(writer, index=False, header=False, sheet_name="Statement_Summary")
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
                max_len = 0
                letter = col_cells[0].column_letter
                for cell in col_cells:
                    value = "" if cell.value is None else str(cell.value)
                    max_len = max(max_len, len(value))
                ws.column_dimensions[letter].width = min(max(max_len + 2, 12), 34)

    output.seek(0)
    return output.getvalue()


def main():
    st.set_page_config(page_title=APP_TITLE, layout="wide")
    st.title(APP_TITLE)
    st.write("Upload the reconciliation workbook and the merchant statement workbook, then generate the reconciliation report.")

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

            st.subheader("Summary")
            st.write({
                "same_day_matches": f"{summary.same_day_count} / {summary.same_day_amount:,.2f}",
                "next_day_matches": f"{summary.next_day_count} / {summary.next_day_amount:,.2f}",
                "rec_unmatched_out_of_scope": f"{summary.rec_unmatched_out_scope_count} / {summary.rec_unmatched_out_scope_amount:,.2f}",
                "stmt_unmatched_other_date": f"{summary.stmt_unmatched_other_date_count} / {summary.stmt_unmatched_other_date_amount:,.2f}",
                "declines": summary.decline_count,
                "duplicates": summary.duplicate_count,
            })

            st.subheader("Matched transactions")
            st.dataframe(matched, use_container_width=True)

            left, right = st.columns(2)
            with left:
                st.subheader("Reconciliation unmatched")
                st.dataframe(rec_unmatched, use_container_width=True)
            with right:
                st.subheader("Merchant unmatched")
                st.dataframe(stmt_unmatched, use_container_width=True)

            st.subheader("Exceptions review")
            exceptions = pd.DataFrame([
                {"Check": "Reconciliation declines", "Count": int(rec["Is_Decline"].sum()), "Amount": float(rec.loc[rec["Is_Decline"], "Amount"].fillna(0).sum())},
                {"Check": "Reconciliation exact duplicates", "Count": int(rec["Is_Duplicate"].sum()), "Amount": float(rec.loc[rec["Is_Duplicate"], "Amount"].fillna(0).sum())},
                {"Check": "Merchant exact duplicates", "Count": int(stmt["Is_Duplicate"].sum()), "Amount": float(stmt.loc[stmt["Is_Duplicate"], "Amount"].fillna(0).sum())},
                {"Check": "Rec non-Visa/MC items", "Count": int((rec["In_Statement_Scope"] == "N").sum()), "Amount": float(rec.loc[rec["In_Statement_Scope"] == "N", "Amount"].fillna(0).sum())},
            ])
            st.dataframe(exceptions, use_container_width=True)

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
