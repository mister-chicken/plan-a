"""Reconciliation helper functions for matching banking and credit card transactions."""

import pandas as pd
from typing import Dict, Optional
from datetime import datetime, timedelta


def load_all_transactions(
    file_path: str = "./data/processed/all_transactions.csv",
) -> pd.DataFrame:
    """Load and prepare all transactions for reconciliation."""
    df = pd.read_csv(file_path)
    df["date"] = pd.to_datetime(df["date"])
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
    df["year_month"] = df["date"].dt.to_period("M")
    return df


def get_credit_card_payments_from_bank(df: pd.DataFrame) -> pd.DataFrame:
    """
    Extract credit card payment transactions from bank statements.

    Returns DataFrame with credit card payments made from bank account.
    """
    bank_trans = df[df["source"] == "td_bank"].copy()
    cc_payments = bank_trans[bank_trans["category"] == "credit_card_payment"].copy()

    # Extract card type from description
    def extract_card_type(desc: str) -> str:
        desc_lower = str(desc).lower()
        if "apple" in desc_lower or "applecard" in desc_lower:
            return "Apple Card"
        elif "robinhood" in desc_lower:
            return "Robinhood"
        else:
            return "Unknown"

    cc_payments["card_type"] = cc_payments["description"].apply(extract_card_type)
    return cc_payments[["date", "description", "card_type", "amount", "source"]]


def get_credit_card_spending_by_period(
    df: pd.DataFrame,
    card_source: str,
    start_date: Optional[pd.Timestamp] = None,
    end_date: Optional[pd.Timestamp] = None,
) -> pd.DataFrame:
    """
    Get credit card spending for a specific card and time period.

    Args:
        df: All transactions DataFrame
        card_source: 'apple_card' or 'robinhood'
        start_date: Start of period (inclusive)
        end_date: End of period (inclusive)

    Returns:
        DataFrame with spending summary
    """
    card_trans = df[df["source"] == card_source].copy()

    if start_date:
        card_trans = card_trans[card_trans["date"] >= start_date]
    if end_date:
        card_trans = card_trans[card_trans["date"] <= end_date]

    # Only expenses (positive amounts)
    expenses = card_trans[card_trans["amount"] > 0]

    summary = {
        "total_spent": expenses["amount"].sum(),
        "num_transactions": len(expenses),
        "avg_transaction": expenses["amount"].mean(),
        "date_range": f"{card_trans['date'].min().date()} to {card_trans['date'].max().date()}",
    }

    return pd.DataFrame([summary])


def reconcile_credit_card_payments(
    df: pd.DataFrame,
    payment_date: pd.Timestamp,
    card_type: str,
    lookback_days: int = 45,
) -> Dict:
    """
    Reconcile a credit card payment with spending in the prior period.

    Args:
        df: All transactions DataFrame
        payment_date: Date of the payment from bank
        card_type: 'Apple Card' or 'Robinhood'
        lookback_days: Number of days to look back for spending

    Returns:
        Dictionary with reconciliation details
    """
    # Map card type to source
    card_source_map = {"Apple Card": "apple_card", "Robinhood": "robinhood"}

    card_source = card_source_map.get(card_type)
    if not card_source:
        return {"error": f"Unknown card type: {card_type}"}

    # Get bank payment amount
    bank_payments = get_credit_card_payments_from_bank(df)
    payment = bank_payments[
        (bank_payments["date"] == payment_date)
        & (bank_payments["card_type"] == card_type)
    ]

    if payment.empty:
        return {"error": "Payment not found"}

    payment_amount = payment["amount"].iloc[0]

    # Look back for credit card spending
    start_date = payment_date - timedelta(days=lookback_days)
    end_date = payment_date - timedelta(days=1)  # Exclude payment date

    card_trans = df[
        (df["source"] == card_source)
        & (df["date"] >= start_date)
        & (df["date"] <= end_date)
    ].copy()

    # Calculate spending (positive amounts = expenses)
    total_spent = card_trans[card_trans["amount"] > 0]["amount"].sum()

    # Calculate difference
    difference = payment_amount - total_spent
    match_pct = (
        (min(payment_amount, total_spent) / max(payment_amount, total_spent) * 100)
        if max(payment_amount, total_spent) > 0
        else 0
    )

    return {
        "payment_date": payment_date.date(),
        "card_type": card_type,
        "payment_amount": round(payment_amount, 2),
        "spending_period": f"{start_date.date()} to {end_date.date()}",
        "total_spent": round(total_spent, 2),
        "difference": round(difference, 2),
        "match_percentage": round(match_pct, 2),
        "num_transactions": len(card_trans),
        "status": "MATCH"
        if abs(difference) < 1
        else "REVIEW"
        if abs(difference) < 100
        else "MISMATCH",
    }


def reconcile_all_credit_card_payments(df: pd.DataFrame) -> pd.DataFrame:
    """
    Reconcile all credit card payments with their corresponding spending.

    Returns DataFrame with reconciliation results for each payment.
    """
    payments = get_credit_card_payments_from_bank(df)

    results = []
    for _, payment in payments.iterrows():
        result = reconcile_credit_card_payments(
            df, payment["date"], payment["card_type"]
        )
        if "error" not in result:
            results.append(result)

    return pd.DataFrame(results)


def get_payment_app_reconciliation(df: pd.DataFrame) -> pd.DataFrame:
    """
    Reconcile payment app transactions (PayPal, Venmo) between bank and app sources.

    Compares bank payments to payment apps with actual transactions in those apps.
    """
    # Get bank payments to PayPal/Venmo
    bank_trans = df[df["source"] == "td_bank"].copy()
    bank_payment_apps = bank_trans[bank_trans["category"] == "payment_app"].copy()

    # Determine which app
    def determine_app(desc: str) -> str:
        desc_lower = str(desc).lower()
        if "paypal" in desc_lower:
            return "paypal"
        elif "venmo" in desc_lower:
            return "venmo"
        return "unknown"

    bank_payment_apps["app"] = bank_payment_apps["description"].apply(determine_app)

    # Get actual app transactions
    venmo_trans = df[df["source"] == "venmo"].copy()
    # Note: PayPal transactions would go here if we had them

    # Summary by month
    bank_summary = (
        bank_payment_apps.groupby(["year_month", "app"])["amount"]
        .agg([("bank_total", "sum"), ("bank_count", "count")])
        .reset_index()
    )

    venmo_summary = (
        venmo_trans[venmo_trans["amount"] > 0]
        .groupby("year_month")["amount"]
        .agg([("venmo_total", "sum"), ("venmo_count", "count")])
        .reset_index()
    )
    venmo_summary["app"] = "venmo"

    # Merge
    reconciliation = pd.merge(
        bank_summary, venmo_summary, on=["year_month", "app"], how="outer"
    ).fillna(0)

    reconciliation["difference"] = (
        reconciliation["bank_total"] - reconciliation["venmo_total"]
    )
    reconciliation["match_status"] = reconciliation["difference"].apply(
        lambda x: "MATCH" if abs(x) < 1 else "REVIEW" if abs(x) < 50 else "MISMATCH"
    )

    return reconciliation


def get_monthly_cash_flow_summary(df: pd.DataFrame) -> pd.DataFrame:
    """
    Generate monthly cash flow summary from bank transactions.

    Shows income, expenses, and net cash flow by month.
    """
    bank_trans = df[df["source"] == "td_bank"].copy()

    # Separate income (negative amounts) and expenses (positive amounts)
    monthly = bank_trans.groupby("year_month").agg(
        {
            "amount": [
                ("total_expenses", lambda x: x[x > 0].sum()),
                ("total_income", lambda x: abs(x[x < 0].sum())),
                ("net_cash_flow", lambda x: -x.sum()),  # Negative expenses + income
            ]
        }
    )

    monthly.columns = monthly.columns.droplevel(0)
    monthly = monthly.reset_index()
    monthly["year_month"] = monthly["year_month"].astype(str)

    return monthly


def find_duplicate_transactions(
    df: pd.DataFrame, threshold_hours: int = 24
) -> pd.DataFrame:
    """
    Find potential duplicate transactions across different sources.

    Looks for transactions with same amount within threshold_hours.
    """
    # Sort by date and amount
    df_sorted = df.sort_values(["date", "amount"]).copy()
    df_sorted["date_hour"] = df_sorted["date"].dt.floor("H")

    duplicates = []

    # Group by amount and find close dates
    for amount in df_sorted["amount"].unique():
        amount_trans = df_sorted[df_sorted["amount"] == amount].copy()

        if len(amount_trans) > 1:
            # Check if transactions are within threshold
            for i, trans1 in amount_trans.iterrows():
                for j, trans2 in amount_trans.iterrows():
                    if i >= j:
                        continue

                    time_diff = abs(
                        (trans1["date"] - trans2["date"]).total_seconds() / 3600
                    )

                    if (
                        time_diff <= threshold_hours
                        and trans1["source"] != trans2["source"]
                    ):
                        duplicates.append(
                            {
                                "date1": trans1["date"],
                                "source1": trans1["source"],
                                "desc1": trans1["description"],
                                "date2": trans2["date"],
                                "source2": trans2["source"],
                                "desc2": trans2["description"],
                                "amount": amount,
                                "hours_apart": round(time_diff, 1),
                            }
                        )

    return pd.DataFrame(duplicates)


def get_unreconciled_items(df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """
    Find transactions that might need reconciliation attention.

    Returns dictionary of DataFrames for different unreconciled categories.
    """
    unreconciled = {}

    # Large bank transactions without clear category
    bank_trans = df[df["source"] == "td_bank"].copy()
    large_banking = bank_trans[
        (bank_trans["category"] == "banking") & (bank_trans["amount"].abs() > 100)
    ][["date", "description", "amount"]]

    if not large_banking.empty:
        unreconciled["large_uncategorized_bank"] = large_banking

    # Credit card transactions without matching payment (last 60 days)
    sixty_days_ago = pd.Timestamp.now() - timedelta(days=60)
    recent_cc = df[
        (df["source"].isin(["apple_card", "robinhood"]))
        & (df["date"] >= sixty_days_ago)
        & (df["amount"] > 0)
    ]

    if not recent_cc.empty:
        cc_total = recent_cc.groupby("source")["amount"].sum()
        unreconciled["recent_cc_spending"] = cc_total.to_frame()

    return unreconciled


def generate_reconciliation_report(df: pd.DataFrame) -> Dict:
    """
    Generate comprehensive reconciliation report.

    Returns dictionary with all reconciliation checks.
    """
    report = {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "data_summary": {
            "total_transactions": len(df),
            "date_range": f"{df['date'].min().date()} to {df['date'].max().date()}",
            "sources": df["source"].value_counts().to_dict(),
        },
    }

    # Credit card reconciliation
    cc_recon = reconcile_all_credit_card_payments(df)
    report["credit_card_reconciliation"] = cc_recon

    # Payment app reconciliation
    app_recon = get_payment_app_reconciliation(df)
    report["payment_app_reconciliation"] = app_recon

    # Monthly cash flow
    cash_flow = get_monthly_cash_flow_summary(df)
    report["monthly_cash_flow"] = cash_flow

    # Potential duplicates
    duplicates = find_duplicate_transactions(df)
    report["potential_duplicates"] = duplicates

    # Unreconciled items
    unreconciled = get_unreconciled_items(df)
    report["unreconciled_items"] = unreconciled

    return report
