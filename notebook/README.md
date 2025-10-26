# Accounting & Reconciliation Notebook

## Overview

The `accounting.ipynb` notebook provides comprehensive financial reconciliation between banking statements and credit card transactions.

## Quick Start

```bash
# From project root
cd notebook
jupyter notebook accounting.ipynb
```

## What It Does

### 1. Credit Card Payment Reconciliation
- Matches credit card payments from bank to actual credit card spending
- Identifies payment amounts vs spending in prior 45 days
- Flags mismatches for review

### 2. Payment App Reconciliation
- Compares PayPal/Venmo transfers from bank to actual app transactions
- Monthly breakdown of transfers vs spending
- Helps identify pass-through transactions

### 3. Monthly Cash Flow Analysis
- Tracks income vs expenses from bank account
- Net cash flow by month
- Identifies saving/deficit months

### 4. Duplicate Detection
- Finds potential duplicate transactions across sources
- Checks for same amount within 24 hours
- Cross-source comparison

### 5. Income Analysis
- Tracks rental income from Apollo Management
- Separates income by source
- Monthly income trends

### 6. Rent Analysis
- Apollo Management = Rent Received (income)
- ManageGo = Rent Paid (expense)
- Net rent income calculation

## Reconciliation Functions (`recon.py`)

### Core Functions

```python
from plan_a import recon

# Load all transactions
df = recon.load_all_transactions()

# Credit card reconciliation
cc_recon = recon.reconcile_all_credit_card_payments(df)

# Payment app reconciliation
app_recon = recon.get_payment_app_reconciliation(df)

# Monthly cash flow
cash_flow = recon.get_monthly_cash_flow_summary(df)

# Find duplicates
duplicates = recon.find_duplicate_transactions(df)

# Generate full report
report = recon.generate_reconciliation_report(df)
```

## Key Insights

### Understanding Reconciliation Status

- **MATCH**: Difference < $1 (perfect match)
- **REVIEW**: Difference < $100 (small variance)
- **MISMATCH**: Difference ≥ $100 (needs attention)

### Common Discrepancies

1. **Credit Card Payments**
   - Billing cycles don't align with calendar months
   - Payments may include previous balance
   - Some spending may be on current statement not yet paid

2. **Payment Apps**
   - PayPal transactions may use credit card directly
   - Venmo can pull from bank or credit card
   - Some transactions use app balance

3. **Timing Differences**
   - Transaction date vs posting date
   - Pending vs cleared transactions
   - Weekend/holiday delays

## Merchant Categorization

### Income Sources
- `APOLLOMANAGEMENPAYMENT` → Rental Income
- Payroll/salary deposits
- Other transfers

### Rent Expenses
- `MANAGEGOPROTECTEDM` → Rent payment for one property
- `MANAGEGONORTHFLAT` → Rent payment for another property

### Banking Operations
- `PAYPAL` / `VENMO` → Payment app transfers
- `ROBINHOOD` → Investment account transfers
- `APPLECARD GSBANK` → Credit card payments
- ATM withdrawals

## Data Flow

```
1. Parse PDFs → Raw CSV
   bank_etl.parse_all_td_bank_pdfs()

2. Normalize & Combine → Unified Format
   cc_etl.combine_all_transactions()

3. Reconcile & Analyze → Insights
   recon.generate_reconciliation_report()

4. Visualize → Notebook
   accounting.ipynb
```

## File Locations

- Raw Data: `data/raw/<source>/`
- Processed: `data/processed/all_transactions.csv`
- Notebook: `notebook/accounting.ipynb`
- Functions: `python/src/plan_a/recon.py`

## Troubleshooting

### If reconciliation shows many mismatches:

1. Check date ranges - are all statement periods included?
2. Verify credit card billing cycles
3. Look for pending transactions not yet posted
4. Check if payments include previous balance

### If notebook can't find modules:

```python
import sys
from pathlib import Path
sys.path.insert(0, str(Path.cwd().parent / 'python' / 'src'))
```

## Next Steps

1. Run the full ETL pipeline first
2. Open the notebook
3. Execute cells in order
4. Review reconciliation results
5. Investigate any MISMATCH items
6. Update categorization rules as needed

## Requirements

- pandas
- matplotlib
- seaborn
- jupyter
- All dependencies in `pyproject.toml`

Install with: `uv sync`
