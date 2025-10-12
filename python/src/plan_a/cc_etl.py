"""Credit card ETL module for normalizing and combining transaction data."""

import pandas as pd
from pathlib import Path
from typing import List, Dict
import json


def normalize_apple_card(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize Apple Card transaction data."""
    normalized = pd.DataFrame({
        'date': pd.to_datetime(df['Transaction Date']),
        'time': None,  # Apple Card doesn't provide time
        'description': df['Description'].astype(str),
        'merchant': df['Merchant'].astype(str),
        'category': df['Category'].astype(str),
        'type': df['Type'].astype(str),
        'amount': pd.to_numeric(df['Amount (USD)'].astype(str).str.replace(',', ''), errors='coerce'),
        'source': 'apple_card',
        'status': 'Posted',  # Apple Card exports are all posted
        'additional_info': df.apply(
            lambda row: json.dumps({
                'clearing_date': str(row['Clearing Date']),
                'purchased_by': str(row['Purchased By'])
            }), 
            axis=1
        )
    })
    return normalized


def normalize_venmo(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize Venmo transaction data."""
    # Venmo has a complex header structure - skip first 3 rows and last disclaimer rows
    # Filter out empty ID rows and disclaimer text
    df = df[df['ID'].notna() & (df['ID'] != '')].copy()
    df['ID'] = df['ID'].astype(str)
    df = df[df['ID'].str.match(r'^\d', na=False)].copy()  # Keep only rows starting with digits
    
    # Parse datetime
    df['parsed_datetime'] = pd.to_datetime(df['Datetime'], errors='coerce')
    
    # Convert Amount (total) to numeric, handling + and - signs
    amount_str = df['Amount (total)'].astype(str).str.replace(r'[\$,\s]', '', regex=True)
    is_negative = amount_str.str.startswith('-')
    amount_numeric = pd.to_numeric(amount_str.str.replace(r'[+\-]', '', regex=True), errors='coerce')
    amount_numeric = amount_numeric * is_negative.map({True: -1, False: 1})
    
    normalized = pd.DataFrame({
        'date': df['parsed_datetime'].dt.date,
        'time': df['parsed_datetime'].dt.time,
        'description': df['Note'].fillna('').astype(str),
        'merchant': df.apply(
            lambda row: f"{str(row['From'])} → {str(row['To'])}" if pd.notna(row['From']) and pd.notna(row['To']) else '',
            axis=1
        ),
        'category': df['Type'].astype(str),
        'type': df['Type'].astype(str),
        'amount': amount_numeric,
        'source': 'venmo',
        'status': df['Status'].astype(str),
        'additional_info': df.apply(
            lambda row: json.dumps({
                'id': str(row['ID']),
                'funding_source': str(row.get('Funding Source', '')),
                'destination': str(row.get('Destination', '')),
                'fee': str(row.get('Amount (fee)', ''))
            }),
            axis=1
        )
    })
    return normalized


def normalize_robinhood(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize Robinhood transaction data."""
    # Combine Date and Time
    df['datetime'] = pd.to_datetime(
        df['Date'].astype(str) + ' ' + df['Time'].astype(str),
        format='%Y-%m-%d %I:%M %p',
        errors='coerce'
    )
    
    normalized = pd.DataFrame({
        'date': df['datetime'].dt.date,
        'time': df['datetime'].dt.time,
        'description': df['Description'].fillna('').astype(str),
        'merchant': df['Merchant'].astype(str),
        'category': df['Type'].astype(str),
        'type': df['Type'].astype(str),
        'amount': pd.to_numeric(df['Amount'], errors='coerce'),
        'source': 'robinhood',
        'status': df['Status'].astype(str),
        'additional_info': df.apply(
            lambda row: json.dumps({
                'cardholder': str(row['Cardholder']),
                'points': str(row.get('Points', '')),
                'balance': str(row.get('Balance', ''))
            }),
            axis=1
        )
    })
    return normalized


def load_and_normalize_csv(file_path: Path, source_type: str) -> pd.DataFrame:
    """Load and normalize a CSV file based on its source type."""
    try:
        if source_type == 'apple':
            df = pd.read_csv(file_path)
            return normalize_apple_card(df)
        elif source_type == 'venmo':
            # Venmo has complex headers - read raw and process
            df = pd.read_csv(file_path, skiprows=2)
            return normalize_venmo(df)
        elif source_type == 'robinhood':
            df = pd.read_csv(file_path)
            return normalize_robinhood(df)
        else:
            print(f"Unknown source type: {source_type}")
            return pd.DataFrame()
    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        return pd.DataFrame()


def combine_all_transactions(raw_data_dir: str = './data/raw', 
                            output_file: str = './data/processed/all_transactions.csv') -> None:
    """
    Normalize and combine all transaction CSV files from different sources.
    
    Args:
        raw_data_dir: Directory containing subdirectories with raw CSV files
        output_file: Path to the output combined CSV file
    """
    raw_path = Path(raw_data_dir)
    output_path = Path(output_file)
    
    # Ensure output directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    all_transactions: List[pd.DataFrame] = []
    
    # Map of subdirectory names to their source types
    source_mapping = {
        'apple': 'apple',
        'venmo': 'venmo',
        'robinhood': 'robinhood',
        'td_bank': 'td_bank',
        'paypal': 'paypal'
    }
    
    # Process each subdirectory
    for subdir_name, source_type in source_mapping.items():
        subdir = raw_path / subdir_name
        
        if not subdir.exists():
            print(f"Directory not found: {subdir}")
            continue
            
        # Find all CSV files in this subdirectory
        csv_files = list(subdir.glob('*.csv'))
        
        if not csv_files:
            print(f"No CSV files found in {subdir}")
            continue
        
        print(f"\nProcessing {len(csv_files)} files from {subdir_name}...")
        
        for csv_file in csv_files:
            print(f"  - {csv_file.name}")
            normalized_df = load_and_normalize_csv(csv_file, source_type)
            
            if not normalized_df.empty:
                all_transactions.append(normalized_df)
    
    # Combine all transactions
    if all_transactions:
        combined_df = pd.concat(all_transactions, ignore_index=True)
        
        # Sort by date (most recent first)
        combined_df['date'] = pd.to_datetime(combined_df['date'])
        combined_df = combined_df.sort_values('date', ascending=False)
        
        # Save to CSV
        combined_df.to_csv(output_path, index=False)
        
        print(f"\n{'='*60}")
        print(f"Successfully combined {len(combined_df)} transactions")
        print(f"Output saved to: {output_path}")
        print(f"{'='*60}")
        print(f"\nTransactions by source:")
        print(combined_df['source'].value_counts())
        print(f"\nDate range: {combined_df['date'].min()} to {combined_df['date'].max()}")
    else:
        print("No transactions found to combine.")


if __name__ == '__main__':
    combine_all_transactions()

