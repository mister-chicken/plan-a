"""Credit card analysis helper functions."""

import pandas as pd
import re
from typing import Dict, List
from pathlib import Path


def load_transactions(file_path: str = './data/processed/all_transactions.csv') -> pd.DataFrame:
    """Load and prepare transaction data for analysis."""
    df = pd.read_csv(file_path)
    df['date'] = pd.to_datetime(df['date'])
    df['year_month'] = df['date'].dt.to_period('M')
    df['amount'] = pd.to_numeric(df['amount'], errors='coerce')
    return df


def categorize_transaction(row: pd.Series) -> str:
    """
    Categorize a transaction into analysis categories.
    
    Categories: rent, utilities, dining_out, groceries, subway, taxi, shopping, subscriptions, home, other
    """
    merchant = str(row['merchant']).lower()
    description = str(row['description']).lower()
    category = str(row['category']).lower()
    amount = row['amount']
    
    # Rent - typically large recurring payments
    rent_keywords = ['rent', 'lease', 'apartment', 'building management', 'property management']
    if any(keyword in merchant or keyword in description for keyword in rent_keywords):
        return 'rent'
    
    # Check for large recurring payments that might be rent (typically $1000+)
    if amount > 1000 and category in ['payment', 'debit', 'standard transfer']:
        return 'rent'
    
    # Utilities
    utility_keywords = ['electric', 'electricity', 'gas', 'water', 'internet', 'wifi', 'cable', 
                        'phone', 'mobile', 'verizon', 'at&t', 'tmobile', 'sprint', 'con ed', 
                        'coned', 'utility', 'national grid']
    if any(keyword in merchant or keyword in description for keyword in utility_keywords):
        return 'utilities'
    
    # Dining Out (restaurants + alcohol)
    if category in ['restaurants', 'alcohol']:
        return 'dining_out'
    
    # Groceries
    if category == 'grocery':
        return 'groceries'
    
    # Also check for common grocery stores in merchant names
    grocery_keywords = ['grocery', 'supermarket', 'whole foods', 'wholefds', 'trader joe', 
                        'food market', 'deli', 'bodega', 'market']
    if any(keyword in merchant for keyword in grocery_keywords):
        return 'groceries'
    
    # Subway
    subway_keywords = ['nyct', 'mta', 'metro', 'subway', 'transit']
    if any(keyword in merchant or keyword in description for keyword in subway_keywords):
        return 'subway'
    
    # Taxi/Rideshare
    taxi_keywords = ['uber', 'lyft', 'taxi', 'cab', 'via', 'curb']
    if any(keyword in merchant or keyword in description for keyword in taxi_keywords):
        return 'taxi'
    
    # Subscriptions
    subscription_keywords = ['subscription', 'netflix', 'spotify', 'apple.com', 'amazon prime', 
                            'hulu', 'disney', 'hbo', 'youtube premium', 'annual subscription',
                            'monthly subscription', 'gym', 'fitness', 'membership']
    if any(keyword in merchant or keyword in description for keyword in subscription_keywords):
        return 'subscriptions'
    
    # Check for recurring small charges that might be subscriptions
    if 5 <= amount <= 50 and 'subscription' in description:
        return 'subscriptions'
    
    # Shopping
    if category == 'shopping':
        return 'shopping'
    
    shopping_keywords = ['amazon', 'target', 'walmart', 'clothing', 'apparel', 'fashion',
                        'store', 'retail', 'bloomingdale', 'macy', 'nordstrom', 'adidas',
                        'nike', 'uniqlo', 'zara', 'h&m']
    if any(keyword in merchant or keyword in description for keyword in shopping_keywords):
        return 'shopping'
    
    # Home (furniture, home improvement, cleaning, etc.)
    home_keywords = ['furniture', 'ikea', 'home depot', 'lowes', 'bed bath', 'cleaner', 
                     'hardware', 'home improvement', 'cleaning', 'laundry', 'dry clean']
    if any(keyword in merchant or keyword in description for keyword in home_keywords):
        return 'home'
    
    # Default to other
    return 'other'


def add_analysis_categories(df: pd.DataFrame) -> pd.DataFrame:
    """Add analysis category column to transactions dataframe."""
    df = df.copy()
    df['analysis_category'] = df.apply(categorize_transaction, axis=1)
    return df


def get_monthly_spending_by_category(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate monthly spending by analysis category.
    
    Returns a pivot table with months as rows and categories as columns.
    """
    df = add_analysis_categories(df)
    
    # Only include expenses (positive amounts)
    expenses = df[df['amount'] > 0].copy()
    
    # Group by month and category
    monthly = expenses.groupby(['year_month', 'analysis_category'])['amount'].sum().reset_index()
    
    # Pivot to wide format
    pivot = monthly.pivot(index='year_month', columns='analysis_category', values='amount')
    pivot = pivot.fillna(0)
    
    # Convert period index to timestamp for better plotting
    pivot.index = pivot.index.to_timestamp()
    
    return pivot


def get_category_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Get summary statistics for each analysis category."""
    df = add_analysis_categories(df)
    expenses = df[df['amount'] > 0].copy()
    
    summary = expenses.groupby('analysis_category')['amount'].agg([
        ('total', 'sum'),
        ('count', 'count'),
        ('average', 'mean'),
        ('median', 'median'),
        ('min', 'min'),
        ('max', 'max')
    ]).round(2)
    
    summary = summary.sort_values('total', ascending=False)
    return summary


def get_top_merchants_by_category(df: pd.DataFrame, category: str, n: int = 10) -> pd.DataFrame:
    """Get top N merchants for a specific analysis category."""
    df = add_analysis_categories(df)
    expenses = df[(df['amount'] > 0) & (df['analysis_category'] == category)].copy()
    
    merchant_summary = expenses.groupby('merchant')['amount'].agg([
        ('total_spent', 'sum'),
        ('num_transactions', 'count')
    ]).round(2)
    
    merchant_summary = merchant_summary.sort_values('total_spent', ascending=False).head(n)
    return merchant_summary


def get_spending_trends(df: pd.DataFrame, categories: List[str] = None) -> pd.DataFrame:
    """
    Calculate spending trends over time for specified categories.
    
    Args:
        df: Transaction dataframe
        categories: List of categories to include. If None, includes all.
    
    Returns:
        DataFrame with monthly spending trends
    """
    monthly_data = get_monthly_spending_by_category(df)
    
    if categories:
        # Filter to only requested categories
        available_categories = [cat for cat in categories if cat in monthly_data.columns]
        monthly_data = monthly_data[available_categories]
    
    return monthly_data


def calculate_category_percentages(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate what percentage of total spending each category represents per month."""
    monthly_data = get_monthly_spending_by_category(df)
    monthly_data['total'] = monthly_data.sum(axis=1)
    
    # Calculate percentages
    for col in monthly_data.columns:
        if col != 'total':
            monthly_data[f'{col}_pct'] = (monthly_data[col] / monthly_data['total'] * 100).round(2)
    
    return monthly_data


