#!/usr/bin/env python3
"""
Schema Validation Script

This script compares the expected schema from revenue_forecasting_train.py
with the actual CSV file structure to validate data format compatibility.
"""

import pandas as pd
import re
from datetime import datetime
from pathlib import Path


def get_expected_schema():
    """Return the expected schema from revenue_forecasting_train.py"""
    schema = {
        "location_id": "INT",
        "year_month": "DATE",
        **{f"feature_{i:03d}": "FLOAT" for i in range(0, 50)},
        "days_in_month": "INT",
        "revenue": "FLOAT"
    }
    return schema


def analyze_csv_file(csv_path):
    """Analyze the CSV file and return column information"""
    try:
        # Read only the header and first few rows for analysis
        df = pd.read_csv(csv_path, nrows=5)
        
        csv_info = {
            'columns': list(df.columns),
            'column_count': len(df.columns),
            'sample_data': {}
        }
        
        # Get sample data for type analysis
        for col in df.columns:
            csv_info['sample_data'][col] = df[col].iloc[0] if len(df) > 0 else None
            
        return csv_info
        
    except Exception as e:
        print(f"Error reading CSV file: {e}")
        return None


def infer_data_type(value, column_name):
    """Infer the data type of a value"""
    if pd.isna(value):
        return "NULL"
    
    # Check for date format (YYYY-MM)
    if isinstance(value, str) and re.match(r'^\d{4}-\d{2}$', value):
        return "DATE"
    
    # Check if it's a numeric type (int or float)
    if isinstance(value, (int, float)):
        # If it's already a float type or has decimal places, it's FLOAT
        if isinstance(value, float) or (isinstance(value, str) and '.' in str(value)):
            return "FLOAT"
        else:
            return "INT"
    
    # Check for string representation of numbers
    try:
        float_val = float(value)
        # If it has decimal places or scientific notation, it's a float
        if '.' in str(value) or 'e' in str(value).lower() or not float_val.is_integer():
            return "FLOAT"
        else:
            return "INT"
    except (ValueError, TypeError):
        pass
    
    return "STRING"


def compare_schemas(expected_schema, csv_info):
    """Compare expected schema with actual CSV structure"""
    print("=" * 60)
    print("SCHEMA VALIDATION REPORT")
    print("=" * 60)
    
    # Check column count
    expected_cols = len(expected_schema)
    actual_cols = csv_info['column_count']
    
    print(f"\n📊 COLUMN COUNT COMPARISON:")
    print(f"   Expected columns: {expected_cols}")
    print(f"   Actual columns:   {actual_cols}")
    print(f"   Match: {'✅ YES' if expected_cols == actual_cols else '❌ NO'}")
    
    # Check feature columns specifically
    expected_features = [col for col in expected_schema.keys() if col.startswith('feature_')]
    actual_features = [col for col in csv_info['columns'] if col.startswith('feature_')]
    
    print(f"\n🎯 FEATURE COLUMNS:")
    print(f"   Expected features: {len(expected_features)} (feature_000 to feature_049)")
    print(f"   Actual features:   {len(actual_features)}")
    print(f"   Match: {'✅ YES' if len(expected_features) == len(actual_features) else '❌ NO'}")
    
    # Detailed column comparison
    print(f"\n📋 DETAILED COLUMN COMPARISON:")
    print(f"{'Column Name':<15} {'Expected Type':<12} {'Actual Type':<12} {'Sample Value':<20} {'Match'}")
    print("-" * 80)
    
    all_match = True
    
    for col_name, expected_type in expected_schema.items():
        if col_name in csv_info['columns']:
            sample_value = csv_info['sample_data'].get(col_name)
            inferred_type = infer_data_type(sample_value, col_name)
            
            # Special handling for type matching
            type_match = (
                (expected_type == inferred_type) or
                (expected_type == "DATE" and inferred_type == "STRING" and 
                 isinstance(sample_value, str) and re.match(r'^\d{4}-\d{2}$', str(sample_value)))
            )
            
            match_symbol = "✅" if type_match else "❌"
            if not type_match:
                all_match = False
                
            print(f"{col_name:<15} {expected_type:<12} {inferred_type:<12} {str(sample_value):<20} {match_symbol}")
        else:
            print(f"{col_name:<15} {expected_type:<12} {'MISSING':<12} {'N/A':<20} ❌")
            all_match = False
    
    # Check for unexpected columns in CSV
    unexpected_cols = set(csv_info['columns']) - set(expected_schema.keys())
    if unexpected_cols:
        print(f"\n⚠️  UNEXPECTED COLUMNS IN CSV:")
        for col in unexpected_cols:
            sample_value = csv_info['sample_data'].get(col)
            inferred_type = infer_data_type(sample_value, col)
            print(f"   {col} ({inferred_type}): {sample_value}")
        all_match = False
    
    # Summary
    print(f"\n" + "=" * 60)
    print(f"OVERALL COMPATIBILITY: {'✅ COMPATIBLE' if all_match else '❌ ISSUES FOUND'}")
    print("=" * 60)
    
    return all_match


def main():
    """Main function to run the schema validation"""
    csv_path = "/Users/moritzberthold/Desktop/dev/data-ingestors/data/monthly_rev.csv"
    
    print("Schema Validation Tool")
    print(f"Analyzing CSV file: {csv_path}")
    
    # Get expected schema
    expected_schema = get_expected_schema()
    
    # Analyze CSV file
    csv_info = analyze_csv_file(csv_path)
    
    if csv_info is None:
        print("❌ Failed to analyze CSV file")
        return
    
    # Compare schemas
    is_compatible = compare_schemas(expected_schema, csv_info)
    
    # Additional analysis
    print(f"\n🔍 ADDITIONAL ANALYSIS:")
    print(f"   CSV file exists: ✅")
    print(f"   File readable: ✅")
    print(f"   Header row present: ✅")
    
    # Feature numbering check
    actual_features = [col for col in csv_info['columns'] if col.startswith('feature_')]
    if actual_features:
        # Extract feature numbers
        feature_nums = []
        for feat in actual_features:
            match = re.search(r'feature_(\d+)', feat)
            if match:
                feature_nums.append(int(match.group(1)))
        
        if feature_nums:
            feature_nums.sort()
            expected_range = list(range(0, 50))
            actual_range = feature_nums
            
            print(f"   Feature range: {min(feature_nums)}-{max(feature_nums)} (expected: 0-49)")
            print(f"   Sequential features: {'✅' if actual_range == expected_range else '❌'}")
            
            if actual_range != expected_range:
                missing = set(expected_range) - set(actual_range)
                extra = set(actual_range) - set(expected_range)
                if missing:
                    print(f"   Missing features: {sorted(missing)}")
                if extra:
                    print(f"   Extra features: {sorted(extra)}")


if __name__ == "__main__":
    main()
