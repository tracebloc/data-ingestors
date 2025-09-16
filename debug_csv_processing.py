#!/usr/bin/env python3
"""
Debug CSV Processing Script

This script investigates the "Unconsumed column names" error by examining
how the CSV data is being processed and identifying potential issues.
"""

import pandas as pd
import logging
from pathlib import Path
import re

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


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


def diagnose_csv_issues(csv_path):
    """Comprehensive diagnosis of CSV processing issues"""
    print("=" * 70)
    print("CSV PROCESSING DIAGNOSTIC TOOL")
    print("=" * 70)
    
    try:
        # Read CSV with different methods to identify issues
        print(f"\n🔍 Analyzing: {csv_path}")
        
        # 1. Basic read
        print("\n1️⃣ BASIC CSV READ:")
        df = pd.read_csv(csv_path, nrows=5)
        print(f"   Shape: {df.shape}")
        print(f"   Columns count: {len(df.columns)}")
        
        # 2. Check for unnamed columns
        print("\n2️⃣ UNNAMED COLUMNS CHECK:")
        unnamed_cols = [col for col in df.columns if 'Unnamed' in str(col)]
        if unnamed_cols:
            print(f"   ❌ Found unnamed columns: {unnamed_cols}")
        else:
            print("   ✅ No unnamed columns found")
        
        # 3. Check column names for whitespace/special chars
        print("\n3️⃣ COLUMN NAME ANALYSIS:")
        issues_found = False
        for i, col in enumerate(df.columns):
            original_col = repr(col)
            stripped_col = col.strip()
            
            if col != stripped_col:
                print(f"   ❌ Column {i}: Whitespace issue: {original_col}")
                issues_found = True
            elif not col.replace('_', '').replace('-', '').isalnum():
                # Check for special characters (excluding underscore and hyphen)
                special_chars = [c for c in col if not c.isalnum() and c not in ['_', '-']]
                if special_chars:
                    print(f"   ⚠️  Column {i}: Special characters found: {original_col} -> {special_chars}")
                    issues_found = True
        
        if not issues_found:
            print("   ✅ All column names look clean")
        
        # 4. Check for duplicate column names
        print("\n4️⃣ DUPLICATE COLUMNS CHECK:")
        duplicates = df.columns[df.columns.duplicated()].tolist()
        if duplicates:
            print(f"   ❌ Duplicate columns found: {duplicates}")
        else:
            print("   ✅ No duplicate columns")
        
        # 5. Schema comparison
        print("\n5️⃣ SCHEMA COMPATIBILITY:")
        expected_schema = get_expected_schema()
        csv_columns = set(df.columns)
        schema_columns = set(expected_schema.keys())
        
        missing_in_csv = schema_columns - csv_columns
        extra_in_csv = csv_columns - schema_columns
        
        if missing_in_csv:
            print(f"   ❌ Missing from CSV: {sorted(missing_in_csv)}")
        if extra_in_csv:
            print(f"   ❌ Extra in CSV: {sorted(extra_in_csv)}")
        if not missing_in_csv and not extra_in_csv:
            print("   ✅ Perfect schema match")
        
        # 6. Feature column analysis
        print("\n6️⃣ FEATURE COLUMNS ANALYSIS:")
        feature_cols = [col for col in df.columns if col.startswith('feature_')]
        print(f"   Found {len(feature_cols)} feature columns")
        
        # Check for gaps in feature numbering
        feature_numbers = []
        for col in feature_cols:
            match = re.search(r'feature_(\d+)', col)
            if match:
                feature_numbers.append(int(match.group(1)))
        
        if feature_numbers:
            feature_numbers.sort()
            expected_range = list(range(0, 50))
            missing_features = set(expected_range) - set(feature_numbers)
            extra_features = set(feature_numbers) - set(expected_range)
            
            if missing_features:
                print(f"   ❌ Missing features: {sorted(missing_features)}")
            if extra_features:
                print(f"   ❌ Extra features: {sorted(extra_features)}")
            if not missing_features and not extra_features:
                print("   ✅ All expected features present (0-49)")
        
        # 7. Data type analysis
        print("\n7️⃣ DATA TYPE ANALYSIS:")
        print("   Column types in CSV:")
        for col, dtype in df.dtypes.items():
            print(f"     {col}: {dtype}")
        
        # 8. Simulate pandas operations that might cause issues
        print("\n8️⃣ PANDAS OPERATIONS TEST:")
        try:
            # Test column selection
            test_cols = ['location_id', 'year_month', 'feature_000', 'feature_020', 'feature_022', 'feature_018']
            available_cols = [col for col in test_cols if col in df.columns]
            subset_df = df[available_cols]
            print(f"   ✅ Column subset selection works: {len(available_cols)} columns")
        except Exception as e:
            print(f"   ❌ Column subset selection failed: {e}")
        
        try:
            # Test column renaming
            rename_dict = {col: col.strip() for col in df.columns}
            renamed_df = df.rename(columns=rename_dict)
            print("   ✅ Column renaming works")
        except Exception as e:
            print(f"   ❌ Column renaming failed: {e}")
        
        # 9. Check the specific columns mentioned in the error
        print("\n9️⃣ ERROR-SPECIFIC ANALYSIS:")
        error_columns = ['feature_020', 'feature_022', 'feature_018']
        for col in error_columns:
            if col in df.columns:
                print(f"   ✅ {col}: Present in CSV")
                print(f"      Sample value: {df[col].iloc[0] if len(df) > 0 else 'N/A'}")
                print(f"      Data type: {df[col].dtype}")
            else:
                print(f"   ❌ {col}: NOT found in CSV")
                # Look for similar columns
                similar = [c for c in df.columns if col.replace('_', '') in c.replace('_', '')]
                if similar:
                    print(f"      Similar columns: {similar}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error during analysis: {e}")
        return False


def test_csv_ingestor_simulation():
    """Simulate the CSV ingestor processing to identify where the error occurs"""
    print("\n" + "=" * 70)
    print("CSV INGESTOR SIMULATION")
    print("=" * 70)
    
    csv_path = "/Users/moritzberthold/Desktop/dev/data-ingestors/data/monthly_revenue_train.csv"
    
    try:
        # Simulate the CSV processing from the ingestor
        print("\n🔄 Simulating Ingestor processing...")
        
        # Read with the same options as the ingestor
        csv_options = {
            'dtype': None,
            'keep_default_na': False,
            'na_values': [''],
            'encoding': 'utf-8',
            'on_bad_lines': 'warn',
            'low_memory': False,
            'engine': 'c'
        }
        
        df = pd.read_csv(csv_path, nrows=10, **csv_options)
        
        # Strip column names (as done in the ingestor)
        print(f"   Original columns: {list(df.columns)}")
        df.columns = df.columns.str.strip()
        print(f"   After stripping: {list(df.columns)}")
        
        # Check for schema validation
        schema = get_expected_schema()
        common_columns = set(schema.keys()) & set(df.columns)
        missing_columns = set(schema.keys()) - set(df.columns)
        
        print(f"   Common columns: {len(common_columns)}")
        print(f"   Missing columns: {missing_columns}")
        
        # Test the type conversion process
        print("\n🔄 Testing type conversions...")
        for column in common_columns:
            dtype = schema[column]
            try:
                if 'INT' in dtype.upper():
                    df[column] = pd.to_numeric(df[column], downcast='integer')
                elif 'FLOAT' in dtype.upper():
                    df[column] = pd.to_numeric(df[column], downcast='float')
                elif 'DATE' in dtype.upper():
                    df[column] = pd.to_datetime(df[column])
                print(f"   ✅ {column}: {dtype} conversion successful")
            except Exception as e:
                print(f"   ❌ {column}: {dtype} conversion failed - {e}")
        
        print("\n✅ CSV ingestor simulation completed successfully")
        return True
        
    except Exception as e:
        print(f"❌ CSV ingestor simulation failed: {e}")
        return False


def main():
    """Run the complete diagnostic"""
    csv_path = "/Users/moritzberthold/Desktop/dev/data-ingestors/data/monthly_revenue_train.csv"
    
    print("Starting comprehensive CSV diagnostic...")
    
    # Run diagnostics
    basic_success = diagnose_csv_issues(csv_path)
    simulation_success = test_csv_ingestor_simulation()
    
    print("\n" + "=" * 70)
    print("DIAGNOSTIC SUMMARY")
    print("=" * 70)
    print(f"Basic CSV analysis: {'✅ PASSED' if basic_success else '❌ FAILED'}")
    print(f"Ingestor simulation: {'✅ PASSED' if simulation_success else '❌ FAILED'}")
    
    if basic_success and simulation_success:
        print("\n🎉 No obvious issues found in CSV processing.")
        print("The 'Unconsumed column names' error might be occurring in:")
        print("   - Database insertion process")
        print("   - API transmission")
        print("   - Record processing/filtering")
        print("\nRecommendation: Check the actual ingestion logs for more context.")
    else:
        print("\n⚠️  Issues detected that could cause the error.")
        print("Review the analysis above for specific problems to fix.")


if __name__ == "__main__":
    main()
