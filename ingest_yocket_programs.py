#!/usr/bin/env python3
"""
Script to ingest Yocket programs data from merged Excel file into database.
"""

import os
import sys
from pathlib import Path
from datetime import datetime

import numpy as np
import pandas as pd
from sqlalchemy import MetaData, create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from urllib.parse import quote_plus
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def get_db_engine():
    """Create database engine for standalone script (SQL Server)."""
    server = os.getenv("DB_SERVER", "localhost,1433")
    database = os.getenv("DB_NAME")
    username = os.getenv("DB_USERNAME")
    password = os.getenv("DB_PASSWORD")
    driver = os.getenv("DB_DRIVER", "ODBC Driver 18 for SQL Server")
    connection_timeout = int(os.getenv("DB_CONNECTION_TIMEOUT", "60"))
    login_timeout = int(os.getenv("DB_LOGIN_TIMEOUT", "60"))
    
    if not all([database, username, password]):
        print("Error: Database credentials not set. Please set DB_SERVER, DB_NAME, DB_USERNAME, and DB_PASSWORD.")
        return None
    
    odbc_params = (
        f"Driver={driver};"
        f"Server={server};"
        f"Database={database};"
        f"UID={username};"
        f"PWD={password};"
        f"Connection Timeout={connection_timeout};"
        f"Login Timeout={login_timeout};"
        "Encrypt=no;"
        "TrustServerCertificate=yes;"
    )
    
    connection_url = f"mssql+pyodbc:///?odbc_connect={quote_plus(odbc_params)}"
    
    try:
        engine = create_engine(
            connection_url,
            pool_pre_ping=True,
            connect_args={
                "timeout": connection_timeout,
            },
            pool_timeout=connection_timeout,
        )
        return engine
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return None

def main():
    """Main function to ingest Yocket programs data."""
    print("="*80)
    print("YOCKET PROGRAMS DATA INGESTION")
    print("="*80)
    
    # Find merged Excel file
    base_dir = Path(__file__).resolve().parent
    excel_file = base_dir / "yocket_courses_merged.xlsx"
    
    if not excel_file.exists():
        print(f"❌ Merged Excel file not found: {excel_file}")
        print("Please run merge_yocket_courses.py first to create the merged file.")
        sys.exit(1)
    
    print(f"\nReading Excel file: {excel_file}")
    
    try:
        df = pd.read_excel(excel_file)
        print(f"✓ Loaded {len(df)} rows from Excel")
    except Exception as e:
        print(f"❌ Error reading Excel file: {e}")
        sys.exit(1)
    
    if df.empty:
        print("❌ Excel file is empty")
        sys.exit(1)
    
    # Connect to database
    print("\nConnecting to database...")
    engine = get_db_engine()
    if not engine:
        print("❌ Failed to connect to database. Exiting.")
        sys.exit(1)
    
    print("✓ Connected to database successfully")
    
    try:
        # Map Excel columns to database columns
        column_mapping = {
            'university_id': 'UniversityID',
            'university_slug': 'UniversitySlug',
            'university_course_id': 'UniversityCourseID',
            'credential': 'Credential',
            'university_course_name': 'UniversityCourseName',
            'school_name': 'SchoolName',
            'course_level': 'CourseLevel',
            'converted_tuition_fee': 'ConvertedTuitionFee',
            'duration': 'Duration',
            'is_fee_waived': 'IsFeeWaived',
            'actual_tuition_fee': 'ActualTuitionFee',
            'level': 'Level',
            'is_partner': 'IsPartner',
            'deadline_dates': 'DeadlineDates',
            'deadline_types': 'DeadlineTypes',
            'source_file': 'SourceFile'
        }
        
        # Prepare data for insertion
        db_df = pd.DataFrame()
        for excel_col, db_col in column_mapping.items():
            if excel_col in df.columns:
                db_df[db_col] = df[excel_col]
            else:
                db_df[db_col] = None
        
        # Convert boolean columns
        bool_columns = ['IsFeeWaived', 'IsPartner']
        for col in bool_columns:
            if col in db_df.columns:
                db_df[col] = db_df[col].fillna(False).astype(bool)
        
        # Convert numeric columns
        numeric_columns = ['ConvertedTuitionFee', 'ActualTuitionFee', 'Duration', 'Level']
        for col in numeric_columns:
            if col in db_df.columns:
                db_df[col] = pd.to_numeric(db_df[col], errors='coerce')
        
        # Convert to string where needed
        string_columns = ['UniversityID', 'UniversitySlug', 'UniversityCourseID', 'Credential', 
                         'UniversityCourseName', 'SchoolName', 'CourseLevel', 'DeadlineDates', 
                         'DeadlineTypes', 'SourceFile']
        for col in string_columns:
            if col in db_df.columns:
                db_df[col] = db_df[col].astype(str).replace('nan', None)
        
        # Clean university names - convert hyphens to spaces and title case
        if 'UniversitySlug' in db_df.columns:
            def clean_university_name(slug):
                if pd.isna(slug) or slug is None or slug == 'None':
                    return None
                # Convert hyphens to spaces and title case
                cleaned = str(slug).replace('-', ' ').title()
                return cleaned
            db_df['UniversitySlug'] = db_df['UniversitySlug'].apply(clean_university_name)
        
        print(f"\nPrepared {len(db_df)} rows for insertion")
        print(f"Columns: {', '.join(db_df.columns)}")
        
        # Replace NaN/NA with None for DB insertion
        db_df = db_df.replace({np.nan: None})
        
        # Insert data into database using SQLAlchemy Core (avoids pandas multi-insert issues)
        print("\nInserting data into YocketPrograms table...")
        
        metadata = MetaData()
        metadata.reflect(bind=engine, only=["YocketPrograms"])
        yocket_table = metadata.tables.get("YocketPrograms")
        
        if yocket_table is None:
            print("❌ YocketPrograms table not found in database. Did you run create_yocket_programs_table.sql?")
            sys.exit(1)
        
        rows_inserted = 0
        chunk_size = 1000
        
        with engine.begin() as conn:
            # Clear existing data (optional - comment out if you want to keep existing data)
            # conn.execute(yocket_table.delete())
            # print("  ✓ Cleared existing data")
            
            records = db_df.to_dict(orient="records")
            total_records = len(records)
            
            for start in range(0, total_records, chunk_size):
                chunk = records[start:start + chunk_size]
                conn.execute(yocket_table.insert(), chunk)
                rows_inserted += len(chunk)
                print(f"  ✓ Inserted {rows_inserted}/{total_records} rows...", end="\r")
        
        print(f"\n✓ Successfully inserted {rows_inserted} rows into YocketPrograms table")
        
        # Verify insertion
        with engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM YocketPrograms"))
            count = result.scalar_one()
            print(f"✓ Total rows in YocketPrograms table: {count}")
        
    except SQLAlchemyError as e:
        print(f"\n❌ Database error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        engine.dispose()
    
    print("\n" + "="*80)
    print("INGESTION COMPLETE")
    print("="*80)

if __name__ == "__main__":
    main()

