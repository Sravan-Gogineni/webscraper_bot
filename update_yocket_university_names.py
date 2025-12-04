#!/usr/bin/env python3
"""
Script to update existing Yocket programs to clean university names.
Converts hyphens to spaces and applies title case.
"""

import os
import sys
from sqlalchemy import MetaData, create_engine, select, update
from sqlalchemy import text
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

def clean_university_name(slug):
    """Clean university name - convert hyphens to spaces and title case."""
    if not slug or slug == 'None' or slug == 'nan':
        return None
    # Convert hyphens to spaces and title case
    cleaned = str(slug).replace('-', ' ').title()
    return cleaned

def main():
    """Main function to update university names."""
    print("="*80)
    print("UPDATE YOCKET UNIVERSITY NAMES")
    print("="*80)
    
    # Connect to database
    print("\nConnecting to database...")
    engine = get_db_engine()
    if not engine:
        print("❌ Failed to connect to database. Exiting.")
        sys.exit(1)
    
    print("✓ Connected to database successfully")
    
    try:
        metadata = MetaData()
        metadata.reflect(bind=engine, only=["YocketPrograms"])
        
        yocket_table = metadata.tables.get("YocketPrograms")
        
        if yocket_table is None:
            print("❌ YocketPrograms table not found in database.")
            sys.exit(1)
        
        # Get all unique university slugs
        print("\nFetching university slugs...")
        with engine.connect() as conn:
            stmt = select(yocket_table.c.UniversitySlug).distinct()
            slugs = [row[0] for row in conn.execute(stmt) if row[0]]
        
        print(f"✓ Found {len(slugs)} unique university slugs")
        
        # Update each slug
        print("\nUpdating university names...")
        updated_count = 0
        
        with engine.begin() as conn:
            for slug in slugs:
                if slug and '-' in str(slug):
                    cleaned = clean_university_name(slug)
                    if cleaned and cleaned != slug:
                        stmt = (
                            update(yocket_table)
                            .where(yocket_table.c.UniversitySlug == slug)
                            .values(UniversitySlug=cleaned)
                        )
                        result = conn.execute(stmt)
                        updated_count += result.rowcount
            
            # Also update any None or 'nan' values
            stmt = (
                update(yocket_table)
                .where(
                    (yocket_table.c.UniversitySlug.is_(None)) |
                    (yocket_table.c.UniversitySlug == 'None') |
                    (yocket_table.c.UniversitySlug == 'nan')
                )
                .values(UniversitySlug=None)
            )
            conn.execute(stmt)
        
        print(f"✓ Updated {updated_count} records")
        
        # Verify update
        with engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM YocketPrograms WHERE UniversitySlug LIKE '%-%'"))
            remaining = result.scalar_one()
            if remaining > 0:
                print(f"⚠️  Warning: {remaining} records still contain hyphens")
            else:
                print("✓ All university names cleaned successfully")
        
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
    print("UPDATE COMPLETE")
    print("="*80)

if __name__ == "__main__":
    main()

