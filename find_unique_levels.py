#!/usr/bin/env python3
"""
Script to find all unique program levels in the database.
"""

import os
import sys
from sqlalchemy import MetaData, create_engine, select, func
from sqlalchemy.exc import SQLAlchemyError
from urllib.parse import quote_plus
from dotenv import load_dotenv
from collections import Counter

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
    """Main function to find unique program levels."""
    print("="*80)
    print("FINDING UNIQUE PROGRAM LEVELS")
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
        metadata.reflect(bind=engine, only=["Program"])
        
        program_table = metadata.tables.get("Program")
        
        if program_table is None:
            print("❌ Program table not found in database.")
            sys.exit(1)
        
        # Query all unique levels with counts
        print("\nFetching unique program levels...")
        with engine.connect() as conn:
            # Get all levels
            stmt = select(program_table.c.Level).where(program_table.c.Level.isnot(None))
            results = conn.execute(stmt).fetchall()
            
            # Count occurrences of each level
            level_counts = Counter([row[0] for row in results if row[0]])
            
            # Also get total count
            total_stmt = select(func.count(program_table.c.ProgramID))
            total_programs = conn.execute(total_stmt).scalar() or 0
            
            # Count programs with null levels
            null_stmt = select(func.count(program_table.c.ProgramID)).where(
                program_table.c.Level.is_(None)
            )
            null_count = conn.execute(null_stmt).scalar() or 0
        
        print(f"✓ Found {len(level_counts)} unique program levels")
        print(f"✓ Total programs: {total_programs}")
        if null_count > 0:
            print(f"⚠️  Programs with null/empty level: {null_count}")
        
        # Display results
        print("\n" + "="*80)
        print("UNIQUE PROGRAM LEVELS (sorted by count)")
        print("="*80)
        print(f"{'Level':<50} {'Count':<10} {'Percentage':<10}")
        print("-" * 80)
        
        # Sort by count (descending)
        sorted_levels = sorted(level_counts.items(), key=lambda x: x[1], reverse=True)
        
        for level, count in sorted_levels:
            percentage = (count / total_programs * 100) if total_programs > 0 else 0
            print(f"{level:<50} {count:<10} {percentage:.2f}%")
        
        # Also show alphabetically sorted
        print("\n" + "="*80)
        print("UNIQUE PROGRAM LEVELS (sorted alphabetically)")
        print("="*80)
        print(f"{'Level':<50} {'Count':<10}")
        print("-" * 80)
        
        sorted_alphabetically = sorted(level_counts.items(), key=lambda x: x[0] or "")
        
        for level, count in sorted_alphabetically:
            print(f"{level:<50} {count:<10}")
        
        # Categorize levels
        print("\n" + "="*80)
        print("LEVEL CATEGORIZATION")
        print("="*80)
        
        graduate_keywords = [
            "MASTER", "MASTERS", "M.S.", "M.A.", "M.B.A.", "M.SC.", "M.ED.", "M.F.A.",
            "M.P.H.", "M.S.W.", "M.E.", "M.ENG", "MS", "MA", "MBA", "MSC", "MED", "MFA",
            "DOCTORATE", "DOCTOR", "PHD", "PH.D.", "ED.D.", "D.PHIL", "DBA", "JD", "MD",
            "GRADUATE CERTIFICATE", "GRAD CERTIFICATE", "GRAD CERT", "POSTGRADUATE",
            "POST-GRADUATE", "POST GRADUATE", "GRADUATE"
        ]
        
        undergraduate_keywords = [
            "BACHELOR", "BACHELORS", "B.S.", "B.A.", "B.SC.", "B.ED.", "B.F.A.", "B.B.A.",
            "B.E.", "B.ENG", "BS", "BA", "BSC", "BED", "BFA", "BBA", "BE", "BENG",
            "ASSOCIATE", "ASSOCIATES", "A.S.", "A.A.", "A.SC.", "A.ED.", "A.F.A.", "A.B.A.",
            "A.E.", "A.ENG", "AS", "AA", "ASC", "AED", "AFA", "ABA", "AE", "AENG",
            "UNDERGRADUATE CERTIFICATE", "UNDERGRAD CERTIFICATE", "UNDERGRAD CERT",
            "UNDERGRADUATE"
        ]
        
        graduate_levels = []
        undergraduate_levels = []
        unknown_levels = []
        
        for level, count in level_counts.items():
            if not level:
                unknown_levels.append((level, count))
                continue
            
            level_upper = level.upper().strip()
            
            is_graduate = any(keyword in level_upper for keyword in graduate_keywords)
            is_undergraduate = any(keyword in level_upper for keyword in undergraduate_keywords)
            
            if is_graduate:
                graduate_levels.append((level, count))
            elif is_undergraduate:
                undergraduate_levels.append((level, count))
            else:
                unknown_levels.append((level, count))
        
        print(f"\n📚 GRADUATE LEVELS ({len(graduate_levels)} unique):")
        graduate_total = sum(count for _, count in graduate_levels)
        for level, count in sorted(graduate_levels, key=lambda x: x[1], reverse=True):
            print(f"   {level:<45} {count:>6} programs")
        print(f"   {'Total:':<45} {graduate_total:>6} programs")
        
        print(f"\n🎓 UNDERGRADUATE LEVELS ({len(undergraduate_levels)} unique):")
        undergraduate_total = sum(count for _, count in undergraduate_levels)
        for level, count in sorted(undergraduate_levels, key=lambda x: x[1], reverse=True):
            print(f"   {level:<45} {count:>6} programs")
        print(f"   {'Total:':<45} {undergraduate_total:>6} programs")
        
        if unknown_levels:
            print(f"\n❓ UNKNOWN/UNCATEGORIZED LEVELS ({len(unknown_levels)} unique):")
            unknown_total = sum(count for _, count in unknown_levels)
            for level, count in sorted(unknown_levels, key=lambda x: x[1], reverse=True):
                print(f"   {level or '(null)':<45} {count:>6} programs")
            print(f"   {'Total:':<45} {unknown_total:>6} programs")
        
        print("\n" + "="*80)
        print("SUMMARY")
        print("="*80)
        print(f"Total unique levels: {len(level_counts)}")
        print(f"Total programs: {total_programs}")
        print(f"Graduate programs: {graduate_total}")
        print(f"Undergraduate programs: {undergraduate_total}")
        print(f"Unknown/Uncategorized: {unknown_total}")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        engine.dispose()

if __name__ == "__main__":
    main()

