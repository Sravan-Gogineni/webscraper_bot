#!/usr/bin/env python3
"""
Script to compare universities between traditional programs (College table) 
and Yocket programs (YocketPrograms table).

Shows how many universities from traditional programs also exist in Yocket programs.
"""

import os
import sys
from sqlalchemy import MetaData, create_engine, select, func, text
from sqlalchemy.exc import SQLAlchemyError
from urllib.parse import quote_plus
from dotenv import load_dotenv
import re

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

def normalize_name(name):
    """Normalize university name for comparison."""
    if not name:
        return None
    # Convert to lowercase, remove extra spaces, remove common words
    normalized = str(name).lower().strip()
    # Remove common words that might differ
    normalized = re.sub(r'\b(the|university|college|of|at|in)\b', '', normalized)
    # Remove special characters and extra spaces
    normalized = re.sub(r'[^\w\s]', '', normalized)
    normalized = re.sub(r'\s+', ' ', normalized).strip()
    return normalized

def find_matches(traditional_names, yocket_names):
    """Find matching universities between two lists using fuzzy matching."""
    matches = []
    traditional_normalized = {normalize_name(name): name for name in traditional_names if name}
    yocket_normalized = {normalize_name(name): name for name in yocket_names if name}
    
    # Exact matches after normalization
    for norm_name, original_trad in traditional_normalized.items():
        if norm_name and norm_name in yocket_normalized:
            matches.append({
                'traditional': original_trad,
                'yocket': yocket_normalized[norm_name],
                'match_type': 'exact'
            })
    
    # Partial matches (one name contains the other)
    for norm_trad, original_trad in traditional_normalized.items():
        if not norm_trad:
            continue
        for norm_yocket, original_yocket in yocket_normalized.items():
            if not norm_yocket:
                continue
            # Check if already matched
            if any(m['traditional'] == original_trad and m['yocket'] == original_yocket for m in matches):
                continue
            
            # Check if one contains the other (and they're meaningful matches)
            if len(norm_trad) > 5 and len(norm_yocket) > 5:
                if norm_trad in norm_yocket or norm_yocket in norm_trad:
                    matches.append({
                        'traditional': original_trad,
                        'yocket': original_yocket,
                        'match_type': 'partial'
                    })
    
    return matches

def main():
    """Main function to compare universities."""
    print("="*80)
    print("UNIVERSITY COMPARISON: TRADITIONAL vs YOCKET PROGRAMS")
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
        metadata.reflect(bind=engine, only=["College", "YocketPrograms"])
        
        college_table = metadata.tables.get("College")
        yocket_table = metadata.tables.get("YocketPrograms")
        
        if college_table is None:
            print("❌ College table not found in database.")
            sys.exit(1)
        
        if yocket_table is None:
            print("❌ YocketPrograms table not found in database.")
            sys.exit(1)
        
        # Get traditional universities
        print("\nFetching traditional universities from College table...")
        with engine.connect() as conn:
            stmt = select(college_table.c.CollegeName).where(college_table.c.CollegeName.isnot(None))
            traditional_rows = conn.execute(stmt).fetchall()
            traditional_universities = [row[0] for row in traditional_rows if row[0]]
        
        print(f"✓ Found {len(traditional_universities)} traditional universities")
        
        # Get Yocket universities
        print("\nFetching universities from YocketPrograms table...")
        with engine.connect() as conn:
            stmt = select(yocket_table.c.UniversitySlug).distinct().where(yocket_table.c.UniversitySlug.isnot(None))
            yocket_rows = conn.execute(stmt).fetchall()
            yocket_universities = [row[0] for row in yocket_rows if row[0]]
        
        print(f"✓ Found {len(yocket_universities)} unique universities in Yocket programs")
        
        # Find matches
        print("\nComparing universities...")
        matches = find_matches(traditional_universities, yocket_universities)
        
        # Separate exact and partial matches
        exact_matches = [m for m in matches if m['match_type'] == 'exact']
        partial_matches = [m for m in matches if m['match_type'] == 'partial']
        
        # Get unique matched traditional universities
        matched_traditional = set(m['traditional'] for m in matches)
        unmatched_traditional = [u for u in traditional_universities if u not in matched_traditional]
        
        # Get unique matched Yocket universities
        matched_yocket = set(m['yocket'] for m in matches)
        unmatched_yocket = [u for u in yocket_universities if u not in matched_yocket]
        
        # Print results
        print("\n" + "="*80)
        print("COMPARISON RESULTS")
        print("="*80)
        print(f"\nTraditional Universities (College table): {len(traditional_universities)}")
        print(f"Yocket Universities (YocketPrograms table): {len(yocket_universities)}")
        print(f"\n📊 OVERLAP STATISTICS:")
        print(f"  ✓ Exact matches: {len(exact_matches)}")
        print(f"  ✓ Partial matches: {len(partial_matches)}")
        print(f"  ✓ Total matches: {len(matches)}")
        print(f"  ✓ Unique traditional universities matched: {len(matched_traditional)}")
        print(f"  ✓ Unique Yocket universities matched: {len(matched_yocket)}")
        print(f"  ✓ Unmatched traditional universities: {len(unmatched_traditional)}")
        print(f"  ✓ Unmatched Yocket universities: {len(unmatched_yocket)}")
        
        if len(traditional_universities) > 0:
            match_percentage = (len(matched_traditional) / len(traditional_universities)) * 100
            print(f"\n  📈 Match Rate: {len(matched_traditional)}/{len(traditional_universities)} ({match_percentage:.1f}%) of traditional universities are also in Yocket programs")
        
        # Show exact matches
        if exact_matches:
            print("\n" + "="*80)
            print("EXACT MATCHES")
            print("="*80)
            for i, match in enumerate(exact_matches[:20], 1):
                print(f"{i}. Traditional: {match['traditional']}")
                print(f"   Yocket:      {match['yocket']}")
            if len(exact_matches) > 20:
                print(f"\n... and {len(exact_matches) - 20} more exact matches")
        
        # Show partial matches
        if partial_matches:
            print("\n" + "="*80)
            print("PARTIAL MATCHES (Possible matches - review manually)")
            print("="*80)
            for i, match in enumerate(partial_matches[:20], 1):
                print(f"{i}. Traditional: {match['traditional']}")
                print(f"   Yocket:      {match['yocket']}")
            if len(partial_matches) > 20:
                print(f"\n... and {len(partial_matches) - 20} more partial matches")
        
        # Show unmatched traditional universities
        if unmatched_traditional:
            print("\n" + "="*80)
            print(f"UNMATCHED TRADITIONAL UNIVERSITIES ({len(unmatched_traditional)})")
            print("="*80)
            for i, uni in enumerate(sorted(unmatched_traditional)[:30], 1):
                print(f"{i}. {uni}")
            if len(unmatched_traditional) > 30:
                print(f"\n... and {len(unmatched_traditional) - 30} more")
        
        # Save detailed results to file
        output_file = "university_comparison_results.txt"
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write("="*80 + "\n")
            f.write("UNIVERSITY COMPARISON RESULTS\n")
            f.write("="*80 + "\n\n")
            f.write(f"Traditional Universities: {len(traditional_universities)}\n")
            f.write(f"Yocket Universities: {len(yocket_universities)}\n")
            f.write(f"Total Matches: {len(matches)}\n")
            f.write(f"Match Rate: {len(matched_traditional)}/{len(traditional_universities)} ({match_percentage:.1f}%)\n\n")
            
            f.write("="*80 + "\n")
            f.write("EXACT MATCHES\n")
            f.write("="*80 + "\n")
            for match in exact_matches:
                f.write(f"Traditional: {match['traditional']}\n")
                f.write(f"Yocket:      {match['yocket']}\n\n")
            
            f.write("="*80 + "\n")
            f.write("PARTIAL MATCHES\n")
            f.write("="*80 + "\n")
            for match in partial_matches:
                f.write(f"Traditional: {match['traditional']}\n")
                f.write(f"Yocket:      {match['yocket']}\n\n")
            
            f.write("="*80 + "\n")
            f.write("UNMATCHED TRADITIONAL UNIVERSITIES\n")
            f.write("="*80 + "\n")
            for uni in sorted(unmatched_traditional):
                f.write(f"{uni}\n")
        
        print(f"\n✓ Detailed results saved to: {output_file}")
        
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
    print("COMPARISON COMPLETE")
    print("="*80)

if __name__ == "__main__":
    main()

