#!/usr/bin/env python3
"""
Script to fetch QS rankings from Yocket API and update existing universities in the database.
"""

import os
import json
import re
import sys
import requests
from typing import Dict, List, Optional, Tuple, Set
from difflib import SequenceMatcher
from dotenv import load_dotenv
from sqlalchemy import create_engine, select, MetaData
from sqlalchemy.exc import SQLAlchemyError

try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False
    print("Warning: pandas not available. Install it with: pip install pandas openpyxl")

load_dotenv()

# Yocket API configuration
YOCKET_API_URL = "https://api.yocket.com/explore/filter?apiVersion=v2"
YOCKET_AUTH_TOKEN = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhbGdvcml0aG0iOiJFUzI1NiIsImlkIjoiYzliMmMxYWYtMDU3OC00YzNkLTkyYTEtYmU0ZDdlNWUwZTM5IiwiaWF0IjoxNzYzNDM0NjYyLCJleHAiOjE3NjYwNjQ0MDh9.oaTkhsNT6PQVup1jBMXIAoazasJCY5DsEXfJRqqrYFo"

API_HEADERS = {
    "accept": "application/json",
    "accept-language": "en-US,en;q=0.7",
    "authorization": f"Bearer {YOCKET_AUTH_TOKEN}",
    "content-type": "application/json",
    "origin": "https://yocket.com",
    "referer": "https://yocket.com/",
    "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36",
}


def build_db_connection_url() -> Optional[str]:
    """Build database connection URL from environment variables."""
    server = os.getenv("DB_SERVER", "localhost,1433")
    database = os.getenv("DB_NAME")
    username = os.getenv("DB_USERNAME")
    password = os.getenv("DB_PASSWORD")
    driver = os.getenv("DB_DRIVER", "ODBC Driver 18 for SQL Server")

    if not all([database, username, password]):
        return None

    odbc_params = (
        f"Driver={driver};"
        f"Server={server};"
        f"Database={database};"
        f"UID={username};"
        f"PWD={password};"
        f"TrustServerCertificate=yes;"
    )
    return f"mssql+pyodbc:///?odbc_connect={odbc_params}"


def fetch_all_universities_from_api() -> List[Dict]:
    """Fetch all universities from Yocket API."""
    all_universities = []
    page = 1
    items_per_page = 800  # As specified in the request
    
    print(f"Fetching universities from Yocket API...")
    
    while True:
        payload = {
            "page": page,
            "items": items_per_page,
            "sort_by": "rank",
            "country_abbreviations": ["US"]
        }
        
        try:
            response = requests.post(YOCKET_API_URL, headers=API_HEADERS, json=payload, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            if not data.get("state") or not data.get("data") or not data["data"].get("result"):
                break
            
            universities = data["data"]["result"]
            all_universities.extend(universities)
            
            metadata = data["data"].get("metadata", {})
            total_pages = metadata.get("total_pages", 1)
            current_page = metadata.get("current_page", page)
            total_items = metadata.get("total_items", len(universities))
            
            print(f"Fetched page {current_page}/{total_pages} - {len(universities)} universities (Total: {len(all_universities)})")
            
            # Check if we've fetched all pages
            if current_page >= total_pages:
                break
            
            page += 1
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching page {page}: {e}")
            break
    
    print(f"\nTotal universities fetched from API: {len(all_universities)}")
    return all_universities


def normalize_university_name(name: str) -> str:
    """Normalize university name for matching by removing punctuation, extra spaces, and common variations."""
    if not name:
        return ""
    
    # Convert to lowercase
    name = name.lower().strip()
    
    # Remove common punctuation (commas, periods, hyphens, etc.) but keep spaces
    name = re.sub(r'[,\-\.;:!?()\[\]{}"\']', ' ', name)
    
    # Remove extra whitespace and normalize to single spaces
    name = " ".join(name.split())
    
    # Remove common suffixes that might differ
    suffixes_to_remove = [
        r'\binc\.?$',
        r'\bllc\.?$',
        r'\buniversity$',
        r'\buniv\.?$',
        r'\bcollege$',
        r'\bcol\.?$',
        r'\binstitute$',
        r'\binst\.?$',
        r'\bof technology$',
        r'\btech\.?$',
    ]
    for suffix in suffixes_to_remove:
        name = re.sub(suffix, '', name, flags=re.IGNORECASE)
    
    # Remove extra whitespace again after suffix removal
    name = " ".join(name.split())
    
    # Remove common words that might differ
    common_words = ['the', 'a', 'an']
    words = name.split()
    words = [w for w in words if w not in common_words]
    name = " ".join(words)
    
    return name.strip()


def similarity_score(name1: str, name2: str) -> float:
    """Calculate similarity score between two names using multiple methods."""
    norm1 = normalize_university_name(name1)
    norm2 = normalize_university_name(name2)
    
    if not norm1 or not norm2:
        return 0.0
    
    # Exact match after normalization
    if norm1 == norm2:
        return 1.0
    
    # Sequence matcher ratio
    seq_ratio = SequenceMatcher(None, norm1, norm2).ratio()
    
    # Check if one name contains the other (handles cases like "MIT" vs "Massachusetts Institute of Technology")
    if norm1 in norm2 or norm2 in norm1:
        # If one is contained in the other, give it a boost
        contained_ratio = min(len(norm1), len(norm2)) / max(len(norm1), len(norm2))
        seq_ratio = max(seq_ratio, 0.7 + (contained_ratio * 0.2))
    
    # Word-based matching (check if most words match)
    words1 = set(norm1.split())
    words2 = set(norm2.split())
    if words1 and words2:
        common_words = words1.intersection(words2)
        all_words = words1.union(words2)
        word_ratio = len(common_words) / len(all_words) if all_words else 0.0
        # Combine sequence ratio with word ratio
        seq_ratio = max(seq_ratio, word_ratio * 0.9)
    
    return seq_ratio


def load_colleges_from_excel(excel_file: str = "Univs-2.xlsx", sheet_index: int = 0) -> Set[str]:
    """Load college names from Excel file to filter which colleges to process."""
    if not PANDAS_AVAILABLE:
        print("Warning: pandas not available. Processing all colleges from database.")
        return set()
    
    if not os.path.exists(excel_file):
        print(f"Warning: Excel file '{excel_file}' not found. Processing all colleges from database.")
        return set()
    
    try:
        # Read the Excel file
        excel_data = pd.ExcelFile(excel_file)
        sheet_names = excel_data.sheet_names
        print(f"Found Excel file with {len(sheet_names)} sheet(s): {sheet_names}")
        
        if sheet_index >= len(sheet_names):
            print(f"Warning: Sheet index {sheet_index} out of range. Using first sheet.")
            sheet_index = 0
        
        # Read the specified sheet
        df = pd.read_excel(excel_file, sheet_name=sheet_index)
        print(f"Reading from sheet: '{sheet_names[sheet_index]}'")
        print(f"Columns in sheet: {list(df.columns)}")
        
        # Try to find the college name column (case-insensitive)
        college_col = None
        possible_names = ['College Name', 'CollegeName', 'College', 'Name', 'University Name', 'UniversityName']
        
        for col_name in possible_names:
            if col_name in df.columns:
                college_col = col_name
                break
        
        # If not found, try case-insensitive match
        if college_col is None:
            for col in df.columns:
                if str(col).lower() in ['college name', 'collegename', 'college', 'name', 'university name', 'universityname']:
                    college_col = col
                    break
        
        if college_col is None:
            print(f"Warning: Could not find college name column. Available columns: {list(df.columns)}")
            print("Using first column as college name.")
            college_col = df.columns[0]
        
        # Extract college names and normalize
        college_names = set()
        for name in df[college_col].dropna():
            if pd.notna(name) and str(name).strip():
                college_names.add(str(name).strip())
        
        print(f"Loaded {len(college_names)} college names from Excel file")
        return college_names
        
    except Exception as e:
        print(f"Error reading Excel file: {e}")
        print("Processing all colleges from database.")
        return set()


def get_all_colleges_from_db(engine, filter_colleges: Optional[Set[str]] = None) -> List[Dict]:
    """Get all colleges from the database, optionally filtered by Excel file."""
    metadata = MetaData()
    metadata.reflect(bind=engine)
    college_table = metadata.tables.get("College")
    
    if college_table is None:
        print("Error: College table not found in database")
        return []
    
    with engine.connect() as conn:
        result = conn.execute(select(college_table.c.CollegeID, college_table.c.CollegeName))
        all_colleges = [{"CollegeID": row.CollegeID, "CollegeName": row.CollegeName} for row in result]
    
    # Filter colleges if Excel file provided
    if filter_colleges:
        print(f"\nFiltering colleges from database based on Excel file...")
        print(f"Total colleges in database: {len(all_colleges)}")
        
        # Create normalized set for matching
        normalized_filter = {normalize_university_name(name) for name in filter_colleges}
        
        filtered_colleges = []
        for college in all_colleges:
            college_name = college.get("CollegeName", "")
            if not college_name:
                continue
            
            normalized_db_name = normalize_university_name(college_name)
            
            # Check if this college matches any in the Excel file
            for excel_name in filter_colleges:
                normalized_excel_name = normalize_university_name(excel_name)
                if normalized_db_name == normalized_excel_name or similarity_score(college_name, excel_name) >= 0.8:
                    filtered_colleges.append(college)
                    break
        
        print(f"Filtered to {len(filtered_colleges)} colleges matching Excel file")
        return filtered_colleges
    
    print(f"Found {len(all_colleges)} colleges in database (no filter applied)")
    return all_colleges


def get_programs_for_college(engine, college_id: int) -> List[Dict]:
    """Get all programs linked to a college via ProgramDepartmentLink."""
    metadata = MetaData()
    metadata.reflect(bind=engine)
    program_table = metadata.tables.get("Program")
    link_table = metadata.tables.get("ProgramDepartmentLink")
    
    if program_table is None or link_table is None:
        return []
    
    with engine.connect() as conn:
        result = conn.execute(
            select(program_table.c.ProgramID, program_table.c.ProgramName, program_table.c.QsWorldRanking)
            .select_from(
                program_table.join(link_table, program_table.c.ProgramID == link_table.c.ProgramID)
            )
            .where(link_table.c.CollegeID == college_id)
        )
        programs = [
            {
                "ProgramID": row.ProgramID,
                "ProgramName": row.ProgramName,
                "QsWorldRanking": row.QsWorldRanking
            }
            for row in result
        ]
    
    return programs


def match_universities(api_universities: List[Dict], db_colleges: List[Dict], threshold: float = 0.75) -> List[Tuple[Dict, Dict, float]]:
    """Match API universities with database colleges by name similarity."""
    matches = []
    unmatched_api = []
    unmatched_db = set(range(len(db_colleges)))  # Track unmatched DB colleges by index
    
    print(f"\nMatching universities (similarity threshold: {threshold})...")
    print(f"API universities: {len(api_universities)}, DB colleges: {len(db_colleges)}")
    
    for api_uni in api_universities:
        api_name = api_uni.get("university_name", "")
        api_qs_rank = api_uni.get("university_qs_rank", "")
        
        if not api_name:
            continue
        
        if not api_qs_rank:
            unmatched_api.append(api_name)
            continue
        
        best_match = None
        best_score = 0.0
        best_index = -1
        
        for idx, db_college in enumerate(db_colleges):
            db_name = db_college.get("CollegeName", "")
            if not db_name:
                continue
            
            score = similarity_score(api_name, db_name)
            if score > best_score:
                best_score = score
                best_match = db_college
                best_index = idx
        
        if best_match and best_score >= threshold:
            matches.append((api_uni, best_match, best_score))
            if best_index >= 0:
                unmatched_db.discard(best_index)
            print(f"  ✓ Match: '{api_name}' <-> '{best_match['CollegeName']}' (score: {best_score:.3f})")
        else:
            unmatched_api.append(api_name)
            if best_match:
                print(f"  ✗ No match: '{api_name}' (best: '{best_match['CollegeName']}' score: {best_score:.3f})")
            else:
                print(f"  ✗ No match: '{api_name}'")
    
    print(f"\nMatching Summary:")
    print(f"  - Matched: {len(matches)}")
    print(f"  - Unmatched from API: {len(unmatched_api)}")
    print(f"  - Unmatched from DB: {len(unmatched_db)}")
    
    if unmatched_api and len(unmatched_api) <= 20:
        print(f"\nUnmatched API universities (first 20):")
        for name in unmatched_api[:20]:
            print(f"    - {name}")
    
    return matches


def update_program_qs_rankings(engine, matches: List[Tuple[Dict, Dict, float]]):
    """Update QS rankings in the Program table for programs linked to matched colleges."""
    metadata = MetaData()
    metadata.reflect(bind=engine)
    program_table = metadata.tables.get("Program")
    
    if program_table is None:
        print("Error: Program table not found")
        return
    
    updated_count = 0
    skipped_count = 0
    no_programs_count = 0
    
    print(f"\nUpdating QS rankings in Program table...")
    
    for api_uni, db_college, score in matches:
        college_id = db_college["CollegeID"]
        qs_rank = api_uni.get("university_qs_rank", "")
        api_name = api_uni.get("university_name", "")
        college_name = db_college["CollegeName"]
        
        if not qs_rank:
            print(f"  Skipped: {college_name} - no QS rank in API data")
            skipped_count += 1
            continue
        
        # Get all programs linked to this college
        programs = get_programs_for_college(engine, college_id)
        
        if not programs:
            print(f"  No programs found for: {college_name} (ID: {college_id})")
            no_programs_count += 1
            continue
        
        print(f"  Processing {college_name} (ID: {college_id}) - {len(programs)} program(s)")
        
        # Update each program's QS ranking
        for program in programs:
            program_id = program["ProgramID"]
            program_name = program["ProgramName"]
            current_rank = program["QsWorldRanking"]
            
            try:
                with engine.begin() as conn:
                    # Update if different or empty
                    if current_rank != qs_rank:
                        conn.execute(
                            program_table.update()
                            .where(program_table.c.ProgramID == program_id)
                            .values(QsWorldRanking=qs_rank)
                        )
                        print(f"    Updated program '{program_name}' (ID: {program_id}) -> QS Rank: {qs_rank}")
                        updated_count += 1
                    else:
                        print(f"    Skipped program '{program_name}' (ID: {program_id}) - already has QS Rank: {qs_rank}")
                        skipped_count += 1
                        
            except SQLAlchemyError as e:
                print(f"    Error updating program '{program_name}' (ID: {program_id}): {e}")
                skipped_count += 1
    
    print(f"\nUpdate complete:")
    print(f"  - Programs updated: {updated_count}")
    print(f"  - Programs skipped: {skipped_count}")
    print(f"  - Colleges with no programs: {no_programs_count}")
    print(f"  - Total colleges processed: {len(matches)}")


def main():
    """Main function to orchestrate the update process."""
    print("=" * 60)
    print("QS Ranking Update Script")
    print("=" * 60)
    
    # Parse command line arguments for Excel file and sheet
    excel_file = "Univs-2.xlsx"
    sheet_index = 1  # Default to sheet 2 (index 1)
    
    if len(sys.argv) > 1:
        excel_file = sys.argv[1]
    if len(sys.argv) > 2:
        try:
            sheet_index = int(sys.argv[2])
        except ValueError:
            print(f"Warning: Invalid sheet index '{sys.argv[2]}'. Using sheet 2 (index 1).")
            sheet_index = 1
    
    # Load colleges from Excel file (if available)
    filter_colleges = None
    if PANDAS_AVAILABLE:
        filter_colleges = load_colleges_from_excel(excel_file, sheet_index)
        if filter_colleges:
            print(f"\nWill only process {len(filter_colleges)} colleges from Excel file")
    
    # Connect to database
    connection_url = build_db_connection_url()
    if not connection_url:
        print("Error: Database configuration is incomplete.")
        print("Set DB_SERVER, DB_NAME, DB_USERNAME, and DB_PASSWORD in your environment.")
        return
    
    try:
        engine = create_engine(connection_url, pool_pre_ping=True)
        print("Connected to database successfully")
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return
    
    # Verify Program table has QsWorldRanking column
    metadata = MetaData()
    metadata.reflect(bind=engine)
    program_table = metadata.tables.get("Program")
    if program_table is None:
        print("Error: Program table not found in database")
        return
    
    if "QsWorldRanking" not in program_table.columns:
        print("Error: QsWorldRanking column not found in Program table")
        return
    
    print("\nQsWorldRanking column found in Program table")
    
    # Fetch universities from API
    api_universities = fetch_all_universities_from_api()
    if not api_universities:
        print("No universities fetched from API. Exiting.")
        return
    
    # Get colleges from database (filtered by Excel if provided)
    db_colleges = get_all_colleges_from_db(engine, filter_colleges)
    if not db_colleges:
        print("No colleges found in database (after filtering). Exiting.")
        return
    
    # Match universities
    matches = match_universities(api_universities, db_colleges, threshold=0.75)
    
    if not matches:
        print("\nNo matches found. Exiting.")
        return
    
    # Update database - update QS rankings in Program table
    update_program_qs_rankings(engine, matches)
    
    print("\n" + "=" * 60)
    print("Script completed successfully!")
    print("=" * 60)


if __name__ == "__main__":
    main()

