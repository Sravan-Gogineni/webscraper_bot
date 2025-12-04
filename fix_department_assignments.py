#!/usr/bin/env python3
"""
Script to correct department/admissions assignments for programs based on their level.

This script ensures:
- Undergraduate levels → Undergraduate Admissions:
  * Bachelor
  * Associate
  * Undergraduate
  * Undergraduate Certificate
  * Diploma

- Graduate levels → Graduate Admissions:
  * Master
  * Doctorate
  * Graduate Certificate
"""

import os
import sys
from sqlalchemy import MetaData, create_engine, select, func, update
from sqlalchemy.exc import SQLAlchemyError
from urllib.parse import quote_plus
from dotenv import load_dotenv
from collections import defaultdict

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

def is_graduate_level(level):
    """Determine if a program level is graduate.
    
    Graduate levels: Master, Doctorate, Graduate Certificate
    """
    if not level:
        return None
    
    level_upper = level.upper().strip()
    
    # Exact matches for graduate levels from database
    graduate_levels = [
        "MASTER",
        "DOCTORATE",
        "GRADUATE CERTIFICATE"
    ]
    
    # Check for exact match (case-insensitive)
    return level_upper in graduate_levels

def is_undergraduate_level(level):
    """Determine if a program level is undergraduate.
    
    Undergraduate levels: Bachelor, Associate, Undergraduate, Undergraduate Certificate, Diploma
    """
    if not level:
        return None
    
    level_upper = level.upper().strip()
    
    # Exact matches for undergraduate levels from database
    undergraduate_levels = [
        "BACHELOR",
        "ASSOCIATE",
        "UNDERGRADUATE",
        "UNDERGRADUATE CERTIFICATE",
        "DIPLOMA"
    ]
    
    # Check for exact match (case-insensitive)
    return level_upper in undergraduate_levels

def find_department_by_type(engine, college_id, is_graduate):
    """Find the appropriate CollegeDepartmentID for a college based on program type.
    
    Args:
        engine: Database engine
        college_id: College ID
        is_graduate: True for graduate, False for undergraduate
    
    Returns:
        CollegeDepartmentID or None
    """
    try:
        metadata = MetaData()
        metadata.reflect(bind=engine, only=["CollegeDepartment", "Department"])
        college_department_table = metadata.tables.get("CollegeDepartment")
        department_table = metadata.tables.get("Department")
        
        if college_department_table is None or department_table is None:
            return None
        
        with engine.connect() as conn:
            if is_graduate:
                # Look for graduate admissions departments
                patterns = [
                    "GRADUATE ADMISSIONS",
                    "GRADUATE SCHOOL ADMISSIONS",
                    "OFFICE OF GRADUATE ADMISSIONS",
                    "GRADUATE STUDIES ADMISSIONS",
                    "GRADUATE PROGRAMS ADMISSIONS",
                    "GRADUATE SCHOOL",
                    "GRADUATE STUDIES",
                    "GRADUATE PROGRAMS"
                ]
                
                for pattern in patterns:
                    stmt = (
                        select(college_department_table.c.CollegeDepartmentID, department_table.c.DepartmentName)
                        .join(department_table, department_table.c.DepartmentID == college_department_table.c.DepartmentID)
                        .where(
                            (college_department_table.c.CollegeID == college_id) &
                            (func.upper(department_table.c.DepartmentName).like(f"%{pattern}%")) &
                            (~func.upper(department_table.c.DepartmentName).like("%UNDERGRADUATE%"))
                        )
                    )
                    result = conn.execute(stmt).first()
                    if result:
                        return result[0]
                
                # More generic "GRADUATE" pattern
                stmt = (
                    select(college_department_table.c.CollegeDepartmentID, department_table.c.DepartmentName)
                    .join(department_table, department_table.c.DepartmentID == college_department_table.c.DepartmentID)
                    .where(
                        (college_department_table.c.CollegeID == college_id) &
                        (func.upper(department_table.c.DepartmentName).like("%GRADUATE%")) &
                        (~func.upper(department_table.c.DepartmentName).like("%UNDERGRADUATE%"))
                    )
                )
                result = conn.execute(stmt).first()
                if result:
                    return result[0]
            else:
                # Look for undergraduate admissions departments
                patterns = [
                    "UNDERGRADUATE ADMISSIONS",
                    "OFFICE OF UNDERGRADUATE ADMISSIONS",
                    "UNDERGRADUATE STUDIES ADMISSIONS",
                    "UNDERGRADUATE PROGRAMS ADMISSIONS",
                    "UNDERGRADUATE STUDIES",
                    "UNDERGRADUATE PROGRAMS"
                ]
                
                for pattern in patterns:
                    stmt = (
                        select(college_department_table.c.CollegeDepartmentID, department_table.c.DepartmentName)
                        .join(department_table, department_table.c.DepartmentID == college_department_table.c.DepartmentID)
                        .where(
                            (college_department_table.c.CollegeID == college_id) &
                            (func.upper(department_table.c.DepartmentName).like(f"%{pattern}%")) &
                            (~func.upper(department_table.c.DepartmentName).like("%GRADUATE%")) &
                            (~func.upper(department_table.c.DepartmentName).like("%GRAD%"))
                        )
                    )
                    result = conn.execute(stmt).first()
                    if result:
                        return result[0]
                
                # More generic "UNDERGRADUATE" pattern
                stmt = (
                    select(college_department_table.c.CollegeDepartmentID, department_table.c.DepartmentName)
                    .join(department_table, department_table.c.DepartmentID == college_department_table.c.DepartmentID)
                    .where(
                        (college_department_table.c.CollegeID == college_id) &
                        (func.upper(department_table.c.DepartmentName).like("%UNDERGRADUATE%")) &
                        (~func.upper(department_table.c.DepartmentName).like("%GRADUATE%"))
                    )
                )
                result = conn.execute(stmt).first()
                if result:
                    return result[0]
                
                # Last resort: generic "ADMISSIONS" (only for undergraduate, exclude graduate)
                stmt = (
                    select(college_department_table.c.CollegeDepartmentID, department_table.c.DepartmentName)
                    .join(department_table, department_table.c.DepartmentID == college_department_table.c.DepartmentID)
                    .where(
                        (college_department_table.c.CollegeID == college_id) &
                        (func.upper(department_table.c.DepartmentName).like("%ADMISSIONS%")) &
                        (~func.upper(department_table.c.DepartmentName).like("%GRADUATE%")) &
                        (~func.upper(department_table.c.DepartmentName).like("%GRAD%"))
                    )
                )
                result = conn.execute(stmt).first()
                if result:
                    return result[0]
            
            return None
            
    except Exception as e:
        print(f"Error finding department: {e}")
        return None

def get_current_department_name(engine, college_department_id):
    """Get the department name for a given CollegeDepartmentID."""
    try:
        metadata = MetaData()
        metadata.reflect(bind=engine, only=["CollegeDepartment", "Department"])
        college_department_table = metadata.tables.get("CollegeDepartment")
        department_table = metadata.tables.get("Department")
        
        if college_department_table is None or department_table is None:
            return None
        
        with engine.connect() as conn:
            stmt = (
                select(department_table.c.DepartmentName)
                .join(college_department_table, department_table.c.DepartmentID == college_department_table.c.DepartmentID)
                .where(college_department_table.c.CollegeDepartmentID == college_department_id)
            )
            result = conn.execute(stmt).first()
            return result[0] if result else None
    except Exception:
        return None

def main():
    """Main function to correct department assignments."""
    print("="*80)
    print("DEPARTMENT ASSIGNMENT CORRECTION SCRIPT")
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
        metadata.reflect(bind=engine, only=[
            "Program",
            "ProgramDepartmentLink",
            "CollegeDepartment",
            "Department",
            "College"
        ])
        
        program_table = metadata.tables.get("Program")
        program_link_table = metadata.tables.get("ProgramDepartmentLink")
        college_department_table = metadata.tables.get("CollegeDepartment")
        department_table = metadata.tables.get("Department")
        college_table = metadata.tables.get("College")
        
        if (program_table is None or program_link_table is None or 
            college_department_table is None or department_table is None):
            print("❌ Required tables not found in database.")
            sys.exit(1)
        
        # Get all unique colleges that have programs
        print("\nFetching colleges with programs...")
        with engine.connect() as conn:
            stmt = (
                select(
                    program_link_table.c.CollegeID,
                    college_table.c.CollegeName,
                    func.count(program_link_table.c.ProgramID).label('program_count')
                )
                .join(college_table, program_link_table.c.CollegeID == college_table.c.CollegeID, isouter=True)
                .group_by(program_link_table.c.CollegeID, college_table.c.CollegeName)
                .order_by(college_table.c.CollegeName)
            )
            colleges_with_programs = conn.execute(stmt).fetchall()
        
        print(f"✓ Found {len(colleges_with_programs)} colleges with programs")
        
        # Map departments based on program level - process college by college and update immediately
        print("\nMapping and reassigning departments based on program levels (college by college)...")
        print("="*80)
        
        stats = {
            'total_colleges': len(colleges_with_programs),
            'total_programs': 0,
            'graduate_programs': 0,
            'undergraduate_programs': 0,
            'unknown_level': 0,
            'no_department_found': 0,
            'colleges_processed': 0,
            'colleges_with_issues': 0,
            'programs_updated': 0,
            'programs_skipped': 0
        }
        
        # Process each college one by one and update immediately
        for idx, college_row in enumerate(colleges_with_programs, 1):
            college_id = college_row.CollegeID
            college_name = college_row.CollegeName or f"College #{college_id}"
            program_count = college_row.program_count
            
            print(f"\n[{idx}/{len(colleges_with_programs)}] Processing: {college_name} ({program_count} programs)")
            
            # Get all programs for this college
            with engine.connect() as conn:
                stmt = (
                    select(
                        program_table.c.ProgramID,
                        program_table.c.ProgramName,
                        program_table.c.Level,
                        program_link_table.c.CollegeDepartmentID,
                        department_table.c.DepartmentName
                    )
                    .join(program_link_table, program_table.c.ProgramID == program_link_table.c.ProgramID)
                    .join(college_department_table, program_link_table.c.CollegeDepartmentID == college_department_table.c.CollegeDepartmentID)
                    .join(department_table, college_department_table.c.DepartmentID == department_table.c.DepartmentID)
                    .where(program_link_table.c.CollegeID == college_id)
                    .order_by(program_table.c.ProgramID)
                )
                college_programs = conn.execute(stmt).fetchall()
            
            college_updated = 0
            college_skipped = 0
            college_issues = 0
            
            # Process each program for this college and update immediately
            with engine.begin() as conn:
                for program_row in college_programs:
                    program_id = program_row.ProgramID
                    program_name = program_row.ProgramName
                    level = program_row.Level
                    current_dept_id = program_row.CollegeDepartmentID
                    current_dept_name = program_row.DepartmentName
                    
                    stats['total_programs'] += 1
                    
                    # Determine if program is graduate or undergraduate based on level only
                    is_graduate = is_graduate_level(level)
                    is_undergraduate = is_undergraduate_level(level)
                    
                    if is_graduate:
                        stats['graduate_programs'] += 1
                        expected_type = "Graduate"
                    elif is_undergraduate:
                        stats['undergraduate_programs'] += 1
                        expected_type = "Undergraduate"
                    else:
                        stats['unknown_level'] += 1
                        college_skipped += 1
                        continue  # Skip programs with unknown levels
                    
                    # Find correct department based on level
                    correct_dept_id = find_department_by_type(engine, college_id, is_graduate)
                    
                    if correct_dept_id:
                        # Check if update is needed (different department)
                        if current_dept_id != correct_dept_id:
                            # Update immediately
                            update_stmt = (
                                update(program_link_table)
                                .where(program_link_table.c.ProgramID == program_id)
                                .where(program_link_table.c.CollegeID == college_id)
                                .values(CollegeDepartmentID=correct_dept_id)
                            )
                            conn.execute(update_stmt)
                            college_updated += 1
                            stats['programs_updated'] += 1
                        else:
                            # Already correct, no update needed
                            college_skipped += 1
                            stats['programs_skipped'] += 1
                    else:
                        stats['no_department_found'] += 1
                        college_issues += 1
                        if college_issues == 1:  # Only show college name once per college
                            print(f"   ⚠️  Warning: Could not find {expected_type} Admissions for some programs")
            
            stats['colleges_processed'] += 1
            if college_updated > 0:
                print(f"   ✓ {college_updated} programs reassigned")
            if college_skipped > 0:
                print(f"   - {college_skipped} programs already correct or skipped")
            if college_issues > 0:
                stats['colleges_with_issues'] += 1
                print(f"   ⚠️  {college_issues} programs could not be mapped (no department found)")
        
        # Print final statistics
        print("\n" + "="*80)
        print("REASSIGNMENT SUMMARY")
        print("="*80)
        print(f"Colleges processed: {stats['colleges_processed']}/{stats['total_colleges']}")
        print(f"Colleges with issues: {stats['colleges_with_issues']}")
        print(f"\nTotal programs analyzed: {stats['total_programs']}")
        print(f"  - Graduate programs: {stats['graduate_programs']}")
        print(f"  - Undergraduate programs: {stats['undergraduate_programs']}")
        print(f"  - Unknown level (skipped): {stats['unknown_level']}")
        print(f"  - No department found: {stats['no_department_found']}")
        print(f"\n✓ Programs reassigned: {stats['programs_updated']}")
        print(f"  - Programs already correct/skipped: {stats['programs_skipped']}")
        print("\n✓ Department assignments have been updated based on program levels!")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        engine.dispose()

if __name__ == "__main__":
    main()

