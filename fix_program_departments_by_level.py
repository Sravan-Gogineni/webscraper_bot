"""
Script to reassign program departments based on:
1. Program's college/university (from ProgramTermDetails)
2. Program's level (Graduate vs Undergraduate)
3. Only assign departments from the same college/university

This ensures data integrity by preventing cross-university department assignments.
"""

import os
import sys
from sqlalchemy import create_engine, select, func, MetaData
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv
from tqdm import tqdm

# Load environment variables
load_dotenv()

def get_db_connection_url():
    """Build database connection URL from environment variables."""
    server = os.getenv("DB_SERVER", "localhost,1433")
    database = os.getenv("DB_NAME")
    username = os.getenv("DB_USERNAME")
    password = os.getenv("DB_PASSWORD")
    driver = os.getenv("DB_DRIVER", "ODBC Driver 18 for SQL Server")

    if not all([database, username, password]):
        print("ERROR: Database credentials not found in environment variables.")
        print("Required: DB_SERVER, DB_NAME, DB_USERNAME, DB_PASSWORD")
        return None

    odbc_params = (
        f"Driver={driver};"
        f"Server={server};"
        f"Database={database};"
        f"UID={username};"
        f"PWD={password};"
        f"TrustServerCertificate=yes;"
    )

    return f"mssql+pyodbc:///?odbc_connect={odbc_params.replace(' ', '+')}"


def fetch_table(engine, table_name, required=True):
    """Fetch a table from the database metadata."""
    try:
        metadata = MetaData()
        metadata.reflect(bind=engine)
        if table_name in metadata.tables:
            return metadata.tables[table_name]
        else:
            if required:
                print(f"ERROR: Table '{table_name}' not found in database.")
                return None
            return None
    except Exception as e:
        print(f"ERROR fetching table {table_name}: {e}")
        return None


def is_graduate_level(level_str):
    """Check if program level indicates graduate studies."""
    if not level_str:
        return False
    level_upper = str(level_str).upper()
    graduate_keywords = [
        "GRADUATE", "GRAD", "MASTER", "M.S.", "M.A.", "MS", "MA",
        "MBA", "DOCTOR", "DOCTORATE", "PHD", "PH.D", "PH.D.",
        "ED.D", "J.D", "D.SC", "DBA", "M.ED", "MFA",
        "POSTGRADUATE", "POST-GRADUATE", "POST GRADUATE"
    ]
    return any(keyword in level_upper for keyword in graduate_keywords)


def is_undergraduate_level(level_str):
    """Check if program level indicates undergraduate studies."""
    if not level_str:
        return False
    level_upper = str(level_str).upper()
    undergraduate_keywords = [
        "UNDERGRADUATE", "UNDERGRAD", "BACHELOR", "B.S.", "B.A.", 
        "BS", "BA", "BSC", "BAC", "BACHELOR'S", "BACHELORS",
        "ASSOCIATE", "A.S.", "A.A.", "AS", "AA"
    ]
    return any(keyword in level_upper for keyword in undergraduate_keywords)


def find_best_department(conn, college_id, program_level, college_department_table, department_table):
    """
    Find the best matching department for a program based on:
    - College/University ID (must match)
    - Program level (Graduate vs Undergraduate)
    
    Priority:
    1. Graduate programs → "Graduate Admissions" departments
    2. Undergraduate programs → "Undergraduate Admissions" departments
    3. Any "Admissions" department
    4. Any department from the college
    """
    is_graduate = is_graduate_level(program_level)
    is_undergraduate = is_undergraduate_level(program_level)
    
    # Build base query for departments from this college only
    dept_query = (
        select(
            college_department_table.c.CollegeDepartmentID,
            department_table.c.DepartmentName
        )
        .select_from(
            college_department_table.join(
                department_table,
                department_table.c.DepartmentID == college_department_table.c.DepartmentID
            )
        )
        .where(college_department_table.c.CollegeID == college_id)
    )
    
    # Strategy 1: For graduate programs, find "Graduate Admissions"
    if is_graduate:
        result = conn.execute(
            dept_query.where(
                func.upper(department_table.c.DepartmentName).like("%GRADUATE%ADMISSIONS%")
            )
        ).mappings().first()
        if result:
            return result
    
    # Strategy 2: For undergraduate programs, find "Undergraduate Admissions"
    if is_undergraduate:
        result = conn.execute(
            dept_query.where(
                func.upper(department_table.c.DepartmentName).like("%UNDERGRADUATE%ADMISSIONS%")
            )
        ).mappings().first()
        if result:
            return result
    
    # Strategy 3: Find any "Admissions" department from this college
    result = conn.execute(
        dept_query.where(
            func.upper(department_table.c.DepartmentName).like("%ADMISSIONS%")
        )
    ).mappings().first()
    if result:
        return result
    
    # Strategy 4: Last resort - any department from this college
    result = conn.execute(dept_query).mappings().first()
    return result


def assign_program_departments():
    """Main function to assign departments to all programs."""
    print("=" * 80)
    print("Program Department Assignment Script")
    print("=" * 80)
    print()
    
    # Get database connection
    connection_url = get_db_connection_url()
    if not connection_url:
        sys.exit(1)
    
    try:
        engine = create_engine(connection_url, echo=False)
        print("✓ Database connection established")
    except Exception as e:
        print(f"ERROR: Failed to connect to database: {e}")
        sys.exit(1)
    
    # Fetch required tables
    print("Fetching database tables...")
    program_table = fetch_table(engine, "Program")
    program_term_table = fetch_table(engine, "ProgramTermDetails", required=False)
    program_link_table = fetch_table(engine, "ProgramDepartmentLink", required=False)
    college_table = fetch_table(engine, "College", required=False)
    college_department_table = fetch_table(engine, "CollegeDepartment", required=False)
    department_table = fetch_table(engine, "Department", required=False)
    
    # Check if all required tables are available
    required_tables = {
        "Program": program_table,
        "ProgramTermDetails": program_term_table,
        "ProgramDepartmentLink": program_link_table,
        "College": college_table,
        "CollegeDepartment": college_department_table,
        "Department": department_table
    }
    
    missing_tables = [name for name, table in required_tables.items() if table is None]
    if missing_tables:
        print(f"ERROR: Required tables are missing: {', '.join(missing_tables)}")
        sys.exit(1)
    
    print("✓ All required tables found")
    print()
    
    # Statistics tracking
    stats = {
        'total_programs': 0,
        'processed': 0,
        'updated': 0,
        'created': 0,
        'skipped_no_college': 0,
        'skipped_no_dept': 0,
        'errors': []
    }
    
    with engine.begin() as conn:
        # Get all programs with their college from ProgramTermDetails (first term record)
        print("Fetching programs with their college assignments...")
        
        program_term_subquery = (
            select(
                program_term_table.c.ProgramID,
                program_term_table.c.CollegeID,
                func.row_number()
                .over(
                    partition_by=program_term_table.c.ProgramID,
                    order_by=program_term_table.c.ProgramTermID
                )
                .label('rn')
            )
            .select_from(program_term_table)
            .subquery()
        )
        
        programs_with_colleges = conn.execute(
            select(
                program_term_subquery.c.ProgramID,
                program_term_subquery.c.CollegeID
            )
            .where(program_term_subquery.c.rn == 1)
        ).mappings().all()
        
        stats['total_programs'] = len(programs_with_colleges)
        print(f"✓ Found {stats['total_programs']} programs with college assignments")
        print()
        
        # Process each program with progress bar
        print("Processing programs...")
        print("-" * 80)
        
        # Create progress bar
        progress_bar = tqdm(
            programs_with_colleges,
            total=stats['total_programs'],
            desc="Processing",
            unit="program",
            ncols=100,
            bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]'
        )
        
        for prog_term_row in progress_bar:
            program_id = prog_term_row["ProgramID"]
            college_id = prog_term_row["CollegeID"]
            
            try:
                # Skip if no college assigned
                if not college_id:
                    stats['skipped_no_college'] += 1
                    continue
                
                # Get program details (especially level)
                program = conn.execute(
                    select(program_table.c.ProgramName, program_table.c.Level)
                    .where(program_table.c.ProgramID == program_id)
                ).mappings().first()
                
                if not program:
                    stats['errors'].append(f"Program ID {program_id}: Program not found")
                    continue
                
                program_name = program.get("ProgramName") or f"Program #{program_id}"
                program_level = program.get("Level") or ""
                
                # Find the best matching department for this college and level
                dept_candidate = find_best_department(
                    conn, college_id, program_level,
                    college_department_table, department_table
                )
                
                if not dept_candidate:
                    stats['skipped_no_dept'] += 1
                    college_name = conn.execute(
                        select(college_table.c.CollegeName)
                        .where(college_table.c.CollegeID == college_id)
                    ).scalar()
                    progress_bar.set_postfix({
                        'status': 'Skipped',
                        'updated': stats['updated'],
                        'created': stats['created'],
                        'errors': len(stats['errors'])
                    })
                    progress_bar.write(f"⚠️  SKIP: {program_name[:50]} (No department found for college ID {college_id} - {college_name})")
                    continue
                
                college_dept_id = dept_candidate["CollegeDepartmentID"]
                dept_name = dept_candidate["DepartmentName"]
                
                # Check if ProgramDepartmentLink exists for this program
                existing_links = conn.execute(
                    select(program_link_table)
                    .where(program_link_table.c.ProgramID == program_id)
                ).mappings().all()
                
                # Check if there's already a link with the correct (CollegeID, ProgramID) combination
                correct_link = None
                for link in existing_links:
                    if link["CollegeID"] == college_id:
                        correct_link = link
                        break
                
                if correct_link:
                    # Check if update is needed (same college, different department)
                    if correct_link["CollegeDepartmentID"] != college_dept_id:
                        # Update the existing correct link
                        conn.execute(
                            program_link_table.update()
                            .where(program_link_table.c.LinkID == correct_link["LinkID"])
                            .values(CollegeDepartmentID=college_dept_id)
                        )
                        stats['updated'] += 1
                        level_info = f" ({program_level})" if program_level else ""
                        progress_bar.set_postfix({
                            'status': 'Updated',
                            'updated': stats['updated'],
                            'created': stats['created'],
                            'errors': len(stats['errors'])
                        })
                        progress_bar.write(f"✓ UPDATE: {program_name[:50]}{level_info} → {dept_name}")
                    else:
                        # Already correct, but may need to clean up duplicates
                        if len(existing_links) > 1:
                            # Delete duplicate links (keep only the correct one)
                            for link in existing_links:
                                if link["LinkID"] != correct_link["LinkID"]:
                                    conn.execute(
                                        program_link_table.delete()
                                        .where(program_link_table.c.LinkID == link["LinkID"])
                                    )
                            stats['updated'] += 1
                            level_info = f" ({program_level})" if program_level else ""
                            progress_bar.set_postfix({
                                'status': 'Cleanup',
                                'updated': stats['updated'],
                                'created': stats['created'],
                                'errors': len(stats['errors'])
                            })
                            progress_bar.write(f"✓ CLEANUP: {program_name[:50]}{level_info} (removed duplicates)")
                        else:
                            stats['processed'] += 1
                            progress_bar.set_postfix({
                                'status': 'OK',
                                'updated': stats['updated'],
                                'created': stats['created'],
                                'errors': len(stats['errors'])
                            })
                else:
                    # No link with correct college exists
                    # First, delete any existing links for this program (to avoid constraint violations)
                    if existing_links:
                        for link in existing_links:
                            conn.execute(
                                program_link_table.delete()
                                .where(program_link_table.c.LinkID == link["LinkID"])
                            )
                    
                    # Now create the new correct link
                    conn.execute(
                        program_link_table.insert().values(
                            ProgramID=program_id,
                            CollegeID=college_id,
                            CollegeDepartmentID=college_dept_id
                        )
                    )
                    if existing_links:
                        stats['updated'] += 1
                        level_info = f" ({program_level})" if program_level else ""
                        progress_bar.set_postfix({
                            'status': 'Fixed',
                            'updated': stats['updated'],
                            'created': stats['created'],
                            'errors': len(stats['errors'])
                        })
                        progress_bar.write(f"✓ FIXED: {program_name[:50]}{level_info} → {dept_name} (replaced wrong college link)")
                    else:
                        stats['created'] += 1
                        level_info = f" ({program_level})" if program_level else ""
                        progress_bar.set_postfix({
                            'status': 'Created',
                            'updated': stats['updated'],
                            'created': stats['created'],
                            'errors': len(stats['errors'])
                        })
                        progress_bar.write(f"✓ CREATE: {program_name[:50]}{level_info} → {dept_name}")
                
                stats['processed'] += 1
                
            except Exception as e:
                error_msg = f"Program ID {program_id}: {str(e)}"
                stats['errors'].append(error_msg)
                progress_bar.set_postfix({
                    'status': 'Error',
                    'updated': stats['updated'],
                    'created': stats['created'],
                    'errors': len(stats['errors'])
                })
                progress_bar.write(f"✗ ERROR: {error_msg}")
        
        # Close progress bar
        progress_bar.close()
    
    # Print summary
    print()
    print("=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(f"Total programs processed:       {stats['total_programs']}")
    print(f"Successfully processed:         {stats['processed']}")
    print(f"  - Updated existing links:     {stats['updated']}")
    print(f"  - Created new links:          {stats['created']}")
    print(f"Skipped (no college):           {stats['skipped_no_college']}")
    print(f"Skipped (no department found):  {stats['skipped_no_dept']}")
    print(f"Errors:                         {len(stats['errors'])}")
    
    if stats['errors']:
        print()
        print("ERROR DETAILS:")
        for error in stats['errors'][:10]:  # Show first 10 errors
            print(f"  - {error}")
        if len(stats['errors']) > 10:
            print(f"  ... and {len(stats['errors']) - 10} more errors")
    
    print()
    print("=" * 80)
    print("✓ Process completed successfully!")
    print("=" * 80)


if __name__ == "__main__":
    try:
        assign_program_departments()
    except KeyboardInterrupt:
        print("\n\nProcess interrupted by user.")
        sys.exit(1)
    except Exception as e:
        print(f"\n\nFATAL ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

