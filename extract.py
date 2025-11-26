from ddgs import DDGS
import google.generativeai as genai
import os
from dotenv import load_dotenv
import json
from sqlalchemy import MetaData, create_engine, select, func
from sqlalchemy.exc import SQLAlchemyError
from urllib.parse import quote_plus

load_dotenv()

def get_db_engine():
    """Create database engine for standalone script (SQL Server)."""
    server = os.getenv("DB_SERVER", "localhost,1433")
    database = os.getenv("DB_NAME")
    username = os.getenv("DB_USERNAME")
    password = os.getenv("DB_PASSWORD")
    driver = os.getenv("DB_DRIVER", "ODBC Driver 18 for SQL Server")
    
    if not all([database, username, password]):
        print("Error: Database credentials not set. Please set DB_SERVER, DB_NAME, DB_USERNAME, and DB_PASSWORD.")
        return None
    
    odbc_params = (
        f"Driver={driver};"
        f"Server={server};"
        f"Database={database};"
        f"UID={username};"
        f"PWD={password};"
        "Encrypt=no;"
        "TrustServerCertificate=yes;"
    )
    
    connection_url = f"mssql+pyodbc:///?odbc_connect={quote_plus(odbc_params)}"
    
    try:
        engine = create_engine(connection_url, pool_pre_ping=True)
        # Test connection
        with engine.connect() as conn:
            conn.execute(select(1))
        return engine
    except Exception as e:
        print(f"Error connecting to database: {e}")
        import traceback
        traceback.print_exc()
        return None

def get_all_colleges(engine):
    """Get all colleges from the database."""
    try:
        metadata = MetaData()
        metadata.reflect(bind=engine)
        college_table = metadata.tables.get("College")
        
        if college_table is None:
            print("Error: College table not found in database.")
            return []
        
        with engine.connect() as conn:
            stmt = select(college_table.c.CollegeID, college_table.c.CollegeName).order_by(college_table.c.CollegeName)
            rows = conn.execute(stmt).fetchall()
            return [(row.CollegeID, row.CollegeName) for row in rows]
    except Exception as e:
        print(f"Error fetching colleges: {e}")
        import traceback
        traceback.print_exc()
        return []

def find_or_create_department(engine, department_name):
    """Find or create a Department entry. Returns DepartmentID."""
    try:
        metadata = MetaData()
        metadata.reflect(bind=engine)
        department_table = metadata.tables.get("Department")
        
        if department_table is None:
            print("Error: Department table not found.")
            return None
        
        with engine.begin() as conn:
            # Try to find existing department - exact match first
            stmt = select(department_table).where(
                department_table.c.DepartmentName == department_name
            )
            existing = conn.execute(stmt).mappings().first()
            
            if existing:
                print(f"    Found existing department: '{existing['DepartmentName']}' (ID: {existing['DepartmentID']})")
                return existing["DepartmentID"]
            
            # Try case-insensitive exact match (not partial)
            stmt = select(department_table).where(
                func.upper(department_table.c.DepartmentName) == func.upper(department_name)
            )
            existing = conn.execute(stmt).mappings().first()
            
            if existing:
                print(f"    Found existing department (case-insensitive): '{existing['DepartmentName']}' (ID: {existing['DepartmentID']})")
                return existing["DepartmentID"]
            
            # Create new department
            print(f"    Creating new department: '{department_name}'")
            result = conn.execute(
                department_table.insert().values(DepartmentName=department_name)
            )
            new_id = result.inserted_primary_key[0]
            print(f"    Created department ID: {new_id}")
            return new_id
    except Exception as e:
        print(f"Error finding/creating department: {e}")
        import traceback
        traceback.print_exc()
        return None

def save_admissions_office(engine, college_id, department_id, address_info, admissions_url):
    """Save or update admissions office details in CollegeDepartment table."""
    try:
        metadata = MetaData()
        metadata.reflect(bind=engine)
        college_department_table = metadata.tables.get("CollegeDepartment")
        
        if college_department_table is None:
            print("Error: CollegeDepartment table not found.")
            return False
        
        # Prepare data for CollegeDepartment
        dept_data = {
            "CollegeID": college_id,
            "DepartmentID": department_id,
            "AdmissionUrl": admissions_url,
        }
        
        # Map address fields
        if address_info.get("building_name"):
            dept_data["BuildingName"] = address_info["building_name"]
        
        # Parse street address
        street_address = address_info.get("street_address", "")
        if street_address:
            # Try to split street address into Street1 and Street2 if needed
            parts = street_address.split(",", 1)
            dept_data["Street1"] = parts[0].strip()
            if len(parts) > 1:
                dept_data["Street2"] = parts[1].strip()
        
        if address_info.get("city"):
            dept_data["City"] = address_info["city"]
        if address_info.get("state"):
            dept_data["State"] = address_info["state"]
        if address_info.get("zip_code"):
            dept_data["ZipCode"] = address_info["zip_code"]
        if address_info.get("country"):
            dept_data["Country"] = address_info["country"]
        if address_info.get("phone"):
            dept_data["PhoneNumber"] = address_info["phone"]
        if address_info.get("email"):
            dept_data["Email"] = address_info["email"]
        
        with engine.begin() as conn:
            # Check if CollegeDepartment link already exists
            stmt = select(college_department_table).where(
                (college_department_table.c.CollegeID == college_id) &
                (college_department_table.c.DepartmentID == department_id)
            )
            existing = conn.execute(stmt).mappings().first()
            
            if existing:
                # Update existing record
                conn.execute(
                    college_department_table.update()
                    .where(college_department_table.c.CollegeDepartmentID == existing["CollegeDepartmentID"])
                    .values(**dept_data)
                )
                print(f"    Updated existing CollegeDepartment record (ID: {existing['CollegeDepartmentID']})")
            else:
                # Create new record
                result = conn.execute(college_department_table.insert().values(**dept_data))
                new_id = result.inserted_primary_key[0]
                print(f"    Created new CollegeDepartment record (ID: {new_id})")
        
        return True
    except Exception as e:
        print(f"Error saving admissions office: {e}")
        import traceback
        traceback.print_exc()
        return False

def google_search(query, college_name, num_results=10, filter_wikipedia=True):
    """
    Perform a search using DuckDuckGo and return results, prioritizing official pages.
    
    Args:
        query: Search query string
        college_name: Name of the college for better filtering
        num_results: Number of results to fetch (default: 10)
        filter_wikipedia: Whether to filter out Wikipedia results (default: True)
    
    Returns:
        List of result URLs, prioritized (official pages first)
    """
    try:
        all_results = []
        with DDGS() as ddgs:
            # Get more results to filter
            for r in ddgs.text(query, max_results=num_results):
                if 'href' in r:
                    all_results.append(r['href'])
                elif 'url' in r:
                    all_results.append(r['url'])
        
        if not all_results:
            return []
        
        # Extract key words from college name for matching
        college_keywords = []
        for word in college_name.lower().split():
            # Remove common words and keep significant ones
            if word not in ['university', 'college', 'state', 'the', 'of', 'and']:
                college_keywords.append(word)
        
        # Filter and prioritize results - STRICT: Only admissions pages
        best_results = []  # .edu + college name + admissions
        good_results = []  # .edu + admissions
        admissions_results = []  # admissions (any domain)
        
        for url in all_results:
            url_lower = url.lower()
            
            # Skip Wikipedia if filtering is enabled
            if filter_wikipedia and 'wikipedia.org' in url_lower:
                continue
            
            # Skip generic state/government sites
            if any(site in url_lower for site in ['ca.gov', 'visitcalifornia.com', 'britannica.com', 'worldatlas.com']):
                continue
            
            # MUST have admissions in URL
            has_admissions = 'admission' in url_lower or 'admissions' in url_lower
            if not has_admissions:
                continue  # Skip if no admissions keyword
            
            # Check if college name keywords appear in URL
            has_college_name = any(keyword in url_lower for keyword in college_keywords if len(keyword) > 3)
            is_edu = '.edu' in url_lower
            
            # Prioritize: .edu + college name + admissions = best
            if is_edu and has_college_name:
                best_results.append(url)
            # .edu + admissions = good
            elif is_edu:
                good_results.append(url)
            # Any admissions page
            else:
                admissions_results.append(url)
        
        # Return prioritized results - only admissions pages
        return best_results + good_results + admissions_results
        
    except Exception as e:
        print(f"Error performing search: {e}")
        import traceback
        traceback.print_exc()
        return []


def extract_address_with_gemini(college_name: str, url: str, office_type: str = None) -> dict:
    """
    Use Gemini to extract admissions office building address directly from URL.
    
    Args:
        college_name: Name of the college
        url: URL of the admissions page
        office_type: Type of office (e.g., "Undergraduate Admissions" or "Graduate Admissions")
    
    Returns:
        Dictionary with extracted address information
    """
    api_key = os.environ.get("GOOGLE_API_KEY")
    if not api_key:
        print("Warning: GOOGLE_API_KEY not set. Cannot use Gemini for address extraction.")
        return {}
    
    try:
        genai.configure(api_key=api_key)
        
        # Try multiple model names with fallback
        model_candidates = [
            "gemini-3-pro-preview"
        ]
        
        model = None
        for candidate in model_candidates:
            try:
                model = genai.GenerativeModel(f"models/{candidate}")
                break
            except Exception:
                continue
        
        if model is None:
            print("Error: Could not initialize any Gemini model")
            return {}
        
        office_type_context = ""
        if office_type:
            office_type_context = f" This is specifically for the {office_type} office. Make sure you extract details ONLY for the {office_type} office, not any other admissions office."
        
        prompt = f"""Extract the physical building address of the {office_type or 'admissions'} office for {college_name} from the following webpage.{office_type_context}

Extract the information and return it in JSON format with these exact fields:
{{
    "building_name": "Name of the admissions office building (if mentioned)",
    "street_address": "Street address including street number and name",
    "city": "City name",
    "state": "State name or abbreviation",
    "zip_code": "ZIP or postal code",
    "country": "Country (default to USA if not specified)",
    "phone": "Phone number if available",
    "email": "Email address if available",
    "office_hours": "Office hours if mentioned"
}}

If any information is not found, use null for that field. Only extract information that is clearly stated on the page.

Return only valid JSON, no additional text or markdown formatting."""
        
        # Pass URL directly to Gemini (Gemini 1.5+ can process URLs)
        response = model.generate_content([url, prompt])
        
        if response and response.text:
            # Try to parse JSON from response
            text = response.text.strip()
            
            # Extract JSON if it's wrapped in markdown code blocks
            if "```json" in text:
                text = text.split("```json")[1].split("```")[0].strip()
            elif "```" in text:
                text = text.split("```")[1].split("```")[0].strip()
            
            try:
                return json.loads(text)
            except json.JSONDecodeError:
                # If JSON parsing fails, return the raw text
                print(f"Warning: Could not parse JSON from Gemini response. Raw response: {text[:200]}")
                return {"raw_response": text}
        
        return {}
        
    except Exception as e:
        print(f"Error extracting address with Gemini: {e}")
        import traceback
        traceback.print_exc()
        return {}

# Main execution
if __name__ == "__main__":
    # Connect to database
    print("Connecting to database...")
    engine = get_db_engine()
    if not engine:
        print("Failed to connect to database. Exiting.")
        exit(1)
    
    print("Connected successfully!\n")
    
    # Get all colleges from database
    print("Fetching colleges from database...")
    colleges = get_all_colleges(engine)
    
    if not colleges:
        print("No colleges found in database. Exiting.")
        exit(1)
    
    print(f"Found {len(colleges)} colleges in database.\n")
    print("="*80 + "\n")
    print("NOTE: This script will search for BOTH Undergraduate and Graduate Admissions offices.\n")
    print("="*80 + "\n")
    
    success_count = 0
    error_count = 0
    total_offices_found = 0
    
    # Process each college
    for college_id, college_name in colleges:
        print(f"Processing: {college_name} (ID: {college_id})")
        print("-" * 80)
        
        # Define office types to search for
        office_types = [
            {
                "name": "Undergraduate Admissions",
                "search_queries": [
                    f'"{college_name}" undergraduate admissions office',
                    f'"{college_name}" undergraduate admissions',
                    f'"{college_name}" undergrad admissions office',
                    f"{college_name} undergraduate admissions office",
                ]
            },
            {
                "name": "Graduate Admissions",
                "search_queries": [
                    f'"{college_name}" graduate admissions office',
                    f'"{college_name}" graduate school admissions office',
                    f'"{college_name}" graduate admissions',
                    f"{college_name} graduate admissions office",
                ]
            }
        ]
        
        # Process each office type separately
        for office_type in office_types:
            dept_name = office_type["name"]
            print(f"\n  Searching for {dept_name.lower()}...")
            
            # Try each search query until we find a result
            link = None
            best_match = None
            
            for search_query in office_type["search_queries"]:
                search_results = google_search(search_query, college_name, num_results=10)
                if not search_results:
                    continue
                
                # Score and verify each result to find the best match
                for result_url in search_results:
                    url_lower = result_url.lower()
                    score = 0
                    is_valid = True
                    
                    if dept_name == "Undergraduate Admissions":
                        # Must NOT be a graduate-only page
                        if "graduate" in url_lower and "undergraduate" not in url_lower and "undergrad" not in url_lower:
                            is_valid = False
                            continue
                        # Prefer pages with undergraduate keywords
                        if "undergraduate" in url_lower:
                            score += 10
                        if "undergrad" in url_lower:
                            score += 8
                        # Penalize if it's clearly graduate
                        if "graduate" in url_lower and "undergraduate" not in url_lower:
                            score -= 5
                    elif dept_name == "Graduate Admissions":
                        # Must have graduate keyword
                        if "graduate" not in url_lower and "grad" not in url_lower:
                            is_valid = False
                            continue
                        # Prefer pages with graduate keywords
                        if "graduate" in url_lower:
                            score += 10
                        if "grad" in url_lower and "undergrad" not in url_lower:
                            score += 8
                        # Penalize if it's clearly undergraduate
                        if ("undergraduate" in url_lower or "undergrad" in url_lower) and "graduate" not in url_lower:
                            score -= 5
                            is_valid = False
                            continue
                    
                    # Must have admissions keyword
                    if "admission" not in url_lower:
                        score -= 3
                    
                    if is_valid and (best_match is None or score > best_match[1]):
                        best_match = (result_url, score)
            
            if best_match:
                link = best_match[0]
                print(f"    Selected URL (score: {best_match[1]}): {link}")
            
            if not link:
                print(f"    ❌ No {dept_name.lower()} office found for {college_name}")
                continue
            
            print(f"    Found {dept_name} URL: {link}")
            
            # Process this specific office
            try:
                # Extract address using Gemini directly from URL
                print(f"    Extracting {dept_name.lower()} address using Gemini...")
                address_info = extract_address_with_gemini(college_name, link, dept_name)
                
                if not address_info or "raw_response" in address_info:
                    print(f"    ⚠️  Could not extract address information for {dept_name}")
                    if "raw_response" in address_info:
                        print(f"    Raw response: {address_info['raw_response'][:200]}")
                    error_count += 1
                    continue
                
                # Find or create department for this specific office type
                print(f"    Finding/creating '{dept_name}' department...")
                dept_id = find_or_create_department(engine, dept_name)
                if not dept_id:
                    print(f"    ❌ Failed to create/find {dept_name} department")
                    error_count += 1
                    continue
                print(f"    Using Department ID: {dept_id} for '{dept_name}'")
                
                # Save to database
                print(f"    Saving {dept_name.lower()} to database...")
                if save_admissions_office(engine, college_id, dept_id, address_info, link):
                    print(f"    ✅ Successfully saved {dept_name} office")
                    print(f"    Address: {address_info.get('street_address', 'N/A')}, {address_info.get('city', 'N/A')}, {address_info.get('state', 'N/A')}")
                    success_count += 1
                    total_offices_found += 1
                else:
                    print(f"    ❌ Failed to save {dept_name} office")
                    error_count += 1
                
            except Exception as e:
                print(f"    ❌ Error processing {dept_name} office: {e}")
                import traceback
                traceback.print_exc()
                error_count += 1
        
        print("\n" + "="*80 + "\n")
    
    # Summary
    print(f"\n{'='*80}")
    print(f"Processing Complete!")
    print(f"  Total colleges processed: {len(colleges)}")
    print(f"  Total admissions offices found: {total_offices_found}")
    print(f"  Successfully saved: {success_count}")
    print(f"  Errors: {error_count}")
    print(f"{'='*80}")
