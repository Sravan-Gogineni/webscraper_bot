import os
import json
from dotenv import load_dotenv
from sqlalchemy import MetaData, create_engine, select
from urllib.parse import quote_plus
from ddgs import DDGS

# Load environment variables
load_dotenv()

def get_db_engine():
    """Create database engine for standalone script (SQL Server)."""
    server = os.getenv("DB_SERVER", "localhost,1433")
    database = os.getenv("DB_NAME")
    username = os.getenv("DB_USERNAME")
    password = os.getenv("DB_PASSWORD")
    driver = os.getenv("DB_DRIVER", "ODBC Driver 18 for SQL Server")
    
    if not all([database, username, password]):
        print("⚠️  Error: Database credentials not set.")
        print("⚠️  Please set DB_SERVER, DB_NAME, DB_USERNAME, and DB_PASSWORD in your environment.")
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
        print(f"⚠️  Error: Could not connect to database: {e}")
        return None

def get_colleges_from_database():
    """Get all college names from the database College table."""
    engine = get_db_engine()
    if not engine:
        return []
    
    try:
        metadata = MetaData()
        metadata.reflect(bind=engine)
        college_table = metadata.tables.get("College")
        
        if college_table is None:
            print("⚠️  Error: College table not found in database.")
            return []
        
        with engine.connect() as conn:
            # Get all college names from the College table
            stmt = select(college_table.c.CollegeName).order_by(college_table.c.CollegeName)
            rows = conn.execute(stmt).fetchall()
            # Filter out None/null values and return as list of strings
            college_names = [str(row.CollegeName).strip() for row in rows 
                           if row.CollegeName and str(row.CollegeName).strip() 
                           and str(row.CollegeName).strip().lower() != 'nan']
            return college_names
    except Exception as e:
        print(f"⚠️  Error fetching colleges from database: {e}")
        import traceback
        traceback.print_exc()
        return []

def search_graduate_tuition_fee_url(college_name, ddgs):
    """Search for graduate tuition fee URL for a college."""
    # Search for the graduate tuition fee URL - only .edu domains
    query = f'"{college_name}" graduate tuition fee site:.edu'
    tuition_url = None
    
    # Get search results - only accept .edu domains
    for r in ddgs.text(query, max_results=20):
        url = None
        if 'href' in r:
            url = r['href']
        elif 'url' in r:
            url = r['url']
        
        if not url:
            continue
        
        url_lower = url.lower()
        
        # Only accept .edu domains - check if domain ends with .edu
        try:
            if url_lower.startswith('http://') or url_lower.startswith('https://'):
                # Remove protocol
                without_protocol = url_lower.split('://', 1)[1]
                # Get domain part (before first /)
                domain = without_protocol.split('/')[0]
                # Check if domain ends with .edu
                if domain.endswith('.edu'):
                    # Also check if URL contains tuition-related keywords
                    if any(keyword in url_lower for keyword in ['tuition', 'fee', 'cost', 'price', 'graduate', 'grad']):
                        tuition_url = url
                        break
        except Exception as e:
            continue
    
    return tuition_url

#Scrape college graduate tuition fee urls for each colleges the colleges are in the database 
def scrape_college_graduate_tution_fee_urls():
    """Scrape graduate tuition fee URLs for all colleges and save to JSON."""
    #Get the colleges from the database 
    colleges = get_colleges_from_database()
    
    if not colleges:
        print("⚠️  No colleges found in database.")
        return {}
    
    print(f"Found {len(colleges)} colleges in database. Starting to scrape URLs...")
    
    # Dictionary to store all URLs: {college_name: url}
    all_tuition_urls = {}
    
    # Use DuckDuckGo Search to find URLs
    with DDGS() as ddgs:
        for idx, college in enumerate(colleges, 1):
            print(f"[{idx}/{len(colleges)}] Searching for: {college}")
            
            # Search for the graduate tuition fee URL
            tuition_url = search_graduate_tuition_fee_url(college, ddgs)
            
            if tuition_url:
                all_tuition_urls[college] = tuition_url
                print(f"  ✓ Found: {tuition_url}")
            else:
                all_tuition_urls[college] = None
                print(f"  ✗ No URL found")
    
    # Save all URLs to JSON file
    json_filename = "graduate_tuition_fee_urls.json"
    try:
        with open(json_filename, 'w', encoding='utf-8') as f:
            json.dump(all_tuition_urls, f, indent=2, ensure_ascii=False)
        print(f"\n✓ Saved {len([v for v in all_tuition_urls.values() if v])} URLs to {json_filename}")
    except Exception as e:
        print(f"⚠️  Error saving to JSON file: {e}")
    
    return all_tuition_urls

if __name__ == "__main__":
    """Run the script to scrape graduate tuition fee URLs for all colleges."""
    print("="*80)
    print("GRADUATE TUITION FEE URL SCRAPER")
    print("="*80)
    scrape_college_graduate_tution_fee_urls()
    print("="*80)
    print("Scraping completed!")
    print("="*80)

