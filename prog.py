# import modules
import pandas as pd
from ddgs import DDGS
import time
import os
import json
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Read the Excel file
df = pd.read_excel('Univs.xlsx')

# Get the first column (by position, regardless of column name)
# Process ALL universities
list_of_univs = df.iloc[:, 0].tolist()

# Program URLs cache file
PROGRAM_URLS_CACHE_FILE = 'university_program_urls_cache.json'

def load_program_urls_cache():
    """Load previously found program URLs from cache file."""
    if os.path.exists(PROGRAM_URLS_CACHE_FILE):
        try:
            with open(PROGRAM_URLS_CACHE_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            print(f"⚠️  Warning: Could not load program URLs cache: {e}")
            return {}
    return {}

def save_program_urls_cache(cache):
    """Save program URLs to cache file."""
    try:
        with open(PROGRAM_URLS_CACHE_FILE, 'w', encoding='utf-8') as f:
            json.dump(cache, f, indent=2, ensure_ascii=False)
    except Exception as e:
        print(f"⚠️  Warning: Could not save program URLs cache: {e}")

def search_program_url(college_name, program_type, ddgs):
    """Search for program URL (graduate or undergraduate) for a college."""
    # Search for the program URL - only .edu domains
    query = f'"{college_name}" {program_type} programs site:.edu'
    program_url = None
    
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
                    # Also check if URL contains program-related keywords
                    if any(keyword in url_lower for keyword in [program_type.lower(), 'program', 'major', 'degree']):
                        program_url = url
                        break
        except (IndexError, AttributeError):
            continue
    
    return program_url

# Load existing program URLs from cache
print("="*80)
print("STEP 1: LOADING CACHED PROGRAM URLs")
print("="*80)

results = load_program_urls_cache()
if results:
    print(f"✓ Loaded program URLs for {len(results)} universities from cache file: {PROGRAM_URLS_CACHE_FILE}")
else:
    print(f"ℹ️  No cache file found. Will search for all program URLs.")

# Initialize results dictionary with all universities from Excel
for college in list_of_univs:
    if college not in results:
        results[college] = {
            "Graduate Programs URL": None,
            "Undergraduate Programs URL": None
        }

# Find universities that need program URLs searched
universities_to_search = []
for college in list_of_univs:
    college_data = results.get(college, {})
    if not college_data.get("Graduate Programs URL") or not college_data.get("Undergraduate Programs URL"):
        universities_to_search.append(college)

print("\n" + "="*80)
print("STEP 2: SEARCHING FOR PROGRAM URLs")
print("="*80)

if universities_to_search:
    print(f"Need to search for program URLs for {len(universities_to_search)} universities...")
    
    # Search for each college's program URLs
    with DDGS() as ddgs:
        for i, college in enumerate(universities_to_search, 1):
            try:
                print(f"\n[{i}/{len(universities_to_search)}] Processing: {college}")
                
                # Search for Graduate Programs URL
                if not results[college].get("Graduate Programs URL"):
                    print(f"  Searching for Graduate Programs URL...")
                    grad_url = search_program_url(college, "graduate", ddgs)
                    if grad_url:
                        results[college]["Graduate Programs URL"] = grad_url
                        print(f"    ✓ Found: {grad_url}")
                    else:
                        print(f"    ✗ Not found")
                    time.sleep(1)
                
                # Search for Undergraduate Programs URL
                if not results[college].get("Undergraduate Programs URL"):
                    print(f"  Searching for Undergraduate Programs URL...")
                    undergrad_url = search_program_url(college, "undergraduate", ddgs)
                    if undergrad_url:
                        results[college]["Undergraduate Programs URL"] = undergrad_url
                        print(f"    ✓ Found: {undergrad_url}")
                    else:
                        print(f"    ✗ Not found")
                    time.sleep(1)
                
            except Exception as e:
                print(f"  ✗ Error: {str(e)}")
                time.sleep(1)
    
    # Save all program URLs to cache
    print(f"\nSaving program URLs to cache...")
    save_program_urls_cache(results)
    print(f"✓ Cache saved to {PROGRAM_URLS_CACHE_FILE}")
else:
    print("✓ All program URLs already in cache. Using cached URLs (no searching needed).")

# Print summary
grad_found = sum(1 for v in results.values() if v.get("Graduate Programs URL"))
undergrad_found = sum(1 for v in results.values() if v.get("Undergraduate Programs URL"))
both_found = sum(1 for v in results.values() if v.get("Graduate Programs URL") and v.get("Undergraduate Programs URL"))

print("\n" + "="*80)
print("SUMMARY")
print("="*80)
print(f"Total universities: {len(list_of_univs)}")
print(f"Graduate Programs URLs found: {grad_found}/{len(list_of_univs)}")
print(f"Undergraduate Programs URLs found: {undergrad_found}/{len(list_of_univs)}")
print(f"Both URLs found: {both_found}/{len(list_of_univs)}")

print("\n" + "="*80)
print("COMPLETED")
print("="*80)
