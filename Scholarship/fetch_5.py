import json
import os
import time
from dotenv import load_dotenv
import google.generativeai as genai
import pandas as pd
import requests
from bs4 import BeautifulSoup

# Load environment variables
load_dotenv()

# BATCH CONFIGURATION
BATCH_NUMBER = 5
COLLEGES_PER_BATCH = 100  # Will process 400-467 (68 colleges)

def get_gemini_model():
    """Initialize and return a Gemini model."""
    api_key = os.environ.get("GOOGLE_API_KEY")
    if not api_key:
        print("⚠️  Error: GOOGLE_API_KEY not set in environment.")
        print("⚠️  Please set GOOGLE_API_KEY in your .env file.")
        return None
    
    try:
        genai.configure(api_key=api_key)
        
        # Try multiple model names with fallback
        model_candidates = [
            "gemini-1.5-pro-latest",
            "gemini-1.5-flash-latest",
            "gemini-pro",
            "gemini-3-pro-preview"
        ]
        
        model = None
        for candidate in model_candidates:
            try:
                model = genai.GenerativeModel(f"models/{candidate}")
                # Test the model with a simple query
                model.generate_content("test")
                print(f"✓ Using Gemini model: {candidate}")
                break
            except Exception as e:
                continue
        
        if model is None:
            print("⚠️  Error: Could not initialize any Gemini model")
            return None
        
        return model
    except Exception as e:
        print(f"⚠️  Error initializing Gemini: {e}")
        return None

def fetch_page_content(url: str) -> str:
    """Fetch and extract text content from a webpage."""
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Remove script and style elements
        for script in soup(["script", "style", "noscript"]):
            script.decompose()
        
        # Get text content
        text = soup.get_text(separator='\n', strip=True)
        # Clean up excessive whitespace
        lines = [line.strip() for line in text.split('\n') if line.strip()]
        return '\n'.join(lines[:5000])  # Limit to first 5000 lines to avoid token limits
    except Exception as e:
        print(f"  ⚠️  Warning: Could not fetch page content: {e}")
        return None

def extract_tuition_fee_with_gemini(college_name: str, url: str, model) -> dict:
    """
    Use Gemini to extract graduate average tuition fee from a URL.
    
    Args:
        college_name: Name of the college
        url: URL of the tuition fee page
        model: Initialized Gemini model
    
    Returns:
        Dictionary with extracted tuition fee information
    """
    if not model:
        return {"error": "Gemini model not available"}
    
    # Enhanced prompt with better instructions
    prompt = f"""You are extracting graduate tuition fee information for {college_name} from a webpage.

TASK: Find the graduate (master's or doctoral) tuition fee per year.

WHAT TO LOOK FOR:
- Look for sections titled: "Graduate Tuition", "Graduate Costs", "Tuition and Fees", "Graduate Programs Cost", "Master's Tuition", "Doctoral Tuition"
- Search for tables, lists, or paragraphs containing tuition information
- Look for numbers followed by terms like: "per year", "per credit hour", "annual", "yearly", "academic year"
- If you see "per credit hour", multiply by typical credits (usually 9-12 per semester, 18-24 per year) to get annual cost
- Prefer OUT-OF-STATE tuition if both in-state and out-of-state are shown
- Look for the most recent academic year (2024-2025, 2025-2026, etc.)

EXTRACTION RULES:
1. Extract ONLY graduate tuition, NOT undergraduate
2. Convert to ANNUAL amount if given per semester or per credit
3. Use USD currency (convert if needed)
4. If multiple programs have different tuition, calculate the average
5. Look carefully - tuition info might be in tables, PDFs, or text paragraphs
6. Check footnotes, sidebars, and expandable sections

IMPORTANT: 
- Extract the actual number if it's clearly stated
- If you see "per credit hour" with a number, calculate: credits_per_year × credit_hour_rate
- If you see "per semester", multiply by 2 for annual
- If information is unclear or missing, set tuition_fee to null

Return your findings in this JSON format:
{{
    "tuition_fee": <number or null>,  // Annual graduate tuition in USD. Calculate if needed (e.g., per credit × credits per year)
    "currency": "USD",
    "is_out_of_state": true/false/null,
    "academic_year": "<year or null>",
    "notes": "<how you found it or calculation method>",
    "confidence": "high/medium/low",
    "source_text": "<exact text from page showing the tuition>"
}}

Return ONLY valid JSON, no markdown formatting."""

    try:
        # Try method 1: Pass URL directly to Gemini
        try:
            response = model.generate_content([url, prompt])
            if response and response.text:
                result = parse_gemini_response(response.text)
                if result and result.get("tuition_fee"):
                    return result
        except Exception as e:
            print(f"  ⚠️  URL method failed, trying page content: {e}")
        
        # Try method 2: Fetch page content and pass to Gemini
        page_content = fetch_page_content(url)
        if page_content:
            # Create a combined prompt with page content
            content_prompt = f"{prompt}\n\nWEBPAGE CONTENT:\n{page_content[:15000]}\n\nExtract the tuition fee from the content above."
            response = model.generate_content(content_prompt)
            if response and response.text:
                result = parse_gemini_response(response.text)
                if result:
                    return result
        
        return {"error": "Could not extract tuition fee from webpage"}
        
    except Exception as e:
        print(f"  ⚠️  Error extracting tuition fee: {e}")
        import traceback
        traceback.print_exc()
        return {"error": str(e)}

def parse_gemini_response(text: str) -> dict:
    """Parse JSON from Gemini response text."""
    if not text:
        return None
    
    text = text.strip()
    
    # Extract JSON if wrapped in markdown
    if "```json" in text:
        text = text.split("```json")[1].split("```")[0].strip()
    elif "```" in text:
        text = text.split("```")[1].split("```")[0].strip()
    
    # Try to find JSON object
    start_idx = text.find('{')
    end_idx = text.rfind('}')
    if start_idx != -1 and end_idx != -1 and end_idx > start_idx:
        text = text[start_idx:end_idx+1]
    
    try:
        result = json.loads(text)
        if isinstance(result, dict):
            # Validate and clean the result
            if "tuition_fee" in result:
                # Ensure tuition_fee is a number or null
                if result["tuition_fee"] is not None:
                    try:
                        result["tuition_fee"] = float(result["tuition_fee"])
                    except (ValueError, TypeError):
                        result["tuition_fee"] = None
            return result
    except json.JSONDecodeError:
        print(f"  ⚠️  JSON parsing failed, raw response: {text[:200]}")
        return None
    
    return None

def load_tuition_urls(json_file_path: str) -> dict:
    """Load tuition fee URLs from JSON file."""
    try:
        with open(json_file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"⚠️  Error: File '{json_file_path}' not found.")
        return {}
    except json.JSONDecodeError as e:
        print(f"⚠️  Error: Invalid JSON in '{json_file_path}': {e}")
        return {}

def load_existing_results(json_filename: str) -> dict:
    """Load existing results from JSON file if it exists."""
    if os.path.exists(json_filename):
        try:
            with open(json_filename, 'r', encoding='utf-8') as f:
                existing = json.load(f)
                print(f"✓ Found existing results file with {len(existing)} colleges")
                return existing
        except Exception as e:
            print(f"⚠️  Warning: Could not load existing results: {e}")
            return {}
    return {}

def save_results_incrementally(json_filename: str, all_tuition_fees: dict):
    """Save results to JSON file incrementally."""
    try:
        with open(json_filename, 'w', encoding='utf-8') as f:
            json.dump(all_tuition_fees, f, indent=2, ensure_ascii=False)
        return True
    except Exception as e:
        print(f"  ⚠️  Error saving to JSON file: {e}")
        return False

def merge_to_main_file(main_json_filename: str, batch_results: dict):
    """Merge batch results into the main results file."""
    try:
        # Load existing main file
        main_results = load_existing_results(main_json_filename)
        
        # Merge batch results into main results
        main_results.update(batch_results)
        
        # Save merged results
        with open(main_json_filename, 'w', encoding='utf-8') as f:
            json.dump(main_results, f, indent=2, ensure_ascii=False)
        return True
    except Exception as e:
        print(f"  ⚠️  Error merging to main file: {e}")
        return False

def fetch_graduate_average_tuition_fees():
    """Main function to fetch graduate average tuition fees for a batch of colleges."""
    print("="*80)
    print(f"GRADUATE AVERAGE TUITION FEE EXTRACTOR - BATCH {BATCH_NUMBER}")
    print("="*80)
    
    # Calculate batch range
    start_idx = (BATCH_NUMBER - 1) * COLLEGES_PER_BATCH
    end_idx = start_idx + COLLEGES_PER_BATCH
    
    # Load URLs from JSON file - try multiple possible paths
    possible_paths = [
        "graduate_tuition_fee_urls.json",
        os.path.join("Scholarship fields", "graduate_tuition_fee_urls.json"),
        os.path.join("Scholarship", "graduate_tuition_fee_urls.json"),
        os.path.join(os.path.dirname(__file__), "graduate_tuition_fee_urls.json"),
    ]
    
    json_file_path = None
    for path in possible_paths:
        if os.path.exists(path):
            json_file_path = path
            break
    
    if not json_file_path:
        print("⚠️  Error: Could not find 'graduate_tuition_fee_urls.json' file.")
        print("   Tried paths:")
        for path in possible_paths:
            print(f"     - {path}")
        print("\n   Please run Grad_Avg_tution.py first to generate URLs.")
        return {}
    
    print(f"\nLoading URLs from: {json_file_path}")
    tuition_urls = load_tuition_urls(json_file_path)
    
    if not tuition_urls:
        print("⚠️  No URLs found. Please run Grad_Avg_tution.py first to generate URLs.")
        return {}
    
    # Filter out colleges without URLs and convert to list for indexing
    colleges_with_urls = [(college, url) for college, url in tuition_urls.items() if url]
    total_colleges = len(colleges_with_urls)
    
    if total_colleges == 0:
        print("⚠️  No colleges with valid URLs found.")
        return {}
    
    # Get batch range
    if start_idx >= total_colleges:
        print(f"⚠️  Batch {BATCH_NUMBER} is out of range. Total colleges: {total_colleges}")
        return {}
    
    batch_colleges = colleges_with_urls[start_idx:end_idx]
    batch_count = len(batch_colleges)
    
    print(f"Found {total_colleges} total colleges with URLs.")
    print(f"Processing Batch {BATCH_NUMBER}: colleges {start_idx + 1} to {min(end_idx, total_colleges)} ({batch_count} colleges)\n")
    
    # Determine output file paths
    output_dir = os.path.dirname(json_file_path) if json_file_path else "."
    batch_json_filename = os.path.join(output_dir, f"fetch_{BATCH_NUMBER}_results.json")
    main_json_filename = os.path.join(output_dir, "graduate_average_tuition_fees.json")
    batch_excel_filename = os.path.join(output_dir, f"fetch_{BATCH_NUMBER}_results.xlsx")
    
    # Load existing batch results to resume
    all_tuition_fees = load_existing_results(batch_json_filename)
    
    # Filter out colleges that have already been processed
    colleges_to_process = []
    for college_name, url in batch_colleges:
        # Skip if already processed
        if college_name in all_tuition_fees:
            existing = all_tuition_fees[college_name]
            if existing.get("url") == url and (existing.get("tuition_fee") is not None or existing.get("error")):
                continue  # Already processed
        colleges_to_process.append((college_name, url))
    
    remaining_count = len(colleges_to_process)
    already_processed = batch_count - remaining_count
    
    if already_processed > 0:
        print(f"✓ Resuming: {already_processed} colleges already processed, {remaining_count} remaining\n")
    
    if remaining_count == 0:
        print(f"✓ All colleges in Batch {BATCH_NUMBER} have already been processed!")
        print(f"  Results file: {batch_json_filename}")
    else:
        # Initialize Gemini model
        print("Initializing Gemini AI...")
        model = get_gemini_model()
        if not model:
            print("⚠️  Cannot proceed without Gemini model. Exiting.")
            return all_tuition_fees
        
        # Process each college
        for idx, (college_name, url) in enumerate(colleges_to_process, 1):
            print(f"\n[{idx}/{remaining_count}] Processing: {college_name}")
            print(f"  URL: {url}")
            
            # Extract tuition fee using Gemini with retry
            result = None
            max_retries = 2
            for attempt in range(max_retries):
                try:
                    result = extract_tuition_fee_with_gemini(college_name, url, model)
                    if result and (result.get("tuition_fee") or result.get("error")):
                        break
                    if attempt < max_retries - 1:
                        print(f"  ⚠️  Retry {attempt + 1}/{max_retries}...")
                        time.sleep(2)
                except Exception as e:
                    print(f"  ⚠️  Attempt {attempt + 1} failed: {e}")
                    if attempt < max_retries - 1:
                        time.sleep(2)
                    else:
                        result = {"error": str(e)}
            
            if not result:
                result = {"error": "No response after retries"}
            
            # Store result
            all_tuition_fees[college_name] = {
                "url": url,
                **result
            }
            
            # Save incrementally after each college
            if save_results_incrementally(batch_json_filename, all_tuition_fees):
                print(f"  💾 Progress saved")
            
            # Also merge to main file
            merge_to_main_file(main_json_filename, {college_name: all_tuition_fees[college_name]})
            
            # Display result
            if "error" in result:
                error_msg = result.get('error', 'Unknown error')
                print(f"  ✗ Error: {error_msg}")
                if result.get("raw_response"):
                    print(f"     Raw response: {result.get('raw_response')[:100]}...")
            elif result.get("tuition_fee"):
                fee = result.get("tuition_fee")
                currency = result.get("currency", "USD")
                year = result.get("academic_year", "N/A")
                confidence = result.get("confidence", "unknown")
                is_out_state = result.get("is_out_of_state")
                state_info = " (Out-of-state)" if is_out_state else " (In-state)" if is_out_state is False else ""
                print(f"  ✓ Found: ${fee:,.0f} {currency}/year{state_info} ({year}) [Confidence: {confidence}]")
                if result.get("notes"):
                    print(f"     Notes: {result.get('notes')[:100]}")
            else:
                confidence = result.get("confidence", "unknown")
                print(f"  ✗ No tuition fee found (confidence: {confidence})")
                if result.get("source_text"):
                    print(f"     Source: {result.get('source_text')[:150]}...")
            
            # Add a small delay to avoid rate limiting
            if idx < remaining_count:
                time.sleep(1.5)
    
    # Final save
    print(f"\n{'='*80}")
    if save_results_incrementally(batch_json_filename, all_tuition_fees):
        print(f"✓ Batch {BATCH_NUMBER} results saved to {batch_json_filename}")
    
    # Final merge to main file
    if merge_to_main_file(main_json_filename, all_tuition_fees):
        print(f"✓ Merged to main results file: {main_json_filename}")
    
    # Prepare data for Excel
    excel_data = []
    for college_name, data in all_tuition_fees.items():
        row = {
            "College Name": college_name,
            "URL": data.get("url", ""),
            "Graduate Average Tuition Fee (USD/year)": data.get("tuition_fee") if data.get("tuition_fee") else "",
            "Currency": data.get("currency", "USD"),
            "Is Out of State": data.get("is_out_of_state") if data.get("is_out_of_state") is not None else "",
            "Academic Year": data.get("academic_year", ""),
            "Confidence": data.get("confidence", ""),
            "Notes": data.get("notes", ""),
            "Error": data.get("error", "")
        }
        excel_data.append(row)
    
    # Save to Excel
    try:
        df = pd.DataFrame(excel_data)
        
        # Reorder columns for better readability
        column_order = [
            "College Name",
            "Graduate Average Tuition Fee (USD/year)",
            "Currency",
            "Is Out of State",
            "Academic Year",
            "Confidence",
            "URL",
            "Notes",
            "Error"
        ]
        df = df[column_order]
        
        # Save to Excel with formatting
        with pd.ExcelWriter(batch_excel_filename, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='Graduate Tuition Fees', index=False)
            
            # Get the worksheet to format
            worksheet = writer.sheets['Graduate Tuition Fees']
            
            # Auto-adjust column widths
            from openpyxl.utils import get_column_letter
            for idx, col in enumerate(df.columns, 1):
                max_length = max(
                    df[col].astype(str).map(len).max(),
                    len(str(col))
                )
                # Set a reasonable max width
                adjusted_width = min(max_length + 2, 50)
                column_letter = get_column_letter(idx)
                worksheet.column_dimensions[column_letter].width = adjusted_width
        
        print(f"✓ Saved Excel results to {batch_excel_filename}")
        
        # Count successful extractions
        successful = len([v for v in all_tuition_fees.values() if v.get("tuition_fee") and not v.get("error")])
        print(f"\n{'='*80}")
        print(f"BATCH {BATCH_NUMBER} SUMMARY:")
        print(f"  Total colleges in batch: {batch_count}")
        print(f"  Successfully extracted: {successful}")
        print(f"  Failed/Not found: {batch_count - successful}")
        print(f"{'='*80}")
    except ImportError:
        print(f"\n⚠️  Warning: pandas or openpyxl not installed. Cannot save Excel file.")
        print(f"   Install with: pip install pandas openpyxl")
    except Exception as e:
        print(f"\n⚠️  Error saving to Excel file: {e}")
        import traceback
        traceback.print_exc()
    
    return all_tuition_fees

if __name__ == "__main__":
    """Run the script to extract graduate average tuition fees for batch."""
    fetch_graduate_average_tuition_fees()

