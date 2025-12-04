#!/usr/bin/env python3
"""
Script to fetch university courses from Yocket API and save to Excel files.

Fetches courses for university IDs 0-5000 and saves data to Excel files.
Uses multiprocessing for faster data fetching.
"""

import requests
import json
import pandas as pd
import time
import os
from datetime import datetime
from pathlib import Path
from multiprocessing import Pool, Manager, cpu_count
from functools import partial

def fetch_university_courses(university_id, page=0, items=500):
    """Fetch courses for a specific university ID from Yocket API.
    
    Args:
        university_id: University ID to fetch courses for
        page: Page number (default 0)
        items: Number of items per page (default 100 to get more courses)
    
    Returns:
        dict: API response or None if error
    """
    url = f"https://api.yocket.com/universities/courses/{university_id}/{page}?is_review=0&page={page}&items={items}&search&partner_only=false&is_gre_required&partner_benefit"
    
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'application/json'
        }
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.Timeout:
        print(f"  ⚠️  Timeout fetching university {university_id}")
        return None
    except requests.exceptions.RequestException as e:
        # Don't print errors for every failed request (many IDs won't exist)
        return None
    except json.JSONDecodeError:
        return None

def extract_course_data(api_response):
    """Extract course data from API response.
    
    Args:
        api_response: Full API response dictionary
    
    Returns:
        list: List of course dictionaries, or None if invalid response
    """
    if not api_response or not isinstance(api_response, dict):
        return None
    
    # Check if state is true
    if api_response.get("state") != True:
        return None
    
    # Extract courses data
    try:
        courses_data = api_response.get("data", {}).get("courses", {})
        results = courses_data.get("results", [])
        return results
    except (AttributeError, KeyError) as e:
        print(f"  ⚠️  Error extracting course data: {e}")
        return None

def courses_to_dataframe(courses, university_id, university_slug=None):
    """Convert list of courses to pandas DataFrame.
    
    Args:
        courses: List of course dictionaries
        university_id: University ID
        university_slug: University slug (if available from first course)
    
    Returns:
        pandas.DataFrame: DataFrame with course data
    """
    if not courses:
        return pd.DataFrame()
    
    # Extract university slug from first course if not provided
    if not university_slug and courses:
        university_slug = courses[0].get("slug", f"university-{university_id}")
    
    # Flatten course data
    rows = []
    for course in courses:
        row = {
            'university_id': course.get('university_id', university_id),
            'university_slug': course.get('slug', university_slug),
            'university_course_id': course.get('university_course_id'),
            'credential': course.get('credential'),
            'university_course_name': course.get('university_course_name'),
            'school_name': course.get('school_name'),
            'course_level': course.get('course_level'),
            'converted_tuition_fee': course.get('converted_tuition_fee'),
            'duration': course.get('duration'),
            'is_fee_waived': course.get('is_fee_waived'),
            'actual_tuition_fee': course.get('actual_tuition_fee'),
            'level': course.get('level'),
            'is_partner': course.get('is_partner'),
        }
        
        # Extract deadlines
        deadlines = course.get('deadlines', [])
        if deadlines:
            deadline_dates = [d.get('deadline_date') for d in deadlines if d.get('deadline_date')]
            deadline_types = [d.get('type') for d in deadlines if d.get('type')]
            row['deadline_dates'] = ', '.join(deadline_dates) if deadline_dates else None
            row['deadline_types'] = ', '.join(deadline_types) if deadline_types else None
        else:
            row['deadline_dates'] = None
            row['deadline_types'] = None
        
        rows.append(row)
    
    return pd.DataFrame(rows)

def save_to_excel(df, university_id, university_slug, output_dir):
    """Save DataFrame to Excel file.
    
    Args:
        df: pandas DataFrame
        university_id: University ID
        university_slug: University slug for filename
        output_dir: Output directory path
    """
    if df.empty:
        return False
    
    # Create output directory if it doesn't exist
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Create filename
    safe_slug = university_slug.replace('/', '_').replace('\\', '_')
    filename = f"university_{university_id}_{safe_slug}_courses.xlsx"
    filepath = output_dir / filename
    
    try:
        df.to_excel(filepath, index=False, engine='openpyxl')
        return True
    except Exception as e:
        print(f"  ❌ Error saving Excel file: {e}")
        return False

def process_university(university_id, output_dir):
    """Process a single university ID - worker function for multiprocessing.
    
    Args:
        university_id: University ID to process
        output_dir: Output directory for Excel files
    
    Returns:
        dict: Result with status and data
    """
    result = {
        'university_id': university_id,
        'status': 'unknown',
        'courses_count': 0,
        'total_courses': 0,
        'university_slug': None,
        'university_name': None,
        'filename': None,
        'error': None
    }
    
    try:
        # Fetch courses
        api_response = fetch_university_courses(university_id, items=500)
        
        if api_response is None:
            result['status'] = 'no_data'
            return result
        
        # Extract course data
        courses = extract_course_data(api_response)
        
        if courses is None:
            result['status'] = 'invalid_response'
            return result
        
        if not courses:
            result['status'] = 'no_courses'
            return result
        
        # Get total courses count from API response
        total_courses = api_response.get("data", {}).get("courses", {}).get("total", len(courses))
        
        # Get university slug from first course
        university_slug = courses[0].get('slug', f'university-{university_id}')
        university_name = university_slug.replace('-', ' ').title()
        
        # Convert to DataFrame
        df = courses_to_dataframe(courses, university_id, university_slug)
        
        if df.empty:
            result['status'] = 'empty_dataframe'
            return result
        
        # Save to Excel
        if save_to_excel(df, university_id, university_slug, output_dir):
            result['status'] = 'success'
            result['courses_count'] = len(courses)
            result['total_courses'] = total_courses
            result['university_slug'] = university_slug
            result['university_name'] = university_name
            result['filename'] = f"university_{university_id}_{university_slug.replace('/', '_')}_courses.xlsx"
        else:
            result['status'] = 'save_error'
        
    except Exception as e:
        result['status'] = 'error'
        result['error'] = str(e)
    
    return result

def main():
    """Main function to fetch courses for all universities using multiprocessing."""
    print("="*80)
    print("YOCKET UNIVERSITY COURSES FETCHER (MULTIPROCESSING)")
    print("="*80)
    
    # Configuration
    start_id = 0
    end_id = 5000
    output_dir = "yocket_courses"
    num_processes = cpu_count()  # Use all available CPU cores
    max_successful_universities = 685  # Stop after this many successful fetches
    
    print(f"\nConfiguration:")
    print(f"  University IDs: {start_id} to {end_id}")
    print(f"  Output directory: {output_dir}")
    print(f"  Number of processes: {num_processes}")
    print(f"  Max successful universities: {max_successful_universities}")
    
    # Create output directory
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    # Create list of all university IDs to process
    university_ids = list(range(start_id, end_id + 1))
    total_universities = len(university_ids)
    
    print(f"\nStarting to fetch courses for {total_universities} universities...")
    print("="*80)
    
    # Process results and collect statistics
    stats = {
        'total_attempted': 0,
        'successful': 0,
        'failed': 0,
        'no_courses': 0,
        'no_data': 0,
        'invalid_response': 0,
        'empty_dataframe': 0,
        'save_error': 0,
        'error': 0,
        'total_courses': 0
    }
    
    summary_data = []
    
    # Process universities in parallel and print results as they come
    start_time = time.time()
    
    with Pool(processes=num_processes) as pool:
        # Create partial function with output_dir
        process_func = partial(process_university, output_dir=output_dir)
        
        # Use imap_unordered to get results as they complete (for real-time printing)
        results = pool.imap_unordered(process_func, university_ids)
        
        # Process results as they come in
        for result in results:
            stats['total_attempted'] += 1
            university_id = result['university_id']
            status = result['status']
            
            if status == 'success':
                stats['successful'] += 1
                stats['total_courses'] += result['courses_count']
                summary_data.append({
                    'university_id': university_id,
                    'university_slug': result['university_slug'],
                    'university_name': result['university_name'],
                    'courses_fetched': result['courses_count'],
                    'total_courses': result['total_courses'],
                    'filename': result['filename']
                })
                print(f"[{university_id}] ✓ {result['university_name']}: {result['courses_count']} courses saved (total: {result['total_courses']})")
            elif status == 'no_data':
                stats['no_data'] += 1
                print(f"[{university_id}] ⏭️  Skipping - No data found")
            elif status == 'invalid_response':
                stats['invalid_response'] += 1
                print(f"[{university_id}] ⏭️  Skipping - Invalid response or state not true")
            elif status == 'no_courses':
                stats['no_courses'] += 1
                print(f"[{university_id}] ⏭️  Skipping - No courses found")
            elif status == 'empty_dataframe':
                stats['empty_dataframe'] += 1
                print(f"[{university_id}] ⏭️  Skipping - Could not create data frame")
            elif status == 'save_error':
                stats['save_error'] += 1
                stats['failed'] += 1
                print(f"[{university_id}] ❌ Error saving file")
            elif status == 'error':
                stats['error'] += 1
                stats['failed'] += 1
                print(f"[{university_id}] ❌ Error: {result.get('error', 'Unknown error')}")
            else:
                stats['failed'] += 1
                print(f"[{university_id}] ⏭️  Skipping - Unknown status: {status}")

            # Stop early once we've reached the desired number of successful universities
            if stats['successful'] >= max_successful_universities:
                print(f"\nReached {max_successful_universities} successful universities. Stopping further processing.")
                break
    
    end_time = time.time()
    elapsed_time = end_time - start_time
    
    # Save summary to Excel
    if summary_data:
        summary_df = pd.DataFrame(summary_data)
        summary_file = output_path / "universities_summary.xlsx"
        try:
            summary_df.to_excel(summary_file, index=False, engine='openpyxl')
            print(f"\n✓ Summary saved to: {summary_file}")
        except Exception as e:
            print(f"\n⚠️  Could not save summary: {e}")
    
    # Final summary
    print("\n" + "="*80)
    print("FETCH SUMMARY")
    print("="*80)
    print(f"Total universities attempted: {stats['total_attempted']}")
    print(f"  ✓ Successful (with courses): {stats['successful']}")
    print(f"  ⏭️  No data found: {stats['no_data']}")
    print(f"  ⏭️  Invalid response: {stats['invalid_response']}")
    print(f"  ⏭️  No courses found: {stats['no_courses']}")
    print(f"  ⏭️  Empty dataframe: {stats['empty_dataframe']}")
    print(f"  ❌ Failed/Errors: {stats['failed']} (save errors: {stats['save_error']}, other errors: {stats['error']})")
    print(f"Total courses fetched: {stats['total_courses']}")
    print(f"\nTime elapsed: {elapsed_time:.2f} seconds")
    print(f"Average time per university: {elapsed_time / total_universities:.3f} seconds")
    print(f"\nExcel files saved to: {output_path.absolute()}")
    print("="*80)

if __name__ == "__main__":
    main()

