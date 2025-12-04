#!/usr/bin/env python3
"""
Merge all course Excel files from the yocket_courses folder into a single Excel file.

Each row from every per-university file will be appended (vertical merge).
"""

import os
from pathlib import Path

import pandas as pd


def main():
    base_dir = Path(__file__).resolve().parent
    input_dir = base_dir / "yocket_courses"
    output_file = base_dir / "yocket_courses_merged.xlsx"

    print("=" * 80)
    print("MERGING YOCKET COURSE EXCEL FILES")
    print("=" * 80)

    if not input_dir.exists():
        print(f"❌ Input directory not found: {input_dir}")
        return

    # Find all Excel files that look like university course files
    excel_files = sorted(input_dir.glob("university_*_courses.xlsx"))

    if not excel_files:
        print(f"❌ No matching Excel files found in {input_dir}")
        return

    print(f"Found {len(excel_files)} Excel files to merge.")

    all_dfs = []
    total_rows = 0

    for idx, file_path in enumerate(excel_files, 1):
        try:
            df = pd.read_excel(file_path)

            if df.empty:
                print(f"[{idx}/{len(excel_files)}] {file_path.name}: empty, skipping.")
                continue

            # Optionally add source file info
            df["source_file"] = file_path.name

            all_dfs.append(df)
            total_rows += len(df)
            print(f"[{idx}/{len(excel_files)}] {file_path.name}: {len(df)} rows")
        except Exception as e:
            print(f"[{idx}/{len(excel_files)}] {file_path.name}: ❌ error reading file: {e}")

    if not all_dfs:
        print("❌ No data frames to merge (all files empty or failed).")
        return

    merged_df = pd.concat(all_dfs, ignore_index=True)

    print("=" * 80)
    print(f"Total rows to write: {len(merged_df)} (from {total_rows} read)")

    try:
        merged_df.to_excel(output_file, index=False, engine="openpyxl")
        print(f"✓ Merged Excel written to: {output_file}")
    except Exception as e:
        print(f"❌ Error writing merged Excel file: {e}")


if __name__ == "__main__":
    main()


