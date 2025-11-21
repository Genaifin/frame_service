#!/usr/bin/env python3
"""
Multi-Fund Data Ingestion Script
Reuses the NexBridge ingestion pipeline to ingest data for multiple funds/sources
including ASOF (Altura Strategic Opportunities) and Stonewell.

Usage (defaults to data/validusDemo/l0):
  python scripts/multi_fund_data_ingestion.py

Optional args:
  DATA_FOLDER=C:\\path\\to\\folder python scripts/multi_fund_data_ingestion.py

File matching strategy:
- For each fund and source, this script searches for .xlsx files whose
  filename contains both the fund display name (or code) and the source name
  (case-insensitive). This avoids hardcoding file_identifier strings.

Sources are auto-created in nexbridge.source via get_or_create_source during ingestion.
"""

import os
import sys
from pathlib import Path

# Make project root importable
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from scripts.nexbridge_data_ingestion import NexBridgeDataIngestion  # noqa: E402


# Default configuration for funds and sources
# fund_id values should correspond to your reporting/validation mappings
FUNDS_CONFIG = [
    {
        'fund_code': 'NexBridge',
        'fund_display': 'NexBridge',
        'alt_display_matches': ['NexBridge Global Ventures'],
        'fund_id': 1,
        'sources': ['Bluefield']
    },
    {
        'fund_code': 'ASOF',
        'fund_display': 'Altura Strategic Opportunities',
        'alt_display_matches': ['ASOF', 'Altura Strategic Opportunities Fund'],
        'fund_id': 2,
        'sources': ['Harborview', 'ClearLedger']
    },
    {
        'fund_code': 'Stonewell',
        'fund_display': 'Stonewell Diversified',
        'alt_display_matches': ['Stonewell'],
        'fund_id': 3,
        'sources': ['StratusGA', 'VeridexAS']
    }
]


def _filename_matches_fund_and_source(filename: str, fund_conf: dict, source_name: str) -> bool:
    name_lower = filename.lower()
    # Match fund by display or alternative labels
    fund_terms = [fund_conf['fund_display']] + fund_conf.get('alt_display_matches', []) + [fund_conf['fund_code']]
    fund_match = any(term.lower() in name_lower for term in fund_terms if term)
    source_match = source_name.lower() in name_lower
    return fund_match and source_match


def run_multi_fund_ingestion(data_folder: str = None) -> None:
    data_path = data_folder or str(project_root / 'data' / 'validusDemo' / 'l0')
    if not os.path.exists(data_path):
        print(f"Data folder does not exist: {data_path}")
        return

    # Collect all xlsx files once
    all_files = [
        os.path.join(data_path, f)
        for f in os.listdir(data_path)
        if f.lower().endswith('.xlsx')
    ]

    if not all_files:
        print("No .xlsx files found in data folder")
        return

    total_success = 0
    total_failed = 0

    # Reuse the existing ingestion pipeline per fund/source
    base_ingestion = NexBridgeDataIngestion(data_folder_path=data_path)

    for fund in FUNDS_CONFIG:
        fund_id = fund['fund_id']
        for source in fund['sources']:
            # Filter files that likely belong to this fund/source
            matched_files = [
                fp for fp in all_files
                if _filename_matches_fund_and_source(os.path.basename(fp), fund, source)
            ]

            if not matched_files:
                print(f"No files matched for fund '{fund['fund_display']}' (id={fund_id}) and source '{source}'")
                continue

            matched_files.sort()
            print(f"Found {len(matched_files)} files for {fund['fund_display']} [{source}]")

            for file_path in matched_files:
                try:
                    # Set fund/source for this run and process file
                    base_ingestion.fund_id = fund_id
                    base_ingestion.source_name = source
                    ok = base_ingestion.process_file(file_path)
                    if ok:
                        total_success += 1
                    else:
                        total_failed += 1
                except Exception as e:
                    print(f"Unexpected error processing {file_path}: {e}")
                    total_failed += 1

    print("\nMulti-fund ingestion completed!")
    print(f"Successfully processed: {total_success} files")
    print(f"Failed to process: {total_failed} files")


def main():
    data_folder = os.getenv('DATA_FOLDER')
    run_multi_fund_ingestion(data_folder=data_folder)


if __name__ == '__main__':
    main()


