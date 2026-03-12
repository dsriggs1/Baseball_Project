#!/usr/bin/env python3
"""
Batched SQL Processing Script for Baseball Data

This script processes the Retrosheet events data year-by-year with optional
parallel processing to avoid memory issues when processing the full dataset (11.8M rows).

Key optimizations:
- Filters the games table JOIN to only the current year (75x reduction)
- Parallel processing: Run multiple years simultaneously (10x speedup on 16-core CPU)

Performance:
- Sequential: ~2 minutes/year × 74 years = 2.5 hours
- Parallel (10 workers): ~2.5 hours ÷ 10 = 15 minutes

Usage:
    python run_batched_processing.py --user USERNAME --password PASSWORD [OPTIONS]

Options:
    --user, -u          MySQL/MariaDB username (required)
    --password, -p      MySQL/MariaDB password (required)
    --host              MySQL/MariaDB host (default: localhost)
    --db                Database name (default: retrosheet)
    --start-year        First year to process (default: 1952)
    --end-year          Last year to process (default: 2025)
    --resume            Resume from a specific year
    --test              Test mode: only process 2 years (2023-2024)
    --sudo              Run mariadb with sudo (for socket authentication)
"""

import subprocess
import sys
import argparse
from datetime import datetime
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

def run_year_batch(year, mysql_user, mysql_password, mysql_host, mysql_db, sql_script_path, is_first_year=False):
    """
    Execute the batched SQL script for a specific year.

    Args:
        is_first_year: If True, will drop and recreate tables. Otherwise preserves existing tables.

    Returns:
        (success: bool, output: str, error: str)
    """
    print(f"\n{'='*60}")
    print(f"Processing Year: {year}")
    if is_first_year:
        print("FIRST YEAR: Will drop and recreate tables")
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}")

    # Construct MariaDB/MySQL command
    # Set @process_year variable and @drop_tables flag, then source the SQL file
    drop_tables = 1 if is_first_year else 0
    cmd = [
        'mariadb',
        '-u', mysql_user,
        f'-p{mysql_password}',
        '-h', mysql_host,
        mysql_db,
        '-e', f"SET @process_year = {year}; SET @drop_tables = {drop_tables}; SOURCE {sql_script_path};"
    ]

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=7200  # 2 hour timeout per year (staged approach with 3 steps)
        )

        success = result.returncode == 0

        if success:
            print(f"✓ Year {year} completed successfully")
        else:
            print(f"✗ Year {year} failed with return code {result.returncode}")

        return success, result.stdout, result.stderr

    except subprocess.TimeoutExpired:
        print(f"✗ Year {year} TIMEOUT after 2 hours")
        return False, "", "Process timed out after 2 hours"
    except Exception as e:
        print(f"✗ Year {year} EXCEPTION: {str(e)}")
        return False, "", str(e)


def main():
    parser = argparse.ArgumentParser(
        description='Process Retrosheet baseball data in yearly batches',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Test mode (3 years: 2023 sequential, then 2024-2025 parallel)
  python run_batched_processing.py -u sean -p mypassword --test --parallel 2

  # Process all years with 10 parallel workers (recommended for 16-core CPU)
  python run_batched_processing.py -u sean -p mypassword --parallel 10

  # Process specific range with 8 workers
  python run_batched_processing.py -u sean -p mypassword --start-year 2020 --end-year 2025 --parallel 8

  # Resume from year 2000 with parallelization
  python run_batched_processing.py -u sean -p mypassword --resume 2000 --parallel 10
        """
    )

    parser.add_argument('-u', '--user', required=True, help='MySQL/MariaDB username')
    parser.add_argument('-p', '--password', required=True, help='MySQL/MariaDB password')
    parser.add_argument('--host', default='localhost', help='MySQL/MariaDB host (default: localhost)')
    parser.add_argument('--db', default='retrosheet', help='Database name (default: retrosheet)')
    parser.add_argument('--start-year', type=int, default=1952, help='First year to process (default: 1952)')
    parser.add_argument('--end-year', type=int, default=2025, help='Last year to process (default: 2025)')
    parser.add_argument('--resume', type=int, help='Resume processing from this year')
    parser.add_argument('--test', action='store_true', help='Test mode: process years 2023-2025 (2023 sequential, then 2024-2025 parallel)')
    parser.add_argument('--parallel', type=int, default=1, help='Number of years to process in parallel (default: 1, recommended: 8-12 for 16-core CPU)')

    args = parser.parse_args()

    # Determine year range
    if args.test:
        start_year = 2023
        end_year = 2025
        print("\n*** TEST MODE: Processing years 2023-2025 ***")
        print("*** 2023 will run first (creates tables), then 2024-2025 in parallel ***\n")
    elif args.resume:
        start_year = args.resume
        end_year = args.end_year
        print(f"\n*** RESUME MODE: Starting from year {start_year} ***\n")
    else:
        start_year = args.start_year
        end_year = args.end_year

    # Find the SQL script
    script_dir = Path(__file__).parent
    sql_script = script_dir / 'Data_Processing_DV.sql'

    if not sql_script.exists():
        print(f"ERROR: SQL script not found at {sql_script}")
        sys.exit(1)

    # Create log file
    log_file = script_dir / f'batched_processing_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'

    print("="*60)
    print("BATCHED SQL PROCESSING - CONFIGURATION")
    print("="*60)
    print(f"Database: {args.db}@{args.host}")
    print(f"User: {args.user}")
    print(f"Year range: {start_year} to {end_year}")
    print(f"Total years: {end_year - start_year + 1}")
    print(f"Parallel workers: {args.parallel}")
    print(f"SQL script: {sql_script}")
    print(f"Log file: {log_file}")
    print(f"Optimization: Filtered games table JOIN")
    print("="*60)

    input("\nPress ENTER to start processing (or Ctrl+C to cancel)...")

    # Track results
    total_years = end_year - start_year + 1
    successful_years = []
    failed_years = []
    log_lock = threading.Lock()

    overall_start = datetime.now()

    def process_and_log_year(year, is_first_year, log_file_handle):
        """Process a single year and log results (thread-safe)"""
        year_start = datetime.now()

        success, stdout, stderr = run_year_batch(
            year,
            args.user,
            args.password,
            args.host,
            args.db,
            str(sql_script),
            is_first_year=is_first_year
        )

        year_end = datetime.now()
        duration = (year_end - year_start).total_seconds()

        # Thread-safe logging
        with log_lock:
            log_file_handle.write(f"\n{'='*80}\n")
            log_file_handle.write(f"YEAR: {year}\n")
            log_file_handle.write(f"Status: {'SUCCESS' if success else 'FAILED'}\n")
            log_file_handle.write(f"Duration: {duration:.1f} seconds\n")
            log_file_handle.write(f"Started: {year_start.strftime('%Y-%m-%d %H:%M:%S')}\n")
            log_file_handle.write(f"Ended: {year_end.strftime('%Y-%m-%d %H:%M:%S')}\n")
            log_file_handle.write(f"\n--- STDOUT ---\n{stdout}\n")
            if stderr:
                log_file_handle.write(f"\n--- STDERR ---\n{stderr}\n")
            log_file_handle.write(f"{'='*80}\n")
            log_file_handle.flush()

        return year, success, stderr

    # Process years
    with open(log_file, 'w') as log:
        log.write(f"Batched Processing Log - Started {overall_start.strftime('%Y-%m-%d %H:%M:%S')}\n")
        log.write(f"Parallel workers: {args.parallel}\n")
        log.write("="*80 + "\n\n")

        # Process first year solo to create tables
        print("\n" + "="*60)
        print("STEP 1: Processing first year (creates tables)")
        print("="*60)

        year, success, stderr = process_and_log_year(start_year, is_first_year=True, log_file_handle=log)

        if success:
            successful_years.append(year)
            print(f"✓ Year {year} completed successfully")
        else:
            failed_years.append(year)
            print(f"✗ Year {year} FAILED")
            print(f"\nERROR OUTPUT:\n{stderr}\n")
            response = input(f"\nFirst year failed. Continue anyway? (y/n): ")
            if response.lower() != 'y':
                print("\nProcessing stopped by user.")
                sys.exit(1)

        # Process remaining years in parallel
        remaining_years = list(range(start_year + 1, end_year + 1))

        if remaining_years:
            print("\n" + "="*60)
            print(f"STEP 2: Processing {len(remaining_years)} remaining years in parallel")
            print(f"Using {args.parallel} workers")
            print("="*60)

            with ThreadPoolExecutor(max_workers=args.parallel) as executor:
                # Submit all years
                future_to_year = {
                    executor.submit(process_and_log_year, year, False, log): year
                    for year in remaining_years
                }

                # Process as they complete
                for future in as_completed(future_to_year):
                    year = future_to_year[future]
                    try:
                        year, success, stderr = future.result()

                        if success:
                            successful_years.append(year)
                        else:
                            failed_years.append(year)
                            print(f"\n✗ Year {year} FAILED: {stderr[:200]}")

                        # Progress update
                        processed = len(successful_years) + len(failed_years)
                        print(f"Progress: {processed}/{total_years} years | Success: {len(successful_years)} | Failed: {len(failed_years)}")

                    except Exception as exc:
                        failed_years.append(year)
                        print(f'\n✗ Year {year} generated an exception: {exc}')
                        with log_lock:
                            log.write(f"\n{'='*80}\n")
                            log.write(f"YEAR: {year}\n")
                            log.write(f"Status: EXCEPTION\n")
                            log.write(f"Exception: {exc}\n")
                            log.write(f"{'='*80}\n")
                            log.flush()

    # Final summary
    overall_end = datetime.now()
    total_duration = (overall_end - overall_start).total_seconds()

    print("\n" + "="*60)
    print("PROCESSING COMPLETE")
    print("="*60)
    print(f"Total duration: {total_duration/60:.1f} minutes ({total_duration:.0f} seconds)")
    print(f"Years processed: {len(successful_years) + len(failed_years)}/{total_years}")
    print(f"Successful: {len(successful_years)}")
    print(f"Failed: {len(failed_years)}")

    if failed_years:
        print(f"\nFailed years: {', '.join(map(str, failed_years))}")
        print(f"\nTo retry failed years, run:")
        print(f"  python {Path(__file__).name} -u {args.user} -p [PASSWORD] --start-year {min(failed_years)} --end-year {max(failed_years)}")

    print(f"\nDetailed log saved to: {log_file}")
    print("="*60)

    # Exit with appropriate code
    sys.exit(0 if len(failed_years) == 0 else 1)


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nProcessing interrupted by user (Ctrl+C)")
        sys.exit(130)
