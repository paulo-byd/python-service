import os
import requests
import pandas as pd
from datetime import datetime, timedelta
from apscheduler.schedulers.blocking import BlockingScheduler
import db_handler  # Our custom module
import logging
from pathlib import Path
import json
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

# Import the PDF processing module
try:
    from process_pdf_dir import run_batch_processing

    PDF_PROCESSING_AVAILABLE = True
    logger = logging.getLogger(__name__)
    logger.info("✅ PDF processing module imported successfully")
except ImportError as e:
    PDF_PROCESSING_AVAILABLE = False
    logger = logging.getLogger(__name__)
    logger.warning(f"⚠️ PDF processing module not available: {e}")

# Import the mock matching module
try:
    from matching_functions import (
        match_invoices_with_dms_estimates,
        batch_match_claims,
        validate_matching_config,
        generate_matching_report,
        get_mock_processing_results_for_claim,
    )

    MATCHING_AVAILABLE = True
    logger.info("✅ Matching functions imported successfully")
except ImportError as e:
    MATCHING_AVAILABLE = False
    logger.warning(f"⚠️ Matching functions not available: {e}")

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("pdf_download_service.log"), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

# Performance tracking
download_times = []
download_times_lock = threading.Lock()


def download_pdf(file_id, create_date, claim_id, config):
    """
    Constructs the URL and downloads a single PDF file.
    Now includes detailed performance logging.

    Returns:
        tuple: (local_file_path, error_message, download_time_seconds)
               (str, None, float) on success
               (None, str, float) on failure
    """
    start_time = time.time()

    try:
        # Format date as YYYYMMDD
        date_str = create_date.strftime("%Y%m%d")

        # Construct the file URL part exactly as specified
        file_url_part = f"/{date_str}/{file_id}"

        # Full download URL - base_url already includes the path
        full_url = f"{config['download']['base_url']}{file_url_part}"

        # Define local path structure
        base_storage_path = config["download"]["storage_path"]

        # Create the date-specific folder
        date_specific_folder = os.path.join(base_storage_path, date_str)
        os.makedirs(date_specific_folder, exist_ok=True)

        # Create filename and store in date-specific folder
        local_filename = f"CLAIM_{claim_id}_{file_id}.pdf"
        local_filepath = os.path.join(date_specific_folder, local_filename)

        logger.info(f"Downloading from: {full_url}")
        logger.info(f"Saving to: {local_filepath}")

        # Download with proper headers (as per API documentation)
        headers = {
            "User-Agent": config["api"]["headers"]["User-Agent"],
            "Accept": config["api"]["headers"]["Accept"],
            "APP_ID": config["api"]["headers"]["APP_ID"],
            "SECRET_KEY": config["api"]["headers"]["SECRET_KEY"],
            "Content-Type": "application/json",
        }

        # PERFORMANCE: Start measuring network time
        network_start_time = time.time()

        response = requests.get(
            full_url,
            headers=headers,
            timeout=config["download"]["timeout_seconds"],
            stream=True,
            verify=config["api"]["verify_ssl"],
            allow_redirects=config["api"]["allow_redirects"],
        )
        response.raise_for_status()

        # PERFORMANCE: Measure time to get response headers
        headers_received_time = time.time()
        headers_time = headers_received_time - network_start_time

        # Save file in chunks to handle large files
        with open(local_filepath, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:  # Filter out keep-alive chunks
                    f.write(chunk)

        # PERFORMANCE: Total download time
        download_complete_time = time.time()
        total_download_time = download_complete_time - start_time
        network_time = download_complete_time - network_start_time
        file_write_time = download_complete_time - headers_received_time

        # Verify file was saved and has content
        file_size = os.path.getsize(local_filepath)
        min_size = config["file_validation"]["min_file_size"]
        max_size = config["file_validation"]["max_file_size"]

        if os.path.exists(local_filepath) and file_size > min_size:
            if file_size <= max_size:
                # PERFORMANCE: Log detailed timing
                logger.info(
                    f"✅ Successfully saved: {local_filepath} ({file_size} bytes)"
                )
                logger.info(f"📊 PERFORMANCE - FILE_ID {file_id}:")
                logger.info(f"   Total time: {total_download_time:.3f}s")
                logger.info(f"   Network time (headers): {headers_time:.3f}s")
                logger.info(f"   Network time (total): {network_time:.3f}s")
                logger.info(f"   File write time: {file_write_time:.3f}s")
                logger.info(
                    f"   Download speed: {file_size / network_time / 1024:.2f} KB/s"
                )

                # Store performance data for analysis
                with download_times_lock:
                    download_times.append(
                        {
                            "file_id": file_id,
                            "file_size": file_size,
                            "total_time": total_download_time,
                            "network_time": network_time,
                            "headers_time": headers_time,
                            "write_time": file_write_time,
                            "speed_kbps": file_size / network_time / 1024
                            if network_time > 0
                            else 0,
                        }
                    )

                return local_filepath, None, total_download_time
            else:
                error_msg = f"File size ({file_size} bytes) exceeds maximum allowed ({max_size} bytes)"
                logger.error(f"❌ {error_msg}")
                return None, error_msg, time.time() - start_time
        else:
            error_msg = f"File was downloaded but appears to be empty or too small (size: {file_size} bytes)"
            logger.error(f"❌ {error_msg}")
            return None, error_msg, time.time() - start_time

    except requests.exceptions.HTTPError as e:
        error_msg = f"HTTP error {e.response.status_code}: {e}"
        total_time = time.time() - start_time
        logger.error(
            f"❌ Download failed for FILE_ID {file_id}. {error_msg} (Time: {total_time:.3f}s)"
        )
        return None, error_msg, total_time
    except requests.exceptions.RequestException as e:
        error_msg = f"Request error: {e}"
        total_time = time.time() - start_time
        logger.error(
            f"❌ Download failed for FILE_ID {file_id}. {error_msg} (Time: {total_time:.3f}s)"
        )
        return None, error_msg, total_time
    except Exception as e:
        error_msg = f"Unexpected error: {e}"
        total_time = time.time() - start_time
        logger.error(
            f"❌ Unexpected error during download for FILE_ID {file_id}. {error_msg} (Time: {total_time:.3f}s)"
        )
        return None, error_msg, total_time


def download_single_file(row, config):
    """
    Helper function to download a single file - used for parallel processing.
    Returns tuple: (index, file_id, claim_id, local_path, error, download_time, row_data)
    """
    file_id = row["FILE_ID"]
    claim_id = row["CLAIM_ID"]

    local_path, error, download_time = download_pdf(
        file_id, row["CREATE_DATE"], claim_id, config
    )

    return (row.name, file_id, claim_id, local_path, error, download_time, row)


def log_performance_summary():
    """Log a summary of download performance"""
    with download_times_lock:
        if not download_times:
            return

        total_files = len(download_times)
        total_size = sum(d["file_size"] for d in download_times)
        total_time = sum(d["total_time"] for d in download_times)
        total_network_time = sum(d["network_time"] for d in download_times)
        avg_speed = sum(d["speed_kbps"] for d in download_times) / total_files

        logger.info("📊 PERFORMANCE SUMMARY:")
        logger.info(f"   Files processed: {total_files}")
        logger.info(f"   Total data: {total_size / 1024 / 1024:.2f} MB")
        logger.info(f"   Total time: {total_time:.3f}s")
        logger.info(f"   Total network time: {total_network_time:.3f}s")
        logger.info(
            f"   Network time %: {(total_network_time / total_time) * 100:.1f}%"
        )
        logger.info(f"   Average speed: {avg_speed:.2f} KB/s")
        logger.info(f"   Time per file: {total_time / total_files:.3f}s")


def run_download_process_sequential():
    """
    Original sequential download process with performance logging.
    """
    logger.info(
        f"\n🚀 Starting SEQUENTIAL PDF download cycle at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    )
    config = db_handler.load_config()

    # Clear previous performance data
    with download_times_lock:
        download_times.clear()

    try:
        files_to_download_df = db_handler.get_new_files_to_download()

        if files_to_download_df.empty:
            logger.info("No new files to download. Ending cycle.")
            return

        logger.info(f"Found {len(files_to_download_df)} files to download")
        successful_downloads = 0
        failed_downloads = 0

        cycle_start_time = time.time()

        for index, row in files_to_download_df.iterrows():
            file_id = row["FILE_ID"]
            claim_id = row["CLAIM_ID"]
            logger.info(f"Processing file n°: {index} / {len(files_to_download_df)}")
            logger.info(
                f"Processing FILE_ID: {file_id} for CLAIM_ID: {claim_id} ({index + 1}/{len(files_to_download_df)})"
            )

            local_path, error, download_time = download_pdf(
                file_id, row["CREATE_DATE"], claim_id, config
            )

            if local_path and error is None:
                # Success
                db_handler.log_download_status(
                    file_id=file_id,
                    claim_id=claim_id,
                    claim_no=row["CLAIM_NO"],
                    remote_name=row["FILE_NAME"],
                    local_path=local_path,
                    status="SUCCESS",
                )
                successful_downloads += 1
            else:
                # Failed
                db_handler.log_download_status(
                    file_id=file_id,
                    claim_id=claim_id,
                    claim_no=row["CLAIM_NO"],
                    remote_name=row["FILE_NAME"],
                    local_path="N/A",
                    status="FAILED",
                    error_msg=error,
                )
                failed_downloads += 1

            # Add delay between downloads if configured
            if config.get("download", {}).get("delay_between_downloads", 0) > 0:
                time.sleep(config["download"]["delay_between_downloads"])

        cycle_end_time = time.time()
        total_cycle_time = cycle_end_time - cycle_start_time

        logger.info(
            f"📊 SEQUENTIAL Download cycle completed: {successful_downloads} successful, {failed_downloads} failed"
        )
        logger.info(f"📊 Total cycle time: {total_cycle_time:.3f}s")

        # Log performance summary
        log_performance_summary()

    except Exception as e:
        logger.error(f"🚨 Critical error in sequential process: {e}")


def run_download_process_parallel(max_workers=4):
    """
    Parallel download process with performance logging.
    """
    logger.info(
        f"\n🚀 Starting PARALLEL PDF download cycle (workers: {max_workers}) at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    )
    config = db_handler.load_config()

    # Clear previous performance data
    with download_times_lock:
        download_times.clear()

    try:
        files_to_download_df = db_handler.get_new_files_to_download()

        if files_to_download_df.empty:
            logger.info("No new files to download. Ending cycle.")
            return

        logger.info(f"Found {len(files_to_download_df)} files to download")
        successful_downloads = 0
        failed_downloads = 0

        cycle_start_time = time.time()

        # Use ThreadPoolExecutor for parallel downloads
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all download tasks
            future_to_file = {
                executor.submit(download_single_file, row, config): row
                for _, row in files_to_download_df.iterrows()
            }

            # Process completed downloads
            for future in as_completed(future_to_file):
                try:
                    index, file_id, claim_id, local_path, error, download_time, row = (
                        future.result()
                    )

                    if local_path and error is None:
                        # Success
                        db_handler.log_download_status(
                            file_id=file_id,
                            claim_id=claim_id,
                            claim_no=row["CLAIM_NO"],
                            remote_name=row["FILE_NAME"],
                            local_path=local_path,
                            status="SUCCESS",
                        )
                        successful_downloads += 1
                        logger.info(
                            f"✅ Completed FILE_ID: {file_id} ({successful_downloads + failed_downloads}/{len(files_to_download_df)})"
                        )
                    else:
                        # Failed
                        db_handler.log_download_status(
                            file_id=file_id,
                            claim_id=claim_id,
                            claim_no=row["CLAIM_NO"],
                            remote_name=row["FILE_NAME"],
                            local_path="N/A",
                            status="FAILED",
                            error_msg=error,
                        )
                        failed_downloads += 1
                        logger.error(
                            f"❌ Failed FILE_ID: {file_id} ({successful_downloads + failed_downloads}/{len(files_to_download_df)})"
                        )

                except Exception as e:
                    failed_downloads += 1
                    logger.error(f"❌ Exception processing future: {e}")

        cycle_end_time = time.time()
        total_cycle_time = cycle_end_time - cycle_start_time

        logger.info(
            f"📊 PARALLEL Download cycle completed: {successful_downloads} successful, {failed_downloads} failed"
        )
        logger.info(f"📊 Total cycle time: {total_cycle_time:.3f}s")

        # Log performance summary
        log_performance_summary()

    except Exception as e:
        logger.error(f"🚨 Critical error in parallel process: {e}")


def run_download_process():
    """
    Main download process - can switch between sequential and parallel modes.
    Now includes automatic audit matching at the end.
    """
    config = db_handler.load_config()

    # Check if parallel mode is enabled in config
    parallel_enabled = config.get("performance", {}).get("parallel_downloads", False)
    max_workers = config.get("performance", {}).get("max_workers", 4)

    if parallel_enabled:
        run_download_process_parallel(max_workers)
    else:
        run_download_process_sequential()

    auto_audit_enabled = config.get("audit_matching", {}).get(
        "auto_run_after_download", True
    )

    if auto_audit_enabled:
        logger.info("\n🔄 Auto-triggering audit matching after download completion...")
        try:
            # First, run PDF processing for any newly completed claims
            processing_results = process_claims_batch_pdfs()

            if processing_results:
                logger.info(f"📄 Processed PDFs for {len(processing_results)} claims")

            # Then, run audit matching for any claims ready for audit
            matching_results = run_batch_audit_matching()

            if matching_results:
                logger.info(
                    f"🔍 Completed audit matching for {len(matching_results)} claims"
                )
            else:
                logger.info("🔍 No claims were ready for audit matching")

        except Exception as e:
            logger.error(f"❌ Error in auto-audit process: {e}")
            # Don't fail the entire download process if audit fails
    else:
        logger.info("Auto-audit disabled in configuration")


def process_claims_batch_pdfs(max_claims=None):
    """
    Process PDF files for claims that are ready for processing.

    Args:
        max_claims (int): Maximum number of claims to process (None = use config)
    """
    if not PDF_PROCESSING_AVAILABLE:
        logger.warning("⚠️ PDF processing not available - skipping PDF processing")
        return {}

    logger.info(
        f"\n🎯 Starting batch PDF processing at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    )

    try:
        config = db_handler.load_config()

        # Get processing configuration
        if max_claims is None:
            max_claims = config.get("pdf_processing", {}).get(
                "max_claims_per_batch", 10
            )
        max_files_per_call = config.get("pdf_processing", {}).get(
            "max_files_per_processing_call", 50
        )

        # Get claims ready for processing
        claims_df = db_handler.get_claims_ready_for_processing()

        if claims_df.empty:
            logger.info("No claims ready for PDF processing")
            return {}

        # Limit to configured batch size
        claims_to_process = claims_df.head(max_claims)
        logger.info(f"Processing {len(claims_to_process)} claims (max: {max_claims})")

        processing_results = {}
        successful_claims = 0
        failed_claims = 0

        for _, claim_row in claims_to_process.iterrows():
            claim_id = claim_row["CLAIM_ID"]

            try:
                logger.info(f"📄 Processing PDFs for CLAIM_ID {claim_id}")

                # Get PDF files for this claim
                pdf_files = db_handler.get_claim_pdf_files(claim_id)

                if not pdf_files:
                    logger.warning(f"No PDF files found for CLAIM_ID {claim_id}")
                    continue

                # Filter to existing files
                existing_files = [f for f in pdf_files if os.path.exists(f)]
                if len(existing_files) != len(pdf_files):
                    logger.warning(
                        f"Some PDF files missing for CLAIM_ID {claim_id}: {len(existing_files)}/{len(pdf_files)} found"
                    )

                if not existing_files:
                    logger.error(f"No existing PDF files for CLAIM_ID {claim_id}")
                    failed_claims += 1
                    continue

                # Limit files per processing call
                if len(existing_files) > max_files_per_call:
                    logger.warning(
                        f"Too many files for CLAIM_ID {claim_id} ({len(existing_files)}), limiting to {max_files_per_call}"
                    )
                    existing_files = existing_files[:max_files_per_call]

                # Convert to Path objects
                pdf_file_paths = [Path(file_path) for file_path in existing_files]

                # Process the PDFs for this claim
                logger.info(
                    f"Processing {len(pdf_file_paths)} PDF files for CLAIM_ID {claim_id}"
                )

                # Call the processing function with specific files
                storage_path = Path(config["download"]["storage_path"])
                processing_results_json = run_batch_processing(
                    input_pdf_dir_path=storage_path,
                    pdf_file_paths=pdf_file_paths,  # Process specific files
                )

                # Parse the results
                claim_processing_results = json.loads(processing_results_json)
                processing_results[claim_id] = claim_processing_results

                # Mark claim as ready for audit
                db_handler.update_audit_status(claim_id, "PENDING")

                successful_claims += 1
                logger.info(
                    f"✅ Successfully processed {len(claim_processing_results)} files for CLAIM_ID {claim_id}"
                )

            except Exception as e:
                logger.error(f"❌ Error processing CLAIM_ID {claim_id}: {e}")
                failed_claims += 1
                continue

        logger.info(
            f"📊 PDF processing completed: {successful_claims} successful, {failed_claims} failed"
        )
        return processing_results

    except Exception as e:
        logger.error(f"🚨 Critical error in batch PDF processing: {e}")
        return {}


def run_batch_audit_matching(max_claims=None):
    """
    Run audit matching for claims that have been processed.
    This is the Phase 5 implementation.

    Args:
        max_claims (int): Maximum number of claims to audit (None = use config)
    """
    if not MATCHING_AVAILABLE:
        logger.warning("⚠️ Matching functions not available - skipping audit matching")
        return {}

    logger.info(
        f"\n🔍 Starting batch audit matching at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    )

    try:
        config = db_handler.load_config()

        # Validate matching configuration
        if not validate_matching_config(config):
            logger.error("Invalid matching configuration - aborting audit matching")
            return {}

        # Get audit configuration
        if max_claims is None:
            max_claims = config.get("audit_matching", {}).get(
                "max_claims_per_batch", 20
            )

        # Get claims ready for audit
        claims_df = db_handler.get_claims_ready_for_audit()

        if claims_df.empty:
            logger.info("No claims ready for audit matching")
            return {}

        # Limit to configured batch size
        claims_to_audit = claims_df.head(max_claims)
        logger.info(f"Auditing {len(claims_to_audit)} claims (max: {max_claims})")

        # Prepare claim data for matching
        claim_data_list = []
        processing_results_dict = {}

        for _, claim_row in claims_to_audit.iterrows():
            claim_id = claim_row["CLAIM_ID"]

            # Add claim data
            claim_data_list.append(
                {
                    "CLAIM_ID": claim_id,
                    "LABOUR_AMOUNT_DMS": claim_row.get("LABOUR_AMOUNT_DMS", 0),
                    "PART_AMOUNT_DMS": claim_row.get("PART_AMOUNT_DMS", 0),
                }
            )

            # Get mock processing results for this claim
            # In reality, this would come from stored processing results
            pdf_files = db_handler.get_claim_pdf_files(claim_id)
            processing_results_dict[claim_id] = get_mock_processing_results_for_claim(
                claim_id, pdf_files
            )

        # Perform batch matching
        matching_results = batch_match_claims(
            claim_data_list, processing_results_dict, config
        )

        # Update audit status based on results
        successful_audits = 0
        failed_audits = 0

        for claim_id, result in matching_results.items():
            try:
                if result["match_success"]:
                    db_handler.update_audit_status(claim_id, "COMPLETE")
                    successful_audits += 1
                    logger.info(
                        f"✅ CLAIM_ID {claim_id}: Audit passed - {result['reason']}"
                    )
                else:
                    db_handler.update_audit_status(claim_id, "REJECTED")
                    failed_audits += 1
                    logger.warning(
                        f"❌ CLAIM_ID {claim_id}: Audit failed - {result['reason']}"
                    )

            except Exception as e:
                logger.error(
                    f"❌ Error updating audit status for CLAIM_ID {claim_id}: {e}"
                )
                failed_audits += 1

        # Generate and log report
        report = generate_matching_report(matching_results)
        logger.info(f"\n{report}")

        logger.info(
            f"📊 Audit matching completed: {successful_audits} passed, {failed_audits} failed"
        )
        return matching_results

    except Exception as e:
        logger.error(f"🚨 Critical error in batch audit matching: {e}")
        return {}
    """
    Process all successfully downloaded PDF files that haven't been processed yet.
    This runs after all download cycles to batch process accumulated files.

    Args:
        config (dict): Configuration dictionary

    Returns:
        dict: Processing results from the batch processing script
    """
    if not PDF_PROCESSING_AVAILABLE:
        logger.warning("⚠️ PDF processing not available - skipping processing step")
        return {}

    try:
        # Get all successfully downloaded files that need processing
        connection = db_handler.get_bgate_db_connection()

        # Query for files that are downloaded but not yet processed
        # TODO: Add a PROCESSED flag to the database table to track processing status
        # For now, we'll process all successful downloads from recent time period
        query = """
            SELECT LOCAL_FILE_PATH, FILE_ID, CLAIM_ID, CLAIM_NO
            FROM PDF_DOWNLOAD_DMS_CLAIMS 
            WHERE STATUS = 'SUCCESS' 
            AND LOCAL_FILE_PATH IS NOT NULL
            AND DOWNLOAD_TIMESTAMP >= SYSDATE - 1  -- Last 24 hours
            ORDER BY DOWNLOAD_TIMESTAMP
        """

        df = pd.read_sql(query, connection)
        connection.close()

        if df.empty:
            logger.info("No files found for processing")
            return {}

        # Filter to only files that actually exist on disk
        existing_files = []
        for _, row in df.iterrows():
            file_path = row["LOCAL_FILE_PATH"]
            if os.path.exists(file_path):
                existing_files.append(file_path)
            else:
                logger.warning(f"File not found on disk: {file_path}")

        if not existing_files:
            logger.warning("No existing files found for processing")
            return {}

        logger.info(
            f"🔍 Starting batch PDF processing for {len(existing_files)} files..."
        )

        # Convert file paths to Path objects
        pdf_file_paths = [Path(file_path) for file_path in existing_files]

        # Run the batch processing on the entire storage directory
        # This is more efficient than processing individual files
        storage_path = Path(config["download"]["storage_path"])
        processing_results_json = run_batch_processing(
            input_pdf_dir_path=storage_path,
            pdf_file_paths=[],  # Empty list means process entire directory
        )

        # Parse the JSON results
        processing_results = json.loads(processing_results_json)

        logger.info(
            f"✅ Batch PDF processing completed successfully for {len(processing_results)} files"
        )

        # Log some sample results for monitoring
        if processing_results:
            logger.info("📄 Sample processing results:")
            for i, (key, value) in enumerate(list(processing_results.items())[:3]):
                logger.info(f"   {key}: {value}")
            if len(processing_results) > 3:
                logger.info(f"   ... and {len(processing_results) - 3} more files")

        # TODO: Future enhancement - Store processing results in database
        # This is where we would add code to store the processing results
        # in a database table for future reference and analysis
        # Also add a PROCESSED flag to PDF_DOWNLOAD_DMS_CLAIMS table
        # Example:
        # db_handler.store_processing_results(processing_results)
        # db_handler.mark_files_as_processed(file_ids)

        return processing_results

    except Exception as e:
        logger.error(f"❌ Error during batch PDF processing: {e}")
        return {}


def run_batch_pdf_processing():
    """
    New claim-based PDF processing function.
    Processes PDFs for claims that have complete file downloads.
    """
    logger.info(
        f"\n🎯 Starting claim-based PDF processing at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    )

    try:
        processing_results = process_claims_batch_pdfs()

        if processing_results:
            logger.info(
                f"🎯 Claim-based PDF processing completed successfully for {len(processing_results)} claims"
            )
        else:
            logger.info("🎯 Claim-based PDF processing completed - no claims processed")

    except Exception as e:
        logger.error(f"🚨 Critical error in claim-based PDF processing: {e}")


def run_batch_audit_matching_job():
    """
    Scheduled job function for audit matching.
    Processes claims that have been through PDF processing.
    """
    logger.info(
        f"\n🔍 Starting scheduled audit matching at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    )

    try:
        matching_results = run_batch_audit_matching()

        if matching_results:
            logger.info(
                f"🔍 Scheduled audit matching completed for {len(matching_results)} claims"
            )
        else:
            logger.info("🔍 Scheduled audit matching completed - no claims audited")

    except Exception as e:
        logger.error(f"🚨 Critical error in scheduled audit matching: {e}")


def run_legacy_batch_pdf_processing():
    """
    Legacy PDF processing function - kept for backward compatibility.
    Use run_batch_pdf_processing() for new claim-based approach.
    """
    logger.info(
        f"\n🎯 Starting legacy PDF processing at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    )

    try:
        config = db_handler.load_config()
        processing_results = process_unprocessed_pdfs(config)

        if processing_results:
            logger.info(
                f"🎯 Legacy PDF processing completed successfully for {len(processing_results)} files"
            )
        else:
            logger.info("🎯 Legacy PDF processing completed - no files processed")

    except Exception as e:
        logger.error(f"🚨 Critical error in legacy PDF processing: {e}")


if __name__ == "__main__":
    # Validate config before starting
    try:
        config = db_handler.load_config()

        # Validate required config sections
        required_sections = ["download", "scheduler", "logging", "monitoring"]
        for section in required_sections:
            if section not in config:
                raise ValueError(f"Missing required config section: {section}")

        # Validate storage path
        storage_path = config["download"]["storage_path"]

        if db_handler._ENVIRONMENT_MODE != "local":
            if not os.path.isabs(storage_path):
                raise ValueError(f"Storage path must be absolute: {storage_path}")

        # Create storage directory if it doesn't exist
        os.makedirs(storage_path, exist_ok=True)
        logger.info(f"Storage directory confirmed: {storage_path}")

        # Set environment mode (you can modify this or add command line argument)

        if len(sys.argv) > 1:
            env_mode = sys.argv[1]
            if env_mode in ["local", "uat", "prod"]:
                db_handler.set_environment_mode(env_mode)
                logger.info(f"Environment mode set to: {env_mode}")
            else:
                logger.warning(
                    f"Invalid environment mode: {env_mode}. Using default: local"
                )
        else:
            logger.info("Using default environment mode: local")

    except Exception as e:
        logger.error(f"Configuration validation failed: {e}")
        exit(1)
    # --- Scheduler Setup ---
    scheduler = BlockingScheduler()

    # Get scheduling intervals from config
    download_interval_hours = config["scheduler"]["periodicity_hours"]
    pdf_processing_hours = config.get("pdf_processing", {}).get("interval_hours", 4)
    audit_matching_hours = config.get("audit_matching", {}).get("interval_hours", 4)

    # Schedule the download job to run periodically
    scheduler.add_job(
        run_download_process,
        "interval",
        hours=download_interval_hours,
        max_instances=config["scheduler"]["max_instances"],
        id="download_job",
    )

    # Schedule claim-based PDF processing
    scheduler.add_job(
        run_batch_pdf_processing,
        "interval",
        hours=pdf_processing_hours,
        max_instances=1,
        id="pdf_processing_job",
    )

    # Schedule audit matching
    scheduler.add_job(
        run_batch_audit_matching_job,
        "interval",
        hours=audit_matching_hours,
        max_instances=1,
        id="audit_matching_job",
    )

    # Run the download job immediately on the first start
    logger.info("Running the first download process immediately...")
    run_download_process()

    # Run PDF processing for any existing claims after a short delay
    logger.info("Running initial PDF processing after 3 minutes...")
    scheduler.add_job(
        run_batch_pdf_processing,
        "date",
        run_date=datetime.now() + timedelta(minutes=3),
        id="initial_pdf_processing",
    )

    # Run audit matching for any existing claims after a longer delay
    logger.info("Running initial audit matching after 5 minutes...")
    scheduler.add_job(
        run_batch_audit_matching_job,
        "date",
        run_date=datetime.now() + timedelta(minutes=5),
        id="initial_audit_matching",
    )

    logger.info(f"🕒 Scheduler started.")
    logger.info(f"   - Downloads will run every {download_interval_hours} hours")
    logger.info(f"   - PDF processing will run every {pdf_processing_hours} hours")
    logger.info(f"   - Audit matching will run every {audit_matching_hours} hours")
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler stopped by user.")
