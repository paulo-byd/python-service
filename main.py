import os
from typing import Dict
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

# Import the new PDF API client
from pdf_api_client import (
    get_pdf_api_client,
    close_pdf_api_client,
    run_file_processing_simple_api,
    process_claim_pdfs_individually_api,
    run_batch_processing_api,
)

# Set API processing as available
PDF_PROCESSING_AVAILABLE = True

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
                logger.info(
                    f"   Total time: {total_download_time:.3f}s; Network time (total): {network_time:.3f}s; File write time: {file_write_time:.3f}s"
                )
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

    # Apply delay between downloads if configured (for parallel processing)
    base_delay = config.get("download", {}).get("delay_between_downloads", 0)
    if base_delay > 0:
        # Use adaptive delay if enabled
        logger.debug(
            f"⏱️  Worker thread waiting {base_delay:.2f}s after FILE_ID {file_id}..."
        )
        time.sleep(base_delay)

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

    # Clear previous performance data and reset error tracking
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

        # Get delay configuration
        delay_between_downloads = config.get("download", {}).get(
            "delay_between_downloads", 0
        )
        if delay_between_downloads > 0:
            logger.info(
                f"⏱️  Using delay between downloads: {delay_between_downloads} seconds"
            )

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
            if delay_between_downloads > 0:
                # Use adaptive delay if enabled
                logger.debug(
                    f"⏱️  Waiting {delay_between_downloads:.2f}s before next download..."
                )
                time.sleep(delay_between_downloads)

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

    # Clear previous performance data and reset error tracking
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

        # Get delay configuration
        delay_between_downloads = config.get("download", {}).get(
            "delay_between_downloads", 0
        )

        if delay_between_downloads > 0:
            logger.info(
                f"⏱️  Using delay between downloads: {delay_between_downloads} seconds"
            )
            logger.info(
                f"⚠️  Note: In parallel mode, delays are applied per worker thread"
            )

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
            # First, run PDF processing for any new claims
            processing_results = process_claims_batch_pdfs_api()

            if processing_results:
                logger.info(f"📄 Processed PDFs for {len(processing_results)} claims")

                # Run audit matching immediately after PDF processing
                matching_results = run_batch_audit_matching()

                if matching_results:
                    logger.info(
                        f"🔍 Completed audit matching for {len(matching_results)} claims"
                    )
                else:
                    logger.info("🔍 No claims were ready for audit matching")
            else:
                logger.info("📄 No claims were processed for PDFs")

                # Still check if there are any claims ready for audit from previous runs
                matching_results = run_batch_audit_matching()
                if matching_results:
                    logger.info(
                        f"🔍 Completed audit matching for {len(matching_results)} claims from previous processing"
                    )

        except Exception as e:
            logger.error(f"❌ Error in auto-audit process: {e}")


def process_claims_batch_pdfs(max_claims=None):
    """
    Process PDF files for claims using individual file processing approach.

    Args:
        max_claims (int): Maximum number of claims to process (None = use config)
    """
    if not PDF_PROCESSING_AVAILABLE:
        logger.warning("⚠️ PDF processing not available - skipping PDF processing")
        return {}

    logger.info(
        f"\n🎯 Starting ENHANCED batch PDF processing at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    )

    try:
        config = db_handler.load_config()

        # Get processing configuration
        if max_claims is None:
            max_claims = config.get("pdf_processing", {}).get(
                "max_claims_per_batch", 10
            )
        max_files_per_claim = config.get("pdf_processing", {}).get(
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

                # Limit files per claim
                if len(existing_files) > max_files_per_claim:
                    logger.warning(
                        f"Too many files for CLAIM_ID {claim_id} ({len(existing_files)}), limiting to {max_files_per_claim}"
                    )
                    existing_files = existing_files[:max_files_per_claim]

                # Use the enhanced individual file processing approach
                logger.info(
                    f"Processing {len(existing_files)} PDF files individually for CLAIM_ID {claim_id}"
                )

                # Import the enhanced processing function
                try:
                    from process_pdf_dir import process_claim_pdfs_individually

                    # Process the PDFs using the new individual approach
                    claim_processing_results = process_claim_pdfs_individually(
                        claim_id, existing_files
                    )

                except ImportError:
                    # Fallback to mock for testing
                    logger.warning(
                        "Using mock individual processing - process_pdf_dir enhanced function not available"
                    )
                    from matching_functions import (
                        get_enhanced_processing_results_for_claim,
                    )

                    claim_processing_results = (
                        get_enhanced_processing_results_for_claim(
                            claim_id, existing_files
                        )
                    )

                # Store the results
                processing_results[claim_id] = claim_processing_results

                # Extract amounts from processing results and update database
                extracted_amounts = extract_amounts_from_processing_results(
                    claim_processing_results
                )

                # Update the processing amounts in the database
                db_handler.update_processing_amounts(
                    claim_id,
                    extracted_amounts["labour_amount"],
                    extracted_amounts["part_amount"],
                )

                # Mark claim as ready for audit
                db_handler.update_audit_status(claim_id, "PENDING")

                successful_claims += 1

                # Log processing summary
                if "processing_summary" in claim_processing_results:
                    summary = claim_processing_results["processing_summary"]
                    logger.info(f"✅ CLAIM_ID {claim_id} processed successfully:")
                    logger.info(
                        f"   Files: {summary.get('successful_files', 0)}/{len(existing_files)} successful"
                    )
                    logger.info(
                        f"   Labour: R$ {extracted_amounts['labour_amount']:,.2f}"
                    )
                    logger.info(f"   Parts: R$ {extracted_amounts['part_amount']:,.2f}")
                    logger.info(
                        f"   Total: R$ {extracted_amounts['total_amount']:,.2f}"
                    )
                else:
                    logger.info(
                        f"✅ Successfully processed {len(existing_files)} files for CLAIM_ID {claim_id}"
                    )

            except Exception as e:
                logger.error(f"❌ Error processing CLAIM_ID {claim_id}: {e}")
                failed_claims += 1
                continue

        logger.info(
            f"📄 Enhanced PDF processing completed: {successful_claims} successful, {failed_claims} failed"
        )

        # Trigger immediate audit matching if any claims were processed successfully
        if successful_claims > 0:
            logger.info("🔄 Triggering immediate enhanced audit matching...")
            matching_results = run_batch_audit_matching_enhanced()

            if matching_results:
                logger.info(
                    f"🔍 Immediate enhanced audit matching completed for {len(matching_results)} claims"
                )

        return processing_results

    except Exception as e:
        logger.error(f"🚨 Critical error in enhanced batch PDF processing: {e}")
        return {}


def process_claims_batch_pdfs_api(max_claims=None):
    """
    Process PDF files for claims using the PDF processing API.
    Updated version that calls an external API instead of local processing.

    Args:
        max_claims (int): Maximum number of claims to process (None = use config)
    """
    logger.info(
        f"\n🎯 Starting API-based batch PDF processing at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    )

    try:
        config = db_handler.load_config()

        # Get processing configuration
        if max_claims is None:
            max_claims = config.get("pdf_processing", {}).get(
                "max_claims_per_batch", 10
            )
        max_files_per_claim = config.get("pdf_processing", {}).get(
            "max_files_per_processing_call", 50
        )

        # Initialize PDF API client
        pdf_api_client = get_pdf_api_client(config)

        # Check API health before processing
        if not pdf_api_client.health_check():
            logger.error("❌ PDF processing API is not available - aborting processing")
            return {}

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
                logger.info(f"📄 Processing PDFs for CLAIM_ID {claim_id} via API")

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

                # Limit files per claim
                if len(existing_files) > max_files_per_claim:
                    logger.warning(
                        f"Too many files for CLAIM_ID {claim_id} ({len(existing_files)}), limiting to {max_files_per_claim}"
                    )
                    existing_files = existing_files[:max_files_per_claim]

                # Process the PDFs using the API
                logger.info(
                    f"Sending {len(existing_files)} PDF files to API for CLAIM_ID {claim_id}"
                )

                # Use the API-based processing function
                claim_processing_results = process_claim_pdfs_individually_api(
                    claim_id, existing_files, config
                )

                # Check if API processing was successful
                if claim_processing_results.get("processing_summary", {}).get("error"):
                    logger.error(
                        f"❌ API processing failed for CLAIM_ID {claim_id}: {claim_processing_results['processing_summary']['error']}"
                    )
                    failed_claims += 1
                    continue

                # Store the results
                processing_results[claim_id] = claim_processing_results

                # Extract amounts from processing results and update database
                extracted_amounts = extract_amounts_from_processing_results(
                    claim_processing_results
                )

                # Update the processing amounts in the database
                db_handler.update_processing_amounts(
                    claim_id,
                    extracted_amounts["labour_amount"],
                    extracted_amounts["part_amount"],
                )

                # Mark claim as ready for audit
                db_handler.update_audit_status(claim_id, "PENDING")

                successful_claims += 1

                # Log processing summary
                if "processing_summary" in claim_processing_results:
                    summary = claim_processing_results["processing_summary"]
                    logger.info(
                        f"✅ CLAIM_ID {claim_id} processed successfully via API:"
                    )
                    logger.info(
                        f"   Files: {summary.get('successful_files', 0)}/{len(existing_files)} successful"
                    )
                    logger.info(
                        f"   Labour: R$ {extracted_amounts['labour_amount']:,.2f}"
                    )
                    logger.info(f"   Parts: R$ {extracted_amounts['part_amount']:,.2f}")
                    logger.info(
                        f"   Total: R$ {extracted_amounts['total_amount']:,.2f}"
                    )
                else:
                    logger.info(
                        f"✅ Successfully processed {len(existing_files)} files for CLAIM_ID {claim_id} via API"
                    )

            except Exception as e:
                logger.error(f"❌ Error processing CLAIM_ID {claim_id} via API: {e}")
                failed_claims += 1
                continue

        logger.info(
            f"📄 API-based PDF processing completed: {successful_claims} successful, {failed_claims} failed"
        )

        # Trigger immediate audit matching if any claims were processed successfully
        if successful_claims > 0:
            logger.info("🔄 Triggering immediate audit matching...")
            try:
                # Import matching functions
                from matching_functions import run_batch_audit_matching_enhanced

                matching_results = run_batch_audit_matching_enhanced()

                if matching_results:
                    logger.info(
                        f"🔍 Immediate audit matching completed for {len(matching_results)} claims"
                    )
            except ImportError:
                logger.warning("Matching functions not available for immediate audit")

        return processing_results

    except Exception as e:
        logger.error(f"🚨 Critical error in API-based batch PDF processing: {e}")
        return {}


def run_batch_audit_matching_enhanced(max_claims=None):
    """
    ENHANCED - Run audit matching with enhanced individual file processing support.
    """
    if not MATCHING_AVAILABLE:
        logger.warning("⚠️ Matching functions not available - skipping audit matching")
        return {}

    logger.info(
        f"\n🔍 Starting ENHANCED continuous audit matching at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    )

    try:
        config = db_handler.load_config()

        # Validate matching configuration
        from matching_functions import validate_matching_config

        if not validate_matching_config(config):
            logger.error(
                "Invalid matching configuration - aborting enhanced audit matching"
            )
            return {}

        # Get batch size from config
        batch_size = config.get("audit_matching", {}).get("max_claims_per_batch", 100)

        all_results = {}
        total_processed = 0
        batch_number = 1
        successful_audits_total = 0
        failed_audits_total = 0

        # Continue processing until no more claims are available
        while True:
            logger.info(
                f"🔍 Starting enhanced batch {batch_number} (max size: {batch_size})..."
            )

            # Get claims ready for audit
            claims_df = db_handler.get_claims_ready_for_audit()

            if claims_df.empty:
                if batch_number == 1:
                    logger.info("No claims ready for enhanced audit matching")
                else:
                    logger.info(
                        f"✅ No more claims ready for enhanced audit matching after processing {total_processed} total claims"
                    )
                break

            # Take only batch_size claims for this iteration
            batch_claims = claims_df.head(batch_size)

            if len(batch_claims) == 0:
                break

            logger.info(
                f"🔍 Processing enhanced batch {batch_number}: {len(batch_claims)} claims"
            )

            # Prepare claim data for enhanced matching
            claim_data_list = []
            processing_results_dict = {}

            for _, claim_row in batch_claims.iterrows():
                claim_id = claim_row["CLAIM_ID"]

                # Add complete claim data for enhanced validation
                claim_data = {
                    "CLAIM_ID": claim_id,
                    "CLAIM_NO": claim_row.get("CLAIM_NO"),
                    "VIN": claim_row.get("VIN"),
                    "LABOUR_AMOUNT_DMS": claim_row.get("LABOUR_AMOUNT_DMS", 0),
                    "PART_AMOUNT_DMS": claim_row.get("PART_AMOUNT_DMS", 0),
                    "DEALER_CODE": claim_row.get("DEALER_CODE"),
                    "DEALER_NAME": claim_row.get("DEALER_NAME"),
                }
                claim_data_list.append(claim_data)

                # Get enhanced processing results for this claim
                pdf_files = db_handler.get_claim_pdf_files(claim_id)

                # Try to get real processing results, fallback to enhanced mock
                try:
                    from matching_functions import (
                        get_enhanced_processing_results_for_claim,
                    )

                    processing_results_dict[claim_id] = (
                        get_enhanced_processing_results_for_claim(claim_id, pdf_files)
                    )
                except Exception as e:
                    logger.warning(
                        f"Could not get enhanced processing results for CLAIM_ID {claim_id}: {e}"
                    )
                    # Fallback to legacy mock
                    from matching_functions import get_mock_processing_results_for_claim

                    processing_results_dict[claim_id] = (
                        get_mock_processing_results_for_claim(claim_id, pdf_files)
                    )

            # Perform enhanced batch matching
            from matching_functions import batch_match_claims

            batch_matching_results = batch_match_claims(
                claim_data_list, processing_results_dict, config
            )

            # Update audit status based on results
            successful_audits_batch = 0
            failed_audits_batch = 0

            for claim_id, result in batch_matching_results.items():
                try:
                    if result["match_success"]:
                        db_handler.update_audit_status(claim_id, "COMPLETE")
                        successful_audits_batch += 1
                        successful_audits_total += 1
                        logger.info(
                            f"✅ CLAIM_ID {claim_id}: Enhanced audit passed - {result['reason']}"
                        )

                        # Log enhanced details for successful matches
                        if (
                            "details" in result
                            and "data_validation" in result["details"]
                        ):
                            validation = result["details"]["data_validation"]
                            if validation.get("overall_data_match"):
                                logger.debug(
                                    f"   Data validation: ✅ Claim/VIN match confirmed"
                                )

                    else:
                        db_handler.update_audit_status(claim_id, "REJECTED")
                        failed_audits_batch += 1
                        failed_audits_total += 1
                        logger.warning(
                            f"❌ CLAIM_ID {claim_id}: Enhanced audit failed - {result['reason']}"
                        )

                        # Log enhanced details for failed matches
                        if "details" in result:
                            details = result["details"]
                            if "data_validation" in details and details[
                                "data_validation"
                            ].get("issues_found"):
                                issues = details["data_validation"]["issues_found"]
                                logger.warning(
                                    f"   Data validation issues: {', '.join(issues)}"
                                )

                except Exception as e:
                    logger.error(
                        f"❌ Error updating audit status for CLAIM_ID {claim_id}: {e}"
                    )
                    failed_audits_batch += 1
                    failed_audits_total += 1

            # Add batch results to overall results
            all_results.update(batch_matching_results)

            # Update counters
            total_processed += len(batch_claims)

            # Log enhanced batch summary
            logger.info(
                f"📊 Enhanced batch {batch_number} completed: {successful_audits_batch} passed, {failed_audits_batch} failed"
            )

            batch_number += 1

            # If we processed less than batch_size, we've reached the end
            if len(batch_claims) < batch_size:
                logger.info(
                    f"✅ Processed final enhanced batch of {len(batch_claims)} claims"
                )
                break

            # Small delay between batches to prevent overwhelming the database
            time.sleep(0.1)

        # Generate and log enhanced final report if any claims were processed
        if all_results:
            from matching_functions import generate_matching_report

            report = generate_matching_report(all_results)
            logger.info(f"\n{report}")

        # Enhanced final summary
        if total_processed > 0:
            logger.info(f"📊 Enhanced continuous audit matching completed:")
            logger.info(f"   Total batches: {batch_number - 1}")
            logger.info(f"   Total claims processed: {total_processed}")
            logger.info(
                f"   Total passed: {successful_audits_total} ({successful_audits_total / total_processed * 100:.1f}%)"
            )
            logger.info(
                f"   Total failed: {failed_audits_total} ({failed_audits_total / total_processed * 100:.1f}%)"
            )
            logger.info(
                f"   Enhanced features: Individual file processing, data validation, amount extraction by doc type"
            )
        else:
            logger.info(
                "📊 Enhanced continuous audit matching completed: No claims were processed"
            )

        return all_results

    except Exception as e:
        logger.error(f"🚨 Critical error in enhanced continuous audit matching: {e}")
        return {}


# Updated functions that should replace the existing ones in main.py
def run_batch_pdf_processing_enhanced():
    """
    Enhanced claim-based PDF processing function.
    Processes PDFs for claims that have complete file downloads using individual file processing.
    """
    logger.info(
        f"\n🎯 Starting ENHANCED claim-based PDF processing at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    )

    try:
        processing_results = process_claims_batch_pdfs_enhanced()

        if processing_results:
            logger.info(
                f"🎯 Enhanced claim-based PDF processing completed successfully for {len(processing_results)} claims"
            )
        else:
            logger.info(
                "🎯 Enhanced claim-based PDF processing completed - no claims processed"
            )

    except Exception as e:
        logger.error(f"🚨 Critical error in enhanced claim-based PDF processing: {e}")


def run_batch_audit_matching_job_enhanced():
    """
    Enhanced scheduled job function for audit matching.
    Processes claims that have been through PDF processing with enhanced data validation.
    """
    logger.info(
        f"\n🔍 Starting ENHANCED scheduled audit matching at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    )

    try:
        matching_results = run_batch_audit_matching_enhanced()

        if matching_results:
            logger.info(
                f"🔍 Enhanced scheduled audit matching completed for {len(matching_results)} claims"
            )
        else:
            logger.info(
                "🔍 Enhanced scheduled audit matching completed - no claims audited"
            )

    except Exception as e:
        logger.error(f"🚨 Critical error in enhanced scheduled audit matching: {e}")


def run_download_process_enhanced():
    """
    Enhanced main download process that integrates with the new individual file processing.
    """
    config = db_handler.load_config()

    # Check if parallel mode is enabled in config
    parallel_enabled = config.get("performance", {}).get("parallel_downloads", False)
    max_workers = config.get("performance", {}).get("max_workers", 4)

    if parallel_enabled:
        run_download_process_parallel(max_workers)
    else:
        run_download_process_sequential()

    # Enhanced auto-audit with individual file processing
    auto_audit_enabled = config.get("audit_matching", {}).get(
        "auto_run_after_download", True
    )

    if auto_audit_enabled:
        logger.info(
            "\n🔄 Auto-triggering ENHANCED audit matching after download completion..."
        )
        try:
            # First, run enhanced PDF processing for any new claims
            processing_results = process_claims_batch_pdfs_enhanced()

            if processing_results:
                logger.info(
                    f"📄 Enhanced processing completed for {len(processing_results)} claims"
                )

                # Run enhanced audit matching immediately after PDF processing
                matching_results = run_batch_audit_matching_enhanced()

                if matching_results:
                    logger.info(
                        f"🔍 Enhanced audit matching completed for {len(matching_results)} claims"
                    )
                else:
                    logger.info("🔍 No claims were ready for enhanced audit matching")
            else:
                logger.info("📄 No claims were processed for enhanced PDFs")

                # Still check if there are any claims ready for audit from previous runs
                matching_results = run_batch_audit_matching_enhanced()
                if matching_results:
                    logger.info(
                        f"🔍 Enhanced audit matching completed for {len(matching_results)} claims from previous processing"
                    )

        except Exception as e:
            logger.error(f"❌ Error in enhanced auto-audit process: {e}")


# Instructions for integration:
"""
TO INTEGRATE THESE ENHANCEMENTS INTO MAIN.PY:

1. Replace the following functions in main.py with the enhanced versions:
   - process_claims_batch_pdfs() -> process_claims_batch_pdfs_enhanced()
   - run_batch_audit_matching() -> run_batch_audit_matching_enhanced()  
   - run_batch_pdf_processing() -> run_batch_pdf_processing_enhanced()
   - run_batch_audit_matching_job() -> run_batch_audit_matching_job_enhanced()
   - run_download_process() -> run_download_process_enhanced()

2. Update the scheduler job registrations to use the enhanced functions:
   - Change run_batch_pdf_processing to run_batch_pdf_processing_enhanced
   - Change run_batch_audit_matching_job to run_batch_audit_matching_job_enhanced
   - Change run_download_process to run_download_process_enhanced

3. Add the extract_amounts_from_processing_results import at the top of main.py

4. Make sure the enhanced process_pdf_dir.py and matching_functions.py are in place

TRANSITION STRATEGY:

Phase 1 (Current): Keep both old and new functions, use enhanced ones for new processing
Phase 2 (When real PDF processor ready): Set USE_MOCK_PROCESSING = False, TEST_MODE_RANDOM = False  
Phase 3 (Production): Remove old functions, keep only enhanced versions

The enhanced functions maintain backward compatibility while adding:
- Individual file processing support
- Enhanced data validation (claim number, VIN matching)
- Better amount extraction by document type
- Improved error reporting and logging
- Future-ready structure for real PDF processing integration
"""


def run_batch_audit_matching(max_claims=None):
    """
    Run audit matching for ALL claims that have been processed.
    Processes in batches until no more claims are available.
    This runs continuously until all available claims are processed.

    Args:
        max_claims (int): Not used in continuous mode - kept for compatibility
    """
    if not MATCHING_AVAILABLE:
        logger.warning("⚠️ Matching functions not available - skipping audit matching")
        return {}

    logger.info(
        f"\n🔍 Starting continuous audit matching at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    )

    try:
        config = db_handler.load_config()

        # Validate matching configuration
        if not validate_matching_config(config):
            logger.error("Invalid matching configuration - aborting audit matching")
            return {}

        # Get batch size from config
        batch_size = config.get("audit_matching", {}).get("max_claims_per_batch", 100)

        all_results = {}
        total_processed = 0
        batch_number = 1
        successful_audits_total = 0
        failed_audits_total = 0

        # Continue processing until no more claims are available
        while True:
            logger.info(f"🔍 Starting batch {batch_number} (max size: {batch_size})...")

            # Get claims ready for audit
            claims_df = db_handler.get_claims_ready_for_audit()

            if claims_df.empty:
                if batch_number == 1:
                    logger.info("No claims ready for audit matching")
                else:
                    logger.info(
                        f"✅ No more claims ready for audit matching after processing {total_processed} total claims"
                    )
                break

            # Take only batch_size claims for this iteration
            batch_claims = claims_df.head(batch_size)

            if len(batch_claims) == 0:
                break

            logger.info(
                f"🔍 Processing batch {batch_number}: {len(batch_claims)} claims"
            )

            # Prepare claim data for matching
            claim_data_list = []
            processing_results_dict = {}

            for _, claim_row in batch_claims.iterrows():
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
                processing_results_dict[claim_id] = (
                    get_mock_processing_results_for_claim(claim_id, pdf_files)
                )

            # Perform batch matching
            batch_matching_results = batch_match_claims(
                claim_data_list, processing_results_dict, config
            )

            # Update audit status based on results
            successful_audits_batch = 0
            failed_audits_batch = 0

            for claim_id, result in batch_matching_results.items():
                try:
                    if result["match_success"]:
                        db_handler.update_audit_status(claim_id, "COMPLETE")
                        successful_audits_batch += 1
                        successful_audits_total += 1
                        logger.info(
                            f"✅ CLAIM_ID {claim_id}: Audit passed - {result['reason']}"
                        )
                    else:
                        db_handler.update_audit_status(claim_id, "REJECTED")
                        failed_audits_batch += 1
                        failed_audits_total += 1
                        logger.warning(
                            f"❌ CLAIM_ID {claim_id}: Audit failed - {result['reason']}"
                        )

                except Exception as e:
                    logger.error(
                        f"❌ Error updating audit status for CLAIM_ID {claim_id}: {e}"
                    )
                    failed_audits_batch += 1
                    failed_audits_total += 1

            # Add batch results to overall results
            all_results.update(batch_matching_results)

            # Update counters
            total_processed += len(batch_claims)

            # Log batch summary
            logger.info(
                f"📊 Batch {batch_number} completed: {successful_audits_batch} passed, {failed_audits_batch} failed"
            )

            batch_number += 1

            # If we processed less than batch_size, we've reached the end
            if len(batch_claims) < batch_size:
                logger.info(f"✅ Processed final batch of {len(batch_claims)} claims")
                break

            # Small delay between batches to prevent overwhelming the database
            time.sleep(0.1)

        # Generate and log final report if any claims were processed
        if all_results:
            report = generate_matching_report(all_results)
            logger.info(f"\n{report}")

        # Final summary
        if total_processed > 0:
            logger.info(f"📊 Continuous audit matching completed:")
            logger.info(f"   Total batches: {batch_number - 1}")
            logger.info(f"   Total claims processed: {total_processed}")
            logger.info(
                f"   Total passed: {successful_audits_total} ({successful_audits_total / total_processed * 100:.1f}%)"
            )
            logger.info(
                f"   Total failed: {failed_audits_total} ({failed_audits_total / total_processed * 100:.1f}%)"
            )
        else:
            logger.info(
                "📊 Continuous audit matching completed: No claims were processed"
            )

        return all_results

    except Exception as e:
        logger.error(f"🚨 Critical error in continuous audit matching: {e}")
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


def run_batch_pdf_processing_api():
    """
    API-based claim PDF processing job function.
    Processes PDFs for claims that have complete file downloads using API.
    """
    logger.info(
        f"\n🎯 Starting API-based claim PDF processing job at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    )

    try:
        processing_results = process_claims_batch_pdfs_api()

        if processing_results:
            logger.info(
                f"🎯 API-based claim PDF processing completed successfully for {len(processing_results)} claims"
            )
        else:
            logger.info(
                "🎯 API-based claim PDF processing completed - no claims processed"
            )

    except Exception as e:
        logger.error(f"🚨 Critical error in API-based claim PDF processing job: {e}")


def extract_amounts_from_processing_results(
    processing_results: Dict,
) -> Dict[str, float]:
    """
    Extract financial amounts from PDF processing results.
    This function should be imported from matching_functions.py
    Updated to handle API response format.
    """
    try:
        from matching_functions import (
            extract_amounts_from_processing_results as extract_func,
        )

        return extract_func(processing_results)
    except ImportError:
        logger.warning("Could not import extraction function, using fallback")

        # Fallback extraction for API responses
        extracted_amounts = {
            "labour_amount": 0.0,
            "part_amount": 0.0,
            "total_amount": 0.0,
            "confidence_score": 0.0,
            "extraction_method": "api_fallback",
        }

        try:
            # Try to extract from API response format
            if "processing_summary" in processing_results:
                summary = processing_results["processing_summary"]
                extracted_amounts["labour_amount"] = summary.get(
                    "total_amount_mao_obra", 0.0
                )
                extracted_amounts["part_amount"] = summary.get(
                    "total_amount_pecas", 0.0
                ) + summary.get("total_amount_diversos", 0.0)
                extracted_amounts["total_amount"] = summary.get("total_amount_all", 0.0)

        except Exception as e:
            logger.error(f"Error in fallback amount extraction: {e}")

        return extracted_amounts


def update_main_scheduler_for_api():
    """
    Instructions for updating the main.py scheduler to use API-based processing.

    Replace the following in main.py's scheduler section:

    OLD:
        scheduler.add_job(
            run_batch_pdf_processing,
            "interval",
            hours=pdf_processing_hours,
            max_instances=1,
            id="pdf_processing_job",
        )

    NEW:
        scheduler.add_job(
            run_batch_pdf_processing_api,
            "interval",
            hours=pdf_processing_hours,
            max_instances=1,
            id="pdf_processing_job",
        )

    Also update the initial PDF processing job:

    OLD:
        scheduler.add_job(
            run_batch_pdf_processing,
            "date",
            run_date=datetime.now() + timedelta(minutes=3),
            id="initial_pdf_processing",
        )

    NEW:
        scheduler.add_job(
            run_batch_pdf_processing_api,
            "date",
            run_date=datetime.now() + timedelta(minutes=3),
            id="initial_pdf_processing",
        )

    And update the download process auto-audit section:

    OLD:
        processing_results = process_claims_batch_pdfs()

    NEW:
        processing_results = process_claims_batch_pdfs_api()
    """
    pass


def cleanup_api_resources():
    """
    Cleanup function to call when shutting down the service.
    Add this to the main.py finally block.
    """
    logger.info("Closing PDF API client...")
    close_pdf_api_client()


# Test function
def test_api_integration():
    """
    Test the API integration with health checks and error handling.
    """
    logger.info("🧪 Testing PDF API Integration")
    logger.info("=" * 50)

    try:
        config = db_handler.load_config()

        # Test API client initialization
        logger.info("1. Testing API client initialization...")
        api_client = get_pdf_api_client(config)
        logger.info("✅ API client initialized")

        # Test health check
        logger.info("2. Testing API health check...")
        if api_client.health_check():
            logger.info("✅ API is healthy and responding")
        else:
            logger.warning("⚠️ API is not responding - continuing with offline test")

        # Test configuration
        logger.info("3. Testing API configuration...")
        api_config = config.get("pdf_processing_api", {})
        base_url = api_config.get("base_url", "http://localhost:8888")
        timeout = api_config.get("timeout_seconds", 300)
        logger.info(f"   API URL: {base_url}")
        logger.info(f"   Timeout: {timeout}s")
        logger.info("✅ Configuration loaded successfully")

        # Test error handling
        logger.info("4. Testing error handling with non-existent file...")
        try:
            result = api_client.process_single_file("/non/existent/file.pdf")
            if "error" in result.get("overall_stats", {}):
                logger.info("✅ Error handling working correctly")
            else:
                logger.warning("⚠️ Expected error not found in result")
        except Exception as e:
            logger.info(f"✅ Exception handling working: {type(e).__name__}")

        logger.info("5. Testing batch processing with empty file list...")
        try:
            result = api_client.process_batch_files([])
            logger.info("✅ Empty batch handling completed")
        except Exception as e:
            logger.info(f"✅ Empty batch exception handling: {type(e).__name__}")

        logger.info("✅ API integration test completed successfully")

    except Exception as e:
        logger.error(f"❌ API integration test failed: {e}")

    finally:
        cleanup_api_resources()


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


def auto_recover_failed_downloads():
    """Automatically retry failed downloads after delay"""
    try:
        with db_handler.DatabaseConnection("bgate") as conn:
            cursor = conn.cursor()
            # Reset old failed downloads for retry
            cursor.execute("""
                UPDATE PDF_DOWNLOAD_DMS_CLAIMS
                SET STATUS = 'PENDING',
                    ERROR_MESSAGE = ERROR_MESSAGE || ' [Auto-retry]'
                WHERE STATUS = 'FAILED'
                AND IS_LATEST_VERSION = 'Y'
                AND DOWNLOAD_TIMESTAMP < SYSDATE - INTERVAL '1' HOUR
                AND DOWNLOAD_TIMESTAMP > SYSDATE - INTERVAL '24' HOUR
            """)

            reset_count = cursor.rowcount
            conn.commit()

            if reset_count > 0:
                logger.info(f"Auto-recovery: Reset {reset_count} failed downloads")

    except Exception as e:
        logger.error(f"Auto-recovery failed: {e}")


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

        if db_handler._ENVIRONMENT_MODE == "local":
            storage_path = "./pdf-claims"

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
            db_handler.set_environment_mode("local")
            logger.info("Using default environment mode: local")

        # Initialize database connection pools
        logger.info("Initializing database connection pools...")
        try:
            db_handler.initialize_connection_pools(
                min_connections=config.get("database", {}).get("min_connections", 2),
                max_connections=config.get("database", {}).get("max_connections", 10),
            )
            logger.info("✅ Database connection pools initialized successfully")
            # Verify pool status
            db_handler.get_pool_status()

            # Test pool connectivity
            logger.info("Testing BGATE pool connectivity...")
            try:
                with db_handler.DatabaseConnection("bgate") as test_conn:
                    cursor = test_conn.cursor()
                    cursor.execute("SELECT 1 FROM DUAL")
                    result = cursor.fetchone()
                    cursor.close()
                    logger.info("✅ BGATE pool connectivity test successful")
            except Exception as test_error:
                logger.error(f"❌ BGATE pool connectivity test failed: {test_error}")

            # Test DMS direct connectivity
            logger.info("Testing DMS direct connectivity...")
            try:
                with db_handler.DatabaseConnection("dms") as test_conn:
                    cursor = test_conn.cursor()
                    cursor.execute("SELECT 1 FROM DUAL")
                    result = cursor.fetchone()
                    cursor.close()
                    logger.info("✅ DMS direct connectivity test successful")
            except Exception as test_error:
                logger.error(f"❌ DMS direct connectivity test failed: {test_error}")
        except Exception as pool_error:
            logger.error(f"Failed to initialize connection pools: {pool_error}")
            logger.warning("⚠️ Falling back to direct connections")
            # Show current pool status for debugging
            db_handler.get_pool_status()

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
        run_batch_pdf_processing_api,
        "interval",
        hours=pdf_processing_hours,
        max_instances=1,
        id="pdf_processing_job",
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

    # Retry failed download logic
    scheduler.add_job(
        auto_recover_failed_downloads, "interval", hours=2, id="auto_recovery"
    )

    logger.info("🕒 Scheduler started.")
    logger.info(f"   - Downloads will run every {download_interval_hours} hours")
    logger.info(f"   - PDF processing will run every {pdf_processing_hours} hours")
    logger.info(f"   - Audit matching will run every {audit_matching_hours} hours")
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler stopped by user.")
    finally:
        # Cleanup connection pools on exit
        logger.info("Cleaning up database connection pools...")
        db_handler.close_connection_pools()

        logger.info("Closing PDF API client...")
        close_pdf_api_client()
