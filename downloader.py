import os
import time
import logging
from datetime import datetime
import requests
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

import db_handler

logger = logging.getLogger(__name__)

# Performance tracking for downloads
download_times = []
download_times_lock = threading.Lock()


def download_pdf(file_id, create_date, claim_id, config):
    """
    Constructs the URL and downloads a single PDF file.

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
    Main download process that integrates with the individual file processing.
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
        logger.info("\n🔄 Auto-triggering audit matching after download completion...")
        try:
            # First, run enhanced PDF processing for any new claims
            processing_results = process_claims_batch_pdfs()

            if processing_results:
                logger.info(
                    f"📄 Enhanced processing completed for {len(processing_results)} claims"
                )

                # Run audit matching immediately after PDF processing
                matching_results = run_batch_audit_matching()

                if matching_results:
                    logger.info(
                        f"🔍 Enhanced audit matching completed for {len(matching_results)} claims"
                    )
                else:
                    logger.info("🔍 No claims were ready for enhanced audit matching")
            else:
                logger.info("📄 No claims were processed for enhanced PDFs")

                # Still check if there are any claims ready for audit from previous runs
                matching_results = run_batch_audit_matching()
                if matching_results:
                    logger.info(
                        f"🔍 Enhanced audit matching completed for {len(matching_results)} claims from previous processing"
                    )

        except Exception as e:
            logger.error(f"❌ Error in enhanced auto-audit process: {e}")


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
