import os
import requests
import pandas as pd
from datetime import datetime, timedelta
from apscheduler.schedulers.blocking import BlockingScheduler
import db_handler  # Our custom module
import logging
from pathlib import Path
import json

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

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("pdf_download_service.log"), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)


def download_pdf(file_id, create_date, claim_id, config):
    """
    Constructs the URL and downloads a single PDF file.

    Returns:
        tuple: (local_file_path, error_message)
               (str, None) on success
               (None, str) on failure
    """
    try:
        # Format date as YYYYMMDD
        date_str = create_date.strftime("%Y%m%d")

        # Construct the file URL part exactly as specified
        file_url_part = f"/{date_str}/{file_id}.pdf"

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

        response = requests.get(
            full_url,
            headers=headers,
            timeout=config["download"]["timeout_seconds"],
            stream=True,
            verify=config["api"]["verify_ssl"],
            allow_redirects=config["api"]["allow_redirects"],
        )
        response.raise_for_status()

        # Save file in chunks to handle large files
        with open(local_filepath, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:  # Filter out keep-alive chunks
                    f.write(chunk)

        # Verify file was saved and has content
        file_size = os.path.getsize(local_filepath)
        min_size = config["file_validation"]["min_file_size"]
        max_size = config["file_validation"]["max_file_size"]

        if os.path.exists(local_filepath) and file_size > min_size:
            if file_size <= max_size:
                logger.info(
                    f"✅ Successfully saved: {local_filepath} ({file_size} bytes)"
                )
                return local_filepath, None
            else:
                error_msg = f"File size ({file_size} bytes) exceeds maximum allowed ({max_size} bytes)"
                logger.error(f"❌ {error_msg}")
                return None, error_msg
        else:
            error_msg = f"File was downloaded but appears to be empty or too small (size: {file_size} bytes)"
            logger.error(f"❌ {error_msg}")
            return None, error_msg

    except requests.exceptions.HTTPError as e:
        error_msg = f"HTTP error {e.response.status_code}: {e}"
        logger.error(f"❌ Download failed for FILE_ID {file_id}. {error_msg}")
        return None, error_msg
    except requests.exceptions.RequestException as e:
        error_msg = f"Request error: {e}"
        logger.error(f"❌ Download failed for FILE_ID {file_id}. {error_msg}")
        return None, error_msg
    except Exception as e:
        error_msg = f"Unexpected error: {e}"
        logger.error(
            f"❌ Unexpected error during download for FILE_ID {file_id}. {error_msg}"
        )
        return None, error_msg


def process_unprocessed_pdfs(config):
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


def run_download_process():
    """
    The main workflow for the PDF download service.
    Now includes PDF processing after successful downloads.
    """
    logger.info(
        f"\n🚀 Starting PDF download cycle at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    )
    config = db_handler.load_config()

    try:
        files_to_download_df = db_handler.get_new_files_to_download()

        if files_to_download_df.empty:
            logger.info("No new files to download. Ending cycle.")
            return

        logger.info(f"Found {len(files_to_download_df)} files to download")
        successful_downloads = 0
        failed_downloads = 0

        for index, row in files_to_download_df.iterrows():
            file_id = row["FILE_ID"]
            claim_id = row["CLAIM_ID"]
            logger.info(
                f"Processing FILE_ID: {file_id} for CLAIM_ID: {claim_id} ({index + 1}/{len(files_to_download_df)})"
            )

            local_path, error = download_pdf(
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
                import time

                time.sleep(config["download"]["delay_between_downloads"])

        logger.info(
            f"📊 Download cycle completed: {successful_downloads} successful, {failed_downloads} failed"
        )

        # Log statistics if enabled
        if config.get("monitoring", {}).get("log_statistics", False):
            try:
                stats_df = db_handler.get_download_statistics()
                if not stats_df.empty:
                    logger.info("📈 Download Statistics:")
                    for _, row in stats_df.iterrows():
                        logger.info(
                            f"   {row['STATUS']}: {row['COUNT']} files ({row['PERCENTAGE']}%)"
                        )
            except Exception as e:
                logger.error(f"Error logging statistics: {e}")

        # Cleanup old failed records if enabled
        if config.get("monitoring", {}).get("cleanup_enabled", False):
            try:
                retention_days = config["monitoring"]["cleanup_retention_days"]
                deleted_count = db_handler.cleanup_old_failed_records(retention_days)
                if deleted_count > 0:
                    logger.info(f"🧹 Cleaned up {deleted_count} old failed records")
            except Exception as e:
                logger.error(f"Error during cleanup: {e}")

    except Exception as e:
        logger.error(f"🚨 Critical error in main process: {e}")


def run_batch_pdf_processing():
    """
    Standalone function to process all accumulated PDF files.
    This should be called after all download cycles are complete.
    """
    logger.info(
        f"\n🎯 Starting batch PDF processing at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    )

    try:
        config = db_handler.load_config()
        processing_results = process_unprocessed_pdfs(config)

        if processing_results:
            logger.info(
                f"🎯 Batch PDF processing completed successfully for {len(processing_results)} files"
            )
        else:
            logger.info("🎯 Batch PDF processing completed - no files processed")

    except Exception as e:
        logger.error(f"🚨 Critical error in batch PDF processing: {e}")


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
        if not os.path.isabs(storage_path):
            raise ValueError(f"Storage path must be absolute: {storage_path}")

        # Create storage directory if it doesn't exist
        os.makedirs(storage_path, exist_ok=True)
        logger.info(f"Storage directory confirmed: {storage_path}")

        # Set environment mode (you can modify this or add command line argument)
        import sys

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
    job_interval_hours = config["scheduler"]["periodicity_hours"]

    # Schedule the download job to run periodically
    scheduler.add_job(
        run_download_process,
        "interval",
        hours=job_interval_hours,
        max_instances=config["scheduler"]["max_instances"],
        id="download_job",
    )

    # Schedule PDF processing to run after downloads (offset by 30 minutes)
    # This ensures downloads have time to complete before processing starts
    pdf_processing_hours = config.get("pdf_processing", {}).get(
        "interval_hours", job_interval_hours * 2
    )
    scheduler.add_job(
        run_batch_pdf_processing,
        "interval",
        hours=pdf_processing_hours,
        max_instances=1,
        id="pdf_processing_job",
    )

    # Run the download job immediately on the first start
    logger.info("Running the first download process immediately...")
    run_download_process()

    # Run PDF processing for any existing files after a short delay
    logger.info("Running initial PDF processing after 2 minutes...")
    scheduler.add_job(
        run_batch_pdf_processing,
        "date",
        run_date=datetime.now() + timedelta(minutes=2),
        id="initial_pdf_processing",
    )

    logger.info(f"🕒 Scheduler started.")
    logger.info(f"   - Downloads will run every {job_interval_hours} hours")
    logger.info(f"   - PDF processing will run every {pdf_processing_hours} hours")
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler stopped by user.")
