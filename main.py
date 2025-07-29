import os
import requests
import pandas as pd
from datetime import datetime, timedelta
from apscheduler.schedulers.blocking import BlockingScheduler
import db_handler  # Our custom module
import logging

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

        # Download with proper headers
        headers = {
            "User-Agent": "BYD-PDF-Download-Service/1.0",
            "Accept": "application/pdf",
            # TODO: ADD THE OTHER HEADERS HERE
        }

        response = requests.get(full_url, headers=headers, timeout=60, stream=True)
        response.raise_for_status()

        # Save file in chunks to handle large files
        with open(local_filepath, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:  # Filter out keep-alive chunks
                    f.write(chunk)

        # Verify file was saved and has content
        if os.path.exists(local_filepath) and os.path.getsize(local_filepath) > 0:
            logger.info(
                f"✅ Successfully saved: {local_filepath} ({os.path.getsize(local_filepath)} bytes)"
            )
            return local_filepath, None
        else:
            error_msg = "File was downloaded but appears to be empty"
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


def run_download_process():
    """
    The main workflow for the PDF download service.
    """
    logger.info(
        f"\n🚀 Starting PDF download cycle at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    )
    config = db_handler.load_config()
    connection = None

    try:
        connection = db_handler.get_db_connection()
        if not connection:
            logger.error("Failed to establish database connection")
            return

        files_to_download_df = db_handler.get_new_files_to_download(connection)

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
                    connection=connection,
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
                    connection=connection,
                    file_id=file_id,
                    claim_id=claim_id,
                    claim_no=row["CLAIM_NO"],
                    remote_name=row["FILE_NAME"],
                    local_path="N/A",
                    status="FAILED",
                    error_msg=error,
                )
                failed_downloads += 1

        logger.info(
            f"📊 Download cycle completed: {successful_downloads} successful, {failed_downloads} failed"
        )

    except Exception as e:
        logger.error(f"🚨 Critical error in main process: {e}")
    finally:
        if connection:
            connection.close()
            logger.info("Database connection closed.")


if __name__ == "__main__":
    # Validate config before starting
    try:
        config = db_handler.load_config()

        # Validate required config sections
        required_sections = ["database", "download", "scheduler", "query_params"]
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

    except Exception as e:
        logger.error(f"Configuration validation failed: {e}")
        exit(1)

    # --- Scheduler Setup ---
    scheduler = BlockingScheduler()
    job_interval_hours = config["scheduler"]["periodicity_hours"]

    # Schedule the job to run periodically
    scheduler.add_job(
        run_download_process,
        "interval",
        hours=job_interval_hours,
        max_instances=1,  # Prevent overlapping jobs
    )

    # Run the job immediately on the first start
    logger.info("Running the first download process immediately...")
    run_download_process()

    logger.info(f"🕒 Scheduler started. Will run every {job_interval_hours} hours.")
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler stopped by user.")
