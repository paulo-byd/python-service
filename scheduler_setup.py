import logging
from apscheduler.schedulers.blocking import BlockingScheduler

# Import the job functions from other modules
from downloader import run_download_process, auto_recover_failed_downloads
from pdf_processor import run_batch_pdf_processing_api
from audit_matcher import run_batch_audit_matching_job

logger = logging.getLogger(__name__)


def start_scheduler(config):
    """Initializes and starts the job scheduler."""
    scheduler = BlockingScheduler()

    # Get intervals from config
    download_interval_hours = config["scheduler"]["periodicity_hours"]
    pdf_processing_hours = config.get("pdf_processing", {}).get("interval_hours", 0.5)
    audit_matching_hours = config.get("audit_matching", {}).get("interval_hours", 0.6)

    # Run the download job immediately on first start
    logger.info("Running the first download process immediately...")
    run_download_process()

    # Schedule recurring jobs
    scheduler.add_job(
        run_download_process(),
        "interval",
        hours=download_interval_hours,
        id="download_job",
    )
    scheduler.add_job(
        run_batch_pdf_processing_api,
        "interval",
        hours=pdf_processing_hours,
        id="pdf_processing_job",
    )
    scheduler.add_job(
        run_batch_audit_matching_job,
        "interval",
        hours=audit_matching_hours,
        id="audit_matching_job",
    )
    scheduler.add_job(
        auto_recover_failed_downloads, "interval", hours=2, id="auto_recovery"
    )

    logger.info("🕒 Scheduler started.")
    logger.info(f"   - Downloads will run every {download_interval_hours} hours")
    logger.info(f"   - PDF processing will run every {pdf_processing_hours} hours")
    logger.info(f"   - Audit matching will run every {audit_matching_hours} hours")

    scheduler.start()
