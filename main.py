import logging
from config_loader import load_and_validate_config, setup_environment
import db_handler
from pdf_api_client import close_pdf_api_client

# Import setup and orchestration functions
from scheduler_setup import start_scheduler

# Setup logging right at the start
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("pdf_download_service.log"), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

if __name__ == "__main__":
    # 1. Set environment (local, uat, prod)
    setup_environment()

    # 2. Load and validate configuration
    config = load_and_validate_config()

    # 3. Initialize database connection pools
    try:
        db_handler.initialize_connection_pools(
            min_connections=config.get("database", {}).get("min_connections", 2),
            max_connections=config.get("database", {}).get("max_connections", 10),
        )
        logger.info("✅ Database connection pools initialized.")
        db_handler.get_pool_status()
    except Exception as pool_error:
        logger.error(f"🚨 Failed to initialize connection pools: {pool_error}")
        logger.warning("⚠️ Falling back to direct connections.")

    # 4. Start the scheduler
    try:
        start_scheduler(config)
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler stopped by user.")
    finally:
        # 5. Cleanup resources on exit
        logger.info("Cleaning up resources...")
        db_handler.close_connection_pools()
        close_pdf_api_client()
        logger.info("✅ Cleanup complete. Exiting.")
