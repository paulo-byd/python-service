import logging
import sys
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


def main():
    try:
        # 1. Set environment (local, uat, prod)
        logger.info("🚀 Starting PDF Download Service...")
        setup_environment()

        # 2. Load and validate configuration
        logger.info("📋 Loading configuration...")
        config = load_and_validate_config()

        # 3. Initialize database connection pools
        logger.info("🔌 Initializing database connections...")
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
        logger.info("⏰ Starting scheduler...")
        start_scheduler(config)

    except KeyboardInterrupt:
        logger.info("👋 Received shutdown signal (Ctrl+C)")
    except SystemExit:
        logger.info("👋 System exit requested")
    except Exception as e:
        logger.error(f"🚨 Critical error in main: {e}")
        sys.exit(1)
    finally:
        # 5. Cleanup resources on exit
        logger.info("🧹 Cleaning up resources...")
        try:
            db_handler.close_connection_pools()
            close_pdf_api_client()
            logger.info("✅ Cleanup complete. Exiting.")
        except Exception as cleanup_error:
            logger.error(f"⚠️ Error during cleanup: {cleanup_error}")


if __name__ == "__main__":
    main()
