import os
import logging
import sys
import db_handler  # Your custom module

logger = logging.getLogger(__name__)


def load_and_validate_config():
    """Loads and validates the application configuration."""
    try:
        config = db_handler.load_config()

        # Validate required config sections
        required_sections = [
            "download",
            "scheduler",
            "logging",
            "monitoring",
            "pdf_processing_api",
        ]
        for section in required_sections:
            if section not in config:
                raise ValueError(f"Missing required config section: {section}")

        # Validate and prepare storage path
        storage_path = config["download"]["storage_path"]
        if db_handler._ENVIRONMENT_MODE != "local" and not os.path.isabs(storage_path):
            raise ValueError(f"Storage path must be absolute: {storage_path}")

        if db_handler._ENVIRONMENT_MODE == "local":
            storage_path = "./pdf-claims"
            config["download"]["storage_path"] = storage_path  # Update config in-place

        os.makedirs(storage_path, exist_ok=True)
        logger.info(f"Storage directory confirmed: {storage_path}")

        return config

    except Exception as e:
        logger.error(f"🚨 Configuration validation failed: {e}")
        sys.exit(1)


def setup_environment():
    """Sets the environment mode based on command-line arguments."""
    if len(sys.argv) > 1:
        env_mode = sys.argv[1]
        if env_mode in ["local", "uat", "prod"]:
            db_handler.set_environment_mode(env_mode)
            logger.info(f"Environment mode set to: {env_mode}")
        else:
            logger.warning(
                f"Invalid environment mode: {env_mode}. Using default: local"
            )
            db_handler.set_environment_mode("local")
    else:
        db_handler.set_environment_mode("local")
        logger.info("Using default environment mode: local")
