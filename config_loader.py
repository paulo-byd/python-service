import os
import logging
import sys
import db_handler  # Your custom module

logger = logging.getLogger(__name__)


def setup_environment():
    """Sets the environment mode based on command-line arguments."""
    if len(sys.argv) > 1:
        env_mode = sys.argv[1].lower()
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


def load_and_validate_config():
    """Loads and validates the application configuration."""
    try:
        if not hasattr(db_handler, "_ENVIRONMENT_MODE"):
            logger.warning("Environment mode not set, defaulting to local")
            db_handler.set_environment_mode("local")

        config = db_handler.load_config()

        # Validate required config sections
        required_sections = [
            "download",
            "scheduler",
            "logging",
            "monitoring",
            "pdf_processing_api",
        ]

        missing_sections = []
        for section in required_sections:
            if section not in config:
                missing_sections.append(section)

        if missing_sections:
            raise ValueError(f"Missing required config sections: {missing_sections}")

        storage_path = config["download"]["storage_path"]

        # Handle environment-specific paths
        if db_handler._ENVIRONMENT_MODE == "local":
            # For local development, use relative path
            storage_path = os.path.abspath("./pdf-claims")
            config["download"]["storage_path"] = storage_path
            logger.info(f"Local environment: Using storage path: {storage_path}")
        else:
            # For UAT/PROD, ensure absolute path
            if not os.path.isabs(storage_path):
                raise ValueError(
                    f"Production storage path must be absolute: {storage_path}"
                )
            logger.info(
                f"{db_handler._ENVIRONMENT_MODE.upper()} environment: Using storage path: {storage_path}"
            )

        try:
            os.makedirs(storage_path, exist_ok=True)
            logger.info(f"Storage directory confirmed: {storage_path}")
        except PermissionError:
            raise ValueError(
                f"No permission to create storage directory: {storage_path}"
            )
        except OSError as e:
            raise ValueError(f"Could not create storage directory {storage_path}: {e}")

        return config

    except Exception as e:
        logger.error(f"🚨 Configuration validation failed: {e}")
        sys.exit(1)
