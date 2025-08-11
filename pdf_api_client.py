"""
PDF Processing API Client
Handles communication with the PDF processing API for claim document analysis.
"""

import requests
import logging
import json
from typing import Dict, List, Any
from pathlib import Path

logger = logging.getLogger(__name__)


class PDFProcessingAPIClient:
    """
    Client for communicating with the PDF processing API.
    """

    def __init__(self, api_base_url: str = "http://localhost:8888", timeout: int = 300):
        """
        Initialize the PDF processing API client.

        Args:
            api_base_url: Base URL of the PDF processing API
            timeout: Request timeout in seconds (5 minutes default for PDF processing)
        """
        self.api_base_url = api_base_url.rstrip("/")
        self.timeout = timeout
        self.session = requests.Session()

        # Set common headers
        self.session.headers.update(
            {
                "Content-Type": "application/json",
                "User-Agent": "PDF-Download-Service/1.0",
            }
        )

    def health_check(self) -> bool:
        """
        Check if the PDF processing API is available.

        Returns:
            bool: True if API is healthy, False otherwise
        """
        try:
            response = self.session.get(f"{self.api_base_url}/health", timeout=10)
            response.raise_for_status()

            logger.info("✅ PDF processing API health check passed")
            return True

        except requests.exceptions.RequestException as e:
            logger.error(f"❌ PDF processing API health check failed: {e}")
            return False

    def process_single_file(self, file_path: str) -> Dict[str, Any]:
        """
        Process a single PDF file via API.

        Args:
            file_path: Full path to the PDF file to process

        Returns:
            Dict containing processing results in the expected format
        """
        try:
            # Verify file exists
            if not Path(file_path).exists():
                raise FileNotFoundError(f"PDF file not found: {file_path}")

            # Prepare API request
            payload = {"file_path": str(file_path), "processing_mode": "single_file"}

            logger.info(f"📄 Sending single file to API: {Path(file_path).name}")

            # Make API call
            response = self.session.post(
                f"{self.api_base_url}/process/single",
                json=payload,
                timeout=self.timeout,
            )
            response.raise_for_status()

            # Parse response
            result = response.json()

            logger.info(f"✅ Single file processing completed: {Path(file_path).name}")

            return result

        except requests.exceptions.RequestException as e:
            error_msg = f"API request failed for {file_path}: {e}"
            logger.error(f"❌ {error_msg}")

            # Return error structure compatible with existing code
            return {
                "file_stats": {
                    str(file_path): {"file_model_output": None, "error": error_msg}
                },
                "overall_stats": {
                    "files_processed": 0,
                    "processing_time_seconds": 0.0,
                    "confidence_score": 0.0,
                    "error": error_msg,
                },
                "overall_cost": {
                    "api_calls": 0,
                    "estimated_cost_usd": 0.0,
                    "error": error_msg,
                },
            }

        except Exception as e:
            error_msg = f"Unexpected error processing {file_path}: {e}"
            logger.error(f"❌ {error_msg}")

            return {
                "file_stats": {
                    str(file_path): {"file_model_output": None, "error": error_msg}
                },
                "overall_stats": {
                    "files_processed": 0,
                    "processing_time_seconds": 0.0,
                    "confidence_score": 0.0,
                    "error": error_msg,
                },
                "overall_cost": {
                    "api_calls": 0,
                    "estimated_cost_usd": 0.0,
                    "error": error_msg,
                },
            }

    def process_claim_files(
        self, claim_id: int, file_paths: List[str]
    ) -> Dict[str, Any]:
        """
        Process all PDF files for a specific claim via API.

        Args:
            claim_id: The claim ID being processed
            file_paths: List of full paths to PDF files for this claim

        Returns:
            Dict containing consolidated processing results for the claim
        """
        try:
            # Verify all files exist
            missing_files = []
            existing_files = []

            for file_path in file_paths:
                if Path(file_path).exists():
                    existing_files.append(str(file_path))
                else:
                    missing_files.append(file_path)

            if missing_files:
                logger.warning(
                    f"Missing files for CLAIM_ID {claim_id}: {missing_files}"
                )

            if not existing_files:
                raise FileNotFoundError(
                    f"No existing files found for CLAIM_ID {claim_id}"
                )

            # Prepare API request
            payload = {
                "claim_id": claim_id,
                "file_paths": existing_files,
                "processing_mode": "claim_batch",
            }

            logger.info(
                f"📄 Sending {len(existing_files)} files to API for CLAIM_ID {claim_id}"
            )

            # Make API call
            response = self.session.post(
                f"{self.api_base_url}/process/claim", json=payload, timeout=self.timeout
            )
            response.raise_for_status()

            # Parse response
            result = response.json()

            logger.info(f"✅ Claim processing completed for CLAIM_ID {claim_id}")

            return result

        except requests.exceptions.RequestException as e:
            error_msg = f"API request failed for CLAIM_ID {claim_id}: {e}"
            logger.error(f"❌ {error_msg}")

            # Return error structure compatible with existing code
            return {
                "claim_id": claim_id,
                "files_processed": len(file_paths),
                "individual_file_results": {},
                "consolidated_data": {
                    "claim_numbers_found": [],
                    "chassis_found": [],
                    "doc_types": {},
                    "cnpjs_found": [],
                },
                "processing_summary": {
                    "successful_files": 0,
                    "failed_files": len(file_paths),
                    "total_amount_pecas": 0.0,
                    "total_amount_mao_obra": 0.0,
                    "total_amount_diversos": 0.0,
                    "total_amount_all": 0.0,
                    "error": error_msg,
                },
            }

        except Exception as e:
            error_msg = f"Unexpected error processing CLAIM_ID {claim_id}: {e}"
            logger.error(f"❌ {error_msg}")

            return {
                "claim_id": claim_id,
                "files_processed": len(file_paths),
                "individual_file_results": {},
                "consolidated_data": {
                    "claim_numbers_found": [],
                    "chassis_found": [],
                    "doc_types": {},
                    "cnpjs_found": [],
                },
                "processing_summary": {
                    "successful_files": 0,
                    "failed_files": len(file_paths),
                    "total_amount_pecas": 0.0,
                    "total_amount_mao_obra": 0.0,
                    "total_amount_diversos": 0.0,
                    "total_amount_all": 0.0,
                    "error": error_msg,
                },
            }

    def process_batch_files(self, file_paths: List[str]) -> str:
        """
        Process multiple PDF files in batch mode via API.
        Maintains compatibility with existing batch processing interface.

        Args:
            file_paths: List of full paths to PDF files

        Returns:
            JSON string with batch processing results
        """
        try:
            # Verify files exist
            existing_files = [fp for fp in file_paths if Path(fp).exists()]

            if not existing_files:
                raise FileNotFoundError("No existing files found in batch")

            # Prepare API request
            payload = {"file_paths": existing_files, "processing_mode": "batch"}

            logger.info(
                f"📄 Sending {len(existing_files)} files to API for batch processing"
            )

            # Make API call
            response = self.session.post(
                f"{self.api_base_url}/process/batch", json=payload, timeout=self.timeout
            )
            response.raise_for_status()

            # Parse response
            result = response.json()

            logger.info(
                f"✅ Batch processing completed for {len(existing_files)} files"
            )

            # Return as JSON string for compatibility
            return json.dumps(result, indent=2)

        except Exception as e:
            error_msg = f"Batch processing failed: {e}"
            logger.error(f"❌ {error_msg}")

            # Return error structure as JSON
            error_result = {
                "files_processed": len(file_paths),
                "processing_results": {},
                "batch_summary": {
                    "successful_files": 0,
                    "failed_files": len(file_paths),
                    "total_processing_time": 0.0,
                    "average_confidence": 0.0,
                    "error": error_msg,
                },
                "compatibility_results": {},
            }

            return json.dumps(error_result, indent=2)

    def close(self):
        """Close the session"""
        if self.session:
            self.session.close()


# Global API client instance
_pdf_api_client = None


def get_pdf_api_client(config: Dict | None = None) -> PDFProcessingAPIClient:
    """
    Get or create the global PDF API client instance.

    Args:
        config: Configuration dictionary with PDF API settings

    Returns:
        PDFProcessingAPIClient instance
    """
    global _pdf_api_client

    if _pdf_api_client is None:
        # Get API configuration
        if config:
            api_config = config.get("pdf_processing_api", {})
            api_url = api_config.get("base_url", "http://localhost:8888")
            timeout = api_config.get("timeout_seconds", 300)
        else:
            api_url = "http://localhost:8888"
            timeout = 300

        _pdf_api_client = PDFProcessingAPIClient(api_url, timeout)

        # Test connection
        if not _pdf_api_client.health_check():
            logger.warning(
                "⚠️ PDF processing API is not available - some functions may fail"
            )

    return _pdf_api_client


def close_pdf_api_client():
    """Close the global PDF API client"""
    global _pdf_api_client
    if _pdf_api_client:
        _pdf_api_client.close()
        _pdf_api_client = None


# Compatibility wrapper functions for existing code
def run_file_processing_simple_api(
    pdf_file_path: Path, config: Dict | None = None
) -> Dict[str, Any]:
    """
    API-based version of run_file_processing_simple.

    Args:
        pdf_file_path: Path to the PDF file to process
        config: Configuration dictionary

    Returns:
        Processing results in the same format as the original function
    """
    api_client = get_pdf_api_client(config)
    return api_client.process_single_file(str(pdf_file_path))


def process_claim_pdfs_individually_api(
    claim_id: int, pdf_file_paths: List[str], config: Dict | None = None
) -> Dict[str, Any]:
    """
    API-based version of process_claim_pdfs_individually.

    Args:
        claim_id: The claim ID being processed
        pdf_file_paths: List of PDF file paths for this claim
        config: Configuration dictionary

    Returns:
        Consolidated processing results for the claim
    """
    api_client = get_pdf_api_client(config)
    return api_client.process_claim_files(claim_id, pdf_file_paths)


def run_batch_processing_api(
    input_pdf_dir_path: Path, pdf_file_paths: List = [], config: Dict = None
) -> str:
    """
    API-based version of run_batch_processing.

    Args:
        input_pdf_dir_path: The directory containing PDF files (for compatibility)
        pdf_file_paths: A list of specific PDF file paths to process
        config: Configuration dictionary

    Returns:
        JSON string with batch processing results
    """
    api_client = get_pdf_api_client(config)

    if not pdf_file_paths:
        if input_pdf_dir_path and input_pdf_dir_path.exists():
            # Get all PDFs in the directory and its subdirectories
            pdf_file_paths = [str(p) for p in input_pdf_dir_path.rglob("*.pdf")]
        else:
            logger.warning("No PDF files provided and directory doesn't exist")
            return json.dumps({"error": "No PDF files to process"})

    return api_client.process_batch_files([str(p) for p in pdf_file_paths])


if __name__ == "__main__":
    """Test the PDF API client"""

    print("🧪 Testing PDF Processing API Client")
    print("=" * 50)

    # Test configuration
    test_config = {
        "pdf_processing_api": {
            "base_url": "http://localhost:8888",
            "timeout_seconds": 60,
        }
    }

    # Initialize client
    client = get_pdf_api_client(test_config)

    # Test health check
    print("1. Testing health check...")
    if client.health_check():
        print("✅ API is healthy")
    else:
        print("❌ API is not available")

    # Test with mock file paths
    test_file_paths = ["/path/to/test_file_1.pdf", "/path/to/test_file_2.pdf"]

    print(f"\n2. Testing single file processing...")
    try:
        result = client.process_single_file(test_file_paths[0])
        print("✅ Single file API call completed")
        print(f"   Result keys: {list(result.keys())}")
    except Exception as e:
        print(f"❌ Single file test failed: {e}")

    print(f"\n3. Testing claim-based processing...")
    try:
        result = client.process_claim_files(12345, test_file_paths)
        print("✅ Claim processing API call completed")
        print(f"   Result keys: {list(result.keys())}")
    except Exception as e:
        print(f"❌ Claim processing test failed: {e}")

    print(f"\n4. Testing batch processing...")
    try:
        result = client.process_batch_files(test_file_paths)
        print("✅ Batch processing API call completed")
        print(f"   Result length: {len(result)} characters")
    except Exception as e:
        print(f"❌ Batch processing test failed: {e}")

    # Cleanup
    close_pdf_api_client()
    print("\n✅ Testing completed")
