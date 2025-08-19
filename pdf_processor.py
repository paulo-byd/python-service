# pdf_processor.py
import os
import logging
import sys
from pathlib import Path
from datetime import datetime

import db_handler
from pdf_api_client import (
    get_pdf_api_client,
    process_claim_pdfs_individually_api,
)

from audit_matcher import extract_amounts_from_processing_results

logger = logging.getLogger(__name__)

# Mock/local processing availability flags
PDF_PROCESSING_AVAILABLE = True

ULTRA_ARENA_MAIN = Path(__file__).resolve().parents[1] / "ultra-arena-frk" / "Ultra_Arena_Main"
if str(ULTRA_ARENA_MAIN) not in sys.path:
    sys.path.append(str(ULTRA_ARENA_MAIN))

from main_modular import run_file_processing_simple  # Ultra Arena entry

def process_pdfs_with_ultra_arena(input_dir: str, pdf_paths: list[str]) -> dict:
    """
    Replace mock processing with a real call to Ultra Arena.
    Returns the structured results dict from Ultra Arena.
    """
    input_dir_path = Path(input_dir)
    file_paths = [Path(p) for p in pdf_paths]
    return run_file_processing_simple(
        input_pdf_dir_path=input_dir_path,
        pdf_file_paths=file_paths
    )

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
            claim_id = int(claim_row["CLAIM_ID"])

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
                from audit_matcher import run_batch_audit_matching

                matching_results = run_batch_audit_matching()

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


def run_batch_pdf_processing_api():
    """
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
