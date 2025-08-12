import logging
import time
from datetime import datetime
from typing import Dict

import db_handler

# Mock/local matching availability flags
try:
    from matching_functions import (
        extract_amounts_from_processing_results as extract_func,
        validate_claim_data_match,
    )

    MATCHING_AVAILABLE = True
except ImportError:
    MATCHING_AVAILABLE = False

logger = logging.getLogger(__name__)


def extract_amounts_from_processing_results(
    processing_results: Dict,
) -> Dict[str, float]:
    """
    Extract financial amounts from PDF processing results.
    This function should be imported from matching_functions.py
    """
    try:
        from matching_functions import (
            extract_amounts_from_processing_results as extract_func,
        )

        return extract_func(processing_results)
    except ImportError:
        logger.warning("Could not import extraction function, using fallback")

        # Fallback extraction for API responses
        extracted_amounts = {
            "labour_amount": 0.0,
            "part_amount": 0.0,
            "total_amount": 0.0,
            "confidence_score": 0.0,
            "extraction_method": "api_fallback",
        }

        try:
            # Try to extract from API response format
            if "processing_summary" in processing_results:
                summary = processing_results["processing_summary"]
                extracted_amounts["labour_amount"] = summary.get(
                    "total_amount_mao_obra", 0.0
                )
                extracted_amounts["part_amount"] = summary.get(
                    "total_amount_pecas", 0.0
                ) + summary.get("total_amount_diversos", 0.0)
                extracted_amounts["total_amount"] = summary.get("total_amount_all", 0.0)

        except Exception as e:
            logger.error(f"Error in fallback amount extraction: {e}")

        return extracted_amounts


def run_batch_audit_matching(max_claims=None):
    """
    Run audit matching with individual file processing support.
    """
    if not MATCHING_AVAILABLE:
        logger.warning("⚠️ Matching functions not available - skipping audit matching")
        return {}

    logger.info(
        f"\n🔍 Starting ENHANCED continuous audit matching at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    )

    try:
        config = db_handler.load_config()

        # Validate matching configuration
        from matching_functions import validate_matching_config

        if not validate_matching_config(config):
            logger.error(
                "Invalid matching configuration - aborting enhanced audit matching"
            )
            return {}

        # Get batch size from config
        batch_size = config.get("audit_matching", {}).get("max_claims_per_batch", 100)

        all_results = {}
        total_processed = 0
        batch_number = 1
        successful_audits_total = 0
        failed_audits_total = 0

        # Continue processing until no more claims are available
        while True:
            logger.info(
                f"🔍 Starting enhanced batch {batch_number} (max size: {batch_size})..."
            )

            # Get claims ready for audit
            claims_df = db_handler.get_claims_ready_for_audit()

            if claims_df.empty:
                if batch_number == 1:
                    logger.info("No claims ready for audit matching")
                else:
                    logger.info(
                        f"✅ No more claims ready for audit matching after processing {total_processed} total claims"
                    )
                break

            # Take only batch_size claims for this iteration
            batch_claims = claims_df.head(batch_size)

            if len(batch_claims) == 0:
                break

            logger.info(
                f"🔍 Processing batch {batch_number}: {len(batch_claims)} claims"
            )

            # Prepare claim data for enhanced matching
            claim_data_list = []
            processing_results_dict = {}

            for _, claim_row in batch_claims.iterrows():
                claim_id = int(claim_row["CLAIM_ID"])

                # Add complete claim data for validation
                claim_data = {
                    "CLAIM_ID": claim_id,
                    "CLAIM_NO": claim_row.get("CLAIM_NO"),
                    "VIN": claim_row.get("VIN"),
                    "LABOUR_AMOUNT_DMS": claim_row.get("LABOUR_AMOUNT_DMS", 0),
                    "PART_AMOUNT_DMS": claim_row.get("PART_AMOUNT_DMS", 0),
                    "DEALER_CODE": claim_row.get("DEALER_CODE"),
                    "DEALER_NAME": claim_row.get("DEALER_NAME"),
                }
                claim_data_list.append(claim_data)

                # Get enhanced processing results for this claim
                pdf_files = db_handler.get_claim_pdf_files(claim_id)

                # Try to get real processing results, fallback to mock
                try:
                    from matching_functions import (
                        get_processing_results_for_claim,
                    )

                    processing_results_dict[claim_id] = (
                        get_processing_results_for_claim(claim_id, pdf_files)
                    )
                except Exception as e:
                    logger.warning(
                        f"Could not get enhanced processing results for CLAIM_ID {claim_id}: {e}"
                    )
                    # Fallback to legacy mock
                    from matching_functions import get_mock_processing_results_for_claim

                    processing_results_dict[claim_id] = (
                        get_mock_processing_results_for_claim(claim_id, pdf_files)
                    )

            # Perform batch matching
            from matching_functions import batch_match_claims

            batch_matching_results = batch_match_claims(
                claim_data_list, processing_results_dict, config
            )

            # Update audit status based on results
            successful_audits_batch = 0
            failed_audits_batch = 0

            for claim_id, result in batch_matching_results.items():
                try:
                    if result["match_success"]:
                        db_handler.update_audit_status(claim_id, "COMPLETE")
                        successful_audits_batch += 1
                        successful_audits_total += 1
                        logger.info(
                            f"✅ CLAIM_ID {claim_id}: audit passed - {result['reason']}"
                        )

                        # Log details for successful matches
                        if (
                            "details" in result
                            and "data_validation" in result["details"]
                        ):
                            validation = result["details"]["data_validation"]
                            if validation.get("overall_data_match"):
                                logger.debug(
                                    "   Data validation: ✅ Claim/VIN match confirmed"
                                )

                    else:
                        db_handler.update_audit_status(claim_id, "REJECTED")
                        failed_audits_batch += 1
                        failed_audits_total += 1
                        logger.warning(
                            f"❌ CLAIM_ID {claim_id}: audit failed - {result['reason']}"
                        )

                        # Log enhanced details for failed matches
                        if "details" in result:
                            details = result["details"]
                            if "data_validation" in details and details[
                                "data_validation"
                            ].get("issues_found"):
                                issues = details["data_validation"]["issues_found"]
                                logger.warning(
                                    f"   Data validation issues: {', '.join(issues)}"
                                )

                except Exception as e:
                    logger.error(
                        f"❌ Error updating audit status for CLAIM_ID {claim_id}: {e}"
                    )
                    failed_audits_batch += 1
                    failed_audits_total += 1

            # Add batch results to overall results
            all_results.update(batch_matching_results)

            # Update counters
            total_processed += len(batch_claims)

            # Log enhanced batch summary
            logger.info(
                f"📊 Batch {batch_number} completed: {successful_audits_batch} passed, {failed_audits_batch} failed"
            )

            batch_number += 1

            # If we processed less than batch_size, we've reached the end
            if len(batch_claims) < batch_size:
                logger.info(
                    f"✅ Processed final enhanced batch of {len(batch_claims)} claims"
                )
                break

            # Small delay between batches to prevent overwhelming the database
            time.sleep(0.1)

        # Generate and log final report if any claims were processed
        if all_results:
            from matching_functions import generate_matching_report

            report = generate_matching_report(all_results)
            logger.info(f"\n{report}")

        # Enhanced final summary
        if total_processed > 0:
            logger.info("📊 Continuous audit matching completed:")
            logger.info(f"   Total batches: {batch_number - 1}")
            logger.info(f"   Total claims processed: {total_processed}")
            logger.info(
                f"   Total passed: {successful_audits_total} ({successful_audits_total / total_processed * 100:.1f}%)"
            )
            logger.info(
                f"   Total failed: {failed_audits_total} ({failed_audits_total / total_processed * 100:.1f}%)"
            )
        else:
            logger.info(
                "📊 Continuous audit matching completed: No claims were processed"
            )

        return all_results

    except Exception as e:
        logger.error(f"🚨 Critical error in enhanced continuous audit matching: {e}")
        return {}


def run_batch_audit_matching_job():
    """
    Scheduled job function for audit matching.
    Processes claims that have been through PDF processing with enhanced data validation.
    """
    logger.info(
        f"\n🔍 Starting scheduled audit matching at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    )

    try:
        matching_results = run_batch_audit_matching()

        if matching_results:
            logger.info(
                f"🔍 Enhanced scheduled audit matching completed for {len(matching_results)} claims"
            )
        else:
            logger.info(
                "🔍 Enhanced scheduled audit matching completed - no claims audited"
            )

    except Exception as e:
        logger.error(f"🚨 Critical error in enhanced scheduled audit matching: {e}")
