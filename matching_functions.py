"""
Matching Functions for PDF Invoice Matching
This module now supports both the old batch processing and new individual file processing.

IMPORTANT: Still includes TEST MODE with random results for frontend testing.
When the real PDF processing is ready, replace this with the production version.
"""

import json
import random
import logging
from pathlib import Path
from typing import Dict, List, Tuple, Any
import pandas as pd
from datetime import datetime

import db_handler

logger = logging.getLogger(__name__)

# TEST MODE FLAG - Set this to False when you have the real PDF processor
TEST_MODE_RANDOM = True
RANDOM_SUCCESS_RATE = 0.7  # 70% will be marked as COMPLETE, 30% as REJECTED


def extract_amounts_from_processing_results(
    processing_results: Dict[str, Any],
) -> Dict[str, float]:
    """
    Extract financial amounts from PDF processing results.

    Supports both old batch format and new individual file processing format.

    Args:
        processing_results: Results from process_claim_pdfs_individually() or legacy format

    Returns:
        Dict with keys: labour_amount, part_amount, total_amount, confidence_score
    """

    extracted_amounts = {
        "labour_amount": 0.0,
        "part_amount": 0.0,
        "total_amount": 0.0,
        "confidence_score": 0.0,
        "extraction_method": "unknown",
    }

    try:
        # Check if this is the new individual file processing format
        if (
            "consolidated_data" in processing_results
            and "processing_summary" in processing_results
        ):
            # New format from process_claim_pdfs_individually()
            summary = processing_results["processing_summary"]

            # Map document types to our amounts
            # "Mão de Obra" -> labour_amount
            # "Peças" -> part_amount
            # "Diversos" -> could be either, for now add to parts

            extracted_amounts["labour_amount"] = summary.get(
                "total_amount_mao_obra", 0.0
            )
            extracted_amounts["part_amount"] = summary.get(
                "total_amount_pecas", 0.0
            ) + summary.get("total_amount_diversos", 0.0)
            extracted_amounts["total_amount"] = summary.get("total_amount_all", 0.0)
            extracted_amounts["extraction_method"] = "individual_file_processing"

            # Calculate confidence from individual file results
            if "individual_file_results" in processing_results:
                total_confidence = 0.0
                file_count = 0

                for file_path, file_result in processing_results[
                    "individual_file_results"
                ].items():
                    if "overall_stats" in file_result:
                        confidence = file_result["overall_stats"].get(
                            "confidence_score", 0.0
                        )
                        if confidence > 0:
                            total_confidence += confidence
                            file_count += 1

                if file_count > 0:
                    extracted_amounts["confidence_score"] = (
                        total_confidence / file_count
                    )

            logger.debug(
                f"Extracted amounts using new format: Labour={extracted_amounts['labour_amount']:.2f}, Parts={extracted_amounts['part_amount']:.2f}"
            )

        else:
            # Legacy format or old batch processing format
            extracted_amounts["extraction_method"] = "legacy_batch_processing"

            # Try to extract from old format
            # This is fallback for compatibility
            if isinstance(processing_results, dict):
                # Look for any amount-like keys
                for key, value in processing_results.items():
                    if isinstance(value, dict) and "file_name" in value:
                        # Old compatibility format
                        break

                # For now, use mock extraction for legacy format
                logger.warning(
                    "Using legacy format extraction - consider upgrading to individual file processing"
                )
                extracted_amounts = _mock_extract_amounts_legacy(processing_results)

    except Exception as e:
        logger.error(f"Error extracting amounts from processing results: {e}")
        # Fallback to zero amounts

    return extracted_amounts


def _mock_extract_amounts_legacy(
    processing_results: Dict[str, Any],
) -> Dict[str, float]:
    """
    Mock extraction for legacy batch processing format.
    """

    # Generate mock amounts based on number of files processed
    file_count = len(processing_results) if processing_results else 1

    # Generate realistic amounts based on file count
    base_amount = random.uniform(500, 2000) * file_count
    labour_ratio = random.uniform(0.3, 0.7)

    labour_amount = base_amount * labour_ratio
    part_amount = base_amount * (1 - labour_ratio)

    return {
        "labour_amount": round(labour_amount, 2),
        "part_amount": round(part_amount, 2),
        "total_amount": round(labour_amount + part_amount, 2),
        "confidence_score": random.uniform(0.75, 0.95),
        "extraction_method": "mock_legacy",
    }


def validate_claim_data_match(
    processing_results: Dict[str, Any], claim_data: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Validate that the claim data from PDF processing matches the DMS claim data.

    Checks:
    - CLAIM_NUMBER from PDFs matches CLAIM_NO from DMS
    - Chassi from PDFs matches VIN from DMS

    Args:
        processing_results: Results from individual file processing
        claim_data: Claim data from DMS (CLAIM_NO, VIN, etc.)

    Returns:
        Dict with validation results
    """

    validation_results = {
        "claim_number_match": False,
        "chassi_vin_match": False,
        "overall_data_match": False,
        "issues_found": [],
        "claim_numbers_in_pdfs": [],
        "chassis_in_pdfs": [],
        "dms_claim_no": claim_data.get("CLAIM_NO"),
        "dms_vin": claim_data.get("VIN"),
    }

    try:
        # Extract claim data from processing results
        if "consolidated_data" in processing_results:
            consolidated = processing_results["consolidated_data"]

            # Get claim numbers and chassis from PDFs
            claim_numbers_found = consolidated.get("claim_numbers_found", [])
            chassis_found = consolidated.get("chassis_found", [])

            validation_results["claim_numbers_in_pdfs"] = claim_numbers_found
            validation_results["chassis_in_pdfs"] = chassis_found

            # Validate claim number match
            dms_claim_no = claim_data.get("CLAIM_NO", "").strip()
            if dms_claim_no:
                # Check if any of the PDF claim numbers match the DMS claim number
                for pdf_claim in claim_numbers_found:
                    if pdf_claim and pdf_claim.strip().upper() == dms_claim_no.upper():
                        validation_results["claim_number_match"] = True
                        break

                if not validation_results["claim_number_match"] and claim_numbers_found:
                    validation_results["issues_found"].append(
                        f"Claim number mismatch: DMS='{dms_claim_no}' vs PDFs={claim_numbers_found}"
                    )

            # Validate VIN/Chassi match
            dms_vin = claim_data.get("VIN", "").strip()
            if dms_vin:
                # Check if any of the PDF chassis match the DMS VIN
                for pdf_chassi in chassis_found:
                    if pdf_chassi and pdf_chassi.strip().upper() == dms_vin.upper():
                        validation_results["chassi_vin_match"] = True
                        break

                if not validation_results["chassi_vin_match"] and chassis_found:
                    validation_results["issues_found"].append(
                        f"VIN/Chassi mismatch: DMS='{dms_vin}' vs PDFs={chassis_found}"
                    )

            # Overall match requires both claim number and VIN to match
            validation_results["overall_data_match"] = (
                validation_results["claim_number_match"]
                and validation_results["chassi_vin_match"]
            )

            if not validation_results["overall_data_match"]:
                if not claim_numbers_found:
                    validation_results["issues_found"].append(
                        "No claim numbers found in PDFs"
                    )
                if not chassis_found:
                    validation_results["issues_found"].append(
                        "No chassis/VIN found in PDFs"
                    )

        else:
            # Legacy format - assume match for compatibility
            validation_results["claim_number_match"] = True
            validation_results["chassi_vin_match"] = True
            validation_results["overall_data_match"] = True
            validation_results["issues_found"].append(
                "Legacy format - data validation skipped"
            )

    except Exception as e:
        logger.error(f"Error validating claim data: {e}")
        validation_results["issues_found"].append(f"Validation error: {str(e)}")

    return validation_results


def match_invoices_with_dms_estimates(
    claim_id: int,
    labour_amount_dms: float,
    part_amount_dms: float,
    processing_results: Dict[str, Any],
    config: Dict | None = None,
    claim_data: Dict[str, Any] = None,
) -> Tuple[bool, str, Dict[str, Any]]:
    """
    ENHANCED VERSION - Now supports individual file processing and data validation.
    Updates LABOUR_AMOUNT_PROCESSING and PART_AMOUNT_PROCESSING in database.

    Args:
        claim_id (int): The claim ID being processed
        labour_amount_dms (float): Labour amount from DMS system
        part_amount_dms (float): Part amount from DMS system
        processing_results (Dict): Results from process_claim_pdfs_individually() or legacy format
        config (Dict): Configuration dictionary (optional)
        claim_data (Dict): Additional claim data for validation (CLAIM_NO, VIN, etc.)

    Returns:
        Tuple[bool, str, Dict]: (match_success, reason, details)
    """

    logger.info(f"🔍 Starting enhanced invoice matching for CLAIM_ID {claim_id}")
    logger.info(f"   DMS Labour Amount: {labour_amount_dms}")
    logger.info(f"   DMS Part Amount: {part_amount_dms}")

    # Determine processing result type
    if "consolidated_data" in processing_results:
        files_count = processing_results.get("files_processed", 0)
        logger.info(f"   Individual file processing results: {files_count} files")
    else:
        files_count = len(processing_results)
        logger.info(f"   Legacy processing results: {files_count} entries")

    if TEST_MODE_RANDOM:
        logger.warning(
            f"⚠️ TEST MODE ACTIVE - Using random results for CLAIM_ID {claim_id}"
        )

    # Calculate total DMS amount
    total_dms_amount = (labour_amount_dms or 0) + (part_amount_dms or 0)

    # Extract amounts from processing results
    extracted_amounts = extract_amounts_from_processing_results(processing_results)

    # Validate claim data match (if claim_data provided)
    data_validation = {}
    if claim_data:
        data_validation = validate_claim_data_match(processing_results, claim_data)
        logger.info(f"   Data validation: {data_validation['overall_data_match']}")
        if data_validation["issues_found"]:
            logger.warning(f"   Validation issues: {data_validation['issues_found']}")

    # TEST MODE: Override with random results
    if TEST_MODE_RANDOM:
        # Use claim_id as seed for consistent results per claim
        random.seed(claim_id)

        # Randomly decide if this claim passes or fails
        match_success = random.random() < RANDOM_SUCCESS_RATE

        if match_success:
            # SUCCESS: Use exact DMS amounts for processing amounts
            extracted_labour = labour_amount_dms or 0
            extracted_part = part_amount_dms or 0
            reason = f"✅ TEST MODE: Perfect match (random success)"
        else:
            # FAILURE: Generate random different amounts
            variation = random.choice(
                [
                    random.uniform(0.5, 0.8),  # 20-50% less
                    random.uniform(1.2, 1.5),  # 20-50% more
                    random.uniform(0.3, 0.5),  # 50-70% less (major discrepancy)
                ]
            )

            labour_ratio = random.uniform(0.2, 0.8)
            extracted_labour = total_dms_amount * labour_ratio * variation
            extracted_part = total_dms_amount * (1 - labour_ratio) * variation

            # Round to 2 decimal places for realism
            extracted_labour = round(extracted_labour, 2)
            extracted_part = round(extracted_part, 2)

            # Generate specific failure reasons
            failure_reasons = [
                "Amount mismatch exceeds tolerance",
                "Labour amount discrepancy detected",
                "Parts amount discrepancy detected",
                "Data validation failed",
                "Multiple discrepancies found",
            ]

            # Add data validation failures if applicable
            if (
                claim_data
                and data_validation
                and not data_validation["overall_data_match"]
            ):
                failure_reasons.extend(
                    [
                        "Claim number mismatch",
                        "VIN/Chassi mismatch",
                    ]
                )

            specific_reason = random.choice(failure_reasons)
            reason = f"❌ TEST MODE: {specific_reason} (random failure)"

        # Update the database with processing amounts
        db_handler.update_processing_amounts(claim_id, extracted_labour, extracted_part)

    else:
        # PRODUCTION MODE: Use real extracted amounts
        extracted_labour = extracted_amounts["labour_amount"]
        extracted_part = extracted_amounts["part_amount"]

        # Get tolerance from config
        tolerance_pct = 0.0
        if config and "audit_matching" in config:
            tolerance_pct = config["audit_matching"].get("tolerance_percentage", 0.0)

        tolerance = (
            total_dms_amount * (tolerance_pct / 100.0) if total_dms_amount > 0 else 0
        )

        # Compare amounts
        labour_match = _amounts_match(
            labour_amount_dms or 0, extracted_labour, tolerance / 2
        )
        part_match = _amounts_match(part_amount_dms or 0, extracted_part, tolerance / 2)

        # Check data validation
        data_match = True
        if claim_data and data_validation:
            data_match = data_validation["overall_data_match"]

        match_success = labour_match and part_match and data_match

        # Generate reason
        if match_success:
            reason = f"✅ All validations passed (tolerance: {tolerance_pct}%)"
        else:
            mismatches = []
            if not labour_match:
                mismatches.append("labour amount")
            if not part_match:
                mismatches.append("parts amount")
            if not data_match:
                mismatches.append("claim data validation")
            reason = f"❌ Failed validation: {', '.join(mismatches)}"

        # Update database with processing amounts (production mode)
        db_handler.update_processing_amounts(claim_id, extracted_labour, extracted_part)

    # Create detailed results
    details = {
        "test_mode": TEST_MODE_RANDOM,
        "extraction_method": extracted_amounts.get("extraction_method", "unknown"),
        "dms_amounts": {
            "labour": labour_amount_dms,
            "parts": part_amount_dms,
            "total": total_dms_amount,
        },
        "extracted_amounts": {
            "labour_amount": extracted_labour,
            "part_amount": extracted_part,
            "total_amount": extracted_labour + extracted_part,
            "extraction_confidence": extracted_amounts.get("confidence_score", 0.95),
            "files_processed": files_count,
        },
        "matching_results": {
            "labour_match": abs((labour_amount_dms or 0) - extracted_labour) < 10,
            "part_match": abs((part_amount_dms or 0) - extracted_part) < 10,
            "overall_match": match_success,
            "tolerance_used": config.get("audit_matching", {}).get(
                "tolerance_percentage", 0.0
            )
            if config
            else 0.0,
        },
        "data_validation": data_validation,
        "processing_timestamp": datetime.now().isoformat(),
    }

    # Add processing results summary to details
    if "consolidated_data" in processing_results:
        details["processing_results_summary"] = {
            "doc_types_found": list(
                processing_results["consolidated_data"].get("doc_types", {}).keys()
            ),
            "claim_numbers_found": processing_results["consolidated_data"].get(
                "claim_numbers_found", []
            ),
            "chassis_found": processing_results["consolidated_data"].get(
                "chassis_found", []
            ),
            "cnpjs_found": processing_results["consolidated_data"].get(
                "cnpjs_found", []
            ),
            "successful_files": processing_results["processing_summary"].get(
                "successful_files", 0
            ),
            "failed_files": processing_results["processing_summary"].get(
                "failed_files", 0
            ),
        }

    logger.info(f"🔍 Matching result for CLAIM_ID {claim_id}: {reason}")

    if TEST_MODE_RANDOM:
        logger.info(f"📊 TEST RESULT - Success: {match_success}")
        logger.info(f"   DMS Total: {total_dms_amount:.2f}")
        logger.info(f"   Processing Total: {(extracted_labour + extracted_part):.2f}")
        logger.info(f"   Updated LABOUR_AMOUNT_PROCESSING: {extracted_labour:.2f}")
        logger.info(f"   Updated PART_AMOUNT_PROCESSING: {extracted_part:.2f}")

    return match_success, reason, details


def _amounts_match(amount1: float, amount2: float, tolerance: float = 0.0) -> bool:
    """
    Check if two amounts match within the specified tolerance.
    """
    if amount1 == 0 and amount2 == 0:
        return True

    diff = abs(amount1 - amount2)
    return diff <= tolerance


def batch_match_claims(
    claim_data_list: List[Dict], processing_results_dict: Dict, config: Dict = None
) -> Dict[int, Dict]:
    """
    ENHANCED - Process multiple claims for invoice matching in batch.
    Now supports both legacy and individual file processing formats.
    """

    results = {}

    logger.info(
        f"🔍 Starting enhanced batch invoice matching for {len(claim_data_list)} claims"
    )

    if TEST_MODE_RANDOM:
        logger.warning(
            f"⚠️ TEST MODE: Generating random audit results for frontend testing"
        )
        logger.warning(f"⚠️ Success rate set to {RANDOM_SUCCESS_RATE * 100}%")
        logger.warning(f"⚠️ Processing amounts will be updated in database")

    for claim_data in claim_data_list:
        claim_id = claim_data["CLAIM_ID"]
        labour_amount_dms = claim_data.get("LABOUR_AMOUNT_DMS", 0)
        part_amount_dms = claim_data.get("PART_AMOUNT_DMS", 0)

        # Get processing results for this claim
        processing_results = processing_results_dict.get(claim_id, {})

        if not processing_results:
            logger.warning(f"No processing results found for CLAIM_ID {claim_id}")
            results[claim_id] = {
                "match_success": False,
                "reason": "No processing results available",
                "details": {
                    "error": "No processing results found",
                    "test_mode": TEST_MODE_RANDOM,
                },
            }
            continue

        # Perform the enhanced matching
        try:
            match_success, reason, details = match_invoices_with_dms_estimates(
                claim_id,
                labour_amount_dms,
                part_amount_dms,
                processing_results,
                config,
                claim_data,  # Pass full claim data for validation
            )

            results[claim_id] = {
                "match_success": match_success,
                "reason": reason,
                "details": details,
            }

        except Exception as e:
            logger.error(f"❌ Error matching CLAIM_ID {claim_id}: {e}")
            results[claim_id] = {
                "match_success": False,
                "reason": f"Processing error: {str(e)}",
                "details": {"error": str(e), "test_mode": TEST_MODE_RANDOM},
            }

    # Log summary
    successful_matches = sum(1 for r in results.values() if r["match_success"])
    failed_matches = len(results) - successful_matches

    logger.info(f"🔍 Enhanced batch matching completed:")
    logger.info(
        f"   ✅ Successful: {successful_matches} ({successful_matches / len(claim_data_list) * 100:.1f}%)"
    )
    logger.info(
        f"   ❌ Failed: {failed_matches} ({failed_matches / len(claim_data_list) * 100:.1f}%)"
    )

    if TEST_MODE_RANDOM:
        logger.warning("⚠️ Remember: These are TEST RESULTS with random data!")
        logger.warning(
            "⚠️ LABOUR_AMOUNT_PROCESSING and PART_AMOUNT_PROCESSING have been updated"
        )
        logger.info(
            "💡 To reset for production: UPDATE CLAIM_STATUS SET AUDIT_STATUS = NULL, LABOUR_AMOUNT_PROCESSING = NULL, PART_AMOUNT_PROCESSING = NULL"
        )

    return results


def get_enhanced_processing_results_for_claim(
    claim_id: int, file_paths: List[str]
) -> Dict[str, Any]:
    """
    Generate enhanced processing results for a specific claim's PDF files.
    Uses the new individual file processing format.
    """

    # Import the enhanced processing function
    try:
        from process_pdf_dir import process_claim_pdfs_individually

        # Use the real function if available
        return process_claim_pdfs_individually(claim_id, file_paths)

    except ImportError:
        # Fallback to mock for testing
        logger.warning("Using mock processing results - process_pdf_dir not available")
        return _mock_process_claim_pdfs_individually(claim_id, file_paths)


def _mock_process_claim_pdfs_individually(
    claim_id: int, file_paths: List[str]
) -> Dict[str, Any]:
    """
    Mock version of process_claim_pdfs_individually for testing.
    """

    # Use claim_id as seed for consistent results
    random.seed(claim_id)

    # Generate realistic claim number and VIN
    claim_number = f"BYDAMEBR{random.randint(1000, 9999)}WCN{claim_id:06d}_01"
    chassi = f"LGXCE4CC{random.randint(1, 9)}S{random.randint(1000000, 9999999)}"

    mock_results = {
        "claim_id": claim_id,
        "files_processed": len(file_paths),
        "individual_file_results": {},
        "consolidated_data": {
            "claim_numbers_found": [claim_number],
            "chassis_found": [chassi],
            "doc_types": {},
            "cnpjs_found": [
                f"{random.randint(10, 99)}.{random.randint(100, 999)}.{random.randint(100, 999)}/{random.randint(1000, 9999)}-{random.randint(10, 99)}"
            ],
        },
        "processing_summary": {
            "successful_files": len(file_paths),
            "failed_files": 0,
            "total_amount_pecas": 0.0,
            "total_amount_mao_obra": 0.0,
            "total_amount_diversos": 0.0,
            "total_amount_all": 0.0,
        },
    }

    # Generate amounts for each file
    for i, file_path in enumerate(file_paths):
        # Generate realistic amounts
        doc_type = random.choice(["Peças", "Mão de Obra", "Diversos"])

        if doc_type == "Peças":
            amount = random.uniform(500.00, 3000.00)
            mock_results["processing_summary"]["total_amount_pecas"] += amount
        elif doc_type == "Mão de Obra":
            amount = random.uniform(200.00, 1500.00)
            mock_results["processing_summary"]["total_amount_mao_obra"] += amount
        else:
            amount = random.uniform(50.00, 500.00)
            mock_results["processing_summary"]["total_amount_diversos"] += amount

        mock_results["processing_summary"]["total_amount_all"] += amount

        # Track doc types
        if doc_type not in mock_results["consolidated_data"]["doc_types"]:
            mock_results["consolidated_data"]["doc_types"][doc_type] = 0.0
        mock_results["consolidated_data"]["doc_types"][doc_type] += amount

        # Mock individual file result
        file_path_obj = Path(file_path)
        valor_total = (
            f"{amount:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        )

        mock_results["individual_file_results"][file_path] = {
            "file_stats": {
                file_path: {
                    "file_model_output": {
                        "file_name_llm": file_path_obj.name,
                        "DOC_TYPE": doc_type,
                        "CNPJ_1": mock_results["consolidated_data"]["cnpjs_found"][0],
                        "CNPJ_2": None,
                        "VALOR_TOTAL": valor_total,
                        "Chassi": chassi,
                        "CLAIM_NUMBER": claim_number,
                    }
                }
            },
            "overall_stats": {
                "files_processed": 1,
                "processing_time_seconds": random.uniform(2.0, 8.0),
                "confidence_score": random.uniform(0.8, 0.95),
                "mock_data": True,
            },
        }

    return mock_results


# Legacy compatibility functions
def get_mock_processing_results_for_claim(
    claim_id: int, file_paths: List[str]
) -> Dict[str, Any]:
    """
    Legacy compatibility function - converts new format to old format.
    """

    # Get enhanced results
    enhanced_results = get_enhanced_processing_results_for_claim(claim_id, file_paths)

    # Convert to legacy format for backward compatibility
    legacy_results = {}

    if "individual_file_results" in enhanced_results:
        for file_path, file_result in enhanced_results[
            "individual_file_results"
        ].items():
            file_path_obj = Path(file_path)
            file_path_hash = hashlib.sha256(str(file_path_obj).encode()).hexdigest()[
                :16
            ]
            key = f"{file_path_hash}_<{file_path_obj.name}>"

            legacy_results[key] = {
                "file_name": file_path_obj.name,
                "claim_id": claim_id,
                "processing_status": "success",
                "enhanced_data": file_result,  # Include enhanced data for future use
            }

    return legacy_results


# Validation and utility functions
def validate_matching_config(config: Dict) -> bool:
    """
    Enhanced validation of the audit matching configuration.
    """

    if not config:
        logger.warning("No configuration provided for matching validation")
        return False

    audit_config = config.get("audit_matching", {})

    # Check required settings
    required_settings = ["max_claims_per_batch", "exact_amount_match"]
    for setting in required_settings:
        if setting not in audit_config:
            logger.error(f"Missing required audit_matching setting: {setting}")
            return False

    # Validate tolerance percentage
    tolerance = audit_config.get("tolerance_percentage", 0.0)
    if not isinstance(tolerance, (int, float)) or tolerance < 0 or tolerance > 100:
        logger.error(
            f"Invalid tolerance_percentage: {tolerance}. Must be between 0 and 100."
        )
        return False

    # Validate batch size
    batch_size = audit_config.get("max_claims_per_batch", 20)
    if not isinstance(batch_size, int) or batch_size <= 0:
        logger.error(
            f"Invalid max_claims_per_batch: {batch_size}. Must be positive integer."
        )
        return False

    logger.info("✅ Enhanced audit matching configuration validated successfully")
    return True


def generate_matching_report(matching_results: Dict[int, Dict]) -> str:
    """
    Generate an enhanced human-readable report of matching results.
    """

    if not matching_results:
        return "No matching results to report."

    total_claims = len(matching_results)
    successful_matches = sum(1 for r in matching_results.values() if r["match_success"])
    failed_matches = total_claims - successful_matches

    report_lines = [
        "📊 ENHANCED INVOICE MATCHING REPORT",
    ]

    if TEST_MODE_RANDOM:
        report_lines.extend(
            [
                "⚠️  TEST MODE ACTIVE - RANDOM RESULTS ⚠️",
                "Enhanced with individual file processing support",
                "Processing amounts have been updated in database",
                "=" * 60,
            ]
        )
    else:
        report_lines.extend(
            [
                "Production mode with enhanced data validation",
                "=" * 60,
            ]
        )

    report_lines.extend(
        [
            f"Total Claims Processed: {total_claims}",
            f"Successful Matches: {successful_matches} ({successful_matches / total_claims * 100:.1f}%)",
            f"Failed Matches: {failed_matches} ({failed_matches / total_claims * 100:.1f}%)",
            "",
            "DETAILED RESULTS:",
            "-" * 40,
        ]
    )

    # Sort by claim_id for consistent reporting
    for claim_id in sorted(matching_results.keys()):
        result = matching_results[claim_id]
        status_icon = "✅" if result["match_success"] else "❌"

        report_lines.append(f"{status_icon} CLAIM_ID {claim_id}: {result['reason']}")

        # Add enhanced details for failed matches
        if not result["match_success"] and "details" in result:
            details = result["details"]

            # Show extraction method
            extraction_method = details.get("extraction_method", "unknown")
            report_lines.append(f"    Extraction method: {extraction_method}")

            # Show amounts
            if "dms_amounts" in details and "extracted_amounts" in details:
                dms = details["dms_amounts"]
                extracted = details["extracted_amounts"]
                report_lines.append(
                    f"    DMS: Labour={dms.get('labour', 0):.2f}, Parts={dms.get('parts', 0):.2f}"
                )
                report_lines.append(
                    f"    Processing: Labour={extracted.get('labour_amount', 0):.2f}, Parts={extracted.get('part_amount', 0):.2f}"
                )

            # Show data validation issues
            if "data_validation" in details and details["data_validation"]:
                validation = details["data_validation"]
                if validation.get("issues_found"):
                    report_lines.append(
                        f"    Data issues: {'; '.join(validation['issues_found'])}"
                    )

    if TEST_MODE_RANDOM:
        report_lines.extend(
            [
                "",
                "🔄 TO RESET FOR PRODUCTION:",
                "1. Set TEST_MODE_RANDOM = False in matching_functions.py",
                "2. Set USE_MOCK_PROCESSING = False in process_pdf_dir.py",
                "3. Run SQL: UPDATE CLAIM_STATUS SET AUDIT_STATUS = NULL, LABOUR_AMOUNT_PROCESSING = NULL, PART_AMOUNT_PROCESSING = NULL",
                "4. Restart the service to reprocess all claims with real PDF processing",
            ]
        )

    return "\n".join(report_lines)


# Test helper functions (unchanged but enhanced)
def reset_audit_status_for_testing(
    claim_ids: List[int] = None, reset_amounts: bool = True
):
    """
    Helper function to reset audit status and processing amounts for testing purposes.
    """
    try:
        import db_handler

        with db_handler.DatabaseConnection("bgate") as connection:
            cursor = connection.cursor()

            if reset_amounts:
                reset_columns = """
                    AUDIT_STATUS = NULL,
                    LABOUR_AMOUNT_PROCESSING = NULL,
                    PART_AMOUNT_PROCESSING = NULL,
                """
            else:
                reset_columns = "AUDIT_STATUS = NULL,"

            if claim_ids:
                placeholders = ",".join([f":id{i}" for i in range(len(claim_ids))])
                query = f"""
                    UPDATE CLAIM_STATUS 
                    SET {reset_columns}
                        LAST_MODIFIED_DATE = CURRENT_TIMESTAMP
                    WHERE CLAIM_ID IN ({placeholders})
                """
                params = {f"id{i}": claim_id for i, claim_id in enumerate(claim_ids)}
                cursor.execute(query, params)
            else:
                query = f"""
                    UPDATE CLAIM_STATUS 
                    SET {reset_columns}
                        LAST_MODIFIED_DATE = CURRENT_TIMESTAMP
                    WHERE AUDIT_STATUS IS NOT NULL
                """
                cursor.execute(query)

            reset_count = cursor.rowcount
            connection.commit()
            cursor.close()

            logger.info(
                f"✅ Reset audit status {'and processing amounts ' if reset_amounts else ''}for {reset_count} claims"
            )
            return reset_count

    except Exception as e:
        logger.error(f"❌ Error resetting audit status: {e}")
        return 0


if __name__ == "__main__":
    """Test the enhanced matching functions"""

    print(f"🧪 Testing Enhanced Matching Functions - TEST MODE: {TEST_MODE_RANDOM}")
    print(f"Success Rate: {RANDOM_SUCCESS_RATE * 100}%")
    print("=" * 70)

    # Test data
    test_claim_data = [
        {
            "CLAIM_ID": 12345,
            "CLAIM_NO": "BYDAMEBR1234WCN123456_01",
            "VIN": "LGXCE4CC1S0123456",
            "LABOUR_AMOUNT_DMS": 1500.00,
            "PART_AMOUNT_DMS": 2500.00,
        },
        {
            "CLAIM_ID": 12346,
            "CLAIM_NO": "BYDAMEBR1235WCN123457_01",
            "VIN": "LGXCE4CC2S0123457",
            "LABOUR_AMOUNT_DMS": 800.00,
            "PART_AMOUNT_DMS": 1200.00,
        },
    ]

    # Enhanced processing results using new format
    test_processing_results = {}
    for claim in test_claim_data:
        claim_id = claim["CLAIM_ID"]
        file_paths = [f"test_file_{claim_id}_1.pdf", f"test_file_{claim_id}_2.pdf"]

        test_processing_results[claim_id] = get_enhanced_processing_results_for_claim(
            claim_id, file_paths
        )

    # Test configuration
    test_config = {
        "audit_matching": {
            "max_claims_per_batch": 20,
            "exact_amount_match": True,
            "tolerance_percentage": 2.0,
        }
    }

    # Run tests
    print("Testing enhanced configuration validation...")
    if validate_matching_config(test_config):
        print("✅ Configuration validation passed")

    print("\nTesting enhanced batch matching...")
    results = batch_match_claims(test_claim_data, test_processing_results, test_config)

    # Generate enhanced report
    print("\n" + generate_matching_report(results))

    print("\n💡 Enhanced features:")
    print("   - Individual file processing support")
    print("   - Claim number and VIN validation")
    print("   - Enhanced amount extraction by document type")
    print("   - Improved error reporting and validation")
    print("   - Backward compatibility maintained")
