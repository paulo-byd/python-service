#!/usr/bin/env python3
"""
Mock Matching Functions for PDF Invoice Matching
This module contains the mock implementation of the invoice matching logic
that will be replaced with real implementation later.
"""

import json
import random
import logging
from pathlib import Path
from typing import Dict, List, Tuple, Any

logger = logging.getLogger(__name__)


def match_invoices_with_dms_estimates(
    claim_id: int,
    labour_amount_dms: float,
    part_amount_dms: float,
    processing_results: Dict[str, Any],
    config: Dict | None = None,
) -> Tuple[bool, str, Dict[str, Any]]:
    """
    Mock function to match invoice amounts from PDF processing with DMS estimates.

    In the future, this will:
    1. Extract financial information from the processing_results
    2. Compare LABOUR_AMOUNT and PART_AMOUNT from PDFs with DMS values
    3. Return match status and detailed breakdown

    Args:
        claim_id (int): The claim ID being processed
        labour_amount_dms (float): Labour amount from DMS system
        part_amount_dms (float): Part amount from DMS system
        processing_results (Dict): Results from run_batch_processing() for this claim's PDFs
        config (Dict): Configuration dictionary (optional)

    Returns:
        Tuple[bool, str, Dict]: (match_success, reason, details)
            - match_success: True if amounts match, False otherwise
            - reason: Human-readable reason for the result
            - details: Dictionary with detailed breakdown of the matching
    """

    logger.info(f"🔍 Starting invoice matching for CLAIM_ID {claim_id}")
    logger.info(f"   DMS Labour Amount: {labour_amount_dms}")
    logger.info(f"   DMS Part Amount: {part_amount_dms}")
    logger.info(f"   Processing results files: {len(processing_results)}")

    # Mock implementation - simulate different scenarios

    # Calculate total DMS amount
    total_dms_amount = (labour_amount_dms or 0) + (part_amount_dms or 0)

    # Mock: Extract amounts from processing results
    # In reality, this would parse the actual PDF content
    mock_extracted_amounts = _mock_extract_amounts_from_pdfs(
        claim_id, processing_results, total_dms_amount
    )

    # Get tolerance from config
    tolerance_pct = 0.0  # Default to exact match
    if config and "audit_matching" in config:
        tolerance_pct = config["audit_matching"].get("tolerance_percentage", 0.0)

    # Calculate tolerance
    tolerance = (
        total_dms_amount * (tolerance_pct / 100.0) if total_dms_amount > 0 else 0
    )

    # Compare amounts
    labour_match = _amounts_match(
        labour_amount_dms or 0,
        mock_extracted_amounts["labour_amount"],
        tolerance / 2,  # Split tolerance between labour and parts
    )

    part_match = _amounts_match(
        part_amount_dms or 0, mock_extracted_amounts["part_amount"], tolerance / 2
    )

    overall_match = labour_match and part_match

    # Create detailed results
    details = {
        "dms_amounts": {
            "labour": labour_amount_dms,
            "parts": part_amount_dms,
            "total": total_dms_amount,
        },
        "extracted_amounts": mock_extracted_amounts,
        "matching_results": {
            "labour_match": labour_match,
            "part_match": part_match,
            "overall_match": overall_match,
            "tolerance_used": tolerance_pct,
        },
        "processed_files": list(processing_results.keys()),
        "processing_timestamp": pd.Timestamp.now().isoformat(),
    }

    # Generate reason
    if overall_match:
        reason = f"✅ Amounts match within tolerance ({tolerance_pct}%)"
    else:
        mismatches = []
        if not labour_match:
            mismatches.append("labour")
        if not part_match:
            mismatches.append("parts")
        reason = f"❌ Amount mismatch in: {', '.join(mismatches)}"

    logger.info(f"🔍 Matching result for CLAIM_ID {claim_id}: {reason}")

    return overall_match, reason, details


def _mock_extract_amounts_from_pdfs(
    claim_id: int, processing_results: Dict[str, Any], target_total: float
) -> Dict[str, float]:
    """
    Mock function to simulate extracting financial amounts from PDF processing results.

    In reality, this would:
    1. Parse the processing_results to extract text/structured data from PDFs
    2. Use regex/NLP to find labour and part amounts
    3. Handle multiple invoices and sum them up
    4. Deal with different PDF formats and languages

    For now, it generates realistic mock data based on the target amounts.
    """

    # Simulate different scenarios based on claim_id
    random.seed(claim_id)  # Deterministic results for testing

    scenario = random.choice(
        [
            "exact_match",  # 60% chance
            "exact_match",
            "exact_match",
            "close_match",  # 20% chance
            "significant_diff",  # 15% chance
            "no_amounts_found",  # 5% chance
        ]
    )

    if scenario == "exact_match":
        # Perfect match
        labour_ratio = random.uniform(0.3, 0.7)  # Labour typically 30-70% of total
        labour_amount = target_total * labour_ratio
        part_amount = target_total - labour_amount

    elif scenario == "close_match":
        # Close but not exact (within 5%)
        variation = random.uniform(0.95, 1.05)
        adjusted_total = target_total * variation
        labour_ratio = random.uniform(0.3, 0.7)
        labour_amount = adjusted_total * labour_ratio
        part_amount = adjusted_total - labour_amount

    elif scenario == "significant_diff":
        # Significant difference (10-30% off)
        variation = random.choice(
            [
                random.uniform(0.7, 0.9),  # 10-30% less
                random.uniform(1.1, 1.3),  # 10-30% more
            ]
        )
        adjusted_total = target_total * variation
        labour_ratio = random.uniform(0.3, 0.7)
        labour_amount = adjusted_total * labour_ratio
        part_amount = adjusted_total - labour_amount

    else:  # no_amounts_found
        # Simulate OCR/processing failure
        labour_amount = 0.0
        part_amount = 0.0

    # Add some realistic noise
    if labour_amount > 0:
        labour_amount = round(labour_amount + random.uniform(-0.50, 0.50), 2)
    if part_amount > 0:
        part_amount = round(part_amount + random.uniform(-0.50, 0.50), 2)

    extracted_amounts = {
        "labour_amount": labour_amount,
        "part_amount": part_amount,
        "total_amount": labour_amount + part_amount,
        "extraction_confidence": random.uniform(0.7, 0.95),
        "files_processed": len(processing_results),
        "scenario_used": scenario,  # For debugging
    }

    logger.debug(f"Mock extracted amounts for CLAIM_ID {claim_id}: {extracted_amounts}")

    return extracted_amounts


def _amounts_match(amount1: float, amount2: float, tolerance: float = 0.0) -> bool:
    """
    Check if two amounts match within the specified tolerance.

    Args:
        amount1 (float): First amount
        amount2 (float): Second amount
        tolerance (float): Absolute tolerance allowed

    Returns:
        bool: True if amounts match within tolerance
    """
    if amount1 == 0 and amount2 == 0:
        return True

    diff = abs(amount1 - amount2)
    return diff <= tolerance


def batch_match_claims(
    claim_data_list: List[Dict], processing_results_dict: Dict, config: Dict = None
) -> Dict[int, Dict]:
    """
    Process multiple claims for invoice matching in batch.

    Args:
        claim_data_list (List[Dict]): List of claim dictionaries with keys:
            - CLAIM_ID, LABOUR_AMOUNT_DMS, PART_AMOUNT_DMS
        processing_results_dict (Dict): Dictionary mapping claim_id to processing results
        config (Dict): Configuration dictionary

    Returns:
        Dict[int, Dict]: Dictionary mapping claim_id to matching results
    """

    results = {}

    logger.info(f"🔍 Starting batch invoice matching for {len(claim_data_list)} claims")

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
                "details": {"error": "No processing results found"},
            }
            continue

        # Perform the matching
        try:
            match_success, reason, details = match_invoices_with_dms_estimates(
                claim_id, labour_amount_dms, part_amount_dms, processing_results, config
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
                "details": {"error": str(e)},
            }

    # Log summary
    successful_matches = sum(1 for r in results.values() if r["match_success"])
    logger.info(
        f"🔍 Batch matching completed: {successful_matches}/{len(claim_data_list)} successful matches"
    )

    return results


# Import pandas for timestamp functionality
import pandas as pd


def get_mock_processing_results_for_claim(
    claim_id: int, file_paths: List[str]
) -> Dict[str, Any]:
    """
    Generate mock processing results for a specific claim's PDF files.
    This simulates what run_batch_processing() would return for this claim.

    Args:
        claim_id (int): The claim ID
        file_paths (List[str]): List of PDF file paths for this claim

    Returns:
        Dict[str, Any]: Mock processing results in the same format as run_batch_processing()
    """

    results = {}

    for file_path in file_paths:
        file_path_obj = Path(file_path)

        # Generate a hash-like key similar to the real processing function
        import hashlib

        file_path_hash = hashlib.sha256(str(file_path_obj).encode()).hexdigest()[:16]
        key = f"{file_path_hash}_<{file_path_obj.name}>"

        # Mock processing result for this file
        results[key] = {
            "file_name": file_path_obj.name,
            "claim_id": claim_id,
            "processing_status": "success",
            "extracted_text_length": random.randint(500, 5000),
            "pages_processed": random.randint(1, 10),
            "confidence_score": random.uniform(0.7, 0.95),
        }

    return results


def validate_matching_config(config: Dict) -> bool:
    """
    Validate the audit matching configuration.

    Args:
        config (Dict): Configuration dictionary

    Returns:
        bool: True if configuration is valid
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

    logger.info("✅ Audit matching configuration validated successfully")
    return True


def generate_matching_report(matching_results: Dict[int, Dict]) -> str:
    """
    Generate a human-readable report of matching results.

    Args:
        matching_results (Dict[int, Dict]): Results from batch_match_claims()

    Returns:
        str: Formatted report
    """

    if not matching_results:
        return "No matching results to report."

    total_claims = len(matching_results)
    successful_matches = sum(1 for r in matching_results.values() if r["match_success"])
    failed_matches = total_claims - successful_matches

    report_lines = [
        "📊 INVOICE MATCHING REPORT",
        "=" * 50,
        f"Total Claims Processed: {total_claims}",
        f"Successful Matches: {successful_matches} ({successful_matches / total_claims * 100:.1f}%)",
        f"Failed Matches: {failed_matches} ({failed_matches / total_claims * 100:.1f}%)",
        "",
        "DETAILED RESULTS:",
        "-" * 30,
    ]

    # Sort by claim_id for consistent reporting
    for claim_id in sorted(matching_results.keys()):
        result = matching_results[claim_id]
        status_icon = "✅" if result["match_success"] else "❌"

        report_lines.append(f"{status_icon} CLAIM_ID {claim_id}: {result['reason']}")

        # Add details for failed matches
        if not result["match_success"] and "details" in result:
            details = result["details"]
            if "dms_amounts" in details and "extracted_amounts" in details:
                dms = details["dms_amounts"]
                extracted = details["extracted_amounts"]
                report_lines.append(
                    f"    DMS: Labour={dms.get('labour', 0):.2f}, Parts={dms.get('parts', 0):.2f}"
                )
                report_lines.append(
                    f"    PDF: Labour={extracted.get('labour_amount', 0):.2f}, Parts={extracted.get('part_amount', 0):.2f}"
                )

    return "\n".join(report_lines)


if __name__ == "__main__":
    """Test the mock matching functions"""

    # Test data
    test_claim_data = [
        {"CLAIM_ID": 12345, "LABOUR_AMOUNT_DMS": 1500.00, "PART_AMOUNT_DMS": 2500.00},
        {"CLAIM_ID": 12346, "LABOUR_AMOUNT_DMS": 800.00, "PART_AMOUNT_DMS": 1200.00},
    ]

    # Mock processing results
    test_processing_results = {
        12345: {
            "hash1_<invoice1.pdf>": {"file_name": "invoice1.pdf"},
            "hash2_<invoice2.pdf>": {"file_name": "invoice2.pdf"},
        },
        12346: {"hash3_<invoice3.pdf>": {"file_name": "invoice3.pdf"}},
    }

    # Test configuration
    test_config = {
        "audit_matching": {
            "max_claims_per_batch": 20,
            "exact_amount_match": True,
            "tolerance_percentage": 0.0,
        }
    }

    # Run tests
    print("Testing mock matching functions...")

    # Validate config
    if validate_matching_config(test_config):
        print("✅ Configuration validation passed")

    # Test batch matching
    results = batch_match_claims(test_claim_data, test_processing_results, test_config)

    # Generate report
    report = generate_matching_report(results)
    print("\n" + report)
