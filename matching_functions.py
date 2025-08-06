"""
Mock Matching Functions for PDF Invoice Matching - TEST VERSION
This module contains the mock implementation with RANDOM results for testing.

IMPORTANT: This version randomly assigns COMPLETE/REJECTED for frontend testing.
When the real PDF processing is ready, replace this with the production version
and reset all AUDIT_STATUS to NULL to reprocess.
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


def match_invoices_with_dms_estimates(
    claim_id: int,
    labour_amount_dms: float,
    part_amount_dms: float,
    processing_results: Dict[str, Any],
    config: Dict | None = None,
) -> Tuple[bool, str, Dict[str, Any]]:
    """
    TEST VERSION - Returns random results for frontend testing.
    Updates LABOUR_AMOUNT_PROCESSING and PART_AMOUNT_PROCESSING in database.

    In production, this will:
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
    """

    logger.info(f"🔍 Starting invoice matching for CLAIM_ID {claim_id}")
    logger.info(f"   DMS Labour Amount: {labour_amount_dms}")
    logger.info(f"   DMS Part Amount: {part_amount_dms}")
    logger.info(f"   Processing results files: {len(processing_results)}")

    if TEST_MODE_RANDOM:
        logger.warning(
            f"⚠️ TEST MODE ACTIVE - Using random results for CLAIM_ID {claim_id}"
        )

    # Calculate total DMS amount
    total_dms_amount = (labour_amount_dms or 0) + (part_amount_dms or 0)

    # TEST MODE: Generate random but deterministic results
    if TEST_MODE_RANDOM:
        # Use claim_id as seed for consistent results per claim
        random.seed(claim_id)

        # Randomly decide if this claim passes or fails
        match_success = random.random() < RANDOM_SUCCESS_RATE

        if match_success:
            # SUCCESS: Use exact DMS amounts for processing amounts
            extracted_labour = labour_amount_dms or 0
            extracted_part = part_amount_dms or 0

            reason = f"✅ TEST MODE: Amounts match exactly (random success)"

            logger.info(f"📊 SUCCESS - Setting processing amounts equal to DMS amounts")

        else:
            # FAILURE: Generate random different amounts
            # Create significant variation for rejected claims
            variation = random.choice(
                [
                    random.uniform(0.5, 0.8),  # 20-50% less
                    random.uniform(1.2, 1.5),  # 20-50% more
                    random.uniform(0.3, 0.5),  # 50-70% less (major discrepancy)
                ]
            )

            # Randomize the labour/part split as well
            labour_ratio = random.uniform(0.2, 0.8)

            extracted_labour = total_dms_amount * labour_ratio * variation
            extracted_part = total_dms_amount * (1 - labour_ratio) * variation

            # Round to 2 decimal places for realism
            extracted_labour = round(extracted_labour, 2)
            extracted_part = round(extracted_part, 2)

            # Generate specific failure reasons
            failure_reasons = [
                "Labour amount mismatch",
                "Parts amount mismatch",
                "Total amount exceeds threshold",
                "Missing invoice data",
                "Multiple discrepancies found",
            ]
            specific_reason = random.choice(failure_reasons)
            reason = f"❌ TEST MODE: {specific_reason} (random failure)"

            logger.info(
                f"📊 FAILURE - Generated random processing amounts: Labour={extracted_labour:.2f}, Parts={extracted_part:.2f}"
            )

        # Update the database with processing amounts
        db_handler.update_processing_amounts(claim_id, extracted_labour, extracted_part)

    else:
        # Production mode - use the original mock logic
        mock_extracted_amounts = _mock_extract_amounts_from_pdfs(
            claim_id, processing_results, total_dms_amount
        )
        extracted_labour = mock_extracted_amounts["labour_amount"]
        extracted_part = mock_extracted_amounts["part_amount"]

        # Get tolerance from config
        tolerance_pct = 0.0
        if config and "audit_matching" in config:
            tolerance_pct = config["audit_matching"].get("tolerance_percentage", 0.0)

        tolerance = (
            total_dms_amount * (tolerance_pct / 100.0) if total_dms_amount > 0 else 0
        )

        # Compare amounts
        labour_match = _amounts_match(
            labour_amount_dms or 0,
            extracted_labour,
            tolerance / 2,
        )

        part_match = _amounts_match(part_amount_dms or 0, extracted_part, tolerance / 2)

        match_success = labour_match and part_match

        # Generate reason
        if match_success:
            reason = f"✅ Amounts match within tolerance ({tolerance_pct}%)"
        else:
            mismatches = []
            if not labour_match:
                mismatches.append("labour")
            if not part_match:
                mismatches.append("parts")
            reason = f"❌ Amount mismatch in: {', '.join(mismatches)}"

        # Update database with processing amounts (production mode)
        db_handler.update_processing_amounts(claim_id, extracted_labour, extracted_part)

    # Create detailed results
    details = {
        "test_mode": TEST_MODE_RANDOM,
        "dms_amounts": {
            "labour": labour_amount_dms,
            "parts": part_amount_dms,
            "total": total_dms_amount,
        },
        "extracted_amounts": {
            "labour_amount": extracted_labour,
            "part_amount": extracted_part,
            "total_amount": extracted_labour + extracted_part,
            "extraction_confidence": random.uniform(0.85, 0.99)
            if TEST_MODE_RANDOM
            else 0.95,
            "files_processed": len(processing_results),
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
        "processed_files": list(processing_results.keys()),
        "processing_timestamp": datetime.now().isoformat(),
    }

    logger.info(f"🔍 Matching result for CLAIM_ID {claim_id}: {reason}")

    if TEST_MODE_RANDOM:
        logger.info(f"📊 TEST RESULT - Success: {match_success}")
        logger.info(f"   DMS Total: {total_dms_amount:.2f}")
        logger.info(f"   Processing Total: {(extracted_labour + extracted_part):.2f}")
        logger.info(f"   Updated LABOUR_AMOUNT_PROCESSING: {extracted_labour:.2f}")
        logger.info(f"   Updated PART_AMOUNT_PROCESSING: {extracted_part:.2f}")

    return match_success, reason, details


def _mock_extract_amounts_from_pdfs(
    claim_id: int, processing_results: Dict[str, Any], target_total: float
) -> Dict[str, float]:
    """
    Mock function to simulate extracting financial amounts from PDF processing results.
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
        labour_ratio = random.uniform(0.3, 0.7)
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
    TEST VERSION - Produces random results for testing and updates processing amounts.
    """

    results = {}

    logger.info(f"🔍 Starting batch invoice matching for {len(claim_data_list)} claims")

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
                "details": {"error": str(e), "test_mode": TEST_MODE_RANDOM},
            }

    # Log summary
    successful_matches = sum(1 for r in results.values() if r["match_success"])
    failed_matches = len(results) - successful_matches

    logger.info(f"🔍 Batch matching completed:")
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


def get_mock_processing_results_for_claim(
    claim_id: int, file_paths: List[str]
) -> Dict[str, Any]:
    """
    Generate mock processing results for a specific claim's PDF files.
    Enhanced for test mode to provide more realistic data.
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
            "processing_status": "success"
            if random.random() > 0.1
            else "partial",  # 90% success
            "extracted_text_length": random.randint(500, 5000),
            "pages_processed": random.randint(1, 10),
            "confidence_score": random.uniform(0.7, 0.95),
            "test_mode": TEST_MODE_RANDOM,
        }

    return results


def validate_matching_config(config: Dict) -> bool:
    """
    Validate the audit matching configuration.
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
    Enhanced for test mode to clearly indicate test status.
    """

    if not matching_results:
        return "No matching results to report."

    total_claims = len(matching_results)
    successful_matches = sum(1 for r in matching_results.values() if r["match_success"])
    failed_matches = total_claims - successful_matches

    report_lines = [
        "📊 INVOICE MATCHING REPORT",
    ]

    if TEST_MODE_RANDOM:
        report_lines.extend(
            [
                "⚠️  TEST MODE ACTIVE - RANDOM RESULTS ⚠️",
                "Processing amounts have been updated in database",
                "=" * 50,
            ]
        )
    else:
        report_lines.append("=" * 50)

    report_lines.extend(
        [
            f"Total Claims Processed: {total_claims}",
            f"Successful Matches: {successful_matches} ({successful_matches / total_claims * 100:.1f}%)",
            f"Failed Matches: {failed_matches} ({failed_matches / total_claims * 100:.1f}%)",
            "",
            "DETAILED RESULTS:",
            "-" * 30,
        ]
    )

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
                    f"    Processing: Labour={extracted.get('labour_amount', 0):.2f}, Parts={extracted.get('part_amount', 0):.2f}"
                )

    if TEST_MODE_RANDOM:
        report_lines.extend(
            [
                "",
                "🔄 TO RESET FOR PRODUCTION:",
                "1. Set TEST_MODE_RANDOM = False in matching_functions.py",
                "2. Run SQL: UPDATE CLAIM_STATUS SET AUDIT_STATUS = NULL, LABOUR_AMOUNT_PROCESSING = NULL, PART_AMOUNT_PROCESSING = NULL",
                "3. Restart the service to reprocess all claims",
            ]
        )

    return "\n".join(report_lines)


# Test helper function to reset audit status for retesting
def reset_audit_status_for_testing(
    claim_ids: List[int] = None, reset_amounts: bool = True
):
    """
    Helper function to reset audit status and processing amounts for testing purposes.

    Args:
        claim_ids: List of claim IDs to reset, or None to reset all
        reset_amounts: Whether to also reset LABOUR_AMOUNT_PROCESSING and PART_AMOUNT_PROCESSING
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


def check_processing_amounts():
    """
    Helper function to check the current state of processing amounts in the database.
    Useful for verifying test results.
    """
    try:
        import db_handler

        with db_handler.DatabaseConnection("bgate") as connection:
            query = """
                SELECT 
                    AUDIT_STATUS,
                    COUNT(*) as count,
                    AVG(LABOUR_AMOUNT_DMS) as avg_labour_dms,
                    AVG(LABOUR_AMOUNT_PROCESSING) as avg_labour_proc,
                    AVG(PART_AMOUNT_DMS) as avg_part_dms,
                    AVG(PART_AMOUNT_PROCESSING) as avg_part_proc
                FROM CLAIM_STATUS
                WHERE AUDIT_STATUS IS NOT NULL
                GROUP BY AUDIT_STATUS
                ORDER BY AUDIT_STATUS
            """

            df = pd.read_sql(query, connection)

            print("\n📊 Processing Amounts Summary:")
            print("=" * 60)
            for _, row in df.iterrows():
                print(f"\n{row['AUDIT_STATUS']} Claims ({row['count']} total):")
                print(
                    f"  Avg Labour - DMS: {row['avg_labour_dms']:.2f}, Processing: {row['avg_labour_proc']:.2f}"
                )
                print(
                    f"  Avg Parts  - DMS: {row['avg_part_dms']:.2f}, Processing: {row['avg_part_proc']:.2f}"
                )

                if row["AUDIT_STATUS"] == "COMPLETE":
                    print(f"  ✅ Processing amounts should match DMS amounts")
                else:
                    print(f"  ❌ Processing amounts should be random/different")

            return df

    except Exception as e:
        logger.error(f"❌ Error checking processing amounts: {e}")
        return None


if __name__ == "__main__":
    """Test the mock matching functions"""

    print(f"🧪 Testing Matching Functions - TEST MODE: {TEST_MODE_RANDOM}")
    print(f"Success Rate: {RANDOM_SUCCESS_RATE * 100}%")
    print("-" * 50)

    # Test data
    test_claim_data = [
        {"CLAIM_ID": 12345, "LABOUR_AMOUNT_DMS": 1500.00, "PART_AMOUNT_DMS": 2500.00},
        {"CLAIM_ID": 12346, "LABOUR_AMOUNT_DMS": 800.00, "PART_AMOUNT_DMS": 1200.00},
        {"CLAIM_ID": 12347, "LABOUR_AMOUNT_DMS": 2000.00, "PART_AMOUNT_DMS": 3000.00},
        {"CLAIM_ID": 12348, "LABOUR_AMOUNT_DMS": 500.00, "PART_AMOUNT_DMS": 750.00},
        {"CLAIM_ID": 12349, "LABOUR_AMOUNT_DMS": 1200.00, "PART_AMOUNT_DMS": 1800.00},
    ]

    # Mock processing results
    test_processing_results = {}
    for claim in test_claim_data:
        claim_id = claim["CLAIM_ID"]
        test_processing_results[claim_id] = {
            f"hash{claim_id}_<invoice_{claim_id}.pdf>": {
                "file_name": f"invoice_{claim_id}.pdf"
            },
            f"hash{claim_id}_2<receipt_{claim_id}.pdf>": {
                "file_name": f"receipt_{claim_id}.pdf"
            },
        }

    # Test configuration
    test_config = {
        "audit_matching": {
            "max_claims_per_batch": 20,
            "exact_amount_match": True,
            "tolerance_percentage": 2.0,  # 2% tolerance
        }
    }

    # Run tests
    print("Testing configuration validation...")
    if validate_matching_config(test_config):
        print("✅ Configuration validation passed")

    print("\nTesting batch matching...")
    results = batch_match_claims(test_claim_data, test_processing_results, test_config)

    # Generate report
    print("\n" + generate_matching_report(results))

    # Show individual results with amounts
    print("\n📋 Individual Results with Processing Amounts:")
    for claim_id, result in results.items():
        status = "PASS" if result["match_success"] else "FAIL"
        details = result.get("details", {})
        extracted = details.get("extracted_amounts", {})
        print(f"  CLAIM {claim_id}: {status}")
        print(f"    Labour: {extracted.get('labour_amount', 0):.2f}")
        print(f"    Parts: {extracted.get('part_amount', 0):.2f}")

    print("\n💡 Note: Processing amounts have been updated in the database")
    print("   COMPLETE claims: LABOUR/PART_AMOUNT_PROCESSING = DMS amounts")
    print("   REJECTED claims: LABOUR/PART_AMOUNT_PROCESSING = random amounts")
