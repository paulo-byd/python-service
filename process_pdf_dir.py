import hashlib
from pathlib import Path
import json
import random
import logging

logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).parent
TEST_INPUT_PDF_DIRECTORY = BASE_DIR / "test_pdf_dir"

# Configuration flag - set to False when the real PDF processor is ready
USE_MOCK_PROCESSING = True


# TODO: Change this for the real thing
def run_file_processing_simple(pdf_file_path: Path):
    """
    Process a single PDF file and extract information.

    MOCK VERSION: This will be replaced with the real PDF processing function.
    The real function should return JSON with the same structure.

    Args:
        pdf_file_path (Path): Path to the PDF file to process

    Returns:
        dict: Processing results with structure:
        {
            "file_stats": {
                "file_path": {
                    "file_model_output": {
                        "file_name_llm": "filename.pdf",
                        "DOC_TYPE": "Peças" or "Mão de Obra",
                        "CNPJ_1": "XX.XXX.XXX/XXXX-XX",
                        "CNPJ_2": null or "XX.XXX.XXX/XXXX-XX",
                        "VALOR_TOTAL": "X.XXX,XX",
                        "Chassi": "VIN_NUMBER",
                        "CLAIM_NUMBER": "CLAIM_NUMBER_FROM_PDF"
                    }
                }
            },
            "overall_stats": {...},
            "overall_cost": {...}
        }
    """

    if USE_MOCK_PROCESSING:
        return _mock_run_file_processing_simple(pdf_file_path)
    else:
        # This will be replaced with the real implementation
        # return real_run_file_processing_simple(pdf_file_path)
        raise NotImplementedError("Real PDF processing function not yet implemented")


def _mock_run_file_processing_simple(pdf_file_path: Path):
    """
    Mock implementation of run_file_processing_simple for testing.
    Generates realistic but fake data.
    """

    if not pdf_file_path.exists():
        raise FileNotFoundError(f"PDF file not found: {pdf_file_path}")

    # Use file path as seed for consistent results
    random.seed(hash(str(pdf_file_path)))

    # Extract potential claim info from filename
    filename = pdf_file_path.name

    # Try to extract claim ID from filename (format: CLAIM_XXX_YYY.pdf)
    claim_number = None
    chassi = None

    if "CLAIM_" in filename:
        # Extract claim number from filename
        parts = filename.split("_")
        if len(parts) >= 2:
            claim_number = "_".join(parts[1:3]).replace(".pdf", "")

    if not claim_number:
        # Generate a realistic claim number
        claim_number = f"BYDAMEBR{random.randint(1000, 9999)}WCN{random.randint(100000, 999999)}_01"

    # Generate realistic VIN/Chassi
    chassi = f"LGXCE4CC{random.randint(1, 9)}S{random.randint(1000000, 9999999)}"

    # Generate realistic amounts
    doc_types = ["Peças", "Mão de Obra", "Diversos"]
    doc_type = random.choice(doc_types)

    # Generate amount based on doc type
    if doc_type == "Peças":
        amount = random.uniform(500.00, 5000.00)
    elif doc_type == "Mão de Obra":
        amount = random.uniform(200.00, 2000.00)
    else:
        amount = random.uniform(50.00, 500.00)

    # Format amount as Brazilian currency (X.XXX,XX)
    valor_total = f"{amount:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

    # Generate realistic CNPJ
    cnpj_1 = f"{random.randint(10, 99)}.{random.randint(100, 999)}.{random.randint(100, 999)}/{random.randint(1000, 9999)}-{random.randint(10, 99)}"
    cnpj_2 = None
    if random.random() < 0.3:  # 30% chance of having a second CNPJ
        cnpj_2 = f"{random.randint(10, 99)}.{random.randint(100, 999)}.{random.randint(100, 999)}/{random.randint(1000, 9999)}-{random.randint(10, 99)}"

    # Simulate processing confidence
    confidence = random.uniform(0.8, 0.98)
    processing_time = random.uniform(2.5, 8.0)

    result = {
        "file_stats": {
            str(pdf_file_path): {
                "file_model_output": {
                    "file_name_llm": filename,
                    "DOC_TYPE": doc_type,
                    "CNPJ_1": cnpj_1,
                    "CNPJ_2": cnpj_2,
                    "VALOR_TOTAL": valor_total,
                    "Chassi": chassi,
                    "CLAIM_NUMBER": claim_number,
                }
            }
        },
        "overall_stats": {
            "files_processed": 1,
            "processing_time_seconds": processing_time,
            "confidence_score": confidence,
            "mock_data": USE_MOCK_PROCESSING,
        },
        "overall_cost": {
            "api_calls": 1,
            "estimated_cost_usd": random.uniform(0.01, 0.05),
            "model_used": "mock-gpt-4o-mini",
        },
    }

    logger.debug(
        f"Mock processed PDF: {filename} -> Claim: {claim_number}, Amount: {valor_total}"
    )

    return result


def run_batch_processing(input_pdf_dir_path: Path, pdf_file_paths: list = []):
    """
    Enhanced batch processing function that uses individual file processing.

    TRANSITION FUNCTION: This maintains compatibility with existing code
    while preparing for the new individual file processing approach.

    Args:
        input_pdf_dir_path (Path): The directory containing PDF files.
        pdf_file_paths (list): A list of specific PDF file paths to process.

    Returns:
        str: JSON string with enhanced processing results
    """

    if not pdf_file_paths:
        if not input_pdf_dir_path.exists():
            raise FileNotFoundError(
                f"The specified path '{input_pdf_dir_path}' does not exist."
            )
        # Get all PDFs in the directory and its subdirectories
        pdf_file_paths = list(input_pdf_dir_path.rglob("*.pdf"))

    logger.info(
        f"Processing {len(pdf_file_paths)} PDF files using {'MOCK' if USE_MOCK_PROCESSING else 'REAL'} processor"
    )

    # Enhanced results structure
    batch_results = {
        "files_processed": len(pdf_file_paths),
        "processing_results": {},
        "batch_summary": {
            "successful_files": 0,
            "failed_files": 0,
            "total_processing_time": 0.0,
            "average_confidence": 0.0,
        },
        "compatibility_results": {},  # For backward compatibility
    }

    total_confidence = 0.0
    successful_files = 0

    for pdf_file in pdf_file_paths:
        try:
            # Process individual file using the new function
            file_result = run_file_processing_simple(pdf_file)

            # Store the detailed results
            batch_results["processing_results"][str(pdf_file)] = file_result

            # Extract stats for batch summary
            if "overall_stats" in file_result:
                batch_results["batch_summary"]["total_processing_time"] += file_result[
                    "overall_stats"
                ].get("processing_time_seconds", 0)
                total_confidence += file_result["overall_stats"].get(
                    "confidence_score", 0
                )

            successful_files += 1

            # Maintain backward compatibility with old format
            file_path_hash = hashlib.sha256(str(pdf_file).encode()).hexdigest()[:16]
            key = f"{file_path_hash}_<{pdf_file.name}>"
            batch_results["compatibility_results"][key] = {
                "file_name": pdf_file.name,
                "processing_details": file_result,
            }

        except Exception as e:
            logger.error(f"Failed to process {pdf_file}: {e}")
            batch_results["batch_summary"]["failed_files"] += 1

    # Calculate final batch statistics
    batch_results["batch_summary"]["successful_files"] = successful_files
    batch_results["batch_summary"]["average_confidence"] = (
        total_confidence / successful_files if successful_files > 0 else 0.0
    )

    logger.info(
        f"Batch processing completed: {successful_files}/{len(pdf_file_paths)} files successful"
    )

    # Return JSON string for compatibility
    return json.dumps(batch_results, indent=4)


def process_claim_pdfs_individually(claim_id: int, pdf_file_paths: list):
    """
    Process all PDF files for a specific claim using individual file processing.

    Args:
        claim_id (int): The claim ID being processed
        pdf_file_paths (list): List of PDF file paths for this claim

    Returns:
        dict: Consolidated processing results for the claim
    """

    logger.info(f"Processing {len(pdf_file_paths)} PDF files for CLAIM_ID {claim_id}")

    claim_results = {
        "claim_id": claim_id,
        "files_processed": len(pdf_file_paths),
        "individual_file_results": {},
        "consolidated_data": {
            "claim_numbers_found": set(),
            "chassis_found": set(),
            "doc_types": {},
            "total_amounts": {},
            "cnpjs_found": set(),
        },
        "processing_summary": {
            "successful_files": 0,
            "failed_files": 0,
            "total_amount_pecas": 0.0,
            "total_amount_mao_obra": 0.0,
            "total_amount_diversos": 0.0,
            "total_amount_all": 0.0,
        },
    }

    for pdf_file_path in pdf_file_paths:
        try:
            # Process individual file
            file_result = run_file_processing_simple(Path(pdf_file_path))

            # Store individual result
            claim_results["individual_file_results"][str(pdf_file_path)] = file_result

            # Extract and consolidate data
            if "file_stats" in file_result:
                for file_path, file_data in file_result["file_stats"].items():
                    if "file_model_output" in file_data:
                        output = file_data["file_model_output"]

                        # Collect claim numbers and chassis
                        if output.get("CLAIM_NUMBER"):
                            claim_results["consolidated_data"][
                                "claim_numbers_found"
                            ].add(output["CLAIM_NUMBER"])

                        if output.get("Chassi"):
                            claim_results["consolidated_data"]["chassis_found"].add(
                                output["Chassi"]
                            )

                        # Collect CNPJs
                        if output.get("CNPJ_1"):
                            claim_results["consolidated_data"]["cnpjs_found"].add(
                                output["CNPJ_1"]
                            )
                        if output.get("CNPJ_2"):
                            claim_results["consolidated_data"]["cnpjs_found"].add(
                                output["CNPJ_2"]
                            )

                        # Process amounts by document type
                        doc_type = output.get("DOC_TYPE", "Diversos")
                        valor_total_str = output.get("VALOR_TOTAL", "0,00")

                        # Convert Brazilian format to float
                        try:
                            valor_total = float(
                                valor_total_str.replace(".", "").replace(",", ".")
                            )

                            # Add to doc type totals
                            if (
                                doc_type
                                not in claim_results["consolidated_data"]["doc_types"]
                            ):
                                claim_results["consolidated_data"]["doc_types"][
                                    doc_type
                                ] = 0.0
                            claim_results["consolidated_data"]["doc_types"][
                                doc_type
                            ] += valor_total

                            # Add to processing summary
                            if doc_type == "Peças":
                                claim_results["processing_summary"][
                                    "total_amount_pecas"
                                ] += valor_total
                            elif doc_type == "Mão de Obra":
                                claim_results["processing_summary"][
                                    "total_amount_mao_obra"
                                ] += valor_total
                            else:
                                claim_results["processing_summary"][
                                    "total_amount_diversos"
                                ] += valor_total

                            claim_results["processing_summary"]["total_amount_all"] += (
                                valor_total
                            )

                        except ValueError:
                            logger.warning(
                                f"Could not parse amount '{valor_total_str}' from {pdf_file_path}"
                            )

            claim_results["processing_summary"]["successful_files"] += 1

        except Exception as e:
            logger.error(
                f"Failed to process {pdf_file_path} for CLAIM_ID {claim_id}: {e}"
            )
            claim_results["processing_summary"]["failed_files"] += 1

    # Convert sets to lists for JSON serialization
    claim_results["consolidated_data"]["claim_numbers_found"] = list(
        claim_results["consolidated_data"]["claim_numbers_found"]
    )
    claim_results["consolidated_data"]["chassis_found"] = list(
        claim_results["consolidated_data"]["chassis_found"]
    )
    claim_results["consolidated_data"]["cnpjs_found"] = list(
        claim_results["consolidated_data"]["cnpjs_found"]
    )

    logger.info(f"CLAIM_ID {claim_id} processing completed:")
    logger.info(
        f"  Files: {claim_results['processing_summary']['successful_files']}/{len(pdf_file_paths)} successful"
    )
    logger.info(
        f"  Total amount: R$ {claim_results['processing_summary']['total_amount_all']:,.2f}"
    )
    logger.info(
        f"  Peças: R$ {claim_results['processing_summary']['total_amount_pecas']:,.2f}"
    )
    logger.info(
        f"  Mão de Obra: R$ {claim_results['processing_summary']['total_amount_mao_obra']:,.2f}"
    )

    return claim_results


if __name__ == "__main__":
    # Test the enhanced processing functions
    print("🧪 Testing Enhanced PDF Processing Functions")
    print("=" * 60)

    # Test individual file processing
    test_files = (
        list(TEST_INPUT_PDF_DIRECTORY.rglob("*.pdf"))
        if TEST_INPUT_PDF_DIRECTORY.exists()
        else []
    )

    if not test_files:
        # Create mock file paths for testing
        test_files = [
            Path("test_pdf_dir/CLAIM_12345_67890.pdf"),
            Path("test_pdf_dir/CLAIM_12345_67891.pdf"),
        ]
        print("⚠️ Using mock file paths for testing")

    # Test individual file processing
    if test_files:
        print(f"\n1. Testing individual file processing with {test_files[0].name}...")
        try:
            individual_result = _mock_run_file_processing_simple(test_files[0])
            print("✅ Individual file processing successful")
            print(
                f"   Claim Number: {individual_result['file_stats'][str(test_files[0])]['file_model_output']['CLAIM_NUMBER']}"
            )
            print(
                f"   Total Amount: {individual_result['file_stats'][str(test_files[0])]['file_model_output']['VALOR_TOTAL']}"
            )
        except Exception as e:
            print(f"❌ Individual file processing failed: {e}")

    # Test claim-based processing
    print(f"\n2. Testing claim-based processing with {len(test_files)} files...")
    try:
        claim_result = process_claim_pdfs_individually(
            12345, [str(f) for f in test_files]
        )
        print("✅ Claim-based processing successful")
        print(
            f"   Files processed: {claim_result['processing_summary']['successful_files']}"
        )
        print(
            f"   Total amount: R$ {claim_result['processing_summary']['total_amount_all']:,.2f}"
        )
        print(
            f"   Claim numbers found: {claim_result['consolidated_data']['claim_numbers_found']}"
        )
    except Exception as e:
        print(f"❌ Claim-based processing failed: {e}")

    # Test backward compatibility
    print(f"\n3. Testing backward compatibility...")
    try:
        batch_json = run_batch_processing(TEST_INPUT_PDF_DIRECTORY, test_files)
        batch_result = json.loads(batch_json)
        print("✅ Backward compatibility maintained")
        print(f"   Files processed: {batch_result['files_processed']}")
        print(f"   Successful: {batch_result['batch_summary']['successful_files']}")
        print(
            f"   Compatibility results: {len(batch_result['compatibility_results'])} entries"
        )
    except Exception as e:
        print(f"❌ Backward compatibility test failed: {e}")

    print(f"\n✅ Testing completed - Mock mode: {USE_MOCK_PROCESSING}")
    print("💡 To switch to real processing: Set USE_MOCK_PROCESSING = False")
