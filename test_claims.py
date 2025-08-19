#!/usr/bin/env python3
"""
Test Claims Processing Script

Usage: python test_claims.py --claims X

This script tests the claim processing workflow by:
1. Getting X claims that need processing from the database
2. Processing each claim's PDF files individually (just like the main service)
3. Showing processing results and statistics

This allows testing the integration between python-service and ultra-arena-frk
without running the full scheduler.
"""

import argparse
import logging
import sys
import time
from datetime import datetime
from pathlib import Path

# Import our existing modules
import db_handler
from config_loader import load_and_validate_config, setup_environment
from process_pdf_dir import process_claim_pdfs_individually
from pdf_api_client import close_pdf_api_client
from audit_matcher import extract_amounts_from_processing_results

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)


def save_processing_results_to_database(claim_id: int, processing_results: dict, processing_time: float) -> bool:
    """
    Save processing results to database just like the production system does.
    
    Args:
        claim_id (int): The claim ID
        processing_results (dict): Results from process_claim_pdfs_individually()
        processing_time (float): Time taken to process the claim
        
    Returns:
        bool: True if saved successfully, False otherwise
    """
    try:
        logger.info(f"💾 Saving processing results to database for CLAIM_ID {claim_id}...")
        
        # Extract amounts from processing results (same logic as production)
        extracted_amounts = extract_amounts_from_processing_results(processing_results)
        
        # Update the processing amounts in the database
        db_handler.update_processing_amounts(
            claim_id,
            extracted_amounts["labour_amount"],
            extracted_amounts["part_amount"],
        )
        
        # Mark claim as ready for audit (same as production workflow)
        db_handler.update_audit_status(claim_id, "PENDING")
        
        logger.info(f"✅ Database updated successfully for CLAIM_ID {claim_id}")
        logger.info(f"   Labour amount: R$ {extracted_amounts['labour_amount']:,.2f}")
        logger.info(f"   Parts amount: R$ {extracted_amounts['part_amount']:,.2f}")
        logger.info(f"   Audit status: PENDING")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to save processing results to database for CLAIM_ID {claim_id}: {e}")
        return False


def get_test_claims(num_claims: int):
    """
    Get a specified number of claims that have PDF files ready for processing.
    
    Args:
        num_claims (int): Number of claims to retrieve for testing
        
    Returns:
        list: List of claim dictionaries with CLAIM_ID and basic info
    """
    logger.info(f"🔍 Getting {num_claims} test claims...")
    
    try:
        # Get claims ready for processing (have downloaded PDFs)
        claims_df = db_handler.get_claims_ready_for_processing()
        
        if claims_df.empty:
            logger.warning("No claims found with downloaded PDFs ready for processing")
            return []
        
        # Limit to requested number
        test_claims_df = claims_df.head(num_claims)
        
        # Convert to list of dictionaries for easier handling
        test_claims = []
        for _, row in test_claims_df.iterrows():
            test_claims.append({
                'CLAIM_ID': int(row['CLAIM_ID']),
                'CLAIM_NO': row.get('CLAIM_NO', 'N/A'),
                'VIN': row.get('VIN', 'N/A'),
                'DEALER_CODE': row.get('DEALER_CODE', 'N/A'),
                'ATTACHMENT_STATUS': row.get('ATTACHMENT_STATUS', 'N/A')
            })
        
        logger.info(f"✅ Found {len(test_claims)} claims ready for testing")
        return test_claims
        
    except Exception as e:
        logger.error(f"❌ Error getting test claims: {e}")
        return []


def process_single_test_claim(claim_info: dict, use_real_processing: bool = True):
    """
    Process a single claim's PDF files and return results.
    
    Args:
        claim_info (dict): Claim information with CLAIM_ID
        use_real_processing (bool): Whether to use real processing or mock
        
    Returns:
        dict: Processing results and statistics
    """
    claim_id = claim_info['CLAIM_ID']
    claim_no = claim_info['CLAIM_NO']
    
    logger.info(f"\n📄 Processing CLAIM_ID {claim_id} ({claim_no})...")
    
    start_time = time.time()
    
    try:
        # Get PDF files for this claim
        pdf_files = db_handler.get_claim_pdf_files(claim_id)
        
        if not pdf_files:
            logger.warning(f"No PDF files found for CLAIM_ID {claim_id}")
            return {
                'claim_id': claim_id,
                'claim_no': claim_no,
                'status': 'NO_FILES',
                'files_found': 0,
                'processing_time': time.time() - start_time,
                'error': 'No PDF files found'
            }
        
        # Check if files actually exist on disk
        existing_files = [f for f in pdf_files if Path(f).exists()]
        if len(existing_files) != len(pdf_files):
            logger.warning(f"Some files missing: {len(existing_files)}/{len(pdf_files)} found")
        
        if not existing_files:
            logger.error(f"No existing PDF files for CLAIM_ID {claim_id}")
            return {
                'claim_id': claim_id,
                'claim_no': claim_no,
                'status': 'FILES_MISSING',
                'files_found': len(pdf_files),
                'files_existing': 0,
                'processing_time': time.time() - start_time,
                'error': 'PDF files not found on disk'
            }
        
        logger.info(f"Found {len(existing_files)} PDF files for processing")
        
        # Process the claim's PDFs
        if use_real_processing:
            logger.info("🚀 Using REAL processing (ultra-arena-frk integration)")
            # This will use the real ultra-arena-frk processing when integrated
            processing_results = process_claim_pdfs_individually(claim_id, existing_files)
        else:
            logger.info("🧪 Using MOCK processing")
            # Force mock processing for testing
            from process_pdf_dir import _mock_process_claim_pdfs_individually
            processing_results = _mock_process_claim_pdfs_individually(claim_id, existing_files)
        
        processing_time = time.time() - start_time
        
        # Save processing results to database (just like production system)
        save_success = save_processing_results_to_database(claim_id, processing_results, processing_time)
        
        # Extract key metrics from results
        summary = processing_results.get('processing_summary', {})
        
        result = {
            'claim_id': claim_id,
            'claim_no': claim_no,
            'status': 'SUCCESS',
            'files_found': len(pdf_files),
            'files_existing': len(existing_files),
            'files_processed': processing_results.get('files_processed', 0),
            'successful_files': summary.get('successful_files', 0),
            'failed_files': summary.get('failed_files', 0),
            'processing_time': processing_time,
            'total_amount_all': summary.get('total_amount_all', 0.0),
            'total_amount_pecas': summary.get('total_amount_pecas', 0.0),
            'total_amount_mao_obra': summary.get('total_amount_mao_obra', 0.0),
            'claim_numbers_found': processing_results.get('consolidated_data', {}).get('claim_numbers_found', []),
            'chassis_found': processing_results.get('consolidated_data', {}).get('chassis_found', []),
            'processing_method': 'REAL' if use_real_processing else 'MOCK',
            'database_saved': save_success
        }
        
        # Save processing results to database (just like production system)
        save_success = save_processing_results_to_database(claim_id, processing_results, processing_time)
        
        result['database_saved'] = save_success
        
        logger.info(f"✅ CLAIM_ID {claim_id} processed successfully")
        logger.info(f"   Files: {result['successful_files']}/{result['files_existing']} successful")
        logger.info(f"   Total amount: R$ {result['total_amount_all']:,.2f}")
        logger.info(f"   Processing time: {result['processing_time']:.2f}s")
        logger.info(f"   Database saved: {'✅ Yes' if save_success else '❌ Failed'}")
        
        return result
        
    except Exception as e:
        processing_time = time.time() - start_time
        logger.error(f"❌ Error processing CLAIM_ID {claim_id}: {e}")
        
        return {
            'claim_id': claim_id,
            'claim_no': claim_no,
            'status': 'ERROR',
            'processing_time': processing_time,
            'error': str(e),
            'processing_method': 'REAL' if use_real_processing else 'MOCK',
            'database_saved': False
        }


def print_test_summary(results: list):
    """
    Print a summary of test results.
    
    Args:
        results (list): List of processing results from test claims
    """
    if not results:
        logger.warning("No results to summarize")
        return
    
    total_claims = len(results)
    successful_claims = len([r for r in results if r['status'] == 'SUCCESS'])
    error_claims = len([r for r in results if r['status'] == 'ERROR'])
    no_files_claims = len([r for r in results if r['status'] == 'NO_FILES'])
    missing_files_claims = len([r for r in results if r['status'] == 'FILES_MISSING'])
    
    total_processing_time = sum([r.get('processing_time', 0) for r in results])
    avg_processing_time = total_processing_time / total_claims if total_claims > 0 else 0
    
    total_files_processed = sum([r.get('files_processed', 0) for r in results if r['status'] == 'SUCCESS'])
    total_amount = sum([r.get('total_amount_all', 0) for r in results if r['status'] == 'SUCCESS'])
    database_saves = len([r for r in results if r.get('database_saved', False)])
    
    logger.info(f"\n📊 TEST SUMMARY")
    logger.info(f"=" * 50)
    logger.info(f"Total claims tested: {total_claims}")
    logger.info(f"Successful: {successful_claims}")
    logger.info(f"Errors: {error_claims}")
    logger.info(f"No files: {no_files_claims}")
    logger.info(f"Missing files: {missing_files_claims}")
    logger.info(f"Success rate: {(successful_claims/total_claims)*100:.1f}%")
    logger.info(f"")
    logger.info(f"Files processed: {total_files_processed}")
    logger.info(f"Total amount extracted: R$ {total_amount:,.2f}")
    logger.info(f"Average processing time: {avg_processing_time:.2f}s per claim")
    logger.info(f"Total processing time: {total_processing_time:.2f}s")
    logger.info(f"Database saves successful: {database_saves}/{total_claims}")
    
    # Show processing method breakdown
    real_processing = len([r for r in results if r.get('processing_method') == 'REAL'])
    mock_processing = len([r for r in results if r.get('processing_method') == 'MOCK'])
    
    if real_processing > 0:
        logger.info(f"")
        logger.info(f"Processing method:")
        logger.info(f"Real processing: {real_processing}")
        logger.info(f"Mock processing: {mock_processing}")
    
    # Show any database save failures
    failed_saves = [r for r in results if r['status'] == 'SUCCESS' and not r.get('database_saved', False)]
    if failed_saves:
        logger.warning(f"⚠️ Database save failures:")
        for result in failed_saves:
            logger.warning(f"   CLAIM_ID {result['claim_id']}: Database save failed")
    
    # Show database status for successful processing
    if database_saves > 0:
        logger.info(f"")
        logger.info(f"💾 Database Updates:")
        logger.info(f"Claims marked as PENDING for audit: {database_saves}")
        logger.info(f"Processing amounts saved in CLAIM_STATUS table: {database_saves}")
        if database_saves == successful_claims:
            logger.info(f"✅ All successful claims saved to database")
        else:
            logger.warning(f"⚠️ Some successful claims not saved to database")


def main():
    """
    Main function to run the test claims processing.
    """
    parser = argparse.ArgumentParser(description='Test Claims Processing Script')
    parser.add_argument(
        '--claims', 
        type=int, 
        required=True,
        help='Number of claims to test (e.g., --claims 5)'
    )
    parser.add_argument(
        '--mock',
        action='store_true',
        help='Use mock processing instead of real processing'
    )
    parser.add_argument(
        '--env',
        choices=['local', 'uat', 'prod'],
        default='local',
        help='Environment to use (default: local)'
    )
    
    args = parser.parse_args()
    
    if args.claims <= 0:
        logger.error("Number of claims must be greater than 0")
        sys.exit(1)
    
    if args.claims > 50:
        logger.warning(f"Testing {args.claims} claims - this might take a while...")
    
    logger.info(f"🧪 Starting test with {args.claims} claims")
    logger.info(f"Environment: {args.env}")
    logger.info(f"Processing mode: {'MOCK' if args.mock else 'REAL'}")
    logger.info(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    try:
        # Setup environment
        import os
        if args.env:
            os.environ['APP_ENV'] = args.env
        setup_environment()
        
        # Load configuration
        config = load_and_validate_config()
        
        # Initialize database connections
        db_handler.initialize_connection_pools(
            min_connections=2,
            max_connections=5,  # Fewer connections for testing
        )
        
        # Get test claims
        test_claims = get_test_claims(args.claims)
        
        if not test_claims:
            logger.error("No test claims found - exiting")
            sys.exit(1)
        
        logger.info(f"Processing {len(test_claims)} claims...")
        
        # Process each claim
        results = []
        for i, claim_info in enumerate(test_claims, 1):
            logger.info(f"\n🔄 Processing claim {i}/{len(test_claims)}")
            
            result = process_single_test_claim(
                claim_info, 
                use_real_processing=not args.mock
            )
            results.append(result)
            
            # Small delay between claims to avoid overwhelming the system
            if i < len(test_claims):
                time.sleep(0.5)
        
        # Print summary
        print_test_summary(results)
        
        logger.info(f"\n✅ Test completed successfully!")
        
    except KeyboardInterrupt:
        logger.info("\n👋 Test interrupted by user")
    except Exception as e:
        logger.error(f"\n🚨 Test failed: {e}")
        sys.exit(1)
    finally:
        # Cleanup
        try:
            db_handler.close_connection_pools()
            close_pdf_api_client()
        except Exception as cleanup_error:
            logger.error(f"Error during cleanup: {cleanup_error}")


if __name__ == "__main__":
    main()