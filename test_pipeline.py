#!/usr/bin/env python3
"""
Test Pipeline Script

Usage: python test_pipeline.py --claims X [--environment ENV]

This script tests the PDF processing and matching pipeline by:
1. Getting X claims that have downloaded files
2. Processing each claim's PDF files 
3. Running the matching logic
4. Showing results and statistics

This allows testing the integration between processing and matching
without running the full scheduler or download process.
"""

import argparse
import logging
import yaml
from datetime import datetime
from pathlib import Path

from database_ops import DatabaseOps
from pdf_processing import PDFProcessor
from matching_invoices import InvoiceMatcher

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)


class PipelineTester:
    """Test harness for PDF processing and matching pipeline"""
    
    def __init__(self, config_file="config.yaml", environment="local"):
        """Initialize tester with configuration"""
        self.config = self.load_config(config_file)
        self.environment = environment
        
        # Initialize components
        self.db_ops = DatabaseOps(self.config)
        self.db_ops.set_environment_mode(environment)
        
        self.pdf_processor = PDFProcessor(self.config, self.db_ops)
        self.matcher = InvoiceMatcher(self.config, self.db_ops)
        
        logger.info(f"Pipeline tester initialized for environment: {environment}")
    
    def load_config(self, config_file):
        """Load configuration from YAML file"""
        try:
            with open(config_file, 'r') as f:
                config = yaml.safe_load(f)
            logger.info(f"Configuration loaded from {config_file}")
            return config
        except Exception as e:
            logger.error(f"Failed to load config: {e}")
            raise
    
    def get_test_claims(self, max_claims: int):
        """Get claims that have downloaded files for testing"""
        logger.info(f"🔍 Finding {max_claims} claims with downloaded files for testing...")
        
        try:
            with self.db_ops.get_bgate_connection() as bgate_conn:
                query = """
                    SELECT DISTINCT 
                        cs.CLAIM_ID,
                        cs.CLAIM_NO,
                        cs.VIN,
                        cs.DEALER_CODE,
                        cs.LABOUR_AMOUNT_DMS,
                        cs.PART_AMOUNT_DMS,
                        cs.ATTACHMENT_STATUS,
                        cs.PROCESSING_STATUS,
                        cs.AUDIT_STATUS,
                        COUNT(pdf.FILE_ID) as FILE_COUNT
                    FROM CLAIM_STATUS cs
                    JOIN PDF_DOWNLOAD_DMS_CLAIMS pdf ON cs.CLAIM_ID = pdf.CLAIM_ID
                    WHERE pdf.STATUS = 'SUCCESS'
                    AND pdf.IS_LATEST_VERSION = 'Y'
                    AND cs.ATTACHMENT_STATUS = 'COMPLETE'
                    GROUP BY cs.CLAIM_ID, cs.CLAIM_NO, cs.VIN, cs.DEALER_CODE, 
                             cs.LABOUR_AMOUNT_DMS, cs.PART_AMOUNT_DMS, 
                             cs.ATTACHMENT_STATUS, cs.PROCESSING_STATUS, cs.AUDIT_STATUS
                    ORDER BY cs.LAST_DMS_UPDATE_DATE DESC
                    FETCH FIRST :max_claims ROWS ONLY
                """
                
                import pandas as pd
                claims_df = pd.read_sql(query, bgate_conn, params={'max_claims': max_claims}) # type: ignore
                
                if claims_df.empty:
                    logger.warning("❌ No claims with downloaded files found for testing")
                    return claims_df
                
                logger.info(f"✅ Found {len(claims_df)} claims for testing")
                
                # Show claim details
                for _, claim in claims_df.iterrows():
                    logger.info(f"   📋 CLAIM_ID {claim['CLAIM_ID']}: "
                               f"{claim['FILE_COUNT']} files, "
                               f"Processing: {claim['PROCESSING_STATUS'] or 'PENDING'}, "
                               f"Audit: {claim['AUDIT_STATUS'] or 'PENDING'}")
                
                return claims_df
                
        except Exception as e:
            logger.error(f"Error getting test claims: {e}")
            return pd.DataFrame()
    
    def test_processing_only(self, max_claims: int):
        """Test only the PDF processing pipeline"""
        logger.info("🔄 Testing PDF Processing Pipeline Only")
        logger.info("=" * 60)
        
        claims_df = self.get_test_claims(max_claims)
        if claims_df.empty:
            return {'success': False, 'error': 'No test claims found'}
        
        # Reset processing status for testing
        self.reset_processing_status(claims_df['CLAIM_ID'].tolist())
        
        # Run processing
        start_time = datetime.now()
        processing_results = self.pdf_processor.process_pending_claims()
        processing_time = datetime.now() - start_time
        
        logger.info("=" * 60)
        logger.info(f"📊 Processing Test Results:")
        logger.info(f"   Claims processed: {processing_results.get('claims_processed', 0)}")
        logger.info(f"   Processing time: {processing_time}")
        
        return {
            'success': True,
            'claims_processed': processing_results.get('claims_processed', 0),
            'processing_time': processing_time.total_seconds()
        }
    
    def test_matching_only(self, max_claims: int):
        """Test only the invoice matching pipeline"""
        logger.info("🎯 Testing Invoice Matching Pipeline Only")
        logger.info("=" * 60)
        
        # Get claims that are ready for matching (already processed)
        claims_df = self.db_ops.get_claims_ready_for_matching()
        if claims_df.empty:
            logger.warning("❌ No claims ready for matching found")
            return {'success': False, 'error': 'No claims ready for matching'}
        
        # Limit to requested number
        test_claims = claims_df.head(max_claims)
        
        # Reset audit status for testing
        self.reset_audit_status(test_claims['CLAIM_ID'].tolist())
        
        # Run matching
        start_time = datetime.now()
        matching_results = self.matcher.match_pending_claims()
        matching_time = datetime.now() - start_time
        
        logger.info("=" * 60)
        logger.info(f"📊 Matching Test Results:")
        logger.info(f"   Claims processed: {matching_results.get('claims_processed', 0)}")
        logger.info(f"   Claims matched: {matching_results.get('claims_matched', 0)}")
        logger.info(f"   Claims rejected: {matching_results.get('claims_rejected', 0)}")
        logger.info(f"   Matching time: {matching_time}")
        
        if matching_results.get('claims_processed', 0) > 0:
            match_rate = (matching_results.get('claims_matched', 0) / matching_results['claims_processed']) * 100
            logger.info(f"   Success rate: {match_rate:.1f}%")
        
        return {
            'success': True,
            'claims_processed': matching_results.get('claims_processed', 0),
            'claims_matched': matching_results.get('claims_matched', 0),
            'claims_rejected': matching_results.get('claims_rejected', 0),
            'matching_time': matching_time.total_seconds()
        }
    
    def test_full_pipeline(self, max_claims: int):
        """Test the complete processing + matching pipeline"""
        logger.info("🚀 Testing Full Pipeline (Processing + Matching)")
        logger.info("=" * 60)
        
        claims_df = self.get_test_claims(max_claims)
        if claims_df.empty:
            return {'success': False, 'error': 'No test claims found'}
        
        claim_ids = claims_df['CLAIM_ID'].tolist()
        
        # Reset statuses for clean testing
        self.reset_processing_status(claim_ids)
        self.reset_audit_status(claim_ids)
        
        start_time = datetime.now()
        
        # Step 1: Process PDFs
        logger.info("🔄 Step 1: Processing PDFs...")
        processing_results = self.pdf_processor.process_pending_claims()
        
        processing_completed = processing_results.get('claims_processed', 0)
        logger.info(f"   ✅ Processing completed: {processing_completed} claims")
        
        if processing_completed == 0:
            logger.warning("❌ No claims were processed, skipping matching")
            return {
                'success': False,
                'error': 'No claims processed',
                'claims_processed': 0,
                'claims_matched': 0
            }
        
        # Step 2: Match invoices
        logger.info("🎯 Step 2: Matching invoices...")
        matching_results = self.matcher.match_pending_claims()
        
        matching_completed = matching_results.get('claims_processed', 0)
        matching_successful = matching_results.get('claims_matched', 0)
        matching_rejected = matching_results.get('claims_rejected', 0)
        
        total_time = datetime.now() - start_time
        
        # Results summary
        logger.info("=" * 60)
        logger.info(f"📊 Full Pipeline Test Results:")
        logger.info(f"   Total test time: {total_time}")
        logger.info(f"   Claims processed: {processing_completed}")
        logger.info(f"   Claims matched: {matching_successful}")
        logger.info(f"   Claims rejected: {matching_rejected}")
        
        if processing_completed > 0:
            processing_rate = (processing_completed / len(claim_ids)) * 100
            logger.info(f"   Processing success rate: {processing_rate:.1f}%")
        
        if matching_completed > 0:
            matching_rate = (matching_successful / matching_completed) * 100
            logger.info(f"   Matching success rate: {matching_rate:.1f}%")
        
        # End-to-end success rate
        if len(claim_ids) > 0:
            e2e_rate = (matching_successful / len(claim_ids)) * 100
            logger.info(f"   End-to-end success rate: {e2e_rate:.1f}%")
        
        return {
            'success': True,
            'total_time': total_time.total_seconds(),
            'claims_input': len(claim_ids),
            'claims_processed': processing_completed,
            'claims_matched': matching_successful,
            'claims_rejected': matching_rejected
        }
    
    def reset_processing_status(self, claim_ids: list):
        """Reset processing status for test claims"""
        try:
            with self.db_ops.get_bgate_connection() as bgate_conn:
                placeholders = ','.join([':id' + str(i) for i in range(len(claim_ids))])
                params = {f'id{i}': claim_id for i, claim_id in enumerate(claim_ids)}
                
                query = f"""
                    UPDATE CLAIM_STATUS 
                    SET PROCESSING_STATUS = NULL,
                        PROCESSING_DATE = NULL,
                        LABOUR_AMOUNT_PROCESSING = NULL,
                        PART_AMOUNT_PROCESSING = NULL,
                        LAST_MODIFIED_DATE = CURRENT_TIMESTAMP
                    WHERE CLAIM_ID IN ({placeholders})
                """
                
                cursor = bgate_conn.cursor()
                cursor.execute(query, params)
                updated_count = cursor.rowcount
                bgate_conn.commit()
                cursor.close()
                
                logger.info(f"🔄 Reset processing status for {updated_count} claims")
                
        except Exception as e:
            logger.error(f"Error resetting processing status: {e}")
    
    def reset_audit_status(self, claim_ids: list):
        """Reset audit status for test claims"""
        try:
            with self.db_ops.get_bgate_connection() as bgate_conn:
                placeholders = ','.join([':id' + str(i) for i in range(len(claim_ids))])
                params = {f'id{i}': claim_id for i, claim_id in enumerate(claim_ids)}
                
                query = f"""
                    UPDATE CLAIM_STATUS 
                    SET AUDIT_STATUS = NULL,
                        AUDIT_REASON = NULL,
                        AUDIT_DATE = NULL,
                        LAST_MODIFIED_DATE = CURRENT_TIMESTAMP
                    WHERE CLAIM_ID IN ({placeholders})
                """
                
                cursor = bgate_conn.cursor()
                cursor.execute(query, params)
                updated_count = cursor.rowcount
                bgate_conn.commit()
                cursor.close()
                
                logger.info(f"🔄 Reset audit status for {updated_count} claims")
                
        except Exception as e:
            logger.error(f"Error resetting audit status: {e}")
    
    def show_statistics(self):
        """Show current pipeline statistics"""
        logger.info("📊 Current Pipeline Statistics")
        logger.info("=" * 40)
        
        # Processing statistics
        proc_stats = self.pdf_processor.get_processing_statistics()
        logger.info("📄 Processing Statistics:")
        for status, count in proc_stats.items():
            if status != 'error':
                logger.info(f"   {status}: {count}")
        
        # Matching statistics  
        match_stats = self.matcher.get_matching_statistics()
        logger.info("🎯 Matching Statistics:")
        for status, count in match_stats.items():
            if status != 'error':
                logger.info(f"   {status}: {count}")


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description='Test PDF Processing Pipeline')
    parser.add_argument('--claims', type=int, required=True, 
                       help='Number of claims to test')
    parser.add_argument('--environment', choices=['local', 'uat', 'prod'], 
                       default='local', help='Environment mode')
    parser.add_argument('--config', default='config.yaml', 
                       help='Configuration file path')
    parser.add_argument('--test-type', choices=['processing', 'matching', 'full'], 
                       default='full', help='Type of test to run')
    parser.add_argument('--show-stats', action='store_true', 
                       help='Show current statistics only')
    
    args = parser.parse_args()
    
    try:
        # Initialize tester
        tester = PipelineTester(args.config, args.environment)
        
        if args.show_stats:
            tester.show_statistics()
            return
        
        logger.info(f"🧪 Starting pipeline test: {args.test_type} mode")
        logger.info(f"📋 Testing {args.claims} claims in {args.environment} environment")
        
        # Run appropriate test
        if args.test_type == 'processing':
            result = tester.test_processing_only(args.claims)
        elif args.test_type == 'matching':
            result = tester.test_matching_only(args.claims)
        else:  # full
            result = tester.test_full_pipeline(args.claims)
        
        # Show final result
        if result['success']:
            logger.info("✅ Test completed successfully!")
        else:
            logger.error(f"❌ Test failed: {result.get('error', 'Unknown error')}")
            exit(1)
            
    except KeyboardInterrupt:
        logger.info("👋 Test interrupted by user")
    except Exception as e:
        logger.error(f"❌ Test failed with error: {e}")
        exit(1)


if __name__ == "__main__":
    main()