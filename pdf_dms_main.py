#!/usr/bin/env python3
"""
PDF DMS Main Service
Main entry point for the PDF processing service
"""

import logging
import time
import yaml
import schedule
from datetime import datetime
from pathlib import Path

from downloader import ClaimDownloader
from pdf_processing import PDFProcessor
from matching_invoices import InvoiceMatcher
from database_ops import DatabaseOps

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("pdf_dms_service.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


class PDFDMSService:
    """Main service class that orchestrates the PDF processing pipeline"""
    
    def __init__(self, config_file="config.yaml"):
        """Initialize the service with configuration"""
        self.config = self.load_config(config_file)
        self.db_ops = DatabaseOps(self.config)
        self.downloader = ClaimDownloader(self.config, self.db_ops)
        self.pdf_processor = PDFProcessor(self.config, self.db_ops)
        self.matcher = InvoiceMatcher(self.config, self.db_ops)
        
        logger.info("PDF DMS Service initialized")
    
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
    
    def run_full_pipeline(self):
        """Run the complete pipeline: download -> process -> match"""
        start_time = datetime.now()
        logger.info("=" * 60)
        logger.info(f"🚀 Starting full pipeline run at {start_time}")
        logger.info("=" * 60)
        
        try:
            # Step 1: Check and download new/updated claims
            logger.info("📥 Step 1: Downloading claims...")
            download_results = self.downloader.download_new_claims()
            
            if download_results.get('claims_processed', 0) > 0:
                logger.info(f"✅ Downloaded files for {download_results['claims_processed']} claims")
            else:
                logger.info("ℹ️ No new claims to download")
            
            # Step 2: Process PDFs
            logger.info("🔄 Step 2: Processing PDFs...")
            processing_results = self.pdf_processor.process_pending_claims()
            
            if processing_results.get('claims_processed', 0) > 0:
                logger.info(f"✅ Processed {processing_results['claims_processed']} claims")
            else:
                logger.info("ℹ️ No claims ready for processing")
            
            # Step 3: Match invoices
            logger.info("🎯 Step 3: Matching invoices...")
            matching_results = self.matcher.match_pending_claims()
            
            if matching_results.get('claims_processed', 0) > 0:
                logger.info(f"✅ Matched {matching_results['claims_processed']} claims")
            else:
                logger.info("ℹ️ No claims ready for matching")
            
            # Summary
            duration = datetime.now() - start_time
            logger.info("=" * 60)
            logger.info(f"✅ Pipeline completed in {duration}")
            logger.info(f"📊 Summary: {download_results.get('claims_processed', 0)} downloaded, "
                       f"{processing_results.get('claims_processed', 0)} processed, "
                       f"{matching_results.get('claims_processed', 0)} matched")
            logger.info("=" * 60)
            
            return {
                'success': True,
                'duration': duration.total_seconds(),
                'downloaded': download_results.get('claims_processed', 0),
                'processed': processing_results.get('claims_processed', 0),
                'matched': matching_results.get('claims_processed', 0)
            }
            
        except Exception as e:
            logger.error(f"❌ Pipeline failed: {e}")
            return {'success': False, 'error': str(e)}
    
    def setup_scheduler(self):
        """Setup scheduled execution based on configuration"""
        schedule_config = self.config.get('scheduling', {})
        
        if schedule_config.get('enabled', True):
            interval_minutes = schedule_config.get('interval_minutes', 60)
            
            logger.info(f"🕐 Scheduling pipeline to run every {interval_minutes} minutes")
            schedule.every(interval_minutes).minutes.do(self.run_full_pipeline)
            
            # Optional: Run immediately on startup
            if schedule_config.get('run_on_startup', True):
                logger.info("🚀 Running initial pipeline execution...")
                self.run_full_pipeline()
        else:
            logger.info("⏸️ Scheduling disabled in configuration")
    
    def run_scheduler(self):
        """Run the scheduler loop"""
        logger.info("📅 Starting scheduler...")
        
        try:
            while True:
                schedule.run_pending()
                time.sleep(10)  # Check every 10 seconds
                
        except KeyboardInterrupt:
            logger.info("⏹️ Received shutdown signal")
        except Exception as e:
            logger.error(f"❌ Scheduler error: {e}")
        finally:
            logger.info("🛑 Service shutting down...")
            self.cleanup()
    
    def cleanup(self):
        """Cleanup resources"""
        try:
            self.db_ops.cleanup()
            logger.info("✅ Cleanup completed")
        except Exception as e:
            logger.error(f"❌ Cleanup error: {e}")


def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='PDF DMS Service')
    parser.add_argument('--config', default='config.yaml', help='Configuration file path')
    parser.add_argument('--run-once', action='store_true', help='Run pipeline once and exit')
    parser.add_argument('--environment', choices=['local', 'uat', 'prod'], 
                       default='local', help='Environment mode')
    
    args = parser.parse_args()
    
    try:
        # Initialize service
        service = PDFDMSService(args.config)
        
        # Set environment mode in database operations
        service.db_ops.set_environment_mode(args.environment)
        
        if args.run_once:
            # Run once and exit
            logger.info("🎯 Running pipeline once...")
            result = service.run_full_pipeline()
            if result['success']:
                logger.info("✅ Single run completed successfully")
                exit(0)
            else:
                logger.error("❌ Single run failed")
                exit(1)
        else:
            # Setup and run scheduler
            service.setup_scheduler()
            service.run_scheduler()
            
    except KeyboardInterrupt:
        logger.info("👋 Service interrupted by user")
    except Exception as e:
        logger.error(f"❌ Service failed: {e}")
        exit(1)


if __name__ == "__main__":
    main()