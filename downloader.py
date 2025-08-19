"""
downloader.py - Simplified Claim File Downloader
Downloads PDF files from DMS for claims that need processing
"""

import logging
import requests
import os
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional
import pandas as pd

logger = logging.getLogger(__name__)


class ClaimDownloader:
    """Downloads PDF files from DMS for claims that need processing"""
    
    def __init__(self, config: Dict, db_ops):
        """Initialize downloader with configuration and database operations"""
        self.config = config
        self.db_ops = db_ops
        self.download_config = config.get('download', {})
        
        # Handle storage path based on environment
        storage_path = self.download_config.get('storage_path', './pdf-claims')
        if self.db_ops.environment == 'local':
            # For local environment, use relative path from script location
            script_dir = Path(__file__).parent.absolute()
            self.storage_path = script_dir / "pdf-claims"
        else:
            # For other environments, use configured path
            self.storage_path = Path(storage_path)
        
        self.batch_size = self.download_config.get('batch_size', 20)
        
        # Create storage directory if it doesn't exist
        self.storage_path.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"Downloader initialized - storage: {self.storage_path}")
    
    def download_new_claims(self) -> Dict:
        """
        Main method to download files for new or updated claims
        Returns summary of download results
        """
        logger.info("🔍 Checking for claims that need file downloads...")
        
        try:
            # Get claims that need downloading
            claims_df = self.db_ops.get_claims_needing_download()
            
            if claims_df.empty:
                logger.info("✅ No claims need file downloads")
                return {'claims_processed': 0, 'files_downloaded': 0}
            
            logger.info(f"📥 Found {len(claims_df)} claims needing downloads")
            
            # Process claims in batches
            total_claims_processed = 0
            total_files_downloaded = 0
            
            for i in range(0, len(claims_df), self.batch_size):
                batch_claims = claims_df.iloc[i:i + self.batch_size]
                batch_num = (i // self.batch_size) + 1
                total_batches = (len(claims_df) + self.batch_size - 1) // self.batch_size
                
                logger.info(f"📦 Processing batch {batch_num}/{total_batches} ({len(batch_claims)} claims)")
                
                batch_results = self._process_claim_batch(batch_claims)
                
                total_claims_processed += batch_results['claims_processed']
                total_files_downloaded += batch_results['files_downloaded']
            
            logger.info(f"✅ Download completed: {total_claims_processed} claims, {total_files_downloaded} files")
            
            return {
                'claims_processed': total_claims_processed,
                'files_downloaded': total_files_downloaded
            }
            
        except Exception as e:
            logger.error(f"❌ Download process failed: {e}")
            return {'claims_processed': 0, 'files_downloaded': 0, 'error': str(e)}
    
    def _process_claim_batch(self, claims_df: pd.DataFrame) -> Dict:
        """Process a batch of claims for downloading"""
        claims_processed = 0
        files_downloaded = 0
        
        for _, claim_row in claims_df.iterrows():
            claim_id = int(claim_row['CLAIM_ID'])
            
            try:
                logger.info(f"📁 Processing CLAIM_ID {claim_id}")
                
                # Get files that need downloading for this claim
                files_df = self.db_ops.get_files_needing_download(claim_id)
                
                if files_df.empty:
                    logger.info(f"   No files to download for CLAIM_ID {claim_id}")
                    continue
                
                logger.info(f"   📄 Found {len(files_df)} files to download")
                
                # Download files for this claim
                claim_files_downloaded = self._download_claim_files(claim_id, files_df)
                
                # Update claim status after downloading
                if claim_files_downloaded > 0:
                    self.db_ops.update_claim_download_status(claim_id, claim_files_downloaded)
                    claims_processed += 1
                    files_downloaded += claim_files_downloaded
                    
                    logger.info(f"   ✅ Downloaded {claim_files_downloaded} files for CLAIM_ID {claim_id}")
                else:
                    logger.warning(f"   ⚠️ No files successfully downloaded for CLAIM_ID {claim_id}")
                
            except Exception as e:
                logger.error(f"   ❌ Error processing CLAIM_ID {claim_id}: {e}")
                continue
        
        return {'claims_processed': claims_processed, 'files_downloaded': files_downloaded}
    
    def _download_claim_files(self, claim_id: int, files_df: pd.DataFrame) -> int:
        """Download all files for a specific claim"""
        successful_downloads = 0
        
        # Create claim directory
        claim_dir = self.storage_path / str(claim_id)
        claim_dir.mkdir(parents=True, exist_ok=True)
        
        for _, file_row in files_df.iterrows():
            file_id = file_row['FILE_ID']
            file_name = file_row['FILE_NAME']
            create_date = file_row.get('CREATE_DATE')
            
            try:
                # Download the file
                success = self._download_single_file(
                    file_id=file_id,
                    file_name=file_name,
                    claim_id=claim_id,
                    target_dir=claim_dir,
                    create_date=create_date
                )
                
                if success:
                    successful_downloads += 1
                    # Update file download status in database
                    self.db_ops.update_file_download_status(file_id, 'SUCCESS')
                else:
                    # Update file download status as failed
                    self.db_ops.update_file_download_status(file_id, 'FAILED')
                
            except Exception as e:
                logger.error(f"     ❌ Error downloading file {file_name}: {e}")
                self.db_ops.update_file_download_status(file_id, 'FAILED', str(e))
                continue
        
        return successful_downloads
    
    def _download_single_file(self, file_id: str, file_name: str, claim_id: int, target_dir: Path, create_date=None) -> bool:
        """
        Download a single file from DMS
        """
        logger.debug(f"     📥 Downloading {file_name}...")
        
        try:
            # Construct file path
            file_path = target_dir / file_name
            
            # Skip if file already exists and is valid
            if file_path.exists() and self._validate_downloaded_file(file_path):
                logger.debug(f"     ✅ File already exists: {file_name}")
                return True
            
            # Get download URL from DMS
            download_url = self._get_dms_download_url(file_id, create_date)
            
            if not download_url:
                logger.error(f"     ❌ Could not get download URL for file {file_name}")
                return False
            
            logger.debug(f"     🌐 Downloading from: {download_url}")
            
            # Download the file
            success = self._download_file_from_url(download_url, file_path)
            
            if success and self._validate_downloaded_file(file_path):
                logger.debug(f"     ✅ Successfully downloaded: {file_name}")
                
                # Add delay between downloads
                delay = self.download_config.get('delay_between_downloads', 0.5)
                if delay > 0:
                    logger.debug(f"     ⏱️ Waiting {delay}s before next download...")
                    import time
                    time.sleep(delay)
                
                return True
            else:
                logger.error(f"     ❌ Download failed or file invalid: {file_name}")
                # Clean up partial download
                if file_path.exists():
                    file_path.unlink()
                return False
                
        except Exception as e:
            logger.error(f"     ❌ Error downloading {file_name}: {e}")
            return False
    
    def _get_dms_download_url(self, file_id: str, create_date) -> Optional[str]:
        """
        Get download URL from DMS system
        Constructs URL based on file ID and creation date
        """
        dms_config = self.config.get('dms_api', {})
        base_url = dms_config.get('base_url', 'https://sadcsapi.bydauto.com/file/download?fileUrl=')
        
        # Format date as YYYYMMDD (matching the original logic)
        if hasattr(create_date, 'strftime'):
            date_str = create_date.strftime("%Y%m%d")
        else:
            # If create_date is already a string, use it directly
            date_str = str(create_date)
        
        # Construct the file URL part exactly as in original code
        file_url_part = f"/{date_str}/{file_id}"
        
        # Full download URL - base_url already includes the path
        full_url = f"{base_url}{file_url_part}"
        
        return full_url
    
    def _download_file_from_url(self, url: str, file_path: Path) -> bool:
        """Download file from URL with retry logic"""
        max_retries = self.download_config.get('max_retries', 3)
        timeout = self.download_config.get('timeout_seconds', 60)
        
        # Get API headers from config
        api_config = self.config.get('api', {})
        headers = {
            "User-Agent": api_config.get('headers', {}).get('User-Agent', 'PDF-DMS-Service/1.0'),
            "Accept": api_config.get('headers', {}).get('Accept', 'application/pdf'),
            "APP_ID": api_config.get('headers', {}).get('APP_ID', ''),
            "SECRET_KEY": api_config.get('headers', {}).get('SECRET_KEY', ''),
            "Content-Type": "application/json",
        }
        
        for attempt in range(max_retries):
            try:
                response = requests.get(
                    url, 
                    timeout=timeout, 
                    stream=True,
                    headers=headers,
                    verify=api_config.get('verify_ssl', True),
                    allow_redirects=api_config.get('allow_redirects', True)
                )
                response.raise_for_status()
                
                with open(file_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                
                return True
                
            except Exception as e:
                logger.warning(f"     ⚠️ Download attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    import time
                    time.sleep(2 ** attempt)  # Exponential backoff
                else:
                    logger.error(f"     ❌ All {max_retries} download attempts failed")
                    return False
        
        return False
    
    def _validate_downloaded_file(self, file_path: Path) -> bool:
        """Validate that downloaded file is valid"""
        try:
            if not file_path.exists():
                return False
            
            # Check file size
            file_size = file_path.stat().st_size
            min_size = self.download_config.get('min_file_size', 1024)  # 1KB
            max_size = self.download_config.get('max_file_size', 100 * 1024 * 1024)  # 100MB
            
            if file_size < min_size:
                logger.warning(f"File too small: {file_path} ({file_size} bytes)")
                return False
            
            if file_size > max_size:
                logger.warning(f"File too large: {file_path} ({file_size} bytes)")
                return False
            
            # Basic PDF validation
            if file_path.suffix.lower() == '.pdf':
                with open(file_path, 'rb') as f:
                    header = f.read(4)
                    if header != b'%PDF':
                        logger.warning(f"Invalid PDF header: {file_path}")
                        return False
            
            return True
            
        except Exception as e:
            logger.error(f"File validation error for {file_path}: {e}")
            return False
    
    def get_download_statistics(self) -> Dict:
        """Get download statistics from database"""
        return self.db_ops.get_download_statistics()