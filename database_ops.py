"""
database_ops.py - Simplified Database Operations
Handles all database interactions for the PDF DMS service
"""

import logging
import oracledb
import pandas as pd
from typing import Dict, List, Optional, Any
from datetime import datetime
from contextlib import contextmanager

logger = logging.getLogger(__name__)


class DatabaseOps:
    """Simplified database operations for PDF DMS service"""
    
    def __init__(self, config: Dict):
        """Initialize database operations with configuration"""
        self.config = config
        self.environment = 'local'  # Default environment
        self._initialize_oracle_client()
        
        logger.info("Database operations initialized")
    
    def _initialize_oracle_client(self):
        """Initialize Oracle client in thick mode"""
        try:
            oracledb.init_oracle_client()
            logger.info("✅ Oracle client initialized in THICK mode")
        except Exception as e:
            logger.warning(f"⚠️ Could not initialize Oracle client: {e}")
    
    def set_environment_mode(self, environment: str):
        """Set the environment mode (local, uat, prod)"""
        self.environment = environment
        logger.info(f"Environment set to: {environment}")
    
    def get_db_config(self, db_type: str) -> Dict:
        """Get database configuration for specified type and environment"""
        db_configs = self.config.get('databases', {})
        env_config = db_configs.get(self.environment, {})
        return env_config.get(db_type, {})
    
    @contextmanager
    def get_dms_connection(self):
        """Get DMS database connection (read-only)"""
        config = self.get_db_config('dms')
        
        if not config:
            raise Exception(f"No DMS configuration found for environment: {self.environment}")
        
        connection = None
        try:
            connection = oracledb.connect(
                user=config['user'],
                password=config['password'],
                dsn=config['dsn'],
                mode=oracledb.DEFAULT_AUTH
            )
            logger.debug("DMS database connection established")
            yield connection
        except Exception as e:
            logger.error(f"DMS connection error: {e}")
            raise
        finally:
            if connection:
                connection.close()
                logger.debug("DMS connection closed")
    
    @contextmanager
    def get_bgate_connection(self):
        """Get BGATE database connection (read/write)"""
        config = self.get_db_config('bgate')
        
        if not config:
            raise Exception(f"No BGATE configuration found for environment: {self.environment}")
        
        connection = None
        try:
            connection = oracledb.connect(
                user=config['user'],
                password=config['password'],
                dsn=config['dsn'],
                mode=oracledb.DEFAULT_AUTH
            )
            logger.debug("BGATE database connection established")
            yield connection
        except Exception as e:
            logger.error(f"BGATE connection error: {e}")
            raise
        finally:
            if connection:
                connection.close()
                logger.debug("BGATE connection closed")
    
    def get_claims_needing_download(self) -> pd.DataFrame:
        """Get claims that need files downloaded (new or updated claims)"""
        logger.info("🔍 Checking for claims needing download...")
        
        try:
            # First check which claims we already have locally
            with self.get_bgate_connection() as bgate_conn:
                local_claims_query = """
                    SELECT CLAIM_ID, LAST_DMS_UPDATE_DATE, ATTACHMENT_STATUS
                    FROM CLAIM_STATUS
                """
                local_claims_df = pd.read_sql(local_claims_query, bgate_conn) # type: ignore
            
            # Get claims from DMS
            with self.get_dms_connection() as dms_conn:
                # Get region and status IDs
                region_id = self._get_region_id(dms_conn)
                status_id = self._get_status_code_id(dms_conn)
                
                if not region_id or not status_id:
                    logger.error("Failed to get required region_id or status_id")
                    return pd.DataFrame()
                
                dms_claims_query = """
                    SELECT
                        claims.CLAIM_ID,
                        claims.CLAIM_NO,
                        claims.VIN,
                        claims.GROSS_CREDIT,
                        claims.REPORT_DATE,
                        claims.LABOUR_AMOUNT,
                        claims.PART_AMOUNT,
                        claims.AUDITING_DATE,
                        claims.UPDATE_DATE,
                        td.DEALER_CODE,
                        td.DEALER_NAME
                    FROM
                        DMS_OEM_PROD.SEC_TT_AS_WR_APPLICATION_V claims
                    JOIN
                        DMS_OEM_PROD.TM_DEALER td ON claims.DEALER_ID = td.DEALER_ID
                    WHERE
                        td.COUNTRY_ID = :region_id
                        AND claims.STATUS = :status_id
                        AND claims.REPORT_DATE >= TO_DATE('2020-07-23', 'YYYY-MM-DD')
                        AND claims.UPDATE_DATE < SYSDATE
                    ORDER BY claims.UPDATE_DATE DESC
                """
                
                dms_claims_df = pd.read_sql(
                    dms_claims_query,
                    dms_conn, # type: ignore
                    params={'region_id': region_id, 'status_id': status_id}
                )
            
            if dms_claims_df.empty:
                logger.info("No claims found in DMS")
                return pd.DataFrame()
            
            # Find claims that need downloading
            if local_claims_df.empty:
                # All DMS claims need downloading
                claims_needing_download = dms_claims_df
            else:
                # Merge to find new or updated claims
                merged_df = dms_claims_df.merge(local_claims_df, on='CLAIM_ID', how='left')
                
                # Claims need download if:
                # 1. New claims (LAST_DMS_UPDATE_DATE is null)
                # 2. Updated claims (UPDATE_DATE > LAST_DMS_UPDATE_DATE)
                # 3. Incomplete attachments (ATTACHMENT_STATUS != 'COMPLETE')
                claims_needing_download = merged_df[
                    merged_df['LAST_DMS_UPDATE_DATE'].isna() |
                    (merged_df['UPDATE_DATE'] > merged_df['LAST_DMS_UPDATE_DATE']) |
                    (merged_df['ATTACHMENT_STATUS'] != 'COMPLETE')
                ]
            
            # Update/insert claim status records
            if not claims_needing_download.empty:
                self._upsert_claim_status(claims_needing_download)
            
            logger.info(f"Found {len(claims_needing_download)} claims needing download")
            return claims_needing_download
            
        except Exception as e:
            logger.error(f"Error getting claims needing download: {e}")
            return pd.DataFrame()
    
    def get_files_needing_download(self, claim_id: int) -> pd.DataFrame:
        """Get files that need to be downloaded for a specific claim"""
        try:
            with self.get_dms_connection() as dms_conn:
                files_query = """
                    SELECT
                        files.FILE_ID,
                        files.FILE_NAME,
                        files.CREATE_DATE,
                        claims.CLAIM_ID,
                        claims.CLAIM_NO
                    FROM
                        DMS_OEM_PROD.TC_FILE_UPLOAD_INFO files
                    JOIN
                        DMS_OEM_PROD.SEC_TT_AS_WR_APPLICATION_V claims ON files.BILL_ID = claims.CLAIM_ID
                    WHERE
                        claims.CLAIM_ID = :claim_id
                        AND files.FILE_TYPE_DETAIL = '.pdf'
                    ORDER BY files.CREATE_DATE ASC
                """
                
                files_df = pd.read_sql(files_query, dms_conn, params={'claim_id': claim_id}) # type: ignore
                
                # Filter out files we already have successfully downloaded
                if not files_df.empty:
                    with self.get_bgate_connection() as bgate_conn:
                        existing_files_query = """
                            SELECT FILE_ID 
                            FROM PDF_DOWNLOAD_DMS_CLAIMS 
                            WHERE CLAIM_ID = :claim_id 
                            AND STATUS = 'SUCCESS'
                            AND IS_LATEST_VERSION = 'Y'
                        """
                        existing_df = pd.read_sql(existing_files_query, bgate_conn, params={'claim_id': claim_id}) # type: ignore
                        
                        if not existing_df.empty:
                            existing_file_ids = existing_df['FILE_ID'].tolist()
                            files_df = files_df[~files_df['FILE_ID'].isin(existing_file_ids)]
                
                return files_df
                
        except Exception as e:
            logger.error(f"Error getting files for claim {claim_id}: {e}")
            return pd.DataFrame()
    
    def get_claims_ready_for_processing(self) -> pd.DataFrame:
        """Get claims that are ready for PDF processing"""
        try:
            with self.get_bgate_connection() as bgate_conn:
                query = """
                    SELECT DISTINCT 
                        cs.CLAIM_ID,
                        cs.CLAIM_NO,
                        cs.VIN,
                        cs.DEALER_CODE,
                        cs.DEALER_NAME,
                        cs.GROSS_CREDIT,
                        cs.LABOUR_AMOUNT_DMS,
                        cs.PART_AMOUNT_DMS
                    FROM CLAIM_STATUS cs
                    WHERE cs.ATTACHMENT_STATUS = 'COMPLETE'
                    AND (cs.AUDIT_STATUS IS NULL OR cs.AUDIT_STATUS = 'PENDING')
                    AND EXISTS (
                        SELECT 1 
                        FROM PDF_DOWNLOAD_DMS_CLAIMS pdf 
                        WHERE pdf.CLAIM_ID = cs.CLAIM_ID 
                        AND pdf.STATUS = 'SUCCESS'
                        AND pdf.IS_LATEST_VERSION = 'Y'
                    )
                    ORDER BY cs.LAST_DMS_UPDATE_DATE DESC
                """
                
                result_df = pd.read_sql(query, bgate_conn) # type: ignore
                logger.info(f"Found {len(result_df)} claims ready for processing")
                return result_df
                
        except Exception as e:
            logger.error(f"Error getting claims ready for processing: {e}")
            return pd.DataFrame()
    
    def get_claims_ready_for_matching(self) -> pd.DataFrame:
        """Get claims that are ready for invoice matching"""
        try:
            with self.get_bgate_connection() as bgate_conn:
                query = """
                    SELECT 
                        cs.CLAIM_ID,
                        cs.CLAIM_NO,
                        cs.VIN,
                        cs.DEALER_CODE,
                        cs.DEALER_NAME,
                        cs.GROSS_CREDIT,
                        cs.LABOUR_AMOUNT_DMS,
                        cs.PART_AMOUNT_DMS,
                        cs.LABOUR_AMOUNT_PROCESSING,
                        cs.PART_AMOUNT_PROCESSING
                    FROM CLAIM_STATUS cs
                    WHERE cs.AUDIT_STATUS = 'COMPLETE'
                    AND (cs.AUDIT_STATUS IS NULL OR cs.AUDIT_STATUS = 'PENDING')
                    AND cs.LABOUR_AMOUNT_PROCESSING IS NOT NULL
                    AND cs.PART_AMOUNT_PROCESSING IS NOT NULL
                    ORDER BY cs.PROCESSING_DATE DESC
                """
                
                result_df = pd.read_sql(query, bgate_conn) # type: ignore
                logger.info(f"Found {len(result_df)} claims ready for matching")
                return result_df
                
        except Exception as e:
            logger.error(f"Error getting claims ready for matching: {e}")
            return pd.DataFrame()
    
    def get_claim_pdf_files(self, claim_id: int) -> List[str]:
        """Get list of PDF file paths for a claim"""
        try:
            with self.get_bgate_connection() as bgate_conn:
                query = """
                    SELECT LOCAL_FILE_PATH
                    FROM PDF_DOWNLOAD_DMS_CLAIMS
                    WHERE CLAIM_ID = :claim_id
                    AND STATUS = 'SUCCESS'
                    AND IS_LATEST_VERSION = 'Y'
                    ORDER BY DOWNLOAD_TIMESTAMP ASC
                """
                
                cursor = bgate_conn.cursor()
                cursor.execute(query, {'claim_id': claim_id})
                results = cursor.fetchall()
                cursor.close()
                
                file_paths = [row[0] for row in results if row[0]]
                logger.debug(f"Found {len(file_paths)} PDF files for claim {claim_id}")
                return file_paths
                
        except Exception as e:
            logger.error(f"Error getting PDF files for claim {claim_id}: {e}")
            return []
    
    def update_claim_download_status(self, claim_id: int, files_downloaded: int):
        """Update claim download status after downloading files"""
        try:
            with self.get_bgate_connection() as bgate_conn:
                # Get total files for this claim
                total_files_query = """
                    SELECT COUNT(*) 
                    FROM PDF_DOWNLOAD_DMS_CLAIMS 
                    WHERE CLAIM_ID = :claim_id
                    AND IS_LATEST_VERSION = 'Y'
                """
                
                cursor = bgate_conn.cursor()
                cursor.execute(total_files_query, {'claim_id': claim_id})
                total_files = cursor.fetchone()[0]
                
                # Get successful downloads
                success_files_query = """
                    SELECT COUNT(*) 
                    FROM PDF_DOWNLOAD_DMS_CLAIMS 
                    WHERE CLAIM_ID = :claim_id 
                    AND STATUS = 'SUCCESS'
                    AND IS_LATEST_VERSION = 'Y'
                """
                
                cursor.execute(success_files_query, {'claim_id': claim_id})
                success_files = cursor.fetchone()[0]
                
                # Determine attachment status
                if success_files == 0:
                    attachment_status = 'PENDING'
                elif success_files < total_files:
                    attachment_status = 'PARTIAL'
                else:
                    attachment_status = 'COMPLETE'
                
                # Update claim status
                update_query = """
                    UPDATE CLAIM_STATUS 
                    SET TOTAL_FILES_COUNT = :total_files,
                        DOWNLOADED_FILES_COUNT = :success_files,
                        ATTACHMENT_STATUS = :attachment_status,
                        LAST_MODIFIED_DATE = CURRENT_TIMESTAMP
                    WHERE CLAIM_ID = :claim_id
                """
                
                cursor.execute(update_query, {
                    'total_files': total_files,
                    'success_files': success_files,
                    'attachment_status': attachment_status,
                    'claim_id': claim_id
                })
                
                bgate_conn.commit()
                cursor.close()
                
                logger.info(f"Updated claim {claim_id}: {success_files}/{total_files} files, status: {attachment_status}")
                
        except Exception as e:
            logger.error(f"Error updating claim download status for {claim_id}: {e}")
    
    def update_file_download_status(self, file_id: str, status: str, error_message: str | None = None):
        """Update individual file download status"""
        try:
            with self.get_bgate_connection() as bgate_conn:
                update_query = """
                    UPDATE PDF_DOWNLOAD_DMS_CLAIMS 
                    SET STATUS = :status,
                        ERROR_MESSAGE = :error_message,
                        LAST_MODIFIED_DATE = CURRENT_TIMESTAMP
                    WHERE FILE_ID = :file_id
                """
                
                cursor = bgate_conn.cursor()
                cursor.execute(update_query, {
                    'status': status,
                    'error_message': error_message,
                    'file_id': file_id
                })
                
                bgate_conn.commit()
                cursor.close()
                
                logger.debug(f"Updated file {file_id} status to {status}")
                
        except Exception as e:
            logger.error(f"Error updating file download status for {file_id}: {e}")
    
    def save_processing_results(self, claim_id: int, processing_results: Dict) -> bool:
        """Save PDF processing results to database"""
        try:
            with self.get_bgate_connection() as bgate_conn:
                # Extract amounts from processing results
                summary = processing_results.get('processing_summary', {})
                labour_amount = summary.get('total_amount_mao_obra', 0.0)
                part_amount = summary.get('total_amount_pecas', 0.0) + summary.get('total_amount_diversos', 0.0)
                
                # Update claim status with processing results
                update_query = """
                    UPDATE CLAIM_STATUS 
                    SET LABOUR_AMOUNT_PROCESSING = :labour_amount,
                        PART_AMOUNT_PROCESSING = :part_amount,
                        AUDIT_STATUS = 'COMPLETE',
                        PROCESSING_DATE = CURRENT_TIMESTAMP,
                        LAST_MODIFIED_DATE = CURRENT_TIMESTAMP
                    WHERE CLAIM_ID = :claim_id
                """
                
                cursor = bgate_conn.cursor()
                cursor.execute(update_query, {
                    'labour_amount': labour_amount,
                    'part_amount': part_amount,
                    'claim_id': claim_id
                })
                
                bgate_conn.commit()
                cursor.close()
                
                logger.info(f"Saved processing results for claim {claim_id}: "
                           f"Labour={labour_amount:.2f}, Parts={part_amount:.2f}")
                return True
                
        except Exception as e:
            logger.error(f"Error saving processing results for claim {claim_id}: {e}")
            return False
    
    def update_audit_status(self, claim_id: int, audit_status: str, reason: str | None = None):
        """Update audit/matching status for a claim"""
        try:
            with self.get_bgate_connection() as bgate_conn:
                update_query = """
                    UPDATE CLAIM_STATUS 
                    SET AUDIT_STATUS = :audit_status,
                        AUDIT_REASON = :reason,
                        AUDIT_DATE = CURRENT_TIMESTAMP,
                        LAST_MODIFIED_DATE = CURRENT_TIMESTAMP
                    WHERE CLAIM_ID = :claim_id
                """
                
                cursor = bgate_conn.cursor()
                cursor.execute(update_query, {
                    'audit_status': audit_status,
                    'reason': reason,
                    'claim_id': claim_id
                })
                
                bgate_conn.commit()
                cursor.close()
                
                logger.info(f"Updated audit status for claim {claim_id}: {audit_status}")
                
        except Exception as e:
            logger.error(f"Error updating audit status for claim {claim_id}: {e}")
    
    def _get_region_id(self, connection, region_name="巴西") -> Optional[int]:
        """Get Brazil region ID from DMS database"""
        try:
            query = "SELECT REGION_ID FROM DMS_OEM_PROD.TM_REGION WHERE REGION_NAME = :region_name"
            cursor = connection.cursor()
            cursor.execute(query, {'region_name': region_name})
            result = cursor.fetchone()
            cursor.close()
            
            if result:
                logger.debug(f"Found region ID {result[0]} for '{region_name}'")
                return result[0]
            else:
                logger.error(f"Region '{region_name}' not found")
                return None
                
        except Exception as e:
            logger.error(f"Error getting region ID: {e}")
            return None
    
    def _get_status_code_id(self, connection, type_code=5618, target_description="待审核付款凭证") -> Optional[int]:
        """Get status code ID for payment documents to be audited"""
        try:
            query = """
                SELECT CODE_ID 
                FROM DMS_OEM_PROD.TC_CODE 
                WHERE TYPE = :type_code 
                AND CODE_DESC = :target_description
            """
            cursor = connection.cursor()
            cursor.execute(query, {
                'type_code': type_code,
                'target_description': target_description
            })
            result = cursor.fetchone()
            cursor.close()
            
            if result:
                logger.debug(f"Found status code ID {result[0]} for '{target_description}'")
                return result[0]
            else:
                logger.error(f"Status code not found for '{target_description}'")
                return None
                
        except Exception as e:
            logger.error(f"Error getting status code ID: {e}")
            return None
    
    def _upsert_claim_status(self, claims_df: pd.DataFrame):
        """Insert or update claim status records"""
        try:
            with self.get_bgate_connection() as bgate_conn:
                for _, claim_row in claims_df.iterrows():
                    claim_data = {
                        'CLAIM_ID': int(claim_row['CLAIM_ID']),
                        'CLAIM_NO': claim_row.get('CLAIM_NO'),
                        'VIN': claim_row.get('VIN'),
                        'DEALER_CODE': claim_row.get('DEALER_CODE'),
                        'DEALER_NAME': claim_row.get('DEALER_NAME'),
                        'REPORT_DATE': claim_row.get('REPORT_DATE'),
                        'GROSS_CREDIT': float(claim_row.get('GROSS_CREDIT', 0)),
                        'LABOUR_AMOUNT_DMS': float(claim_row.get('LABOUR_AMOUNT', 0)),
                        'PART_AMOUNT_DMS': float(claim_row.get('PART_AMOUNT', 0)),
                        'LAST_DMS_UPDATE_DATE': claim_row.get('UPDATE_DATE'),
                        'AUDITING_DATE': claim_row.get('AUDITING_DATE')
                    }
                    
                    merge_query = """
                        MERGE INTO CLAIM_STATUS cs
                        USING (SELECT :CLAIM_ID as CLAIM_ID FROM DUAL) src
                        ON (cs.CLAIM_ID = src.CLAIM_ID)
                        WHEN MATCHED THEN
                            UPDATE SET
                                CLAIM_NO = :CLAIM_NO,
                                VIN = :VIN,
                                DEALER_CODE = :DEALER_CODE,
                                DEALER_NAME = :DEALER_NAME,
                                REPORT_DATE = :REPORT_DATE,
                                GROSS_CREDIT = :GROSS_CREDIT,
                                LABOUR_AMOUNT_DMS = :LABOUR_AMOUNT_DMS,
                                PART_AMOUNT_DMS = :PART_AMOUNT_DMS,
                                LAST_DMS_UPDATE_DATE = :LAST_DMS_UPDATE_DATE,
                                AUDITING_DATE = :AUDITING_DATE,
                                LAST_MODIFIED_DATE = CURRENT_TIMESTAMP
                        WHEN NOT MATCHED THEN
                            INSERT (
                                CLAIM_ID, CLAIM_NO, VIN, DEALER_CODE, DEALER_NAME, REPORT_DATE,
                                GROSS_CREDIT, LABOUR_AMOUNT_DMS, PART_AMOUNT_DMS,
                                LAST_DMS_UPDATE_DATE, AUDITING_DATE, ATTACHMENT_STATUS,
                                CREATED_DATE
                            )
                            VALUES (
                                :CLAIM_ID, :CLAIM_NO, :VIN, :DEALER_CODE, :DEALER_NAME, :REPORT_DATE,
                                :GROSS_CREDIT, :LABOUR_AMOUNT_DMS, :PART_AMOUNT_DMS,
                                :LAST_DMS_UPDATE_DATE, :AUDITING_DATE, 'PENDING',
                                CURRENT_TIMESTAMP
                            )
                    """
                    
                    cursor = bgate_conn.cursor()
                    cursor.execute(merge_query, claim_data)
                    cursor.close()
                
                bgate_conn.commit()
                logger.info(f"Upserted {len(claims_df)} claim status records")
                
        except Exception as e:
            logger.error(f"Error upserting claim status: {e}")
    
    def get_download_statistics(self) -> Dict:
        """Get download statistics"""
        try:
            with self.get_bgate_connection() as bgate_conn:
                stats_query = """
                    SELECT 
                        STATUS,
                        COUNT(*) as COUNT
                    FROM PDF_DOWNLOAD_DMS_CLAIMS
                    WHERE IS_LATEST_VERSION = 'Y'
                    GROUP BY STATUS
                """
                
                stats_df = pd.read_sql(stats_query, bgate_conn) # type: ignore
                
                if stats_df.empty:
                    return {'total_files': 0}
                
                stats_dict = dict(zip(stats_df['STATUS'], stats_df['COUNT']))
                stats_dict['total_files'] = stats_df['COUNT'].sum()
                
                return stats_dict
                
        except Exception as e:
            logger.error(f"Error getting download statistics: {e}")
            return {'error': str(e)}
    
    def cleanup(self):
        """Cleanup resources"""
        logger.info("Database operations cleanup completed")