"""
matching_invoices.py - Simplified Invoice Matching Service
Compares PDF processing results with DMS claim data to verify accuracy
"""

import logging
from typing import Dict, List, Tuple
import pandas as pd

logger = logging.getLogger(__name__)


class InvoiceMatcher:
    """Matches invoice data from PDF processing with DMS claim information"""
    
    def __init__(self, config: Dict, db_ops):
        """Initialize invoice matcher with configuration and database operations"""
        self.config = config
        self.db_ops = db_ops
        self.matching_config = config.get('matching', {})
        self.batch_size = self.matching_config.get('max_claims_per_batch', 50)
        self.exact_match = self.matching_config.get('exact_amount_match', True)
        self.tolerance_percentage = self.matching_config.get('tolerance_percentage', 0.0)
        
        logger.info("Invoice matcher initialized")
    
    def match_pending_claims(self) -> Dict:
        """
        Main method to match claims that are ready for invoice matching
        Returns summary of matching results
        """
        logger.info("🎯 Checking for claims ready for invoice matching...")
        
        try:
            # Get claims ready for matching
            claims_df = self.db_ops.get_claims_ready_for_matching()
            
            if claims_df.empty:
                logger.info("✅ No claims ready for invoice matching")
                return {'claims_processed': 0}
            
            logger.info(f"🎯 Found {len(claims_df)} claims ready for matching")
            
            # Process claims in batches
            total_claims_processed = 0
            total_claims_matched = 0
            total_claims_rejected = 0
            
            for i in range(0, len(claims_df), self.batch_size):
                batch_claims = claims_df.iloc[i:i + self.batch_size]
                batch_num = (i // self.batch_size) + 1
                total_batches = (len(claims_df) + self.batch_size - 1) // self.batch_size
                
                logger.info(f"📦 Processing batch {batch_num}/{total_batches} ({len(batch_claims)} claims)")
                
                batch_results = self._process_matching_batch(batch_claims)
                
                total_claims_processed += batch_results['claims_processed']
                total_claims_matched += batch_results['claims_matched']
                total_claims_rejected += batch_results['claims_rejected']
            
            logger.info(f"✅ Invoice matching completed: {total_claims_processed} processed, "
                       f"{total_claims_matched} matched, {total_claims_rejected} rejected")
            
            return {
                'claims_processed': total_claims_processed,
                'claims_matched': total_claims_matched,
                'claims_rejected': total_claims_rejected
            }
            
        except Exception as e:
            logger.error(f"❌ Invoice matching failed: {e}")
            return {'claims_processed': 0, 'error': str(e)}
    
    def _process_matching_batch(self, claims_df: pd.DataFrame) -> Dict:
        """Process a batch of claims for matching"""
        claims_processed = 0
        claims_matched = 0
        claims_rejected = 0
        
        for _, claim_row in claims_df.iterrows():
            claim_id = int(claim_row['CLAIM_ID'])
            
            try:
                logger.info(f"🎯 Matching CLAIM_ID {claim_id}")
                
                # Perform the matching
                match_result = self._match_claim(claim_row)
                
                # Update audit status based on matching result
                if match_result['match_success']:
                    self.db_ops.update_audit_status(claim_id, 'COMPLETE', match_result['reason'])
                    claims_matched += 1
                    logger.info(f"   ✅ Match successful: {match_result['reason']}")
                else:
                    self.db_ops.update_audit_status(claim_id, 'REJECTED', match_result['reason'])
                    claims_rejected += 1
                    logger.warning(f"   ❌ Match failed: {match_result['reason']}")
                
                claims_processed += 1
                
            except Exception as e:
                logger.error(f"   ❌ Error matching CLAIM_ID {claim_id}: {e}")
                # Mark as rejected due to processing error
                self.db_ops.update_audit_status(claim_id, 'REJECTED', f"Processing error: {str(e)}")
                claims_rejected += 1
                claims_processed += 1
                continue
        
        return {
            'claims_processed': claims_processed,
            'claims_matched': claims_matched,
            'claims_rejected': claims_rejected
        }
    
    def _match_claim(self, claim_row: pd.Series) -> Dict:
        """
        Match a single claim's processing results with DMS data
        Returns dict with match_success (bool) and reason (str)
        """
        claim_id = int(claim_row['CLAIM_ID'])
        
        try:
            # Extract DMS amounts
            dms_labour = float(claim_row.get('LABOUR_AMOUNT_DMS', 0))
            dms_parts = float(claim_row.get('PART_AMOUNT_DMS', 0))
            dms_total = dms_labour + dms_parts
            
            # Extract processing amounts
            proc_labour = float(claim_row.get('LABOUR_AMOUNT_PROCESSING', 0))
            proc_parts = float(claim_row.get('PART_AMOUNT_PROCESSING', 0))
            proc_total = proc_labour + proc_parts
            
            logger.debug(f"   DMS amounts: Labour={dms_labour:.2f}, Parts={dms_parts:.2f}, Total={dms_total:.2f}")
            logger.debug(f"   Processing amounts: Labour={proc_labour:.2f}, Parts={proc_parts:.2f}, Total={proc_total:.2f}")
            
            # Perform amount matching
            amount_match_result = self._check_amount_match(
                dms_labour, dms_parts, dms_total,
                proc_labour, proc_parts, proc_total
            )
            
            if not amount_match_result['match']:
                return {
                    'match_success': False,
                    'reason': f"Amount mismatch: {amount_match_result['reason']}"
                }
            
            # Perform data validation (VIN, claim number, etc.)
            data_validation_result = self._validate_claim_data(claim_row)
            
            if not data_validation_result['valid']:
                return {
                    'match_success': False,
                    'reason': f"Data validation failed: {data_validation_result['reason']}"
                }
            
            # If we get here, all checks passed
            return {
                'match_success': True,
                'reason': f"All validations passed. Amount match: {amount_match_result['reason']}"
            }
            
        except Exception as e:
            logger.error(f"Error in claim matching for {claim_id}: {e}")
            return {
                'match_success': False,
                'reason': f"Matching error: {str(e)}"
            }
    
    def _check_amount_match(self, dms_labour: float, dms_parts: float, dms_total: float,
                           proc_labour: float, proc_parts: float, proc_total: float) -> Dict:
        """
        Check if processing amounts match DMS amounts within configured tolerance
        Returns dict with match (bool) and reason (str)
        """
        try:
            if self.exact_match and self.tolerance_percentage == 0.0:
                # Exact match required
                labour_match = abs(dms_labour - proc_labour) < 0.01  # Allow for float precision
                parts_match = abs(dms_parts - proc_parts) < 0.01
                total_match = abs(dms_total - proc_total) < 0.01
                
                if labour_match and parts_match and total_match:
                    return {
                        'match': True,
                        'reason': f"Exact match - DMS: {dms_total:.2f}, Processing: {proc_total:.2f}"
                    }
                else:
                    differences = []
                    if not labour_match:
                        diff = proc_labour - dms_labour
                        differences.append(f"Labour: {diff:+.2f}")
                    if not parts_match:
                        diff = proc_parts - dms_parts
                        differences.append(f"Parts: {diff:+.2f}")
                    
                    return {
                        'match': False,
                        'reason': f"Exact match failed. Differences: {', '.join(differences)}"
                    }
            
            else:
                # Tolerance-based matching
                tolerance_amount = dms_total * (self.tolerance_percentage / 100.0)
                total_diff = abs(proc_total - dms_total)
                
                if total_diff <= tolerance_amount:
                    percentage_diff = (total_diff / dms_total * 100.0) if dms_total > 0 else 0
                    return {
                        'match': True,
                        'reason': f"Within tolerance - Difference: {total_diff:.2f} ({percentage_diff:.1f}%)"
                    }
                else:
                    percentage_diff = (total_diff / dms_total * 100.0) if dms_total > 0 else float('inf')
                    return {
                        'match': False,
                        'reason': f"Exceeds tolerance - Difference: {total_diff:.2f} ({percentage_diff:.1f}%), "
                                f"Allowed: {tolerance_amount:.2f} ({self.tolerance_percentage}%)"
                    }
            
        except Exception as e:
            return {
                'match': False,
                'reason': f"Amount comparison error: {str(e)}"
            }
    
    def _validate_claim_data(self, claim_row: pd.Series) -> Dict:
        """
        Validate claim data consistency (VIN, claim number, etc.)
        Returns dict with valid (bool) and reason (str)
        """
        try:
            claim_id = int(claim_row['CLAIM_ID'])
            claim_no = claim_row.get('CLAIM_NO', '')
            vin = claim_row.get('VIN', '')
            
            # Basic validation - ensure required fields are present
            if not claim_no:
                return {
                    'valid': False,
                    'reason': "Missing claim number"
                }
            
            if not vin:
                return {
                    'valid': False,
                    'reason': "Missing VIN"
                }
            
            # VIN format validation (basic check for Brazilian BYD VINs)
            if len(vin) != 17:
                return {
                    'valid': False,
                    'reason': f"Invalid VIN length: {len(vin)} (expected 17)"
                }
            
            # Additional validation could be added here:
            # - Check if VIN matches processing results
            # - Validate claim number format
            # - Cross-reference with processing data
            
            # For now, basic validation passes
            return {
                'valid': True,
                'reason': "Basic data validation passed"
            }
            
        except Exception as e:
            return {
                'valid': False,
                'reason': f"Data validation error: {str(e)}"
            }
    
    def get_matching_statistics(self) -> Dict:
        """Get matching statistics from database"""
        try:
            with self.db_ops.get_bgate_connection() as bgate_conn:
                stats_query = """
                    SELECT 
                        AUDIT_STATUS,
                        COUNT(*) as COUNT
                    FROM CLAIM_STATUS
                    WHERE AUDIT_STATUS IS NOT NULL
                    GROUP BY AUDIT_STATUS
                """
                
                stats_df = pd.read_sql(stats_query, bgate_conn)
                
                if stats_df.empty:
                    return {'total_audited': 0}
                
                stats_dict = dict(zip(stats_df['AUDIT_STATUS'], stats_df['COUNT']))
                stats_dict['total_audited'] = stats_df['COUNT'].sum()
                
                # Calculate success rate
                complete_count = stats_dict.get('COMPLETE', 0)
                total_count = stats_dict['total_audited']
                if total_count > 0:
                    stats_dict['success_rate'] = (complete_count / total_count) * 100
                else:
                    stats_dict['success_rate'] = 0.0
                
                return stats_dict
                
        except Exception as e:
            logger.error(f"Error getting matching statistics: {e}")
            return {'error': str(e)}
    
    def get_detailed_matching_report(self, limit: int = 100) -> pd.DataFrame:
        """Get detailed matching report for recent claims"""
        try:
            with self.db_ops.get_bgate_connection() as bgate_conn:
                report_query = """
                    SELECT 
                        cs.CLAIM_ID,
                        cs.CLAIM_NO,
                        cs.VIN,
                        cs.DEALER_CODE,
                        cs.LABOUR_AMOUNT_DMS,
                        cs.PART_AMOUNT_DMS,
                        cs.LABOUR_AMOUNT_PROCESSING,
                        cs.PART_AMOUNT_PROCESSING,
                        cs.AUDIT_STATUS,
                        cs.AUDIT_REASON,
                        cs.AUDIT_DATE,
                        (cs.LABOUR_AMOUNT_DMS + cs.PART_AMOUNT_DMS) as DMS_TOTAL,
                        (cs.LABOUR_AMOUNT_PROCESSING + cs.PART_AMOUNT_PROCESSING) as PROCESSING_TOTAL,
                        ABS((cs.LABOUR_AMOUNT_PROCESSING + cs.PART_AMOUNT_PROCESSING) - 
                            (cs.LABOUR_AMOUNT_DMS + cs.PART_AMOUNT_DMS)) as AMOUNT_DIFFERENCE
                    FROM CLAIM_STATUS cs
                    WHERE cs.AUDIT_STATUS IS NOT NULL
                    ORDER BY cs.AUDIT_DATE DESC
                    FETCH FIRST :limit ROWS ONLY
                """
                
                report_df = pd.read_sql(report_query, bgate_conn, params={'limit': limit})
                
                # Calculate percentage differences
                if not report_df.empty:
                    report_df['PERCENTAGE_DIFFERENCE'] = (
                        report_df['AMOUNT_DIFFERENCE'] / report_df['DMS_TOTAL'] * 100
                    ).round(2)
                
                return report_df
                
        except Exception as e:
            logger.error(f"Error generating detailed matching report: {e}")
            return pd.DataFrame()
    
    def reprocess_rejected_claims(self, max_claims: int = 10) -> Dict:
        """
        Reprocess claims that were previously rejected
        Useful for testing new matching logic or fixing data issues
        """
        logger.info(f"🔄 Reprocessing up to {max_claims} rejected claims...")
        
        try:
            with self.db_ops.get_bgate_connection() as bgate_conn:
                rejected_query = """
                    SELECT 
                        cs.CLAIM_ID,
                        cs.CLAIM_NO,
                        cs.VIN,
                        cs.DEALER_CODE,
                        cs.DEALER_NAME,
                        cs.LABOUR_AMOUNT_DMS,
                        cs.PART_AMOUNT_DMS,
                        cs.LABOUR_AMOUNT_PROCESSING,
                        cs.PART_AMOUNT_PROCESSING
                    FROM CLAIM_STATUS cs
                    WHERE cs.AUDIT_STATUS = 'REJECTED'
                    AND cs.PROCESSING_STATUS = 'COMPLETE'
                    ORDER BY cs.AUDIT_DATE DESC
                    FETCH FIRST :max_claims ROWS ONLY
                """
                
                rejected_df = pd.read_sql(rejected_query, bgate_conn, params={'max_claims': max_claims})
            
            if rejected_df.empty:
                logger.info("No rejected claims found for reprocessing")
                return {'claims_processed': 0}
            
            logger.info(f"Found {len(rejected_df)} rejected claims to reprocess")
            
            # Process the rejected claims
            batch_results = self._process_matching_batch(rejected_df)
            
            logger.info(f"✅ Reprocessing completed: {batch_results['claims_processed']} processed, "
                       f"{batch_results['claims_matched']} now matched, {batch_results['claims_rejected']} still rejected")
            
            return batch_results
            
        except Exception as e:
            logger.error(f"❌ Reprocessing failed: {e}")
            return {'claims_processed': 0, 'error': str(e)}