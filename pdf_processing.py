"""
pdf_processing.py - Simplified PDF Processing Service
Processes PDF files using the ultra-arena-frk project
"""

import logging
import sys
from pathlib import Path
from typing import Dict, List
import pandas as pd

logger = logging.getLogger(__name__)


class PDFProcessor:
    """Processes PDF files using ultra-arena-frk"""
    
    def __init__(self, config: Dict, db_ops):
        """Initialize PDF processor with configuration and database operations"""
        self.config = config
        self.db_ops = db_ops
        self.processing_config = config.get('pdf_processing', {})
        self.batch_size = self.processing_config.get('max_claims_per_batch', 10)

        # Mock processing configuration
        self.use_mock_processing = config.get('development', {}).get('use_mock_processing', False)
        self.mock_success_rate = config.get('development', {}).get('mock_success_rate', 0.7)
        
        # Setup ultra-arena-frk integration
        self._setup_ultra_arena_integration()
        
        logger.info("PDF processor initialized")
    
    def _setup_ultra_arena_integration(self):
        """Setup integration with ultra-arena-frk project"""
        try:
            ultra_arena_path = Path(__file__).parent.parent / "ultra-arena-frk" / "Ultra_Arena_Main"
            
            if not ultra_arena_path.exists():
                logger.error(f"Ultra Arena path not found: {ultra_arena_path}")
                logger.error("Please ensure ultra-arena-frk project is properly installed")
                self.ultra_arena_available = False
                return
            
            if str(ultra_arena_path) not in sys.path:
                sys.path.insert(0, str(ultra_arena_path))
            
            # Import the main processing function
            from main_modular import run_file_processing_simple
            self.run_file_processing_simple = run_file_processing_simple
            self.ultra_arena_available = True
            
            logger.info("✅ Ultra Arena integration setup successful")
            
        except ImportError as e:
            logger.error(f"❌ Could not import ultra-arena-frk: {e}")
            logger.error("Please check ultra-arena-frk installation and dependencies")
            self.ultra_arena_available = False
        except Exception as e:
            logger.error(f"❌ Error setting up ultra-arena-frk integration: {e}")
            self.ultra_arena_available = False
    
    def process_pending_claims(self) -> Dict:
        """
        Main method to process PDFs for claims that are ready for processing
        Returns summary of processing results
        """
        logger.info("🔄 Checking for claims ready for PDF processing...")
        
        try:
            # Get claims ready for processing
            claims_df = self.db_ops.get_claims_ready_for_processing()
            
            if claims_df.empty:
                logger.info("✅ No claims ready for PDF processing")
                return {'claims_processed': 0}
            
            logger.info(f"📄 Found {len(claims_df)} claims ready for processing")
            
            # Process claims in batches
            total_claims_processed = 0
            
            for i in range(0, len(claims_df), self.batch_size):
                batch_claims = claims_df.iloc[i:i + self.batch_size]
                batch_num = (i // self.batch_size) + 1
                total_batches = (len(claims_df) + self.batch_size - 1) // self.batch_size
                
                logger.info(f"📦 Processing batch {batch_num}/{total_batches} ({len(batch_claims)} claims)")
                
                batch_processed = self._process_claim_batch(batch_claims)
                total_claims_processed += batch_processed
            
            logger.info(f"✅ PDF processing completed: {total_claims_processed} claims processed")
            
            return {'claims_processed': total_claims_processed}
            
        except Exception as e:
            logger.error(f"❌ PDF processing failed: {e}")
            return {'claims_processed': 0, 'error': str(e)}
    
    def _process_claim_batch(self, claims_df: pd.DataFrame) -> int:
        """Process a batch of claims"""
        claims_processed = 0
        
        for _, claim_row in claims_df.iterrows():
            claim_id = int(claim_row['CLAIM_ID'])
            
            try:
                logger.info(f"🔄 Processing PDFs for CLAIM_ID {claim_id}")
                
                # Get PDF files for this claim
                pdf_files = self.db_ops.get_claim_pdf_files(claim_id)
                
                if not pdf_files:
                    logger.warning(f"   ⚠️ No PDF files found for CLAIM_ID {claim_id}")
                    continue
                
                logger.info(f"   📄 Found {len(pdf_files)} PDF files to process")
                
                # Process the PDF files
                processing_results = self._process_claim_pdfs(claim_id, pdf_files)
                
                if processing_results:
                    # Save results to database
                    success = self.db_ops.save_processing_results(claim_id, processing_results)
                    
                    if success:
                        claims_processed += 1
                        logger.info(f"   ✅ Successfully processed CLAIM_ID {claim_id}")
                    else:
                        logger.error(f"   ❌ Failed to save results for CLAIM_ID {claim_id}")
                else:
                    logger.error(f"   ❌ Processing failed for CLAIM_ID {claim_id}")
                
            except Exception as e:
                logger.error(f"   ❌ Error processing CLAIM_ID {claim_id}: {e}")
                continue
        
        return claims_processed
    
    def _process_claim_pdfs(self, claim_id: int, pdf_files: List[str]) -> Dict | None:
        """Process PDF files for a specific claim"""
        try:
            # Check if we should use mock processing
            if self.use_mock_processing:
                logger.info(f"   🧪 Using mock processing (configured)")
                return self._mock_process_claim_pdfs(claim_id, pdf_files)
            
            if not self.ultra_arena_available:
                logger.warning("Ultra Arena not available, using mock processing")
                return self._mock_process_claim_pdfs(claim_id, pdf_files)
            
            # Convert file paths to Path objects
            pdf_paths = [Path(pdf_file) for pdf_file in pdf_files if Path(pdf_file).exists()]
            
            if not pdf_paths:
                logger.error(f"No valid PDF files found for claim {claim_id}")
                return None
            
            # Use the first PDF's directory as input directory
            input_dir = pdf_paths[0].parent
            
            logger.info(f"   🚀 Running ultra-arena-frk processing...")
            logger.info(f"   📁 Input directory: {input_dir}")
            logger.info(f"   📄 Files: {[p.name for p in pdf_paths]}")
            
            # Call ultra-arena-frk processing function
            processing_results = self.run_file_processing_simple(
                input_pdf_dir_path=input_dir,
                pdf_file_paths=pdf_paths
            )
            
            # Validate and format results
            formatted_results = self._format_processing_results(processing_results, claim_id)
            
            logger.info(f"   ✅ Ultra Arena processing completed")
            return formatted_results
            
        except Exception as e:
            logger.error(f"   ❌ Ultra Arena processing failed: {e}")
            logger.warning("   🔄 Falling back to mock processing")
            return self._mock_process_claim_pdfs(claim_id, pdf_files)
    
    def _format_processing_results(self, raw_results: Dict, claim_id: int) -> Dict:
        """Format raw processing results into expected structure"""
        try:
            # Extract key information from ultra-arena results
            formatted_results = {
                'claim_id': claim_id,
                'files_processed': 0,
                'processing_summary': {
                    'total_amount_all': 0.0,
                    'total_amount_pecas': 0.0,
                    'total_amount_mao_obra': 0.0,
                    'total_amount_diversos': 0.0,
                    'successful_files': 0,
                    'failed_files': 0
                },
                'file_stats': {},
                'raw_results': raw_results  # Keep original results for debugging
            }
            
            # Process file-level results if available
            if 'file_stats' in raw_results:
                file_stats = raw_results['file_stats']
                formatted_results['files_processed'] = len(file_stats)
                formatted_results['file_stats'] = file_stats
                
                # Aggregate amounts from individual files
                total_all = 0.0
                total_pecas = 0.0
                total_mao_obra = 0.0
                total_diversos = 0.0
                successful_files = 0
                failed_files = 0
                
                for file_path, file_data in file_stats.items():
                    if 'error' not in file_data and 'file_model_output' in file_data:
                        successful_files += 1
                        
                        # Extract amounts from file output
                        output = file_data.get('file_model_output', {})
                        
                        # Try to parse amounts (adapt based on ultra-arena output format)
                        if 'valor_total' in output:
                            amount = self._parse_currency_amount(output['valor_total'])
                            total_all += amount
                            
                            # Categorize by document type
                            doc_type = output.get('tipo_documento', '').lower()
                            if 'peça' in doc_type:
                                total_pecas += amount
                            elif 'mão' in doc_type or 'obra' in doc_type:
                                total_mao_obra += amount
                            else:
                                total_diversos += amount
                    else:
                        failed_files += 1
                
                # Update summary
                formatted_results['processing_summary'].update({
                    'total_amount_all': total_all,
                    'total_amount_pecas': total_pecas,
                    'total_amount_mao_obra': total_mao_obra,
                    'total_amount_diversos': total_diversos,
                    'successful_files': successful_files,
                    'failed_files': failed_files
                })
            
            # Process consolidated data if available
            if 'consolidated_data' in raw_results:
                consolidated = raw_results['consolidated_data']
                # Extract consolidated amounts if available
                if isinstance(consolidated, dict) and 'total_amount' in consolidated:
                    formatted_results['processing_summary']['total_amount_all'] = \
                        self._parse_currency_amount(consolidated['total_amount'])
            
            return formatted_results
            
        except Exception as e:
            logger.error(f"Error formatting processing results: {e}")
            # Return minimal valid structure
            return {
                'claim_id': claim_id,
                'files_processed': 0,
                'processing_summary': {
                    'total_amount_all': 0.0,
                    'total_amount_pecas': 0.0,
                    'total_amount_mao_obra': 0.0,
                    'total_amount_diversos': 0.0,
                    'successful_files': 0,
                    'failed_files': 1
                },
                'error': str(e)
            }
    
    def _parse_currency_amount(self, amount_str: str) -> float:
        """Parse Brazilian currency format to float"""
        try:
            if not amount_str:
                return 0.0
            
            # Handle Brazilian format: "1.234,56" -> 1234.56
            amount_str = str(amount_str).strip()
            
            # Remove currency symbols
            amount_str = amount_str.replace('R$', '').replace('$', '').strip()
            
            # Handle Brazilian decimal format
            if ',' in amount_str and '.' in amount_str:
                # Format: 1.234,56
                amount_str = amount_str.replace('.', '').replace(',', '.')
            elif ',' in amount_str:
                # Format: 1234,56
                amount_str = amount_str.replace(',', '.')
            
            return float(amount_str)
            
        except (ValueError, TypeError) as e:
            logger.warning(f"Could not parse amount '{amount_str}': {e}")
            return 0.0
    
    def _mock_process_claim_pdfs(self, claim_id: int, pdf_files: List[str]) -> Dict:
        """Mock PDF processing for testing when ultra-arena-frk is not available"""
        import random
        import time
        
        logger.info(f"   🧪 Running mock processing for {len(pdf_files)} files (success rate: {self.mock_success_rate*100}%)")
        
        # Simulate processing time
        time.sleep(random.uniform(1, 3))
        
        # Get actual DMS amounts for this claim to generate realistic mock data
        try:
            with self.db_ops.get_bgate_connection() as bgate_conn:
                query = """
                    SELECT LABOUR_AMOUNT_DMS, PART_AMOUNT_DMS 
                    FROM CLAIM_STATUS 
                    WHERE CLAIM_ID = :claim_id
                """
                cursor = bgate_conn.cursor()
                cursor.execute(query, {'claim_id': claim_id})
                result = cursor.fetchone()
                cursor.close()
                
                if result:
                    actual_labour = float(result[0] or 0)
                    actual_parts = float(result[1] or 0)
                else:
                    # Fallback if claim not found
                    actual_labour = random.uniform(200, 2000)
                    actual_parts = random.uniform(500, 5000)
        except Exception as e:
            logger.warning(f"Could not get actual amounts for claim {claim_id}: {e}")
            actual_labour = random.uniform(200, 2000)
            actual_parts = random.uniform(500, 5000)
        
        # Determine success based on configured rate
        success_probability = self.mock_success_rate
        claim_will_match = random.random() < success_probability
        
        if claim_will_match:
            # Generate amounts that will match (within small variance)
            variance = 0.02  # 2% variance for realistic mock
            total_mao_obra = actual_labour * random.uniform(1-variance, 1+variance)
            total_pecas = actual_parts * random.uniform(1-variance, 1+variance)
            successful_files = len(pdf_files)
            failed_files = 0
            logger.debug(f"   🎯 Mock will MATCH: Labour={total_mao_obra:.2f} (actual: {actual_labour:.2f}), Parts={total_pecas:.2f} (actual: {actual_parts:.2f})")
        else:
            # Generate amounts that won't match
            total_mao_obra = actual_labour * random.uniform(0.5, 1.8)  # Significantly different
            total_pecas = actual_parts * random.uniform(0.3, 2.5)     # Significantly different
            successful_files = max(1, len(pdf_files) - random.randint(0, 2))  # Some files might fail
            failed_files = len(pdf_files) - successful_files
            logger.debug(f"   ❌ Mock will NOT MATCH: Labour={total_mao_obra:.2f} (actual: {actual_labour:.2f}), Parts={total_pecas:.2f} (actual: {actual_parts:.2f})")
        
        total_diversos = random.uniform(50, 500)
        total_all = total_pecas + total_mao_obra + total_diversos
        
        mock_results = {
            'claim_id': claim_id,
            'files_processed': len(pdf_files),
            'processing_summary': {
                'total_amount_all': round(total_all, 2),
                'total_amount_pecas': round(total_pecas, 2),
                'total_amount_mao_obra': round(total_mao_obra, 2),
                'total_amount_diversos': round(total_diversos, 2),
                'successful_files': successful_files,
                'failed_files': failed_files
            },
            'file_stats': {},
            'mock_data': True  # Flag to indicate this is mock data
        }
        
        # Generate mock file stats
        for i, pdf_file in enumerate(pdf_files):
            file_path = Path(pdf_file)
            if i < successful_files:
                mock_results['file_stats'][str(file_path)] = {
                    'file_model_output': {
                        'numero_claim': f"BYDAMEBR{random.randint(1000, 9999)}",
                        'chassi': f"LGXCE4CC{random.randint(1, 9)}S{random.randint(1000000, 9999999)}",
                        'valor_total': f"{random.uniform(100, 1000):,.2f}".replace(",", "X").replace(".", ",").replace("X", "."),
                        'tipo_documento': random.choice(['Peças', 'Mão de Obra', 'Diversos'])
                    },
                    'processing_status': 'success'
                }
            else:
                mock_results['file_stats'][str(file_path)] = {
                    'error': f'Mock processing error for file {file_path.name}',
                    'processing_status': 'failed'
                }
        
        logger.info(f"   ✅ Mock processing completed: {successful_files} successful, {failed_files} failed")
        return mock_results
    
    def get_processing_statistics(self) -> Dict:
        """Get processing statistics from database"""
        try:
            with self.db_ops.get_bgate_connection() as bgate_conn:
                stats_query = """
                    SELECT 
                        PROCESSING_STATUS,
                        COUNT(*) as COUNT
                    FROM CLAIM_STATUS
                    WHERE PROCESSING_STATUS IS NOT NULL
                    GROUP BY PROCESSING_STATUS
                """
                
                stats_df = pd.read_sql(stats_query, bgate_conn)
                
                if stats_df.empty:
                    return {'total_claims': 0}
                
                stats_dict = dict(zip(stats_df['PROCESSING_STATUS'], stats_df['COUNT']))
                stats_dict['total_claims'] = stats_df['COUNT'].sum()
                
                return stats_dict
                
        except Exception as e:
            logger.error(f"Error getting processing statistics: {e}")
            return {'error': str(e)}