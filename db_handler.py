import oracledb
import pandas as pd
import yaml
import logging
import warnings
from datetime import datetime, timedelta

# Suppress pandas SQLAlchemy warning for Oracle connections
warnings.filterwarnings('ignore', message='pandas only supports SQLAlchemy connectable')

# Initialize Oracle client for THICK mode
try:
    oracledb.init_oracle_client()
    logger = logging.getLogger(__name__)
    logger.info("✅ Oracle client initialized in THICK mode")
except Exception as e:
    logger = logging.getLogger(__name__)
    logger.warning(f"⚠️ Could not initialize Oracle client in THICK mode: {e}")

# Global environment mode
_ENVIRONMENT_MODE = "local"  # default to local

# Global connection pools
_DMS_POOL = None
_BGATE_POOL = None


def set_environment_mode(mode):
    """Set the environment mode for database connections"""
    global _ENVIRONMENT_MODE
    _ENVIRONMENT_MODE = mode


def initialize_connection_pools(min_connections=2, max_connections=10):
    """Initialize connection pools for BGATE database only (DMS uses direct connections)"""
    global _DMS_POOL, _BGATE_POOL
    
    logger.info(f"Starting pool initialization with environment mode: {_ENVIRONMENT_MODE}")
    
    # Verify Oracle client is initialized
    try:
        # Test Oracle client availability
        import oracledb
        logger.info(f"Oracle client version: {oracledb.__version__}")
    except Exception as oracle_error:
        logger.error(f"Oracle client not available: {oracle_error}")
        raise
    
    try:
        db_config = get_current_config()
        logger.debug(f"Database config retrieved for environment: {_ENVIRONMENT_MODE}")
        logger.debug(f"DMS config: user={db_config['dms_db']['user']}, dsn={db_config['dms_db']['dsn']}")
        logger.debug(f"BGATE config: user={db_config['bgate_db']['user']}, dsn={db_config['bgate_db']['dsn']}")
        
        # DMS will use direct connections (THICK mode only)
        logger.info("DMS database will use direct connections (THICK mode)")
        _DMS_POOL = None  # Explicitly set to None to force direct connections
        
        # Initialize BGATE pool only
        logger.info("Initializing BGATE connection pool...")
        try:
            _BGATE_POOL = oracledb.create_pool(
                user=db_config["bgate_db"]["user"],
                password=db_config["bgate_db"]["password"],
                dsn=db_config["bgate_db"]["dsn"],
                min=min_connections,
                max=max_connections,
                increment=1,
                getmode=oracledb.POOL_GETMODE_WAIT
            )
        except Exception as e:
            if "getmode" in str(e):
                logger.info("Falling back to basic pool parameters...")
                _BGATE_POOL = oracledb.create_pool(
                    user=db_config["bgate_db"]["user"],
                    password=db_config["bgate_db"]["password"],
                    dsn=db_config["bgate_db"]["dsn"],
                    min=min_connections,
                    max=max_connections,
                    increment=1
                )
            else:
                raise
        logger.info(f"✅ BGATE connection pool initialized (min={min_connections}, max={max_connections})")
        
        # Verify BGATE pool is accessible globally
        if _BGATE_POOL is None:
            raise Exception("BGATE pool was not properly assigned to global variable")
            
        logger.info(f"✅ Connection pools initialized successfully (DMS: direct, BGATE: pooled)")
        
    except Exception as error:
        logger.error(f"❌ Error initializing connection pools: {error}")
        logger.error(f"❌ Environment mode: {_ENVIRONMENT_MODE}")
        # Don't set pools to None if they were partially initialized
        raise


def get_pool_status():
    """Get current status of connection pools for debugging"""
    global _DMS_POOL, _BGATE_POOL
    
    dms_status = "direct connections" if _DMS_POOL is None else "pooled"
    bgate_status = "initialized" if _BGATE_POOL is not None else "not initialized"
    
    logger.info(f"Pool Status - DMS: {dms_status}, BGATE: {bgate_status}, Environment: {_ENVIRONMENT_MODE}")
    
    if _DMS_POOL:
        try:
            logger.info(f"DMS Pool - Open: {_DMS_POOL.opened}, Busy: {_DMS_POOL.busy}, Max: {_DMS_POOL.max}")
        except:
            logger.warning("Could not get DMS pool statistics")
    else:
        logger.info("DMS using direct connections (THICK mode)")
            
    if _BGATE_POOL:
        try:
            logger.info(f"BGATE Pool - Open: {_BGATE_POOL.opened}, Busy: {_BGATE_POOL.busy}, Max: {_BGATE_POOL.max}")
        except:
            logger.warning("Could not get BGATE pool statistics")


def close_connection_pools():
    """Close connection pools - call during application shutdown"""
    global _DMS_POOL, _BGATE_POOL
    
    try:
        # DMS uses direct connections, no pool to close
        if _DMS_POOL:
            _DMS_POOL.close()
            _DMS_POOL = None
            logger.info("✅ DMS connection pool closed")
        else:
            logger.info("DMS using direct connections - no pool to close")
            
        if _BGATE_POOL:
            _BGATE_POOL.close()
            _BGATE_POOL = None
            logger.info("✅ BGATE connection pool closed")
            
    except Exception as error:
        logger.error(f"❌ Error closing connection pools: {error}")


class DatabaseConnection:
    """Context manager for database connections that properly returns connections to pool"""
    
    def __init__(self, pool_type='bgate', retry_count=3):
        self.pool_type = pool_type
        self.connection = None
        self.from_pool = False
        self.retry_count = retry_count
        
    def __enter__(self):
        if self.pool_type == 'dms':
            self.connection = get_dms_db_connection(retry_count=self.retry_count)
            self.from_pool = False  # DMS always uses direct connections
        else:  # bgate
            self.connection = get_bgate_db_connection()
            self.from_pool = _BGATE_POOL is not None
        return self.connection
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.connection:
            try:
                if self.from_pool:
                    # Return to pool
                    self.connection.close()
                    logger.debug(f"{self.pool_type.upper()} connection returned to pool")
                else:
                    # Direct connection, close normally
                    self.connection.close()
                    logger.debug(f"{self.pool_type.upper()} direct connection closed")
            except Exception as close_error:
                logger.warning(f"Error closing {self.pool_type.upper()} connection: {close_error}")


class DMSConnection:
    """Specialized context manager for DMS connections with enhanced error handling"""
    
    def __init__(self, retry_count=3, operation_name="DMS operation"):
        self.retry_count = retry_count
        self.operation_name = operation_name
        self.connection = None
        
    def __enter__(self):
        self.connection = get_dms_db_connection(retry_count=self.retry_count)
        return self.connection
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.connection:
            try:
                self.connection.close()
                logger.debug(f"DMS connection closed for {self.operation_name}")
            except Exception as close_error:
                logger.warning(f"Error closing DMS connection for {self.operation_name}: {close_error}")
        
        # Handle specific Oracle errors
        if exc_type and issubclass(exc_type, Exception):
            if "ORA-03113" in str(exc_val) or "ORA-03135" in str(exc_val):
                logger.error(f"DMS connection lost during {self.operation_name}: {exc_val}")
                # Don't suppress the exception, let it bubble up for retry at higher level
                return False
        
        return False  # Don't suppress exceptions


# Environment-specific database configurations
LOCAL_CONFIG = {
    "dms_db": {
        "user": "DMS_OEM_SL",
        "password": "-oVDmYP6-,=*",
        "dsn": "10.42.253.86:1027/dms11g",
    },
    "bgate_db": {
        "user": "temp_dms",
        "password": "0<wS16q:F}|o.+",
        "dsn": "10.42.253.86:1092/dms19g_pdb1",
    },
}

UAT_CONFIG = {
    "dms_db": {
        "user": "DMS_OEM_SL",
        "password": "-oVDmYP6-,=*",
        "dsn": "10.42.253.27:1521/dms11g",
    },
    "bgate_db": {
        "user": "temp_dms",
        "password": "0<wS16q:F}|o.+",
        "dsn": "10.42.253.92:1521/dms19g_pdb1",
    },
}

PROD_CONFIG = {
    "dms_db": {
        "user": "DMS_OEM_SL",
        "password": "-oVDmYP6-,=*",
        "dsn": "10.42.253.27:1521/dms11g",
    },
    "bgate_db": {
        "user": "prod_dms",
        "password": "1gHH16Dkjqyj:>D",
        "dsn": "10.42.253.92:1521/dms19g_pdb1",
    },
}


def get_current_config():
    """Get the current configuration based on environment mode"""
    if _ENVIRONMENT_MODE == "uat":
        return UAT_CONFIG
    elif _ENVIRONMENT_MODE == "prod":
        return PROD_CONFIG
    else:
        return LOCAL_CONFIG


def load_config():
    """Loads configuration from config.yaml"""
    try:
        with open("config.yaml", "r") as f:
            return yaml.safe_load(f)
    except FileNotFoundError:
        logger.error("config.yaml not found")
        raise
    except yaml.YAMLError as e:
        logger.error(f"Error parsing config.yaml: {e}")
        raise


def get_dms_db_connection(retry_count=3, retry_delay=2):
    """Gets a direct connection to the DMS database (for reading) using Oracle DB THICK mode with retry logic."""
    global _DMS_POOL
    
    db_config = get_current_config()["dms_db"]
    last_error = None
    
    for attempt in range(retry_count):
        try:
            connection = oracledb.connect(
                user=db_config["user"],
                password=db_config["password"],
                dsn=db_config["dsn"],
                mode=oracledb.DEFAULT_AUTH,
            )
            
            # Test the connection immediately
            cursor = connection.cursor()
            cursor.execute("SELECT 1 FROM DUAL")
            cursor.fetchone()
            cursor.close()
            
            if attempt > 0:
                logger.info(f"DMS Database connection established (THICK mode - attempt {attempt + 1})")
            else:
                logger.debug("DMS Database connection established (THICK mode - direct)")
            return connection
            
        except oracledb.Error as error:
            last_error = error
            if attempt < retry_count - 1:
                logger.warning(f"DMS connection attempt {attempt + 1} failed: {error}. Retrying in {retry_delay} seconds...")
                import time
                time.sleep(retry_delay)
                retry_delay *= 2  # Exponential backoff
            else:
                logger.error(f"All {retry_count} DMS connection attempts failed. Last error: {error}")
    
    # If we get here, all attempts failed
    raise last_error


def get_bgate_db_connection():
    """Gets a connection from the BGATE database pool (for writing)."""
    global _BGATE_POOL
    
    logger.debug(f"get_bgate_db_connection called, pool status: {_BGATE_POOL is not None}")
    
    if _BGATE_POOL is None:
        # Fallback to direct connection if pool not initialized
        logger.warning(f"BGATE pool not initialized (env: {_ENVIRONMENT_MODE}), creating direct connection")
        db_config = get_current_config()["bgate_db"]
        try:
            connection = oracledb.connect(
                user=db_config["user"],
                password=db_config["password"],
                dsn=db_config["dsn"],
                mode=oracledb.DEFAULT_AUTH,
            )
            logger.info("BGATE Database connection established (THICK mode - direct)")
            return connection
        except oracledb.Error as error:
            logger.error(f"Error connecting to BGATE Oracle Database: {error}")
            raise
    
    try:
        connection = _BGATE_POOL.acquire()
        logger.debug("BGATE Database connection acquired from pool")
        return connection
    except oracledb.Error as error:
        logger.error(f"Error acquiring BGATE connection from pool: {error}")
        raise


def get_region_id(connection, region_name="巴西"):
    """Get Brazil region ID from DMS database"""
    try:
        query = "SELECT REGION_ID FROM DMS_OEM_PROD.TM_REGION WHERE REGION_NAME = :region_name"
        cursor = connection.cursor()
        cursor.execute(query, {"region_name": region_name})
        result = cursor.fetchone()
        cursor.close()

        if result:
            logger.info(f"Found region ID {result[0]} for '{region_name}'")
            return result[0]
        else:
            logger.error(f"Region '{region_name}' not found")
            return None
    except oracledb.Error as error:
        logger.error(f"Error getting region ID: {error}")
        return None


def get_status_code_id(connection, type_code=5618, target_description="待审核付款凭证"):
    """
    Get status code ID for 'Payment documents TO be audited' from DMS database
    Searches by CODE_DESC to find the correct CODE_ID
    """
    try:
        query = """
            SELECT CODE_ID, CODE_DESC 
            FROM DMS_OEM_PROD.TC_CODE 
            WHERE TYPE = :type_code 
            AND CODE_DESC = :target_description
        """
        cursor = connection.cursor()
        cursor.execute(
            query, {"type_code": type_code, "target_description": target_description}
        )
        result = cursor.fetchone()
        cursor.close()

        if result:
            code_id = result[0]
            code_desc = result[1]
            logger.info(
                f"Found status code ID {code_id} for description '{code_desc}' (type {type_code})"
            )
            return code_id
        else:
            # If exact match fails, try to find all codes for this type for debugging
            logger.warning(
                f"Exact match not found for '{target_description}'. Searching all codes for type {type_code}..."
            )

            debug_query = """
                SELECT CODE_ID, CODE_DESC 
                FROM DMS_OEM_PROD.TC_CODE 
                WHERE TYPE = :type_code
                ORDER BY CODE_ID
            """
            cursor = connection.cursor()
            cursor.execute(debug_query, {"type_code": type_code})
            debug_results = cursor.fetchall()
            cursor.close()

            logger.info(f"Available codes for type {type_code}:")
            for code_id, code_desc in debug_results:
                logger.info(f"  {code_id}: {code_desc}")
                # Try partial match as fallback
                if target_description in code_desc:
                    logger.info(f"Found partial match: {code_id} - {code_desc}")
                    return code_id

            logger.error(
                f"Status code with description '{target_description}' not found for type {type_code}"
            )
            return None

    except oracledb.Error as error:
        logger.error(f"Error getting status code ID: {error}")
        return None


def get_dms_region_and_status_ids():
    """
    Helper function to get region and status IDs with lazy DMS connection.
    Returns tuple (region_id, status_id) or (None, None) on failure.
    """
    try:
        with DMSConnection(operation_name="get_region_status_ids") as dms_connection:
            region_id = get_region_id(dms_connection)
            status_id = get_status_code_id(dms_connection)
            return region_id, status_id
    except Exception as error:
        logger.error(f"❌ Error getting DMS region and status IDs: {error}")
        return None, None


def should_skip_dms_operations():
    """
    Check if we can skip DMS operations by examining local state first.
    Returns True if we can skip, False if we need to proceed.
    """
    try:
        with DatabaseConnection('bgate') as bgate_connection:
            # Check if we have any claims that might need processing
            check_query = """
                SELECT COUNT(*) 
                FROM CLAIM_STATUS 
                WHERE ATTACHMENT_STATUS != 'COMPLETE' 
                OR AUDIT_STATUS IS NULL 
                OR AUDIT_STATUS = 'PENDING'
            """
            cursor = bgate_connection.cursor()
            cursor.execute(check_query)
            pending_count = cursor.fetchone()[0]
            cursor.close()
            
            if pending_count == 0:
                logger.info("No pending claims found, skipping DMS operations")
                return True
                
            logger.debug(f"Found {pending_count} claims that may need DMS data")
            return False
            
    except Exception as error:
        logger.warning(f"Could not check local state, proceeding with DMS operations: {error}")
        return False

def convert_pandas_types_for_oracle(data):
    """
    Convert pandas/numpy data types to Python native types for Oracle compatibility.
    
    Args:
        data (dict): Dictionary with potentially numpy/pandas typed values
        
    Returns:
        dict: Dictionary with Oracle-compatible Python native types
    """
    safe_data = {}
    for key, value in data.items():
        if pd.isna(value):
            safe_data[key] = None
        elif hasattr(value, 'dtype'):  # numpy types
            if 'int' in str(value.dtype):
                safe_data[key] = int(value)
            elif 'float' in str(value.dtype):
                safe_data[key] = float(value)
            else:
                safe_data[key] = value.item() if hasattr(value, 'item') else str(value)
        else:
            safe_data[key] = value
    return safe_data


def upsert_claim_status(claim_data):
    """
    Update or insert claim status in CLAIM_STATUS table.

    Args:
        claim_data (dict): Dictionary containing claim information with keys:
            CLAIM_ID, CLAIM_NO, VIN, DEALER_CODE, DEALER_NAME, REPORT_DATE,
            GROSS_CREDIT, LABOUR_AMOUNT_DMS, PART_AMOUNT_DMS, LAST_DMS_UPDATE_DATE, AUDITING_DATE
    """
    try:
        with DatabaseConnection('bgate') as connection:
            cursor = connection.cursor()
            
            merge_query = """
                MERGE INTO CLAIM_STATUS dest
                USING (
                    SELECT 
                        :claim_id AS CLAIM_ID,
                        :claim_no AS CLAIM_NO,
                        :vin AS VIN,
                        :dealer_code AS DEALER_CODE,
                        :dealer_name AS DEALER_NAME,
                        :report_date AS REPORT_DATE,
                        :gross_credit AS GROSS_CREDIT,
                        :labour_amount AS LABOUR_AMOUNT_DMS,
                        :part_amount AS PART_AMOUNT_DMS,
                        :last_dms_update AS LAST_DMS_UPDATE_DATE,
                        :auditing_date AS AUDITING_DATE
                    FROM DUAL
                ) src ON (dest.CLAIM_ID = src.CLAIM_ID)
                WHEN MATCHED THEN
                    UPDATE SET
                        dest.CLAIM_NO = src.CLAIM_NO,
                        dest.VIN = src.VIN,
                        dest.DEALER_CODE = src.DEALER_CODE,
                        dest.DEALER_NAME = src.DEALER_NAME,
                        dest.REPORT_DATE = src.REPORT_DATE,
                        dest.GROSS_CREDIT = src.GROSS_CREDIT,
                        dest.LABOUR_AMOUNT_DMS = src.LABOUR_AMOUNT_DMS,
                        dest.PART_AMOUNT_DMS = src.PART_AMOUNT_DMS,
                        dest.LAST_DMS_UPDATE_DATE = src.LAST_DMS_UPDATE_DATE,
                        dest.AUDITING_DATE = src.AUDITING_DATE,
                        dest.LAST_MODIFIED_DATE = CURRENT_TIMESTAMP
                    WHERE (
                        dest.CLAIM_NO != src.CLAIM_NO OR
                        dest.VIN != src.VIN OR
                        dest.DEALER_CODE != src.DEALER_CODE OR
                        dest.DEALER_NAME != src.DEALER_NAME OR
                        dest.REPORT_DATE != src.REPORT_DATE OR
                        dest.GROSS_CREDIT != src.GROSS_CREDIT OR
                        dest.LABOUR_AMOUNT_DMS != src.LABOUR_AMOUNT_DMS OR
                        dest.PART_AMOUNT_DMS != src.PART_AMOUNT_DMS OR
                        dest.LAST_DMS_UPDATE_DATE != src.LAST_DMS_UPDATE_DATE OR
                        dest.AUDITING_DATE != src.AUDITING_DATE OR
                        (dest.CLAIM_NO IS NULL AND src.CLAIM_NO IS NOT NULL) OR
                        (dest.VIN IS NULL AND src.VIN IS NOT NULL) OR
                        (dest.DEALER_CODE IS NULL AND src.DEALER_CODE IS NOT NULL) OR
                        (dest.DEALER_NAME IS NULL AND src.DEALER_NAME IS NOT NULL) OR
                        (dest.REPORT_DATE IS NULL AND src.REPORT_DATE IS NOT NULL) OR
                        (dest.GROSS_CREDIT IS NULL AND src.GROSS_CREDIT IS NOT NULL) OR
                        (dest.LABOUR_AMOUNT_DMS IS NULL AND src.LABOUR_AMOUNT_DMS IS NOT NULL) OR
                        (dest.PART_AMOUNT_DMS IS NULL AND src.PART_AMOUNT_DMS IS NOT NULL) OR
                        (dest.LAST_DMS_UPDATE_DATE IS NULL AND src.LAST_DMS_UPDATE_DATE IS NOT NULL) OR
                        (dest.AUDITING_DATE IS NULL AND src.AUDITING_DATE IS NOT NULL)
                    )
                WHEN NOT MATCHED THEN
                    INSERT (
                        CLAIM_ID, CLAIM_NO, VIN, DEALER_CODE, DEALER_NAME, REPORT_DATE,
                        GROSS_CREDIT, LABOUR_AMOUNT_DMS, PART_AMOUNT_DMS, 
                        LAST_DMS_UPDATE_DATE, AUDITING_DATE, ATTACHMENT_STATUS
                    )
                    VALUES (
                        src.CLAIM_ID, src.CLAIM_NO, src.VIN, src.DEALER_CODE, src.DEALER_NAME, src.REPORT_DATE,
                        src.GROSS_CREDIT, src.LABOUR_AMOUNT_DMS, src.PART_AMOUNT_DMS,
                        src.LAST_DMS_UPDATE_DATE, src.AUDITING_DATE, 'PENDING'
                    )
            """

            # Convert pandas/numpy types to Oracle-compatible Python types
            safe_claim_data = convert_pandas_types_for_oracle(claim_data)
            
            cursor.execute(merge_query, safe_claim_data)
            rows_affected = cursor.rowcount
            connection.commit()
            cursor.close()

            if rows_affected > 0:
                logger.info(f"✅ Updated claim status for CLAIM_ID {safe_claim_data['claim_id']} ({rows_affected} rows affected)")
            else:
                logger.debug(f"📋 No changes needed for CLAIM_ID {safe_claim_data['claim_id']} (data unchanged)")

    except Exception as error:
        logger.error(
            f"❌ Error upserting claim status for CLAIM_ID {claim_data.get('claim_id', 'unknown')}: {error}"
        )
        raise


def batch_upsert_claim_status(claims_df, batch_size=500):
    """
    High-performance batch upsert for claim status data.
    Processes DataFrame in chunks for optimal performance.
    
    Args:
        claims_df (pd.DataFrame): DataFrame containing claim data
        batch_size (int): Number of records to process per batch
    """
    if claims_df.empty:
        logger.info("No claims to upsert")
        return
    
    total_claims = len(claims_df)
    total_batches = (total_claims + batch_size - 1) // batch_size
    processed_count = 0
    
    logger.info(f"Starting batch upsert of {total_claims} claims in {total_batches} batches of {batch_size}")
    
    for batch_num in range(total_batches):
        start_idx = batch_num * batch_size
        end_idx = min(start_idx + batch_size, total_claims)
        batch_df = claims_df.iloc[start_idx:end_idx]
        
        try:
            with DatabaseConnection('bgate') as connection:
                cursor = connection.cursor()
                
                # Prepare batch data with proper type conversion
                batch_data = []
                for _, row in batch_df.iterrows():
                    raw_claim_data = {
                        "claim_id": row["CLAIM_ID"],
                        "claim_no": row["CLAIM_NO"],
                        "vin": row["VIN"],
                        "dealer_code": row["DEALER_CODE"],
                        "dealer_name": row["DEALER_NAME"],
                        "report_date": row["REPORT_DATE"],
                        "gross_credit": row["GROSS_CREDIT"],
                        "labour_amount": row["LABOUR_AMOUNT"],
                        "part_amount": row["PART_AMOUNT"],
                        "last_dms_update": row["UPDATE_DATE"],
                        "auditing_date": row["AUDITING_DATE"],
                    }
                    # Convert pandas/numpy types to Oracle-compatible types
                    claim_data = convert_pandas_types_for_oracle(raw_claim_data)
                    batch_data.append(claim_data)
                
                # Execute batch merge using executemany for better performance
                merge_query = """
                    MERGE INTO CLAIM_STATUS dest
                    USING (
                        SELECT 
                            :claim_id AS CLAIM_ID,
                            :claim_no AS CLAIM_NO,
                            :vin AS VIN,
                            :dealer_code AS DEALER_CODE,
                            :dealer_name AS DEALER_NAME,
                            :report_date AS REPORT_DATE,
                            :gross_credit AS GROSS_CREDIT,
                            :labour_amount AS LABOUR_AMOUNT_DMS,
                            :part_amount AS PART_AMOUNT_DMS,
                            :last_dms_update AS LAST_DMS_UPDATE_DATE,
                            :auditing_date AS AUDITING_DATE
                        FROM DUAL
                    ) src ON (dest.CLAIM_ID = src.CLAIM_ID)
                    WHEN MATCHED THEN
                        UPDATE SET
                            dest.CLAIM_NO = src.CLAIM_NO,
                            dest.VIN = src.VIN,
                            dest.DEALER_CODE = src.DEALER_CODE,
                            dest.DEALER_NAME = src.DEALER_NAME,
                            dest.REPORT_DATE = src.REPORT_DATE,
                            dest.GROSS_CREDIT = src.GROSS_CREDIT,
                            dest.LABOUR_AMOUNT_DMS = src.LABOUR_AMOUNT_DMS,
                            dest.PART_AMOUNT_DMS = src.PART_AMOUNT_DMS,
                            dest.LAST_DMS_UPDATE_DATE = src.LAST_DMS_UPDATE_DATE,
                            dest.AUDITING_DATE = src.AUDITING_DATE,
                            dest.LAST_MODIFIED_DATE = CURRENT_TIMESTAMP
                        WHERE (
                            dest.CLAIM_NO != src.CLAIM_NO OR
                            dest.VIN != src.VIN OR
                            dest.DEALER_CODE != src.DEALER_CODE OR
                            dest.DEALER_NAME != src.DEALER_NAME OR
                            dest.REPORT_DATE != src.REPORT_DATE OR
                            dest.GROSS_CREDIT != src.GROSS_CREDIT OR
                            dest.LABOUR_AMOUNT_DMS != src.LABOUR_AMOUNT_DMS OR
                            dest.PART_AMOUNT_DMS != src.PART_AMOUNT_DMS OR
                            dest.LAST_DMS_UPDATE_DATE != src.LAST_DMS_UPDATE_DATE OR
                            dest.AUDITING_DATE != src.AUDITING_DATE OR
                            (dest.CLAIM_NO IS NULL AND src.CLAIM_NO IS NOT NULL) OR
                            (dest.VIN IS NULL AND src.VIN IS NOT NULL) OR
                            (dest.DEALER_CODE IS NULL AND src.DEALER_CODE IS NOT NULL) OR
                            (dest.DEALER_NAME IS NULL AND src.DEALER_NAME IS NOT NULL) OR
                            (dest.REPORT_DATE IS NULL AND src.REPORT_DATE IS NOT NULL) OR
                            (dest.GROSS_CREDIT IS NULL AND src.GROSS_CREDIT IS NOT NULL) OR
                            (dest.LABOUR_AMOUNT_DMS IS NULL AND src.LABOUR_AMOUNT_DMS IS NOT NULL) OR
                            (dest.PART_AMOUNT_DMS IS NULL AND src.PART_AMOUNT_DMS IS NOT NULL) OR
                            (dest.LAST_DMS_UPDATE_DATE IS NULL AND src.LAST_DMS_UPDATE_DATE IS NOT NULL) OR
                            (dest.AUDITING_DATE IS NULL AND src.AUDITING_DATE IS NOT NULL)
                        )
                    WHEN NOT MATCHED THEN
                        INSERT (
                            CLAIM_ID, CLAIM_NO, VIN, DEALER_CODE, DEALER_NAME, REPORT_DATE,
                            GROSS_CREDIT, LABOUR_AMOUNT_DMS, PART_AMOUNT_DMS, 
                            LAST_DMS_UPDATE_DATE, AUDITING_DATE, ATTACHMENT_STATUS
                        )
                        VALUES (
                            src.CLAIM_ID, src.CLAIM_NO, src.VIN, src.DEALER_CODE, src.DEALER_NAME, src.REPORT_DATE,
                            src.GROSS_CREDIT, src.LABOUR_AMOUNT_DMS, src.PART_AMOUNT_DMS,
                            src.LAST_DMS_UPDATE_DATE, src.AUDITING_DATE, 'PENDING'
                        )
                """
                
                cursor.executemany(merge_query, batch_data)
                rows_affected = cursor.rowcount
                connection.commit()
                cursor.close()
                
                processed_count += len(batch_data)
                progress_percent = (processed_count / total_claims) * 100
                
                logger.info(f"✅ Batch {batch_num + 1}/{total_batches}: {len(batch_data)} claims processed ({rows_affected} rows affected) - {progress_percent:.1f}% complete")
                
        except Exception as error:
            logger.error(f"❌ Error in batch {batch_num + 1}: {error}")
            # Continue with next batch instead of failing entirely
            continue
    
    logger.info(f"✅ Batch upsert completed: {processed_count}/{total_claims} claims processed")


def get_claims_needing_download():
    """
    Get claims that need files downloaded by comparing DMS UPDATE_DATE with stored LAST_DMS_UPDATE_DATE.
    Returns DataFrame with claims that are new or have been updated in DMS.
    Uses lazy DMS connection instantiation.
    """
    try:
        # First get BGATE data to see if we even need to query DMS
        with DatabaseConnection('bgate') as bgate_connection:
            bgate_query = """
                SELECT 
                    CLAIM_ID,
                    LAST_DMS_UPDATE_DATE,
                    ATTACHMENT_STATUS
                FROM CLAIM_STATUS
            """
            bgate_df = pd.read_sql(bgate_query, bgate_connection)

        # Get region and status IDs with lazy DMS connection
        region_id, status_id = get_dms_region_and_status_ids()
        
        if not region_id or not status_id:
            logger.error("Failed to get required region_id or status_id")
            return pd.DataFrame()

        # Now create DMS connection only for the main query
        with DMSConnection(operation_name="get_claims_data") as dms_connection:
            # Updated DMS query with new fields
            dms_query = """
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
                    AND claims.REPORT_DATE BETWEEN TO_DATE('2020-07-23', 'YYYY-MM-DD') 
                                                AND SYSDATE
                    AND claims.UPDATE_DATE < SYSDATE
                ORDER BY claims.REPORT_DATE ASC, claims.UPDATE_DATE ASC
            """

            # Get all claims from DMS
            dms_df = pd.read_sql(
                dms_query,
                dms_connection,
                params={
                    "region_id": region_id,
                    "status_id": status_id,
                },
            )

        # DMS connection automatically closed here

        if dms_df.empty:
            logger.info("✅ No claims found in DMS database")
            return pd.DataFrame()

        logger.info(f"Found {len(dms_df)} claims in DMS database")

        # Merge to find claims needing updates
        if not bgate_df.empty:
            merged_df = dms_df.merge(bgate_df, on="CLAIM_ID", how="left")

            # Claims need download if:
            # 1. New claims (not in CLAIM_STATUS)
            # 2. DMS UPDATE_DATE > stored LAST_DMS_UPDATE_DATE
            # 3. Claims with ATTACHMENT_STATUS != 'COMPLETE'
            needs_download = merged_df[
                merged_df["LAST_DMS_UPDATE_DATE"].isna()  # New claims
                | (merged_df["UPDATE_DATE"] > merged_df["LAST_DMS_UPDATE_DATE"])  # Updated claims
                | (merged_df["ATTACHMENT_STATUS"] != "COMPLETE")  # Incomplete downloads
            ]
        else:
            # No existing claims, all are new
            needs_download = dms_df

        logger.info(f"✅ Found {len(needs_download)} claims needing download")

        # Batch upsert claim status for all claims (update metadata)
        if not dms_df.empty:
            config = load_config()
            batch_size = config.get("database", {}).get("batch_upsert_size", 500)
            logger.info(f"Batch upserting {len(dms_df)} claims...")
            batch_upsert_claim_status(dms_df, batch_size=batch_size)
            logger.info(f"✅ Batch upsert completed")

        logger.info(f"Returning {len(needs_download)} downloadable claims from {len(dms_df)} total claims")

        return needs_download

    except Exception as error:
        logger.error(f"❌ Error getting claims needing download: {error}")
        return pd.DataFrame()


def get_new_files_to_download(max_claims=1000):
    """
    Gets PDF files for claims that need downloading with robust connection handling and early filtering.
    
    Args:
        max_claims (int): Maximum number of claims to process in one batch (performance limit)
    """
    max_retries = 2
    
    for attempt in range(max_retries):
        try:
            return _get_new_files_to_download_impl(max_claims)
        except Exception as error:
            if attempt < max_retries - 1 and ("ORA-03113" in str(error) or "ORA-03135" in str(error)):
                logger.warning(f"DMS connection failed on attempt {attempt + 1}, retrying: {error}")
                import time
                time.sleep(3)  # Wait before retry
                continue
            else:
                logger.error(f"❌ Error getting new files to download (attempt {attempt + 1}): {error}")
                return pd.DataFrame()
    
    return pd.DataFrame()


def _get_new_files_to_download_impl(max_claims=None):
    """
    Internal implementation of get_new_files_to_download with robust connection handling and performance optimization.
    """
    try:
        # Load config for performance settings
        config = load_config()
        max_claims = config.get("database", {}).get("max_claims_per_cycle")
        batch_size = config.get("database", {}).get("file_query_batch_size", 500)
        
        # First, get claims that need downloading with early filtering
        claims_needing_download = get_claims_needing_download()

        if claims_needing_download.empty:
            logger.info("No claims need file downloads")
            return pd.DataFrame()

        # Limit the number of claims processed in one batch for performance
        if max_claims:
            if len(claims_needing_download) > max_claims:
                logger.info(f"Limiting processing to {max_claims} claims out of {len(claims_needing_download)} total (configured limit)")
                # Sort by priority (most recent updates first)
                claims_needing_download = claims_needing_download.sort_values('UPDATE_DATE', ascending=False).head(max_claims)
        else:
            logger.info("No max_claims configured, processing all claims.")
            claims_needing_download = claims_needing_download.sort_values('UPDATE_DATE', ascending=False)
        claim_ids = claims_needing_download["CLAIM_ID"].tolist()
        logger.info(f"Getting files for {len(claim_ids)} claims")

        # Get region and status IDs with lazy DMS connection
        region_id, status_id = get_dms_region_and_status_ids()
        
        if not region_id or not status_id:
            logger.error("Failed to get required region_id or status_id")
            return pd.DataFrame()

        # For each claim that needs downloading, mark old files as obsolete
        if len(claim_ids) > 1000:
            logger.info(f"Marking old files as obsolete for {len(claim_ids)} claims (will process in batches due to Oracle limit)...")
        else:
            logger.info(f"Marking old files as obsolete for {len(claim_ids)} claims...")
        mark_old_files_obsolete(claim_ids)

        # Get PDF files for claims needing download, in smaller batches for better performance
        all_files = []
        total_batches = (len(claim_ids) + batch_size - 1) // batch_size

        logger.info(f"Processing {len(claim_ids)} claims in {total_batches} batches of {batch_size} (configured size)")

        for i in range(0, len(claim_ids), batch_size):
            batch_claim_ids = claim_ids[i : i + batch_size]
            batch_num = (i // batch_size) + 1
            
            placeholders = ",".join([f":id{j}" for j in range(len(batch_claim_ids))])

            files_query = f"""
                SELECT
                    claims.CLAIM_ID,
                    claims.CLAIM_NO,
                    claims.VIN,
                    claims.GROSS_CREDIT,
                    claims.REPORT_DATE,
                    claims.LABOUR_AMOUNT,
                    claims.PART_AMOUNT,
                    claims.UPDATE_DATE,
                    td.DEALER_CODE,
                    td.DEALER_NAME,
                    files.FILE_ID,
                    files.FILE_NAME,
                    files.CREATE_DATE
                FROM
                    DMS_OEM_PROD.SEC_TT_AS_WR_APPLICATION_V claims
                JOIN
                    DMS_OEM_PROD.TC_FILE_UPLOAD_INFO files ON claims.CLAIM_ID = files.BILL_ID
                JOIN
                    DMS_OEM_PROD.TM_DEALER td ON claims.DEALER_ID = td.DEALER_ID
                WHERE
                    claims.CLAIM_ID IN ({placeholders})
                    AND files.FILE_TYPE_DETAIL = '.pdf'
                ORDER BY claims.CLAIM_ID, files.CREATE_DATE ASC
            """

            params = {f"id{j}": claim_id for j, claim_id in enumerate(batch_claim_ids)}

            # Use fresh DMS connection for each batch to avoid timeout issues
            with DMSConnection(operation_name=f"get_files_batch_{batch_num}") as dms_connection:
                cursor = dms_connection.cursor()
                cursor.execute(files_query, params)
                batch_results = cursor.fetchall()
                cursor.close()

            all_files.extend(batch_results)
            logger.info(f"✅ Batch {batch_num}/{total_batches}: Found {len(batch_results)} files for {len(batch_claim_ids)} claims")

        if not all_files:
            logger.info("No PDF files found for claims needing download")
            return pd.DataFrame()

        # Convert to DataFrame
        columns = [
            "CLAIM_ID",
            "CLAIM_NO",
            "VIN",
            "GROSS_CREDIT",
            "REPORT_DATE",
            "LABOUR_AMOUNT",
            "PART_AMOUNT",
            "UPDATE_DATE",
            "DEALER_CODE",
            "DEALER_NAME",
            "FILE_ID",
            "FILE_NAME",
            "CREATE_DATE",
        ]
        files_df = pd.DataFrame(all_files, columns=columns)  # type: ignore

        # Update total file counts for each claim in batches
        logger.info("Updating file counts...")
        file_counts = (
            files_df.groupby("CLAIM_ID").size().reset_index(name="total_files")
        )
        
        # Batch update file counts for better performance
        batch_update_file_counts(file_counts)

        logger.info(
            f"✅ Found {len(files_df)} PDF files to download for {len(claim_ids)} claims"
        )

        return files_df

    except Exception as error:
        logger.error(f"❌ Error in _get_new_files_to_download_impl: {error}")
        raise


def batch_update_file_counts(file_counts_df, batch_size=100):
    """
    Batch update file counts for better performance.
    
    Args:
        file_counts_df (pd.DataFrame): DataFrame with CLAIM_ID and total_files columns
        batch_size (int): Number of updates per batch
    """
    if file_counts_df.empty:
        return
        
    total_updates = len(file_counts_df)
    total_batches = (total_updates + batch_size - 1) // batch_size
    
    logger.debug(f"Updating file counts for {total_updates} claims in {total_batches} batches")
    
    for batch_num in range(total_batches):
        start_idx = batch_num * batch_size
        end_idx = min(start_idx + batch_size, total_updates)
        batch_df = file_counts_df.iloc[start_idx:end_idx]
        
        try:
            with DatabaseConnection('bgate') as connection:
                cursor = connection.cursor()
                
                update_data = []
                for _, row in batch_df.iterrows():
                    raw_data = {
                        "total_files": row["total_files"],
                        "claim_id": row["CLAIM_ID"]
                    }
                    # Convert pandas/numpy types to Oracle-compatible types
                    safe_data = convert_pandas_types_for_oracle(raw_data)
                    update_data.append(safe_data)
                
                update_query = """
                    UPDATE CLAIM_STATUS 
                    SET TOTAL_FILES_COUNT = :total_files,
                        LAST_MODIFIED_DATE = CURRENT_TIMESTAMP
                    WHERE CLAIM_ID = :claim_id
                """
                
                cursor.executemany(update_query, update_data)
                connection.commit()
                cursor.close()
                
        except Exception as error:
            logger.error(f"❌ Error updating file counts batch {batch_num + 1}: {error}")
            continue
    
    logger.debug(f"✅ File count updates completed")


def mark_old_files_obsolete(claim_ids):
    """
    Mark existing PDF files for claims as obsolete (IS_LATEST_VERSION = 'N')
    when the claims have been updated in DMS.
    Handles Oracle's 1000-item IN clause limit by processing in batches.
    """
    if not claim_ids:
        return  # Nothing to do
    
    # Convert single ID to list for consistency
    if not isinstance(claim_ids, list):
        claim_ids = [claim_ids]
    
    total_updated = 0
    batch_size = 999  # Oracle limit is 1000, use 999 to be safe
    total_batches = (len(claim_ids) + batch_size - 1) // batch_size
    
    logger.debug(f"Marking old files obsolete for {len(claim_ids)} claims in {total_batches} batches")
    
    for batch_num in range(total_batches):
        start_idx = batch_num * batch_size
        end_idx = min(start_idx + batch_size, len(claim_ids))
        batch_claim_ids = claim_ids[start_idx:end_idx]
        
        try:
            with DatabaseConnection('bgate') as connection:
                cursor = connection.cursor()

                # Build the correct number of bind variables for the IN clause
                bind_vars = ','.join([f':id{i}' for i in range(len(batch_claim_ids))])
                update_query = f"""
                    UPDATE PDF_DOWNLOAD_DMS_CLAIMS 
                    SET IS_LATEST_VERSION = 'N',
                        LAST_MODIFIED_DATE = CURRENT_TIMESTAMP
                    WHERE CLAIM_ID IN ({bind_vars})
                    AND IS_LATEST_VERSION = 'Y'
                """

                # Build the parameter dictionary with type conversion
                params = {}
                for i, claim_id in enumerate(batch_claim_ids):
                    # Convert numpy int64 to Python int if needed
                    if hasattr(claim_id, 'dtype'):
                        params[f'id{i}'] = int(claim_id)
                    else:
                        params[f'id{i}'] = claim_id

                cursor.execute(update_query, params)
                batch_updated = cursor.rowcount
                connection.commit()
                cursor.close()
                
                total_updated += batch_updated
                
                if batch_updated > 0:
                    logger.debug(f"Batch {batch_num + 1}/{total_batches}: Marked {batch_updated} files obsolete")
                    
        except Exception as error:
            logger.error(f"❌ Error marking old files obsolete for batch {batch_num + 1}: {error}")
            # Continue with next batch instead of failing entirely
            continue
    
    if total_updated > 0:
        # Show sample of claim IDs for logging
        sample_ids = claim_ids[:3]
        id_display = f"{sample_ids}{'...' if len(claim_ids) > 3 else ''}"
        logger.info(f"✅ Marked {total_updated} files as obsolete for {len(claim_ids)} claims {id_display}")
    else:
        logger.debug(f"No files needed to be marked obsolete for {len(claim_ids)} claims")


def update_claim_file_count(claim_id, total_files):
    """Update the total file count for a claim"""
    connection = None
    cursor = None

    try:
        connection = get_bgate_db_connection()
        cursor = connection.cursor()

        update_query = """
            UPDATE CLAIM_STATUS 
            SET TOTAL_FILES_COUNT = :total_files,
                LAST_MODIFIED_DATE = CURRENT_TIMESTAMP
            WHERE CLAIM_ID = :claim_id
        """

        cursor.execute(update_query, {"total_files": total_files, "claim_id": claim_id})
        connection.commit()

        logger.debug(f"Updated file count for CLAIM_ID {claim_id}: {total_files} files")

    except Exception as error:
        logger.error(f"❌ Error updating file count for CLAIM_ID {claim_id}: {error}")
        if connection:
            connection.rollback()
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()


def update_attachment_status(claim_id):
    """
    Update the attachment status for a claim based on download progress.
    Calculates status as PENDING/PARTIAL/COMPLETE based on successful downloads.
    """
    connection = None
    cursor = None

    try:
        connection = get_bgate_db_connection()
        cursor = connection.cursor()

        # Get download statistics for this claim
        stats_query = """
            SELECT 
                cs.TOTAL_FILES_COUNT,
                COUNT(CASE WHEN pdf.STATUS = 'SUCCESS' AND pdf.IS_LATEST_VERSION = 'Y' THEN 1 END) AS SUCCESS_COUNT,
                COUNT(CASE WHEN pdf.IS_LATEST_VERSION = 'Y' THEN 1 END) AS TOTAL_ATTEMPTED
            FROM CLAIM_STATUS cs
            LEFT JOIN PDF_DOWNLOAD_DMS_CLAIMS pdf ON cs.CLAIM_ID = pdf.CLAIM_ID
            WHERE cs.CLAIM_ID = :claim_id
            GROUP BY cs.TOTAL_FILES_COUNT
        """

        cursor.execute(stats_query, {"claim_id": claim_id})
        result = cursor.fetchone()

        if not result:
            logger.warning(f"No claim found for CLAIM_ID {claim_id}")
            return

        total_files, success_count, total_attempted = result

        # Determine attachment status
        if success_count == 0:
            attachment_status = "PENDING"
        elif success_count == total_files:
            attachment_status = "COMPLETE"
        else:
            attachment_status = "PARTIAL"

        # Update the claim status
        update_query = """
            UPDATE CLAIM_STATUS 
            SET ATTACHMENT_STATUS = :status,
                DOWNLOADED_FILES_COUNT = :success_count,
                LAST_MODIFIED_DATE = CURRENT_TIMESTAMP
            WHERE CLAIM_ID = :claim_id
        """

        cursor.execute(
            update_query,
            {
                "status": attachment_status,
                "success_count": success_count,
                "claim_id": claim_id,
            },
        )
        connection.commit()

        logger.info(
            f"✅ Updated attachment status for CLAIM_ID {claim_id}: {attachment_status} ({success_count}/{total_files})"
        )

    except Exception as error:
        logger.error(
            f"❌ Error updating attachment status for CLAIM_ID {claim_id}: {error}"
        )
        if connection:
            connection.rollback()
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()


def log_download_status(
    file_id,
    claim_id,
    claim_no,
    remote_name,
    local_path,
    status,
    error_msg=None,
):
    """
    Inserts or updates a record in the BGATE tracking table.
    Now includes claim last modified date and updates claim attachment status.
    """
    sql_merge = """
        MERGE INTO PDF_DOWNLOAD_DMS_CLAIMS dest
        USING (
            SELECT
                :file_id AS FILE_ID,
                :claim_id AS CLAIM_ID,
                :claim_no AS CLAIM_NO,
                :remote_name AS REMOTE_FILE_NAME,
                :local_path AS LOCAL_FILE_PATH,
                :status AS STATUS,
                :error_msg AS ERROR_MESSAGE,
                :claim_last_modified AS CLAIM_LAST_MODIFIED,
                CURRENT_TIMESTAMP AS DOWNLOAD_TIMESTAMP
            FROM DUAL
        ) src ON (dest.FILE_ID = src.FILE_ID)
        WHEN MATCHED THEN
            UPDATE SET
                dest.STATUS = src.STATUS,
                dest.DOWNLOAD_TIMESTAMP = src.DOWNLOAD_TIMESTAMP,
                dest.ERROR_MESSAGE = src.ERROR_MESSAGE,
                dest.CLAIM_LAST_MODIFIED = src.CLAIM_LAST_MODIFIED,
                dest.IS_LATEST_VERSION = 'Y',
                dest.LOCAL_FILE_PATH = CASE 
                    WHEN src.STATUS = 'SUCCESS' THEN src.LOCAL_FILE_PATH 
                    ELSE dest.LOCAL_FILE_PATH 
                END
        WHEN NOT MATCHED THEN
            INSERT (
                FILE_ID, CLAIM_ID, CLAIM_NO, REMOTE_FILE_NAME, 
                LOCAL_FILE_PATH, STATUS, ERROR_MESSAGE, DOWNLOAD_TIMESTAMP,
                CLAIM_LAST_MODIFIED, IS_LATEST_VERSION
            )
            VALUES (
                src.FILE_ID, src.CLAIM_ID, src.CLAIM_NO, src.REMOTE_FILE_NAME, 
                src.LOCAL_FILE_PATH, src.STATUS, src.ERROR_MESSAGE, src.DOWNLOAD_TIMESTAMP,
                src.CLAIM_LAST_MODIFIED, 'Y'
            )
    """

    connection = None
    cursor = None
    try:
        # Connect to BGATE database for writing
        connection = get_bgate_db_connection()
        cursor = connection.cursor()

        # Get the claim's last modified date from DMS
        claim_last_modified = get_claim_last_modified_date(claim_id)

        # Truncate error message if too long
        truncated_error = None
        if error_msg:
            truncated_error = (
                str(error_msg)[:2000] if len(str(error_msg)) > 2000 else str(error_msg)
            )

        cursor.execute(
            sql_merge,
            {
                "file_id": file_id,
                "claim_id": claim_id,
                "claim_no": claim_no,
                "remote_name": remote_name,
                "local_path": local_path if local_path != "N/A" else None,
                "status": status,
                "error_msg": truncated_error,
                "claim_last_modified": claim_last_modified,
            },
        )

        connection.commit()
        logger.info(f"✅ Logged download status for FILE_ID {file_id}: {status}")

        # Update the claim's attachment status after logging the file
        update_attachment_status(claim_id)

    except oracledb.Error as error:
        logger.error(
            f"❌ Critical Error: Could not log download status for FILE_ID {file_id}. Reason: {error}"
        )
        try:
            if connection:
                connection.rollback()
        except:
            pass
        # Re-raise to let caller handle
        raise
    except Exception as error:
        logger.error(
            f"❌ Unexpected error logging download status for FILE_ID {file_id}: {error}"
        )
        try:
            if connection:
                connection.rollback()
        except:
            pass
        raise
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()


def get_claim_last_modified_date(claim_id):
    """Get the last modified date for a claim from CLAIM_STATUS table"""
    connection = None
    cursor = None

    try:
        connection = get_bgate_db_connection()
        cursor = connection.cursor()

        query = """
            SELECT LAST_DMS_UPDATE_DATE 
            FROM CLAIM_STATUS 
            WHERE CLAIM_ID = :claim_id
        """

        cursor.execute(query, {"claim_id": claim_id})
        result = cursor.fetchone()

        if result:
            return result[0]
        else:
            logger.warning(f"No claim status found for CLAIM_ID {claim_id}")
            return None

    except Exception as error:
        logger.error(
            f"Error getting claim last modified date for CLAIM_ID {claim_id}: {error}"
        )
        return None
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()


def get_claims_ready_for_processing():
    """
    Get claims that have ATTACHMENT_STATUS='COMPLETE' and AUDIT_STATUS='PENDING' or NULL.
    These are claims ready for PDF processing.
    """
    connection = None

    try:
        connection = get_bgate_db_connection()

        query = """
            SELECT 
                CLAIM_ID,
                CLAIM_NO,
                VIN,
                DEALER_CODE,
                DEALER_NAME,
                GROSS_CREDIT,
                LABOUR_AMOUNT_DMS,
                PART_AMOUNT_DMS,
                TOTAL_FILES_COUNT,
                DOWNLOADED_FILES_COUNT
            FROM CLAIM_STATUS
            WHERE ATTACHMENT_STATUS = 'COMPLETE'
            AND (AUDIT_STATUS IS NULL OR AUDIT_STATUS = 'PENDING')
            ORDER BY LAST_DMS_UPDATE_DATE ASC
        """

        df = pd.read_sql(query, connection)

        logger.info(f"Found {len(df)} claims ready for processing")
        return df

    except Exception as error:
        logger.error(f"❌ Error getting claims ready for processing: {error}")
        return pd.DataFrame()
    finally:
        if connection:
            connection.close()


def get_claim_pdf_files(claim_id):
    """
    Get all successfully downloaded PDF files for a specific claim.
    Returns list of file paths.
    """
    connection = None

    try:
        connection = get_bgate_db_connection()

        query = """
            SELECT LOCAL_FILE_PATH
            FROM PDF_DOWNLOAD_DMS_CLAIMS
            WHERE CLAIM_ID = :claim_id
            AND STATUS = 'SUCCESS'
            AND IS_LATEST_VERSION = 'Y'
            AND LOCAL_FILE_PATH IS NOT NULL
            ORDER BY DOWNLOAD_TIMESTAMP
        """

        df = pd.read_sql(query, connection, params={"claim_id": claim_id})

        file_paths = df["LOCAL_FILE_PATH"].tolist()
        logger.debug(f"Found {len(file_paths)} PDF files for CLAIM_ID {claim_id}")

        return file_paths

    except Exception as error:
        logger.error(f"❌ Error getting PDF files for CLAIM_ID {claim_id}: {error}")
        return []
    finally:
        if connection:
            connection.close()


def update_audit_status(claim_id, audit_status):
    """
    Update the audit status for a claim.

    Args:
        claim_id: The claim ID
        audit_status: 'PENDING', 'COMPLETE', or 'REJECTED'
    """
    connection = None
    cursor = None

    try:
        connection = get_bgate_db_connection()
        cursor = connection.cursor()

        update_query = """
            UPDATE CLAIM_STATUS 
            SET AUDIT_STATUS = :audit_status,
                LAST_MODIFIED_DATE = CURRENT_TIMESTAMP
            WHERE CLAIM_ID = :claim_id
        """

        cursor.execute(
            update_query, {"audit_status": audit_status, "claim_id": claim_id}
        )
        connection.commit()

        logger.info(f"✅ Updated audit status for CLAIM_ID {claim_id}: {audit_status}")

    except Exception as error:
        logger.error(f"❌ Error updating audit status for CLAIM_ID {claim_id}: {error}")
        if connection:
            connection.rollback()
        raise
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()


def get_claims_ready_for_audit():
    """
    Get claims that have been processed but need audit matching.
    These are claims with AUDIT_STATUS='PENDING'.
    """
    connection = None

    try:
        connection = get_bgate_db_connection()

        query = """
            SELECT 
                CLAIM_ID,
                CLAIM_NO,
                VIN,
                DEALER_CODE,
                DEALER_NAME,
                GROSS_CREDIT,
                LABOUR_AMOUNT_DMS,
                PART_AMOUNT_DMS,
                TOTAL_FILES_COUNT,
                DOWNLOADED_FILES_COUNT
            FROM CLAIM_STATUS
            WHERE AUDIT_STATUS = 'PENDING'
            ORDER BY LAST_DMS_UPDATE_DATE ASC
        """

        df = pd.read_sql(query, connection)

        logger.info(f"Found {len(df)} claims ready for audit")
        return df

    except Exception as error:
        logger.error(f"❌ Error getting claims ready for audit: {error}")
        return pd.DataFrame()
    finally:
        if connection:
            connection.close()


def get_download_statistics():
    """Get download statistics for monitoring from BGATE database"""
    connection = None
    cursor = None
    try:
        connection = get_bgate_db_connection()
        cursor = connection.cursor()
        query = """
            SELECT 
                STATUS,
                COUNT(*) AS COUNT,
                ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS PERCENTAGE
            FROM PDF_DOWNLOAD_DMS_CLAIMS 
            WHERE IS_LATEST_VERSION = 'Y'
            GROUP BY STATUS
            ORDER BY COUNT DESC
        """

        cursor.execute(query)
        results = cursor.fetchall()

        if not results:
            return pd.DataFrame()

        # Convert results to DataFrame
        columns = ["STATUS", "COUNT", "PERCENTAGE"]
        df = pd.DataFrame(results, columns=columns)  # type: ignore
        return df

    except Exception as error:
        logger.error(f"Error getting download statistics: {error}")
        return pd.DataFrame()
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()


def get_recent_downloads(days=1):
    """Get recent downloads for monitoring from BGATE database"""
    connection = None
    cursor = None
    try:
        connection = get_bgate_db_connection()
        cursor = connection.cursor()
        query = """
            SELECT 
                CLAIM_NO,
                FILE_ID,
                REMOTE_FILE_NAME,
                STATUS,
                DOWNLOAD_TIMESTAMP,
                ERROR_MESSAGE,
                IS_LATEST_VERSION
            FROM PDF_DOWNLOAD_DMS_CLAIMS 
            WHERE DOWNLOAD_TIMESTAMP >= SYSDATE - :days
            ORDER BY DOWNLOAD_TIMESTAMP DESC
        """

        cursor.execute(query, {"days": days})
        results = cursor.fetchall()

        if not results:
            return pd.DataFrame()

        # Convert results to DataFrame
        columns = [
            "CLAIM_NO",
            "FILE_ID",
            "REMOTE_FILE_NAME",
            "STATUS",
            "DOWNLOAD_TIMESTAMP",
            "ERROR_MESSAGE",
            "IS_LATEST_VERSION",
        ]
        df = pd.DataFrame(results, columns=columns)  # type: ignore
        return df

    except Exception as error:
        logger.error(f"Error getting recent downloads: {error}")
        return pd.DataFrame()
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()


def cleanup_old_failed_records(days=30):
    """Clean up old failed records to prevent table bloat in BGATE database"""
    connection = None
    cursor = None
    try:
        connection = get_bgate_db_connection()
        query = """
            DELETE FROM PDF_DOWNLOAD_DMS_CLAIMS 
            WHERE STATUS = 'FAILED' 
            AND IS_LATEST_VERSION = 'N'
            AND DOWNLOAD_TIMESTAMP < SYSDATE - :days
        """

        cursor = connection.cursor()
        cursor.execute(query, {"days": days})
        deleted_count = cursor.rowcount
        connection.commit()

        if deleted_count > 0:
            logger.info(f"Cleaned up {deleted_count} old failed records")

        return deleted_count

    except Exception as error:
        logger.error(f"Error during cleanup: {error}")
        return 0
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()


def get_claim_statistics():
    """Get claim-level statistics for monitoring"""
    connection = None

    try:
        connection = get_bgate_db_connection()

        query = """
            SELECT 
                ATTACHMENT_STATUS,
                AUDIT_STATUS,
                COUNT(*) AS COUNT
            FROM CLAIM_STATUS
            GROUP BY ATTACHMENT_STATUS, AUDIT_STATUS
            ORDER BY ATTACHMENT_STATUS, AUDIT_STATUS
        """

        df = pd.read_sql(query, connection)

        logger.info("📊 Claim Status Statistics:")
        for _, row in df.iterrows():
            logger.info(
                f"   {row['ATTACHMENT_STATUS']} / {row['AUDIT_STATUS']}: {row['COUNT']} claims"
            )

        return df

    except Exception as error:
        logger.error(f"Error getting claim statistics: {error}")
        return pd.DataFrame()
    finally:
        if connection:
            connection.close()
