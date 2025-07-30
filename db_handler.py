import oracledb
import pandas as pd
import yaml
import logging
from datetime import datetime, timedelta

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


def set_environment_mode(mode):
    """Set the environment mode for database connections"""
    global _ENVIRONMENT_MODE
    _ENVIRONMENT_MODE = mode


# Environment-specific database configurations
LOCAL_CONFIG = {
    "dms_db": {
        "user": "DMS_OEM_SL",
        "password": "-oVDmYP6-,=*",
        "dsn": "10.42.253.86:1027/dms11g",
    },
    "bgate_db": {
        "user": "C##SILVERTREE",
        "password": "test123",
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
        "dsn": "10.42.253.86:1092/dms19g_pdb1",
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


def get_dms_db_connection():
    """Establishes and returns a connection to the DMS database (for reading) using Oracle DB THICK mode."""
    db_config = get_current_config()["dms_db"]
    try:
        connection = oracledb.connect(
            user=db_config["user"], password=db_config["password"], dsn=db_config["dsn"], mode=oracledb.DEFAULT_AUTH
        )
        logger.info("DMS Database connection established (THICK mode)")
        return connection
    except oracledb.Error as error:
        logger.error(f"Error connecting to DMS Oracle Database (THICK mode): {error}")
        raise


def get_bgate_db_connection():
    """Establishes and returns a connection to the BGATE database (for writing)."""
    db_config = get_current_config()["bgate_db"]
    try:
        connection = oracledb.connect(
            user=db_config["user"], password=db_config["password"], dsn=db_config["dsn"], mode=oracledb.DEFAULT_AUTH
        )
        logger.info("BGATE Database connection established (THICK mode)")
        return connection
    except oracledb.Error as error:
        logger.error(f"Error connecting to BGATE Oracle Database: {error}")
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


def get_status_code_id(connection, type_code=5618):
    """Get status code ID for 'Payment documents TO be audited' from DMS database"""
    try:
        query = "SELECT CODE_ID FROM DMS_OEM_PROD.TC_CODE WHERE TYPE = :type_code"
        cursor = connection.cursor()
        cursor.execute(query, {"type_code": type_code})
        result = cursor.fetchone()
        cursor.close()

        if result:
            logger.info(
                f"Found status code ID {result[0]} for type {type_code} (expected: 56181016)"
            )
            return result[0]
        else:
            logger.error(f"Status code type {type_code} not found")
            return None
    except oracledb.Error as error:
        logger.error(f"Error getting status code ID: {error}")
        return None


def get_new_files_to_download():
    """
    Queries the DMS database to find PDF files that have not been successfully downloaded yet.
    Returns a DataFrame with file information.
    """
    dms_connection = None
    bgate_connection = None

    try:
        # Get connections to both databases
        dms_connection = get_dms_db_connection()
        bgate_connection = get_bgate_db_connection()

        # Get region and status IDs dynamically from DMS database
        region_id = get_region_id(dms_connection)
        status_id = get_status_code_id(dms_connection)

        if not region_id or not status_id:
            logger.error("Failed to get required region_id or status_id")
            return pd.DataFrame()

        # Enhanced query with date range filtering using DMS_OEM_PROD schema
        query = """
            SELECT
                claims.CLAIM_ID,
                claims.CLAIM_NO,
                claims.VIN,
                claims.GROSS_CREDIT,
                claims.REPORT_DATE,
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
                td.COUNTRY_ID = :region_id
                AND claims.STATUS = :status_id
                AND files.FILE_TYPE_DETAIL = '.pdf'
                AND claims.REPORT_DATE BETWEEN TO_DATE('2020-07-23', 'YYYY-MM-DD') 
                                            AND SYSDATE
                AND claims.UPDATE_DATE < SYSDATE
            ORDER BY claims.REPORT_DATE ASC, files.CREATE_DATE ASC
        """

        logger.info("🔎 Searching for new PDF files to download from DMS database...")
        params = {
            "region_id": region_id,
            "status_id": status_id,
        }

        # Execute query on DMS database
        df = pd.read_sql(query, dms_connection, params=params)

        if len(df) == 0:
            logger.info("✅ No files found in DMS database")
            return df

        # Now check against BGATE database to filter out already downloaded files
        file_ids = df["FILE_ID"].tolist()
        file_ids_str = ",".join([f"'{fid}'" for fid in file_ids])

        tracking_query = f"""
            SELECT FILE_ID 
            FROM PDF_DOWNLOAD_DMS_CLAIMS 
            WHERE FILE_ID IN ({file_ids_str}) 
            AND STATUS = 'SUCCESS'
        """

        # Execute tracking query on BGATE database
        downloaded_df = pd.read_sql(tracking_query, bgate_connection)
        downloaded_file_ids = (
            downloaded_df["FILE_ID"].tolist() if not downloaded_df.empty else []
        )

        # Filter out already successfully downloaded files
        df_filtered = df[~df["FILE_ID"].isin(downloaded_file_ids)]

        logger.info(
            f"✅ Found {len(df)} total files, {len(downloaded_file_ids)} already downloaded, {len(df_filtered)} new files to download"
        )

        if len(df_filtered) > 0:
            # Log some statistics
            date_range = df_filtered["REPORT_DATE"].agg(["min", "max"])
            logger.info(f"Date range: {date_range['min']} to {date_range['max']}")

        return df_filtered

    except oracledb.Error as error:
        logger.error(f"❌ Error executing query to find new files: {error}")
        return pd.DataFrame()
    except Exception as error:
        logger.error(f"❌ Unexpected error in get_new_files_to_download: {error}")
        return pd.DataFrame()
    finally:
        # Close both connections
        if dms_connection:
            dms_connection.close()
        if bgate_connection:
            bgate_connection.close()


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
    Enhanced with better error handling and logging.
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
                CURRENT_TIMESTAMP AS DOWNLOAD_TIMESTAMP
            FROM DUAL
        ) src ON (dest.FILE_ID = src.FILE_ID)
        WHEN MATCHED THEN
            UPDATE SET
                dest.STATUS = src.STATUS,
                dest.DOWNLOAD_TIMESTAMP = src.DOWNLOAD_TIMESTAMP,
                dest.ERROR_MESSAGE = src.ERROR_MESSAGE,
                dest.LOCAL_FILE_PATH = CASE 
                    WHEN src.STATUS = 'SUCCESS' THEN src.LOCAL_FILE_PATH 
                    ELSE dest.LOCAL_FILE_PATH 
                END
        WHEN NOT MATCHED THEN
            INSERT (
                FILE_ID, CLAIM_ID, CLAIM_NO, REMOTE_FILE_NAME, 
                LOCAL_FILE_PATH, STATUS, ERROR_MESSAGE, DOWNLOAD_TIMESTAMP
            )
            VALUES (
                src.FILE_ID, src.CLAIM_ID, src.CLAIM_NO, src.REMOTE_FILE_NAME, 
                src.LOCAL_FILE_PATH, src.STATUS, src.ERROR_MESSAGE, src.DOWNLOAD_TIMESTAMP
            )
    """

    connection = None
    cursor = None
    try:
        # Connect to BGATE database for writing
        connection = get_bgate_db_connection()
        cursor = connection.cursor()

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
            },
        )

        connection.commit()
        logger.info(f"✅ Logged download status for FILE_ID {file_id}: {status}")

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


def get_download_statistics():
    """Get download statistics for monitoring from BGATE database"""
    connection = None
    try:
        connection = get_bgate_db_connection()
        query = """
            SELECT 
                STATUS,
                COUNT(*) AS COUNT,
                ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS PERCENTAGE
            FROM PDF_DOWNLOAD_DMS_CLAIMS 
            GROUP BY STATUS
            ORDER BY COUNT DESC
        """

        df = pd.read_sql(query, connection)
        return df

    except Exception as error:
        logger.error(f"Error getting download statistics: {error}")
        return pd.DataFrame()
    finally:
        if connection:
            connection.close()


def get_recent_downloads(days=1):
    """Get recent downloads for monitoring from BGATE database"""
    connection = None
    try:
        connection = get_bgate_db_connection()
        query = """
            SELECT 
                CLAIM_NO,
                FILE_ID,
                REMOTE_FILE_NAME,
                STATUS,
                DOWNLOAD_TIMESTAMP,
                ERROR_MESSAGE
            FROM PDF_DOWNLOAD_DMS_CLAIMS 
            WHERE DOWNLOAD_TIMESTAMP >= SYSDATE - :days
            ORDER BY DOWNLOAD_TIMESTAMP DESC
        """

        df = pd.read_sql(query, connection, params={"days": days})
        return df

    except Exception as error:
        logger.error(f"Error getting recent downloads: {error}")
        return pd.DataFrame()
    finally:
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
