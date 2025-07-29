import oracledb
import pandas as pd
import yaml
import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


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


def get_db_connection():
    """Establishes and returns a database connection."""
    config = load_config()["database"]
    try:
        connection = oracledb.connect(
            user=config["user"], password=config["password"], dsn=config["dsn"]
        )
        logger.info("✅ Database connection established")
        return connection
    except oracledb.Error as error:
        logger.error(f"❌ Error connecting to Oracle Database: {error}")
        raise


def get_region_id(connection, region_name="巴西"):
    """Get Brazil region ID"""
    try:
        query = "SELECT REGION_ID FROM TM_REGION WHERE REGION_NAME = :region_name"
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
    """Get status code ID for 'Payment documents TO be audited'"""
    try:
        query = "SELECT CODE_ID FROM TC_CODE WHERE TYPE = :type_code"
        cursor = connection.cursor()
        cursor.execute(query, {"type_code": type_code})
        result = cursor.fetchone()
        cursor.close()

        if result:
            logger.info(f"Found status code ID {result[0]} for type {type_code}")
            return result[0]
        else:
            logger.error(f"Status code type {type_code} not found")
            return None
    except oracledb.Error as error:
        logger.error(f"Error getting status code ID: {error}")
        return None


def get_new_files_to_download(connection):
    """
    Queries the database to find PDF files that have not been successfully downloaded yet.
    Includes date range filtering as per original requirements.
    """
    config = load_config()["query_params"]

    # Get region and status IDs dynamically
    region_id = get_region_id(connection)
    status_id = get_status_code_id(connection)

    if not region_id or not status_id:
        logger.error("Failed to get required region_id or status_id")
        return pd.DataFrame()

    # Enhanced query with date range filtering as per original requirements
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
            DMS_DEALER_PROD.SEC_TT_AS_WR_APPLICATION_V claims
        JOIN
            DMS_DEALER_PROD.TC_FILE_UPLOAD_INFO files ON claims.CLAIM_ID = files.BILL_ID
        JOIN
            DMS_DEALER_PROD.TM_DEALER td ON claims.DEALER_ID = td.DEALER_ID
        WHERE
            td.COUNTRY_ID = :region_id
            AND claims.STATUS = :status_id
            AND files.FILE_TYPE_DETAIL = '.pdf'
            AND claims.REPORT_DATE BETWEEN TO_DATE('2020-07-23', 'YYYY-MM-DD') 
                                        AND TO_DATE('2025-07-23', 'YYYY-MM-DD')
            AND claims.UPDATE_DATE < TO_DATE('2025-07-23', 'YYYY-MM-DD')
            AND NOT EXISTS (
                SELECT 1
                FROM PDF_DOWNLOAD_DMS_CLAIMS tracked
                WHERE tracked.FILE_ID = files.FILE_ID 
                AND tracked.STATUS = 'SUCCESS'
            )
        ORDER BY claims.REPORT_DATE ASC, files.CREATE_DATE ASC
    """

    try:
        logger.info("🔎 Searching for new PDF files to download...")
        params = {
            "region_id": region_id,
            "status_id": status_id,
        }

        df = pd.read_sql(query, connection, params=params)
        logger.info(f"✅ Found {len(df)} new files to download")

        if len(df) > 0:
            # Log some statistics
            date_range = df["REPORT_DATE"].agg(["min", "max"])
            logger.info(f"Date range: {date_range['min']} to {date_range['max']}")

        return df

    except oracledb.Error as error:
        logger.error(f"❌ Error executing query to find new files: {error}")
        return pd.DataFrame()
    except Exception as error:
        logger.error(f"❌ Unexpected error in get_new_files_to_download: {error}")
        return pd.DataFrame()


def log_download_status(
    connection,
    file_id,
    claim_id,
    claim_no,
    remote_name,
    local_path,
    status,
    error_msg=None,
):
    """
    Inserts or updates a record in the tracking table.
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

    cursor = None
    try:
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
            connection.rollback()
        except:
            pass
        raise
    finally:
        if cursor:
            cursor.close()


def get_download_statistics(connection):
    """Get download statistics for monitoring"""
    try:
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


def get_recent_downloads(connection, days=1):
    """Get recent downloads for monitoring"""
    try:
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


def cleanup_old_failed_records(connection, days=30):
    """Clean up old failed records to prevent table bloat"""
    try:
        query = """
            DELETE FROM PDF_DOWNLOAD_DMS_CLAIMS 
            WHERE STATUS = 'FAILED' 
            AND DOWNLOAD_TIMESTAMP < SYSDATE - :days
        """

        cursor = connection.cursor()
        cursor.execute(query, {"days": days})
        deleted_count = cursor.rowcount
        connection.commit()
        cursor.close()

        if deleted_count > 0:
            logger.info(f"Cleaned up {deleted_count} old failed records")

        return deleted_count

    except Exception as error:
        logger.error(f"Error during cleanup: {error}")
        return 0
