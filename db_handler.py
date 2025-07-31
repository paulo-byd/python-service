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


def get_dms_db_connection():
    """Establishes and returns a connection to the DMS database (for reading) using Oracle DB THICK mode."""
    db_config = get_current_config()["dms_db"]
    try:
        connection = oracledb.connect(
            user=db_config["user"],
            password=db_config["password"],
            dsn=db_config["dsn"],
            mode=oracledb.DEFAULT_AUTH,
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
            user=db_config["user"],
            password=db_config["password"],
            dsn=db_config["dsn"],
            mode=oracledb.DEFAULT_AUTH,
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


def upsert_claim_status(claim_data):
    """
    Update or insert claim status in CLAIM_STATUS table.

    Args:
        claim_data (dict): Dictionary containing claim information with keys:
            CLAIM_ID, CLAIM_NO, VIN, DEALER_CODE, DEALER_NAME, REPORT_DATE,
            GROSS_CREDIT, LABOUR_AMOUNT_DMS, PART_AMOUNT_DMS, LAST_DMS_UPDATE_DATE, AUDITING_DATE
    """
    connection = None
    cursor = None

    try:
        connection = get_bgate_db_connection()
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

        cursor.execute(merge_query, claim_data)
        connection.commit()

        logger.info(f"✅ Upserted claim status for CLAIM_ID {claim_data['claim_id']}")

    except Exception as error:
        logger.error(
            f"❌ Error upserting claim status for CLAIM_ID {claim_data.get('claim_id', 'unknown')}: {error}"
        )
        if connection:
            connection.rollback()
        raise
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()


def get_claims_needing_download():
    """
    Get claims that need files downloaded by comparing DMS UPDATE_DATE with stored LAST_DMS_UPDATE_DATE.
    Returns DataFrame with claims that are new or have been updated in DMS.
    """
    dms_connection = None
    bgate_connection = None

    try:
        # Get connections
        dms_connection = get_dms_db_connection()
        bgate_connection = get_bgate_db_connection()

        # Get region and status IDs
        region_id = get_region_id(dms_connection)
        status_id = get_status_code_id(dms_connection)

        if not region_id or not status_id:
            logger.error("Failed to get required region_id or status_id")
            return pd.DataFrame()

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

        if dms_df.empty:
            logger.info("✅ No claims found in DMS database")
            return pd.DataFrame()

        logger.info(f"Found {len(dms_df)} claims in DMS database")

        # Get existing claim statuses from BGATE
        bgate_query = """
            SELECT 
                CLAIM_ID,
                LAST_DMS_UPDATE_DATE,
                ATTACHMENT_STATUS
            FROM CLAIM_STATUS
        """

        bgate_df = pd.read_sql(bgate_query, bgate_connection)

        # Merge to find claims needing updates
        if not bgate_df.empty:
            merged_df = dms_df.merge(bgate_df, on="CLAIM_ID", how="left")

            # Claims need download if:
            # 1. New claims (not in CLAIM_STATUS)
            # 2. DMS UPDATE_DATE > stored LAST_DMS_UPDATE_DATE
            # 3. Claims with ATTACHMENT_STATUS != 'COMPLETE'

            needs_download = merged_df[
                (merged_df["LAST_DMS_UPDATE_DATE"].isna())  # New claims
                | (
                    merged_df["UPDATE_DATE"] > merged_df["LAST_DMS_UPDATE_DATE"]
                )  # Updated claims
                | (merged_df["ATTACHMENT_STATUS"] != "COMPLETE")  # Incomplete downloads
            ]
        else:
            # No existing claims, all are new
            needs_download = dms_df

        logger.info(f"✅ Found {len(needs_download)} claims needing download")

        # Upsert claim status for all claims (update metadata)
        for _, row in dms_df.iterrows():
            claim_data = {
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
            upsert_claim_status(claim_data)

        return needs_download

    except Exception as error:
        logger.error(f"❌ Error getting claims needing download: {error}")
        return pd.DataFrame()
    finally:
        if dms_connection:
            dms_connection.close()
        if bgate_connection:
            bgate_connection.close()


def get_new_files_to_download():
    """
    Gets PDF files for claims that need downloading.
    """
    dms_connection = None
    bgate_connection = None

    try:
        # First, get claims that need downloading
        claims_needing_download = get_claims_needing_download()

        if claims_needing_download.empty:
            logger.info("No claims need file downloads")
            return pd.DataFrame()

        claim_ids = claims_needing_download["CLAIM_ID"].tolist()
        logger.info(f"Getting files for {len(claim_ids)} claims")

        # Get connections
        dms_connection = get_dms_db_connection()
        bgate_connection = get_bgate_db_connection()

        # Get region and status IDs
        region_id = get_region_id(dms_connection)
        status_id = get_status_code_id(dms_connection)

        if not region_id or not status_id:
            logger.error("Failed to get required region_id or status_id")
            return pd.DataFrame()

        # For each claim that needs downloading, mark old files as obsolete
        for claim_id in claim_ids:
            mark_old_files_obsolete(claim_id)

        # Get PDF files for claims needing download, in batches
        all_files = []
        batch_size = 999  # Oracle IN clause limit

        for i in range(0, len(claim_ids), batch_size):
            batch_claim_ids = claim_ids[i : i + batch_size]
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

            cursor = dms_connection.cursor()
            cursor.execute(files_query, params)
            batch_results = cursor.fetchall()
            cursor.close()

            all_files.extend(batch_results)

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

        # Update total file counts for each claim
        file_counts = (
            files_df.groupby("CLAIM_ID").size().reset_index(name="total_files")
        )
        for _, row in file_counts.iterrows():
            update_claim_file_count(row["CLAIM_ID"], row["total_files"])

        logger.info(
            f"✅ Found {len(files_df)} PDF files to download for {len(claim_ids)} claims"
        )

        return files_df

    except Exception as error:
        logger.error(f"❌ Error getting new files to download: {error}")
        return pd.DataFrame()
    finally:
        if dms_connection:
            dms_connection.close()
        if bgate_connection:
            bgate_connection.close()


def mark_old_files_obsolete(claim_id):
    """
    Mark existing PDF files for a claim as obsolete (IS_LATEST_VERSION = 'N')
    when the claim has been updated in DMS.
    """
    connection = None
    cursor = None

    try:
        connection = get_bgate_db_connection()
        cursor = connection.cursor()

        update_query = """
            UPDATE PDF_DOWNLOAD_DMS_CLAIMS 
            SET IS_LATEST_VERSION = 'N',
                LAST_MODIFIED_DATE = CURRENT_TIMESTAMP
            WHERE CLAIM_ID = :claim_id 
            AND IS_LATEST_VERSION = 'Y'
        """

        cursor.execute(update_query, {"claim_id": claim_id})
        updated_count = cursor.rowcount
        connection.commit()

        if updated_count > 0:
            logger.info(
                f"✅ Marked {updated_count} files as obsolete for CLAIM_ID {claim_id}"
            )

    except Exception as error:
        logger.error(
            f"❌ Error marking old files obsolete for CLAIM_ID {claim_id}: {error}"
        )
        if connection:
            connection.rollback()
        raise
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()


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
