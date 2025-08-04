#!/usr/bin/env python3
"""
PDF Download Service Monitoring Script
Provides health checks, statistics, and maintenance for the PDF download service
Updated to work with the new database structure including CLAIM_STATUS table
"""

import sys
import argparse
import logging
from datetime import datetime, timedelta
import db_handler

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def check_service_health(connection):
    """Check the overall health of the PDF download service"""
    print("🔍 PDF Download Service Health Check")
    print("=" * 50)

    try:
        # Get basic download statistics
        stats_query = """
            SELECT 
                STATUS,
                COUNT(*) as COUNT
            FROM PDF_DOWNLOAD_DMS_CLAIMS 
            WHERE IS_LATEST_VERSION = 'Y'
            GROUP BY STATUS
        """
        cursor = connection.cursor()
        cursor.execute(stats_query)
        stats = cursor.fetchall()
        cursor.close()

        total_files = sum([row[1] for row in stats])
        print(f"📊 Total files tracked (latest version): {total_files}")

        for status, count in stats:
            percentage = (count / total_files * 100) if total_files > 0 else 0
            print(f"   {status}: {count} ({percentage:.1f}%)")

        # Check recent activity (last 24 hours)
        recent_query = """
            SELECT COUNT(*) 
            FROM PDF_DOWNLOAD_DMS_CLAIMS 
            WHERE DOWNLOAD_TIMESTAMP >= SYSDATE - 1
            AND IS_LATEST_VERSION = 'Y'
        """
        cursor = connection.cursor()
        cursor.execute(recent_query)
        recent_count = cursor.fetchone()[0]
        cursor.close()

        print(f"📈 Downloads in last 24 hours: {recent_count}")

        # Check for failed downloads that need attention
        failed_query = """
            SELECT COUNT(*) 
            FROM PDF_DOWNLOAD_DMS_CLAIMS 
            WHERE STATUS = 'FAILED' 
            AND IS_LATEST_VERSION = 'Y'
        """
        cursor = connection.cursor()
        cursor.execute(failed_query)
        failed_count = cursor.fetchone()[0]
        cursor.close()

        if failed_count > 0:
            print(f"⚠️  Failed downloads needing attention: {failed_count}")
        else:
            print("✅ No failed downloads")

        # Check claim-level statistics
        claim_stats_query = """
            SELECT 
                ATTACHMENT_STATUS,
                COUNT(*) as COUNT
            FROM CLAIM_STATUS
            GROUP BY ATTACHMENT_STATUS
        """
        cursor = connection.cursor()
        cursor.execute(claim_stats_query)
        claim_stats = cursor.fetchall()
        cursor.close()

        print(f"\n📋 Claim Status Overview:")
        for status, count in claim_stats:
            print(f"   {status}: {count} claims")

        # Check storage usage
        storage_query = """
            SELECT 
                ROUND(SUM(FILE_SIZE_BYTES) / 1024 / 1024 / 1024, 2) as SIZE_GB,
                COUNT(*) as FILE_COUNT
            FROM PDF_DOWNLOAD_DMS_CLAIMS 
            WHERE STATUS = 'SUCCESS' 
            AND IS_LATEST_VERSION = 'Y'
            AND FILE_SIZE_BYTES IS NOT NULL
        """
        cursor = connection.cursor()
        cursor.execute(storage_query)
        storage_result = cursor.fetchone()
        cursor.close()

        if storage_result[0]:
            print(
                f"💾 Storage used: {storage_result[0]} GB ({storage_result[1]} files)"
            )

        # Check for claims with incomplete downloads
        incomplete_claims_query = """
            SELECT COUNT(*)
            FROM CLAIM_STATUS
            WHERE ATTACHMENT_STATUS IN ('PENDING', 'PARTIAL')
        """
        cursor = connection.cursor()
        cursor.execute(incomplete_claims_query)
        incomplete_count = cursor.fetchone()[0]
        cursor.close()

        if incomplete_count > 0:
            print(f"⚠️  Claims with incomplete downloads: {incomplete_count}")
        else:
            print("✅ All tracked claims have complete downloads")

    except Exception as e:
        print(f"❌ Health check failed: {e}")
        return False

    print("✅ Health check completed")
    return True


def show_recent_activity(connection, hours=24):
    """Show recent download activity"""
    print(f"\n📋 Recent Activity (Last {hours} hours)")
    print("=" * 50)

    try:
        query = """
            SELECT 
                pdf.CLAIM_NO,
                pdf.FILE_ID,
                pdf.REMOTE_FILE_NAME,
                pdf.STATUS,
                TO_CHAR(pdf.DOWNLOAD_TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS') as DOWNLOAD_TIME,
                CASE 
                    WHEN pdf.ERROR_MESSAGE IS NOT NULL THEN 
                        SUBSTR(pdf.ERROR_MESSAGE, 1, 50) || CASE WHEN LENGTH(pdf.ERROR_MESSAGE) > 50 THEN '...' ELSE '' END
                    ELSE NULL 
                END as ERROR_SUMMARY,
                cs.ATTACHMENT_STATUS
            FROM PDF_DOWNLOAD_DMS_CLAIMS pdf
            LEFT JOIN CLAIM_STATUS cs ON pdf.CLAIM_ID = cs.CLAIM_ID
            WHERE pdf.DOWNLOAD_TIMESTAMP >= SYSDATE - :hours/24
            AND pdf.IS_LATEST_VERSION = 'Y'
            ORDER BY pdf.DOWNLOAD_TIMESTAMP DESC
            FETCH FIRST 20 ROWS ONLY
        """

        cursor = connection.cursor()
        cursor.execute(query, {"hours": hours})
        results = cursor.fetchall()
        cursor.close()

        if not results:
            print("No recent activity found")
            return

        print(
            f"{'Claim':<12} {'File ID':<10} {'Status':<8} {'Download Time':<20} {'Claim Status':<12} {'Error':<30}"
        )
        print("-" * 100)

        for row in results:
            claim_no = row[0] or "N/A"
            file_id = row[1][:8] + "..." if len(row[1]) > 8 else row[1]
            status = row[2]
            download_time = row[3]
            error = row[4] or ""
            claim_status = row[5] or "N/A"

            print(
                f"{claim_no:<12} {file_id:<10} {status:<8} {download_time:<20} {claim_status:<12} {error:<30}"
            )

    except Exception as e:
        print(f"❌ Failed to get recent activity: {e}")


def show_failed_downloads(connection):
    """Show failed downloads that need attention"""
    print("\n❌ Failed Downloads Needing Attention")
    print("=" * 50)

    try:
        query = """
            SELECT 
                pdf.CLAIM_NO,
                pdf.FILE_ID,
                TO_CHAR(pdf.DOWNLOAD_TIMESTAMP, 'YYYY-MM-DD HH24:MI') as DOWNLOAD_TIME,
                SUBSTR(pdf.ERROR_MESSAGE, 1, 80) as ERROR_SUMMARY,
                cs.ATTACHMENT_STATUS,
                cs.TOTAL_FILES_COUNT,
                cs.DOWNLOADED_FILES_COUNT
            FROM PDF_DOWNLOAD_DMS_CLAIMS pdf
            LEFT JOIN CLAIM_STATUS cs ON pdf.CLAIM_ID = cs.CLAIM_ID
            WHERE pdf.STATUS = 'FAILED' 
            AND pdf.IS_LATEST_VERSION = 'Y'
            ORDER BY pdf.DOWNLOAD_TIMESTAMP DESC
            FETCH FIRST 20 ROWS ONLY
        """

        cursor = connection.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        cursor.close()

        if not results:
            print("✅ No failed downloads found")
            return

        print(
            f"{'Claim':<12} {'File ID':<12} {'Download Time':<16} {'Files':<8} {'Error':<40}"
        )
        print("-" * 100)

        for row in results:
            claim_no = row[0] or "N/A"
            file_id = row[1][:10] + ".." if len(row[1]) > 10 else row[1]
            download_time = row[2]
            error = row[3] or "No error message"
            attachment_status = row[4] or "N/A"
            total_files = row[5] or 0
            downloaded_files = row[6] or 0

            files_info = f"{downloaded_files}/{total_files}"

            print(
                f"{claim_no:<12} {file_id:<12} {download_time:<16} {files_info:<8} {error:<40}"
            )

    except Exception as e:
        print(f"❌ Failed to get failed downloads: {e}")


def show_claim_summary(connection):
    """Show summary of claims by status"""
    print("\n📊 Claims Summary by Status")
    print("=" * 50)

    try:
        query = """
            SELECT 
                ATTACHMENT_STATUS,
                AUDIT_STATUS,
                COUNT(*) as CLAIM_COUNT,
                AVG(TOTAL_FILES_COUNT) as AVG_FILES,
                AVG(DOWNLOADED_FILES_COUNT) as AVG_DOWNLOADED,
                SUM(TOTAL_FILES_COUNT) as TOTAL_FILES,
                SUM(DOWNLOADED_FILES_COUNT) as TOTAL_DOWNLOADED
            FROM CLAIM_STATUS
            GROUP BY ATTACHMENT_STATUS, AUDIT_STATUS
            ORDER BY ATTACHMENT_STATUS, AUDIT_STATUS
        """

        cursor = connection.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        cursor.close()

        if not results:
            print("No claims found")
            return

        print(
            f"{'Attachment':<12} {'Audit':<10} {'Claims':<8} {'Avg Files':<10} {'Total Files':<12} {'Downloaded':<12}"
        )
        print("-" * 80)

        for row in results:
            attachment_status = row[0] or "NULL"
            audit_status = row[1] or "NULL"
            claim_count = row[2]
            avg_files = f"{row[3]:.1f}" if row[3] else "0.0"
            avg_downloaded = f"{row[4]:.1f}" if row[4] else "0.0"
            total_files = row[5] or 0
            total_downloaded = row[6] or 0

            print(
                f"{attachment_status:<12} {audit_status:<10} {claim_count:<8} {avg_files:<10} {total_files:<12} {total_downloaded:<12}"
            )

    except Exception as e:
        print(f"❌ Failed to get claim summary: {e}")


def show_problematic_claims(connection):
    """Show claims that might need attention"""
    print("\n⚠️  Problematic Claims Needing Attention")
    print("=" * 50)

    try:
        query = """
            SELECT 
                cs.CLAIM_ID,
                cs.CLAIM_NO,
                cs.ATTACHMENT_STATUS,
                cs.TOTAL_FILES_COUNT,
                cs.DOWNLOADED_FILES_COUNT,
                (cs.TOTAL_FILES_COUNT - cs.DOWNLOADED_FILES_COUNT) as MISSING_FILES,
                TO_CHAR(cs.LAST_DMS_UPDATE_DATE, 'YYYY-MM-DD') as LAST_UPDATE
            FROM CLAIM_STATUS cs
            WHERE cs.TOTAL_FILES_COUNT > cs.DOWNLOADED_FILES_COUNT
            OR cs.ATTACHMENT_STATUS = 'PARTIAL'
            ORDER BY (cs.TOTAL_FILES_COUNT - cs.DOWNLOADED_FILES_COUNT) DESC
            FETCH FIRST 15 ROWS ONLY
        """

        cursor = connection.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        cursor.close()

        if not results:
            print("✅ No problematic claims found")
            return

        print(
            f"{'Claim ID':<12} {'Claim No':<15} {'Status':<10} {'Missing':<8} {'Last Update':<12}"
        )
        print("-" * 70)

        for row in results:
            claim_id = row[0]
            claim_no = row[1] or "N/A"
            attachment_status = row[2]
            total_files = row[3] or 0
            downloaded_files = row[4] or 0
            missing_files = row[5] or 0
            last_update = row[6] or "N/A"

            print(
                f"{claim_id:<12} {claim_no:<15} {attachment_status:<10} {missing_files:<8} {last_update:<12}"
            )

    except Exception as e:
        print(f"❌ Failed to get problematic claims: {e}")


def cleanup_old_records(connection, days=30):
    """Clean up old failed records"""
    print(f"\n🧹 Cleaning up failed records older than {days} days")
    print("=" * 50)

    try:
        # First, show what will be deleted
        count_query = """
            SELECT COUNT(*) 
            FROM PDF_DOWNLOAD_DMS_CLAIMS 
            WHERE STATUS = 'FAILED' 
            AND IS_LATEST_VERSION = 'N'
            AND DOWNLOAD_TIMESTAMP < SYSDATE - :days
        """

        cursor = connection.cursor()
        cursor.execute(count_query, {"days": days})
        count_to_delete = cursor.fetchone()[0]
        cursor.close()

        if count_to_delete == 0:
            print("✅ No old failed records to clean up")
            return

        print(
            f"Found {count_to_delete} old failed records to delete (non-latest versions only)"
        )

        # Ask for confirmation
        response = input("Do you want to proceed with deletion? (y/N): ")
        if response.lower() != "y":
            print("❌ Cleanup cancelled")
            return

        # Perform the deletion
        delete_query = """
            DELETE FROM PDF_DOWNLOAD_DMS_CLAIMS 
            WHERE STATUS = 'FAILED' 
            AND IS_LATEST_VERSION = 'N'
            AND DOWNLOAD_TIMESTAMP < SYSDATE - :days
        """

        cursor = connection.cursor()
        cursor.execute(delete_query, {"days": days})
        deleted_count = cursor.rowcount
        connection.commit()
        cursor.close()

        print(f"✅ Deleted {deleted_count} old failed records")

    except Exception as e:
        print(f"❌ Cleanup failed: {e}")
        connection.rollback()


def reset_failed_for_retry(connection, file_ids=None):
    """Reset failed downloads to allow retry"""
    print("\n🔄 Resetting Failed Downloads for Retry")
    print("=" * 50)

    try:
        if file_ids:
            # Reset specific file IDs
            placeholders = ",".join([":id" + str(i) for i in range(len(file_ids))])
            query = f"""
                UPDATE PDF_DOWNLOAD_DMS_CLAIMS 
                SET STATUS = 'PENDING',
                    ERROR_MESSAGE = NULL,
                    LAST_MODIFIED_DATE = CURRENT_TIMESTAMP
                WHERE FILE_ID IN ({placeholders})
                AND STATUS = 'FAILED'
                AND IS_LATEST_VERSION = 'Y'
            """

            params = {f"id{i}": file_id for i, file_id in enumerate(file_ids)}

        else:
            # Reset all failed downloads
            query = """
                UPDATE PDF_DOWNLOAD_DMS_CLAIMS 
                SET STATUS = 'PENDING',
                    ERROR_MESSAGE = NULL,
                    LAST_MODIFIED_DATE = CURRENT_TIMESTAMP
                WHERE STATUS = 'FAILED' 
                AND IS_LATEST_VERSION = 'Y'
            """
            params = {}

        cursor = connection.cursor()
        cursor.execute(query, params)
        updated_count = cursor.rowcount
        connection.commit()
        cursor.close()

        print(f"✅ Reset {updated_count} failed downloads for retry")

        # Update claim attachment status for affected claims
        if updated_count > 0:
            print("🔄 Updating claim attachment statuses...")
            # This would need to call your db_handler function to recalculate claim statuses
            # For now, just print a reminder
            print("💡 Consider running the download process to pick up the reset files")

    except Exception as e:
        print(f"❌ Reset failed: {e}")
        connection.rollback()


def export_statistics(connection, output_file=None):
    """Export detailed statistics to a file"""
    if not output_file:
        output_file = (
            f"pdf_download_stats_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        )

    print(f"\n📤 Exporting statistics to {output_file}")
    print("=" * 50)

    try:
        with open(output_file, "w") as f:
            f.write(f"PDF Download Service Statistics Report\n")
            f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write("=" * 60 + "\n\n")

            # Overall download statistics
            stats_query = """
                SELECT 
                    STATUS,
                    COUNT(*) as COUNT,
                    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as PERCENTAGE,
                    MIN(DOWNLOAD_TIMESTAMP) as FIRST_DOWNLOAD,
                    MAX(DOWNLOAD_TIMESTAMP) as LAST_DOWNLOAD
                FROM PDF_DOWNLOAD_DMS_CLAIMS 
                WHERE IS_LATEST_VERSION = 'Y'
                GROUP BY STATUS
                ORDER BY COUNT DESC
            """

            cursor = connection.cursor()
            cursor.execute(stats_query)
            results = cursor.fetchall()
            cursor.close()

            f.write("Download Statistics (Latest Versions Only):\n")
            f.write("-" * 45 + "\n")
            for row in results:
                f.write(f"Status: {row[0]}\n")
                f.write(f"  Count: {row[1]} ({row[2]}%)\n")
                f.write(f"  Period: {row[3]} to {row[4]}\n\n")

            # Claim-level statistics
            claim_stats_query = """
                SELECT 
                    ATTACHMENT_STATUS,
                    AUDIT_STATUS,
                    COUNT(*) as COUNT,
                    AVG(TOTAL_FILES_COUNT) as AVG_FILES,
                    SUM(TOTAL_FILES_COUNT) as TOTAL_FILES,
                    SUM(DOWNLOADED_FILES_COUNT) as TOTAL_DOWNLOADED
                FROM CLAIM_STATUS
                GROUP BY ATTACHMENT_STATUS, AUDIT_STATUS
                ORDER BY ATTACHMENT_STATUS, AUDIT_STATUS
            """

            cursor = connection.cursor()
            cursor.execute(claim_stats_query)
            results = cursor.fetchall()
            cursor.close()

            f.write("\nClaim Status Statistics:\n")
            f.write("-" * 25 + "\n")
            for row in results:
                f.write(f"Attachment: {row[0]}, Audit: {row[1]}\n")
                f.write(f"  Claims: {row[2]}\n")
                f.write(f"  Avg Files per Claim: {row[3]:.1f}\n")
                f.write(f"  Total Files: {row[4]}, Downloaded: {row[5]}\n\n")

            # Daily breakdown for last 30 days
            daily_query = """
                SELECT 
                    TO_CHAR(DOWNLOAD_TIMESTAMP, 'YYYY-MM-DD') as DOWNLOAD_DATE,
                    STATUS,
                    COUNT(*) as COUNT
                FROM PDF_DOWNLOAD_DMS_CLAIMS 
                WHERE DOWNLOAD_TIMESTAMP >= SYSDATE - 30
                AND IS_LATEST_VERSION = 'Y'
                GROUP BY TO_CHAR(DOWNLOAD_TIMESTAMP, 'YYYY-MM-DD'), STATUS
                ORDER BY DOWNLOAD_DATE DESC, STATUS
            """

            cursor = connection.cursor()
            cursor.execute(daily_query)
            results = cursor.fetchall()
            cursor.close()

            f.write("\nDaily Breakdown (Last 30 Days):\n")
            f.write("-" * 35 + "\n")
            for row in results:
                f.write(f"{row[0]} - {row[1]}: {row[2]}\n")

        print(f"✅ Statistics exported to {output_file}")

    except Exception as e:
        print(f"❌ Export failed: {e}")


def main():
    parser = argparse.ArgumentParser(description="PDF Download Service Monitor")
    parser.add_argument("--health", action="store_true", help="Show service health")
    parser.add_argument(
        "--recent", type=int, default=24, help="Show recent activity (hours)"
    )
    parser.add_argument("--failed", action="store_true", help="Show failed downloads")
    parser.add_argument("--claims", action="store_true", help="Show claim summary")
    parser.add_argument(
        "--problems", action="store_true", help="Show problematic claims"
    )
    parser.add_argument("--cleanup", type=int, help="Cleanup old failed records (days)")
    parser.add_argument(
        "--reset-failed", action="store_true", help="Reset failed downloads for retry"
    )
    parser.add_argument("--export", type=str, help="Export statistics to file")
    parser.add_argument("--all", action="store_true", help="Show all information")
    parser.add_argument(
        "--env",
        type=str,
        choices=["local", "uat", "prod"],
        default="local",
        help="Environment mode",
    )

    args = parser.parse_args()

    # If no arguments provided, show help
    if len(sys.argv) == 1:
        parser.print_help()
        return

    # Set environment mode
    db_handler.set_environment_mode(args.env)

    # Connect to database
    try:
        connection = db_handler.get_bgate_db_connection()
        if not connection:
            print("❌ Failed to connect to database")
            return

        print(
            f"🚀 PDF Download Service Monitor - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        )
        print(f"Environment: {args.env}")

        if args.all or args.health:
            check_service_health(connection)

        if args.all or args.recent:
            show_recent_activity(connection, args.recent)

        if args.all or args.failed:
            show_failed_downloads(connection)

        if args.all or args.claims:
            show_claim_summary(connection)

        if args.all or args.problems:
            show_problematic_claims(connection)

        if args.cleanup:
            cleanup_old_records(connection, args.cleanup)

        if args.reset_failed:
            reset_failed_for_retry(connection)

        if args.export:
            export_statistics(connection, args.export)

    except Exception as e:
        logger.error(f"Monitor failed: {e}")
    finally:
        if "connection" in locals() and connection:
            connection.close()


if __name__ == "__main__":
    main()
