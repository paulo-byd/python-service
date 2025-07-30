#!/usr/bin/env python3
"""
PDF Download Service Monitoring Script
Provides health checks, statistics, and maintenance for the PDF download service
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
        # Get basic statistics
        stats_query = """
            SELECT 
                STATUS,
                COUNT(*) as COUNT
            FROM PDF_DOWNLOAD_DMS_CLAIMS 
            GROUP BY STATUS
        """
        cursor = connection.cursor()
        cursor.execute(stats_query)
        stats = cursor.fetchall()
        cursor.close()

        total_files = sum([row[1] for row in stats])
        print(f"📊 Total files tracked: {total_files}")

        for status, count in stats:
            percentage = (count / total_files * 100) if total_files > 0 else 0
            print(f"   {status}: {count} ({percentage:.1f}%)")

        # Check recent activity (last 24 hours)
        recent_query = """
            SELECT COUNT(*) 
            FROM PDF_DOWNLOAD_DMS_CLAIMS 
            WHERE DOWNLOAD_TIMESTAMP >= SYSDATE - 1
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
            AND (RETRY_COUNT < 3 OR RETRY_COUNT IS NULL)
        """
        cursor = connection.cursor()
        cursor.execute(failed_query)
        retry_count = cursor.fetchone()[0]
        cursor.close()

        if retry_count > 0:
            print(f"⚠️  Files pending retry: {retry_count}")
        else:
            print("✅ No files pending retry")

        # Check storage usage
        storage_query = """
            SELECT 
                ROUND(SUM(FILE_SIZE_BYTES) / 1024 / 1024 / 1024, 2) as SIZE_GB,
                COUNT(*) as FILE_COUNT
            FROM PDF_DOWNLOAD_DMS_CLAIMS 
            WHERE STATUS = 'SUCCESS' AND FILE_SIZE_BYTES IS NOT NULL
        """
        cursor = connection.cursor()
        cursor.execute(storage_query)
        storage_result = cursor.fetchone()
        cursor.close()

        if storage_result[0]:
            print(
                f"💾 Storage used: {storage_result[0]} GB ({storage_result[1]} files)"
            )

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
                CLAIM_NO,
                FILE_ID,
                REMOTE_FILE_NAME,
                STATUS,
                TO_CHAR(DOWNLOAD_TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS') as DOWNLOAD_TIME,
                CASE 
                    WHEN ERROR_MESSAGE IS NOT NULL THEN 
                        SUBSTR(ERROR_MESSAGE, 1, 50) || CASE WHEN LENGTH(ERROR_MESSAGE) > 50 THEN '...' ELSE '' END
                    ELSE NULL 
                END as ERROR_SUMMARY
            FROM PDF_DOWNLOAD_DMS_CLAIMS 
            WHERE DOWNLOAD_TIMESTAMP >= SYSDATE - :hours/24
            ORDER BY DOWNLOAD_TIMESTAMP DESC
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
            f"{'Claim':<12} {'File ID':<10} {'Status':<8} {'Download Time':<20} {'Error':<30}"
        )
        print("-" * 80)

        for row in results:
            claim_no = row[0] or "N/A"
            file_id = row[1][:8] + "..." if len(row[1]) > 8 else row[1]
            status = row[3]
            download_time = row[4]
            error = row[5] or ""

            print(
                f"{claim_no:<12} {file_id:<10} {status:<8} {download_time:<20} {error:<30}"
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
                CLAIM_NO,
                FILE_ID,
                RETRY_COUNT,
                TO_CHAR(DOWNLOAD_TIMESTAMP, 'YYYY-MM-DD HH24:MI') as FIRST_ATTEMPT,
                TO_CHAR(LAST_RETRY_TIMESTAMP, 'YYYY-MM-DD HH24:MI') as LAST_RETRY,
                SUBSTR(ERROR_MESSAGE, 1, 60) as ERROR_SUMMARY
            FROM PDF_DOWNLOAD_DMS_CLAIMS 
            WHERE STATUS = 'FAILED' 
            AND (RETRY_COUNT < 3 OR RETRY_COUNT IS NULL)
            ORDER BY DOWNLOAD_TIMESTAMP
        """

        cursor = connection.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        cursor.close()

        if not results:
            print("✅ No failed downloads needing attention")
            return

        print(
            f"{'Claim':<12} {'File ID':<12} {'Retries':<8} {'First Try':<16} {'Last Retry':<16} {'Error':<30}"
        )
        print("-" * 100)

        for row in results:
            claim_no = row[0] or "N/A"
            file_id = row[1][:10] + ".." if len(row[1]) > 10 else row[1]
            retry_count = row[2] or 0
            first_attempt = row[3]
            last_retry = row[4] or "Never"
            error = row[5] or ""

            print(
                f"{claim_no:<12} {file_id:<12} {retry_count:<8} {first_attempt:<16} {last_retry:<16} {error:<30}"
            )

    except Exception as e:
        print(f"❌ Failed to get failed downloads: {e}")


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
            AND DOWNLOAD_TIMESTAMP < SYSDATE - :days
        """

        cursor = connection.cursor()
        cursor.execute(count_query, {"days": days})
        count_to_delete = cursor.fetchone()[0]
        cursor.close()

        if count_to_delete == 0:
            print("✅ No old failed records to clean up")
            return

        print(f"Found {count_to_delete} old failed records to delete")

        # Ask for confirmation
        response = input("Do you want to proceed with deletion? (y/N): ")
        if response.lower() != "y":
            print("❌ Cleanup cancelled")
            return

        # Perform the deletion
        delete_query = """
            DELETE FROM PDF_DOWNLOAD_DMS_CLAIMS 
            WHERE STATUS = 'FAILED' 
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
                    RETRY_COUNT = 0,
                    LAST_RETRY_TIMESTAMP = NULL,
                    ERROR_MESSAGE = NULL
                WHERE FILE_ID IN ({placeholders})
                AND STATUS = 'FAILED'
            """

            params = {f"id{i}": file_id for i, file_id in enumerate(file_ids)}

        else:
            # Reset all failed downloads with less than 3 retries
            query = """
                UPDATE PDF_DOWNLOAD_DMS_CLAIMS 
                SET STATUS = 'PENDING',
                    RETRY_COUNT = 0,
                    LAST_RETRY_TIMESTAMP = NULL
                WHERE STATUS = 'FAILED' 
                AND (RETRY_COUNT < 3 OR RETRY_COUNT IS NULL)
            """
            params = {}

        cursor = connection.cursor()
        cursor.execute(query, params)
        updated_count = cursor.rowcount
        connection.commit()
        cursor.close()

        print(f"✅ Reset {updated_count} failed downloads for retry")

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

            # Overall statistics
            stats_query = """
                SELECT 
                    STATUS,
                    COUNT(*) as COUNT,
                    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as PERCENTAGE,
                    MIN(DOWNLOAD_TIMESTAMP) as FIRST_DOWNLOAD,
                    MAX(DOWNLOAD_TIMESTAMP) as LAST_DOWNLOAD
                FROM PDF_DOWNLOAD_DMS_CLAIMS 
                GROUP BY STATUS
                ORDER BY COUNT DESC
            """

            cursor = connection.cursor()
            cursor.execute(stats_query)
            results = cursor.fetchall()
            cursor.close()

            f.write("Overall Statistics:\n")
            f.write("-" * 20 + "\n")
            for row in results:
                f.write(f"Status: {row[0]}\n")
                f.write(f"  Count: {row[1]} ({row[2]}%)\n")
                f.write(f"  Period: {row[3]} to {row[4]}\n\n")

            # Daily breakdown for last 30 days
            daily_query = """
                SELECT 
                    TO_CHAR(DOWNLOAD_TIMESTAMP, 'YYYY-MM-DD') as DOWNLOAD_DATE,
                    STATUS,
                    COUNT(*) as COUNT
                FROM PDF_DOWNLOAD_DMS_CLAIMS 
                WHERE DOWNLOAD_TIMESTAMP >= SYSDATE - 30
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
    parser.add_argument("--cleanup", type=int, help="Cleanup old failed records (days)")
    parser.add_argument(
        "--reset-failed", action="store_true", help="Reset failed downloads for retry"
    )
    parser.add_argument("--export", type=str, help="Export statistics to file")
    parser.add_argument("--all", action="store_true", help="Show all information")

    args = parser.parse_args()

    # If no arguments provided, show help
    if len(sys.argv) == 1:
        parser.print_help()
        return

    # Connect to database
    try:
        connection = db_handler.get_db_connection()
        if not connection:
            print("❌ Failed to connect to database")
            return

        print(
            f"🚀 PDF Download Service Monitor - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        )

        if args.all or args.health:
            check_service_health(connection)

        if args.all or args.recent:
            show_recent_activity(connection, args.recent)

        if args.all or args.failed:
            show_failed_downloads(connection)

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
