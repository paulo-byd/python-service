#!/usr/bin/env python3
"""
Performance Test Script for PDF Download Service
Tests both sequential and parallel download modes to compare performance.
"""

import sys
import time
import logging
from datetime import datetime
import db_handler

# Import the main functions
from main import (
    run_download_process_sequential,
    run_download_process_parallel,
    log_performance_summary,
    download_times,
    download_times_lock,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("performance_test.log"), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)


def run_performance_comparison(max_files=10):
    """
    Run performance comparison between sequential and parallel downloads.

    Args:
        max_files (int): Maximum number of files to test with (for safety)
    """
    logger.info("🔬 Starting Performance Comparison Test")
    logger.info("=" * 60)

    try:
        # Get files to test with
        files_df = db_handler.get_new_files_to_download()

        if files_df.empty:
            logger.warning("No files available for testing")
            return

        # Limit the test to a reasonable number of files
        test_files = files_df.head(max_files)
        logger.info(f"Testing with {len(test_files)} files")

        # Test 1: Sequential Downloads
        logger.info("\n📊 TEST 1: Sequential Downloads")
        logger.info("-" * 40)

        # Clear performance data
        with download_times_lock:
            download_times.clear()

        sequential_start = time.time()

        # Temporarily replace the get_new_files_to_download function to return our test set
        original_get_files = db_handler.get_new_files_to_download
        db_handler.get_new_files_to_download = lambda: test_files

        try:
            run_download_process_sequential()
            sequential_end = time.time()
            sequential_time = sequential_end - sequential_start

            # Get performance data
            with download_times_lock:
                sequential_data = download_times.copy()

        finally:
            # Restore original function
            db_handler.get_new_files_to_download = original_get_files

        # Test 2: Parallel Downloads (4 workers)
        logger.info("\n📊 TEST 2: Parallel Downloads (4 workers)")
        logger.info("-" * 40)

        # Clear performance data
        with download_times_lock:
            download_times.clear()

        # Wait a bit between tests
        time.sleep(2)

        parallel_start = time.time()

        # Temporarily replace the get_new_files_to_download function again
        db_handler.get_new_files_to_download = lambda: test_files

        try:
            run_download_process_parallel(max_workers=4)
            parallel_end = time.time()
            parallel_time = parallel_end - parallel_start

            # Get performance data
            with download_times_lock:
                parallel_data = parallel_data.copy()

        finally:
            # Restore original function
            db_handler.get_new_files_to_download = original_get_files

        # Test 3: Parallel Downloads (8 workers)
        logger.info("\n📊 TEST 3: Parallel Downloads (8 workers)")
        logger.info("-" * 40)

        # Clear performance data
        with download_times_lock:
            download_times.clear()

        # Wait a bit between tests
        time.sleep(2)

        parallel8_start = time.time()

        # Temporarily replace the get_new_files_to_download function again
        db_handler.get_new_files_to_download = lambda: test_files

        try:
            run_download_process_parallel(max_workers=8)
            parallel8_end = time.time()
            parallel8_time = parallel8_end - parallel8_start

            # Get performance data
            with download_times_lock:
                parallel8_data = download_times.copy()

        finally:
            # Restore original function
            db_handler.get_new_files_to_download = original_get_files

        # Analysis and Results
        logger.info("\n📊 PERFORMANCE COMPARISON RESULTS")
        logger.info("=" * 60)

        logger.info(f"Test files: {len(test_files)}")
        logger.info(f"Sequential time: {sequential_time:.3f}s")
        logger.info(f"Parallel (4w) time: {parallel_time:.3f}s")
        logger.info(f"Parallel (8w) time: {parallel8_time:.3f}s")

        if sequential_time > 0:
            speedup_4w = sequential_time / parallel_time
            speedup_8w = sequential_time / parallel8_time
            logger.info(f"Speedup (4 workers): {speedup_4w:.2f}x")
            logger.info(f"Speedup (8 workers): {speedup_8w:.2f}x")

        # Detailed analysis
        if sequential_data:
            seq_network_time = sum(d["network_time"] for d in sequential_data)
            seq_total_time = sum(d["total_time"] for d in sequential_data)
            seq_network_pct = (
                (seq_network_time / seq_total_time) * 100 if seq_total_time > 0 else 0
            )

            logger.info(f"\nSequential - Network time: {seq_network_pct:.1f}% of total")

        if parallel_data:
            par_network_time = sum(d["network_time"] for d in parallel_data)
            par_total_time = sum(d["total_time"] for d in parallel_data)
            par_network_pct = (
                (par_network_time / par_total_time) * 100 if par_total_time > 0 else 0
            )

            logger.info(
                f"Parallel (4w) - Network time: {par_network_pct:.1f}% of total"
            )

        if parallel8_data:
            par8_network_time = sum(d["network_time"] for d in parallel8_data)
            par8_total_time = sum(d["total_time"] for d in parallel8_data)
            par8_network_pct = (
                (par8_network_time / par8_total_time) * 100
                if par8_total_time > 0
                else 0
            )

            logger.info(
                f"Parallel (8w) - Network time: {par8_network_pct:.1f}% of total"
            )

        # Recommendations
        logger.info("\n💡 RECOMMENDATIONS:")
        if speedup_4w > 1.5:
            logger.info("✅ Parallel downloads show significant improvement!")
            logger.info(
                f"   Recommend enabling parallel_downloads with {4 if speedup_4w > speedup_8w else 8} workers"
            )
        elif speedup_4w > 1.1:
            logger.info("⚠️ Parallel downloads show modest improvement")
            logger.info("   Consider enabling for large batches")
        else:
            logger.info("❌ Parallel downloads do not show significant improvement")
            logger.info("   Network bottleneck likely dominant - keep sequential")

    except Exception as e:
        logger.error(f"Performance test failed: {e}")
        raise


def test_single_file_timing():
    """
    Test detailed timing for a single file download to understand bottlenecks.
    """
    logger.info("\n🔍 Single File Timing Analysis")
    logger.info("-" * 40)

    try:
        files_df = db_handler.get_new_files_to_download()

        if files_df.empty:
            logger.warning("No files available for single file test")
            return

        # Take just one file for detailed analysis
        test_file = files_df.iloc[0]

        logger.info(f"Testing FILE_ID: {test_file['FILE_ID']}")

        # Import the download function
        from main import download_pdf

        config = db_handler.load_config()

        # Run the download with detailed timing
        local_path, error, download_time = download_pdf(
            test_file["FILE_ID"],
            test_file["CREATE_DATE"],
            test_file["CLAIM_ID"],
            config,
        )

        if local_path and not error:
            logger.info("✅ Single file test completed successfully")
        else:
            logger.error(f"❌ Single file test failed: {error}")

    except Exception as e:
        logger.error(f"Single file timing test failed: {e}")


def main():
    """Main function to run performance tests"""
    logger.info("🚀 PDF Download Performance Testing Suite")
    logger.info(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # Check command line arguments
    max_files = 5  # Default safe number for testing

    if len(sys.argv) > 1:
        try:
            max_files = int(sys.argv[1])
            logger.info(f"Testing with {max_files} files")
        except ValueError:
            logger.warning(
                f"Invalid max_files argument: {sys.argv[1]}, using default: {max_files}"
            )

    try:
        # Set environment mode if provided
        if len(sys.argv) > 2:
            env_mode = sys.argv[2]
            if env_mode in ["local", "uat", "prod"]:
                db_handler.set_environment_mode(env_mode)
                logger.info(f"Environment mode set to: {env_mode}")

        # Run single file timing test first
        test_single_file_timing()

        # Run comparison test
        run_performance_comparison(max_files)

        logger.info("\n🎉 Performance testing completed!")
        logger.info("Check the logs above for detailed results and recommendations.")

    except Exception as e:
        logger.error(f"Performance testing failed: {e}")
        return 1

    return 0


if __name__ == "__main__":
    exit(main())
