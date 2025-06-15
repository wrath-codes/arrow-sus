"""
Test script for the arrow_sus logger module.
Tests various logging configurations, formatters, and features.
"""

import logging
import os
import sys
import tempfile
import time
from pathlib import Path

# Add the src directory to the path so we can import the logger
sys.path.insert(0, "src")

from arrow_sus.log.logger import (
    CatppuccinFormatter,
    JSONFormatter,
    NonErrorFilter,
    VerboseFilter,
    add_custom_level,
    get_dynamic_log_filename,
    get_logger,
    setup_logging,
)


def test_custom_levels():
    """Test the custom logging levels."""
    print("\n" + "=" * 60)
    print("TESTING CUSTOM LOGGING LEVELS")
    print("=" * 60)

    # Add custom levels first
    add_custom_level("TRACE", 5)
    add_custom_level("SUCCESS", 22)
    add_custom_level("NOTICE", 25)

    # Setup logger with catppuccin theme
    logger = setup_logging(
        logger_name="test_custom_levels",
        theme="catppuccin",
        config_dir="src/arrow_sus/log/config",
        app_name="custom_levels_test",
        include_pid=True,
    )

    # Test all levels including custom ones
    logger.trace("This is a TRACE message - very detailed debugging info")
    logger.debug("This is a DEBUG message with some data: %s", {"key": "value"})
    logger.info("This is an INFO message with number: %d", 42)
    logger.success("This is a SUCCESS message - operation completed!")
    logger.notice("This is a NOTICE message - important information")
    logger.warning("This is a WARNING message with list: %s", [1, 2, 3])
    logger.error("This is an ERROR message with string: %s", "error_data")
    logger.critical("This is a CRITICAL message with float: %.2f", 3.14159)

    # Test with extra fields
    logger.success(
        "User registration completed",
        extra={
            "user_id": "user456",
            "email": "test@example.com",
            "registration_time": "2025-01-15T10:30:00Z",
        },
    )

    logger.trace(
        "Function entry trace",
        extra={
            "function": "process_data",
            "args": ["param1", "param2"],
            "thread_id": 12345,
        },
    )

    return logger


def test_basic_logging():
    """Test basic logging functionality."""
    print("\n" + "=" * 60)
    print("TESTING BASIC LOGGING")
    print("=" * 60)

    # Setup logger with catppuccin theme using the correct config directory
    logger = setup_logging(
        logger_name="test_basic",
        theme="catppuccin",
        config_dir="src/arrow_sus/log/config",  # Use your existing config directory
        app_name="test_app",
        include_pid=True,
    )

    # Test different log levels
    logger.debug("This is a debug message with some data: %s", {"key": "value"})
    logger.info("This is an info message with number: %d", 42)
    logger.warning("This is a warning message with list: %s", [1, 2, 3])
    logger.error("This is an error message with string: %s", "error_data")
    logger.critical("This is a critical message with float: %.2f", 3.14159)

    # Test verbose-only logging (should only appear in file, not console)
    logger.debug_verbose("This verbose message should only appear in the log file")

    return logger


def test_queued_logging():
    """Test the queued logging configuration."""
    print("\n" + "=" * 60)
    print("TESTING QUEUED LOGGING")
    print("=" * 60)

    # Setup logger with queued theme
    logger = setup_logging(
        logger_name="test_queued",
        theme="queued",  # This should use the queued_stderr_json_file.yml
        config_dir="src/arrow_sus/log/config",
        app_name="queued_test",
        include_pid=False,
    )

    # Test different log levels with queued handler
    logger.debug("Queued debug message with data: %s", {"queue": "test"})
    logger.info("Queued info message")
    logger.warning("Queued warning message")
    logger.error("Queued error message")

    return logger


def test_exception_logging():
    """Test exception logging."""
    print("\n" + "=" * 60)
    print("TESTING EXCEPTION LOGGING")
    print("=" * 60)

    logger = get_logger("test_exceptions")

    try:
        # Intentionally cause an exception
        result = 10 / 0
        logger.trace(result)
    except ZeroDivisionError:
        logger.error("Caught an exception while dividing", exc_info=True)
        logger.exception("This is using logger.exception() method")

    try:
        # Another exception
        my_dict = {"a": 1}
        value = my_dict["nonexistent_key"]
        logger.trace(value)
    except KeyError as e:
        logger.warning("Key error occurred: %s", str(e), exc_info=True)


def test_custom_fields():
    """Test logging with custom fields."""
    print("\n" + "=" * 60)
    print("TESTING CUSTOM FIELDS")
    print("=" * 60)

    logger = get_logger("test_custom")

    # Add custom fields using extra parameter
    logger.info(
        "User action performed",
        extra={
            "user_id": "user123",
            "action": "login",
            "ip_address": "192.168.1.100",
            "session_id": "sess_abc123",
        },
    )

    logger.error(
        "Database connection failed",
        extra={
            "database": "postgres",
            "host": "db.example.com",
            "port": 5432,
            "retry_count": 3,
        },
    )

    # Test custom levels with extra fields
    logger.success(
        "Payment processed successfully",
        extra={
            "transaction_id": "txn_789",
            "amount": 99.99,
            "currency": "USD",
            "payment_method": "credit_card",
        },
    )

    logger.notice(
        "System maintenance scheduled",
        extra={
            "maintenance_window": "2025-01-20 02:00-04:00 UTC",
            "affected_services": ["api", "web", "database"],
            "estimated_downtime": "2 hours",
        },
    )


def test_json_formatter():
    """Test the JSON formatter directly."""
    print("\n" + "=" * 60)
    print("TESTING JSON FORMATTER")
    print("=" * 60)

    # Create a logger with JSON formatter
    logger = logging.getLogger("test_json")
    logger.setLevel(logging.DEBUG)

    # Clear any existing handlers
    logger.handlers.clear()

    # Create a stream handler with JSON formatter
    handler = logging.StreamHandler(sys.stdout)
    formatter = JSONFormatter(
        fmt_keys={
            "level": "levelname",
            "logger": "name",
            "module": "module",
            "message": "message",
            "timestamp": "timestamp",
        }
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    print("JSON formatted output:")
    logger.info(
        "This is a JSON formatted message", extra={"custom_field": "custom_value"}
    )
    logger.error(
        "JSON error message",
        extra={"error_code": 500, "details": {"reason": "server_error"}},
    )

    # Test custom levels in JSON
    logger.success(
        "JSON success message",
        extra={"operation": "data_export", "records_processed": 1500},
    )


def test_catppuccin_formatter():
    """Test the Catppuccin formatter directly."""
    print("\n" + "=" * 60)
    print("TESTING CATPPUCCIN FORMATTER")
    print("=" * 60)

    # Create a logger with Catppuccin formatter
    logger = logging.getLogger("test_catppuccin")
    logger.setLevel(logging.DEBUG)

    # Remove any existing handlers
    logger.handlers.clear()

    # Create a stream handler with Catppuccin formatter
    handler = logging.StreamHandler(sys.stdout)
    formatter = CatppuccinFormatter(datefmt="%H:%M:%S")
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    print("Catppuccin themed output:")
    logger.trace("Trace message with detailed info: %s", {"trace_data": "detailed"})
    logger.debug("Debug message with args: %s, %d", "test", 123)
    logger.info("Info message with data: %s", {"status": "ok"})
    logger.success("Success message with result: %s", {"result": "completed"})
    logger.notice("Notice message with config: %s", {"config_updated": True})
    logger.warning("Warning message with list: %s", [1, 2, 3])
    logger.error("Error message with error code: %d", 404)
    logger.critical("Critical message with details: %s", "system_failure")


def test_dynamic_filename():
    """Test dynamic filename generation."""
    print("\n" + "=" * 60)
    print("TESTING DYNAMIC FILENAME GENERATION")
    print("=" * 60)

    # Test different filename configurations
    filename1 = get_dynamic_log_filename()
    print(f"Default filename: {filename1}")

    filename2 = get_dynamic_log_filename(app_name="my_app")
    print(f"With app name: {filename2}")

    filename3 = get_dynamic_log_filename(app_name="my_app", include_pid=True)
    print(f"With app name and PID: {filename3}")

    filename4 = get_dynamic_log_filename(include_pid=True, prefix="custom")
    print(f"With custom prefix and PID: {filename4}")


def test_multiple_loggers():
    """Test multiple logger instances."""
    print("\n" + "=" * 60)
    print("TESTING MULTIPLE LOGGERS")
    print("=" * 60)

    # Create multiple loggers
    logger1 = get_logger("module1")
    logger2 = get_logger("module2")
    logger3 = get_logger("module3")

    # Each should log with their own name
    logger1.info("Message from module1")
    logger1.success("Success from module1")
    logger2.warning("Message from module2")
    logger2.notice("Notice from module2")
    logger3.error("Message from module3")
    logger3.trace("Trace from module3")

    # Test that they're actually different loggers
    print(f"Logger1 name: {logger1.name}")
    print(f"Logger2 name: {logger2.name}")
    print(f"Logger3 name: {logger3.name}")


def test_verbose_filter():
    """Test the verbose filter functionality."""
    print("\n" + "=" * 60)
    print("TESTING VERBOSE FILTER")
    print("=" * 60)

    logger = setup_logging(
        logger_name="test_verbose",
        theme="catppuccin",
        config_dir="src/arrow_sus/log/config",
        app_name="verbose_test",
    )

    print("Regular messages (should appear in console and file):")
    logger.trace("This is a regular trace message")
    logger.info("This is a regular info message")
    logger.success("This is a regular success message")
    logger.warning("This is a regular warning message")

    print("\nVerbose-only messages (should only appear in file, not console):")
    logger.debug_verbose("This verbose debug message should only be in the file")
    logger.debug_verbose("Another verbose message with data: %s", {"verbose": True})

    print(
        "If you don't see the verbose messages above in console, the filter is working!"
    )


def test_performance():
    """Test logging performance."""
    print("\n" + "=" * 60)
    print("TESTING LOGGING PERFORMANCE")
    print("=" * 60)

    logger = get_logger("performance_test")

    # Test performance with many log messages
    start_time = time.time()
    num_messages = 1000

    for i in range(num_messages):
        level_choice = i % 8
        if level_choice == 0:
            logger.trace("Performance trace %d with data: %s", i, {"iteration": i})
        elif level_choice == 1:
            logger.debug("Performance debug message %d", i)
        elif level_choice == 2:
            logger.info("Performance info message %d", i)
        elif level_choice == 3:
            logger.success("Performance success %d", i)
        elif level_choice == 4:
            logger.notice("Performance notice %d", i)
        elif level_choice == 5:
            logger.warning("Performance warning %d", i)
        elif level_choice == 6:
            logger.error("Performance error %d", i)
        else:
            logger.critical("Performance critical %d", i)

    end_time = time.time()
    duration = end_time - start_time

    print(f"Logged {num_messages} messages in {duration:.3f} seconds")
    print(f"Rate: {num_messages / duration:.1f} messages/second")


def test_file_output():
    """Test that log files are actually created and contain data."""
    print("\n" + "=" * 60)
    print("TESTING FILE OUTPUT")
    print("=" * 60)

    # Create a logger with a specific log file
    test_log_file = "a_sus_logs/test_output.log.jsonl"
    logger = setup_logging(
        logger_name="file_test",
        theme="catppuccin",
        config_dir="src/arrow_sus/log/config",
        log_file=test_log_file,
    )

    # Log some messages including custom levels
    logger.trace("Trace message for file output")
    logger.info("Test message for file output")
    logger.success(
        "Success message for file output", extra={"test_field": "test_value"}
    )
    logger.notice("Notice message for file output")
    logger.error("Error message for file output", extra={"test_field": "test_value"})
    logger.warning("Warning with Unicode: 🚀 🌟 ✨", extra={"unicode": "🎉"})

    # Give the queue handler time to process
    time.sleep(0.1)

    # Check if file exists and has content
    if os.path.exists(test_log_file):
        with open(test_log_file, "r", encoding="utf-8") as f:
            content = f.read()
            print(f"Log file created: {test_log_file}")
            print(f"File size: {len(content)} bytes")
            print("Sample content (last few lines):")
            lines = content.strip().split("\n")
            for i, line in enumerate(lines[-3:], 1):  # Show last 3 lines
                print(f"  Line {i}: {line}")
    else:
        print(f"ERROR: Log file {test_log_file} was not created!")
        # List files in logs directory
        logs_dir = Path("a_sus_logs")
        if logs_dir.exists():
            print("Files in logs directory:")
            for file in logs_dir.iterdir():
                print(f"  - {file}")


def test_config_files():
    """Test that the configuration files exist and are valid."""
    print("\n" + "=" * 60)
    print("TESTING CONFIGURATION FILES")
    print("=" * 60)

    config_dir = Path("src/arrow_sus/log/config")

    # Check catppuccin config
    catppuccin_config = config_dir / "catppuccin_stderr_json_file.yml"
    if catppuccin_config.exists():
        print(f"✓ Found catppuccin config: {catppuccin_config}")
        print(f"  Size: {catppuccin_config.stat().st_size} bytes")
    else:
        print(f"✗ Missing catppuccin config: {catppuccin_config}")

    # Check queued config
    queued_config = config_dir / "queued_stderr_json_file.yml"
    if queued_config.exists():
        print(f"✓ Found queued config: {queued_config}")
        print(f"  Size: {queued_config.stat().st_size} bytes")
    else:
        print(f"✗ Missing queued config: {queued_config}")

    # Try to load and validate the configs
    try:
        import yaml

        if catppuccin_config.exists():
            with open(catppuccin_config) as f:
                config = yaml.safe_load(f)
                print(
                    f"✓ Catppuccin config is valid YAML with {len(config)} top-level keys"
                )

        if queued_config.exists():
            with open(queued_config) as f:
                config = yaml.safe_load(f)
                print(
                    f"✓ Queued config is valid YAML with {len(config)} top-level keys"
                )

    except Exception as e:
        print(f"✗ Error loading config files: {e}")


def test_level_hierarchy():
    """Test that custom levels work correctly in the hierarchy."""
    print("\n" + "=" * 60)
    print("TESTING LEVEL HIERARCHY")
    print("=" * 60)

    logger = get_logger("hierarchy_test")

    print("Testing level hierarchy (all levels should be visible):")
    print(
        "TRACE (5) < DEBUG (10) < INFO (20) < SUCCESS (22) < NOTICE (25) < WARNING (30) < ERROR (40) < CRITICAL (50)"
    )
    print()

    # Set logger to TRACE level to see all messages
    logger.setLevel(5)  # TRACE level

    logger.trace("TRACE level message (5)")
    logger.debug("DEBUG level message (10)")
    logger.info("INFO level message (20)")
    logger.success("SUCCESS level message (22)")
    logger.notice("NOTICE level message (25)")
    logger.warning("WARNING level message (30)")
    logger.error("ERROR level message (40)")
    logger.critical("CRITICAL level message (50)")

    print(
        "\nTesting level filtering (setting level to NOTICE - only NOTICE and above should show):"
    )
    logger.setLevel(25)  # NOTICE level

    logger.trace("TRACE - should NOT appear")
    logger.debug("DEBUG - should NOT appear")
    logger.info("INFO - should NOT appear")
    logger.success("SUCCESS - should NOT appear")
    logger.notice("NOTICE - should appear")
    logger.warning("WARNING - should appear")
    logger.error("ERROR - should appear")
    logger.critical("CRITICAL - should appear")


def test_custom_levels_with_extras():
    """Test custom levels with extra fields and complex data."""
    print("\n" + "=" * 60)
    print("TESTING CUSTOM LEVELS WITH COMPLEX DATA")
    print("=" * 60)

    logger = get_logger("complex_test")

    # TRACE with debugging info
    logger.trace(
        "Function call trace",
        extra={
            "function": "process_user_data",
            "args": ["user_123", {"preferences": {"theme": "dark"}}],
            "execution_time_ms": 45.2,
            "memory_usage_mb": 12.8,
        },
    )

    # SUCCESS with operation results
    logger.success(
        "Data processing completed successfully",
        extra={
            "operation": "batch_import",
            "records_processed": 15420,
            "success_rate": 99.8,
            "duration_seconds": 342.1,
            "output_file": "/data/processed/batch_20250115.json",
        },
    )

    # NOTICE with system information
    logger.notice(
        "Configuration updated",
        extra={
            "config_file": "/etc/app/config.yml",
            "changed_keys": ["database.pool_size", "cache.ttl", "api.rate_limit"],
            "previous_values": {"pool_size": 10, "ttl": 300, "rate_limit": 1000},
            "new_values": {"pool_size": 20, "ttl": 600, "rate_limit": 2000},
            "restart_required": False,
        },
    )

    # Test with Unicode and special characters
    logger.success(
        "🎉 User registration completed! Welcome aboard! 🚀",
        extra={
            "user_name": "José María González",
            "email": "josé@example.com",
            "country": "España",
            "signup_method": "social_oauth",
            "referral_code": "FRIEND2025",
        },
    )


def main():
    """Run all tests."""
    print("Starting logger tests...")
    print(
        "This will test various aspects of the arrow_sus logger module including custom levels."
    )

    try:
        # Add custom levels first - this must be done before any logging setup
        print("Setting up custom logging levels...")
        add_custom_level("TRACE", 5)
        add_custom_level("SUCCESS", 22)
        add_custom_level("NOTICE", 25)
        print("✓ Custom levels added: TRACE (5), SUCCESS (22), NOTICE (25)")

        # First check if config files exist
        test_config_files()

        # Run all tests
        test_dynamic_filename()
        test_custom_levels()  # Test custom levels first
        test_level_hierarchy()
        test_basic_logging()
        test_queued_logging()
        test_exception_logging()
        test_custom_fields()
        test_custom_levels_with_extras()
        test_verbose_filter()
        test_json_formatter()
        test_catppuccin_formatter()
        test_multiple_loggers()
        test_performance()
        test_file_output()

        print("\n" + "=" * 60)
        print("ALL TESTS COMPLETED")
        print("=" * 60)
        print("Check the 'a_sus_logs/' directory for generated log files.")
        print(
            "The console output shows the Catppuccin-themed formatting with custom levels."
        )
        print("Custom levels tested:")
        print("  - TRACE (5): For detailed debugging and tracing")
        print("  - SUCCESS (22): For successful operations")
        print("  - NOTICE (25): For important notices")
        print("The JSON log files contain structured data for analysis.")

    except Exception as e:
        print(f"ERROR during testing: {e}")
        import traceback

        traceback.print_exc()
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
