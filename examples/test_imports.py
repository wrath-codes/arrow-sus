#!/usr/bin/env python3
"""
Test script to verify all imports work correctly after refactoring
to use specific imports instead of full module imports.
"""


def test_high_performance_metadata():
    """Test imports from high_performance_metadata.py"""
    print("Testing high_performance_metadata.py imports...")

    try:
        from asyncio import run, gather, sleep
        from pathlib import Path
        from datetime import datetime
        from time import time
        from json import dumps

        from arrow_sus.metadata import (
            DataSUSMetadataClient,
            UFCode,
            DatasetSource,
            DataSUSConfig,
            CacheConfig,
            PerformanceConfig,
        )

        print("✅ high_performance_metadata.py imports successful")
        return True
    except ImportError as e:
        print(f"❌ high_performance_metadata.py import failed: {e}")
        return False


def test_integration_patterns():
    """Test imports from integration_patterns.py"""
    print("Testing integration_patterns.py imports...")

    try:
        from asyncio import run
        from json import loads, dumps
        from subprocess import run as subprocess_run, CalledProcessError, PIPE
        from datetime import datetime, timedelta
        from pathlib import Path
        from typing import Dict, List, Any

        from arrow_sus.metadata import DataSUSMetadataClient, UFCode, DatasetSource

        print("✅ integration_patterns.py core imports successful")

        # Test optional imports
        optional_imports = []

        try:
            from polars import DataFrame as PlDataFrame, count, col

            optional_imports.append("polars")
        except ImportError:
            pass

        try:
            from pandas import DataFrame as PdDataFrame

            optional_imports.append("pandas")
        except ImportError:
            pass

        try:
            from duckdb import connect

            optional_imports.append("duckdb")
        except ImportError:
            pass

        if optional_imports:
            print(f"✅ Optional imports available: {', '.join(optional_imports)}")
        else:
            print(
                "ℹ️  No optional imports available (install polars, pandas, duckdb for full functionality)"
            )

        return True
    except ImportError as e:
        print(f"❌ integration_patterns.py import failed: {e}")
        return False


def test_production_monitoring():
    """Test imports from production_monitoring.py"""
    print("Testing production_monitoring.py imports...")

    try:
        from asyncio import run, sleep
        from json import dumps, loads
        from logging import (
            basicConfig,
            FileHandler,
            StreamHandler,
            getLogger,
            INFO,
            ERROR,
            WARNING,
        )
        from time import time
        from datetime import datetime, timedelta
        from pathlib import Path
        from typing import Dict, List, Any, Optional

        from arrow_sus.metadata import DataSUSMetadataClient, DataSUSConfig, CacheConfig

        print("✅ production_monitoring.py core imports successful")

        # Test optional psutil imports
        try:
            from psutil import (
                cpu_percent,
                virtual_memory,
                disk_usage,
                net_connections,
                Process,
            )

            print("✅ psutil imports available")
        except ImportError:
            print("ℹ️  psutil not available (install psutil for system monitoring)")

        return True
    except ImportError as e:
        print(f"❌ production_monitoring.py import failed: {e}")
        return False


def test_metadata_system():
    """Test the core metadata system imports."""
    print("Testing core metadata system...")

    try:
        from arrow_sus.metadata import DataSUSMetadataClient, UFCode, DatasetSource

        print("✅ Core metadata system imports successful")

        # Test enum values
        print(f"✅ UFCode sample: {UFCode.SP}")
        print(f"✅ DatasetSource sample: {DatasetSource.SIH}")

        return True
    except ImportError as e:
        print(f"❌ Core metadata system import failed: {e}")
        return False


def main():
    """Run all import tests."""
    print("🧪 Testing Import Refactoring")
    print("=" * 40)

    tests = [
        test_metadata_system,
        test_high_performance_metadata,
        test_integration_patterns,
        test_production_monitoring,
    ]

    results = []
    for test in tests:
        try:
            result = test()
            results.append(result)
        except Exception as e:
            print(f"❌ Test {test.__name__} failed with exception: {e}")
            results.append(False)
        print()  # Add spacing between tests

    # Summary
    passed = sum(results)
    total = len(results)

    print("📊 Test Summary")
    print("=" * 15)
    print(f"Passed: {passed}/{total}")

    if passed == total:
        print("🎉 All import tests passed!")
        print("\n✨ Benefits of specific imports:")
        print("• Reduced memory footprint")
        print("• Faster import times")
        print("• Clearer dependencies")
        print("• Better IDE autocomplete")
        print("• Explicit function/class usage")
    else:
        print(f"⚠️  {total - passed} test(s) failed")
        print("Check the error messages above for details")

    return passed == total


if __name__ == "__main__":
    import sys

    success = main()
    sys.exit(0 if success else 1)
