#!/usr/bin/env python3
"""Debug FTP connection and listing with performance optimizations."""

import asyncio
import logging
from arrow_sus.metadata.async_ftp import AsyncFTPClient

# Set to INFO to reduce noise
logging.basicConfig(level=logging.INFO)


async def debug_ftp():
    print("Testing optimized FTP connection...")

    client = AsyncFTPClient()

    try:
        # Test limited directory listing for performance
        print("Testing directory listing (limited to 10 files, 5s timeout)...")

        files = await client.list_directory(
            "/dissemin/publicos/SIHSUS/200801_/Dados",
            max_depth=1,
            use_cache=False,
            max_files=10,
            timeout=5.0,
        )
        print(f"Found {len(files)} files:")
        for file in files:
            print(f"  {file['filename']} - {file['size']} bytes - {file['extension']}")

        # Test another dataset path
        print("\nTesting another dataset path...")
        files2 = await client.list_directory(
            "/dissemin/publicos/SINASC/DADOS",
            max_depth=2,
            use_cache=False,
            max_files=20,
            timeout=10.0,
        )
        print(f"Found {len(files2)} files in SINASC:")
        for file in files2[:5]:  # Show first 5
            print(f"  {file['full_path']} - {file['size']} bytes")

    except asyncio.TimeoutError:
        print("Timeout! FTP operation took too long")
    except Exception as e:
        print(f"Error: {e}")
        import traceback

        traceback.print_exc()
    finally:
        await client.close()


if __name__ == "__main__":
    asyncio.run(debug_ftp())
