"""
Manual testing script for Telegram ingestion service
Import and test the production code with sample configuration
"""

import asyncio
from datetime import datetime, timedelta, timezone
from ingest import run_fetcher_task

if __name__ == "__main__":
    # Sample configuration for testing
    config = {
        'last_fetched_at': datetime.now(timezone.utc) - timedelta(hours=24)  # Last 24 hours
    }
    
    try:
        asyncio.run(run_fetcher_task(config))
    except Exception as e:
        print(f"Test failed: {e}")
        exit(1)