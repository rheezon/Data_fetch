"""
Production scheduler service for Telegram job fetcher
Runs the ingestion task at configurable intervals
"""

import asyncio
import time
import logging
import os
import sys
import io
from datetime import datetime, timedelta, timezone
from urllib.parse import unquote
from dotenv import load_dotenv
import mysql.connector
from ingest import run_fetcher_task

# Fix UTF-8 encoding for Windows console
if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

# Load environment variables
load_dotenv()

# Setup scheduler logger
log_dir = os.path.join(os.path.dirname(__file__), 'Logs')
os.makedirs(log_dir, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(log_dir, 'scheduler.log'), encoding='utf-8'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger('Scheduler')

def get_last_fetch_time():
    """Get the last fetch time from database or default to 24 hours ago"""
    database_url = os.getenv('DATABASE_URL')
    
    try:
        # Parse MySQL connection URL
        url_parts = database_url.replace('mysql+mysqlconnector://', '').replace('mysql://', '').split('/')
        auth_host = url_parts[0]
        database = url_parts[1] if len(url_parts) > 1 else 'test'
        
        auth, host_port = auth_host.rsplit('@', 1)
        username, password = auth.split(':', 1)
        password = unquote(password)
        
        host = host_port.split(':')[0]
        port = int(host_port.split(':')[1]) if ':' in host_port else 3306
        
        conn = mysql.connector.connect(
            host=host,
            port=port,
            user=username,
            password=password,
            database=database
        )
        cursor = conn.cursor()
        
        # Get the most recent job_timestamp from jobs table
        cursor.execute("SELECT MAX(job_timestamp) FROM jobs")
        result = cursor.fetchone()
        
        cursor.close()
        conn.close()
        
        if result and result[0]:
            # Ensure timezone-aware
            timestamp = result[0]
            if timestamp.tzinfo is None:
                timestamp = timestamp.replace(tzinfo=timezone.utc)
            # Add 1 second to avoid re-fetching the last message
            timestamp = timestamp + timedelta(seconds=1)
            logger.info(f"Resuming from last database entry: {timestamp}")
            return timestamp
        else:
            # No data in table, start from 24 hours ago
            default_time = datetime.now(timezone.utc) - timedelta(hours=24)
            logger.info(f"No previous data found, starting from: {default_time}")
            return default_time
            
    except Exception as e:
        logger.warning(f"Could not query database for last fetch time: {e}")
        # Fallback to 24 hours ago
        default_time = datetime.now(timezone.utc) - timedelta(hours=24)
        logger.info(f"Using default starting time: {default_time}")
        return default_time

def run_task():
    """Execute the fetcher task"""
    logger.info("=" * 60)
    logger.info("Starting scheduled fetch task")
    logger.info("=" * 60)
    
    # Get last fetch time from database
    last_fetch_time = get_last_fetch_time()
    
    config = {
        'last_fetched_at': last_fetch_time
    }
    
    try:
        asyncio.run(run_fetcher_task(config))
        logger.info("Task completed successfully")
        
        # Run cleanup after fetch
        from ingest import TelegramIngestionService
        service = TelegramIngestionService()
        service.cleanup_old_processed()
        
    except Exception as e:
        logger.error(f"Task failed: {e}")
        import traceback
        logger.error(traceback.format_exc())

def main():
    """Main scheduler loop"""
    # Get configurable schedule interval in milliseconds
    interval_ms = int(os.getenv('SCHEDULE_INTERVAL_MS', 18000000))  # Default: 5 hours = 18000000 ms
    interval_seconds = interval_ms / 1000.0
    
    logger.info("Telegram Job Fetcher Scheduler Started")
    logger.info(f"Schedule: Every {interval_ms} milliseconds ({interval_seconds} seconds)")
    logger.info("Press Ctrl+C to stop")
    
    # Run immediately on startup
    logger.info("Running initial fetch...")
    run_task()
    
    # Keep running with millisecond-based timing
    while True:
        time.sleep(interval_seconds)
        run_task()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("\nScheduler stopped by user")
    except Exception as e:
        logger.error(f"Scheduler crashed: {e}")
        import traceback
        logger.error(traceback.format_exc())
