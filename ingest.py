"""
Production Telegram message ingestion service
Fetches messages from configured Telegram groups and saves to MySQL database
"""

import asyncio
import os
import sys
import io
import logging
import re
import hashlib

# Fix UTF-8 encoding for Windows console
if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')
from logging.handlers import TimedRotatingFileHandler
from datetime import datetime, timezone
from urllib.parse import unquote
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import FloodWaitError
from dotenv import load_dotenv
import mysql.connector

class TelegramIngestionService:
    def __init__(self):
        self.client = None
        self.logger = self._setup_logger()
        load_dotenv()
        
        self.session_string = os.getenv('TELEGRAM_SESSION_STRING')
        self.groups = self._parse_groups()
        
        # Ensure database tables exist on startup
        self.ensure_tables_exist()
    
    def _setup_logger(self):
        """Setup rotating file logger for 72-hour retention"""
        logger = logging.getLogger('TelegramIngestion')
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            # Create Logs directory if it doesn't exist
            log_dir = os.path.join(os.path.dirname(__file__), 'Logs')
            os.makedirs(log_dir, exist_ok=True)
            
            # New log every 12 hours, keep 6 files (72 hours total)
            handler = TimedRotatingFileHandler(
                os.path.join(log_dir, 'ingest.log'),
                when='h',
                interval=12,
                backupCount=6,
                encoding='utf-8'
            )
            
            formatter = logging.Formatter(
                '%(asctime)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        
        return logger
    
    def _parse_groups(self):
        """Parse comma-separated groups from environment"""
        groups_str = os.getenv('TELEGRAM_GROUPS', '')
        if not groups_str:
            self.logger.error("TELEGRAM_GROUPS not found in environment")
            return []
        
        groups = [group.strip() for group in groups_str.split(',') if group.strip()]
        self.logger.info(f"Configured groups: {groups}")
        return groups
    
    def _generate_text_hash(self, text):
        """Generate SHA-256 hash of normalized text for deduplication"""
        if not text:
            return hashlib.sha256(b'').hexdigest()
        
        # Normalize text: remove URLs, punctuation, lowercase, trim spaces
        normalized = text.lower()
        normalized = re.sub(r'http[s]?://\S+', '', normalized)  # Remove URLs
        normalized = re.sub(r'[^\w\s]', '', normalized)  # Remove punctuation
        normalized = re.sub(r'\s+', ' ', normalized).strip()  # Normalize whitespace
        
        return hashlib.sha256(normalized.encode('utf-8')).hexdigest()
    
    def _validate_config(self):
        """Validate required configuration"""
        if not self.session_string:
            self.logger.error("TELEGRAM_SESSION_STRING not found in environment")
            return False
        
        if not self.groups:
            self.logger.error("No groups configured")
            return False
        
        if not os.getenv('DATABASE_URL'):
            self.logger.error("DATABASE_URL not found in environment")
            return False
        
        return True
    
    def ensure_tables_exist(self):
        """Create database tables if they don't exist and add foreign key if missing"""
        database_url = os.getenv('DATABASE_URL')
        if not database_url:
            self.logger.error("DATABASE_URL not found for table creation")
            return
        
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
            
            # Create job_hashes table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS job_hashes (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    text_hash VARCHAR(64) UNIQUE NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Create jobs table with proper schema
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS jobs (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    job TEXT,
                    job_timestamp DATETIME,
                    processed BIT(1) DEFAULT 0,
                    timestamp DATETIME,
                    created_at DATETIME,
                    hash_id BIGINT,
                    INDEX idx_hash_id (hash_id)
                )
            """)
            
            # Add hash_id column if it doesn't exist
            try:
                cursor.execute("ALTER TABLE jobs ADD COLUMN hash_id BIGINT")
                self.logger.info("Added hash_id column to jobs table")
            except mysql.connector.Error as e:
                if "Duplicate column name" in str(e):
                    pass  # Column already exists
                else:
                    raise
            
            # Add foreign key constraint if it doesn't exist
            try:
                cursor.execute("""
                    ALTER TABLE jobs ADD CONSTRAINT fk_jobs_hash_id 
                    FOREIGN KEY (hash_id) REFERENCES job_hashes(id) 
                    ON DELETE SET NULL ON UPDATE CASCADE
                """)
                self.logger.info("Added foreign key constraint to jobs table")
            except mysql.connector.Error as e:
                if "Duplicate foreign key constraint" in str(e) or "already exists" in str(e):
                    pass  # Constraint already exists
                else:
                    raise
            
            conn.commit()
            self.logger.info("Database schema ensured: job_hashes, jobs with foreign key")
            
        except Exception as e:
            self.logger.error(f"Error ensuring database schema: {e}")
        finally:
            if 'cursor' in locals():
                cursor.close()
            if 'conn' in locals():
                conn.close()
    
    async def connect_client(self):
        """Connect to Telegram using session string"""
        try:
            api_id = os.getenv('TELEGRAM_API_ID')
            api_hash = os.getenv('TELEGRAM_API_HASH')
            
            if not api_id or not api_hash:
                self.logger.error("TELEGRAM_API_ID or TELEGRAM_API_HASH not found")
                return False
            
            self.client = TelegramClient(StringSession(self.session_string), int(api_id), api_hash)
            
            self.logger.info("Connecting to Telegram")
            await self.client.connect()
            
            if not await self.client.is_user_authorized():
                self.logger.error("Session invalid or expired")
                return False
            
            self.logger.info("Successfully connected to Telegram")
            return True
            
        except Exception as e:
            self.logger.error(f"Error connecting to Telegram: {e}")
            return False
    
    async def find_group(self, group_name):
        """Find a group by name"""
        try:
            async for dialog in self.client.iter_dialogs():
                if dialog.is_group or dialog.is_channel:
                    if dialog.name.lower() == group_name.lower():
                        return dialog.entity
                    if group_name.lower() in dialog.name.lower():
                        return dialog.entity
            return None
        except Exception as e:
            self.logger.error(f"Error finding group '{group_name}': {e}")
            return None
    
    async def fetch_messages_from_group(self, group, group_name, group_id, time_limit):
        """Fetch messages from a specific group since time_limit"""
        try:
            messages = []
            self.logger.info(f"Fetching messages from '{group_name}'")
            
            async for message in self.client.iter_messages(group, limit=None):
                # Ensure both datetimes are timezone-aware for comparison
                msg_date = message.date
                if time_limit.tzinfo is None:
                    time_limit = time_limit.replace(tzinfo=timezone.utc)
                if msg_date.tzinfo is None:
                    msg_date = msg_date.replace(tzinfo=timezone.utc)
                
                if msg_date < time_limit:
                    break
                
                messages.append({
                    'id': message.id,
                    'date': message.date,
                    'sender_id': message.sender_id,
                    'group_id': group_id,
                    'text': message.text or '',
                    'media': bool(message.media),
                    'forward_from': getattr(message.forward, 'from_id', None) if message.forward else None
                })
                
                if len(messages) % 100 == 0:
                    await asyncio.sleep(0.1)
            
            self.logger.info(f"Found {len(messages)} messages in '{group_name}'")
            return messages
            
        except FloodWaitError as e:
            self.logger.warning(f"Rate limited. Waiting {e.seconds} seconds")
            await asyncio.sleep(e.seconds)
            return await self.fetch_messages_from_group(group, group_name, group_id, time_limit)
        except Exception as e:
            self.logger.error(f"Error fetching messages from '{group_name}': {e}")
            return []
    
    def save_messages_to_db(self, messages):
        """Save messages using proper foreign key relationship"""
        if not messages:
            self.logger.info("No messages to save")
            return
        
        database_url = os.getenv('DATABASE_URL')
        
        try:
            # Parse MySQL connection URL
            url_parts = database_url.replace('mysql+mysqlconnector://', '').replace('mysql://', '').split('/')
            auth_host = url_parts[0]
            database = url_parts[1] if len(url_parts) > 1 else 'test'
            
            # Split from the right to handle @ in password
            auth, host_port = auth_host.rsplit('@', 1)
            username, password = auth.split(':', 1)
            
            # URL decode the password
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
            
            inserted_jobs = 0
            skipped_jobs = 0
            
            for msg in messages:
                try:
                    # Step 1: Generate normalized hash
                    text_hash = self._generate_text_hash(msg['text'])
                    
                    # Step 2: Insert hash only if not exists (prevents duplicates)
                    cursor.execute(
                        "INSERT IGNORE INTO job_hashes (text_hash) VALUES (%s)",
                        (text_hash,)
                    )
                    
                    # Step 3: Fetch hash_id (always exists after INSERT IGNORE)
                    cursor.execute(
                        "SELECT id FROM job_hashes WHERE text_hash = %s",
                        (text_hash,)
                    )
                    hash_result = cursor.fetchone()
                    
                    if not hash_result:
                        self.logger.error(f"Critical error: hash_id not found for {text_hash[:16]}...")
                        skipped_jobs += 1
                        continue
                    
                    hash_id = hash_result[0]
                    
                    # Step 4: Insert job using hash_id (prevents duplicate jobs)
                    cursor.execute(
                        """
                        INSERT INTO jobs (job, job_timestamp, hash_id, processed, timestamp, created_at)
                        SELECT %s, %s, %s, 0, %s, NOW()
                        WHERE NOT EXISTS (
                            SELECT 1 FROM jobs WHERE hash_id = %s
                        )
                        """,
                        (msg['text'], msg['date'], hash_id, msg['date'], hash_id)
                    )
                    
                    # Step 5: Count results
                    if cursor.rowcount > 0:
                        inserted_jobs += 1
                    else:
                        skipped_jobs += 1
                        
                except Exception as e:
                    self.logger.error(f"Error processing message: {e}")
                    skipped_jobs += 1
                    continue
            
            conn.commit()
            self.logger.info(f"✅ Inserted {inserted_jobs} new jobs, skipped {skipped_jobs} duplicates")
            
        except Exception as e:
            self.logger.error(f"❌ Database error: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
        finally:
            if 'cursor' in locals():
                cursor.close()
            if 'conn' in locals():
                conn.close()
    
    def cleanup_old_processed(self):
        """Delete processed jobs older than 7 days and orphaned hashes"""
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
            
            # Delete old processed jobs (foreign key will handle hash cleanup)
            cursor.execute(
                """
                DELETE FROM jobs
                WHERE processed = 1
                  AND job_timestamp < (NOW() - INTERVAL 7 DAY)
                """
            )
            
            deleted_jobs = cursor.rowcount
            
            # Clean up orphaned hashes (no jobs reference them)
            cursor.execute(
                """
                DELETE jh FROM job_hashes jh
                LEFT JOIN jobs j ON jh.id = j.hash_id
                WHERE j.hash_id IS NULL
                  AND jh.created_at < (NOW() - INTERVAL 7 DAY)
                """
            )
            
            deleted_hashes = cursor.rowcount
            
            conn.commit()
            
            if deleted_jobs > 0 or deleted_hashes > 0:
                self.logger.info(f"🧹 Cleanup: Deleted {deleted_jobs} old jobs, {deleted_hashes} orphaned hashes")
            else:
                self.logger.info("🧹 Cleanup: No old data to remove")
            
        except Exception as e:
            self.logger.error(f"❌ Cleanup error: {e}")
        finally:
            if 'cursor' in locals():
                cursor.close()
            if 'conn' in locals():
                conn.close()

async def run_fetcher_task(config):
    """Main ingestion task - fetches messages from all configured groups"""
    service = TelegramIngestionService()
    
    if not service._validate_config():
        return
    
    if not await service.connect_client():
        return
    
    try:
        last_fetched_at = config['last_fetched_at']
        service.logger.info(f"Fetching messages newer than: {last_fetched_at}")
        
        all_messages = []
        
        for group_name in service.groups:
            try:
                service.logger.info(f"Processing group: '{group_name}'")
                
                group = await service.find_group(group_name)
                if not group:
                    service.logger.warning(f"Group '{group_name}' not found")
                    continue
                
                group_id = group.id
                service.logger.info(f"Found group: {group.title} (ID: {group_id})")
                
                messages = await service.fetch_messages_from_group(group, group_name, group_id, last_fetched_at)
                
                for msg in messages:
                    msg['group_name'] = group_name
                
                all_messages.extend(messages)
                
            except Exception as e:
                service.logger.error(f"Error processing group '{group_name}': {e}")
                continue
        
        all_messages.sort(key=lambda x: x['date'], reverse=True)
        
        service.logger.info(f"Total messages found: {len(all_messages)}")
        
        if all_messages:
            service.save_messages_to_db(all_messages)
        else:
            service.logger.info("No new messages found")
            
    except Exception as e:
        service.logger.error(f"Error during execution: {e}")
    
    finally:
        if service.client:
            await service.client.disconnect()
            service.logger.info("Disconnected from Telegram")

# Database maintenance SQL (run as MySQL Event):
"""
CREATE EVENT IF NOT EXISTS cleanup_old_notifications
ON SCHEDULE EVERY 1 DAY
STARTS CURRENT_TIMESTAMP
DO
DELETE FROM JobNotification 
WHERE created_at < DATE_SUB(NOW(), INTERVAL 48 HOUR);
"""