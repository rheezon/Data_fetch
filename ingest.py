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
        """Save messages to MySQL database using batch insert"""
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
            
            # Prepare batch data with text hashes
            batch_data = [
                (msg['text'], msg['date'], self._generate_text_hash(msg['text']))
                for msg in messages
            ]
            
            total_messages = len(batch_data)
            
            # Batch insert with IGNORE to skip duplicates
            cursor.executemany(
                """
                INSERT IGNORE INTO jobs 
                (job, job_timestamp, text_hash)
                VALUES (%s, %s, %s)
                """,
                batch_data
            )
            
            conn.commit()
            inserted = cursor.rowcount
            skipped = total_messages - inserted
            
            self.logger.info(f"Inserted {inserted} new messages, skipped {skipped} duplicates")
            
        except Exception as e:
            self.logger.error(f"Error saving messages to database: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
        finally:
            if 'cursor' in locals():
                cursor.close()
            if 'conn' in locals():
                conn.close()
    
    def cleanup_old_processed(self):
        """Delete processed jobs older than 7 days"""
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
            
            # Delete old processed jobs
            cursor.execute(
                """
                DELETE FROM jobs
                WHERE processed = 1
                  AND job_timestamp < (NOW() - INTERVAL 7 DAY)
                """
            )
            
            conn.commit()
            deleted = cursor.rowcount
            
            if deleted > 0:
                self.logger.info(f"Cleanup: Deleted {deleted} old processed jobs")
            
        except Exception as e:
            self.logger.error(f"Error during cleanup: {e}")
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