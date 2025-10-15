"""
Production Telegram message ingestion service
Fetches messages from configured Telegram groups and saves to MySQL database
"""

import asyncio
import os
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime
from telethon import TelegramClient
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
            # New log every 12 hours, keep 6 files (72 hours total)
            handler = RotatingFileHandler(
                'ingest.log',
                maxBytes=50*1024*1024,  # 50MB per file
                backupCount=6,
                when='h',
                interval=12
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
            self.client = TelegramClient('session', session=self.session_string)
            
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
                if message.date < time_limit:
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
            url_parts = database_url.replace('mysql://', '').split('/')
            auth_host = url_parts[0]
            database = url_parts[1] if len(url_parts) > 1 else 'test'
            
            auth, host_port = auth_host.split('@')
            username, password = auth.split(':')
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
            
            # Prepare batch data
            batch_data = [
                (msg['id'], msg['group_id'], msg['text'], 'pending_ai_review', msg['date'])
                for msg in messages
            ]
            
            # Batch insert with IGNORE for deduplication
            cursor.executemany(
                """
                INSERT IGNORE INTO JobNotification 
                (message_id, group_id, job_post_text, status, created_at)
                VALUES (%s, %s, %s, %s, %s)
                """,
                batch_data
            )
            
            conn.commit()
            self.logger.info(f"Batch saved {cursor.rowcount} new messages to database")
            
        except Exception as e:
            self.logger.error(f"Error saving messages to database: {e}")
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