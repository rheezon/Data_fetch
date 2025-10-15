"""
Telegram Group Data Fetcher
Fetches messages from Telegram groups and saves to MySQL database
"""

import asyncio
import sys
import os
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime, timedelta
from telethon import TelegramClient
from telethon.errors import FloodWaitError
from dotenv import load_dotenv
import mysql.connector

class TelegramGroupFetcher:
    def __init__(self):
        self.client = None
        self.logger = self._setup_logger()
        load_dotenv()
        
        self.api_id = os.getenv('TELEGRAM_API_ID')
        self.api_hash = os.getenv('TELEGRAM_API_HASH')
        self.phone = os.getenv('TELEGRAM_PHONE_NUMBER')
    
    def _setup_logger(self):
        """Setup rotating file logger"""
        logger = logging.getLogger('TelegramFetcher')
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            handler = RotatingFileHandler(
                'fetcher.log',
                maxBytes=5*1024*1024,  # 5MB
                backupCount=5
            )
            
            formatter = logging.Formatter(
                '%(asctime)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        
        return logger
        
    def get_credentials(self):
        """Validate environment credentials"""
        self.logger.info("Loading Telegram API credentials from environment")
        
        if not self.api_id:
            self.logger.error("TELEGRAM_API_ID not found in environment")
            return False
            
        if not self.api_hash:
            self.logger.error("TELEGRAM_API_HASH not found in environment")
            return False
            
        if not self.phone:
            self.logger.error("TELEGRAM_PHONE_NUMBER not found in environment")
            return False
        
        try:
            self.api_id = int(self.api_id)
        except ValueError:
            self.logger.error("TELEGRAM_API_ID must be a valid integer")
            return False
            
        self.logger.info("Credentials loaded successfully")
        return True
    
    async def connect_client(self):
        """Connect to Telegram and authenticate"""
        try:
            self.client = TelegramClient('session_name', self.api_id, self.api_hash)
            
            self.logger.info("Connecting to Telegram")
            await self.client.connect()
            
            if not await self.client.is_user_authorized():
                self.logger.error("Client not authorized. Manual authentication required.")
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
    
    async def fetch_messages_from_group(self, group, group_name, time_limit):
        """Fetch messages from a specific group within the time limit"""
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
                    'text': message.text or '',
                    'media': bool(message.media),
                    'forward_from': getattr(message.forward, 'from_id', None) if message.forward else None
                })
                
                if len(messages) % 100 == 0:
                    await asyncio.sleep(0.1)
            
            self.logger.info(f"Found {len(messages)} messages in '{group_name}' since last fetch")
            return messages
            
        except FloodWaitError as e:
            self.logger.warning(f"Rate limited. Waiting {e.seconds} seconds")
            await asyncio.sleep(e.seconds)
            return await self.fetch_messages_from_group(group, group_name, time_limit)
        except Exception as e:
            self.logger.error(f"Error fetching messages from '{group_name}': {e}")
            return []
    
    def save_messages_to_db(self, messages):
        """Save messages to MySQL database using batch insert"""
        database_url = os.getenv('DATABASE_URL')
        if not database_url:
            self.logger.error("DATABASE_URL not found in environment")
            return
        
        if not messages:
            self.logger.info("No messages to save")
            return
        
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
                (msg['id'], msg['sender_id'], msg['text'], 'pending_ai_review', msg['date'])
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
    """Non-interactive task to fetch messages from Telegram groups"""
    fetcher = TelegramGroupFetcher()
    
    if not fetcher.get_credentials():
        return
    
    if not await fetcher.connect_client():
        return
    
    try:
        channels = config['channels']
        last_fetched_at = config['last_fetched_at']
        
        fetcher.logger.info(f"Fetching messages newer than: {last_fetched_at}")
        
        all_messages = []
        
        for channel_name in channels:
            try:
                fetcher.logger.info(f"Processing channel: '{channel_name}'")
                
                group = await fetcher.find_group(channel_name)
                if not group:
                    fetcher.logger.warning(f"Channel '{channel_name}' not found")
                    continue
                
                fetcher.logger.info(f"Found channel: {group.title}")
                
                messages = await fetcher.fetch_messages_from_group(group, channel_name, last_fetched_at)
                
                for msg in messages:
                    msg['group_name'] = channel_name
                
                all_messages.extend(messages)
                
            except Exception as e:
                fetcher.logger.error(f"Error processing channel '{channel_name}': {e}")
                continue
        
        all_messages.sort(key=lambda x: x['date'], reverse=True)
        
        fetcher.logger.info(f"Total messages found: {len(all_messages)}")
        
        if all_messages:
            fetcher.save_messages_to_db(all_messages)
        else:
            fetcher.logger.info("No new messages found")
            
    except Exception as e:
        fetcher.logger.error(f"Error during execution: {e}")
    
    finally:
        if fetcher.client:
            await fetcher.client.disconnect()
            fetcher.logger.info("Disconnected from Telegram")

if __name__ == "__main__":
    # Sample configuration for manual testing
    config = {
        'channels': ['Tech Jobs', 'Dev Careers', 'Python Jobs'],
        'last_fetched_at': datetime.now() - timedelta(hours=24)  # 24 hours ago
    }
    
    try:
        asyncio.run(run_fetcher_task(config))
    except Exception as e:
        logging.error(f"Fatal error: {e}")
        sys.exit(1)