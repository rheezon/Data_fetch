"""
One-time Telegram session generator
Run this script once to generate a session string for production use
"""

import asyncio
import os
from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError
from dotenv import load_dotenv
import getpass

async def generate_session():
    """Generate Telegram session string"""
    load_dotenv()
    
    api_id = os.getenv('TELEGRAM_API_ID')
    api_hash = os.getenv('TELEGRAM_API_HASH')
    phone = os.getenv('TELEGRAM_PHONE_NUMBER')
    
    if not all([api_id, api_hash, phone]):
        print("Error: Missing credentials in .env file")
        return
    
    try:
        api_id = int(api_id)
    except ValueError:
        print("Error: TELEGRAM_API_ID must be a valid integer")
        return
    
    client = TelegramClient('temp_session', api_id, api_hash)
    
    try:
        await client.connect()
        
        if not await client.is_user_authorized():
            print(f"Sending verification code to {phone}")
            await client.send_code_request(phone)
            
            code = input("Enter verification code: ").strip()
            
            try:
                await client.sign_in(phone, code)
            except SessionPasswordNeededError:
                password = getpass.getpass("Enter 2FA password: ")
                await client.sign_in(password=password)
        
        # Generate and print session string
        session_string = client.session.save()
        print("\nSession generated successfully!")
        print("Add this to your .env file:")
        print(f"TELEGRAM_SESSION_STRING={session_string}")
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        await client.disconnect()

if __name__ == "__main__":
    asyncio.run(generate_session())