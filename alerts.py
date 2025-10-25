#!/usr/bin/env python3

import logging
import asyncio
import telegram
import db
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Function to read the listings from database and format messages
def format_messages_from_db():
    properties = db.get_properties_for_today_filtered()
    messages = []

    # Create a summary message first
    summary = f"🏠 Found {len(properties)} new listings matching your criteria:\n\n"

    for prop in properties:
        # Format each listing nicely
        price_usd = f"${prop['price_total_usd']:.0f}" if prop['price_total_usd'] else "N/A"
        size = f"{prop['size']}m²" if prop['size'] else "N/A"
        bedrooms = f"{int(prop['bedrooms'])} bed" if prop['bedrooms'] else "N/A"

        message = (
            f"📍 {prop['address']}\n"
            f"💵 {price_usd}/month (total)\n"
            f"📏 {size} | 🛏️ {bedrooms}\n"
            f"🔗 {prop['listing_url']}\n"
            f"---"
        )
        messages.append(message)

    return summary, messages

async def send_messages_batch(token, chat_id, summary, messages, max_messages=10):
    # Initialize bot with token
    bot = telegram.Bot(token)
    
    # Send summary first
    await bot.send_message(chat_id=chat_id, text=summary)
    await asyncio.sleep(0.5)
    
    # Send first max_messages listings (to avoid spam)
    for i, message in enumerate(messages[:max_messages]):
        try:
            await bot.send_message(chat_id=chat_id, text=message)
            await asyncio.sleep(0.5)  # Small delay to avoid rate limiting
        except Exception as e:
            print(f"Error sending message {i+1}: {e}")
    
    if len(messages) > max_messages:
        remaining = len(messages) - max_messages
        final_msg = f"\n📋 Plus {remaining} more listings! Check the full CSV file for all results."
        await bot.send_message(chat_id=chat_id, text=final_msg)

def main():
    # Set up logging
    logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)

    # Initialize database
    db.init_database()

    # Get credentials
    TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
    CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')

    if not TOKEN or not CHAT_ID:
        print("\n⚠️  Telegram credentials not found in environment variables.")
        print("\nTo send alerts, please set the following in .env file:")
        print("  TELEGRAM_BOT_TOKEN='your_bot_token_here'")
        print("  TELEGRAM_CHAT_ID='your_chat_id_here'")
        return

    # Format messages from database
    summary, messages = format_messages_from_db()

    if not messages:
        print("No filtered listings found for today.")
        return

    print(f"📤 Sending summary and first 10 of {len(messages)} listings to Telegram...")

    # Create event loop and send messages
    loop = asyncio.get_event_loop()
    loop.run_until_complete(send_messages_batch(TOKEN, CHAT_ID, summary, messages))

    print("✅ Messages sent successfully!")

if __name__ == '__main__':
    main()