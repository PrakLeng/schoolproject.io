import asyncio
import sqlite3
import feedparser
import logging
import os
import aiohttp
from datetime import datetime
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
BOT_TOKEN = "8660833208:AAHN_VwPxowqsyNgSNGj3B4rVOkW2HeLyF8"

# Configure Logging for long-term monitoring
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Initialize Bot and Dispatcher
bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()

# --- Configuration & Sources ---
# Fast-updating feeds suitable for FX/Gold and Global Events
RSS_FEEDS = [
    "https://www.forexlive.com/feed/news",        # Fast forex, central banks, gold
    "https://www.aljazeera.com/xml/rss/all.xml",  # Fast geopolitical breaking news
    "http://feeds.bbci.co.uk/news/world/rss.xml"  # Global headlines
]

# Keywords for precision sorting
CATEGORIES = {
    "Geopolitics": [
        "war", "strike", "troops", "conflict", "border", "dispute", "treaty", 
        "un", "nato", "military", "operation", "missile", "defense", "sanctions"
    ],
    "Economics": [
        "usd", "eur", "gold", "xau", "xauusd", "cpi", "nfp", "inflation", 
        "rates", "fomc", "ecb", "fed", "imf", "balance of payments", 
        "deficit", "gdp", "powell", "lagarde"
    ]
}

# --- Database Setup ---
def init_db():
    conn = sqlite3.connect('bot_database.db')
    cursor = conn.cursor()
    # Store subscribed users and their preferences (stored as comma-separated string)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            chat_id INTEGER PRIMARY KEY,
            categories TEXT
        )
    ''')
    # Store sent news links to prevent duplicate broadcasts
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS sent_news (
            url TEXT PRIMARY KEY,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    conn.commit()
    conn.close()

# --- Database Helper Functions ---
def add_user(chat_id):
    conn = sqlite3.connect('bot_database.db')
    cursor = conn.cursor()
    # Default to receiving both categories
    cursor.execute('INSERT OR IGNORE INTO users (chat_id, categories) VALUES (?, ?)', 
                   (chat_id, "Geopolitics,Economics"))
    conn.commit()
    conn.close()

def get_users():
    conn = sqlite3.connect('bot_database.db')
    cursor = conn.cursor()
    cursor.execute('SELECT chat_id, categories FROM users')
    users = cursor.fetchall()
    conn.close()
    return users

def is_news_sent(url):
    conn = sqlite3.connect('bot_database.db')
    cursor = conn.cursor()
    cursor.execute('SELECT 1 FROM sent_news WHERE url = ?', (url,))
    result = cursor.fetchone()
    conn.close()
    return bool(result)

def mark_news_sent(url):
    conn = sqlite3.connect('bot_database.db')
    cursor = conn.cursor()
    cursor.execute('INSERT INTO sent_news (url) VALUES (?)', (url,))
    conn.commit()
    conn.close()

# --- Core Logic: Sorting and Fetching ---
def categorize_news(title, summary):
    text_to_check = f"{title} {summary}".lower()
    matched_categories = []
    
    for category, keywords in CATEGORIES.items():
        if any(keyword in text_to_check for keyword in keywords):
            matched_categories.append(category)
            
    return matched_categories

async def fetch_feed(session, url):
    try:
        async with session.get(url, timeout=10) as response:
            if response.status == 200:
                xml_data = await response.text()
                return feedparser.parse(xml_data)
    except Exception as e:
        logging.error(f"Error fetching {url}: {e}")
    return None

async def process_news():
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_feed(session, url) for url in RSS_FEEDS]
        feeds = await asyncio.gather(*tasks)

        users = get_users()
        if not users:
            return # Skip processing if no one is subscribed

        for feed in feeds:
            if not feed or 'entries' not in feed:
                continue
                
            # Check the top 5 most recent entries per feed
            for entry in feed.entries[:5]:
                link = entry.get('link', '')
                if is_news_sent(link):
                    continue

                title = entry.get('title', 'No Title')
                summary = entry.get('summary', '')
                
                # Determine relevance
                matched_categories = categorize_news(title, summary)
                
                # If it hits our highly filtered keywords, broadcast it
                if matched_categories:
                    mark_news_sent(link)
                    tags = " ".join([f"#{cat}" for cat in matched_categories])
                    
                    message_text = (
                        f"🚨 <b>BREAKING NEWS</b>\n\n"
                        f"<b>{title}</b>\n\n"
                        f"{tags}\n\n"
                        f"<a href='{link}'>Read Full Update</a>"
                    )

                    for chat_id, user_categories in users:
                        user_cats = user_categories.split(',')
                        # Check if user is subscribed to any of the matched categories
                        if any(cat in user_cats for cat in matched_categories):
                            try:
                                await bot.send_message(chat_id=chat_id, text=message_text)
                                await asyncio.sleep(0.1) # Prevent hitting Telegram rate limits
                            except Exception as e:
                                logging.error(f"Failed to send to {chat_id}: {e}")

# --- Background Scheduler ---
async def news_loop():
    while True:
        try:
            await process_news()
        except Exception as e:
            logging.error(f"Error in news loop: {e}")
        # Fetch updates every 60 seconds. Extremely fast without getting IP banned by sources.
        await asyncio.sleep(60)

# --- Telegram Bot Handlers ---
@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    add_user(message.chat.id)
    welcome_text = (
        "Greetings. I am your high-impact news monitor.\n\n"
        "I continuously scan for major geopolitical events and macroeconomic shifts affecting XAUUSD and major fiat currencies.\n\n"
        "You are now subscribed to all alerts. Use /help to see commands."
    )
    await message.answer(welcome_text)

@dp.message(Command("help"))
async def cmd_help(message: types.Message):
    help_text = (
        "<b>Available Commands:</b>\n"
        "/start - Subscribe to the feed\n"
        "/status - Check your current settings\n"
        "/ping - Test bot responsiveness"
    )
    await message.answer(help_text)

@dp.message(Command("status"))
async def cmd_status(message: types.Message):
    conn = sqlite3.connect('bot_database.db')
    cursor = conn.cursor()
    cursor.execute('SELECT categories FROM users WHERE chat_id = ?', (message.chat.id,))
    result = cursor.fetchone()
    conn.close()
    
    if result:
        await message.answer(f"Status: 🟢 Active\nListening for: {result[0]}")
    else:
        await message.answer("Status: 🔴 Inactive\nPress /start to activate.")

# --- Main Execution ---
async def main():
    init_db()
    logging.info("Database initialized. Starting background news loop...")
    
    # Start the background news fetching loop
    asyncio.create_task(news_loop())
    
    # Start polling for Telegram commands
    logging.info("Bot is polling for commands...")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
