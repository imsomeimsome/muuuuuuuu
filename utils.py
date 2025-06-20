
import asyncio
from functools import partial
import time
import requests
from datetime import datetime

async def run_blocking(func, *args, **kwargs):
    """Run blocking sync function in executor to avoid blocking bot."""
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, partial(func, *args, **kwargs))

def safe_get(url, headers=None, retries=3):
    for attempt in range(retries):
        response = requests.get(url, headers=headers)
        if response.status_code == 429:
            retry_after = int(response.headers.get("Retry-After", 5))
            print(f"Rate limited. Sleeping for {retry_after} seconds...")
            time.sleep(retry_after)
            continue
        response.raise_for_status()
        return response
    return None

class SimpleCache:
    def __init__(self):
        self.cache = {}

    def get(self, key):
        data = self.cache.get(key)
        if not data:
            return None
        timestamp, value, ttl = data
        if (time.time() - timestamp) > ttl:
            del self.cache[key]
            return None
        return value

    def set(self, key, value, ttl=300):
        self.cache[key] = (time.time(), value, ttl)

cache = SimpleCache()


# === Release logger ===

import discord
import os

LOG_CHANNEL_ID = int(os.getenv("LOG_CHANNEL_ID", 0))

async def log_release(bot, message):
    if LOG_CHANNEL_ID == 0:
        return  # logging disabled

    try:
        channel = bot.get_channel(LOG_CHANNEL_ID)
        if not channel:
            return

        await channel.send(message)
    except Exception as e:
        print(f"Failed to log release: {e}")

def parse_datetime(dt_str):
    if not dt_str:
        return None
    try:
        return datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
    except Exception:
        return None
