import os
import sqlite3
from datetime import datetime, timedelta
import asyncio
import logging
from dateutil.parser import isoparse
import requests

DB_PATH = "/data/artists.db"

def run_blocking(func, *args, **kwargs):
    """
    Run a blocking function in an asynchronous context.
    :param func: The blocking function to run.
    :param args: Positional arguments for the function.
    :param kwargs: Keyword arguments for the function.
    :return: Result of the blocking function.
    """
    loop = asyncio.get_event_loop()
    return loop.run_in_executor(None, func, *args, **kwargs)


def get_cache(key):
    """Get a value from SQLite cache."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT value, expires_at FROM cache WHERE key = ?
    """, (key,))
    result = cursor.fetchone()
    conn.close()
    if result:
        value, expires_at = result
        if expires_at and datetime.fromisoformat(expires_at) < datetime.now():
            delete_cache(key)  # Expired, delete the key
            return None
        return value
    return None

def set_cache(key, value, ttl=None):
    """Set a value in SQLite cache with an optional TTL."""
    expires_at = (datetime.now() + timedelta(seconds=ttl)).isoformat() if ttl else None
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        REPLACE INTO cache (key, value, expires_at)
        VALUES (?, ?, ?)
    """, (key, value, expires_at))
    conn.commit()
    conn.close()

def delete_cache(key):
    """Delete a value from SQLite cache."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        DELETE FROM cache WHERE key = ?
    """, (key,))
    conn.commit()
    conn.close()

# Configure logging with color-coded levels
class CustomFormatter(logging.Formatter):
    COLORS = {
        "DEBUG": "\033[90m",  # Gray
        "INFO": "\033[94m",  # Blue
        "WARNING": "\033[93m",  # Orange
        "ERROR": "\033[91m",  # Red
        "CRITICAL": "\033[91m",  # Red
    }
    RESET = "\033[0m"

    def format(self, record):
        color = self.COLORS.get(record.levelname, self.RESET)
        record.msg = f"{color}{record.msg}{self.RESET}"
        return super().format(record)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logging.getLogger().handlers[0].setFormatter(CustomFormatter())

def log_release(artist_name, release_title, platform):
    """
    Log a release event.
    :param artist_name: Name of the artist.
    :param release_title: Title of the release.
    :param platform: Platform o f the release (e.g., Spotify, SoundCloud).
    """
    logging.info(f"🎵 New release by {artist_name}: '{release_title}' on {platform}")

def parse_datetime(date_str):
    """
    Parse an ISO 8601 date string into a timezone-aware datetime object.
    :param date_str: ISO 8601 date string.
    :return: A timezone-aware datetime object.
    """
    try:
        return isoparse(date_str)
    except Exception as e:
        logging.error(f"Failed to parse datetime: {e}")
        return None

def clear_all_cache():
    """Clear all entries in the SQLite cache."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM cache")  # Delete all rows in the cache table
    conn.commit()
    conn.close()
    logging.info("✅ Cleared all cache entries.")

def get_highest_quality_artwork(url: str) -> str:
    """Get highest quality version of artwork URL with fallbacks."""
    if not url:
        return None
        
    # For SoundCloud URLs
    if "sndcdn.com" in url:
        # Try upgrading in order: original > t500x500 > large > t300x300
        variants = [
            url.replace("-large.", "-original."),
            url.replace("-large.", "-t500x500."),
            url,  # Original URL as fallback
            url.replace("-t500x500.", "-t300x300.")
        ]
        
        # Remove duplicates while preserving order
        seen = set()
        variants = [x for x in variants if not (x in seen or seen.add(x))]
        
        # Verify URL exists before returning
        for variant in variants:
            try:
                response = requests.head(variant)
                if response.status_code == 200:
                    return variant
            except:
                continue
                
        return url  # Return original if no variants work
        
    # For Spotify URLs
    elif "i.scdn.co" in url:
        # Handle both old and new Spotify URL formats
        try:
            if '/image/' in url:
                base_url = url.split('/image/')[0] + '/image/'
                spotify_id = url.split('/')[-1]
            else:
                base_url = url.rsplit('/', 1)[0] + '/'
                spotify_id = url.split('/')[-1]
            
            sizes = ['1000x1000', '640x640', '300x300']
            for size in sizes:
                high_res = f"{base_url}{size}/{spotify_id}"
                try:
                    response = requests.head(high_res)
                    if response.status_code == 200:
                        return high_res
                except:
                    continue
        except:
            pass
            
    return url  # Return original if no upgrades possible