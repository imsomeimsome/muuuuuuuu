import os
import redis
import asyncio
import logging
from dateutil.parser import isoparse

redis_client = None  # Initialize global Redis client
cache = None  # Initialize global cache object

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

def init_redis():
    """
    Initialize the Redis connection pool.
    """
    global redis_client, cache
    redis_url = os.getenv("REDIS_URL", "redis://localhost")  # Use Railway's REDIS_URL environment variable
    redis_client = redis.Redis.from_url(redis_url)
    cache = redis_client  # Assign Redis client to cache for compatibility

def close_redis():
    """
    Close the Redis connection pool.
    """
    global redis_client
    if redis_client:
        redis_client.close()

def get_cache(key):
    """Get a value from Redis."""
    return redis_client.get(key)

def set_cache(key, value, ttl=None):
    """Set a value in Redis with an optional TTL."""
    redis_client.set(key, value, ex=ttl)

def delete_cache(key):
    """Delete a value from Redis."""
    redis_client.delete(key)

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
    :param platform: Platform of the release (e.g., Spotify, SoundCloud).
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