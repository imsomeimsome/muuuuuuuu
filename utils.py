import os
import redis
import asyncio

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
    global redis_client
    redis_url = os.getenv("REDIS_URL", "redis://localhost")  # Use Railway's REDIS_URL environment variable
    redis_client = redis.Redis.from_url(redis_url)

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