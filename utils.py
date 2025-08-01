import os
import aioredis
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

async def init_redis():
    """
    Initialize the Redis connection pool.
    """
    global redis
    redis_url = os.getenv("REDIS_URL", "redis://localhost")  # Use Railway's REDIS_URL environment variable
    redis = aioredis.from_url(redis_url)

async def close_redis():
    """
    Close the Redis connection pool.
    """
    global redis
    if redis:
        await redis.close()

async def get_cache(key):
    """Get a value from Redis."""
    return await redis.get(key)

async def set_cache(key, value, ttl=None):
    """Set a value in Redis with an optional TTL."""
    await redis.set(key, value, ex=ttl)

async def delete_cache(key):
    """Delete a value from Redis."""
    await redis.delete(key)