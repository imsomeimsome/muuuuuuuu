# filepath: /workspaces/muuuuuuuu/utils.py
import asyncio
import redis.asyncio as aioredis  # Updated import for redis.asyncio

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

# Create a Redis connection pool
redis = None

async def init_redis():
    """
    Initialize the Redis connection pool.
    """
    global redis
    redis = aioredis.from_url("redis://localhost")  # Updated method

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