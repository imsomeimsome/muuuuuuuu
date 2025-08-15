# REMEMBER, TO ADD CHANNELS LINE 75 and 189 IN TABLES.PY

import os
import typing
from typing import Optional, Literal
import discord
import functools
import logging
import json
from dateutil.parser import isoparse
from discord.ext import tasks
import asyncio
from datetime import datetime, timezone, timedelta
from keep_alive import keep_alive
from functools import partial
from discord.ext import commands
from discord import app_commands
from dotenv import load_dotenv
from database_utils import (
    add_artist, remove_artist, artist_exists, get_artist_by_id, import_artists_from_json,
    update_last_release_date, add_release, get_release_stats, get_all_artists, is_already_posted_repost, mark_posted_repost,
    get_artists_by_owner, add_user, is_user_registered, get_username, is_already_posted_like, mark_posted_like, update_last_like_date,
    log_untrack, get_untrack_count, get_user_registered_at, get_global_artist_count, get_artist_full_record,
    set_channel, get_channel, set_release_prefs, get_release_prefs, get_connection, get_artist_by_identifier,
    update_last_repost_date, update_last_playlist_date, is_already_posted_playlist, mark_posted_playlist,
    record_bot_startup, record_bot_shutdown, get_downtime_duration, get_playlist_state, store_playlist_state
)
from embed_utils import create_music_embed, create_repost_embed, create_like_embed
from spotify_utils import (
    extract_spotify_id,
    get_artist_name as get_spotify_artist_name,
    get_artist_info as get_spotify_artist_info,
    get_last_release_date as get_spotify_last_release_date,
    get_release_info as get_spotify_release_info,
    get_latest_album_id as get_spotify_latest_album_id
)

from soundcloud_utils import (
    extract_soundcloud_id,
    get_artist_name_by_url as get_soundcloud_artist_name,
    get_last_release_date as get_soundcloud_last_release_date,
    get_soundcloud_release_info,
    get_soundcloud_artist_id,
    extract_soundcloud_username as extract_soundcloud_id,
    get_soundcloud_playlist_info,
    get_soundcloud_likes_info,
    get_soundcloud_reposts,
    get_soundcloud_likes,
    get_soundcloud_reposts_info,
    get_artist_info,
    init_key_manager,
    key_manager
)
from utils import run_blocking, log_release, parse_datetime, get_cache, set_cache, delete_cache, clear_all_cache
from reset_artists import reset_tables
from tables import initialize_fresh_database, initialize_cache_table
import sqlite3
import signal
import sys


# ===== Below are the 2 commands to delete all saved data, use top one for a full wipe
# initialize_fresh_database()
# reset_tables()

# Ensure the /data directory exists
os.makedirs('/data', exist_ok=True)

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

# Helper function to summarize errors
def summarize_errors(errors):
    if not errors:
        return "No errors encountered."
    summary = "\n".join([f"{error['type']}: {error['message']}" for error in errors])
    return f"Errors:\n{summary}"

# Update logging logic in the bot
async def log_summary(total_checked, new_releases, errors):
    logging.info("==================================================")
    logging.info(f"✅ Checked {total_checked} artists, found {new_releases} new releases")
    logging.info(summarize_errors(errors))
    logging.info("==================================================")


logger = logging.getLogger("release_checker")

def parse_date(date_str: str) -> datetime:
    """Handle multiple date formats consistently."""
    if not date_str:
        return datetime.min.replace(tzinfo=timezone.utc)
    
    try:
        # Try parsing with timezone info
        dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except:
        try:
            # Handle plain date format
            dt = datetime.strptime(date_str, '%Y-%m-%d')
            return dt.replace(tzinfo=timezone.utc)
        except:
            # Last resort - try parsing with dateutil
            dt = parse_datetime(date_str)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt

load_dotenv()
TOKEN = os.getenv("DISCORD_TOKEN")
TEST_GUILD_ID = os.getenv("TEST_GUILD_ID")
LOG_CHANNEL_ID = int(os.getenv("LOG_CHANNEL_ID", 0))

# CATCH-UP CONFIGURATION
CATCH_UP_THRESHOLD = timedelta(hours=24)  # Only catch up if downtime < 24 hours
MAX_CATCH_UP_ITEMS = 5  # Limit catch-up posts to prevent spam

class MusicBot(commands.Bot):
    def __init__(self):
        intents = discord.Intents.default()
        intents.message_content = True
        super().__init__(command_prefix="!", intents=intents)
        self.log_channel = None
        
    async def setup_hook(self):
        if LOG_CHANNEL_ID:
            self.log_channel = self.get_channel(LOG_CHANNEL_ID)
        if TEST_GUILD_ID:
            guild = discord.Object(id=int(TEST_GUILD_ID))
            await self.tree.sync(guild=guild)
        else:
            await self.tree.sync()

    async def log_event(self, content: str):
        if self.log_channel:
            await self.log_channel.send(
                f"`[{datetime.now(timezone.utc)}]` {content}"
            )

bot = MusicBot()
CLIENT_ID = init_key_manager(bot)

# --- Decorators ---
def require_registration(func):
    @functools.wraps(func)
    async def wrapper(interaction: discord.Interaction, *args, **kwargs):
        if not is_user_registered(interaction.user.id):
            await interaction.response.send_message(
                "🚫 Register first with `/register`", ephemeral=True)
            return
        return await func(interaction, *args, **kwargs)
    return wrapper

# --- CATCH-UP SYSTEM ---

async def handle_bot_startup_catchup():
    """Handle catch-up logic when bot starts."""
    last_shutdown = record_bot_startup()
    
    if not last_shutdown:
        logging.info("🚀 First startup - no catch-up needed")
        return False
    
    downtime = get_downtime_duration()
    if not downtime:
        logging.info("🚀 No downtime detected")
        return False
    
    logging.info(f"🚀 Bot was down for: {downtime}")
    
    # Only catch up if downtime was reasonable (not weeks/months)
    if downtime > CATCH_UP_THRESHOLD:
        logging.warning(f"⏭️ Downtime too long ({downtime}), skipping catch-up to prevent spam")
        return False
    
    logging.info(f"🔄 Starting catch-up for {downtime} of missed activity...")
    return True

async def reset_bot_state():
    """Reset bot state for a fresh start."""
    bot.catchup_done = False
    bot.release_checker_started = False
    logging.info("✅ Bot state reset.")
    
def should_catch_up_content(content_date, last_check_date, bot_shutdown_time):
    """Determine if content should be posted during catch-up."""
    if not content_date or not bot_shutdown_time:
        return False

    # Normalize all dates to be offset-aware
    content_date = parse_date(content_date)
    last_check_date = parse_date(last_check_date) if last_check_date else None
    bot_shutdown_time = parse_date(bot_shutdown_time)

    # Only catch up on content that happened while bot was down
    return content_date > bot_shutdown_time

def get_platform_emoji(platform):
    """Get emoji for platform."""
    return "🟢" if platform == "spotify" else "🟠"

def get_content_emoji(content_type):
    """Get emoji for content type."""
    emojis = {
        "release": "🎵",
        "album": "💿", 
        "single": "🎵",
        "ep": "🎶",
        "playlist": "📑",
        "like": "❤️",
        "repost": "🔄"
    }
    return emojis.get(content_type, "🎵")

async def get_release_channel(guild_id: str, platform: str) -> Optional[discord.TextChannel]:
    logging.info(f"🔎 Looking for release channel: Guild ID = {guild_id}, Platform = {platform}")

    channel_id = get_channel(str(guild_id), platform)

    if not channel_id:
        logging.warning(f"⚠️ No channel configured for {platform} in guild {guild_id}")
        return None

    channel = bot.get_channel(int(channel_id))
    if channel is None:
        logging.warning(f"⚠️ Channel ID {channel_id} for {platform} not found in bot cache")
        return None
    
    if not isinstance(channel, discord.TextChannel):
        logging.warning(f"⚠️ Channel ID {channel_id} exists but is not a text channel")
        return None

    logging.info(
        f"✅ Found release channel #{channel.name} ({channel.id}) for {platform} in guild {guild_id}"
    )
    return channel

async def handle_release(bot, artist, release_info, release_type):
    guild_id = artist.get('guild_id')
    platform = artist['platform']

    if not guild_id:
        logging.warning(f"❌ Missing guild_id for artist {artist['artist_name']} — cannot post {release_type}.")
        return

    channel = await get_release_channel(guild_id=guild_id, platform=platform)
    if not channel:
        logging.warning(f"⚠️ No channel configured for {platform} in guild {guild_id} — skipping post for {artist['artist_name']}.")
        return

    embed = create_music_embed(
        platform=platform,
        artist_name=release_info.get('artist_name', artist['artist_name']),
        title=release_info.get('title', 'New Release'),
        url=release_info.get('url', artist['artist_url']),
        release_date=release_info.get('release_date')[:10] if release_info.get('release_date') else "Unknown",
        cover_url=release_info.get('cover_url'),
        features=release_info.get('features'),
        track_count=release_info.get('track_count'),
        duration=release_info.get('duration'),
        repost=release_info.get('repost', False),
        genres=release_info.get('genres')
    )

    await channel.send(embed=embed)
    logging.info(f"✅ Posted new {release_type} for {artist['artist_name']}")
# --- Playlist changes here ---

async def check_for_playlist_changes(bot, artist, playlist_info):
    artist_id = artist["artist_id"]
    guild_id = artist["guild_id"]
    playlist_id = playlist_info["url"]

    # Get stored playlist state
    stored_tracks = get_playlist_state(artist_id, guild_id, playlist_id)
    current_tracks = playlist_info["tracks"]

    if not stored_tracks:
        # First time tracking this playlist
        store_playlist_state(artist_id, guild_id, playlist_id, current_tracks)
        logging.info(f"✅ Stored initial state for playlist: {playlist_info['title']}")
        return

    # Detect changes
    added_tracks = [track for track in current_tracks if track not in stored_tracks]
    removed_tracks = [track for track in stored_tracks if track not in current_tracks]
    order_changed = any(
        track["order"] != stored_tracks[index]["order"]
        for index, track in enumerate(current_tracks)
        if index < len(stored_tracks)
    )

    if added_tracks or removed_tracks or order_changed:
        logging.info(f"✨ Playlist changes detected for {playlist_info['title']}")
        embed = discord.Embed(
            title=f"Playlist Updated: {playlist_info['title']}",
            url=playlist_info["url"],
            description="Changes detected in playlist:",
            color=discord.Color.orange()
        )
        if added_tracks:
            embed.add_field(name="Added Tracks", value="\n".join([track["title"] for track in added_tracks]), inline=False)
        if removed_tracks:
            embed.add_field(name="Removed Tracks", value="\n".join([track["title"] for track in removed_tracks]), inline=False)
        if order_changed:
            embed.add_field(name="Order Changed", value="Track order has been updated.", inline=False)

        channel = await get_release_channel(guild_id, "soundcloud")
        if channel:
            await channel.send(embed=embed)

        # Update stored state
        store_playlist_state(artist_id, guild_id, playlist_id, current_tracks)

# --- MAIN RELEASE CHECK FUNCTION WITH CATCH-UP ---
########## NEW CHECK FOR NEW RELEASES IDK IF IT WORKS #########################################

async def check_for_new_releases(bot, is_catchup=False):
    """Coordinate independent platform checks."""
    
    # Get general setup data
    artists, shutdown_time, general_errors = await check_general_tasks(bot, is_catchup)
    if not artists:
        return

    # Run platform checks independently
    spotify_task = asyncio.create_task(check_spotify_updates(bot, artists, shutdown_time, is_catchup))
    soundcloud_task = asyncio.create_task(check_soundcloud_updates(bot, artists, shutdown_time, is_catchup))

    # Process results
    try:
        spotify_results = await spotify_task
        spotify_releases, spotify_errors = spotify_results
    except Exception as e:
        logging.error(f"Spotify checks failed: {e}")
        spotify_releases, spotify_errors = 0, [{"type": "Spotify", "message": str(e)}]

    try:
        soundcloud_results = await soundcloud_task
        soundcloud_counts, soundcloud_errors = soundcloud_results
    except Exception as e:
        logging.error(f"SoundCloud checks failed: {e}")
        soundcloud_counts = {"releases": 0, "playlists": 0, "reposts": 0, "likes": 0}
        soundcloud_errors = [{"type": "SoundCloud", "message": str(e)}]

    # Compile results
    total_releases = spotify_releases + sum(soundcloud_counts.values())
    all_errors = (general_errors or []) + spotify_errors + soundcloud_errors

    # Log final summary
    await log_summary(len(artists), total_releases, all_errors)

async def check_general_tasks(bot, is_catchup=False):
    """Handle general setup and get common data needed for all checks."""
    errors = []
    
    logging.info(f"\n🔍 Starting {'catch-up ' if is_catchup else ''}check cycle...")
    
    try:
        artists = get_all_artists()
        if not artists:
            logging.warning("⚠️ No artists found to check")
            return None, None, None
    except Exception as e:
        logging.error(f"❌ Failed to fetch artists from database: {e}")
        return None, None, None

    # Get shutdown time for catch-up logic
    shutdown_time = None
    if is_catchup:
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT timestamp FROM activity_logs 
                WHERE user_id = 'system' AND action = 'bot_shutdown'
                ORDER BY timestamp DESC LIMIT 1
            """)
            result = cursor.fetchone()
            shutdown_time = result[0] if result else None
            if shutdown_time:
                logging.info(f"📅 Last shutdown: {shutdown_time}")
            else:
                logging.info("📅 No previous shutdown time found")

    return artists, shutdown_time, errors

async def check_spotify_updates(bot, artists, shutdown_time=None, is_catchup=False):
    """Handle all Spotify-related checks."""
    errors = []
    spotify_releases = 0
    
    logging.info(f"\n🟢 CHECKING SPOTIFY{'(CATCH-UP)' if is_catchup else ''}...")
    logging.info("=" * 50)

    for artist in artists:
        if artist.get("platform") != "spotify":
            continue

        try:
            artist_name = artist.get("artist_name", "unknown")
            artist_id = artist.get("artist_id")
            artist_url = artist.get("artist_url")
            last_date = artist.get("last_release_date")

            logging.info(f"🟢 Checking {artist_name}")
            
            try:
                latest_album_id = await run_blocking(get_spotify_latest_album_id, artist_id)
                if not latest_album_id:
                    logging.info(f"     ⚠️ No releases found for {artist_name}")
                    continue

                release_info = await run_blocking(get_spotify_release_info, latest_album_id)
                if not release_info:
                    logging.info(f"     ⚠️ No release info found for {artist_name}")
                    continue

                current_date = release_info.get("release_date")
                if not current_date:
                    logging.info(f"     ⚠️ No release date found for {artist_name}")
                    continue

                # Handle release posting logic
                should_post = False
                if not is_catchup:
                    if not last_date or parse_date(current_date) > parse_date(last_date):
                        should_post = True
                        logging.info(f"     ✨ NEW RELEASE DETECTED: {release_info['title']}")
                elif should_catch_up_content(current_date, last_date, shutdown_time):
                    should_post = True
                    logging.info(f"     ✨ [CATCH-UP] NEW RELEASE DETECTED: {release_info['title']}")

                if should_post:
                    logging.info(f"     📝 Posting release: {release_info['title']}")
                    embed = create_music_embed(
                        platform="spotify",
                        artist_name=artist_name,
                        title=release_info["title"],
                        url=release_info["url"],
                        release_date=release_info["release_date"],
                        cover_url=release_info["cover_url"],
                        features=release_info["features"],
                        track_count=release_info["track_count"],
                        duration=release_info["duration"],
                        genres=release_info["genres"]
                    )

                    channel = await get_release_channel(guild_id=artist["guild_id"], platform="spotify")
                    if channel:
                        await channel.send(embed=embed)
                        update_last_release_date(artist_id, artist["owner_id"], artist["guild_id"], current_date)
                        spotify_releases += 1
                        if is_catchup:
                            await asyncio.sleep(2)  # Rate limit catch-up posts
                    else:
                        logging.warning(f"     ⚠️ No channel configured for {artist['platform']}")
                else:
                    logging.info(f"     ⏳ No new releases found for {artist_name}")

            except Exception as e:
                logging.error(f"     ❌ Error processing release: {e}")
                continue

        except Exception as e:
            errors.append({"type": "Spotify Check", "message": str(e)})
            logging.error(f"❌ Error checking {artist_name}: {e}")

    logging.info(f"✅ Found {spotify_releases} new Spotify releases")
    logging.info("=" * 50)
    return spotify_releases, errors

async def check_soundcloud_updates(bot, artists, shutdown_time=None, is_catchup=False):
    """Handle all SoundCloud-related checks."""
    errors = []
    soundcloud_counts = {
        "releases": 0,
        "playlists": 0,
        "reposts": 0,
        "likes": 0
    }
    
    if not CLIENT_ID:
        logging.error("❌ No valid SoundCloud API key available")
        errors.append({"type": "SoundCloud", "message": "No valid API key"})
        return soundcloud_counts, errors
    
    now = datetime.now(timezone.utc)
    retry_after = None

    # === RELEASES AND PLAYLISTS ===
    logging.info(f"\n🟠 CHECKING SOUNDCLOUD{'(CATCH-UP)' if is_catchup else ''}...")
    logging.info("=" * 50)

    for artist in artists:
        if artist.get("platform") != "soundcloud":
            continue

        if retry_after and now < retry_after:
            logging.warning(f"⏭️ Skipping remaining SoundCloud checks until {retry_after}")
            break

        try:
            artist_name = artist.get("artist_name", "unknown")
            artist_id = artist.get("artist_id")
            artist_url = artist.get("artist_url")
            last_date = artist.get("last_release_date")

            logging.info(f"🟠 Checking {artist_name}")

            # Check releases
            try:
                release_info = await run_blocking(get_soundcloud_release_info, artist_url)
                if release_info:
                    current_date = release_info.get("release_date")
                    if current_date:
                        should_post = False
                        if not is_catchup and parse_date(current_date) > parse_date(last_date):
                            should_post = True
                            logging.info(f"     ✨ NEW RELEASE DETECTED!")
                        elif is_catchup and should_catch_up_content(current_date, last_date, shutdown_time):
                            should_post = True
                            logging.info(f"     ✨ [CATCH-UP] NEW RELEASE DETECTED!")

                        if should_post:
                            logging.info(f"     📝 Posting release: {release_info.get('title')}")
                            embed = create_music_embed(
                                platform="soundcloud",
                                artist_name=artist_name,
                                title=release_info["title"],
                                url=release_info["url"],
                                release_date=release_info["release_date"],
                                cover_url=release_info["cover_url"],
                                features=release_info["features"],
                                track_count=release_info["track_count"],
                                duration=release_info["duration"],
                                genres=release_info["genres"]
                            )

                            channel = await get_release_channel(guild_id=artist["guild_id"], platform="soundcloud")
                            if channel:
                                await channel.send(embed=embed)
                                update_last_release_date(artist_id, artist["owner_id"], artist["guild_id"], current_date)
                                soundcloud_counts["releases"] += 1
                                if is_catchup:
                                    await asyncio.sleep(2)
                            else:
                                logging.warning(f"     ⚠️ No channel configured for {artist['platform']}")

            except Exception as e:
                if "rate/request limit" in str(e).lower():
                    retry_after = now + timedelta(hours=12)
                    logging.warning(f"⚠️ Rate limit hit. Attempting key rotation...")
                    try:
                        new_key = key_manager.rotate_key()
                        if new_key:
                            CLIENT_ID = new_key
                            logging.info("🔄 Successfully rotated to new API key")
                            continue
                    except ValueError:
                        logging.error("❌ No more API keys available")
                        break
                raise

            # Check playlists if not rate limited
            if not retry_after:
                try:
                    playlist_info = await run_blocking(get_soundcloud_playlist_info, artist_url)
                    if playlist_info:
                        playlist_id = playlist_info["url"]
                        if not is_already_posted_playlist(artist_id, artist["guild_id"], playlist_id):
                            await handle_release(bot, artist, playlist_info, "playlist")
                            mark_posted_playlist(artist_id, artist["guild_id"], playlist_id)
                            update_last_playlist_date(artist_id, artist["guild_id"], playlist_info["release_date"])
                            await check_for_playlist_changes(bot, artist, playlist_info)
                            soundcloud_counts["playlists"] += 1
                except Exception as e:
                    logging.error(f"Error checking playlists: {e}")

            # Check reposts if not rate limited
            if not retry_after:
                try:
                    reposts = await run_blocking(get_soundcloud_reposts_info, artist_url)
                    if reposts:
                        for repost in reposts[:MAX_CATCH_UP_ITEMS if is_catchup else None]:
                            repost_id = str(repost.get("track_id"))
                            if not repost_id or is_already_posted_repost(artist_id, artist["guild_id"], repost_id):
                                continue

                            logging.info(f"     📢 New repost found: {repost.get('title')}")
                            embed = create_repost_embed(
                                platform="soundcloud",
                                reposted_by=artist_name,
                                title=repost["title"],
                                artist_name=repost["artist_name"],
                                url=repost["url"],
                                release_date=repost["release_date"],
                                reposted_date=repost["reposted_date"],
                                cover_url=repost["cover_url"],
                                features=repost["features"],
                                track_count=repost["track_count"],
                                duration=repost["duration"],
                                genres=repost["genres"]
                            )

                            channel = await get_release_channel(guild_id=artist["guild_id"], platform="soundcloud")
                            if channel:
                                await channel.send(embed=embed)
                                mark_posted_repost(artist_id, artist["guild_id"], repost_id)
                                update_last_repost_date(artist_id, artist["guild_id"], repost["release_date"])
                                soundcloud_counts["reposts"] += 1
                                if is_catchup:
                                    await asyncio.sleep(2)
                            else:
                                logging.warning(f"     ⚠️ No channel configured for {artist['platform']}")
                except Exception as e:
                    logging.error(f"Error checking reposts: {e}")

            # Check likes if not rate limited
            if not retry_after:
                try:
                    likes = await run_blocking(get_soundcloud_likes_info, artist_url)
                    if likes:
                        for like in likes[:MAX_CATCH_UP_ITEMS if is_catchup else None]:
                            like_id = str(like.get("track_id"))
                            if not like_id or is_already_posted_like(artist_id, artist["guild_id"], like_id):
                                continue

                            logging.info(f"     ❤️ New like found: {like.get('title')}")
                            embed = create_like_embed(
                                platform="soundcloud",
                                liked_by=artist_name,
                                title=like["title"],
                                artist_name=like["artist_name"],
                                url=like["url"],
                                release_date=like["release_date"],
                                liked_date=like["liked_date"],
                                cover_url=like["cover_url"],
                                features=like["features"],
                                track_count=like["track_count"],
                                duration=like["duration"],
                                genres=like["genres"],
                                content_type="like"
                            )

                            channel = await get_release_channel(guild_id=artist["guild_id"], platform="soundcloud")
                            if channel:
                                await channel.send(embed=embed)
                                mark_posted_like(artist_id, artist["guild_id"], like_id)
                                update_last_like_date(artist_id, artist["guild_id"], like["liked_date"])
                                soundcloud_counts["likes"] += 1
                                if is_catchup:
                                    await asyncio.sleep(2)
                            else:
                                logging.warning(f"     ⚠️ No channel configured for {artist['platform']}")
                except Exception as e:
                    logging.error(f"Error checking likes: {e}")

        except Exception as e:
            errors.append({"type": "SoundCloud Check", "message": str(e)})
            logging.error(f"❌ Error checking {artist_name}: {e}")

    logging.info(f"✅ SoundCloud Summary:")
    logging.info(f"   Releases: {soundcloud_counts['releases']}")
    logging.info(f"   Playlists: {soundcloud_counts['playlists']}")
    logging.info(f"   Reposts: {soundcloud_counts['reposts']}")
    logging.info(f"   Likes: {soundcloud_counts['likes']}")
    logging.info("=" * 50)
    
    return soundcloud_counts, errors
########## BACK TO NORMAL #####################################################################
# --- SCHEDULER ---
async def release_check_scheduler(bot):
    await bot.wait_until_ready()
    logging.info("🚀 Release checker started")
    logging.info("⏳ Release checker initializing...")

    # Run catch-up check immediately after bot starts
    if not hasattr(bot, 'catchup_done') or not bot.catchup_done:
        try:
            logging.info("🔄 Running catch-up check...")
            is_catchup = await handle_bot_startup_catchup()
            if is_catchup:
                await check_for_new_releases(bot, is_catchup=True)
                logging.info("✅ Catch-up check complete")
            else:
                logging.info("⏭️ No catch-up needed")
        except Exception as e:
            logging.error(f"❌ Error during catch-up check: {e}")

        # Mark catch-up as done
        bot.catchup_done = True

    # Schedule normal checks at fixed intervals
    while not bot.is_closed():
        now = datetime.now(timezone.utc)
        # Calculate the next run time (round to the nearest 5-minute mark)
        next_run = now.replace(second=1, microsecond=0) + timedelta(minutes=5 - (now.minute % 5))

        delay = (next_run - now).total_seconds()
        logging.info(f"🕰️ Next release check at {next_run.strftime('%H:%M:%S')} UTC (in {delay:.1f}s)")
        await asyncio.sleep(delay)

        try:
            check_time = datetime.now(timezone.utc).strftime('%H:%M:%S')
            logging.info(f"🔍 Starting release check at {check_time} UTC...")
            await check_for_new_releases(bot, is_catchup=False)
            logging.info("✅ Release check complete")
        except Exception as e:
            logging.error(f"❌ Error during release check: {e}")

# --- EVENT HANDLERS ---

@bot.event
async def on_ready():
    await bot.wait_until_ready()
    logging.info(f"✅ Logged in as {bot.user} (ID: {bot.user.id})")

    # ✅ Sync slash commands
    try:
        synced = await bot.tree.sync()
        logging.info(f"🌐 Synced {len(synced)} slash commands.")
    except Exception as e:
        logging.error(f"❌ Failed to sync slash commands: {e}")

    # ✅ Handle startup catch-up
    if not hasattr(bot, 'catchup_done') or not bot.catchup_done:
        should_catchup = await handle_bot_startup_catchup()

        # ✅ Run catch-up check if needed
        if should_catchup:
            try:
                await check_for_new_releases(bot, is_catchup=True)
            except Exception as e:
                logging.error(f"❌ Catch-up failed: {e}")

        # Mark catch-up as done
        bot.catchup_done = True

    # ✅ Start regular scheduler
    if not hasattr(bot, 'release_checker_started'):
        bot.release_checker_started = True
        asyncio.create_task(release_check_scheduler(bot))
        logging.info("🚀 Started release checker")

# Handle graceful shutdown
def signal_handler(sig, frame):
    """Handle shutdown gracefully."""
    logging.info("🛑 Bot shutting down...")
    record_bot_shutdown()
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

async def run_blocking(func, *args, **kwargs):
    """Run blocking (sync) function safely without blocking bot."""
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, partial(func, *args, **kwargs))
# --- Commands --- 
@bot.tree.command(name="setchannel")
@app_commands.checks.has_permissions(administrator=True)
async def setchannel_command(interaction: discord.Interaction, 
                            type: Literal["spotify", "soundcloud", "logs", "commands"],
                            channel: discord.TextChannel):
    set_channel(str(interaction.guild.id), type, str(channel.id))
    await bot.log_event(f"Channel set: {type} => #{channel.name}")
    await interaction.response.send_message(
        f"✅ {type.capitalize()} messages to {channel.mention}", ephemeral=True)

@bot.tree.command(name="trackchange")
@require_registration
async def trackchange_command(interaction: discord.Interaction,
                            artist_identifier: str,
                            release_type: Literal["album", "single", "ep", "repost"],
                            state: Literal["on", "off"]):
    user_id = interaction.user.id
    artist = get_artist_by_identifier(artist_identifier, user_id)

    if not artist:
        await interaction.response.send_message("❌ Artist not found", ephemeral=True)
        return

    set_release_prefs(user_id, artist['artist_id'], release_type, state)
    await interaction.response.send_message(
        f"✅ {artist['artist_name']} will {'now' if state == 'on' else 'no longer'} track {release_type}s",
        ephemeral=True)

# ... [Keep all your original commands exactly as they were, 
#      only modifying where necessary for new features]

@bot.tree.command(name="testrelease", description="Test a release embed using an artist ID or link")
@app_commands.describe(artist_input="Artist ID or Spotify/SoundCloud link")
async def testrelease_command(interaction: discord.Interaction, artist_input: str):
            await interaction.response.defer(ephemeral=True)

            user_id = interaction.user.id

            # Try to extract ID from URL
            artist_id = None
            if "spotify.com/artist" in artist_input:
                artist_id = extract_spotify_id(artist_input)
            elif "soundcloud.com" in artist_input:
                artist_id = extract_soundcloud_id(artist_input)
            else:
                # Assume it's already an ID
                artist_id = artist_input.strip()

            if not artist_id:
                await interaction.followup.send("❌ Invalid link or ID format")
                return

            # Check tracking using the extracted ID
            artist = get_artist_full_record(artist_id, user_id)
            if not artist:
                await interaction.followup.send("❌ You're not tracking this artist")
                return

            # Rest of your existing logic
            try:
                guild_id = artist.get('guild_id') or str(interaction.guild.id if interaction.guild else artist['owner_id'])
                channel = await get_release_channel(guild_id, artist['platform'])

                if not channel:
                    channel = interaction.channel

                embed = create_music_embed(
                    platform=artist['platform'],
                    artist_name=artist['artist_name'],
                    title="Test Release",
                    url=artist['artist_url'],
                    release_date=datetime.now().strftime("%Y-%m-%d"),
                    cover_url="https://i.imgur.com/test.jpg",
                    features="Test Feature",
                    track_count=1,
                    duration="3:00",
                    repost=False,
                    genres=artist['genres'].split(",") if artist['genres'] else []
                )

                if channel.type == discord.ChannelType.news:
                    message = await channel.send(embed=embed)
                    await message.publish()
                else:
                    await channel.send(embed=embed)

                await interaction.followup.send("✅ Test release published!")
            except discord.Forbidden:
                await interaction.followup.send("❌ Missing 'Manage Webhooks' permission")


# ... [Previous commands and event handlers]

@bot.tree.command(name="register", description="Register yourself to use the bot and track your own artists.")
async def register_command(interaction: discord.Interaction):
    user_id = interaction.user.id
    username = interaction.user.name
    if is_user_registered(user_id):
        await interaction.response.send_message(f"✅ You're already registered as **{username}**!")
        return
    if add_user(user_id, username):
        await interaction.response.send_message(f"🎉 Registered successfully as **{username}**!")
    else:
        await interaction.response.send_message("❌ Registration failed. Try again.")

@bot.tree.command(name="help", description="Show all available commands.")
@require_registration
async def help_command(interaction: discord.Interaction):
    help_text = (
        "**📜 Available Commands:**\n"
        "🟢 `/track` — Start tracking an artist by link\n"
        "🔴 `/untrack` — Stop tracking an artist\n"
        "📋 `/list` — Show all tracked artists\n"
        "📦 `/export` — Export your tracked artists list\n"
        "🧪 `/testembed` — Preview a release embed using a link\n"
        "🧪 `/testrelease` — Preview a release using tracked artist ID\n"
        "🛰 `/setchannel` — Set notification channels for releases/logs\n"
        "🔁 `/trackchange` — Toggle tracking of specific release types\n"
        "📡 `/channels` — View which channels are configured\n"
        "🔍 `/debugsoundcloud` — Manually fetch SoundCloud release info\n"
        "📊 `/info` — Show general bot usage stats\n"
        "🎨 `/key` — Emoji and color key for releases\n"
        "👤 `/userinfo` — Show your bot stats\n"
        "👤 `/userinfo other` — Admins: Check someone else's stats\n"
        "🌐 `/ping` — Check if the bot is responsive\n"
        "🧾 `/register` — Register yourself to start tracking"
    )
    await interaction.response.send_message(help_text, ephemeral=True)


@bot.tree.command(name="ping", description="Pong!")
@require_registration
async def ping_command(interaction: discord.Interaction):
    await interaction.response.send_message("🏓 Pong!")

@bot.tree.command(name="track", description="Track a new artist from Spotify or SoundCloud")
@require_registration
@app_commands.describe(link="A Spotify or SoundCloud artist URL")
async def track_command(interaction: discord.Interaction, link: str):
    await interaction.response.defer(ephemeral=True)
    user_id = interaction.user.id
    guild_id = str(interaction.guild.id) if interaction.guild else None

    print(f"📥 /track called by {interaction.user.name} in guild: {guild_id}")

    # Detect platform
    if "spotify.com" in link:
        platform = "spotify"
        artist_id = extract_spotify_id(link)
        artist_name = await run_blocking(get_spotify_artist_name, artist_id)
        artist_url = f"https://open.spotify.com/artist/{artist_id}"
        artist_info = await run_blocking(get_spotify_artist_info, artist_id)
        genres = artist_info.get("genres", []) if artist_info else []


    elif "soundcloud.com" in link:
        platform = "soundcloud"
        artist_id = extract_soundcloud_id(link)
        artist_info = await run_blocking(get_artist_info, link)
        artist_name = artist_info.get("name", artist_id)  # Fallback to artist_id if name is unavailable
        artist_url = artist_info.get("url", f"https://soundcloud.com/{artist_id}")
        genres = []  # Optional

    else:
        await interaction.followup.send("❌ Link must be a valid Spotify or SoundCloud artist URL.")
        return

    # Already tracked?
    if artist_exists(platform, artist_id, user_id):
        await interaction.followup.send("⚠️ You're already tracking this artist.")
        return

    # ✅ Set last_release_date as time of tracking to prevent false first posts
    from datetime import datetime, timezone
    current_time = datetime.now(timezone.utc).isoformat()

    # Add artist
    add_artist(
        platform=platform,
        artist_id=artist_id,
        artist_name=artist_name,
        artist_url=artist_url,
        owner_id=user_id,
        guild_id=guild_id,
        genres=genres,
        last_release_date=current_time  # ✅ store track time
    )

    print(f"✅ Added artist '{artist_name}' ({platform}) with guild_id: {guild_id}")

    await interaction.followup.send(f"✅ Now tracking **{artist_name}** on {platform.capitalize()}.")

@bot.tree.command(name="untrack", description="Stop tracking an artist.")
@app_commands.describe(artist_identifier="Spotify/SoundCloud artist link or artist ID")
@require_registration
async def untrack_command(interaction: discord.Interaction, artist_identifier: str):
    user_id = interaction.user.id
    await interaction.response.defer()
    try:
        if "spotify.com/artist" in artist_identifier:
            artist_id = extract_spotify_id(artist_identifier)
        elif "soundcloud.com" in artist_identifier:
            artist_id = extract_soundcloud_id(artist_identifier)
        else:
            artist_id = artist_identifier.strip()
        guild_id = str(interaction.guild.id)
        artist = get_artist_by_id(artist_id, user_id, guild_id)
        if not artist:
            await interaction.followup.send(f"❌ No artist found.")
            return
        remove_artist(artist_id, user_id)
        log_untrack(user_id, artist_id)
        await bot.log_event(f"➖ {interaction.user.name} stopped tracking **{artist['artist_name']}**.")
        await interaction.followup.send(f"✅ Untracked **{artist['artist_name']}**.")
    except Exception as e:
        await bot.log_event(f"❌ Error: {str(e)}")
        await interaction.followup.send(f"❌ Error: `{str(e)}`")

@bot.tree.command(name="list", description="List your tracked artists.")
@require_registration
async def list_command(interaction: discord.Interaction):
        user_id = interaction.user.id
        artists = get_artists_by_owner(user_id)
        if not artists:
            await interaction.response.send_message("No artists tracked.")
            return

        # Group artists by name (case-insensitive)
        from collections import defaultdict

        grouped = defaultdict(list)
        for artist in artists:
            grouped[artist['artist_name'].lower()].append(artist)

        merged_artists = []
        for name_lower, group in grouped.items():
            # Prefer Spotify casing for display name
            spotify_names = [a['artist_name'] for a in group if a['platform'] == 'spotify']
            if spotify_names:
                display_name = spotify_names[0]
            else:
                # fallback: use first artist's name with title case
                display_name = group[0]['artist_name'].title()

            # Emoji order: Spotify first, then SoundCloud
            platforms = set(a['platform'] for a in group)
            emojis = []
            if 'spotify' in platforms:
                emojis.append('🟢')
            if 'soundcloud' in platforms:
                emojis.append('🟠')

            merged_artists.append({'name': display_name, 'emojis': emojis})

        # Sort alphabetically by display name (case-insensitive)
        merged_artists.sort(key=lambda x: x['name'].lower())

        # Build the message
        message_lines = [f"{' '.join(artist['emojis'])} {artist['name']}" for artist in merged_artists]
        message = "**🎵 Your Artists:**\n" + "\n".join(message_lines)

        await interaction.response.send_message(message)

@bot.tree.command(name="userinfo", description="Show your or another user's stats.")
@app_commands.describe(user="Optional: another user")
@require_registration
async def userinfo_command(interaction: discord.Interaction, user: typing.Optional[discord.User] = None):
    await interaction.response.defer()
    target = user or interaction.user
    requester = interaction.user
    if user and user != requester and not requester.guild_permissions.administrator:
        await interaction.followup.send("❌ Admins only.")
        return
    if not is_user_registered(target.id):
        await interaction.followup.send(f"❌ {target.mention} isn't registered.")
        return
    username = get_username(target.id)
    tracked = len(get_artists_by_owner(target.id))
    untracked = get_untrack_count(target.id)
    registered_at = get_user_registered_at(target.id) or "Unknown"
    embed = discord.Embed(title=f"📊 {username}'s Stats", color=discord.Color.blurple())
    embed.add_field(name="User", value=f"{target.mention}", inline=True)
    embed.add_field(name="Registered", value=registered_at, inline=True)
    embed.add_field(name="Tracked Artists", value=tracked, inline=True)
    embed.add_field(name="Untracked Artists", value=untracked, inline=True)
    if user is None and requester.guild_permissions.administrator:
        total_artists = get_global_artist_count()
        embed.add_field(name="🌐 Server Total Artists", value=total_artists, inline=False)
    await interaction.followup.send(embed=embed)

@bot.tree.command(name="info", description="Show bot info and stats.")
@require_registration
async def info_command(interaction: discord.Interaction):
    total_artists = get_global_artist_count()
    stats = get_release_stats()
    message = (
        f"**ℹ️ Bot Info**\n"
        f"Artists Tracked: **{total_artists}**\n"
        f"Releases: **{stats['total']}**\n"
        f"💿 Albums: {stats['albums']}\n"
        f"🎶 EPs: {stats['eps']}\n"
        f"🎵 Singles: {stats['singles']}\n"
        f"📀 Deluxes: {stats['deluxes']}\n"
    )
    await interaction.response.send_message(message)

@bot.tree.command(name="key", description="Show release tracking key for what the bot posts.")
async def key_command(interaction: discord.Interaction):
    embed = discord.Embed(
        title="📚 Release Key",
        description="Here's what each release type and field means:",
        color=0x7289DA
    )

    embed.add_field(name="💿 Album", value="7 or more tracks released together or marked as album/mixtape.", inline=False)
    embed.add_field(name="🎶 EP", value="2 to 6 tracks released together or marked as EP.", inline=False)
    embed.add_field(name="🎵 Single", value="Only 1 track released.", inline=False)
    embed.add_field(name="📑 Playlist", value="Newly posted playlist by artist.", inline=False)
    embed.add_field(name="❤️ Like", value="Track liked by the artist.", inline=False)
    embed.add_field(name="📢 Repost", value="Release reposted by the artist (not uploaded by them).", inline=False)
    embed.add_field(name="Features", value="Artists featured in the release, if detected.", inline=False)
    embed.add_field(name="Genres", value="Genres of the release if available.", inline=False)
    embed.add_field(name="Tracks", value="Total tracks in release/playlist.", inline=False)
    embed.add_field(name="Released on", value="Release date from SoundCloud or Spotify.", inline=False)

    await interaction.response.send_message(embed=embed)

@bot.tree.command(name="testembed", description="Preview a music release embed from a Spotify or SoundCloud link.")
@app_commands.describe(link="Spotify or SoundCloud release link")
@require_registration
async def testembed_command(interaction: discord.Interaction, link: str):
    await interaction.response.defer()
    try:
        if "spotify.com" in link:
            release_id = extract_spotify_id(link)
            if not release_id:
                raise ValueError("Invalid Spotify URL.")
            release_info = await run_blocking(get_spotify_release_info, release_id)
            color = 0x1DB954
        elif "soundcloud.com" in link:
            release_info = await run_blocking(get_soundcloud_release_info, link)
            color = 0xFF5500
        else:
            raise ValueError("Unsupported link. Only Spotify and SoundCloud are supported.")
        track_count = release_info['track_count']
        title = release_info['title'].lower()
        if track_count == 1:
            emoji = "🎵"
        elif track_count <= 6:
            emoji = "🎶"
        elif track_count >= 20:
            emoji = "📀" if "deluxe" in title else "💿"
        else:
            emoji = "💿"
        repost_emoji = "📌" if release_info.get('repost') else ""
        genre_text = f"\n**Genres:** {release_info.get('genres')}" if release_info.get('genres') else ""
        embed = discord.Embed(
            title=release_info['title'],
            url=release_info.get('url', link),
            color=color,
            description=genre_text
        )
        embed.set_author(name=f"{emoji} New {release_info['artist_name']} Release! {repost_emoji}")
        embed.set_thumbnail(url=release_info['cover_url'])
        embed.add_field(name="Duration", value=release_info['duration'], inline=True)
        embed.add_field(name="Tracks", value=track_count, inline=True)
        embed.add_field(name="Features", value=release_info['features'], inline=True)
        embed.add_field(name="Release Date", value=release_info['release_date'], inline=True)
        await interaction.followup.send(embed=embed)
    except Exception as e:
        await bot.log_event(f"❌ Error in /testembed: {str(e)}")
        await interaction.followup.send(f"❌ Failed to create test embed.\n{e}")

@bot.tree.command(name="export", description="Export your list of tracked artists.")
@require_registration
async def export_command(interaction: discord.Interaction):
    user_id = str(interaction.user.id)
    artists = get_artists_by_owner(user_id)
    if not artists:
        await interaction.response.send_message("📭 You aren't currently tracking any artists.")
        return
    # Build CSV lines
    lines = ["Platform,Artist Name,Artist ID,Artist URL,Last Release"]
    for artist in artists:
        lines.append(f"{artist['platform']},{artist['artist_name']},{artist['artist_id']},{artist['artist_url']},{artist['last_release_date']}")
    content = "\n".join(lines)
    # Save to file
    filename = f"tracked_artists_{user_id}.csv"
    with open(filename, "w", encoding="utf-8") as f:
        f.write(content)
    file = discord.File(filename, filename=filename)
    await interaction.response.send_message("📤 Here's your exported list of tracked artists:", file=file)

@bot.tree.command(name="channels", description="Show the current channels for releases, logs, and commands.")
@require_registration
@app_commands.checks.has_permissions(manage_guild=True)
async def channels_command(interaction: discord.Interaction):
    guild = interaction.guild
    guild_id = str(guild.id)

    platforms = {
        "spotify": "🟢 Spotify",
        "soundcloud": "🎧 SoundCloud",
        "logs": "🪵 Logs",
        "commands": "💬 Commands"
    }

    lines = []
    for key, label in platforms.items():
        channel_id = get_channel(guild_id, key)
        if channel_id:
            channel = bot.get_channel(int(channel_id))
            channel_mention = channel.mention if channel else f"`{channel_id}`"
        else:
            channel_mention = "*Not Set*"
        lines.append(f"{label} — {channel_mention}")

    embed = discord.Embed(
        title="📡 Configured Channels",
        description="\n".join(lines),
        color=discord.Color.orange()
    )
    await interaction.response.send_message(embed=embed, ephemeral=True)

@bot.tree.command(name="debugsoundcloud", description="Test fetch SoundCloud release info manually.")
@app_commands.describe(url="A SoundCloud artist or release URL")
@require_registration
async def debug_soundcloud(interaction: discord.Interaction, url: str):
    from soundcloud_utils import get_soundcloud_release_info
    await interaction.response.defer()

    try:
        info = get_soundcloud_release_info(url)
        if info is None:
            await interaction.followup.send("❌ Could not fetch release info. Check the URL or client ID.")
            return

        embed = discord.Embed(
            title=info["title"],
            description=f"By {info['artist_name']}\nReleased: {info['release_date']}\nTracks: {info['track_count']}",
            color=discord.Color.orange()
        )
        embed.set_thumbnail(url=info["cover_url"])
        embed.add_field(name="Duration", value=info["duration"], inline=True)
        embed.add_field(name="Features", value=info["features"], inline=True)
        embed.add_field(name="Genres", value=", ".join(info["genres"]) or "None", inline=False)
        embed.add_field(name="Repost?", value="📌 Yes" if info.get("repost") else "No", inline=True)
        embed.url = info["url"]

        await interaction.followup.send(embed=embed)

    except Exception as e:
        await interaction.followup.send(f"❌ Error: {e}")

@bot.tree.command(name="checkscid", description="Verify SoundCloud client ID is valid")
@require_registration
async def check_scid_command(interaction: discord.Interaction):
    from soundcloud_utils import verify_client_id, refresh_client_id
    await interaction.response.defer(ephemeral=True)
    if verify_client_id():
        await interaction.followup.send("✅ SoundCloud client ID appears valid.")
    else:
        new_client_id = refresh_client_id()
        if new_client_id:
            await interaction.followup.send(f"✅ Refreshed SoundCloud client ID: `{new_client_id}`")
        else:
            await interaction.followup.send("❌ Failed to refresh SoundCloud client ID. Verify the ID manually.")

@bot.tree.command(name="import", description="Import previously exported tracked artists")
@app_commands.describe(file="Upload a previously exported JSON file")
async def import_command(interaction: discord.Interaction, file: discord.Attachment):
    await interaction.response.defer(ephemeral=True)

    try:
        if not file.filename.endswith(".json"):
            await interaction.followup.send("❌ File must be a `.json` export.")
            return

        contents = await file.read()
        data = json.loads(contents.decode())

        owner_id = interaction.user.id
        guild_id = str(interaction.guild.id) if interaction.guild else None

        added_count = import_artists_from_json(data, owner_id, guild_id)
        await interaction.followup.send(f"✅ Imported {added_count} artists.")

    except Exception as e:
        await interaction.followup.send(f"❌ Failed to import: {e}")

@bot.tree.command(name="testcache", description="Test SQLite cache.")
async def test_cache_command(interaction: discord.Interaction):
    try:
        set_cache("test_key", "test_value", ttl=60)
        value = get_cache("test_key")
        await interaction.response.send_message(f"✅ Cache is working. Test value: {value}")
    except Exception as e:
        await interaction.response.send_message(f"❌ Cache error: {e}")

@bot.tree.command(name="resetbot", description="Reset all bot data and state.")
@app_commands.checks.has_permissions(administrator=True)
async def reset_bot_command(interaction: discord.Interaction):
    try:
        # Clear cache
        clear_all_cache()
        initialize_fresh_database()

        # Reset activity tracking
        from database_utils import reset_activity_tracking
        reset_activity_tracking()

        # Reset bot state
        await reset_bot_state()

        await interaction.response.send_message("✅ Bot data and state reset successfully.")
    except Exception as e:
        await interaction.response.send_message(f"❌ Failed to reset bot: {e}")

if __name__ == "__main__":
    # Initialize SQLite cache table
    initialize_cache_table()

    try:
        keep_alive()  # Start the web server for UptimeRobot
        bot.run(TOKEN)
    finally:
        logging.info("✅ Bot shutdown complete.")