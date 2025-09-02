# REMEMBER, TO ADD CHANNELS LINE 75 and 189 IN TABLES.PY

import os
import typing
from typing import Optional, Literal
import discord
import functools
import logging
import json
from dateutil.parser import isoparse
from dateutil.parser import parse as parse_datetime
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
    record_bot_startup, record_bot_shutdown, get_downtime_duration, get_playlist_state, store_playlist_state,
    set_guild_feature, is_feature_enabled, get_guild_features,
    get_last_release_check, update_last_release_check  # <-- added imports
)
from embed_utils import create_music_embed, create_repost_embed, create_like_embed
from spotify_utils import (
    extract_spotify_id,
    get_artist_name as get_spotify_artist_name,
    get_artist_info as get_spotify_artist_info,
    get_last_release_date as get_spotify_last_release_date,
    get_release_info as get_spotify_release_info,
    get_latest_album_id as get_spotify_latest_album_id,
    init_spotify_key_manager,
    manual_rotate_spotify_key,
    get_spotify_key_status,  # <-- added
    validate_spotify_client,  # <-- added
    ping_spotify  # <-- added
)
import spotify_utils  # added for dynamic key manager access

from soundcloud_utils import (
    extract_soundcloud_id,
    get_artist_name_by_url as get_soundcloud_artist_name,
    get_last_release_date as get_soundcloud_last_release_date,
    get_soundcloud_release_info,
    get_soundcloud_artist_id,
    get_soundcloud_playlist_info,
    get_soundcloud_likes_info,
    get_soundcloud_reposts,
    get_soundcloud_likes,
    get_soundcloud_reposts_info,
    get_artist_info,
    init_key_manager,
    CLIENT_ID as SC_CLIENT_ID,
    manual_rotate_soundcloud_key,
    get_soundcloud_telemetry_snapshot,  # <-- added import (already existed comment)
    get_soundcloud_key_status,  # <-- added
    expand_soundcloud_short_url,  # <-- NEW
    begin_soundcloud_release_batch,        # <-- added for silent limit detection
    note_soundcloud_release_fetch          # <-- added for silent limit detection
)
import soundcloud_utils  # added for dynamic key manager access
from utils import run_blocking, log_release, parse_datetime, get_cache, set_cache, delete_cache, clear_all_cache, get_cache_stats
from reset_artists import reset_tables
from tables import initialize_fresh_database, initialize_cache_table, create_all_tables
import sqlite3
import signal
import sys


# ===== Below are the 2 commands to delete all saved data, use top one for a full wipe
# initialize_fresh_database()
# reset_tables()

# Ensure the /data directory exists
os.makedirs('/data', exist_ok=True)
# Initialize database schema (idempotent) before any key managers use persistence
try:
    create_all_tables()
    logging.info("✅ Database schema ensured (all tables created if missing)")
except Exception as e:
    logging.error(f"❌ Failed ensuring database schema: {e}")

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

# Optional JSON logging toggle
if os.getenv('LOG_JSON') == '1':
    class _JSONFormatter(logging.Formatter):
        def format(self, record):
            payload = {
                'ts': datetime.utcnow().isoformat() + 'Z',
                'level': record.levelname,
                'msg': record.getMessage(),
                'logger': record.name,
                'module': record.module
            }
            return json.dumps(payload, ensure_ascii=False)
    for h in logging.getLogger().handlers:
        h.setFormatter(_JSONFormatter())

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
        # Pure date -> set to end of day to avoid being 'older' than a stored same-day timestamp
        if len(date_str) == 10 and date_str.count('-') == 2:
            dt = datetime.strptime(date_str, '%Y-%m-%d').replace(tzinfo=timezone.utc)
            return dt + timedelta(hours=23, minutes=59, seconds=59)
        if 'T' in date_str:
            # Accept both with and without timezone / microseconds
            ds = date_str.replace('Z', '+0000')
            fmts = ['%Y-%m-%dT%H:%M:%S%z', '%Y-%m-%dT%H:%M:%S.%f%z']
            for fmt in fmts:
                try:
                    return datetime.strptime(ds, fmt)
                except ValueError:
                    pass
        # Fallback to dateutil
        dt = isoparse(date_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception as e:
        logging.error(f"Failed to parse date '{date_str}': {e}")
        return datetime.min.replace(tzinfo=timezone.utc)

def _fmt_dt(dt_obj: datetime | None):
    # Shared date formatter for logging
    if not dt_obj:
        return "Unknown"
    if dt_obj.tzinfo is None:
        dt_obj = dt_obj.replace(tzinfo=timezone.utc)
    return dt_obj.astimezone(timezone.utc).strftime('%Y-%m-%d %I:%M:%S %p')

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
        self._health_task = None
    
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

    async def start_health_logger(self):
        if self._health_task:
            return
        async def _loop():
            while not self.is_closed():
                try:
                    await asyncio.sleep(900)  # 15 minutes
                    await self.emit_health_log()
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logging.error(f"Health logger error: {e}")
        self._health_task = asyncio.create_task(_loop())

    async def emit_health_log(self):
        try:
            # Attempt lazy init if managers somehow missing
            if not getattr(spotify_utils, 'spotify_key_manager', None):
                init_spotify_key_manager(self)
            if not getattr(soundcloud_utils, 'key_manager', None):
                init_key_manager(self)

            artists = get_all_artists()
            total = len(artists)
            spotify_count = sum(1 for a in artists if a.get('platform') == 'spotify')
            sc_count = sum(1 for a in artists if a.get('platform') == 'soundcloud')

            # Use public status helpers (more robust & future-proof)
            sp_status = get_spotify_key_status()
            sc_status = get_soundcloud_key_status()

            spotify_rows = []
            if sp_status.get('loaded') and sp_status.get('keys'):
                for row in sp_status['keys']:
                    spotify_rows.append(f"K{row['index']+1}:{row['state']}")
            else:
                spotify_rows.append('none')

            sc_rows = []
            if sc_status.get('loaded') and sc_status.get('keys'):
                for row in sc_status['keys']:
                    sc_rows.append(f"K{row['index']+1}:{row['state']}")
            else:
                sc_rows.append('none')

            # Extra diagnostic line if showing none but keys actually configured in env
            if ('none' in spotify_rows) and os.getenv('SPOTIFY_CLIENT_ID'):
                logging.warning("HealthLog: Spotify keys appear loaded in env but manager returned none – possible init timing issue.")
            if ('none' in sc_rows) and os.getenv('SOUNDCLOUD_CLIENT_ID'):
                logging.warning("HealthLog: SoundCloud keys appear loaded in env but manager returned none – possible init timing issue.")

            msg = (
                "🩺 **Health Report**\n"
                f"Artists: {total} (Spotify {spotify_count} / SoundCloud {sc_count})\n"
                f"Spotify Keys: {' | '.join(spotify_rows)}\n"
                f"SoundCloud Keys: {' | '.join(sc_rows)}"
            )
            logging.info(msg)
            if self.log_channel:
                try:
                    await self.log_channel.send(msg)
                except Exception:
                    pass
        except Exception as e:
            logging.error(f"Failed to emit health log: {e}")

bot = MusicBot()
CLIENT_ID = init_key_manager(bot) 
init_spotify_key_manager(bot)  # ✅ initialize Spotify key rotation manager

if not CLIENT_ID:
    logging.error("❌ No valid SoundCloud CLIENT_ID available")

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
    """Coordinate all platform checks with per-platform timeout watchdog.
    If a platform phase exceeds PLATFORM_PHASE_TIMEOUT seconds, rotate that
    platform's key and abort the current cycle (next run will occur on schedule).
    """
    PLATFORM_PHASE_TIMEOUT = int(os.getenv('PLATFORM_PHASE_TIMEOUT', '120'))

    logging.info("\n🚀 Starting check for new releases")
    if is_catchup:
        logging.info("(Catch-up mode)")

    # General tasks
    artists, shutdown_time, general_errors = await check_general_tasks(bot, is_catchup)
    if not artists:
        logging.warning("No artists available; aborting cycle early.")
        return

    # --- Spotify Phase ---
    logging.info("▶️ Starting Spotify phase")
    # Added explicit debug of active Spotify credentials
    try: 
        if getattr(spotify_utils, 'spotify_key_manager', None) and spotify_utils.spotify_key_manager.keys:
            _cid, _sec = spotify_utils.spotify_key_manager.get_current_key()
            logging.warning("Starting Spotify Phase using....\nclient_id: %s\nclient_secret: %s" % (_cid, _sec))  # INTENTIONAL FULL OUTPUT FOR DEBUG
        else:
            logging.warning("Starting Spotify Phase using....\nclient_id: <unavailable>\nclient_secret: <unavailable> (manager not initialized)")
    except Exception as _cred_e:
        logging.error(f"Failed retrieving current Spotify credentials for debug: {_cred_e}")
    try:
        spotify_results = await asyncio.wait_for(
            check_spotify_updates(bot, artists, shutdown_time, is_catchup),
            timeout=PLATFORM_PHASE_TIMEOUT
        )
        logging.info("✅ Spotify phase finished")
    except asyncio.TimeoutError:
        logging.error(f"⏱️ Spotify phase exceeded {PLATFORM_PHASE_TIMEOUT}s; rotating Spotify API key and aborting cycle")
        try:
            manual_rotate_spotify_key(reason="phase_timeout")
        except Exception as e:
            logging.error(f"Failed rotating Spotify key after timeout: {e}")
        return
    except Exception as e:
        logging.error(f"❌ Spotify phase failed unexpectedly: {e}")
        try:
            manual_rotate_spotify_key(reason="phase_exception")
        except Exception:
            pass
        return
    spotify_releases, spotify_errors = spotify_results

    # --- SoundCloud Phase ---
    logging.info("▶️ Starting SoundCloud phase")
    try:
        soundcloud_results = await asyncio.wait_for(
            check_soundcloud_updates(bot, artists, shutdown_time, is_catchup),
            timeout=PLATFORM_PHASE_TIMEOUT
        )
        logging.info("✅ SoundCloud phase finished")
    except asyncio.TimeoutError:
        logging.error(f"⏱️ SoundCloud phase exceeded {PLATFORM_PHASE_TIMEOUT}s; rotating SoundCloud API key and aborting cycle")
        try:
            manual_rotate_soundcloud_key(reason="phase_timeout")
        except Exception as e:
            logging.error(f"Failed rotating SoundCloud key after timeout: {e}")
        return
    except Exception as e:
        logging.error(f"❌ SoundCloud phase failed unexpectedly: {e}")
        try:
            manual_rotate_soundcloud_key(reason="phase_exception")
        except Exception:
            pass
        return
    soundcloud_counts, soundcloud_errors = soundcloud_results

    # Compile results only if both phases finished
    total_releases = spotify_releases + sum(soundcloud_counts.values())
    all_errors = (general_errors or []) + spotify_errors + soundcloud_errors

    logging.info("🎯 All platform checks finished successfully!")
    await log_summary(len(artists), total_releases, all_errors)

# === Helper platform check functions (added) ===
async def check_general_tasks(bot, is_catchup: bool = False):
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
    shutdown_time = None
    if is_catchup:
        try:
            with get_connection() as conn:
                cur = conn.cursor()
                cur.execute("""
                    SELECT timestamp FROM activity_logs 
                    WHERE user_id='system' AND action='bot_shutdown'
                    ORDER BY timestamp DESC LIMIT 1
                """)
                row = cur.fetchone()
                shutdown_time = row[0] if row else None
                if shutdown_time:
                    logging.info(f"📅 Last shutdown: {shutdown_time}")
        except Exception as e:
            logging.error(f"❌ Failed retrieving shutdown time: {e}")
    return artists, shutdown_time, errors

async def check_spotify_updates(bot, artists, shutdown_time=None, is_catchup: bool = False):
    def _fmt_dt(dt_obj: datetime | None):
        if not dt_obj:
            return "Unknown"
        # Ensure timezone aware
        if dt_obj.tzinfo is None:
            dt_obj = dt_obj.replace(tzinfo=timezone.utc)
        return dt_obj.astimezone(timezone.utc).strftime('%Y-%m-%d %I:%M:%S %p')
    # ...existing code before loop...
    try:
        validate_spotify_client()
        if not ping_spotify():
            manual_rotate_spotify_key(reason="pre_check_ping_fail")
            validate_spotify_client()
    except Exception as e:
        logging.error(f"Spotify validation error: {e}")
    errors = []
    releases = 0
    batch_check_time = datetime.now(timezone.utc).isoformat()
    logging.info(f"\n🟢 CHECKING SPOTIFY{' (CATCH-UP)' if is_catchup else ''}…")
    logging.info("="*50)
    for artist in artists:
        if artist.get('platform') != 'spotify':
            continue
        artist_name = artist.get('artist_name','unknown')
        artist_id = artist.get('artist_id')
        owner_id = artist.get('owner_id')
        guild_id = artist.get('guild_id')
        last_release_check = get_last_release_check(artist_id, owner_id, guild_id)
        last_release_date_raw = artist.get('last_release_date')
        try:
            # Parse stored timestamps
            last_release_dt = parse_date(last_release_date_raw) if last_release_date_raw else None
            last_check_dt = parse_date(last_release_check) if last_release_check else None
            logging.info(f"🟢 Checking {artist_name}")
            logging.info("")  # blank line for readability
            logging.info(f"     Last '{artist_name}' release: {_fmt_dt(last_release_dt)}")
            logging.info(f"     Last '{artist_name}' release check: {_fmt_dt(last_check_dt)}")
            latest_album_id = await run_blocking(get_spotify_latest_album_id, artist_id)
            if not latest_album_id:
                logging.info("     API returned: None")
                logging.info("     ⏭️ No releases returned")
                update_last_release_check(artist_id, owner_id, guild_id, batch_check_time)
                continue
            release_info = await run_blocking(get_spotify_release_info, latest_album_id)
            if not release_info:
                logging.info("     API returned: None (no release info)")
                logging.info("     ⏭️ No release info")
                update_last_release_check(artist_id, owner_id, guild_id, batch_check_time)
                continue
            api_release_date = release_info.get('release_date')
            if not api_release_date:
                logging.info("     API returned: <missing release_date>")
                logging.info("     ⏭️ Skipping (no release_date)")
                update_last_release_check(artist_id, owner_id, guild_id, batch_check_time)
                continue
            api_dt = parse_date(api_release_date)
            logging.info(f"     API returned: {_fmt_dt(api_dt)}")
            if last_check_dt is None:
                logging.info(f"     ⏭️ Baseline established (no previous check)")
                update_last_release_check(artist_id, owner_id, guild_id, batch_check_time)
                continue
            if api_dt > last_check_dt:
                cache_key = f"posted_spotify:{artist_id}:{latest_album_id}:{api_release_date}"
                if get_cache(cache_key):
                    logging.info(f"     ⏭️ Duplicate suppressed (api_release_date {_fmt_dt(api_dt)} > last_check {_fmt_dt(last_check_dt)})")
                else:
                    channel = await get_release_channel(guild_id, 'spotify')
                    if channel:
                        embed = create_music_embed(
                            platform='spotify',
                            artist_name=artist_name,
                            title=release_info.get('title','New Release'),
                            url=release_info.get('url'),
                            release_date=api_release_date,
                            cover_url=release_info.get('cover_url'),
                            features=release_info.get('features'),
                            track_count=release_info.get('track_count'),
                            duration=release_info.get('duration'),
                            genres=release_info.get('genres',[]),
                            repost=False
                        )
                        await channel.send(embed=embed)
                        update_last_release_date(artist_id, owner_id, guild_id, api_release_date)
                        set_cache(cache_key, 'posted', ttl=86400)
                        releases += 1
                        logging.info(f"     ⏭️ NEW (api_release_date {_fmt_dt(api_dt)} > last_check {_fmt_dt(last_check_dt)})")
                    else:
                        logging.warning("     ⚠️ No Spotify channel configured")
                        logging.info(f"     ⏭️ NEW (not posted - no channel)")
            else:
                logging.info(f"     ⏭️ Not new (api_release_date {_fmt_dt(api_dt)} <= last_check {_fmt_dt(last_check_dt)})")
            update_last_release_check(artist_id, owner_id, guild_id, batch_check_time)
        except Exception as e:
            logging.error(f"     ❌ Error for {artist_name}: {e}")
            errors.append({'type':'Spotify','message':str(e)})
            update_last_release_check(artist_id, owner_id, guild_id, batch_check_time)
    return releases, errors

async def check_soundcloud_updates(bot, artists, shutdown_time=None, is_catchup: bool = False):
    global CLIENT_ID
    begin_soundcloud_release_batch()
    errors = []
    counts = {'releases':0,'playlists':0,'reposts':0,'likes':0}
    if not CLIENT_ID:
        logging.error("❌ No valid SoundCloud API key")
        errors.append({'type':'SoundCloud','message':'No API key'})
        return counts, errors
    batch_check_time = datetime.now(timezone.utc).isoformat()
    logging.info(f"\n🟠 CHECKING SOUNDCLOUD{' (CATCH-UP)' if is_catchup else ''}…")
    logging.info("="*50)

    def _is_new_activity(activity_dt, last_check_dt):
        """
        Mirror Spotify logic:
        - If last_check_dt is None -> baseline (do not post; return False)
        - Post only if activity_dt > last_check_dt
        - Equal or older -> not new
        """
        if last_check_dt is None:
            return False
        if not activity_dt:
            return False
        return activity_dt > last_check_dt

    def _log_header(artist_name, section, last_item_dt, last_check_dt):
        # Standardized header similar to Spotify format
        logging.info(f"     Last '{artist_name}' {section}: {_fmt_dt(last_item_dt)}")
        logging.info(f"     Last '{artist_name}' release check: {_fmt_dt(last_check_dt)}")

    for artist in artists:
        if artist.get('platform') != 'soundcloud':
            continue
        artist_name = artist.get('artist_name','unknown')
        artist_id = artist.get('artist_id')
        owner_id = artist.get('owner_id')
        guild_id = artist.get('guild_id')
        artist_url = artist.get('artist_url')

        # Stored per-type dates (may be None)
        last_release_date_raw = artist.get('last_release_date')
        last_playlist_date_raw = artist.get('last_playlist_date')
        last_repost_date_raw = artist.get('last_repost_date')
        last_like_date_raw = artist.get('last_like_date')

        last_release_check = get_last_release_check(artist_id, owner_id, guild_id)
        last_check_dt = parse_date(last_release_check) if last_release_check else None
        baseline = last_check_dt is None
        try:
            logging.info(f"🟠 Checking {artist_name}")
            # === RELEASE (latest) ===
            release_info = await run_blocking(get_soundcloud_release_info, artist_url)
            last_release_dt = parse_date(last_release_date_raw) if last_release_date_raw else None
            if release_info:
                api_release_date = release_info.get('release_date')
                api_dt = parse_date(api_release_date) if api_release_date else None
                _log_header(artist_name, 'release', last_release_dt, last_check_dt)
                logging.info(f"     API returned: {_fmt_dt(api_dt)}")
                if baseline:
                    logging.info("     ⏭️ Baseline established (no previous check)")
                elif api_dt and _is_new_activity(api_dt, last_check_dt):
                    cache_key = f"posted_sc:{artist_id}:{release_info.get('url')}:{api_release_date}"
                    if get_cache(cache_key):
                        logging.info(f"     ⏭️ Duplicate suppressed (api_release_date {_fmt_dt(api_dt)} > last_check {_fmt_dt(last_check_dt)})")
                    else:
                        channel = await get_release_channel(guild_id, 'soundcloud')
                        if channel:
                            embed = create_music_embed(
                                platform='soundcloud',
                                artist_name=artist_name,
                                title=release_info.get('title','New Release'),
                                url=release_info.get('url'),
                                release_date=api_release_date,
                                cover_url=release_info.get('cover_url'),
                                features=release_info.get('features'),
                                track_count=release_info.get('track_count'),
                                duration=release_info.get('duration'),
                                genres=release_info.get('genres'),
                                repost=False
                            )
                            await channel.send(embed=embed)
                            update_last_release_date(artist_id, owner_id, guild_id, api_release_date)
                            set_cache(cache_key, 'posted', ttl=86400)
                            counts['releases'] += 1
                            logging.info(f"     ✅ NEW (api_release_date {_fmt_dt(api_dt)} > last_check {_fmt_dt(last_check_dt)})")
                        else:
                            logging.warning("     ⚠️ No SoundCloud channel configured for release")
                else:
                    if api_dt and last_check_dt and api_dt == last_check_dt:
                        logging.info(f"     ⏭️ Not new (same timestamp)")
                    else:
                        logging.info(f"     ⏭️ Not new (api_release_date {_fmt_dt(api_dt)} <= last_check {_fmt_dt(last_check_dt)})")
            else:
                _log_header(artist_name, 'release', last_release_dt, last_check_dt)
                logging.info("     API returned: None")
                logging.info("     ⏭️ No releases returned")

            # === PLAYLIST (single latest) ===
            try:
                playlist_info = await run_blocking(get_soundcloud_playlist_info, artist_url)
            except Exception as e_pl:
                playlist_info = None
                logging.error(f"     ❌ Playlist fetch error: {e_pl}")
                errors.append({'type':'SoundCloud Playlist','message':str(e_pl)})
            playlist_dt_stored = parse_date(last_playlist_date_raw) if last_playlist_date_raw else None
            if playlist_info:
                playlist_date = playlist_info.get('release_date')
                playlist_dt = parse_date(playlist_date) if playlist_date else None
                playlist_id = playlist_info.get('url') or f"playlist_{artist_id}_{playlist_date}"
                _log_header(artist_name, 'playlist', playlist_dt_stored, last_check_dt)
                logging.info(f"     API returned: {_fmt_dt(playlist_dt)}")
                if baseline:
                    logging.info("     ⏭️ Baseline established (no previous check)")
                elif playlist_dt and _is_new_activity(playlist_dt, last_check_dt):
                    if is_already_posted_playlist(artist_id, guild_id, playlist_id):
                        logging.info("     ⏭️ Playlist already posted")
                    else:
                        channel = await get_release_channel(guild_id, 'soundcloud')
                        if channel:
                            await handle_release(bot, artist, playlist_info, 'playlist')
                            mark_posted_playlist(artist_id, guild_id, playlist_id)
                            update_last_playlist_date(artist_id, guild_id, playlist_date)
                            counts['playlists'] += 1
                            logging.info(f"     ✅ NEW (playlist_date {_fmt_dt(playlist_dt)} > last_check {_fmt_dt(last_check_dt)})")
                        else:
                            logging.warning("     ⚠️ No SoundCloud channel configured for playlist")
                else:
                    if playlist_dt and last_check_dt and playlist_dt == last_check_dt:
                        logging.info(f"     ⏭️ Not new (same timestamp)")
                    else:
                        logging.info(f"     ⏭️ Not new (playlist_date {_fmt_dt(playlist_dt)} <= last_check {_fmt_dt(last_check_dt)})")
            else:
                _log_header(artist_name, 'playlist', playlist_dt_stored, last_check_dt)
                logging.info("     API returned: None")
                logging.info("     ⏭️ No playlists returned")

            # === REPOSTS (multiple) ===
            try:
                reposts = await run_blocking(get_soundcloud_reposts_info, artist_url)
            except Exception as e_repost:
                reposts = []
                logging.error(f"     ❌ Error processing reposts for {artist_name}: {e_repost}")
                errors.append({'type':'SoundCloud Reposts','message':str(e_repost)})
            last_repost_dt_stored = parse_date(last_repost_date_raw) if last_repost_date_raw else None
            logging.info(f"     🔄 Reposts returned: {len(reposts)}")
            _log_header(artist_name, 'repost', last_repost_dt_stored, last_check_dt)
            if baseline:
                logging.info("     ⏭️ Baseline established (reposts skipped this cycle)")
            else:
                for repost in reposts:
                    repost_id = repost.get('url') or repost.get('track_id') or repost.get('title')
                    if not repost_id:
                        continue
                    repost_id = str(repost_id)
                    repost_activity_date = parse_date(repost.get('reposted_date')) if repost.get('reposted_date') else None
                    logging.info(f"          🔄 Repost: {repost.get('title')} -> {_fmt_dt(repost_activity_date)}")
                    if not repost_activity_date:
                        continue
                    # NEW baseline & grace logic for reposts
                    baseline_repost_dt = last_repost_dt_stored  # independent of last_check_dt
                    GRACE_SECONDS = 90  # allow slight skew before last_check_dt
                    is_baseline = baseline_repost_dt is None
                    # A repost is new if:
                    #  - we have no stored repost date yet (post the freshest ones once)
                    #  - or its reposted_date > stored repost date
                    # Additionally, if it failed the above but (repost_date <= last_check_dt) only by a small grace window, still allow once
                    new_by_repost_clock = (baseline_repost_dt is None) or (repost_activity_date and baseline_repost_dt and repost_activity_date > baseline_repost_dt)
                    within_grace = False
                    if (not new_by_repost_clock) and repost_activity_date and last_check_dt and repost_activity_date <= last_check_dt:
                        if (last_check_dt - repost_activity_date).total_seconds() <= GRACE_SECONDS:
                            within_grace = True

                    if is_already_posted_repost(artist_id, guild_id, repost_id):
                        logging.info("              ⏭️ Already posted")
                        # Advance stored repost date if this item is newer than stored to prevent repeated scans
                        if repost_activity_date and (not baseline_repost_dt or repost_activity_date > baseline_repost_dt):
                            update_last_repost_date(artist_id, guild_id, repost.get('reposted_date'))
                        continue

                    if new_by_repost_clock or within_grace:
                        reason = "baseline (first reposts)" if is_baseline else (
                            "reposted_date > last_repost_date" if new_by_repost_clock else
                            f"grace_window ({int((last_check_dt - repost_activity_date).total_seconds())}s ≤ {GRACE_SECONDS}s)")
                        channel = await get_release_channel(guild_id, 'soundcloud')
                        if channel:
                            embed = create_repost_embed(
                                platform=artist.get('platform'),
                                reposted_by=artist_name,
                                original_artist=repost.get('artist_name'),
                                title=repost.get('title'),
                                url=repost.get('url'),
                                release_date=repost.get('release_date'),
                                reposted_date=repost.get('reposted_date'),
                                cover_url=repost.get('cover_url'),
                                features=repost.get('features'),
                                track_count=repost.get('track_count'),
                                duration=repost.get('duration'),
                                genres=repost.get('genres'),
                            )
                            await channel.send(embed=embed)
                            mark_posted_repost(artist_id, guild_id, repost_id)
                            update_last_repost_date(artist_id, guild_id, repost.get('reposted_date'))
                            counts['reposts'] += 1
                            logging.info(f"              ✅ NEW ({reason})")
                        else:
                            logging.warning("              ⚠️ No channel for repost")
                    else:
                        # Detailed skip reasons
                        if baseline_repost_dt and repost_activity_date and repost_activity_date <= baseline_repost_dt:
                            logging.info(f"              ⏭️ Not new (reposted_date {_fmt_dt(repost_activity_date)} <= last_repost {_fmt_dt(baseline_repost_dt)})")
                        elif last_check_dt and repost_activity_date and repost_activity_date <= last_check_dt:
                            logging.info(f"              ⏭️ Not new (reposted_date {_fmt_dt(repost_activity_date)} <= last_check {_fmt_dt(last_check_dt)})")
                        else:
                            logging.info("              ⏭️ Not new (no qualifying condition)")

            # === LIKES (multiple) ===
            try:
                likes = await run_blocking(get_soundcloud_likes_info, artist_url)
            except Exception as e_likes:
                likes = []
                logging.error(f"     ❌ Error processing likes for {artist_name}: {e_likes}")
                errors.append({'type':'SoundCloud Likes','message':str(e_likes)})
            last_like_dt_stored = parse_date(last_like_date_raw) if last_like_date_raw else None
            logging.info(f"     ❤️ Likes returned: {len(likes)}")
            _log_header(artist_name, 'like', last_like_dt_stored, last_check_dt)
            if baseline:
                logging.info("     ⏭️ Baseline established (likes skipped this cycle)")
            else:
                for like in likes:
                    track_id = like.get('track_id')
                    if not track_id:
                        continue
                    like_id = str(track_id)
                    like_activity_date = parse_date(like.get('liked_date')) if like.get('liked_date') else None
                    logging.info(f"          ❤️ Like: {like.get('title')} -> {_fmt_dt(like_activity_date)}")
                    if not like_activity_date:
                        continue
                    if is_already_posted_like(artist_id, guild_id, like_id):
                        logging.info("              ⏭️ Already posted")
                        continue
                    if _is_new_activity(like_activity_date, last_check_dt):
                        channel = await get_release_channel(guild_id, 'soundcloud')
                        if channel:
                            embed = create_like_embed(
                                platform=artist.get('platform'),
                                liked_by=artist_name,
                                title=like.get('title'),
                                artist_name=like.get('artist_name'),
                                url=like.get('url'),
                                release_date=like.get('release_date'),  # original upload date (for display)
                                liked_date=like.get('liked_date'),      # activity date (comparison basis)
                                cover_url=like.get('cover_url'),
                                features=like.get('features'),
                                track_count=like.get('track_count'),
                                duration=like.get('duration'),
                                genres=like.get('genres'),
                                content_type=like.get('content_type') or 'like'
                            )
                            await channel.send(embed=embed)
                            mark_posted_like(artist_id, guild_id, like_id)
                            # FIX: store liked_date (activity) not release_date
                            update_last_like_date(artist_id, guild_id, like.get('liked_date'))
                            counts['likes'] += 1
                            logging.info(f"              ✅ NEW (like_date {_fmt_dt(like_activity_date)} > last_check {_fmt_dt(last_check_dt)})")
                        else:
                            logging.warning("              ⚠️ No channel for like")
                    else:
                        if last_check_dt and like_activity_date and like_activity_date == last_check_dt:
                            logging.info(f"              ⏭️ Not new (same timestamp)")
                        else:
                            logging.info(f"              ⏭️ Not new (like_date {_fmt_dt(like_activity_date)} <= last_check {_fmt_dt(last_check_dt)})")

            # Unified last check update AFTER all comparisons
            update_last_release_check(artist_id, owner_id, guild_id, batch_check_time)
        except Exception as e:
            logging.error(f"     ❌ Error for {artist_name}: {e}")
            errors.append({'type':'SoundCloud','message':str(e)})
            update_last_release_check(artist_id, owner_id, guild_id, batch_check_time)
            continue
    logging.info(f"Summary SC -> Releases:{counts['releases']} Playlists:{counts['playlists']} Reposts:{counts['reposts']} Likes:{counts['likes']}")
    return counts, errors

# --- SCHEDULER ---
# (Deprecated unified scheduler kept for reference; platform-specific schedulers below)
async def release_check_scheduler(bot):
    # ...existing code...
    pass  # deprecated

async def spotify_release_scheduler(bot):
    await bot.wait_until_ready()
    logging.info("🚀 Spotify scheduler started (hourly at HH:00:01 UTC)")
    PLATFORM_PHASE_TIMEOUT = int(os.getenv('PLATFORM_PHASE_TIMEOUT', '120'))
    while not bot.is_closed():
        now = datetime.now(timezone.utc)
        hour_anchor = now.replace(minute=0, second=1, microsecond=0)
        if now < hour_anchor:
            next_run = hour_anchor
        else:
            next_run = hour_anchor + timedelta(hours=1)
        delay = (next_run - now).total_seconds()
        logging.info(f"🕰️ Next Spotify check at {next_run.strftime('%Y-%m-%d %H:%M:%S')} UTC (in {delay/60:.2f} min)")
        try:
            await asyncio.sleep(delay)
        except asyncio.CancelledError:
            logging.info("🛑 Spotify scheduler cancelled")
            return
        try:
            logging.info("▶️ Spotify scheduled run starting")
            artists, _, _ = await check_general_tasks(bot, is_catchup=False)
            if not artists:
                logging.info("⚠️ No artists to check (Spotify)")
                continue
            try:
                spotify_results = await asyncio.wait_for(
                    check_spotify_updates(bot, artists, shutdown_time=None, is_catchup=False),
                    timeout=PLATFORM_PHASE_TIMEOUT
                )
                releases, errors = spotify_results
                logging.info(f"✅ Spotify run complete: releases={releases} errors={len(errors)}")
            except asyncio.TimeoutError:
                logging.error(f"⏱️ Spotify phase exceeded {PLATFORM_PHASE_TIMEOUT}s; rotating key")
                try:
                    manual_rotate_spotify_key(reason="scheduler_timeout")
                except Exception:
                    pass
            except Exception as e:
                logging.error(f"❌ Spotify scheduler run failed: {e}")
        except Exception as e_outer:
            logging.error(f"❌ Unexpected Spotify scheduler error: {e_outer}")

async def soundcloud_release_scheduler(bot):
    await bot.wait_until_ready()
    logging.info("🚀 SoundCloud scheduler started (every 5 min at mm multiple of 5, second 1 UTC)")
    PLATFORM_PHASE_TIMEOUT = int(os.getenv('PLATFORM_PHASE_TIMEOUT', '120'))
    interval_minutes = 5
    while not bot.is_closed():
        now = datetime.now(timezone.utc)
        midnight = now.replace(hour=0, minute=0, second=1, microsecond=0)
        if now < midnight:
            next_run = midnight
        else:
            minutes_since_midnight = int((now - midnight).total_seconds() // 60)
            next_multiple = ((minutes_since_midnight // interval_minutes) + 1) * interval_minutes
            next_run = midnight + timedelta(minutes=next_multiple)
        delay = (next_run - now).total_seconds()
        if delay < 0.5:
            next_run += timedelta(minutes=interval_minutes)
            delay = (next_run - now).total_seconds()
        logging.info(f"🕰️ Next SoundCloud check at {next_run.strftime('%Y-%m-%d %H:%M:%S')} UTC (in {delay/60:.2f} min)")
        try:
            await asyncio.sleep(delay)
        except asyncio.CancelledError:
            logging.info("🛑 SoundCloud scheduler cancelled")
            return
        try:
            logging.info("▶️ SoundCloud scheduled run starting")
            artists, _, _ = await check_general_tasks(bot, is_catchup=False)
            if not artists:
                logging.info("⚠️ No artists to check (SoundCloud)")
                continue
            try:
                sc_results = await asyncio.wait_for(
                    check_soundcloud_updates(bot, artists, shutdown_time=None, is_catchup=False),
                    timeout=PLATFORM_PHASE_TIMEOUT
                )
                counts, errors = sc_results
                logging.info(f"✅ SoundCloud run complete: releases={counts['releases']} playlists={counts['playlists']} reposts={counts['reposts']} likes={counts['likes']} errors={len(errors)}")
            except asyncio.TimeoutError:
                logging.error(f"⏱️ SoundCloud phase exceeded {PLATFORM_PHASE_TIMEOUT}s; rotating key")
                try:
                    manual_rotate_soundcloud_key(reason="scheduler_timeout")
                except Exception:
                    pass
            except Exception as e:
                logging.error(f"❌ SoundCloud scheduler run failed: {e}")
        except Exception as e_outer:
            logging.error(f"❌ Unexpected SoundCloud scheduler error: {e_outer}")

# --- EVENT HANDLERS ---

@bot.event
async def on_ready():
    await bot.wait_until_ready()
    logging.info(f"✅ Logged in as {bot.user} (ID: {bot.user.id})")
    # ...existing code...
    # ✅ Handle startup catch-up
    if not hasattr(bot, 'catchup_done') or not bot.catchup_done:
        should_catchup = await handle_bot_startup_catchup()
        if should_catchup:
            try:
                await check_for_new_releases(bot, is_catchup=True)
            except Exception as e:
                logging.error(f"❌ Catch-up failed: {e}")
        bot.catchup_done = True
    # Start platform-specific schedulers
    if not hasattr(bot, 'spotify_scheduler_started'):
        bot.spotify_scheduler_started = True
        asyncio.create_task(spotify_release_scheduler(bot))
        logging.info("🚀 Started Spotify hourly scheduler")
    if not hasattr(bot, 'soundcloud_scheduler_started'):
        bot.soundcloud_scheduler_started = True
        asyncio.create_task(soundcloud_release_scheduler(bot))
        logging.info("🚀 Started SoundCloud 5-minute scheduler")
    # Start health logger
    await bot.start_health_logger()

# Handle graceful shutdown
def signal_handler(sig, frame):
    """Handle shutdown gracefully."""
    logging.info("🛑 Bot shutting down...")
    try:
        record_bot_shutdown()
    except Exception:
        pass
    try:
        if soundcloud_utils.key_manager:
            soundcloud_utils.key_manager.stop_background_tasks()
        if spotify_utils.spotify_key_manager:
            pass  # (no background loop presently)
    except Exception as e:
        logging.error(f"Error stopping background tasks: {e}")
    finally:
        loop = asyncio.get_event_loop()
        loop.stop()
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
        # Expand short Smart Link if needed (on.soundcloud.com/...)
        try:
            expanded_link = await run_blocking(expand_soundcloud_short_url, link)
        except Exception:
            expanded_link = link  # fallback silently
        try:
            artist_id = extract_soundcloud_id(expanded_link)
            artist_info = await run_blocking(get_artist_info, expanded_link)
        except Exception:
            await interaction.followup.send("❌ Invalid SoundCloud artist URL. Provide a profile like https://soundcloud.com/artistname", ephemeral=True)
            return
        artist_name = artist_info.get("name", artist_id)
        artist_url = artist_info.get("url", f"https://soundcloud.com/{artist_id}")
        genres = []
    else:
        await interaction.followup.send("❌ Link must be a valid Spotify or SoundCloud artist URL.")
        return

    # Already tracked?
    if artist_exists(platform, artist_id, user_id):
        await interaction.followup.send("⚠️ You're already tracking this artist.")
        return

    from datetime import datetime, timezone
    current_time = datetime.now(timezone.utc).isoformat()

    add_artist(
        platform=platform,
        artist_id=artist_id,
        artist_name=artist_name,
        artist_url=artist_url,
        owner_id=user_id,
        guild_id=guild_id,
        genres=genres,
        last_release_date=current_time
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
        from collections import defaultdict
        grouped = defaultdict(list)
        for artist in artists:
            grouped[artist['artist_name'].lower()].append(artist)
        merged = []
        for name_lower, group in grouped.items():
            # Prefer Spotify case if available
            display_name = next((a['artist_name'] for a in group if a['platform']=='spotify'), group[0]['artist_name'])
            platforms = sorted({a['platform'] for a in group})
            merged.append((display_name, ", ".join(p.capitalize() for p in platforms)))
        merged.sort(key=lambda x: x[0].lower())
        lines = [f"• {name} ({plats})" for name, plats in merged]
        msg = "**🎧 Tracked Artists:**\n" + "\n".join(lines[:50])
        if len(lines) > 50:
            msg += f"\n…and {len(lines)-50} more"
        await interaction.response.send_message(msg, ephemeral=True)

# ...existing code...
@bot.tree.command(name="rotatekeys", description="Force rotate API key for a platform (admin)")
@app_commands.checks.has_permissions(administrator=True)
async def rotatekeys_command(interaction: discord.Interaction, platform: Literal["spotify", "soundcloud"]):
    await interaction.response.defer(ephemeral=True)
    try:
        if platform == "spotify":
            result = manual_rotate_spotify_key(reason="manual_command")
            if not result.get("rotated"):
                msg = f"⚠️ Spotify rotation not performed: {result.get('error','unknown')}"
            else:
                msg = f"🔄 Spotify key rotated (Key {result['old_index']+1} ➜ {result['active_index']+1})."
            status_lines = []
            for row in result.get('keys', []):
                status_lines.append(f"K{row['index']+1}: {row['state']} ({row.get('client_id_preview','')})")
            if status_lines:
                msg += "\n" + "\n".join(status_lines)
            await interaction.followup.send(msg, ephemeral=True)
        else:  # soundcloud
            result = manual_rotate_soundcloud_key(reason="manual_command")
            if not result.get("rotated"):
                msg = f"⚠️ SoundCloud rotation not performed: {result.get('error','unknown')}"
            else:
                msg = f"🔄 SoundCloud key rotated (Key {result['old_index']+1} ➜ {result['active_index']+1})."
            status_lines = []
            for row in result.get('keys', []):
                preview = row.get('key_preview','')
                status_lines.append(f"K{row['index']+1}: {row['state']} ({preview})")
            if status_lines:
                msg += "\n" + "\n".join(status_lines)
            await interaction.followup.send(msg, ephemeral=True)
    except Exception as e:
        await interaction.followup.send(f"❌ Error rotating keys: {e}", ephemeral=True)
# ...existing code...

# === Entry Point ===
if __name__ == "__main__":
    if not TOKEN:
        logging.error("❌ DISCORD_TOKEN not set. Bot will not start.")
        raise SystemExit(1)
    try:
        # Optional keep-alive (only if hosted on services requiring a ping)
        try:
            keep_alive()
        except Exception as e:
            logging.warning(f"Keep-alive server failed to start: {e}")
        logging.info("🚀 Starting Discord bot run loop…")
        bot.run(TOKEN)
    except KeyboardInterrupt:
        logging.info("🛑 Interrupted by user")
    except Exception as e:
        logging.error(f"❌ Unhandled exception in bot run: {e}")
        raise