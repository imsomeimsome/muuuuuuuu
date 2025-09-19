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
    get_spotify_key_status,
    validate_spotify_client,
    ping_spotify,
    get_latest_featured_release as get_spotify_latest_featured_release  # NEW
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
    """Handle multiple date formats consistently (normalized for ordering, not display).
       Date-only values are anchored at 12:00 UTC (not 23:59:59) to avoid artificial 'future' drift."""
    if not date_str:
        return datetime.min.replace(tzinfo=timezone.utc)
    try:
        # Pure date (YYYY-MM-DD)
        if len(date_str) == 10 and date_str.count('-') == 2 and 'T' not in date_str:
            dt = datetime.strptime(date_str, '%Y-%m-%d').replace(tzinfo=timezone.utc)
            # Anchor at midday UTC so every timezone still maps to the same calendar date
            return dt + timedelta(hours=12)
        if 'T' in date_str:
            ds = date_str.replace('Z', '+00:00')
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

def _next_hour_boundary(second: int = 1) -> datetime:
    """
    Next hourly boundary at HH:00:01 UTC anchored from today's 00:00:01.
    """
    now = datetime.now(timezone.utc)
    anchor = now.replace(hour=0, minute=0, second=second, microsecond=0)  # 00:00:01 today
    if now < anchor:
        return anchor
    hours_since_anchor = (now - anchor).seconds // 3600
    # If we're before the :second inside this hour (e.g. 12:00:00.500 and second=1), stay on current upcoming boundary
    if now.minute == 0 and now.second < second:
        return now.replace(minute=0, second=second, microsecond=0)
    next_hour_index = hours_since_anchor + 1
    return anchor + timedelta(hours=next_hour_index)

def _next_5min_boundary(second: int = 1) -> datetime:
    """
    Next 5‑minute boundary at mm divisible by 5 with :01 seconds,
    anchored from today's 00:00:01 UTC.
    """
    now = datetime.now(timezone.utc)
    anchor = now.replace(hour=0, minute=0, second=second, microsecond=0)  # 00:00:01 today
    if now < anchor:
        return anchor
    elapsed = now - anchor
    elapsed_minutes = int(elapsed.total_seconds() // 60)
    current_slot_min = (elapsed_minutes // 5) * 5
    current_boundary = anchor + timedelta(minutes=current_slot_min)
    # If we are in the exact slot minute but before the target second, use this boundary
    if (elapsed_minutes % 5 == 0) and now.second < second:
        return current_boundary
    # Otherwise move to the next slot
    return current_boundary + timedelta(minutes=5)

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
    logging.info(f"🔎 Looking for release channel: Guild ID={guild_id} Platform={platform}")
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

    heading_result = create_music_embed(
        platform=platform,
        artist_name=release_info.get('artist_name', artist['artist_name']),
        title=release_info.get('title', 'New Release'),
        url=release_info.get('url', artist['artist_url']),
        release_date=release_info.get('release_date') if release_info.get('release_date') else "Unknown",
        cover_url=release_info.get('cover_url'),
        features=release_info.get('features'),
        track_count=release_info.get('track_count'),
        duration=release_info.get('duration'),
        repost=False,
        genres=release_info.get('genres'),
        content_type=release_info.get('type'),
        return_heading=True
    )
    heading_text, release_type_detected, embed = heading_result

    # Only make a big heading for non-playlist releases (tracks/albums/EPs/deluxe)
    if (release_info.get('type') or release_type_detected).lower() == 'playlist':
        await channel.send(embed=embed)
    else:
        await channel.send(content=f"# {heading_text}", embed=embed)

    logging.info(f"✅ Posted new {release_type} for {artist['artist_name']}")

async def check_for_playlist_changes(bot, artist, playlist_info):
    artist_id = artist["artist_id"]
    guild_id = artist["guild_id"]
    playlist_id = playlist_info["url"]

    stored_state = get_playlist_state(artist_id, guild_id, playlist_id)
    if isinstance(stored_state, dict):
        old_title = stored_state.get('title')
        stored_tracks = stored_state.get('tracks', [])
    else:
        old_title = None
        stored_tracks = stored_state

    current_tracks = playlist_info["tracks"]

    if not stored_tracks:
        # First time tracking this playlist
        store_playlist_state(artist_id, guild_id, playlist_id, current_tracks, playlist_info.get('title'))
        logging.info(f"✅ Stored initial state for playlist: {playlist_info['title']}")
        return

    # Detect changes
    added_tracks = [t for t in current_tracks if t not in stored_tracks]
    removed_tracks = [t for t in stored_tracks if t not in current_tracks]
    order_changed = any(
        track["order"] != stored_tracks[index]["order"]
        for index, track in enumerate(current_tracks)
        if index < len(stored_tracks)
    )
    title_changed = old_title is not None and old_title != playlist_info.get('title')

    if added_tracks or removed_tracks or order_changed or title_changed:
        logging.info(f"✨ Playlist changes detected for {playlist_info['title']}")
        embed = discord.Embed(
            title=f"📝 Playlist Updated: {playlist_info['title']}",
            url=playlist_info["url"],
            description="Changes detected in playlist:",
            color=discord.Color.orange()
        )
        if title_changed:
            embed.add_field(name="Renamed", value=f"{old_title} ➜ {playlist_info['title']}", inline=False)
        if added_tracks:
            embed.add_field(
                name="Added Tracks",
                value="\n".join([trk["title"] for trk in added_tracks])[:1024],
                inline=False
            )
        if removed_tracks:
            embed.add_field(
                name="Removed Tracks",
                value="\n".join([trk["title"] for trk in removed_tracks])[:1024],
                inline=False
            )
        if order_changed and not (added_tracks or removed_tracks):
            embed.add_field(name="Order Changed", value="Track order was modified.", inline=False)

        # Add highest quality artwork for playlist change notifications
        cover_url = playlist_info.get('cover_url')
        if cover_url:
            try:
                from utils import get_highest_quality_artwork
                high_res = get_highest_quality_artwork(cover_url)
                embed.set_thumbnail(url=high_res or cover_url)
            except Exception:
                embed.set_thumbnail(url=cover_url)

        channel = await get_release_channel(guild_id, "soundcloud")
        if channel:
            await channel.send(embed=embed)

# --- MAIN RELEASE CHECK FUNCTION WITH CATCH-UP ---
async def check_for_new_releases(bot, is_catchup=False):
    """Coordinate all platform checks with per-platform timeout watchdog."""
    PLATFORM_PHASE_TIMEOUT = int(os.getenv('PLATFORM_PHASE_TIMEOUT', '120'))

    now_utc = datetime.now(timezone.utc)
    run_spotify = (now_utc.minute == 0)  # only at HH:00:01
    logging.info(f"\n🚀 Starting check for new releases (UTC {now_utc:%Y-%m-%d %H:%M:%S})")
    if is_catchup:
        logging.info("(Catch-up mode)")
    if not run_spotify:
        logging.info("⏭️ Skipping Spotify this cycle (hourly cadence)")

    # General tasks
    artists, shutdown_time, general_errors = await check_general_tasks(bot, is_catchup)
    if not artists:
        logging.warning("No artists available; aborting cycle early.")
        return

    spotify_releases, spotify_errors = 0, []
    if run_spotify:
        logging.info("▶️ Starting Spotify phase")
        try:
            if getattr(spotify_utils, 'spotify_key_manager', None) and spotify_utils.spotify_key_manager.keys:
                _cid, _sec = spotify_utils.spotify_key_manager.get_current_key()
                logging.warning("Starting Spotify Phase using....\nclient_id: %s\nclient_secret: %s" % (_cid, _sec))
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
            spotify_releases, spotify_errors = spotify_results
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

    # --- SoundCloud Phase (always every 5m) ---
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

    total_releases = spotify_releases + sum(soundcloud_counts.values())
    all_errors = (general_errors or []) + spotify_errors + soundcloud_errors
    logging.info("🎯 All platform checks finished successfully!")
    unique_checked = len({(a.get('platform'), a.get('artist_id')) for a in (artists or [])})
    await log_summary(unique_checked, total_releases, all_errors)

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

def _subscribers_for(artists, platform: str, artist_id: str):
    """Return all rows (guild subscriptions) tracking this artist on this platform."""
    return [a for a in (artists or []) if a.get('platform') == platform and a.get('artist_id') == artist_id]

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
    cycle_dedupe = set()  # (album_id, release_date)
    logging.info(f"\n🟢 CHECKING SPOTIFY{' (CATCH-UP)' if is_catchup else ''}…")
    logging.info("="*50)
    for artist in artists:
        if artist.get('platform') != 'spotify':
            continue
        artist_name = artist.get('artist_name','unknown')
        artist_id = artist.get('artist_id')
        owner_id = artist.get('owner_id')
        guild_id = artist.get('guild_id')

        # Validate artist_id early; try to extract from URL/URI/raw ID if missing/invalid
        if not artist_id or str(artist_id).lower() in ("none", "null", ""):
            candidate = artist.get('artist_url') or artist.get('url') or artist.get('artist_id') or ''
            try:
                from spotify_utils import extract_spotify_id  # local import avoids top-level cycles
                recovered = extract_spotify_id(candidate)
            except Exception:
                recovered = None
            artist_id = recovered

        if not artist_id:
            logging.error(f"❌ Missing Spotify artist_id for {artist_name}; skipping artist")
            update_last_release_check(artist.get('artist_id') or 'unknown', owner_id, guild_id, datetime.now(timezone.utc).isoformat())
            continue
        # Parse stored timestamps
        last_release_check = get_last_release_check(artist_id, owner_id, guild_id)
        last_release_date_raw = artist.get('last_release_date')
        try:
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

            # Determine if this is the artist's first ever cycle (baseline)
            is_baseline = last_check_dt is None

            # Force fresh fetch on baseline or if release date is today's date (to avoid stale cached IDs)
            if is_baseline or (api_release_date[:10] == datetime.now(timezone.utc).strftime("%Y-%m-%d")):
                try:
                    latest_album_id = await run_blocking(get_spotify_latest_album_id, artist_id, True)
                    if latest_album_id:
                        release_info_fresh = await run_blocking(get_spotify_release_info, latest_album_id, True)
                        if release_info_fresh and release_info_fresh.get('release_date'):
                            release_info = release_info_fresh
                            api_release_date = release_info.get('release_date')
                            api_dt = parse_date(api_release_date)
                            logging.info("     🔄 Forced fresh fetch (baseline/today)")

                except Exception as fr_e:
                    logging.debug(f"     Fresh fetch skipped: {fr_e}")

            # NEW: Treat baseline as eligible for posting (no skip)
            # We only skip if api_dt is None
            if not api_dt:
                logging.info("     ⏭️ Skipping (no valid api_dt)")
                update_last_release_check(artist_id, owner_id, guild_id, batch_check_time)
                continue

            # Drift correction (stored future vs API)
            if last_release_dt and api_dt and last_release_dt > api_dt:
                logging.warning(f"     ⚠️ Stored last_release_date {_fmt_dt(last_release_dt)} > API newest {_fmt_dt(api_dt)}; correcting.")
                update_last_release_date(artist_id, owner_id, guild_id, api_release_date)
                last_release_dt = api_dt

            def _newer(a, b):
                return a is not None and (b is None or a > b)

            if _newer(api_dt, last_check_dt):
                album_id = release_info.get('album_id')
                cache_key_global = f"posted_spotify:{artist_id}:{album_id}:{api_release_date}"
                if get_cache(cache_key_global):
                    logging.info("     ⏭️ Duplicate suppressed (cache)")
                elif (album_id, api_release_date) in cycle_dedupe:
                    logging.info("     ⏭️ Duplicate suppressed (cycle memory)")
                else:
                    heading_text, release_type_detected, embed = create_music_embed(
                        platform='spotify',
                        artist_name=release_info.get('artist_name', artist_name),
                        title=release_info.get('title','New Release'),
                        url=release_info.get('url'),
                        release_date=api_release_date,
                        cover_url=release_info.get('cover_url'),
                        features=release_info.get('features'),
                        track_count=release_info.get('track_count'),
                        duration=release_info.get('duration'),
                        genres=release_info.get('genres'),
                        repost=False,
                        content_type=release_info.get('type'),
                        return_heading=True
                    )

                    posted_any = False
                    for sub in _subscribers_for(artists, 'spotify', artist_id):
                        sub_gid = sub.get('guild_id'); sub_oid = sub.get('owner_id')
                        channel = await get_release_channel(sub_gid, 'spotify')
                        if not channel:
                            logging.info(f"      - guild id = {sub_gid} - no channel")
                            continue
                        try:
                            await channel.send(embed=embed)
                            update_last_release_date(artist_id, sub_oid, sub_gid, api_release_date)
                            posted_any = True
                            logging.info(f"      - guild id = {sub_gid} - posted")
                        except Exception as se:
                            logging.error(f"      - guild id = {sub_gid} - send failed: {se}")

                    if posted_any:
                        set_cache(cache_key_global, '1', ttl=86400)
                        cycle_dedupe.add((album_id, api_release_date))
                        releases += 1
                        logging.info(f"     ✅ NEW (api_release_date {_fmt_dt(api_dt)} > last_check {_fmt_dt(last_check_dt) if last_check_dt else 'None'})")
                    else:
                        logging.info("     ⚠️ Not posted anywhere (no channels configured)")
                # Always update last_check after evaluation
                update_last_release_check(artist_id, owner_id, guild_id, batch_check_time)
                continue  # skip feature block if main release handled

            # Not newer → proceed to feature logic
            update_last_release_check(artist_id, owner_id, guild_id, batch_check_time)
            # (feature logic continues unchanged below)

            # Only reach feature logic if not a new main release
            # === NEW: Featured-on detection (one fetch, broadcast per guild) ===
            try:
                feat_info = await run_blocking(get_spotify_latest_featured_release, artist_id)
            except Exception as e_feat:
                feat_info = None
                logging.debug(f"     (feature) fetch skipped/failed: {e_feat}")

            if feat_info and feat_info.get('release_date'):
                feat_dt = parse_date(feat_info['release_date'])
                logging.info(f"     (feature) API returned: {_fmt_dt(feat_dt)}")
                # Fan-out per guild using each guild's last_check
                found_any = False
                for sub in _subscribers_for(artists, 'spotify', artist_id):
                    sub_gid = sub.get('guild_id'); sub_oid = sub.get('owner_id')
                    sub_last = get_last_release_check(artist_id, sub_oid, sub_gid)
                    sub_last_dt = parse_date(sub_last) if sub_last else None
                    if sub_last_dt is None or not feat_dt or feat_dt <= sub_last_dt:
                        continue
                    cache_key_feat = f"posted_spotify_feature:{sub_gid}:{artist_id}:{feat_info.get('album_id')}:{feat_info['release_date']}"
                    if get_cache(cache_key_feat):
                        continue
                    channel = await get_release_channel(sub_gid, 'spotify')
                    if not channel:
                        continue
                    # Build base embed then override heading and add "By"
                    heading_text, release_type, embed = create_music_embed(
                        platform='spotify',
                        artist_name=artist_name,  # tracked artist
                        title=feat_info.get('title','New Release'),
                        url=feat_info.get('url'),
                        release_date=feat_info.get('release_date'),
                        cover_url=feat_info.get('cover_url'),
                        features=feat_info.get('features'),   # already formatted with __Tracked__
                        track_count=feat_info.get('track_count'),
                        duration=feat_info.get('duration'),
                        genres=feat_info.get('genres',[]),
                        repost=False,
                        return_heading=True
                    )
                    custom_heading = f"➕ {artist_name} is featured on a {release_type}!"
                    main_artist = (feat_info.get('main_artist_name') or "").strip() or "Unknown"
                    # If tracked artist IS the main artist, skip duplicate feature embed
                    if artist_name.lower() == main_artist.lower():
                        continue
                    embed.title = custom_heading
                    embed.add_field(name="By", value=f"{main_artist}, {artist_name}", inline=True)
                    # Reorder fields so "By" appears first
                    try:
                        fields = list(embed.fields)
                        by_field = fields[-1]
                        embed._fields = [by_field] + fields[:-1]
                    except Exception:
                        pass

                    # Dedupe before sending
                    album_id = feat_info.get('album_id')
                    feat_release_date = feat_info.get('release_date')
                    feat_key = f"posted_spotify:{artist_id}:{album_id}:{feat_release_date}"
                    if get_cache(feat_key) or (album_id, feat_release_date) in cycle_dedupe:
                        logging.info("     ⏭️ Featured duplicate suppressed")
                        continue

                    posted_any = False
                    for sub in _subscribers_for(artists, 'spotify', artist_id):
                        sub_gid = sub.get('guild_id'); sub_oid = sub.get('owner_id')
                        channel = await get_release_channel(sub_gid, 'spotify')
                        if not channel:
                            continue
                        try:
                            await channel.send(embed=embed)
                            # Do NOT update last_release_date (featured doesn't represent artist's own drop)
                            posted_any = True
                        except Exception as se:
                            logging.error(f"      - featured send failed guild={sub_gid}: {se}")
                    if posted_any:
                        set_cache(feat_key, '1', ttl=86400)
                        cycle_dedupe.add((album_id, feat_release_date))
                        releases += 1
                if found_any:
                    releases += 1  # count as a new event surfaced
            # === end featured-on block ===

            update_last_release_check(artist_id, owner_id, guild_id, batch_check_time)
        except Exception as e:
            logging.error(f"     ❌ Error for {artist_name}: {e}")
            errors.append({'type':'Spotify','message':str(e)})
            # Only mark last_release_check if we successfully got api_release_date earlier in loop
            try:
                if 'api_release_date' in locals() and api_release_date:
                    update_last_release_check(artist_id, owner_id, guild_id, batch_check_time)
                else:
                    logging.info("     ⚠️ Skipping last_release_check update due to failure before fetch completion")
            except Exception:
                pass
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

    FRESH_SKEW_HOURS = int(os.getenv('SC_FRESH_SKEW_HOURS', '12'))

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

            # Fetch both first so we can decide about suppression
            release_info = await run_blocking(get_soundcloud_release_info, artist_url, True)
            playlist_info = None
            try:
                playlist_info = await run_blocking(get_soundcloud_playlist_info, artist_url, True)
            except Exception as e_pl:
                playlist_info = None
                logging.error(f"     ❌ Playlist fetch error: {e_pl}")
                errors.append({'type':'SoundCloud Playlist','message':str(e_pl)})

            last_release_dt = parse_date(last_release_date_raw) if last_release_date_raw else None
            last_playlist_dt = parse_date(last_playlist_date_raw) if last_playlist_date_raw else None

            # Current cycle last_check_dt already parsed above
            baseline = last_check_dt is None

            # Determine playlist newness first
            playlist_new = False
            playlist_reason = None
            playlist_dt = None
            playlist_date_raw = None
            if playlist_info:
                playlist_date_raw = playlist_info.get('release_date')
                playlist_dt = parse_date(playlist_date_raw) if playlist_date_raw else None
                _log_header(artist_name, 'playlist', last_playlist_dt, last_check_dt)
                logging.info(f"     API returned (playlist): {_fmt_dt(playlist_dt)}")
                if baseline:
                    logging.info("     ⏭️ Baseline established (no previous check)")
                elif playlist_dt:
                    # Primary comparison vs last check
                    if _is_new_activity(playlist_dt, last_check_dt):
                        playlist_new = True
                        playlist_reason = "playlist_date > last_check"
                    else:
                        # Skew fallback: newer than last stored playlist but backdated <= last_check
                        if (last_playlist_dt is None or playlist_dt > last_playlist_dt):
                            now_utc = datetime.now(timezone.utc)
                            age = now_utc - (playlist_dt if playlist_dt.tzinfo else playlist_dt.replace(tzinfo=timezone.utc))
                            if age < timedelta(hours=FRESH_SKEW_HOURS):
                                playlist_new = True
                                playlist_reason = f"fresh_skew_window (<{FRESH_SKEW_HOURS}h & > last_playlist)"
                    if not playlist_new:
                        if last_check_dt and playlist_dt == last_check_dt:
                            logging.info("     ⏭️ Not new (same timestamp)")
                        else:
                            logging.info(f"     ⏭️ Not new (playlist_date {_fmt_dt(playlist_dt)} <= last_check {_fmt_dt(last_check_dt)})")
                else:
                    logging.info("     ⏭️ No playlist date present")
            else:
                _log_header(artist_name, 'playlist', last_playlist_dt, last_check_dt)
                logging.info("     API returned (playlist): None")
                logging.info("     ⏭️ No playlists returned")

            # Post playlist if NEW (after potential suppression decision)
            if playlist_new:
                # Guard: skip placeholder single-track zero-duration playlists until real duration is available
                if playlist_info.get('pending_zero_duration'):
                    logging.info("     ⏭️ Skipping placeholder playlist (single track, zero duration) – will retry next cycle")
                    playlist_new = False
                else:
                    playlist_id = playlist_info.get('url') or f"playlist_{artist_id}_{playlist_date_raw}"
                    # (existing code continues)
            if playlist_new:
                playlist_id = playlist_info.get('url') or f"playlist_{artist_id}_{playlist_date_raw}"
                # existing posting logic remains unchanged
                playlist_id = playlist_info.get('url') or f"playlist_{artist_id}_{playlist_date_raw}"
                # ...
                logging.info("     Playlist found! Looking for channels to post in")
                for sub in _subscribers_for(artists, 'soundcloud', artist_id):
                    sub_gid = sub.get('guild_id')
                    if is_already_posted_playlist(artist_id, sub_gid, playlist_id):
                        continue
                    channel = await get_release_channel(sub_gid, 'soundcloud')
                    if channel:
                        logging.info(f"      - guild id = {sub_gid} - found")
                        await handle_release(bot, sub, playlist_info, 'playlist')
                        mark_posted_playlist(artist_id, sub_gid, playlist_id)
                        update_last_playlist_date(artist_id, sub_gid, playlist_date_raw)
                        counts['playlists'] += 1
                    else:
                        logging.info(f"      - guild id = {sub_gid} - not found")
                logging.info(f"     ✅ NEW ({playlist_reason}; playlist_date {_fmt_dt(playlist_dt)} vs last_check {_fmt_dt(last_check_dt)})")

            # Process release (single track) but allow suppression if parent playlist will post
            api_release_date = release_info.get('release_date') if release_info else None
            api_dt = parse_date(api_release_date) if api_release_date else None
            _log_header(artist_name, 'release', last_release_dt, last_check_dt)
            logging.info(f"     API returned (release): {_fmt_dt(api_dt)}" if api_dt else ("     API returned: None" if release_info is None else "     API returned: <missing release_date>"))

            suppress_track = False
            if release_info and playlist_new and playlist_info:
                # Check membership: compare track id against playlist tracks
                track_id = str(release_info.get('track_id') or release_info.get('id') or '')
                playlist_track_ids = {str(t.get('id')) for t in (playlist_info.get('tracks') or []) if t.get('id') is not None}
                if track_id and track_id in playlist_track_ids:
                    suppress_track = True

            if suppress_track:
                logging.info("     ⏭️ Suppressing single track (included in NEW playlist/album being posted this cycle)")
            else:
                if not release_info:
                    logging.info("     ⏭️ No releases returned")
                elif not api_release_date:
                    logging.info("     ⏭️ Skipping (no release_date)")
                elif baseline:
                    logging.info("     ⏭️ Baseline established (no previous check)")
                elif api_dt:
                    is_new = False
                    reason = None
                    if _is_new_activity(api_dt, last_check_dt):
                        is_new = True
                        reason = "api_release_date > last_check"
                    else:
                        # Skew fallback: created_at earlier than last_check but still newer than last_release
                        if (last_release_dt is None or api_dt > last_release_dt):
                            now_utc = datetime.now(timezone.utc)
                            age = now_utc - api_dt if api_dt.tzinfo else now_utc - api_dt.replace(tzinfo=timezone.utc)
                            if age < timedelta(hours=FRESH_SKEW_HOURS):
                                is_new = True
                                reason = f"fresh_skew_window (<{FRESH_SKEW_HOURS}h & > last_release)"
                    if is_new:
                        cache_key = f"posted_sc:{artist_id}:{release_info.get('url')}:{api_release_date}"
                        if get_cache(cache_key):
                            logging.info(f"     ⏭️ Duplicate suppressed ({reason})")
                        else:
                            # Build once then broadcast to all guilds tracking this artist
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
                                repost=False,
                                upload_date=release_info.get('upload_date')  # NEW
                            )
                            logging.info("     Release found! Looking for channels to post in")
                            for sub in _subscribers_for(artists, 'soundcloud', artist_id):
                                sub_gid = sub.get('guild_id'); sub_oid = sub.get('owner_id')
                                channel = await get_release_channel(sub_gid, 'soundcloud')
                                if channel:
                                    logging.info(f"      - guild id = {sub_gid} - found")
                                    await channel.send(embed=embed)
                                    update_last_release_date(artist_id, sub_oid, sub_gid, api_release_date)
                                else:
                                    logging.info(f"      - guild id = {sub_gid} - not found")
                            set_cache(cache_key, 'posted', ttl=86400)
                            counts['releases'] += 1
                            logging.info(f"     ✅ NEW ({reason}; api_release_date {_fmt_dt(api_dt)} vs last_check {_fmt_dt(last_check_dt)})")
                    else:
                        if api_dt and last_check_dt and api_dt == last_check_dt:
                            logging.info("     ⏭️ Not new (same timestamp)")
                        else:
                            logging.info(f"     ⏭️ Not new (api_release_date {_fmt_dt(api_dt)} <= last_check {_fmt_dt(last_check_dt)})")

            # === REPOSTS (multiple) ===
            try:
                # Force refresh to avoid stale 5‑minute cache causing 2-cycle delay
                reposts = await run_blocking(get_soundcloud_reposts_info, artist_url, True)
            except Exception as e_repost:
                reposts = []
                logging.error(f"     ❌ Error processing reposts for {artist_name}: {e_repost}")
                errors.append({'type':'SoundCloud Reposts','message':str(e_repost)})
            last_repost_dt_stored = parse_date(last_repost_date_raw) if last_repost_date_raw else None
            logging.info(f"     🔄 Reposts returned: {len(reposts)}")
            _log_header(artist_name, 'repost', last_repost_dt_stored, last_check_dt)

            # Sort newest first
            def _rd(r): return r.get('reposted_date') or ''
            reposts_sorted = sorted(reposts, key=_rd, reverse=True)

            INITIAL_REPOST_LIMIT = 1  # how many to post on very first cycle (baseline)
            posted_initial = 0

            if last_check_dt is None:
                logging.info("     🟡 First repost cycle (baseline) — evaluating newest repost(s)")
                for repost in reposts_sorted:
                    if posted_initial >= INITIAL_REPOST_LIMIT:
                        logging.info("          ⏭️ Baseline limit reached")
                        break
                    repost_id = (
                        str(repost.get('track_id') or '') or
                        (repost.get('url') or '') or
                        repost.get('title') or ''
                    )
                    if not repost_id:
                        continue
                    repost_activity_date = parse_date(repost.get('reposted_date')) if repost.get('reposted_date') else None
                    logging.info(f"          🔄 Repost (baseline): {repost.get('title')} -> {_fmt_dt(repost_activity_date)}")
                    if not repost_activity_date:
                        continue
                    if is_already_posted_repost(artist_id, guild_id, repost_id):
                        logging.info("              ⏭️ Already posted (baseline)")
                        continue
                    # Broadcast to all guilds tracking this artist (baseline)
                    for sub in _subscribers_for(artists, 'soundcloud', artist_id):
                        sub_gid = sub.get('guild_id')
                        if is_already_posted_repost(artist_id, sub_gid, repost_id):
                            continue
                        channel = await get_release_channel(sub_gid, 'soundcloud')
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
                                content_type=repost.get('content_type'),
                                upload_date=repost.get('upload_date')  # NEW
                            )
                            await channel.send(embed=embed)
                            mark_posted_repost(artist_id, sub_gid, repost_id)
                            update_last_repost_date(artist_id, sub_gid, repost.get('reposted_date'))
                            counts['reposts'] += 1
                            posted_initial += 1
                    logging.info("              ✅ NEW (baseline repost broadcast complete)")
                if posted_initial == 0:
                    logging.info("     ⏭️ Baseline established (no repost posted)")
            else:
                # Regular cycle (compare reposted_date > last_check_dt)
                for repost in reposts_sorted:
                    repost_id = (
                        str(repost.get('track_id') or '') or
                        (repost.get('url') or '') or
                        repost.get('title') or ''
                    )
                    if not repost_id:
                        continue
                    repost_activity_date = parse_date(repost.get('reposted_date')) if repost.get('reposted_date') else None
                    logging.info(f"          🔄 Repost: {repost.get('title')} -> {_fmt_dt(repost_activity_date)}")
                    if not repost_activity_date:
                        continue
                    if is_already_posted_repost(artist_id, guild_id, repost_id):
                        logging.info("              ⏭️ Already posted")
                        continue
                    # Broadcast per guild using each guild's last_check
                    for sub in _subscribers_for(artists, 'soundcloud', artist_id):
                        sub_gid = sub.get('guild_id'); sub_oid = sub.get('owner_id')
                        sub_last = get_last_release_check(artist_id, sub_oid, sub_gid)
                        sub_last_dt = parse_date(sub_last) if sub_last else None
                        if not _is_new_activity(repost_activity_date, sub_last_dt):
                            continue
                        if is_already_posted_repost(artist_id, sub_gid, repost_id):
                            continue
                        channel = await get_release_channel(sub_gid, 'soundcloud')
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
                                content_type=repost.get('content_type'),
                                upload_date=repost.get('upload_date')  # NEW
                            )
                            await channel.send(embed=embed)
                            mark_posted_repost(artist_id, sub_gid, repost_id)
                            update_last_repost_date(artist_id, sub_gid, repost.get('reposted_date'))
                            counts['reposts'] += 1
                    # keep existing else-logging for not-new

            # === LIKES (multiple) ===
            try:
                likes = await run_blocking(get_soundcloud_likes_info, artist_url)
            except Exception as e_likes:
                likes = []
                logging.error(f"     ❌ Error processing likes for {artist_name}: {e_likes}")
                errors.append({'type':'SoundCloud Likes','message':str(e_likes)})

            # --- NEW: Likes processing (previously missing) ---
            last_like_dt_stored = parse_date(last_like_date_raw) if last_like_date_raw else None
            _log_header(artist_name, 'like', last_like_dt_stored, last_check_dt)
            logging.info(f"     Likes returned: {len(likes)}")

            def _ld(l): return l.get('liked_date') or ''
            likes_sorted = sorted(likes, key=_ld, reverse=True)

            LIKE_BASELINE_LIMIT = int(os.getenv('SC_LIKE_BASELINE_LIMIT', '1'))
            posted_like_baseline = 0

            if last_check_dt is None:
                # Baseline cycle: post newest like(s) up to limit to establish state
                if likes_sorted:
                    logging.info("     🟡 First like cycle (baseline) — evaluating newest like(s)")
                for like in likes_sorted:
                    if posted_like_baseline >= LIKE_BASELINE_LIMIT:
                        logging.info("          ⏭️ Baseline like limit reached")
                        break
                    like_id = str(like.get('track_id') or like.get('url') or like.get('title') or '')
                    if not like_id:
                        continue
                    liked_activity_date = parse_date(like.get('liked_date')) if like.get('liked_date') else None
                    logging.info(f"          ❤️ Like (baseline): {like.get('title')} -> {_fmt_dt(liked_activity_date)}")
                    if not liked_activity_date:
                        continue
                    if is_already_posted_like(artist_id, guild_id, like_id):
                        logging.info("              ⏭️ Already posted (baseline)")
                        continue
                    for sub in _subscribers_for(artists, 'soundcloud', artist_id):
                        sub_gid = sub.get('guild_id')
                        if is_already_posted_like(artist_id, sub_gid, like_id):
                            continue
                        channel = await get_release_channel(sub_gid, 'soundcloud')
                        if channel:
                            embed = create_like_embed(
                                platform='soundcloud',
                                liked_by=artist_name,
                                original_artist=like.get('artist_name'),
                                title=like.get('title'),
                                url=like.get('url'),
                                release_date=like.get('release_date'),
                                liked_date=like.get('liked_date'),
                                cover_url=like.get('cover_url'),
                                features=like.get('features'),
                                track_count=like.get('track_count'),
                                duration=like.get('duration'),
                                genres=like.get('genres'),
                                content_type=like.get('content_type'),
                                upload_date=like.get('upload_date')  # NEW
                            )
                            await channel.send(embed=embed)
                            mark_posted_like(artist_id, sub_gid, like_id)
                            update_last_like_date(artist_id, sub_gid, like.get('liked_date'))
                            counts['likes'] += 1
                            posted_like_baseline += 1
                if posted_like_baseline == 0:
                    logging.info("     ⏭️ Baseline established (no like posted)")
            else:
                # Normal cycle: post likes where liked_date > last_check_dt
                for like in likes_sorted:
                    like_id = str(like.get('track_id') or like.get('url') or like.get('title') or '')
                    if not like_id:
                        continue
                    liked_activity_date = parse_date(like.get('liked_date')) if like.get('liked_date') else None
                    logging.info(f"          ❤️ Like: {like.get('title')} -> {_fmt_dt(liked_activity_date)}")
                    if not liked_activity_date:
                        continue
                    if is_already_posted_like(artist_id, guild_id, like_id):
                        logging.info("              ⏭️ Already posted")
                        continue
                    for sub in _subscribers_for(artists, 'soundcloud', artist_id):
                        sub_gid = sub.get('guild_id'); sub_oid = sub.get('owner_id')
                        sub_last = get_last_release_check(artist_id, sub_oid, sub_gid)
                        sub_last_dt = parse_date(sub_last) if sub_last else None
                        if not _is_new_activity(liked_activity_date, sub_last_dt):
                            continue
                        if is_already_posted_like(artist_id, sub_gid, like_id):
                            continue
                        channel = await get_release_channel(sub_gid, 'soundcloud')
                        if channel:
                            embed = create_like_embed(
                                platform='soundcloud',
                                liked_by=artist_name,
                                original_artist=like.get('artist_name'),
                                title=like.get('title'),
                                url=like.get('url'),
                                release_date=like.get('release_date'),
                                liked_date=like.get('liked_date'),
                                cover_url=like.get('cover_url'),
                                features=like.get('features'),
                                track_count=like.get('track_count'),
                                duration=like.get('duration'),
                                genres=like.get('genres'),
                                content_type=like.get('content_type'),
                                upload_date=like.get('upload_date')  # NEW
                            )
                            await channel.send(embed=embed)
                            mark_posted_like(artist_id, sub_gid, like_id)
                            update_last_like_date(artist_id, sub_gid, like.get('liked_date'))
                            counts['likes'] += 1
                    # keep existing else-logging for not-new
        except Exception as e:
            logging.error(f"     ❌ Unhandled SoundCloud artist error for {artist_name}: {e}")
            errors.append({'type':'SoundCloud Artist','message':f'{artist_name}: {e}'})
            # keep existing best-effort update (ok to keep)
            try:
                update_last_release_check(artist_id, owner_id, guild_id, batch_check_time)
            except Exception:
                pass
            continue
        finally:
            # ALWAYS mark the check time for this artist (success or not)
            try:
                update_last_release_check(artist_id, owner_id, guild_id, batch_check_time)
            except Exception as up_e:
                logging.debug(f"update_last_release_check failed for {artist_name}: {up_e}")
    return counts, errors

CHECK_INTERVAL_MIN = int(os.getenv("CHECK_INTERVAL_MIN", "5"))

@tasks.loop(minutes=CHECK_INTERVAL_MIN)
async def release_checker():
    try:
        await check_for_new_releases(bot)
    except Exception as e:
        logging.error(f"Release checker loop error: {e}")

@release_checker.before_loop
async def _before_release_checker():
    await bot.wait_until_ready()
    target = _next_5min_boundary(second=1)
    while True:
        now = datetime.now(timezone.utc)
        delta = (target - now).total_seconds()
        if delta <= 0:
            break
        await asyncio.sleep(min(delta, 30))
    logging.info(f"⏱️ Aligned release checker to first run at {target.isoformat()}Z (then every {CHECK_INTERVAL_MIN}m)")

@bot.event
async def on_ready():
    if not getattr(bot, "release_checker_started", False):
        logging.info(f"🤖 Logged in as {bot.user} (starting release checker every {CHECK_INTERVAL_MIN} min)")
        try:
            catchup = await handle_bot_startup_catchup()
        except Exception as e:
            logging.error(f"Catch-up init failed: {e}")
            catchup = False
        await bot.start_health_logger()
        # Immediate first cycle (normal)
        try:
            await check_for_new_releases(bot, is_catchup=False)
        except Exception as e:
            logging.error(f"Initial release check failed: {e}")
        # Optional catch-up pass
        if (catchup):
            try:
                await check_for_new_releases(bot, is_catchup=True)
            except Exception as e:
                logging.error(f"Catch-up cycle failed: {e}")
        release_checker.start()
        bot.release_checker_started = True
        logging.info("✅ Release checker loop started")

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

    embed.add_field(name="📀 Deluxe", value="7 or more tracks released together or marked as album/mixtape.", inline=False)
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

@bot.tree.command(name="testcache", description="Test SQLite cache.")
async def test_cache_command(interaction: discord.Interaction):
    try:
        set_cache("test_key", "test_value", ttl=60)
        value = get_cache("test_key")
        await interaction.response.send_message(f"✅ Cache is working. Test value: {value}")
    except Exception as e:
        await interaction.response.send_message(f"❌ Cache error: {e}")
    
@bot.tree.command(name="checkscid", description="Verify SoundCloud client ID is valid")
@require_registration
async def check_scid_command(interaction: discord.Interaction):
    from soundcloud_utils import verify_client_id, refresh_client_id
    await interaction.response.defer(ephemeral=True)
    try:
        if verify_client_id():
            await interaction.followup.send("✅ SoundCloud client ID appears valid.")
            return
        new_client_id = refresh_client_id()
        if new_client_id:
            await interaction.followup.send(f"✅ Refreshed SoundCloud client ID: `{new_client_id}`")
        else:
            await interaction.followup.send("❌ Failed to refresh SoundCloud client ID. Try again later or set it manually.")
    except Exception as e:
        # Ensure the command never raises
        await interaction.followup.send(f"❌ Check failed: {e}")

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

@bot.tree.command(name="export", description="Export your list of tracked artists.")
@require_registration
async def export_command(interaction: discord.Interaction):
    user_id = str(interaction.user.id)
    artists = get_artists_by_owner(user_id)
    if not artists:
        await interaction.response.send_message("📭 You aren't currently tracking any artists.")
        return

    # Build JSON payload compatible with /import (database_utils.import_artists_from_json)
    payload = []
    for a in artists:
        payload.append({
            "platform": a.get("platform"),
            "artist_id": a.get("artist_id"),
            "artist_name": a.get("artist_name"),
            "artist_url": a.get("artist_url"),
            "genres": a.get("genres") or [],
            "last_release_date": a.get("last_release_date"),
        })

    filename = f"tracked_artists_{user_id}.json"
    with open(filename, "w", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=False, indent=2))

    file = discord.File(filename, filename=filename)
    await interaction.response.send_message("📤 Here's your exported list (JSON):", file=file)

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

@bot.tree.command(name="forcecheck", description="Force an immediate check for a tracked artist (Spotify or SoundCloud).")
@require_registration
@app_commands.describe(link="Artist profile URL or ID (must already be tracked)")
async def forcecheck_command(interaction: discord.Interaction, link: str):
    await interaction.response.defer(ephemeral=True)
    user_id = interaction.user.id
    guild_id = str(interaction.guild.id) if interaction.guild else None

    # Normalize input
    raw = link.strip()
    platform = None
    artist_id = None
    artist = None

    try:
        if "spotify.com" in raw:
            platform = "spotify"
            try:
                from spotify_utils import extract_spotify_id
                artist_id = extract_spotify_id(raw)
            except Exception:
                await interaction.followup.send("❌ Could not extract Spotify artist ID.", ephemeral=True)
                return
        elif "soundcloud.com" in raw:
            platform = "soundcloud"
            try:
                from soundcloud_utils import extract_soundcloud_id
                artist_id = extract_soundcloud_id(raw)
            except Exception:
                await interaction.followup.send("❌ Could not extract SoundCloud artist ID.", ephemeral=True)
                return
        else:
            # Fallback: user supplied raw ID they already track
            # Determine platform by lookup
            artist_sp = get_artist_by_id(raw, user_id, guild_id)
            if artist_sp:
                platform = artist_sp.get("platform")
                artist_id = raw
            else:
                await interaction.followup.send("❌ Provide a valid Spotify/SoundCloud artist URL or a tracked artist ID.", ephemeral=True)
                return

        artist = get_artist_by_id(artist_id, user_id, guild_id)
        if not artist:
            await interaction.followup.send("❌ This artist is not tracked in this server (or by you).", ephemeral=True)
            return

        # Build a single-artist list shaped like main checker expects
        artists_payload = [artist]

        started = datetime.now(timezone.utc)
        if platform == "spotify":
            logging.info(f"⚡ /forcecheck (Spotify) for {artist.get('artist_name')} ({artist_id}) by {interaction.user.name}")
            releases, errors = await check_spotify_updates(bot, artists_payload, shutdown_time=None, is_catchup=False)
            finished = datetime.now(timezone.utc)
            msg = f"✅ Forced Spotify check complete in {(finished-started).total_seconds():.1f}s.\nNew events: {releases}\nErrors: {len(errors)}"
        else:
            logging.info(f"⚡ /forcecheck (SoundCloud) for {artist.get('artist_name')} ({artist_id}) by {interaction.user.name}")
            counts, errors = await check_soundcloud_updates(bot, artists_payload, shutdown_time=None, is_catchup=False)
            finished = datetime.now(timezone.utc)
            summary = ", ".join([f"{k}:{v}" for k, v in counts.items()])
            msg = f"✅ Forced SoundCloud check complete in {(finished-started).total_seconds():.1f}s.\nEvents: {summary}\nErrors: {len(errors)}"

        await interaction.followup.send(msg, ephemeral=True)

    except Exception as e:
        logging.error(f"/forcecheck error: {e}")
        await interaction.followup.send(f"❌ Error running forced check: {e}")


# (Ensure bot.run(TOKEN) remains at bottom)
bot.run(TOKEN)