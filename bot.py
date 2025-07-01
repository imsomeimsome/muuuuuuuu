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
    set_channel, get_channel, set_release_prefs, get_release_prefs, get_connection, get_artist_by_identifier
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
    get_soundcloud_reposts_info
)
from utils import run_blocking, log_release, parse_datetime
from reset_artists import reset_tables
from tables import initialize_fresh_database
import sqlite3

# Ensure the /data directory exists
os.makedirs('/data', exist_ok=True)

# reset_tables() # USE THIS LINE TO RESET ARTISTS TABLES
initialize_fresh_database()  # Uncomment this line ONCE to initialize the database, then comment it back out

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("release_checker")

# Helper to parse dates consistently
def parse_date(date_str: str) -> datetime:
    """Return timezone-aware datetime for any ISO date string."""
    dt = isoparse(date_str)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt

load_dotenv()
TOKEN = os.getenv("DISCORD_TOKEN")
TEST_GUILD_ID = os.getenv("TEST_GUILD_ID")
LOG_CHANNEL_ID = int(os.getenv("LOG_CHANNEL_ID", 0))


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

import sqlite3

def check_artist_table_columns():
    conn = sqlite3.connect("/data/artists.db")
    c = conn.cursor()
    c.execute("PRAGMA table_info(artists);")
    columns = c.fetchall()
    print("📊 Columns in artists table:")
    for col in columns:
        print(f"- {col[1]}")
    conn.close()

# --- Release Checker ---

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

async def check_for_new_releases(bot):
    import logging
    from datetime import datetime, timedelta, timezone
    from dateutil.parser import isoparse

    logging.info("🔍 Checking for new releases...")
    now = datetime.now(timezone.utc)
    soundcloud_retry_after = None

    try:
        artists = get_all_artists()
    except Exception as e:
        logging.error(f"❌ Failed to fetch artists from database: {e}")
        return

    total_checked = 0
    new_release_count = 0
    for artist in artists:
        total_checked += 1
        platform = artist.get("platform")
        artist_name = artist.get("artist_name", "unknown")
        artist_id = artist.get("artist_id")
        artist_url = artist.get("artist_url")
        owner_id = artist.get("owner_id")
        guild_id = artist.get("guild_id")
        last_date = artist.get("last_release_date")

        try:
            if platform == "spotify":
                latest_album_id = await run_blocking(get_spotify_latest_album_id, artist_id)
                if not latest_album_id:
                    continue
                release_info = await run_blocking(get_spotify_release_info, latest_album_id)

            elif platform == "soundcloud":
                if soundcloud_retry_after and now < soundcloud_retry_after:
                    logging.warning(f"⚠️ Skipping SoundCloud fetch for {artist_name} due to cooldown.")
                    continue
                try:
                    release_info = await run_blocking(get_soundcloud_release_info, artist_url)
                except Exception as e:
                    if "rate/request limit" in str(e).lower():
                        soundcloud_retry_after = now + timedelta(hours=12)
                        logging.warning(f"⚠️ SoundCloud rate limit hit. Skipping until {soundcloud_retry_after}")
                    raise

            else:
                continue

            if not release_info:
                logging.info(f"⚠️ No release info found for {artist_name} ({platform})")
                continue

            current_date = release_info.get("release_date")
            if not current_date:
                continue

            logging.info(f"👀 Checking {artist_name} ({platform})")
            logging.info(f"→ Stored last_release_date: {last_date}")
            logging.info(f"→ New release date from API: {current_date}")

            if not last_date:
                logging.info(f"⏭️ Skipping first check for {artist_name} — storing current release date.")
                update_last_release_date(artist_id, owner_id, guild_id, current_date)
                continue

            if parse_date(current_date) > parse_date(last_date):
                update_last_release_date(artist_id, owner_id, guild_id, current_date)
                await handle_release(bot, artist, release_info, "release")
                new_release_count += 1

        except Exception as e:
            logging.error(f"❌ Failed to check {platform} artist {artist_name}: {e}")

    logging.info(f"✅ Checked {total_checked} artists")
    logging.info(f"🆕 New releases detected: {new_release_count}")

    if soundcloud_retry_after and now < soundcloud_retry_after:
        logging.warning("⏭️ Skipping SoundCloud playlist/repost/like checks due to cooldown.")
        return

    # PLAYLISTS
    logging.info("🔍 Checking for new playlists...")
    for artist in artists:
        if artist.get("platform") != 'soundcloud':
            continue

        try:
            logging.info(
                f"👀 Checking playlists for {artist.get('artist_name', 'unknown')}"
            )
            info = await run_blocking(get_soundcloud_playlist_info, artist["artist_url"])
            if not info:
                continue
            release_date = info["release_date"]
            logging.info(f"→ Stored last_release_date: {artist.get('last_release_date')}")
            logging.info(f"→ Playlist release date from API: {release_date}")
            if not artist["last_release_date"] or parse_date(release_date) > parse_date(artist["last_release_date"]):
                update_last_release_date(artist["artist_id"], artist["owner_id"], artist["guild_id"], release_date)
                await handle_release(bot, artist, info, "playlist")

        except Exception as e:
            logging.error(f"❌ Playlist check failed for {artist.get('artist_name', 'unknown')}: {e}")

    # === REPOSTS ===
    logging.info("🔍 Checking for reposts...")
    for artist in artists:
        if artist.get("platform") != "soundcloud":
            continue

        try:
            logging.info(f"👀 Checking reposts for {artist.get('artist_name', 'unknown')}")
            reposts = await run_blocking(get_soundcloud_reposts_info, artist["artist_url"])
            logging.info(f"→ {len(reposts)} recent repost(s) fetched")

            for repost in reposts:
                # Determine repost ID from URL or fallback to track_id
                repost_id = repost.get("url") or repost.get("track_id") or repost.get("title")
                repost_id = str(repost_id)

                if not repost_id:
                    logging.warning(f"⚠️ No ID found for repost: {repost}")
                    continue

                if is_already_posted_repost(artist["artist_id"], artist["guild_id"], repost_id):
                    logging.info(f"⏭️ Repost already posted: {repost_id}")
                    continue

                repost_date = parse_datetime(repost.get("release_date"))
                last_check = parse_datetime(artist.get("last_release_date"))

                if repost_date and last_check and repost_date <= last_check:
                    logging.info(f"⏭️ Skipping old repost ({repost_date} <= {last_check}): {repost['title']}")
                    continue

                # Create and send embed
                embed = create_repost_embed(
                    platform=artist.get("platform"),
                    reposted_by=artist.get("artist_name"),
                    original_artist=repost.get("artist_name"),
                    title=repost.get("title"),
                    url=repost.get("url"),
                    release_date=repost.get("release_date"),
                    cover_url=repost.get("cover_url"),
                    features=repost.get("features"),
                    track_count=repost.get("track_count"),
                    duration=repost.get("duration"),
                    genres=repost.get("genres"),
                )

                channel = await get_release_channel(guild_id=artist["guild_id"], platform="soundcloud")
                if channel:
                    await channel.send(embed=embed)
                    logging.info(f"✅ Posted repost {repost_id} for {artist.get('artist_name')} to #{channel.name}")
                    mark_posted_repost(artist["artist_id"], artist["guild_id"], repost_id)

        except Exception as e:
            logging.error(f"❌ Repost check failed for {artist.get('artist_name', 'unknown')}: {e}")

    # LIKES
    logging.info("🔍 Checking for likes...")
    for artist in artists:
        if artist.get("platform") != 'soundcloud':
            continue

        try:
            logging.info(f"👀 Checking likes for {artist.get('artist_name', 'unknown')}")
            likes = await run_blocking(get_soundcloud_likes_info, artist["artist_url"])
            logging.info(f"→ {len(likes)} recent like(s) fetched")

            for like in likes:
                like_id = str(like.get("track_id"))
                like_date = parse_datetime(like.get("release_date"))
                last_like_date = parse_datetime(artist.get("last_like_date", "1970-01-01T00:00:00Z"))

                if not like_id or is_already_posted_like(artist["artist_id"], artist["guild_id"], like_id):
                    continue

                if like_date <= last_like_date:
                    logging.debug(f"⏩ Skipping like from {like_date}, older than last_like_date {last_like_date}")

                    continue

                embed = create_like_embed(
                    platform=artist.get("platform"),
                    artist_name=artist.get("artist_name"),
                    title=like.get("title"),
                    url=like.get("url"),
                    release_date=like.get("release_date"),
                    cover_url=like.get("cover_url"),
                    duration=like.get("duration"),
                    genres=like.get("genres"),
                    features=like.get("features"),
                )

                channel = await get_release_channel(guild_id=artist["guild_id"], platform="soundcloud")
                if channel:
                    await channel.send(embed=embed)
                    logging.info(f"✅ Posted liked track {like_id} for {artist.get('artist_name')} to #{channel.name}")
                    mark_posted_like(artist["artist_id"], artist["guild_id"], like_id)
                    update_last_like_date(artist["artist_id"], artist["guild_id"], like.get("release_date"))

        except Exception as e:
            logging.error(f"❌ Like check failed for {artist.get('artist_name', 'unknown')}: {e}")

async def release_check_scheduler(bot):
    await bot.wait_until_ready()
    logging.info("🚀 Release checker started")
    logging.info("⏳ Release checker initializing...")

    while not bot.is_closed():
        now = datetime.now(timezone.utc)

        next_run_minute = (now.minute // 5 + 1) * 5
        if next_run_minute >= 60:
            next_run = now.replace(hour=(now.hour + 1) % 24, minute=0, second=1, microsecond=0)
        else:
            next_run = now.replace(minute=next_run_minute, second=1, microsecond=0)

        delay = (next_run - now).total_seconds()
        delay = max(delay, 0)

        logging.info(f"🕰️ First check at {next_run.strftime('%H:%M:%S')} UTC (in {delay:.1f}s)")
        sleep_interval = 60
        while delay > 0:
            await asyncio.sleep(min(delay, sleep_interval))
            delay -= sleep_interval


        try:
            check_time = datetime.now(timezone.utc).strftime('%H:%M:%S')
            logging.info(f"🔍 Starting release check at {check_time} UTC...")

            await check_for_new_releases(bot)

            logging.info("✅ Completed release check cycle")
        except Exception as e:
            logging.error(f"❌ Error during release check: {e}")

@bot.event
async def on_ready():
    await bot.wait_until_ready()
    logging.info(f"✅ Logged in as {bot.user} (ID: {bot.user.id})")
    logging.info("🚀 Release checker started")
    logging.info("⏳ Release checker initializing...")

    # ✅ Sync slash commands on startup
    try:
        synced = await bot.tree.sync()
        logging.info(f"🌐 Synced {len(synced)} slash commands.")
    except Exception as e:
        logging.error(f"❌ Failed to sync slash commands: {e}")

    # ✅ Only start scheduler once
    if not hasattr(bot, 'release_checker_started'):
        bot.release_checker_started = True
        asyncio.create_task(release_check_scheduler(bot))
        logging.info("🚀 Started release checker")


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
        artist_name = await run_blocking(get_soundcloud_artist_name, link)
        artist_url = f"https://soundcloud.com/{artist_id}"
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
    from soundcloud_utils import verify_client_id
    await interaction.response.defer(ephemeral=True)
    if verify_client_id():
        await interaction.followup.send("✅ SoundCloud client ID appears valid.")
    else:
        await interaction.followup.send("❌ SoundCloud client ID check failed. Verify the ID.")

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


if __name__ == "__main__":
    keep_alive()  # Start the web server for UptimeRobot
    bot.run(TOKEN)