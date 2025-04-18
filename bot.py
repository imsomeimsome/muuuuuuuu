import os
import typing
from typing import Optional, Literal
import discord
import functools
from discord.ext import tasks
import asyncio
from datetime import datetime
from keep_alive import keep_alive
from discord.ext import commands
from discord import app_commands
from dotenv import load_dotenv
from datetime import datetime
from database_utils import (
    add_artist, remove_artist, artist_exists, get_artist_by_id, get_artist_url,
    update_last_release_date, add_release, get_release_stats, get_all_artists,
    get_artists_by_owner, add_user, is_user_registered, get_username,
    get_user, log_untrack, get_untrack_count, get_user_registered_at, get_global_artist_count, get_artist_full_record,
    set_channel, get_channel, set_release_prefs, get_release_prefs, get_connection
)
from embed_utils import create_music_embed
from spotify_utils import (
    extract_spotify_id, get_artist_name as get_spotify_artist_name,
    get_release_info as get_spotify_release_info
)
from soundcloud_utils import (
    extract_soundcloud_id, get_artist_name_by_url as get_soundcloud_artist_name,
    get_release_info as get_soundcloud_release_info
)

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
            await self.log_channel.send(f"`[{datetime.utcnow()}]` {content}")

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

# --- Release Checker Task ---

@tasks.loop(minutes=5)
async def release_check_loop():
    print(f"[{datetime.now()}] Checking for new releases...")
    await check_for_new_releases()

@release_check_loop.before_loop
async def before_release_check_loop():
    print("Waiting for bot to be ready before starting release check loop...")
    await bot.wait_until_ready()

@bot.event
async def on_ready():
    print(f"Logged in as {bot.user} (ID: {bot.user.id})")
    if not release_check_loop.is_running():
        release_check_loop.start()


# --- Channel Routing ---
async def get_release_channel(guild_id: str, platform: str) -> Optional[discord.TextChannel]:
        channel_id = get_channel(guild_id, platform)
        return bot.get_channel(int(channel_id)) if channel_id else None

async def check_for_new_releases():
    artists = get_all_artists()
    for artist in artists:
        try:
            # Get latest album ID for this artist
            latest_album_id = get_latest_album_id(artist['artist_id'])
            if not latest_album_id:
                continue

            # Check if this is a new release
            release_info = get_spotify_release_info(latest_album_id)
            current_release_date = release_info['release_date']

            if current_release_date != artist['last_release_date']:
                # Update database with new release date
                update_last_release_date(
                    artist['artist_id'],
                    artist['owner_id'],
                    current_release_date
                )

                # Post to Discord
                channel = await get_release_channel(artist['guild_id'], artist['platform'])
                embed = create_music_embed(
                    platform=artist['platform'],
                    artist_name=artist['artist_name'],
                    title=release_info['title'],
                    url=release_info['url'],
                    release_date=current_release_date,
                    cover_url=release_info['cover_url'],
                    features=release_info['features'],
                    track_count=release_info['track_count'],
                    duration=release_info['duration'],
                    repost=False,
                    genres=release_info.get('genres', [])
                )
                await channel.send(embed=embed)

        except Exception as e:
            await bot.log_event(f"❌ Release check failed for {artist['artist_name']}: {str(e)}")

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
                channel = await get_release_channel(str(interaction.guild.id), artist['platform'])
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
        "**Available Commands:**\n"
        "📝 `/list` — Show tracked artists\n"
        "➕ `/track` — Start tracking an artist\n"
        "➖ `/untrack` — Stop tracking an artist\n"
        "🏓 `/ping` — Pong!\n"
        "🎨 `/testembed` — Preview a release embed\n"
        "📤 `/export` — Export your artist list\n"
        "ℹ️ `/info` — Show bot stats\n"
        "📖 `/key` — Show emoji/color key\n"
        "👤 `/userinfo` — Show your or other users' stats"
    )
    await interaction.response.send_message(help_text)

@bot.tree.command(name="ping", description="Pong!")
@require_registration
async def ping_command(interaction: discord.Interaction):
    await interaction.response.send_message("🏓 Pong!")

@bot.tree.command(name="track", description="Start tracking an artist.")
@app_commands.describe(link="Spotify or SoundCloud artist link")
@require_registration
async def track_command(interaction: discord.Interaction, link: str):
    user_id = str(interaction.user.id)
    try:
        if "spotify.com/artist" in link:
            artist_id = extract_spotify_id(link)
            artist_name = get_spotify_artist_name(artist_id)
            if artist_exists(artist_id, user_id):
                await interaction.response.send_message(f"❌ Already tracking **{artist_name}**.")
                return

            # Add artist with tracking info
            conn = get_connection()
            c = conn.cursor()
            c.execute('''
                INSERT OR REPLACE INTO artists 
                (platform, artist_id, artist_name, artist_url, last_release_date, owner_id, tracked_users)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (
                "spotify", 
                artist_id, 
                artist_name, 
                link, 
                None, 
                user_id,
                f"{user_id},"  # Initial tracked users list
            ))
            conn.commit()
            conn.close()

        elif "soundcloud.com" in link:
            artist_id = extract_soundcloud_id(link)
            artist_name = get_soundcloud_artist_name(link)
            if artist_exists(artist_id, user_id):
                await interaction.response.send_message(f"❌ Already tracking **{artist_name}**.")
                return

            # Add artist with tracking info
            conn = get_connection()
            c = conn.cursor()
            c.execute('''
                INSERT OR REPLACE INTO artists 
                (platform, artist_id, artist_name, artist_url, last_release_date, owner_id, tracked_users)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (
                "soundcloud", 
                artist_id, 
                artist_name, 
                link, 
                None, 
                user_id,
                f"{user_id},"  # Initial tracked users list
            ))
            conn.commit()
            conn.close()

        else:
            await interaction.response.send_message("❌ Invalid link.")
            return

        await bot.log_event(f"➕ {interaction.user.name} started tracking **{artist_name}**.")
        await interaction.response.send_message(f"✅ Now tracking **{artist_name}**.")

    except Exception as e:
        await bot.log_event(f"❌ Error: {str(e)}")
        await interaction.response.send_message(f"❌ Error: `{str(e)}`")


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
        artist = get_artist_by_id(artist_id, user_id)
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

@bot.tree.command(name="key", description="Show the emoji and color key.")
@require_registration
async def key_command(interaction: discord.Interaction):
    text = (
        "**🎨 Emoji & Color Key:**\n"
        "💿 Album | 📀 Deluxe | 🎶 EP | 🎵 Single | 🔊 Feature | 📌 Repost\n"
        "🟢 Spotify (#1DB954) | 🟠 SoundCloud (#FF5500)"
    )
    await interaction.response.send_message(text)

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
            release_info = get_spotify_release_info(release_id)
            color = 0x1DB954
        elif "soundcloud.com" in link:
            release_info = get_soundcloud_release_info(link)
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

if __name__ == "__main__":
    keep_alive()  # Start the web server for UptimeRobot
    bot.run(TOKEN)
