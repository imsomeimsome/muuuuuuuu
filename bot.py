import discord
from discord.ext import commands, tasks
from discord import app_commands
from dotenv import load_dotenv
import sqlite3  # Import sqlite3
import os
from keep_alive import keep_alive
from spotify_utils import track_spotify_artist, get_artist_releases, get_artist_name as get_spotify_artist_name
from soundcloud_utils import track_soundcloud_artist, get_artist_tracks, get_artist_name_by_url as get_soundcloud_artist_name, get_access_token
from database_utils import add_artist, initialize_database, artist_exists
from embed_utils import create_embed
from utils import run_blocking  # Import run_blocking from utils.py
import logging
import re  # Import the regex module for URL validation

# get_access_token() # Use again if I need it
# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Load environment variables
load_dotenv()
TOKEN = os.getenv("DISCORD_TOKEN")  # Bot token
GUILD_ID = int(os.getenv("DISCORD_GUILD_ID"))  # Discord server ID (must be an integer)

# Initialize bot
intents = discord.Intents.default()
intents.messages = True
intents.guilds = True
bot = commands.Bot(command_prefix="!", intents=intents)
tree = bot.tree  # For slash commands

# Initialize database
initialize_database()

@bot.event
async def on_ready():
    print(f"Bot is online as {bot.user}")
    await tree.sync(guild=discord.Object(id=GUILD_ID))  # Sync slash commands to the guild
    check_for_updates.start()  # Start periodic checks for updates

@bot.tree.command(name="track", description="Track a new artist from Spotify or SoundCloud")
@app_commands.describe(link="A Spotify or SoundCloud artist URL")
async def track_command(interaction: discord.Interaction, link: str):
    """
    Track an artist's activities using a URL.
    Usage: /track <link>
    """
    await interaction.response.defer(ephemeral=True)

    # Validate the artist URL
    url_pattern = re.compile(r"https?://(?:www\.)?(spotify\.com|soundcloud\.com)/.+")
    if not url_pattern.match(link):
        await interaction.followup.send("❌ Invalid URL. Please provide a valid Spotify or SoundCloud link.")
        return

    # Detect platform and extract artist ID
    if "spotify.com" in link:
        platform = "spotify"
        artist_id = link.split("/")[-1]  # Extract the last part of the URL as the artist ID
        artist_name = await run_blocking(get_spotify_artist_name, artist_id)
        artist_url = f"https://open.spotify.com/artist/{artist_id}"
    elif "soundcloud.com" in link:
        platform = "soundcloud"
        artist_id = link.split("/")[-1]  # Extract the last part of the URL as the artist ID
        artist_name = await run_blocking(get_soundcloud_artist_name, link)
        artist_url = f"https://soundcloud.com/{artist_id}"
    else:
        await interaction.followup.send("❌ Unsupported platform. Use a Spotify or SoundCloud link.")
        return

    # Check if the artist is already tracked
    user_id = interaction.user.id
    guild_id = str(interaction.guild.id) if interaction.guild else None
    if artist_exists(platform, artist_id, user_id):
        await interaction.followup.send("⚠️ You're already tracking this artist.")
        return

    # Add the artist to the database
    from datetime import datetime, timezone
    current_time = datetime.now(timezone.utc).isoformat()
    add_artist(
        platform=platform,
        artist_id=artist_id,
        artist_name=artist_name,
        artist_url=artist_url,
        owner_id=user_id,
        guild_id=guild_id,
        last_release_date=current_time  # Store the current time to prevent false first posts
    )

    await interaction.followup.send(f"✅ Now tracking **{artist_name}** on {platform.capitalize()}.")

# Periodic task to check for new releases
@tasks.loop(minutes=5)  # Runs every 5 minutes
async def check_for_updates():
    """
    Periodically check for new releases from tracked artists.
    """
    logging.info("Starting periodic check for updates...")
    conn = sqlite3.connect("artists.db")
    cursor = conn.cursor()
    cursor.execute("SELECT platform, artist_id FROM artists")
    tracked_artists = cursor.fetchall()
    conn.close()

    logging.info(f"Found {len(tracked_artists)} tracked artists.")

    for platform, artist_id in tracked_artists:
        logging.info(f"Checking updates for artist {artist_id} on {platform}...")
        if platform == "spotify":
            releases = get_artist_releases(artist_id)
            logging.info(f"Found {len(releases)} new releases for Spotify artist {artist_id}.")
            for release in releases:
                embed = create_embed(
                    title=release['name'],
                    description=f"New release by {release['artists'][0]['name']}",
                    url=release['external_urls']['spotify'],
                    thumbnail_url=release['images'][0]['url'] if release['images'] else None
                )
                channel = bot.get_channel(int(os.getenv("DISCORD_CHANNEL_ID")))  # Add your channel ID to .env
                await channel.send(embed=embed)
                logging.info(f"Posted new release: {release['name']} by {release['artists'][0]['name']}.")
        elif platform == "soundcloud":
            tracks = get_artist_tracks(artist_id)
            logging.info(f"Found {len(tracks)} new tracks for SoundCloud artist {artist_id}.")
            for track in tracks:
                embed = create_embed(
                    title=track['title'],
                    description=f"New track by {track['user']['username']}",
                    url=track['permalink_url'],
                    thumbnail_url=track['artwork_url']
                )
                channel = bot.get_channel(int(os.getenv("DISCORD_CHANNEL_ID")))  # Add your channel ID to .env
                await channel.send(embed=embed)
                logging.info(f"Posted new track: {track['title']} by {track['user']['username']}.")

    logging.info("Periodic check for updates completed.")

# Start the periodic task when the bot is ready
@bot.event
async def on_ready():
    print(f"Bot is online as {bot.user}")
    try:
        # Force sync commands to the guild
        await tree.sync(guild=discord.Object(id=GUILD_ID))
        print(f"Slash commands synced to guild ID {GUILD_ID}.")
    except Exception as e:
        print(f"Failed to sync commands: {e}")
    check_for_updates.start()  # Start periodic checks for updates
    
# Keep the bot alive
keep_alive()

# Run the bot
bot.run(TOKEN)