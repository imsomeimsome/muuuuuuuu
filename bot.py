import discord
from discord.ext import commands, tasks
from discord import app_commands
from dotenv import load_dotenv
import sqlite3  # Import sqlite3
import os
from keep_alive import keep_alive
from spotify_utils import track_spotify_artist, get_artist_releases
from soundcloud_utils import track_soundcloud_artist, get_artist_tracks
from database_utils import add_artist, initialize_database
from embed_utils import create_embed
import logging

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

@tree.command(name="track", description="Track an artist's activities on Spotify or SoundCloud", guild=discord.Object(id=GUILD_ID))
async def track_command(interaction: discord.Interaction, platform: str, artist_id: str):
    """
    Track an artist's activities.
    Usage: /track <platform> <artist_id>
    """
    if platform.lower() == "spotify":
        success = track_spotify_artist(artist_id)
    elif platform.lower() == "soundcloud":
        success = track_soundcloud_artist(artist_id)
    else:
        await interaction.response.send_message("Unsupported platform. Use 'spotify' or 'soundcloud'.", ephemeral=True)
        return

    if success:
        db_success = add_artist(platform.lower(), artist_id)
        if db_success:
            embed = create_embed(
                title="Artist Added",
                description=f"Successfully added artist {artist_id} on {platform}.",
            )
            await interaction.response.send_message(embed=embed)
        else:
            await interaction.response.send_message(f"Artist {artist_id} is already being tracked.", ephemeral=True)
    else:
        await interaction.response.send_message(f"Failed to add artist {artist_id} on {platform}.", ephemeral=True)

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