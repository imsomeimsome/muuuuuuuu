import discord
from discord.ext import commands
from dotenv import load_dotenv
import os
from keep_alive import keep_alive
from spotify_utils import track_spotify_artist
from soundcloud_utils import track_soundcloud_artist
from database_utils import add_artist, initialize_database
from embed_utils import create_embed

# Load environment variables
load_dotenv()
TOKEN = os.getenv("DISCORD_TOKEN")

# Initialize bot
intents = discord.Intents.default()
intents.messages = True
intents.guilds = True
bot = commands.Bot(command_prefix="!", intents=intents)

# Initialize database
initialize_database()

@bot.event
async def on_ready():
    print(f"Bot is online as {bot.user}")

@bot.command(name="add_artist")
async def add_artist_command(ctx, platform: str, artist_id: str):
    """
    Add an artist to track.
    Usage: !add_artist <platform> <artist_id>
    """
    if platform.lower() == "spotify":
        success = track_spotify_artist(artist_id)
    elif platform.lower() == "soundcloud":
        success = track_soundcloud_artist(artist_id)
    else:
        await ctx.send("Unsupported platform. Use 'spotify' or 'soundcloud'.")
        return

    if success:
        db_success = add_artist(platform.lower(), artist_id)
        if db_success:
            embed = create_embed(
                title="Artist Added",
                description=f"Successfully added artist {artist_id} on {platform}.",
            )
            await ctx.send(embed=embed)
        else:
            await ctx.send(f"Artist {artist_id} is already being tracked.")
    else:
        await ctx.send(f"Failed to add artist {artist_id} on {platform}.")

# Keep the bot alive
keep_alive()

# Run the bot
bot.run(TOKEN)