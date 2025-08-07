import discord
from utils import get_highest_quality_artwork
import datetime

def create_music_embed(
    platform,
    artist_name,
    title,
    url,
    release_date,
    cover_url,
    features,
    track_count,
    duration,
    repost,
    genres=None,
    release_type=None,
    custom_color=None
):
    platform_color = 0x1DB954 if platform == "spotify" else 0xFF5500
    embed_color = int(custom_color, 16) if custom_color else platform_color

    if release_type == "Album":
        emoji = "💿"
    elif release_type == "EP":
        emoji = "🎶"
    elif release_type == "Single":
        emoji = "🎵"
    elif release_type == "Playlist":
        emoji = "📑"
    elif release_type == "Like":
        emoji = "❤️"
    else:
        emoji = "🔊"

    if repost:
        emoji += " 📢"

    description = f"[{title}]({url})\n\n"

    if release_type:
        description += f"**Release Type**\n{release_type}\n"

    # ✅ FIXED GENRES SECTION
    genre_text = "Unknown"
    if genres:
        if isinstance(genres, list) and genres:
            genre_text = ", ".join(filter(None, genres))  # Filter out None/empty values
        elif isinstance(genres, str):
            genre_text = genres
    description += f"**Genres**\n{genre_text}\n"

    description += f"**Tracks**\n{track_count}\n"
    description += f"**Features**\n{features or 'None'}\n"
    description += f"**Repost?**\n{'Yes' if repost else 'No'}\n"
    description += f"**Released on** {release_date[:10]}"

    embed = discord.Embed(
        title=f"{emoji} New {artist_name} Release!",
        description=description,
        color=embed_color
    )

    high_res_cover = get_highest_quality_artwork(cover_url) if cover_url else None
    embed.set_thumbnail(url=high_res_cover or cover_url or discord.Embed.Empty)

    return embed

# In embed_utils.py
def create_repost_embed(platform, reposted_by, original_artist, title, url,
                        release_date, cover_url, features, track_count,
                        duration, genres) -> discord.Embed:
    embed = discord.Embed(
        title=title,
        url=url,
        description=f"📢 Reposted by **{reposted_by}**",
        color=discord.Color.orange()
    )
    embed.set_author(name=f"By {original_artist}")
    embed.set_thumbnail(url=cover_url or discord.Embed.Empty)
    embed.add_field(name="Release Date", value=release_date, inline=True)
    embed.add_field(name="Tracks", value=track_count or 1, inline=True)
    embed.add_field(name="Duration", value=duration or "Unknown", inline=True)
    if features:
        embed.add_field(name="Features", value=features, inline=False)
    if genres:
        embed.add_field(name="Genres", value=', '.join(genres), inline=False)
    return embed

import discord

def create_like_embed(platform, liked_by, title, artist_name, url, release_date, liked_date=None, cover_url=None, features=None, track_count=None, duration=None, genres=None):
    """Create an embed for a liked track."""
    
    # Determine release type based on track count
    if release_type == "playlist":
        release_type = "playlist"
    else:
        release_type = "track"
        if track_count:
            if track_count >= 7:
                release_type = "deluxe" if "deluxe" in title.lower() else "album"
            elif track_count >= 2:
                release_type = "EP"

    # Format duration to include hours if needed
    if duration and ":" in duration:
        try:
            minutes, seconds = map(int, duration.split(":"))
            if minutes >= 60:
                hours = minutes // 60
                minutes = minutes % 60
                duration = f"{hours}:{minutes:02d}:{seconds:02d}"
            else:
                duration = f"{minutes}:{seconds:02d}"
        except ValueError:
            # Keep original duration if parsing fails
            pass
    
    # Convert timestamps
    try:
        release_timestamp = None
        if release_date:
            release_timestamp = int(datetime.datetime.strptime(
                release_date.replace('Z', '+0000'), 
                '%Y-%m-%dT%H:%M:%S%z'
            ).timestamp())
    except Exception as e:
        print(f"Error parsing release date: {e}")
        release_timestamp = None
        
    try:
        like_timestamp = None
        if liked_date:
            like_timestamp = int(datetime.datetime.strptime(
                liked_date.replace('Z', '+0000'),
                '%Y-%m-%dT%H:%M:%S%z'
            ).timestamp())
    except Exception as e:
        print(f"Error parsing like date: {e}")
        like_timestamp = None

    embed = discord.Embed(
        title=f"❤️ __{liked_by}__ liked a{release_type.startswith(('a','e','i','o','u')) and 'n' or ''} {release_type}!",
        description=f"[{title}]({url})",
        color=0xfa5a02
    )

    # First row: Artist, Tracks, Duration
    embed.add_field(name="By", value=artist_name, inline=True)
    if track_count:
        embed.add_field(name="Tracks", value=track_count, inline=True)
    if duration:
        embed.add_field(name="Duration", value=duration, inline=True)

    # Second row: Release Date, Like Date
    if release_timestamp:
        embed.add_field(name="Release Date", value=f"<t:{release_timestamp}:R>", inline=True)
    if like_timestamp:
        embed.add_field(name="Liked", value=f"<t:{like_timestamp}:R>", inline=True)
    
    # Always add genres field, even if empty
    genre_text = "Unknown"
    genre_name = "Genre"
    
    if genres:
        if isinstance(genres, list) and genres:
            genre_text = ", ".join(filter(None, genres))  # Filter out None/empty values
            genre_name = "Genres" if len(genres) > 1 else "Genre"
        elif isinstance(genres, str):
            genre_text = genres
    
    embed.add_field(name=genre_name, value=genre_text, inline=True)

    # High-res thumbnail
    if cover_url:
        high_res_cover = get_highest_quality_artwork(cover_url)
        embed.set_thumbnail(url=high_res_cover or cover_url)

    return embed

#
#    # Build embed description
#    description = f"[{title}]({url})\n\n"
#    
#    # ✅ Genres (Always show, or show "None" if empty)
#    if genres and len(genres) > 0:
#        description += f"**Genres**\n{', '.join(genres[:3])}\n"
#    else:
#        description += f"**Genres**\nNone\n"
#
#    description += (
#        f"**Duration**\n{duration}\n"
#        f"**Features**\n{features}\n"
#        f"**Released on** {release_date[:10]}"
#    )
#
#    # Create embed with correct description
#    embed = discord.Embed(
#        title=f"{emoji} New {artist_name} Release!",
#        description=description,
#        color=embed_color
#    )
#
#    embed.set_thumbnail(url=cover_url)
#    return embed
