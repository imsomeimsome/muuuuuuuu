import discord
import datetime
from datetime import datetime, timezone, timedelta
from utils import get_highest_quality_artwork

def create_music_embed(platform, artist_name, title, url, release_date, cover_url, features, track_count, duration, repost, genres=None, release_type=None, custom_color=None):
    """Create an embed for a music release."""
    # Platform color
    platform_color = 0x1DB954 if platform == "spotify" else 0xfa5a02
    embed_color = int(custom_color, 16) if custom_color else platform_color

    # Determine release type
    if release_type == "playlist":
        release_type = "playlist"
    else:
        release_type = "track"
        if track_count:
            if track_count >= 7:
                release_type = "deluxe" if "deluxe" in title.lower() else "album"
            elif track_count >= 2:
                release_type = "EP"

    # Release type emoji
    emoji = {
        "album": "💿",
        "deluxe": "💿",
        "EP": "🎶",
        "track": "🎵",
        "playlist": "📑"
    }.get(release_type.lower(), "🎵")

    if repost:
        emoji += " 📢"

    # Convert timestamps for Discord's relative time format
    try:
        release_timestamp = int(datetime.datetime.strptime(
            release_date.replace('Z', '+0000'), 
            '%Y-%m-%dT%H:%M:%S%z'
        ).timestamp())
    except Exception as e:
        print(f"Error parsing release date: {e}")
        release_timestamp = None

    # Create embed with consistent style
    embed = discord.Embed(
        title=f"{emoji} __{artist_name}__ released a{release_type.startswith(('a','e','i','o','u')) and 'n' or ''} {release_type}!",
        description=f"[{title}]({url})",
        color=embed_color
    )

    # Add fields in consistent order
    embed.add_field(name="By", value=artist_name, inline=True)
    if track_count:
        embed.add_field(name="Tracks", value=track_count, inline=True)
    if duration:
        embed.add_field(name="Duration", value=duration, inline=True)

    # Release date with Discord timestamp
    if release_timestamp:
        embed.add_field(name="Release Date", value=f"<t:{release_timestamp}:R>", inline=True)

    # Format genres with proper casing and quotes
    if genres:
        genre_list = []
        if isinstance(genres, list):
            genre_list = [f"'{g.title()}'" for g in genres if g]
        elif isinstance(genres, str):
            genre_list = [f"'{genres.title()}'"]
        
        genre_name = "Genres" if len(genre_list) > 1 else "Genre"
        if genre_list:
            embed.add_field(name=genre_name, value=", ".join(genre_list), inline=True)

    # High-res thumbnail
    if cover_url:
        high_res_cover = get_highest_quality_artwork(cover_url)
        embed.set_thumbnail(url=high_res_cover or cover_url)

    return embed

def create_repost_embed(platform, reposted_by, title, artist_name, url, release_date, reposted_date, cover_url, features, track_count, duration, genres):
    """Create an embed for a reposted track."""
    embed = discord.Embed(
        title=f"📢 {reposted_by} reposted a track!",
        description=f"[{title}]({url})",
        color=0xfa5a02
    )
    
    embed.set_author(name=f"By {artist_name}")
    
    if cover_url:
        embed.set_thumbnail(url=cover_url)
        
    if release_date:
        embed.add_field(name="Release Date", value=release_date[:10], inline=True)
        
    if reposted_date:
        embed.add_field(name="Reposted Date", value=reposted_date[:10], inline=True)
        
    embed.add_field(name="Tracks", value=track_count or 1, inline=True)
    
    if duration:
        embed.add_field(name="Duration", value=duration, inline=True)
        
    if features:
        embed.add_field(name="Features", value=features, inline=False)
        
    if genres and len(genres) > 0:
        embed.add_field(name="Genres", value=", ".join(genres), inline=False)

    return embed 

def create_like_embed(platform, liked_by, title, artist_name, url, release_date, liked_date=None, cover_url=None, features=None, track_count=None, duration=None, genres=None, content_type=None, upload_date=None):
    """Create an embed for a liked track."""
    
    # Enhanced release type detection
    def determine_release_type(content_type, title, track_count, tracks_data=None):
        # 1. Check for playlist type from API
        if content_type == "playlist":
            # Common keywords indicating type in title
            title_lower = title.lower()
            type_keywords = {
                "album": ["album", "lp", "record"],
                "EP": ["ep", "extended play"],
                "mixtape": ["mixtape", "mix tape"],
                "compilation": ["compilation", "various artists", "various", "va"],
                "playlist": ["playlist", "mix", "selection", "picks", "favorites"]
            }
            
            # Check title for type indicators
            for type_name, keywords in type_keywords.items():
                if any(keyword in title_lower for keyword in keywords):
                    return type_name.lower()
            
            # 2. Check for multiple artists if tracks data available
            if tracks_data:
                artists = set(track.get('artist_name') for track in tracks_data)
                if len(artists) > 1:
                    return "playlist"
            
            # 3. Fallback to track count logic
            if track_count:
                if track_count >= 7:
                    return "deluxe" if "deluxe" in title_lower else "album"
                elif track_count >= 2:
                    return "EP"
                
        return "track"

    # Get release type
    release_type = determine_release_type(content_type, title, track_count)

    # Determine release type based on track count
    if content_type == "playlist":
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
        upload_timestamp = None
        if upload_date:
            upload_timestamp = int(datetime.datetime.strptime(
                upload_date.replace('Z', '+0000'), 
                '%Y-%m-%dT%H:%M:%S%z'
            ).timestamp())
    except Exception as e:
        print(f"Error parsing upload date: {e}")
        upload_timestamp = None
        
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

    # First row: By, Tracks, Duration
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

    # Third row: Upload Date (if different from release date)
    if upload_timestamp: # and upload_timestamp != release_timestamp:
        embed.add_field(name="Uploaded", value=f"<t:{upload_timestamp}:R>", inline=True)
    
    # Always add genres field, even if empty
    genre_text = "None"
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
