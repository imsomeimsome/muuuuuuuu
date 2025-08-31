import discord
import datetime
from datetime import datetime, timezone, timedelta
from utils import get_highest_quality_artwork
import logging

def create_music_embed(platform, artist_name, title, url, release_date, cover_url, features, track_count, duration, repost, genres=None, content_type=None, custom_color=None):
    """Create an embed for a music release."""
    
    # Determine release type based on track count and title
    release_type = "track"
    if content_type:
        release_type = content_type
    else:
        if track_count:
            if track_count >= 7:
                release_type = "deluxe" if "deluxe" in title.lower() else "album"
            elif track_count >= 2:
                release_type = "EP"
    
    # Create base embed
    if platform.lower() == "spotify":
        embed = discord.Embed(
            title=f"# 🎵 {artist_name} released a {release_type}!",
            # Emphasize title (Discord headings inside embeds are not larger; using bold+underline for emphasis)
            description=f"__**{title}**__\n[{title}]({url})",
            color=0x1DB954  # Spotify green
        )
    else:
        # Keep existing SoundCloud embed format
        embed = discord.Embed(
            title=f"# 🎵 {artist_name} released a {release_type}!",
            description=f"[{title}]({url})",
            color=0xfa5a02  # SoundCloud orange
        )

    # Add fields in consistent order
    embed.add_field(name="By", value=artist_name, inline=True)
    if track_count:
        embed.add_field(name="Tracks", value=track_count, inline=True)
    if duration:
        embed.add_field(name="Duration", value=duration, inline=True)

    # Add release date
    try:
        release_timestamp = None
        if release_date:
            rd = release_date
            try:
                # Standard full datetime
                if 'T' in rd:
                    rd_norm = rd.replace('Z', '+0000')
                    for fmt in ('%Y-%m-%dT%H:%M:%S%z', '%Y-%m-%dT%H:%M:%S.%f%z'):
                        try:
                            release_timestamp = int(datetime.strptime(rd_norm, fmt).timestamp())
                            break
                        except ValueError:
                            continue
                if release_timestamp is None:
                    # Date-only fallback
                    dt = datetime.strptime(rd[:10], '%Y-%m-%d').replace(tzinfo=timezone.utc)
                    release_timestamp = int(dt.timestamp())
            except Exception as e:
                logging.warning(f"Error parsing release date '{release_date}': {e}")
                release_timestamp = None
        if release_timestamp:
            embed.add_field(name="Release Date", value=f"<t:{release_timestamp}:R>", inline=True)
    except Exception as e:
        logging.error(f"Error parsing release date: {e}")

    # Add genre if available
    if genres:
        if isinstance(genres, list):
            genre_text = ', '.join(filter(None, genres))
        else:
            genre_text = str(genres)
        if genre_text and genre_text.lower() != "none":
            embed.add_field(name="Genre", value=genre_text, inline=True)

    # Set thumbnail if available
    if cover_url:
        high_res_cover = get_highest_quality_artwork(cover_url)
        embed.set_thumbnail(url=high_res_cover or cover_url)

    return embed

def create_repost_embed(
    platform,
    reposted_by,
    title,
    artist_name=None,
    url=None,
    release_date=None,
    reposted_date=None,
    cover_url=None,
    features=None,
    track_count=None,
    duration=None,
    genres=None,
    *,
    original_artist=None  # keyword-only to avoid positional ambiguity
):
    """Create an embed for a reposted track.
    Supports both artist_name and original_artist (alias)."""
    display_artist = artist_name or original_artist or "Unknown"

    embed = discord.Embed(
        title=f"📢 {reposted_by} reposted a track!",
        description=f"[{title}]({url})" if title and url else (title or url or "Repost"),
        color=0xfa5a02
    )

    embed.set_author(name=f"By {display_artist}")

    if cover_url:
        embed.set_thumbnail(url=cover_url)

    # Add dates (support various formats)
    try:
        if release_date:
            rd = str(release_date)
            if len(rd) >= 10:
                embed.add_field(name="Release Date", value=rd[:10], inline=True)
    except Exception as e:
        logging.debug(f"repost embed release_date parse issue: {e}")

    try:
        if reposted_date:
            rpd = str(reposted_date)
            if len(rpd) >= 10:
                embed.add_field(name="Reposted Date", value=rpd[:10], inline=True)
    except Exception as e:
        logging.debug(f"repost embed reposted_date parse issue: {e}")

    embed.add_field(name="Tracks", value=track_count or 1, inline=True)

    if duration:
        embed.add_field(name="Duration", value=duration, inline=True)

    if features:
        embed.add_field(name="Features", value=features, inline=False)

    if genres:
        if isinstance(genres, (list, tuple)) and genres:
            embed.add_field(name="Genres", value=", ".join(genres), inline=False)
        elif isinstance(genres, str) and genres.strip():
            embed.add_field(name="Genres", value=genres, inline=False)

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
            release_timestamp = int(datetime.strptime(
                release_date.replace('Z', '+0000'), 
                '%Y-%m-%dT%H:%M:%S%z'
            ).timestamp())
    except Exception as e:
        print(f"Error parsing release date: {e}")
        release_timestamp = None

    try:
        upload_timestamp = None
        if upload_date:
            upload_timestamp = int(datetime.strptime(
                upload_date.replace('Z', '+0000'), 
                '%Y-%m-%dT%H:%M:%S%z'
            ).timestamp())
    except Exception as e:
        print(f"Error parsing upload date: {e}")
        upload_timestamp = None
        
    try:
        like_timestamp = None
        if liked_date:
            like_timestamp = int(datetime.strptime(
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
