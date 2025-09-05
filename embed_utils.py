import discord
import datetime
from datetime import datetime, timezone, timedelta
from utils import get_highest_quality_artwork
import logging

def _indef_article(word: str) -> str:
    if not word:
        return "a"
    return "an" if word[0].lower() in "aeiou" else "a"

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
    content_type=None,
    custom_color=None,
    return_heading: bool = False
):
    """Create an embed for a music release (Spotify or SoundCloud) with correct playlist vs album/EP labeling.
       If return_heading=True returns (heading_text, release_type, embed)."""
    release_type = "track"
    title_lower = (title or "").lower()
    is_sc = platform.lower() == "soundcloud"
    is_playlist_url = bool(url and "/sets/" in url)
    explicit_album = any(k in title_lower for k in ["album", " lp", " record"])
    explicit_ep = any(k in title_lower for k in [" ep", "extended play"])
    is_deluxe = "deluxe" in title_lower

    if is_sc and content_type in ("album", "ep"):
        # Trust upstream classification (set_type) even if URL looks like a playlist
        release_type = content_type
    else:
        if content_type == "playlist" or (is_sc and is_playlist_url):
            if explicit_album:
                release_type = "album"
            elif explicit_ep:
                release_type = "ep"
            else:
                release_type = "playlist"
        else:
            if is_deluxe:
                release_type = "deluxe"
            elif explicit_album:
                release_type = "album"
            elif explicit_ep:
                release_type = "ep"
            else:
                if not is_sc:
                    if track_count:
                        try:
                            tc = int(track_count)
                            if tc >= 7:
                                release_type = "album"
                            elif tc >= 2:
                                release_type = "ep"
                            else:
                                release_type = "track"
                        except Exception:
                            release_type = "track"
                    else:
                        release_type = content_type or "track"
                else:
                    release_type = content_type or "track"

    color = custom_color if custom_color is not None else (0x1DB954 if platform.lower() == "spotify" else 0xfa5a02)

    # Ensure emoji map & duration field logic (re‑add or reinforce)
    emoji_map = {
        "playlist": "📂",
        "album": "💿",
        "ep": "🎶",
        "deluxe": "💿",
        "track": "🎵"
    }
    heading_emoji = emoji_map.get(release_type, "🎵")
    heading = f"{heading_emoji} {artist_name} released {_indef_article(release_type)} {release_type}!"
    embed = discord.Embed(
        title=heading,
        description=f"[{title}]({url})" if title and url else (title or url or "Release"),
        color=color
    )
    # Add duration if non-empty string (including '0:00')
    if track_count:
        embed.add_field(name="Tracks", value=track_count, inline=True)
    if isinstance(duration, str) and duration.strip():
        embed.add_field(name="Duration", value=duration, inline=True)

    # Release date (show raw date for date-only strings)
    if release_date:
        rd = str(release_date).strip()
        if 'T' in rd:
            rd_norm = rd.replace('Z', '+0000')
            ts = None
            for fmt in ('%Y-%m-%dT%H:%M:%S%z', '%Y-%m-%dT%H:%M:%S.%f%z'):
                try:
                    ts = int(datetime.strptime(rd_norm, fmt).timestamp())
                    break
                except Exception:
                    continue
            if ts:
                embed.add_field(name="Release Date", value=f"<t:{ts}:R>", inline=True)
            else:
                embed.add_field(name="Release Date", value=rd[:10], inline=True)
        else:
            embed.add_field(name="Release Date", value=rd[:10], inline=True)

    if genres:
        if isinstance(genres, list):
            clean = [g for g in genres if g]
            if clean:
                embed.add_field(name="Genres" if len(clean) > 1 else "Genre", value=', '.join(clean)[:1024], inline=True)
        else:
            gtxt = str(genres).strip()
            if gtxt and gtxt.lower() != "none":
                embed.add_field(name="Genre", value=gtxt[:1024], inline=True)

    if cover_url:
        try:
            # Upgrade to highest quality (handles SoundCloud -large -> -original/-t500x500, Spotify sizes, etc.)
            high_res = get_highest_quality_artwork(cover_url)
            embed.set_thumbnail(url=high_res or cover_url)
        except Exception:
            embed.set_thumbnail(url=cover_url)

    if return_heading:
        return heading, release_type, embed
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
    content_type=None,          # <--- ADDED
    *,
    original_artist=None
):
    """Create an embed for a reposted track.
    Updated to match the SoundCloud like embed styling EXACTLY (structure & field order) without modifying the like embed itself."""
    display_artist = artist_name or original_artist or "Unknown"

    # Normalize track_count
    if not track_count or track_count == 0:
        track_count = 1

    # Duration formatting (mirror like embed hour support)
    if duration and ":" in duration:
        try:
            parts = duration.split(":")
            if len(parts) == 2:
                minutes, seconds = map(int, parts)
                if minutes >= 60:
                    hours = minutes // 60
                    minutes = minutes % 60
                    duration = f"{hours}:{minutes:02d}:{seconds:02d}"
                else:
                    duration = f"{minutes}:{seconds:02d}"
        except ValueError:
            pass

    # Parse timestamps -> Discord relative format
    release_timestamp = None
    if release_date:
        rd = str(release_date).replace('Z', '+0000')
        for fmt in ('%Y-%m-%dT%H:%M:%S%z', '%Y-%m-%dT%H:%M:%S.%f%z'):
            try:
                release_timestamp = int(datetime.strptime(rd, fmt).timestamp())
                break
            except Exception:
                continue
        if release_timestamp is None:
            # Fallback date-only
            try:
                release_timestamp = int(datetime.strptime(str(release_date)[:10], '%Y-%m-%d').replace(tzinfo=timezone.utc).timestamp())
            except Exception:
                release_timestamp = None

    repost_timestamp = None
    if reposted_date:
        rpd = str(reposted_date).replace('Z', '+0000')
        for fmt in ('%Y-%m-%dT%H:%M:%S%z', '%Y-%m-%dT%H:%M:%S.%f%z'):
            try:
                repost_timestamp = int(datetime.strptime(rpd, fmt).timestamp())
                break
            except Exception:
                continue
        if repost_timestamp is None:
            try:
                repost_timestamp = int(datetime.strptime(str(reposted_date)[:10], '%Y-%m-%d').replace(tzinfo=timezone.utc).timestamp())
            except Exception:
                repost_timestamp = None

    # Determine repost type (prefer explicit content_type from SC classification)
    repost_type = (content_type or "").lower()
    if repost_type not in ("album","ep","playlist","track"):
        # Fallback to prior heuristics
        title_lower = (title or "").lower()
        is_playlist_url = bool(url and "/sets/" in url)
        explicit_album = any(k in title_lower for k in ["album"," lp"," record"])
        explicit_ep = any(k in title_lower for k in [" ep","extended play"])
        if is_playlist_url:
            if explicit_album:
                repost_type = "album"
            elif explicit_ep:
                repost_type = "ep"
            else:
                repost_type = "playlist"
        else:
            if explicit_album:
                repost_type = "album"
            elif explicit_ep:
                repost_type = "ep"
            else:
                repost_type = "track"

    embed = discord.Embed(
        title=f"📢 {reposted_by} reposted {_indef_article(repost_type)} {repost_type}!",
        description=f"[{title}]({url})" if title and url else (title or url or "Repost"),
        color=0xfa5a02
    )

    # First row: By, Tracks, Duration
    embed.add_field(name="By", value=display_artist, inline=True)
    if track_count:
        embed.add_field(name="Tracks", value=track_count, inline=True)
    if duration:
        embed.add_field(name="Duration", value=duration, inline=True)

    # Second row: Release Date, Reposted (relative times)
    if release_timestamp:
        embed.add_field(name="Release Date", value=f"<t:{release_timestamp}:R>", inline=True)
    if repost_timestamp:
        # Use 'Liked' label to mirror like embed exactly per request
        embed.add_field(name="Reposted", value=f"<t:{repost_timestamp}:R>", inline=True)

    # Always add Genres (even if None)
    genre_text = "None"
    genre_name = "Genre"
    if genres:
        if isinstance(genres, list) and genres:
            genre_text = ", ".join(filter(None, genres))
            if len(genres) > 1:
                genre_name = "Genres"
        elif isinstance(genres, str) and genres.strip():
            genre_text = genres.strip()
    embed.add_field(name=genre_name, value=genre_text, inline=True)

    # High-res thumbnail like like embed
    if cover_url:
        high_res_cover = get_highest_quality_artwork(cover_url)
        embed.set_thumbnail(url=high_res_cover or cover_url)

    return embed

def create_like_embed(
    platform,
    liked_by,
    title,
    artist_name,
    url,
    release_date,
    liked_date=None,
    cover_url=None,
    features=None,
    track_count=None,
    duration=None,
    genres=None,
    content_type=None,
    upload_date=None,
    original_artist=None  # NEW (optional; matches create_repost_embed usage)
):
    """Create an embed for a liked track."""
    # Enhanced release type detection
    title_lower = (title or "").lower()
    is_playlist_url = bool(url and "/sets/" in url)
    explicit_album = any(k in title_lower for k in ["album"," lp"," record"])
    explicit_ep = any(k in title_lower for k in [" ep","extended play"])
    is_playlist = (content_type == "playlist") or is_playlist_url

    if is_playlist:
        if explicit_album:
            release_type = "album"
        elif explicit_ep:
            release_type = "ep"
        else:
            release_type = "playlist"
    else:
        # Track logic only; do not auto promote by track_count for SC likes
        if explicit_album:
            release_type = "album"
        elif explicit_ep:
            release_type = "ep"
        else:
            release_type = "track"

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
        title=f"❤️ __{liked_by}__ liked {_indef_article(release_type)} {release_type}!",
        description=f"[{title}]({url})",
        color=0xfa5a02
    )

    # First row: By, Tracks, Duration
    display_artist = artist_name or original_artist or "Unknown"
    embed.add_field(name="By", value=display_artist, inline=True)
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
    if upload_timestamp: # and upload_timestamp != release_timestamp
        embed.add_field(name="Uploaded", value=f"<t:{upload_timestamp}:R>", inline=True)

    # Genres (mirror approach used in repost embed but only if provided)
    if genres:
        if isinstance(genres, list):
            genre_text = ', '.join(filter(None, genres))
        else:
            genre_text = str(genres)
        if genre_text and genre_text.lower() != 'none':
            # Pluralize if multiple genres
            field_name = 'Genres' if ',' in genre_text else 'Genre'
            embed.add_field(name=field_name, value=genre_text, inline=True)

    # Thumbnail
    if cover_url:
        high_res_cover = get_highest_quality_artwork(cover_url)
        embed.set_thumbnail(url=high_res_cover or cover_url)

    return embed