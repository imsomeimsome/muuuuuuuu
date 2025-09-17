import discord
import datetime
from datetime import datetime, timezone, timedelta
from utils import get_highest_quality_artwork
import logging
from dateutil.parser import parse as isoparse
import os

# New configurable offset (hours) for SoundCloud display day adjustment
SC_DISPLAY_TZ_OFFSET = int(os.getenv("SC_DISPLAY_TZ_OFFSET", "0"))

def _indef_article(word: str) -> str:
    if not word:
        return "a"
    return "an" if word[0].lower() in "aeiou" else "a"

def _first_feat_name_strict(features):
    """
    Return the first valid feature name or None.
    - Ignores placeholders like 'none'/'unknown'
    - Handles list or comma-separated string
    """
    def _is_valid(n: str) -> bool:
        if not n:
            return False
        s = n.strip()
        if not s:
            return False
        low = s.lower()
        return low not in {"none", "unknown", "n/a", "na", "-"}

    if not features:
        return None
    if isinstance(features, list):
        for f in features:
            if isinstance(f, str) and _is_valid(f):
                return f.strip()
        return None
    # string
    parts = [p.strip() for p in str(features).split(",")]
    for p in parts:
        if _is_valid(p):
            return p
    return None

def _to_unix_ts(s):
    """Robust timestamp parser for SoundCloud and Spotify dates."""
    if not s:
        return None
    try:
        txt = str(s).strip()
        # Normalize Z to +00:00 for fromisoformat
        if txt.endswith('Z'):
            txt = txt[:-1] + '+00:00'
        # fromisoformat handles: YYYY-MM-DD, YYYY-MM-DDTHH:MM:SS(.fff)(±HH:MM)
        dt = datetime.fromisoformat(txt)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp())
    except Exception:
        pass
    # Legacy SoundCloud format: 2025/09/06 16:40:00 +0000
    for fmt in ('%Y/%m/%d %H:%M:%S %z',
                '%Y-%m-%dT%H:%M:%S%z',
                '%Y-%m-%dT%H:%M:%S.%f%z'):
        try:
            txt = str(s).strip().replace('Z', '+0000')
            return int(datetime.strptime(txt, fmt).timestamp())
        except Exception:
            continue
    # Date-only fallback
    try:
        return int(datetime.strptime(str(s)[:10], '%Y-%m-%d').replace(tzinfo=timezone.utc).timestamp())
    except Exception:
        return None

def _is_date_only(s: str) -> bool:
    return isinstance(s, str) and len(s) == 10 and s.count('-') == 2 and 'T' not in s

def _discord_ts(dt: datetime) -> int:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp())

def _format_discord_release_date(date_str: str) -> str:
    """
    Date-only (YYYY-MM-DD) -> absolute calendar date (:D) using noon UTC to avoid TZ edge cases.
    Full timestamps -> relative (:R).
    """
    if not date_str:
        return "Unknown"
    try:
        if _is_date_only(date_str):
            # Anchor at 12:00 UTC so Discord shows the correct calendar day everywhere
            day = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc) + timedelta(hours=12)
            return f"<t:{_discord_ts(day)}:D>"
        # Full timestamp -> relative
        dt = isoparse(date_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return f"<t:{_discord_ts(dt)}:R>"
    except Exception:
        # Fallback to raw value if parsing fails
        return str(date_str)

def _parse_dt_any(s: str) -> datetime | None:
    if not s:
        return None
    try:
        dt = isoparse(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None

def _sc_adjust_calendar_day(dt: datetime) -> datetime:
    """
    Shift SoundCloud UTC timestamp by SC_DISPLAY_TZ_OFFSET hours (can be negative).
    Used only for deciding the calendar day to show; time-of-day is discarded.
    """
    if SC_DISPLAY_TZ_OFFSET == 0:
        return dt
    return dt + timedelta(hours=SC_DISPLAY_TZ_OFFSET)

def format_release_date_for_platform(platform: str, release_date: str) -> str:
    """
    Unified field formatter:
    - Spotify:
        * Date-only (YYYY-MM-DD) -> absolute calendar <t:...:D>
        * Full timestamp -> relative <t:...:R>
    - SoundCloud:
        * If date-only -> absolute <t:...:D>
        * If full timestamp -> convert to adjusted local day (offset) and display absolute calendar <t:...:D>
          (prevents 'previous day 8pm' confusion).
    """
    if not release_date:
        return "Unknown"
    # Spotify uses existing _format_discord_release_date logic
    if platform.lower() == "spotify":
        return _format_discord_release_date(release_date)

    if platform.lower() == "soundcloud":
        # Date-only case
        if _is_date_only(release_date):
            return _format_discord_release_date(release_date)
        # Full timestamp -> adjust & show absolute calendar day
        dt = _parse_dt_any(release_date)
        if not dt:
            return release_date
        adj = _sc_adjust_calendar_day(dt)
        # Anchor at noon local (after shift) to stabilize the calendar date across user timezones
        noon = adj.replace(hour=12, minute=0, second=0, microsecond=0)
        return f"<t:{_discord_ts(noon)}:D>"
    return _format_discord_release_date(release_date)

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
    return_heading: bool = False,
    upload_date=None  # NEW: SoundCloud only
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

    # Ensure emoji map & heading
    emoji_map = {
        "playlist": "📂",
        "album": "💿",
        "ep": "🎶",
        "deluxe": "💿",
        "track": "🎵"
    }
    heading_emoji = emoji_map.get(release_type, "🎵")
    heading = f"{heading_emoji} {artist_name} released {_indef_article(release_type)} {release_type}!"

    # Title: do NOT append (Feat. …) for SoundCloud (avoid duplication)
    feat_in_title = _first_feat_name_strict(features)
    title_for_desc = title if platform.lower() == "soundcloud" else (f"{title} (Feat. {feat_in_title})" if feat_in_title else title)

    embed = discord.Embed(
        title=heading,
        description=f"[{title_for_desc}]({url})" if title_for_desc and url else (title_for_desc or url or "Release"),
        color=color
    )
    # Add duration if non-empty string (including '0:00')
    if track_count:
        embed.add_field(name="Tracks", value=track_count, inline=True)
    if isinstance(duration, str) and duration.strip():
        embed.add_field(name="Duration", value=duration, inline=True)

    # Release Date (use absolute date for YYYY-MM-DD, relative otherwise)
    if release_date:
        embed.add_field(name="Release Date", value=format_release_date_for_platform(platform, release_date), inline=True)

    if genres:
        if isinstance(genres, list):
            clean = [g for g in genres if g]
            if clean:
                embed.add_field(name="Genres" if len(clean) > 1 else "Genre", value=', '.join(clean)[:1024], inline=True)
        else:
            gtxt = str(genres).strip()
            if gtxt and gtxt.lower() != "none":
                embed.add_field(name="Genre", value=gtxt[:1024], inline=True)

    # Features
    if isinstance(features, list):
        feat_text = ", ".join([f for f in features if f]) or None
    elif isinstance(features, str):
        s = features.strip()
        feat_text = s if (s and s.lower() != "none") else None
    else:
        feat_text = None
    if feat_text:
        embed.add_field(name="Features", value=feat_text[:1024], inline=True)

    # Upload Date (SoundCloud only), only if present and different from Release Date
    if platform.lower() == "soundcloud" and upload_date:
        rd_ts = _discord_ts(isoparse(release_date)) if (release_date and 'T' in release_date) else None
        up_dt = isoparse(upload_date)
        up_ts = _discord_ts(up_dt)
        if up_ts and (not rd_ts or up_ts != rd_ts):
            embed.add_field(name="Upload Date", value=f"<t:{up_ts}:R>", inline=True)

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
    upload_date=None,  # NEW
    *,
    original_artist=None
):
    """Create an embed for a reposted track."""
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

    # Parse timestamps with robust helper
    release_timestamp = _to_unix_ts(release_date)
    repost_timestamp = _to_unix_ts(reposted_date)

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

    # Title: NEVER append (Feat. …) in reposts
    title_for_desc = title

    embed = discord.Embed(
        title=f"📢 {reposted_by} reposted {_indef_article(repost_type)} {repost_type}!",
        description=f"[{title_for_desc}]({url})" if title_for_desc and url else (title_for_desc or url or "Repost"),
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

    # Row 3: Upload Date (only if different), Features (only if present)
    up_ts = _to_unix_ts(upload_date) if upload_date else None
    if up_ts and (not release_timestamp or up_ts != release_timestamp):
        embed.add_field(name="Upload Date", value=f"<t:{up_ts}:R>", inline=True)

    if features:
        ftxt = ", ".join([f for f in features if f]) if isinstance(features, list) else (str(features).strip() if features else "")
        if ftxt and ftxt.lower() != "none":
            embed.add_field(name="Features", value=ftxt[:1024], inline=True)

    # High-res thumbnail like like embed
    if cover_url:
        high_res_cover = get_highest_quality_artwork(cover_url)
        embed.set_thumbnail(url=high_res_cover or cover_url)

    return embed

def create_like_embed(
    platform,
    liked_by,
    title,
    artist_name=None,
    url=None,
    release_date=None,
    liked_date=None,
    cover_url=None,
    features=None,
    track_count=None,
    duration=None,
    genres=None,
    content_type=None,
    upload_date=None,  # NEW (now used)
    original_artist=None
):
    """Create an embed for a liked item (track / album / ep / playlist)."""
    display_artist = artist_name or original_artist or "Unknown"

    # Normalize track_count
    if not track_count or track_count == 0:
        track_count = 1

    # Duration formatting (mirror repost embed hour support)
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
            elif len(parts) == 3:
                # Already H:MM:SS
                pass
        except ValueError:
            pass

    # Parse timestamps with robust helper
    release_timestamp = _to_unix_ts(release_date)
    like_timestamp = _to_unix_ts(liked_date)

    # Determine like_type (existing logic)
    like_type = (content_type or "").lower()
    if like_type not in ("album", "ep", "playlist", "track"):
        title_lower = (title or "").lower()
        is_playlist_url = bool(url and "/sets/" in url)
        explicit_album = any(k in title_lower for k in ["album"," lp"," record"])
        explicit_ep = any(k in title_lower for k in [" ep","extended play"])
        if is_playlist_url:
            if explicit_album:
                like_type = "album"
            elif explicit_ep:
                like_type = "ep"
            else:
                like_type = "playlist"
        else:
            if explicit_album:
                like_type = "album"
            elif explicit_ep:
                like_type = "ep"
            else:
                like_type = "track"

    # Title: NEVER append (Feat. …) in likes
    title_for_desc = title

    embed = discord.Embed(
        title=f"❤️ {liked_by} liked {_indef_article(like_type)} {like_type}!",
        description=f"[{title_for_desc}]({url})" if title_for_desc and url else (title_for_desc or url or "Like"),
        color=0xfa5a02
    )

    # First row: By, Tracks, Duration
    embed.add_field(name="By", value=display_artist, inline=True)
    if track_count:
        embed.add_field(name="Tracks", value=track_count, inline=True)
    if duration:
        embed.add_field(name="Duration", value=duration, inline=True)

    # Second row: Release Date, Liked
    if release_timestamp:
        if platform.lower() == "soundcloud":
            # Match repost style (relative)
            embed.add_field(name="Release Date", value=f"<t:{release_timestamp}:R>", inline=True)
        else:
            # Spotify keeps platform-aware formatting
            embed.add_field(name="Release Date", value=format_release_date_for_platform(platform, release_date), inline=True)
    if like_timestamp:
        embed.add_field(name="Liked", value=f"<t:{like_timestamp}:R>", inline=True)

    # Always add Genre(s) (even if None) to mirror repost embed
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

    # Row 3: Upload Date and Features
    up_ts = _to_unix_ts(upload_date) if upload_date else None
    if up_ts and (not release_timestamp or up_ts != release_timestamp):
        embed.add_field(name="Upload Date", value=f"<t:{up_ts}:R>", inline=True)
    if features:
        ftxt = ", ".join([f for f in features if f]) if isinstance(features, list) else (str(features).strip() if features else "")
        if ftxt and ftxt.lower() != "none":
            embed.add_field(name="Features", value=ftxt[:1024], inline=True)

    if cover_url:
        high_res_cover = get_highest_quality_artwork(cover_url)
        embed.set_thumbnail(url=high_res_cover or cover_url)

    return embed