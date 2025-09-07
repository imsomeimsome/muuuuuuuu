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

    # Ensure emoji map & duration field logic (re‑add or reinforce)
    emoji_map = {
        "playlist": "📂",
        "album": "💿",
        "ep": "🎶",
        "deluxe": "💿",
        "track": "🎵"
    }

    # Build "(Feat. X)" suffix for title if any features present
    def _first_feat_name(f):
        if not f:
            return None
        if isinstance(f, list):
            for n in f:
                if n:
                    return str(n).strip()
            return None
        s = str(f).strip()
        return s.split(",")[0].strip() if s else None

    heading_emoji = emoji_map.get(release_type, "🎵")
    heading = f"{heading_emoji} {artist_name} released {_indef_article(release_type)} {release_type}!"
    feat_in_title = _first_feat_name(features)
    title_for_desc = f"{title} (Feat. {feat_in_title})" if feat_in_title else title

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
            # Parse date-only and render as relative time
            ts = None
            try:
                if len(rd) >= 10:
                    dt = datetime.strptime(rd[:10], '%Y-%m-%d').replace(tzinfo=timezone.utc)
                    ts = int(dt.timestamp())
                elif len(rd) == 7:
                    # YYYY-MM -> assume first of month
                    dt = datetime.strptime(rd, '%Y-%m').replace(day=1, tzinfo=timezone.utc)
                    ts = int(dt.timestamp())
                elif len(rd) == 4:
                    # YYYY -> assume Jan 1st
                    dt = datetime.strptime(rd, '%Y').replace(tzinfo=timezone.utc)
                    ts = int(dt.timestamp())
            except Exception:
                ts = None
            if ts is not None:
                embed.add_field(name="Release Date", value=f"<t:{ts}:R>", inline=True)
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

    # Features (now for all platforms, next to Genres)
    feat_text = None
    if isinstance(features, list):
        feat_text = ", ".join([f for f in features if f]) if features else None
    elif isinstance(features, str):
        s = features.strip()
        if s and s.lower() != "none":
            feat_text = s
    if feat_text:
        embed.add_field(name="Features", value=feat_text[:1024], inline=True)

    # Upload Date (SoundCloud only), only if present and different from Release Date
    if platform.lower() == "soundcloud" and upload_date:
        def _to_ts(s):
            try:
                s2 = str(s).replace('Z', '+0000')
                for fmt in ('%Y-%m-%dT%H:%M:%S%z','%Y-%m-%dT%H:%M:%S.%f%z'):
                    try:
                        return int(datetime.strptime(s2, fmt).timestamp())
                    except Exception:
                        continue
                return int(datetime.strptime(str(s)[:10], '%Y-%m-%d').replace(tzinfo=timezone.utc).timestamp())
            except Exception:
                return None
        rd_ts = _to_ts(release_date) if release_date else None
        up_ts = _to_ts(upload_date)
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

    # Append (Feat. X) to description title if available
    def _first_feat_name(f):
        if not f:
            return None
        if isinstance(f, list):
            for n in f:
                if n:
                    return str(n).strip()
            return None
        s = str(f).strip()
        return s.split(",")[0].strip() if s else None
    feat_in_title = _first_feat_name(features)
    title_for_desc = f"{title} (Feat. {feat_in_title})" if feat_in_title else title

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
    def _to_ts(s):
        try:
            s2 = str(s).replace('Z', '+0000')
            for fmt in ('%Y-%m-%dT%H:%M:%S%z','%Y-%m-%dT%H:%M:%S.%f%z'):
                try:
                    return int(datetime.strptime(s2, fmt).timestamp())
                except Exception:
                    continue
            return int(datetime.strptime(str(s)[:10], '%Y-%m-%d').replace(tzinfo=timezone.utc).timestamp())
        except Exception:
            return None
    up_ts = _to_ts(upload_date) if upload_date else None
    if up_ts and (not release_timestamp or up_ts != release_timestamp):
        embed.add_field(name="Upload Date", value=f"<t:{up_ts}:R>", inline=True)

    if features:
        if isinstance(features, list):
            ftxt = ", ".join([f for f in features if f])
        else:
            ftxt = str(features).strip()
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
    """Create an embed for a liked item (track / album / ep / playlist) mirroring repost embed styling."""
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
            try:
                release_timestamp = int(datetime.strptime(str(release_date)[:10], '%Y-%m-%d').replace(tzinfo=timezone.utc).timestamp())
            except Exception:
                release_timestamp = None

    like_timestamp = None
    if liked_date:
        ld = str(liked_date).replace('Z', '+0000')
        for fmt in ('%Y-%m-%dT%H:%M:%S%z', '%Y-%m-%dT%H:%M:%S.%f%z'):
            try:
                like_timestamp = int(datetime.strptime(ld, fmt).timestamp())
                break
            except Exception:
                continue
        if like_timestamp is None:
            try:
                like_timestamp = int(datetime.strptime(str(liked_date)[:10], '%Y-%m-%d').replace(tzinfo=timezone.utc).timestamp())
            except Exception:
                like_timestamp = None

    # Determine type (prefer upstream classification)
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

    # Append (Feat. X) to description title if available
    def _first_feat_name(f):
        if not f:
            return None
        if isinstance(f, list):
            for n in f:
                if n:
                    return str(n).strip()
            return None
        s = str(f).strip()
        return s.split(",")[0].strip() if s else None
    feat_in_title = _first_feat_name(features)
    title_for_desc = f"{title} (Feat. {feat_in_title})" if feat_in_title else title

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
        embed.add_field(name="Release Date", value=f"<t:{release_timestamp}:R>", inline=True)
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

    # Row 3: Upload Date (only if different), Features (only if present)
    def _to_ts(s):
        try:
            s2 = str(s).replace('Z', '+0000')
            for fmt in ('%Y-%m-%dT%H:%M:%S%z','%Y-%m-%dT%H:%M:%S.%f%z'):
                try:
                    return int(datetime.strptime(s2, fmt).timestamp())
                except Exception:
                    continue
            return int(datetime.strptime(str(s)[:10], '%Y-%m-%d').replace(tzinfo=timezone.utc).timestamp())
        except Exception:
            return None
    up_ts = _to_ts(upload_date) if upload_date else None
    if up_ts and (not release_timestamp or up_ts != release_timestamp):
        embed.add_field(name="Upload Date", value=f"<t:{up_ts}:R>", inline=True)

    if features:
        if isinstance(features, list):
            ftxt = ", ".join([f for f in features if f])
        else:
            ftxt = str(features).strip()
        if ftxt and ftxt.lower() != "none":
            embed.add_field(name="Features", value=ftxt[:1024], inline=True)

    if cover_url:
        high_res_cover = get_highest_quality_artwork(cover_url)
        embed.set_thumbnail(url=high_res_cover or cover_url)

    return embed