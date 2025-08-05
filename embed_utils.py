import discord

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
    if genres:
        if isinstance(genres, list):
            genre_text = ", ".join(genres)
        else:
            genre_text = str(genres)
        description += f"**Genres**\n{genre_text}\n"
    else:
        description += f"**Genres**\nNone\n"

    if duration:
        description += f"**Duration**\n{duration}\n"

    description += f"**Tracks**\n{track_count}\n"
    description += f"**Features**\n{features or 'None'}\n"
    description += f"**Repost?**\n{'Yes' if repost else 'No'}\n"
    description += f"**Released on** {release_date[:10]}"

    embed = discord.Embed(
        title=f"{emoji} New {artist_name} Release!",
        description=description,
        color=embed_color
    )
    embed.set_thumbnail(url=cover_url)

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

def create_like_embed(platform, liked_by, title, artist_name, url, release_date, cover_url=None, features=None, track_count=None, duration=None, genres=None):
    """Create an embed for a liked track."""
    embed = discord.Embed(
        title=f"❤️ {liked_by} liked a track!",
        description=f"[{title}]({url})",
        color=0xff5c5c
    )

    if artist_name:
        embed.add_field(name="Artist", value=artist_name, inline=True)
    if release_date:
        embed.add_field(name="Release Date", value=release_date, inline=True)
    if duration:
        embed.add_field(name="Duration", value=duration, inline=True)
    if track_count:
        embed.add_field(name="Tracks", value=track_count, inline=True)
    if genres:
        if isinstance(genres, list):
            genre_text = ", ".join(genres)
        else:
            genre_text = str(genres)
        embed.add_field(name="Genres", value=genre_text, inline=True)
    if features:
        embed.add_field(name="Features", value=features, inline=True)
    if cover_url:
        embed.set_thumbnail(url=cover_url)

    embed.set_footer(text=f"Platform: {platform.title()}")
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
