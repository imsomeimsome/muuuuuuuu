import discord

import discord

def create_music_embed(platform, artist_name, title, url, release_date, cover_url, features, track_count, duration, repost, genres=None, release_type=None, custom_color=None):
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

    if genres and len(genres) > 0:
        description += f"**Genres**\n{', '.join(genres)}\n"
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

def create_repost_embed(platform, reposted_by, original_artist, title, url, release_date, cover_url, features, track_count, duration, genres=None, custom_color=None):
    platform_color = 0x1DB954 if platform == "spotify" else 0xFF5500
    embed_color = int(custom_color, 16) if custom_color else platform_color

    emoji = "📢"

    description = f"[{title}]({url})\n\n"
    description += f"**By**\n{original_artist}\n"

    if genres and len(genres) > 0:
        description += f"**Genres**\n{', '.join(genres)}\n"
    else:
        description += f"**Genres**\nNone\n"

    if duration:
        description += f"**Duration**\n{duration}\n"

    description += f"**Tracks**\n{track_count}\n"
    description += f"**Features**\n{features or 'None'}\n"
    description += f"**Original Release Date**\n{release_date[:10]}"

    embed = discord.Embed(
        title=f"{emoji} New {reposted_by} Repost!",
        description=description,
        color=embed_color
    )
    embed.set_thumbnail(url=cover_url)

    return embed


    # Build embed description
    description = f"[{title}]({url})\n\n"
    
    # ✅ Genres (Always show, or show "None" if empty)
    if genres and len(genres) > 0:
        description += f"**Genres**\n{', '.join(genres[:3])}\n"
    else:
        description += f"**Genres**\nNone\n"

    description += (
        f"**Duration**\n{duration}\n"
        f"**Tracks**\n{track_count}\n"
        f"**Features**\n{features}\n"
        f"**Released on** {release_date[:10]}"
    )

    # Create embed with correct description
    embed = discord.Embed(
        title=f"{emoji} New {artist_name} Release!",
        description=description,
        color=embed_color
    )

    embed.set_thumbnail(url=cover_url)
    return embed
