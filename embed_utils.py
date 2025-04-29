import discord

def create_music_embed(platform, artist_name, title, url, release_date, cover_url, features, track_count, duration, repost, genres=None, custom_color=None):
    # Platform default colors
    platform_color = 0x1DB954 if platform == "spotify" else 0xFF5500
    embed_color = int(custom_color, 16) if custom_color else platform_color

    # Emoji based on type
    if "deluxe" in title.lower():
        emoji = "📀"
    elif track_count >= 7:
        emoji = "💿"
    elif track_count >= 3:
        emoji = "🎶"
    elif track_count == 2 or track_count == 1:
        emoji = "🎵"
    else:
        emoji = "🔊"

    if repost:
        emoji += " 📢"  # new repost emoji

    # Description with genres
    description = f"[{title}]({url})\n\n"
    if genres:
        description += f"**Genres:** {', '.join(genres[:3])}\n"
    description += (
        f"**Duration**\n{duration}\n"
        f"**Tracks**\n{track_count}\n"
        f"**Features**\n{features}\n"
        f"**Released on** {release_date[:10]}"
    )

    embed = discord.Embed(
        title=f"{emoji} New {artist_name} Release!",
        description=f"[{title}]({url})\n\n"
                    f"**Duration**\n{duration}\n"
                    f"**Tracks**\n{track_count}\n"
                    f"**Features**\n{features}\n"
                    f"**Released on** {release_date}",
        color=embed_color
    )
    embed.set_thumbnail(url=cover_url)
    return embed
