import discord

def create_embed(title, description, url=None, thumbnail_url=None):
    """
    Create a Discord embed message.
    :param title: Embed title
    :param description: Embed description
    :param url: Optional URL for the embed
    :param thumbnail_url: Optional thumbnail URL
    :return: discord.Embed object
    """
    embed = discord.Embed(title=title, description=description, color=discord.Color.blue())
    if url:
        embed.url = url
    if thumbnail_url:
        embed.set_thumbnail(url=thumbnail_url)
    return embed