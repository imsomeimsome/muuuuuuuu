
# Discord Music Tracker Bot

A powerful Discord bot that tracks new releases from Spotify and SoundCloud artists.

## Features

✅ Spotify Releases (Albums, Singles, EPs)  
✅ SoundCloud Releases (Tracks)  
✅ SoundCloud Playlists/Albums  
✅ SoundCloud Reposts (Special embed style)  
✅ SoundCloud Likes (New!)  
✅ Genres, Release Type, Features all shown in embed  
✅ Async safe → no freezing / no heartbeat issues  
✅ Staggered artist checking → avoids burst API usage and rate limits  
✅ Caching to reduce API hits  
✅ Database-backed artist tracking (with guild support)  
✅ Fully automatic posting to Discord channels

## Setup

### Requirements

- Python 3.10+
- `pip install -r requirements.txt` (dependencies)

### Environment Variables

Create `.env` file or use environment vars:

```
DISCORD_TOKEN=your_discord_token_here
SPOTIFY_CLIENT_ID=your_spotify_client_id_here
SPOTIFY_CLIENT_SECRET=your_spotify_client_secret_here
SOUNDCLOUD_CLIENT_ID=your_soundcloud_client_id_here
LOG_CHANNEL_ID=your_log_channel_id_here (optional, default 0)
```

### Running the bot

```bash
python bot.py
```

Once the bot is online, you can use:

```
/track <Spotify or SoundCloud Artist URL>
```
To start tracking any artist!

## Automatic Background Checks

- Every 5 minutes:
  - Checks all tracked artists for new releases, playlists, reposts, likes
  - Automatically posts in the correct configured channels

## Discord Commands

- `/track` → Start tracking an artist
- `/untrack` → Stop tracking an artist
- `/channels` → View configured channels
- `/key` → Debug + info command

## Advanced Configuration (Optional)

You can manually configure artist post channels via database if needed.

## Notes

- SoundCloud Likes are now supported and post as releases when new likes are found.
- SoundCloud Playlists and reposts are also supported with special handling.
- This bot is async safe and production ready.

## LICENSE

MIT License. Use freely.
