import os
from urllib.parse import urlparse
from dotenv import load_dotenv
import spotipy
import logging
from spotipy.oauth2 import SpotifyClientCredentials

# Load environment variables
load_dotenv()
SPOTIFY_CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID")
SPOTIFY_CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET")

# === PATCHED: Enhanced exception handling and rate limiting ===

import time
from spotipy.exceptions import SpotifyException

import asyncio

# Gather multiple Spotify credential sets (primary + backups)
SPOTIFY_KEYS = [
    (os.getenv("SPOTIFY_CLIENT_ID"), os.getenv("SPOTIFY_CLIENT_SECRET")),
    (os.getenv("SPOTIFY_CLIENT_ID_2"), os.getenv("SPOTIFY_CLIENT_SECRET_2")),
    (os.getenv("SPOTIFY_CLIENT_ID_3"), os.getenv("SPOTIFY_CLIENT_SECRET_3")),
]

# Filter out incomplete pairs
SPOTIFY_KEYS = [(cid, sec) for cid, sec in SPOTIFY_KEYS if cid and sec]

spotify_key_manager = None  # will be initialized via init_spotify_key_manager

class SpotifyKeyManager:
    def __init__(self, bot=None):
        self.bot = bot
        self.keys = SPOTIFY_KEYS
        self.index = 0
        self.key_cooldowns = {}  # index -> datetime when usable again
        if not self.keys:
            logging.error("❌ No Spotify credentials found in environment.")
        else:
            logging.info(f"✅ Loaded {len(self.keys)} Spotify credential set(s). Using index 0.")

    def get_current_key(self):
        if not self.keys:
            raise ValueError("No Spotify credentials configured.")
        return self.keys[self.index]

    def mark_rate_limited(self, minutes=15):
        # Put current key on cooldown
        from datetime import datetime, timezone, timedelta
        self.key_cooldowns[self.index] = datetime.now(timezone.utc) + timedelta(minutes=minutes)

    def rotate_key(self):
        from datetime import datetime, timezone
        if not self.keys or len(self.keys) == 1:
            logging.warning("⚠️ Cannot rotate Spotify key (only one set configured).")
            return None
        start = self.index
        for _ in range(len(self.keys)):
            self.index = (self.index + 1) % len(self.keys)
            # Skip if still on cooldown
            cooldown_until = self.key_cooldowns.get(self.index)
            if cooldown_until and datetime.now(timezone.utc) < cooldown_until:
                continue
            logging.info(f"🔄 Switching to Spotify key index {self.index}")
            return self.get_current_key()
        logging.error("❌ All Spotify keys are on cooldown.")
        return None

    async def log_key_rotation(self, old_index, new_index, reason):
        if not self.bot:
            return
        from datetime import datetime, timezone
        lines = []
        now = datetime.now(timezone.utc)
        for i, (cid, _) in enumerate(self.keys):
            if i == new_index:
                status = "▶️ Active"
            else:
                cd = self.key_cooldowns.get(i)
                if cd and cd > now:
                    status = f"⏳ Cooldown until {cd.isoformat()}"
                else:
                    status = "✅ Ready"
            lines.append(f"Key {i+1}: {status} (client_id={cid[:10]}…)")
        message = (
            "🔄 **Spotify Key Rotation**\n"
            f"Reason: {reason}\n"
            f"Switched from Key {old_index+1} ➜ Key {new_index+1}\n\n"
            + "\n".join(lines)
        )
        # Send to all guild log channels
        try:
            from database_utils import get_channel
            for guild in self.bot.guilds:
                channel_id = get_channel(str(guild.id), "logs")
                if channel_id:
                    ch = self.bot.get_channel(int(channel_id))
                    if ch:
                        await ch.send(message)
        except Exception as e:
            logging.error(f"Failed to send Spotify rotation log: {e}")

def _rebuild_spotify_client(client_id, client_secret):
    global spotify
    spotify = spotipy.Spotify(
        auth_manager=SpotifyClientCredentials(
            client_id=client_id,
            client_secret=client_secret
        )
    )
    logging.info(f"✅ Reinitialized Spotify client with key {client_id[:10]}…")

def init_spotify_key_manager(bot=None):
    """Call from bot startup to enable rotation + logging."""
    global spotify_key_manager
    if spotify_key_manager is None:
        spotify_key_manager = SpotifyKeyManager(bot)
        if spotify_key_manager.keys:
            cid, sec = spotify_key_manager.get_current_key()
            _rebuild_spotify_client(cid, sec)
    return spotify_key_manager

# Replace initial single-client construction with manager-based init guard
if 'spotify_key_manager' not in globals() or spotify_key_manager is None:
    try:
        # Fallback to original single credentials if manager not yet set up
        if SPOTIFY_KEYS:
            cid, sec = SPOTIFY_KEYS[0]
            spotify = spotipy.Spotify(
                auth_manager=SpotifyClientCredentials(
                    client_id=cid,
                    client_secret=sec
                )
            )
        else:
            spotify = None
    except Exception as e:
        logging.error(f"Failed to initialize Spotify client: {e}")
        spotify = None

def _attempt_rotation(reason):
    """Try rotating Spotify credentials; return True if rotated."""
    global spotify_key_manager
    if not spotify_key_manager or not spotify_key_manager.keys:
        return False
    old = spotify_key_manager.index
    new_pair = spotify_key_manager.rotate_key()
    if not new_pair:
        return False
    cid, sec = new_pair
    _rebuild_spotify_client(cid, sec)
    try:
        # schedule async log if event loop running
        loop = asyncio.get_event_loop()
        if loop.is_running():
            asyncio.create_task(spotify_key_manager.log_key_rotation(old, spotify_key_manager.index, reason))
    except RuntimeError:
        pass
    return True

def safe_spotify_call(callable_fn, *args, retries=3, delay=2, **kwargs):
    """Make a Spotify API call with error handling, rate limit + key rotation."""
    global spotify_key_manager
    for attempt in range(retries):
        try:
            return callable_fn(*args, **kwargs)
        except SpotifyException as e:
            status = getattr(e, "http_status", None)
            msg_lc = str(e).lower()
            # Handle invalid credentials
            if "invalid_client" in msg_lc or status in (400, 401):
                logging.error("❌ Spotify invalid/expired credentials. Rotating...")
                if _attempt_rotation("invalid_client"):
                    continue
                raise
            # Rate limit
            if status == 429:
                wait = 0
                if hasattr(e, "headers") and e.headers and e.headers.get("Retry-After"):
                    try:
                        wait = int(e.headers["Retry-After"])
                    except ValueError:
                        wait = 5
                logging.warning(f"⚠️ Spotify rate limited (Retry-After {wait}s). Rotating...")
                if spotify_key_manager:
                    spotify_key_manager.mark_rate_limited(minutes=max(1, wait // 60))
                rotated = _attempt_rotation("rate_limit")
                if rotated:
                    continue
                # If rotation failed, sleep then retry same key
                time.sleep(wait or delay)
                continue
            # 5xx transient
            if status and 500 <= status < 600:
                logging.warning(f"⚠️ Spotify server error {status}. Retry {attempt+1}/{retries}")
                time.sleep(delay)
                continue
            logging.error(f"Spotify API error (no rotation): {e}")
            break
        except Exception as e:
            logging.error(f"Unexpected Spotify error: {e}")
            time.sleep(delay)
            continue
    return None

# --- Utilities ---

def extract_spotify_id(url):
    """Extract the Spotify artist or album ID from a URL."""
    parsed_url = urlparse(url)
    if "spotify.com" in parsed_url.netloc:
        if "/artist/" in parsed_url.path:
            return parsed_url.path.split("/artist/")[1].split("?")[0]
        elif "/album/" in parsed_url.path:
            return parsed_url.path.split("/album/")[1].split("?")[0]
    return None

# --- Artist Data ---

def get_artist_name(artist_id):
    """Fetch the artist's display name by Spotify artist ID."""
    try:
        artist = safe_spotify_call(spotify.artist, artist_id)
        if artist:
            return artist["name"]
    except Exception as e:
        print(f"Error fetching artist name for {artist_id}: {e}")
    return "Unknown Artist"

def get_artist_info(artist_id):
    """Fetch artist info including genres and URL."""
    try:
        artist = safe_spotify_call(spotify.artist, artist_id)
        if not artist:
            return None
        return {
            'name': artist['name'],
            'genres': artist.get('genres', []),
            'url': artist['external_urls']['spotify'],
            'popularity': artist.get('popularity', 0)
        }
    except Exception as e:
        print(f"Error fetching artist info for {artist_id}: {e}")
        return None

def get_artist_discography(artist_id):
    """Get full discography with genre tagging."""
    try:
        albums = safe_spotify_call(spotify.artist_albums, artist_id, album_type='album,single')
        artist = safe_spotify_call(spotify.artist, artist_id)
        if not albums or not artist:
            return []
        artist_genres = artist.get('genres', [])
        return [{
            'id': album['id'],
            'name': album['name'],
            'type': album['album_type'],
            'release_date': album['release_date'],
            'genres': artist_genres
        } for album in albums['items']]
    except Exception as e:
        print(f"Error fetching discography for {artist_id}: {e}")
        return []

def search_artist(query):
    """Search artists by name."""
    try:
        results = safe_spotify_call(spotify.search, q=query, type='artist', limit=5)
        if not results:
            return []
        return [{
            'id': item['id'],
            'name': item['name'],
            'genres': item.get('genres', []),
            'popularity': item.get('popularity', 0)
        } for item in results['artists']['items']]
    except Exception as e:
        print(f"Error searching artist '{query}': {e}")
        return []

# --- Release Data ---

def get_last_release_date(artist_id):
    """Fetch the most recent release date for an artist."""
    try:
        releases = safe_spotify_call(
            spotify.artist_albums,
            artist_id,
            album_type='album,single',
            limit=1
        )
        if releases and releases.get('items'):
            return releases['items'][0]['release_date']
        return "N/A"
    except Exception as e:
        print(f"Error fetching last release date for {artist_id}: {e}")
        return "N/A"

def get_latest_album_id(artist_id):
    """Get the latest album/single ID for an artist."""
    try:
        # Fetch multiple releases
        releases = safe_spotify_call(
            spotify.artist_albums,
            artist_id,
            album_type='album,single',
            limit=10,
            country='US'
        )
        if not releases or not releases.get('items'):
            return None
        
        # Sort releases by release_date
        sorted_releases = sorted(
            releases['items'],
            key=lambda x: x['release_date'],
            reverse=True
        )
        
        latest_album = sorted_releases[0]
        return latest_album['id']
    except Exception as e:
        print(f"Error getting latest album for {artist_id}: {e}")
        return None

def get_release_info(release_id):
    """Fetch detailed release info for a Spotify album or single."""
    try:
        album = safe_spotify_call(spotify.album, release_id)
        if not album:
            return None

        main_artists = [artist['name'] for artist in album['artists']]
        main_artist_ids = [artist['id'] for artist in album['artists']]
        artist_name = ', '.join(main_artists)

        title = album['name']
        release_date = album['release_date']
        cover_url = album['images'][0]['url'] if album.get('images') else None
        track_count = album.get('total_tracks', 0)

        total_ms = sum(track['duration_ms'] for track in album['tracks']['items'])
        minutes, seconds = divmod(total_ms // 1000, 60)
        duration_min = f"{minutes}:{seconds:02d}"

        features = set()
        for track in album['tracks']['items']:
            for artist in track['artists']:
                if artist['name'] not in main_artists:
                    features.add(artist['name'])
        features_str = ", ".join(sorted(features)) if features else "None"

        genres = album.get('genres', [])
        for artist_id in main_artist_ids:
            try:
                artist_info = safe_spotify_call(spotify.artist, artist_id)
                if artist_info:
                    genres.extend(artist_info.get('genres', []))
            except Exception:
                continue
        genres = list(sorted(set(genres)))

        return {
            "artist_name": artist_name,
            "title": title,
            "url": album['external_urls']['spotify'],
            "release_date": release_date,
            "cover_url": cover_url,
            "track_count": track_count,
            "duration": duration_min,
            "features": features_str,
            "genres": genres,
            "repost": False
        }
    except Exception as e:
        print(f"Error fetching release info for {release_id}: {str(e)}")
        return None

import logging

# Custom logging formatter for Railway logs
class RailwayLogFormatter(logging.Formatter):
    COLORS = {
        "DEBUG": "\033[90m",  # Gray
        "INFO": "\033[94m",  # Blue
        "WARNING": "\033[93m",  # Orange
        "ERROR": "\033[91m",  # Red
    }
    RESET = "\033[0m"

    def format(self, record):
        color = self.COLORS.get(record.levelname, self.RESET)
        record.msg = f"{color}{record.msg}{self.RESET}"
        return super().format(record)

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')
logging.getLogger().handlers[0].setFormatter(RailwayLogFormatter())