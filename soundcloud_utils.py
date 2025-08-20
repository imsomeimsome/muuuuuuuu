import os
import requests
import re
import time
import sqlite3
import asyncio
from urllib.parse import urlparse
from dotenv import load_dotenv
from datetime import datetime, timezone, timedelta
import logging
from utils import get_cache, set_cache, delete_cache
import json
from database_utils import DB_PATH, get_channel
from dateutil.parser import parse as isoparse


# At the top after imports
load_dotenv()

# Initialize global variables
CLIENT_ID = os.getenv("SOUNDCLOUD_CLIENT_ID")
key_manager = None

class SoundCloudKeyManager:
    def __init__(self, bot=None):
        self.bot = bot  # Store bot reference for logging
        self.api_keys = [
            os.getenv("SOUNDCLOUD_CLIENT_ID"),
            os.getenv("SOUNDCLOUD_CLIENT_ID_2"),
            os.getenv("SOUNDCLOUD_CLIENT_ID_3")
#            os.getenv("SOUNDCLOUD_CLIENT_ID_4")
        ]
        self.current_key_index = 0
        self.key_cooldowns = {}  # Track when each key hits rate limit


    async def log_key_rotation(self, old_index, new_index):
        """Log key rotation to Discord logs channel with status of all keys."""
        if self.bot:
            try:
                now = datetime.now(timezone.utc)
                status_lines = []

                # Build status for each key
                for i, key in enumerate(self.api_keys):
                    if not key:
                        status = "Not configured"
                    elif i == new_index:
                        status = "Currently in use"
                    elif i in self.key_cooldowns:
                        cooldown_time = int(self.key_cooldowns[i].timestamp())
                        status = f"Cooldown until <t:{cooldown_time}:R>"
                    else:
                        status = "Ready for use"
                    
                    status_lines.append(f"ID {i+1}: {status}")

                message = (
                    "🔄 **SoundCloud API Key Status Update**\n"
                    f"Switched from Key {old_index + 1} to Key {new_index + 1}\n\n"
                    "**Current Key Status:**\n" + 
                    "\n".join(status_lines)
                )

                # Send to all configured log channels
                for guild in self.bot.guilds:
                    channel_id = get_channel(str(guild.id), "logs")
                    if channel_id:
                        try:
                            channel = self.bot.get_channel(int(channel_id))
                            if channel:
                                await channel.send(message)
                        except Exception as e:
                            logging.error(f"Failed to send key rotation log to guild {guild.id}: {e}")

            except Exception as e:
                logging.error(f"Failed to log key rotation: {e}")

    def get_current_key(self):
        """Get current working API key."""
        return self.api_keys[self.current_key_index]

    def rotate_key(self):
        """Switch to next available API key."""
        old_index = self.current_key_index
        self.key_cooldowns[old_index] = datetime.now(timezone.utc) + timedelta(hours=12)
        
        # Try each key in sequence
        for _ in range(len(self.api_keys)):
            next_index = (self.current_key_index + 1) % len(self.api_keys)
            # Check if next key exists and isn't on cooldown
            if self.api_keys[next_index] and (
                next_index not in self.key_cooldowns or 
                datetime.now(timezone.utc) > self.key_cooldowns[next_index]
            ):
                self.current_key_index = next_index
                new_key = self.get_current_key()
                # Log the rotation
                if self.bot:
                    asyncio.create_task(self.log_key_rotation(old_index, next_index))
                return new_key
            self.current_key_index = next_index  # Move to next key even if it fails
            
        raise ValueError("No API keys available - all on cooldown")
        
        # Find next available key
        for i in range(len(self.api_keys)):
            next_index = (self.current_key_index + i + 1) % len(self.api_keys)
            if self.api_keys[next_index] and (
                next_index not in self.key_cooldowns or 
                datetime.now(timezone.utc) > self.key_cooldowns[next_index]
            ):
                self.current_key_index = next_index
                return self.get_current_key()
        
        raise ValueError("No API keys available - all on cooldown")

def init_key_manager(bot):
    """Initialize the key manager with bot reference."""
    global key_manager, CLIENT_ID
    key_manager = SoundCloudKeyManager(bot)
    CLIENT_ID = key_manager.get_current_key()
    return CLIENT_ID

# Cache duration for repeated SoundCloud lookups
CACHE_TTL = 300  # 5 minutes
# Load environment variables



def resolve_url(url):
    """Resolve a SoundCloud URL to its API data."""
    url = clean_soundcloud_url(url)  # Normalize the URL
    cache_key = f"resolve:{url}"
    cached = get_cache(cache_key)
    if cached:
        return json.loads(cached)

    resolve_endpoint = f"https://api-v2.soundcloud.com/resolve?url={url}&client_id={CLIENT_ID}"
    
    # Try multiple times with key rotation
    for attempt in range(3):
        try:
            response = safe_request(resolve_endpoint)
            if response and response.status_code == 200:
                data = response.json()
                set_cache(cache_key, json.dumps(data), ttl=3600)  # Cache for 1 hour
                return data
                
            # Handle rate limits in safe_request
            if not response:
                logging.warning(f"Failed to resolve URL (attempt {attempt + 1})")
                time.sleep(2)
                continue

        except Exception as e:
            logging.error(f"Error resolving URL: {e}")
            if attempt < 2:  # Try again if not last attempt
                time.sleep(2)
                continue
            break

    return None

def safe_request(url, headers=None, retries=3, timeout=10):
    """Make a request with automatic key rotation on rate limits."""
    global CLIENT_ID, key_manager
    
    if not CLIENT_ID:
        raise ValueError("No SoundCloud CLIENT_ID available")
    
    original_url = url
    for attempt in range(retries):
        try:
            response = requests.get(url, headers=headers or HEADERS, timeout=timeout)
            
            # Enhanced rate limit detection
            is_rate_limited = (
                response.status_code in [401, 429] or
                "rate/request limit" in response.text.lower() or
                "retry will occur after:" in response.text.lower()
            )
            
            if is_rate_limited:
                logging.warning(f"⚠️ Rate limit hit for key {CLIENT_ID[:8]}...")
                try:
                    new_key = key_manager.rotate_key()
                    if new_key:
                        CLIENT_ID = new_key
                        logging.info(f"🔄 Rotated to new key: {new_key[:8]}...")
                        # Update URL with new key and retry
                        url = re.sub(r'client_id=[^&]+', f'client_id={new_key}', original_url)
                        time.sleep(1)  # Small delay between retries
                        continue
                except ValueError as e:
                    logging.error(f"❌ Key rotation failed: {e}")
                    break  # All keys are on cooldown

            if response.status_code == 200:
                return response
                
            response.raise_for_status()
            
        except Exception as e:
            if attempt < retries - 1:
                time.sleep(2)  # Add delay between retries
                continue
            raise
    
    return None

# Global headers for all requests to avoid 403 errors
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Connection": "keep-alive",
}

# Try to refresh client_id automatically when unauthorized
def refresh_client_id():
    """Attempt to fetch a working SoundCloud client ID."""
    global CLIENT_ID
    try:
        for attempt in range(3):  # Retry up to 3 times
            logging.info(f"🔄 Attempting to refresh SoundCloud client ID (Attempt {attempt + 1})...")
            html = requests.get("https://soundcloud.com", headers=HEADERS, timeout=10).text
            match = re.search(r"client_id\s*:\s*\"([a-zA-Z0-9_-]{32})\"", html)
            if match:
                CLIENT_ID = match.group(1)
                logging.info(f"✅ Refreshed SoundCloud client ID: {CLIENT_ID}")
                return CLIENT_ID
            logging.warning(f"⚠️ Attempt {attempt + 1}: Failed to find a new SoundCloud client ID.")
            time.sleep(2)  # Wait before retrying
        logging.error("❌ Failed to find a new SoundCloud client ID after multiple attempts.")
        raise ValueError("Failed to refresh SoundCloud client ID.")
    except requests.exceptions.RequestException as e:
        logging.error(f"❌ Error refreshing SoundCloud client ID: {e}")
        raise ValueError("Error during client ID refresh.")

# Quick helper to verify the configured client ID works
def verify_client_id():
    """Verify if the SoundCloud CLIENT_ID is valid."""
    test_url = f"https://api-v2.soundcloud.com/resolve?url=https://soundcloud.com&client_id={CLIENT_ID}"
    try:
        response = requests.get(test_url, headers=HEADERS, timeout=10)
        if response.status_code == 200:
            logging.info("✅ SoundCloud CLIENT_ID is valid.")
            return True
        elif response.status_code == 403:
            logging.warning("⚠️ SoundCloud CLIENT_ID is forbidden (403).")
            return False
        else:
            logging.error(f"❌ SoundCloud CLIENT_ID verification failed with status code {response.status_code}.")
            return False
    except requests.RequestException as e:
        logging.error(f"❌ Error verifying SoundCloud CLIENT_ID: {e}")
        return False

# --- Core URL Handling ---

def extract_soundcloud_user_id(artist_url):
    """Fetch SoundCloud user ID from artist profile URL."""
    cache_key = f"sc_user_id:{artist_url}"
    cached = get_cache(cache_key)  # Use get_cache
    if cached:
        return cached
    try:
        res = safe_request(
            f"https://api-v2.soundcloud.com/resolve?url={artist_url}&client_id={CLIENT_ID}",
            headers=HEADERS,
        )
        if not res:
            raise ValueError("Request failed")
        data = res.json()
        user_id = data.get("id")
        if user_id:
            set_cache(cache_key, user_id, ttl=CACHE_TTL)  # Use set_cache
        return user_id
    except Exception as e:
        raise ValueError(f"Failed to extract user ID from URL: {e}")



def clean_soundcloud_url(url):
    """Normalize and verify SoundCloud URLs."""
    try:
        # Remove duplicate prefixes
        while "https://soundcloud.com/https://soundcloud.com/" in url:
            url = url.replace("https://soundcloud.com/https://soundcloud.com/", "https://soundcloud.com/")

        # Ensure the URL starts with the correct base
        if not url.startswith("https://soundcloud.com/"):
            raise ValueError(f"Invalid SoundCloud URL: {url}")

        # Handle shortened URLs (e.g., on.soundcloud.com)
        if 'on.soundcloud.com' in url:
            response = requests.head(url, headers=HEADERS, allow_redirects=True, timeout=10)
            url = response.url
            logging.debug(f"🔄 Redirected URL: {url}")

        # Validate domain
        parsed = urlparse(url)
        if 'soundcloud.com' not in parsed.netloc:
            logging.warning(f"⚠️ Invalid SoundCloud domain for URL: {url}")
            raise ValueError("Invalid SoundCloud domain")

        # Validate URL existence
        response = requests.get(url, headers=HEADERS, timeout=10)
        if response.status_code == 404:
            logging.warning(f"⚠️ 404 Not Found for SoundCloud URL: {url}")
            raise ValueError("SoundCloud URL returned 404")
        response.raise_for_status()
        logging.info(f"✅ Successfully validated SoundCloud URL: {url}")

        # Extract canonical URL
        match = re.search(r'<link rel="canonical" href="([^"]+)"', response.text)
        return match.group(1) if match else url

    except Exception as e:
        logging.error(f"❌ URL validation failed for {url}: {e}")
        raise ValueError(f"URL validation failed: {e}")
    
def extract_soundcloud_username(url):
    """Extract the username from a SoundCloud URL."""
    clean_url = clean_soundcloud_url(url)
    parsed = urlparse(clean_url)
    path_segments = [p for p in parsed.path.strip('/').split('/') if p]

    if not path_segments:
        raise ValueError("No path segments in URL")

    return path_segments[0]

# --- Artist Data Fetching ---

def get_artist_info(url_or_username):
    """Resolve a SoundCloud user from a full profile URL or username."""
    url_or_username = clean_soundcloud_url(url_or_username)  # Normalize the URL
    cache_key = f"sc_artist_info:{url_or_username}"
    cached = get_cache(cache_key)  # Use get_cache
    if cached:
        return json.loads(cached)

    try:
        # Extract username from URL if necessary
        if url_or_username.startswith("http"):
            username = extract_soundcloud_username(url_or_username)
        else:
            username = url_or_username

        # Build the resolve URL
        resolve_url = f"https://api-v2.soundcloud.com/resolve?url=https://soundcloud.com/{username}&client_id={CLIENT_ID}"
        response = safe_request(resolve_url, headers=HEADERS)

        # Handle invalid responses
        if not response or response.status_code != 200:
            raise ValueError(f"Failed to resolve SoundCloud user: {url_or_username}")

        data = response.json()

        # Ensure the response contains valid artist data
        if not data or data.get('kind') != 'user':
            raise ValueError(f"Invalid artist data for: {url_or_username}")

        # Extract artist information
        info = {
            'id': data.get('id', username),
            'name': data.get('username', 'Unknown Artist'),
            'url': data.get('permalink_url', f"https://soundcloud.com/{username}"),
            'track_count': data.get('track_count', 0),
            'avatar_url': data.get('avatar_url', ''),
            'followers': data.get('followers_count', 0)
        }

        # Cache the result
        set_cache(cache_key, json.dumps(info), ttl=CACHE_TTL)  # Use set_cache
        return info

    except Exception as e:
        logging.error(f"Error fetching artist info for {url_or_username}: {e}")
        return {'id': url_or_username, 'name': 'Unknown Artist', 'url': f"https://soundcloud.com/{url_or_username}"}
       
# --- Release Data Fetching ---

def get_last_release_date(artist_url):
    cache_key = f"sc_last_release:{artist_url}"
    cached = get_cache(cache_key)  # Use get_cache
    if cached:
        return cached
    try:
        artist_info = get_artist_info(artist_url)
        artist_id = artist_info['id']

        tracks_url = f"https://api-v2.soundcloud.com/users/{artist_id}/tracks?client_id={CLIENT_ID}&limit=5&order=created_at"
        response = safe_request(tracks_url, headers=HEADERS)
        if not response:
            return None
        
        tracks = response.json()
        if not tracks:
            return None

        latest_track = max(tracks, key=lambda t: t['created_at'])
        created = latest_track.get('created_at')
        if created:
            set_cache(cache_key, created, ttl=CACHE_TTL)  # Use set_cache
        return created
    except Exception as e:
        print(f"Error getting last release: {e}")
        return None

def get_release_info(url):
    """Universal release info fetcher for tracks/playlists/artists."""
    cache_key = f"sc_release:{url}"
    cached = get_cache(cache_key)  # Use get_cache
    if cached:
        return json.loads(cached)
    try:
        clean_url = clean_soundcloud_url(url)
        resolve_url = f"https://api-v2.soundcloud.com/resolve?url={clean_url}&client_id={CLIENT_ID}"
        response = safe_request(resolve_url, headers=HEADERS)
        if not response:
            raise ValueError("Request failed")

        data = response.json()

        if data['kind'] == 'track':
            info = process_track(data)
        elif data['kind'] == 'playlist':
            info = process_playlist(data)
        elif data['kind'] == 'user':
            info = get_artist_release(data)
        else:
            raise ValueError("Unsupported content type")
    
        set_cache(cache_key, json.dumps(info), ttl=CACHE_TTL)  # Use set_cache
        return info
    except Exception as e:
        raise ValueError(f"Release info fetch failed: {e}")

def get_soundcloud_playlist_info(artist_url):
    try:
        cache_key = f"playlists:{artist_url}"
        cached = get_cache(cache_key)
        if cached:
            return json.loads(cached) if isinstance(cached, str) else cached

        resolved = get_artist_info(artist_url)
        user_id = resolved.get("id")
        if not user_id:
            raise ValueError(f"Could not resolve user ID for {artist_url}")

        url = f"https://api-v2.soundcloud.com/users/{user_id}/playlists?client_id={CLIENT_ID}&limit=5"
        response = safe_request(url)
        if not response or response.status_code != 200:
            logging.warning(f"No playlists found for {artist_url}")
            return None

        data = response.json()
        playlists = data.get("collection", [])
        if not playlists:
            logging.warning(f"No playlists found for {artist_url}")
            return None

        latest_playlist = max(playlists, key=lambda p: p.get("created_at", ""))
        tracks = []
        
        for index, track in enumerate(latest_playlist.get("tracks", [])):
            if isinstance(track, dict):  # Ensure track is a dictionary
                tracks.append({
                    "id": str(track.get("id")),
                    "title": str(track.get("title")),
                    "duration": track.get("duration"),
                    "order": index
                })

        result = {
            "title": latest_playlist.get("title"),
            "artist_name": latest_playlist.get("user", {}).get("username"),
            "url": latest_playlist.get("permalink_url"),
            "release_date": latest_playlist.get("created_at"),
            "cover_url": latest_playlist.get("artwork_url"),
            "track_count": len(tracks),
            "tracks": tracks
        }

        set_cache(cache_key, json.dumps(result), ttl=300)
        return result

    except Exception as e:
        logging.error(f"Error checking playlists: {e}")
        return None


def get_soundcloud_likes_info(artist_url, force_refresh=False):
    """Fetch and process liked tracks/playlists from a SoundCloud user."""
    try:
        cache_key = f"likes:{artist_url}"
        if not force_refresh:
            cached = get_cache(cache_key)
            if cached:
                logging.info(f"✅ Cache hit for likes: {artist_url}")
                return json.loads(cached)

        logging.info(f"⏳ Fetching likes for {artist_url}...")
        resolved = resolve_url(artist_url)
        if not resolved or "id" not in resolved:
            logging.warning(f"⚠️ Could not resolve SoundCloud user ID from {artist_url}")
            return []

        user_id = resolved["id"]
        url = f"https://api-v2.soundcloud.com/users/{user_id}/likes?client_id={CLIENT_ID}&limit=10"
        response = safe_request(url)
        if not response:
            logging.warning(f"⚠️ No response received for likes: {artist_url}")
            return []

        data = response.json()
        if not data or "collection" not in data:
            logging.warning(f"⚠️ Invalid or empty data received for likes: {artist_url}")
            return []

        likes = []
        for item in data.get("collection", []):
            original = item.get("track") or item.get("playlist")
            if not original:
                continue

            # Determine if it's a playlist or track and get tracks data
            content_type = "playlist" if original.get('kind') == 'playlist' else "track"
            tracks_data = None

            # Get timestamps with fallbacks
            like_date = item.get("created_at")
            if not like_date:
                continue
            
            track_release_date = original.get("created_at") or like_date

            # Handle genres and content type for playlists/albums
            genres = []
            if content_type == "playlist":
                try:
                    playlist_url = original.get('permalink_url')
                    if playlist_url:
                        playlist_resolve_url = f"https://api-v2.soundcloud.com/resolve?url={playlist_url}&client_id={CLIENT_ID}"
                        playlist_response = safe_request(playlist_resolve_url, headers=HEADERS)
                        if playlist_response:
                            playlist_data = playlist_response.json()
                            tracks_data = playlist_data.get('tracks', [])
                            
                            # Determine the actual release type
                            content_type = determine_release_type(playlist_data, tracks_data)
                            
                            # Get genres from all tracks
                            unique_genres = set()
                            for track in tracks_data:
                                if track.get('genre'):
                                    genre = track['genre'].strip()
                                    if genre:
                                        unique_genres.add(genre)
                            
                            # Also check playlist-level genre
                            if playlist_data.get('genre'):
                                unique_genres.add(playlist_data['genre'].strip())
                            
                            genres = sorted(list(unique_genres)) if unique_genres else ["N/A"]
                except Exception as e:
                    logging.warning(f"Error fetching playlist data: {e}")
                    genres = ["N/A"]
            else:
                # Single track genre handling
                if original.get('genre'):
                    genres = [original['genre']]

            # Handle duration formatting
            duration = None
            if original.get('duration'):
                ms = original['duration']
                seconds = ms // 1000
                minutes = seconds // 60
                remaining_seconds = seconds % 60
                if minutes >= 60:
                    hours = minutes // 60
                    minutes = minutes % 60
                    duration = f"{hours}:{minutes:02d}:{remaining_seconds:02d}"
                else:
                    duration = f"{minutes}:{remaining_seconds:02d}"

            likes.append({
                "track_id": original.get("id"),
                "title": original.get("title"),
                "artist_name": original.get("user", {}).get("username"),
                "url": original.get("permalink_url"),
                "upload_date": original.get("created_at"),
                "release_date": original.get("display_date") or original.get("created_at"),
                "liked_date": like_date,
                "cover_url": original.get("artwork_url"),
                "features": extract_features(original.get("title", "")),
                "track_count": original.get("track_count", 1),
                "duration": duration,
                "genres": genres,
                "content_type": content_type,
                "tracks_data": tracks_data if 'tracks_data' in locals() else None,  # Include tracks data
                "liked": True
            })

        set_cache(cache_key, json.dumps(likes), ttl=60)  # Short cache TTL for likes
        return likes

    except Exception as e:
        logging.error(f"Error fetching likes for {artist_url}: {e}")
        return []

    
def get_soundcloud_reposts_info(artist_url):
    """Fetch and process reposts from a SoundCloud user."""
    try:
        cache_key = f"reposts:{artist_url}"
        cached = get_cache(cache_key)
        if cached:
            return json.loads(cached)

        resolved = resolve_url(artist_url)
        if not resolved or "id" not in resolved:
            logging.warning(f"⚠️ Could not resolve SoundCloud user ID from {artist_url}")
            return []

        user_id = resolved["id"]
        # Try multiple repost endpoints
        endpoints = [
            f"https://api-v2.soundcloud.com/users/{user_id}/reposts?client_id={CLIENT_ID}&limit=10",
            f"https://api-v2.soundcloud.com/users/{user_id}/track_reposts?client_id={CLIENT_ID}&limit=10",
            f"https://api-v2.soundcloud.com/stream/users/{user_id}/reposts?client_id={CLIENT_ID}&limit=10"
        ]
        
        response = None
        for endpoint in endpoints:
            try:
                response = safe_request(endpoint)
                if response and response.status_code == 200:
                    break
            except Exception as e:
                logging.debug(f"Failed endpoint {endpoint}: {e}")
                continue

        if not response:
            logging.warning(f"⚠️ Could not fetch reposts from any endpoint for {artist_url}")
            return []

        data = response.json()
        reposts = []

        for item in data.get("collection", []):
            try:
                original = item.get("track") or item.get("playlist")
                if not original:
                    continue

                repost_date = item.get("created_at")
                if not repost_date:
                    continue

                reposts.append({
                    "track_id": str(original.get("id")),
                    "title": original.get("title"),
                    "artist_name": original.get("user", {}).get("username"),
                    "url": original.get("permalink_url"),
                    "release_date": original.get("created_at"),
                    "reposted_date": repost_date,
                    "cover_url": original.get("artwork_url"),
                    "features": extract_features(original.get("title", "")),
                    "track_count": original.get("track_count", 1),
                    "duration": format_duration(original.get("duration", 0)),
                    "genres": [original.get("genre")] if original.get("genre") else []
                })
            except Exception as e:
                logging.warning(f"Error processing repost: {e}")
                continue

        set_cache(cache_key, json.dumps(reposts), ttl=300)
        return reposts

    except Exception as e:
        logging.error(f"Error fetching reposts for {artist_url}: {e}")
        return []
    
# --- Data Processing ---

def process_track(track_data):
    """Convert track data to standardized format."""
    return {
        'type': 'track',
        'artist_name': track_data['user']['username'],
        'title': track_data['title'],
        'url': track_data['permalink_url'],
        'release_date': track_data.get('created_at', ''),
        'cover_url': track_data.get('artwork_url') or track_data['user'].get('avatar_url', ''),
        'duration': format_duration(track_data.get('duration', 0)),
        'features': extract_features(track_data['title']),
        'genres': [track_data.get('genre', '')] if track_data.get('genre') else [],
        'repost': track_data.get('repost', False),
        'track_count': 1
    }

def process_playlist(playlist_data):
    """Convert playlist data to standardized format."""
    total_duration = sum(t.get('duration', 0) for t in playlist_data['tracks'])
    features = set()
    genres = set()

    # Process each track's genres and tags
    for track in playlist_data['tracks']:
        # Add features from track titles
        features.update(extract_features(track['title']).split(', '))
        
        # Add direct genre
        if track.get('genre'):
            genres.add(track.get('genre'))
            
        # Add genre tags
        if track.get('tags'):
            genres.update([
                tag.strip() for tag in track.get('tags', '').split() 
                if 'genre:' in tag.lower() or 
                any(g in tag.lower() for g in ['rap', 'hip-hop', 'trap', 'edm', 'electronic', 'rock'])
            ])

    # Also check playlist-level genres/tags
    if playlist_data.get('genre'):
        genres.add(playlist_data.get('genre'))
    if playlist_data.get('tags'):
        genres.update([
            tag.strip() for tag in playlist_data.get('tags', '').split() 
            if 'genre:' in tag.lower() or 
            any(g in tag.lower() for g in ['rap', 'hip-hop', 'trap', 'edm', 'electronic', 'rock'])
        ])

    # Clean up sets
    features.discard('None')
    genres.discard('None')
    genres.discard('')

    return {
        'type': 'playlist',
        'artist_name': playlist_data['user']['username'],
        'title': playlist_data['title'],
        'url': playlist_data['permalink_url'],
        'release_date': playlist_data.get('created_at', ''),
        'cover_url': playlist_data.get('artwork_url') or playlist_data['user'].get('avatar_url', ''),
        'duration': format_duration(total_duration),
        'features': ', '.join(sorted(features)) if features else None,
        'genres': sorted(list(genres)) if genres else ['Unknown'],  # Return list of genres or ['Unknown']
        'repost': False,
        'track_count': len(playlist_data['tracks'])
    }

def get_artist_release(artist_data):
    """Get latest track release for artist."""
    try:
        tracks_url = (
            f"https://api-v2.soundcloud.com/users/{artist_data['id']}/tracks"
            f"?client_id={CLIENT_ID}&limit=1&linked_partitioning=1&representation=full"
        )
        response = safe_request(tracks_url, headers=HEADERS)
        if not response:
            logging.warning("SoundCloud artist tracks request failed")
            return None

        data = response.json()


        # Some responses return 'collection', not raw list
        tracks = data.get('collection', data if isinstance(data, list) else [])

        if not tracks:
            return None

        return process_track(tracks[0])
    except Exception as e:
        logging.error(f"Artist release fetch failed: {e}")
        return None

# --- Utility Functions ---

def format_duration(ms):
    """Convert milliseconds to formatted duration string."""
    if not ms:
        return None
    seconds = ms // 1000
    minutes = seconds // 60
    seconds = seconds % 60
    if minutes >= 60:
        hours = minutes // 60
        minutes = minutes % 60
        return f"{hours}:{minutes:02d}:{seconds:02d}"
    return f"{minutes}:{seconds:02d}"


def extract_features(title):
    """Extract featured artists from track titles."""
    patterns = [
        r"\((?:feat|ft|with)\.?\s*([^)]+)\)",
        r"\[(?:feat|ft|with)\.?\s*([^\]]+)\]",
        r"(?:feat|ft|with)\.?\s+([^\-–()\[\]]+)",
        r"w/\s*([^\-–()\[\]]+)"
    ]
    features = set()

    for pattern in patterns:
        matches = re.findall(pattern, title, re.IGNORECASE)
        for match in matches:
            cleaned = match.strip()
            for sep in ['/', '&', ',', ' and ', ' x ']:
                cleaned = cleaned.replace(sep, ',')
            features.update(
                [name.strip() for name in cleaned.split(',') if name.strip()]
            )
    return ", ".join(sorted(features)) if features else "None"

# --- Bot Integration Helpers ---

def get_soundcloud_artist_name(url):
    """Get artist name from SoundCloud profile URL."""
    try:
        return get_artist_info(url)['name']
    except Exception as e:
        print(f"Error getting artist name: {e}")
        return "Unknown Artist"

def get_artist_name_by_url(url):
    """Get artist name from any SoundCloud URL."""
    return get_soundcloud_artist_name(url)

def get_soundcloud_artist_id(url):
    """Get the numeric artist ID from a SoundCloud URL."""
    try:
        artist_info = get_artist_info(url)
        return artist_info['id']
    except Exception as e:
        print(f"Error getting artist ID: {e}")
        return None

# In soundcloud_utils.py
def get_soundcloud_release_info(url):
    """Universal release info fetcher for tracks/playlists/artists."""
    try:
        cache_key = f"sc_release:{url}"
        cached = get_cache(cache_key)
        if cached:
            return json.loads(cached)

        clean_url = clean_soundcloud_url(url)
        resolve_url = f"https://api-v2.soundcloud.com/resolve?url={clean_url}&client_id={CLIENT_ID}"
        response = safe_request(resolve_url, headers=HEADERS)
        if not response:
            logging.warning(f"⚠️ Could not resolve URL: {url}")
            return None

        data = response.json()
        if not data:
            logging.warning(f"⚠️ Empty response for URL: {url}")
            return None

        # Handle user profile
        if data.get('kind') == 'user':
            user_id = data.get('id')
            tracks_url = f"https://api-v2.soundcloud.com/users/{user_id}/tracks?client_id={CLIENT_ID}&limit=1"
            tracks_response = safe_request(tracks_url)
            
            # Set a default datetime for 'no tracks' case
            default_date = datetime.now(timezone.utc).isoformat()
            
            if not tracks_response or tracks_response.status_code != 200:
                return {
                    'type': 'profile',
                    'artist_name': data.get('username', 'Unknown Artist'),
                    'title': 'No tracks yet',
                    'url': data.get('permalink_url', url),
                    'release_date': default_date,
                    'cover_url': data.get('avatar_url'),
                    'duration': None,
                    'features': None,
                    'genres': [],
                    'repost': False,
                    'track_count': 0
                }

            tracks = tracks_response.json().get('collection', [])
            if not tracks:
                logging.info(f"ℹ️ No tracks available for {url}")
                return None

            track = tracks[0]
            # Ensure we have a valid release date
            release_date = track.get('created_at')
            if not release_date:
                release_date = default_date

            result = {
                'type': 'track',
                'artist_name': data.get('username', 'Unknown Artist'),
                'title': track.get('title', 'Unknown Title'),
                'url': track.get('permalink_url', url),
                'release_date': release_date,
                'cover_url': track.get('artwork_url') or data.get('avatar_url'),
                'duration': format_duration(track.get('duration', 0)),
                'features': extract_features(track.get('title', '')),
                'genres': [track.get('genre')] if track.get('genre') else [],
                'repost': False,
                'track_count': 1
            }
            
            set_cache(cache_key, json.dumps(result), ttl=CACHE_TTL)
            return result

    except Exception as e:
        logging.error(f"Error fetching release info for {url}: {e}")
        raise ValueError(f"Release info fetch failed: {e}")

def extract_soundcloud_id(url):
    """Alias for extract_soundcloud_username, for compatibility."""
    return extract_soundcloud_username(url)

def seconds_to_minutes_seconds(seconds):
    """Helper to format duration."""
    minutes = seconds // 60
    seconds = seconds % 60
    return f"{minutes}:{str(seconds).zfill(2)}"

def safe_get(url, headers=None, retries=3):
    for attempt in range(retries):
        response = requests.get(url, headers=headers)
        if response.status_code == 429:
            retry_after = int(response.headers.get("Retry-After", 5))
            print(f"Rate limited. Sleeping for {retry_after} seconds...")
            time.sleep(retry_after)
            continue
        response.raise_for_status()
        return response
    return None

def get_soundcloud_likes(artist_url):
    try:
        artist_info = get_artist_info(artist_url)
        artist_id = artist_info['id']

        likes_url = f"https://api-v2.soundcloud.com/users/{artist_id}/likes?client_id={CLIENT_ID}&limit=5"
        response = safe_request(likes_url, headers=HEADERS)
        if not response:
            return []

        items = response.json().get('collection', [])
        likes = []
        for item in items:
            if item.get('track'):
                track = item['track']
                likes.append({
                    "artist_name": track.get('user', {}).get('username'),
                    "title": track.get('title'),
                    "url": track.get('permalink_url'),
                    "release_date": track.get('created_at'),
                    "cover_url": track.get('artwork_url'),
                    "features": None,
                    "track_count": 1,
                    "duration": track.get('full_duration', 0) // 1000,
                    "repost": False,
                    "genres": track.get('genre', []),
                    "release_type": "Like"
                })
        return likes
    except Exception as e:
        print(f"SoundCloud likes fetch failed: {e}")
        return []
    
def get_soundcloud_reposts(artist_url):
    cache_key = f"sc_reposts:{artist_url}"
    cached = get_cache(cache_key)  # Use get_cache
    if cached:
        return json.loads(cached)
    try:
        user_id = extract_soundcloud_user_id(artist_url)
        url = f"https://api-v2.soundcloud.com/users/{user_id}/reposts?client_id={CLIENT_ID}&limit=5"
        response = safe_request(url)
        if response is None or response.status_code == 404:
            alt_url = f"https://api-v2.soundcloud.com/users/{user_id}/track_reposts?client_id={CLIENT_ID}&limit=5"
            response = safe_request(alt_url)
        if not response:
            return []
        
        data = response.json()
        reposts = []

        for item in data.get("collection", []):
            if item.get("type") == "track-repost":
                track = item.get("track")
                if not track:
                    continue
                reposts.append({
                    "track_id": track.get("id"),
                    "title": track.get("title"),
                    "artist_name": track.get("user", {}).get("username"),
                    "url": track.get("permalink_url"),
                    "release_date": track.get("created_at"),
                    "cover_url": track.get("artwork_url"),
                    "features": extract_features(track.get("title", "")),
                    "track_count": 1,
                    "duration": str(round(track.get("duration", 0) / 1000)) + "s",
                    "genres": [track.get("genre")] if track.get("genre") else [],
                    "repost": True
                })

        set_cache(cache_key, json.dumps(reposts), ttl=CACHE_TTL)  # Use set_cache
        return reposts
    except Exception as e:
        logging.error(f"SoundCloud repost fetch failed: {e}")
        return []
    
RATE_LIMIT_DELAY = 5  # Delay in seconds between requests

def rate_limited_request(url, headers=None):
    time.sleep(RATE_LIMIT_DELAY)  # Enforce delay between requests
    return safe_request(url, headers=headers)

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
    
def clear_cache(key):
    """Clear a specific cache key."""
    delete_cache(key)
    logging.info(f"✅ Cleared cache for key: {key}")

def clear_malformed_cache():
    """Clear cache entries with malformed URLs."""
    for key in get_all_cache_keys():  # Retrieve all cache keys
        if "https://soundcloud.com/https://soundcloud.com/" in key:
            delete_cache(key)
            logging.info(f"✅ Cleared malformed cache key: {key}")

def get_all_cache_keys():
    """Retrieve all cache keys from SQLite."""
    try:
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT key FROM cache")
            return [row[0] for row in cursor.fetchall()]
    except Exception as e:
        logging.error(f"❌ Error retrieving cache keys: {e}")
        return []
    
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')
logging.getLogger().handlers[0].setFormatter(RailwayLogFormatter())

def determine_release_type(playlist_data, tracks_data):
    """Determine release type with priority system.
    1. Check API playlist type
    2. Check title keywords
    3. Check multiple artists
    4. Fall back to track count
    """
    title = playlist_data.get('title', '').lower()
    track_count = len(tracks_data) if tracks_data else 0

    # 1. Check for API playlist type (if available)
    playlist_type = playlist_data.get('playlist_type')
    if playlist_type:
        type_mapping = {
            'album': 'album',
            'ep': 'EP',
            'compilation': 'compilation',
            'single': 'single'
        }
        if playlist_type in type_mapping:
            return type_mapping[playlist_type]

    # 2. Check title keywords
    title_indicators = {
        'album': ['album', 'lp', 'record'],
        'EP': ['ep', 'extended play'],
        'mixtape': ['mixtape', 'mix tape'],
        'compilation': ['compilation', 'various artists', 'va'],
        'playlist': ['playlist', 'mix', 'selection', 'picks', 'favorites']
    }
    
    for release_type, keywords in title_indicators.items():
        if any(keyword in title for keyword in keywords):
            return release_type.lower()

    # 3. Check for multiple artists
    if tracks_data:
        artists = set(track.get('user', {}).get('username') for track in tracks_data)
        if len(artists) > 1:
            return 'playlist'

    # 4. Fall back to track count logic
    if track_count >= 7:
        return 'deluxe' if 'deluxe' in title else 'album'
    elif track_count >= 2:
        return 'EP'
    else:
        return 'track'