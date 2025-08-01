import os
import requests
import re
import time
from urllib.parse import urlparse
from dotenv import load_dotenv
from datetime import datetime
import logging
from utils import cache

# Cache duration for repeated SoundCloud lookups
CACHE_TTL = 300  # 5 minutes


def resolve_url(url):
    cache_key = f"resolve:{url}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    resolve_endpoint = f"https://api-v2.soundcloud.com/resolve?url={url}&client_id={CLIENT_ID}"
    response = safe_request(resolve_endpoint)
    if response and response.status_code == 200:
        data = response.json()
        cache.set(cache_key, data, ttl=3600)
        return data
    return None

def safe_request(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response
    except Exception as e:
        logging.error(f"Request failed: {e}")
        return None


# Load environment variables
load_dotenv()
CLIENT_ID = os.getenv("SOUNDCLOUD_CLIENT_ID")

# Global headers for all requests to avoid 403 errors
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
}

# Try to refresh client_id automatically when unauthorized
def refresh_client_id():
    """Attempt to fetch a working SoundCloud client ID."""
    global CLIENT_ID
    try:
        html = requests.get("https://soundcloud.com", headers=HEADERS, timeout=10).text
        match = re.search(r"client_id\s*:\s*\"([a-zA-Z0-9_-]{32})\"", html)
        if match:
            CLIENT_ID = match.group(1)
            logging.info(f"✅ Refreshed SoundCloud client ID: {CLIENT_ID}")
            return CLIENT_ID
        else:
            logging.error("❌ Failed to find a new SoundCloud client ID.")
    except Exception as e:
        logging.error(f"❌ Error refreshing SoundCloud client ID: {e}")
    return None

# Quick helper to verify the configured client ID works
def verify_client_id():
    """Verify if the SoundCloud CLIENT_ID is valid."""
    test_url = f"https://api-v2.soundcloud.com/resolve?url=https://soundcloud.com&client_id={CLIENT_ID}"
    response = safe_request(test_url)
    return response and response.status_code == 200

# === PATCHED: Enhanced exception handling and rate limiting ===

def safe_request(url, headers=None, retries=3, timeout=10):
    for attempt in range(retries):
        try:
            response = requests.get(url, headers=headers or HEADERS, timeout=timeout)
            if response.status_code == 404:
                # Not found - no need to retry
                return None
            if response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", 5))
                print(
                    f"SoundCloud rate limited. Sleeping for {retry_after} seconds..."
                )
                time.sleep(retry_after)
                continue
            if response.status_code in {401, 403}:
                # Do not auto-refresh when using official client IDs
                print(
                    f"SoundCloud request unauthorized (status {response.status_code})."
                )
            response.raise_for_status()
            return response
        except Exception as e:
            print(f"SoundCloud request error: {e}")
            if attempt < retries - 1:
                time.sleep(2)
    return None

# --- Core URL Handling ---

def extract_soundcloud_user_id(artist_url):
    """Fetch SoundCloud user ID from artist profile URL."""
    cache_key = f"sc_user_id:{artist_url}"
    cached = cache.get(cache_key)
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
            cache.set(cache_key, user_id, ttl=CACHE_TTL)
        return user_id
    except Exception as e:
        raise ValueError(f"Failed to extract user ID from URL: {e}")


def clean_soundcloud_url(url):
    """Normalize and verify SoundCloud URLs."""
    try:
        if 'on.soundcloud.com' in url:
            response = requests.head(url, headers=HEADERS, allow_redirects=True, timeout=10)
            url = response.url

        parsed = urlparse(url)
        if 'soundcloud.com' not in parsed.netloc:
            raise ValueError("Invalid SoundCloud domain")

        try:
            response = requests.get(url, headers=HEADERS, timeout=10)
            if response.status_code == 404:
                logging.warning(f"⚠️ 404 from SoundCloud for {url}")
                raise ValueError("SoundCloud URL returned 404")
            response.raise_for_status()
        except requests.RequestException as req_err:
            logging.error(f"❌ Request failed for SoundCloud URL {url}: {req_err}")
            raise ValueError("SoundCloud request error")

        match = re.search(r'<link rel="canonical" href="([^"]+)"', response.text)
        return match.group(1) if match else url

    except Exception as e:
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
    cache_key = f"sc_artist_info:{url_or_username}"
    cached = cache.get(cache_key)
    if cached:
        return cached
    try:
        if url_or_username.startswith("http"):
            username = extract_soundcloud_username(url_or_username)
        else:
            username = url_or_username

        resolve_url = f"https://api-v2.soundcloud.com/resolve?url=https://soundcloud.com/{username}&client_id={CLIENT_ID}"
        response = safe_request(resolve_url, headers=HEADERS)
        if not response or response.status_code != 200:
            raise ValueError(f"Failed to resolve SoundCloud user: {url_or_username}")

        data = response.json()
        if not data or data.get('kind') != 'user':
            raise ValueError(f"Invalid artist data for: {url_or_username}")

        info = {
            'id': data.get('id', username),
            'name': data.get('username', 'Unknown Artist'),
            'url': data.get('permalink_url', f"https://soundcloud.com/{username}"),
            'track_count': data.get('track_count', 0),
            'avatar_url': data.get('avatar_url', ''),
            'followers': data.get('followers_count', 0)
        }
        cache.set(cache_key, info, ttl=CACHE_TTL)
        return info
    except Exception as e:
        logging.error(f"Error fetching artist info for {url_or_username}: {e}")
        return {'id': url_or_username, 'name': 'Unknown Artist', 'url': f"https://soundcloud.com/{url_or_username}"}

# --- Release Data Fetching ---

def get_last_release_date(artist_url):
    cache_key = f"sc_last_release:{artist_url}"
    cached = cache.get(cache_key)
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
            cache.set(cache_key, created, ttl=CACHE_TTL)
        return created
    except Exception as e:
        print(f"Error getting last release: {e}")
        return None

def get_release_info(url):
    """Universal release info fetcher for tracks/playlists/artists."""
    cache_key = f"sc_release:{url}"
    cached = cache.get(cache_key)
    if cached:
        return cached
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
    
        cache.set(cache_key, info, ttl=CACHE_TTL)
        return info
    except Exception as e:
        raise ValueError(f"Release info fetch failed: {e}")

def get_soundcloud_playlist_info(artist_url):
    try:
        cache_key = f"playlists:{artist_url}"
        cached = cache.get(cache_key)
        if cached:
            return cached

        resolved = get_artist_info(artist_url)
        user_id = resolved.get("id")
        if not user_id:
            raise ValueError(f"Could not resolve user ID for {artist_url}")

        url = f"https://api-v2.soundcloud.com/users/{user_id}/playlists?client_id={CLIENT_ID}&limit=5"
        response = safe_request(url)
        if not response or response.status_code != 200:
            raise ValueError(f"Failed to fetch playlists for {artist_url}")

        data = response.json()
        playlists = data.get("collection", [])
        if not playlists:
            logging.warning(f"No playlists found for {artist_url}")
            return None

        latest_playlist = max(playlists, key=lambda p: p.get("created_at", ""))
        tracks = [
            {
                "id": track.get("id"),
                "title": track.get("title"),
                "duration": track.get("duration"),
                "order": index
            }
            for index, track in enumerate(latest_playlist.get("tracks", []))
        ]

        result = {
            "title": latest_playlist.get("title"),
            "artist_name": latest_playlist.get("user", {}).get("username"),
            "url": latest_playlist.get("permalink_url"),
            "release_date": latest_playlist.get("created_at"),
            "cover_url": latest_playlist.get("artwork_url"),
            "track_count": len(tracks),
            "tracks": tracks
        }

        cache.set(cache_key, result, ttl=300)
        return result
    except Exception as e:
        logging.error(f"Error fetching playlists for {artist_url}: {e}")
        return None

def get_soundcloud_likes_info(artist_url, force_refresh=False):
    try:
        cache_key = f"likes:{artist_url}"
        if not force_refresh:
            cached = cache.get(cache_key)
            if cached:
                logging.info(f"✅ Cache hit for likes: {artist_url}")
                return cached

        logging.info(f"⏳ Fetching likes for {artist_url}...")
        resolved = resolve_url(artist_url)
        if not resolved or "id" not in resolved:
            logging.warning(f"⚠️ Could not resolve SoundCloud user ID from {artist_url}")
            return []

        user_id = resolved["id"]
        url = f"https://api-v2.soundcloud.com/users/{user_id}/likes?client_id={CLIENT_ID}&limit=10"
        response = safe_request(url)
        if not response:
            logging.warning(f"No likes found for {artist_url}")
            return []

        data = response.json().get("collection", [])
        likes = []

        for item in data:
            original = item.get("track") or item.get("playlist")
            if not original:
                continue

            like_date = item.get("created_at")
            track_release_date = original.get("created_at")

            logging.info(f"✅ Found like: {original.get('title')} (Liked at {like_date})")

            likes.append({
                "track_id": original.get("id"),
                "title": original.get("title"),
                "artist_name": original.get("user", {}).get("username"),
                "url": original.get("permalink_url"),
                "release_date": like_date,
                "track_release_date": track_release_date,
                "cover_url": original.get("artwork_url"),
                "features": None,
                "track_count": original.get("track_count", 1),
                "duration": format_duration(original.get("duration", 0)) if original.get("duration") else None,
                "genres": [original.get("genre")] if original.get("genre") else [],
                "liked": True
            })

        cache.set(cache_key, likes, ttl=CACHE_TTL)
        return likes

    except Exception as e:
        logging.error(f"Error fetching likes for {artist_url}: {e}")
        return []
    
def get_soundcloud_reposts_info(artist_url):
    try:
        cache_key = f"reposts:{artist_url}"
        cached = cache.get(cache_key)
        if cached:
            return cached

        resolved = resolve_url(artist_url)
        if not resolved or "id" not in resolved:
            logging.warning(f"⚠️ Could not resolve SoundCloud user ID from {artist_url}")
            return []

        user_id = resolved["id"]
        endpoints = [
            f"https://api-v2.soundcloud.com/users/{user_id}/reposts?client_id={CLIENT_ID}&limit=10",
            f"https://api-v2.soundcloud.com/users/{user_id}/track_reposts?client_id={CLIENT_ID}&limit=10",
            f"https://api-v2.soundcloud.com/stream/users/{user_id}/reposts?client_id={CLIENT_ID}&limit=10"
        ]
        
        response = None
        for endpoint in endpoints:
            response = safe_request(endpoint)
            if response and response.status_code == 200:
                break
        
        if not response:
            logging.warning(f"No reposts found for {artist_url} (tried all endpoints)")
            return []

        data = response.json()
        reposts = []

        for item in data.get("collection", []):
            original = item.get("track") or item.get("playlist")
            if not original:
                continue

            repost_date = item.get("created_at")  # When the repost happened
            track_release_date = original.get("created_at")  # Original track date

            reposts.append({
                "track_id": original.get("id"),
                "type": "track" if item.get("type") == "track-repost" else "playlist",
                "title": original.get("title"),
                "artist_name": original.get("user", {}).get("username"),
                "url": original.get("permalink_url"),
                "release_date": repost_date,  # Use repost date
                "track_release_date": track_release_date,  # Original track date
                "cover_url": original.get("artwork_url"),
                "features": None,
                "track_count": original.get("track_count", 1),
                "duration": format_duration(original.get("duration", 0)) if original.get("duration") else None,
                "genres": [original.get("genre")] if original.get("genre") else [],
                "repost": True
            })

        cache.set(cache_key, reposts, ttl=300)
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

    for track in playlist_data['tracks']:
        features.update(extract_features(track['title']).split(', '))
        if track.get('genre'):
            genres.add(track.get('genre'))

    features.discard('None')

    return {
        'type': 'playlist',
        'artist_name': playlist_data['user']['username'],
        'title': playlist_data['title'],
        'url': playlist_data['permalink_url'],
        'release_date': playlist_data.get('created_at', '')[:10],
        'cover_url': playlist_data.get('artwork_url') or playlist_data['user'].get('avatar_url', ''),
        'duration': format_duration(total_duration),
        'features': ', '.join(sorted(features)) if features else 'None',
        'genres': sorted(genres),
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
    if not ms:
        return None
    seconds = ms // 1000
    minutes = seconds // 60
    seconds = seconds % 60
    return f"{minutes}:{seconds:02d}"

def format_duration(milliseconds):
    """Convert ms to mm:ss format."""
    seconds = int(milliseconds / 1000)
    return f"{seconds // 60}:{seconds % 60:02d}"

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

def get_soundcloud_release_info(url):
    """Main function for release checking."""
    try:
        return get_release_info(url)
    except Exception as e:
        print(f"SoundCloud release info fetch failed: {e}")
        return None

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
    cached = cache.get(cache_key)
    if cached:
        return cached
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

        cache.set(cache_key, reposts, ttl=CACHE_TTL)
        return reposts
    except Exception as e:
        logging.error(f"SoundCloud repost fetch failed: {e}")
        return []