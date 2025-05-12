import os
import requests
import re
import time
from urllib.parse import urlparse
from dotenv import load_dotenv
from datetime import datetime
import logging

# Load environment variables
load_dotenv()
CLIENT_ID = os.getenv("SOUNDCLOUD_CLIENT_ID")

# Global headers for all requests to avoid 403 errors
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
}

# --- Core URL Handling ---

def extract_soundcloud_user_id(artist_url):
    """Fetch SoundCloud user ID from artist profile URL."""
    try:
        res = requests.get(f"https://api-v2.soundcloud.com/resolve?url={artist_url}&client_id={CLIENT_ID}", timeout=10)
        res.raise_for_status()
        data = res.json()
        return data.get("id")
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
    try:
        if url_or_username.startswith("http"):
            username = extract_soundcloud_username(url_or_username)
        else:
            username = url_or_username

        resolve_url = f"https://api-v2.soundcloud.com/resolve?url=https://soundcloud.com/{username}&client_id={CLIENT_ID}"
        response = requests.get(resolve_url, headers=HEADERS, timeout=10)
        response.raise_for_status()

        data = response.json()
        if data.get('kind') != 'user':
            raise ValueError("Not an artist profile")

        return {
            'id': data['id'],
            'name': data['username'],
            'url': data['permalink_url'],
            'track_count': data['track_count'],
            'avatar_url': data.get('avatar_url', ''),
            'followers': data.get('followers_count', 0)
        }
    except Exception as e:
        raise ValueError(f"Artist info fetch failed: {e}")

# --- Release Data Fetching ---

def get_last_release_date(artist_url):
    try:
        artist_info = get_artist_info(artist_url)
        artist_id = artist_info['id']

        tracks_url = f"https://api-v2.soundcloud.com/users/{artist_id}/tracks?client_id={CLIENT_ID}&limit=5&order=created_at"
        response = requests.get(tracks_url, headers=HEADERS, timeout=10)
        response.raise_for_status()

        tracks = response.json()
        if not tracks:
            return None

        # Get track with latest created_at timestamp
        latest_track = max(tracks, key=lambda t: t['created_at'])

        return latest_track.get('created_at')  # full timestamp
    except Exception as e:
        print(f"Error getting last release: {e}")
        return None

def get_release_info(url):
    """Universal release info fetcher for tracks/playlists/artists."""
    try:
        clean_url = clean_soundcloud_url(url)
        resolve_url = f"https://api-v2.soundcloud.com/resolve?url={clean_url}&client_id={CLIENT_ID}"
        response = requests.get(resolve_url, headers=HEADERS, timeout=10)
        response.raise_for_status()

        data = response.json()

        if data['kind'] == 'track':
            return process_track(data)
        elif data['kind'] == 'playlist':
            return process_playlist(data)
        elif data['kind'] == 'user':
            return get_artist_release(data)
        else:
            raise ValueError("Unsupported content type")
    except Exception as e:
        raise ValueError(f"Release info fetch failed: {e}")

def get_soundcloud_playlist_info(artist_url):
    try:
        artist_info = get_artist_info(artist_url)
        artist_id = artist_info['id']

        playlists_url = f"https://api-v2.soundcloud.com/users/{artist_id}/playlists?client_id={CLIENT_ID}&limit=5&order=created_at"
        response = requests.get(playlists_url, headers=HEADERS, timeout=10)
        response.raise_for_status()

        playlists = response.json()
        if not playlists:
            return None

        latest_playlist = playlists[0]

        release_date = latest_playlist.get('created_at')

        playlist_type = latest_playlist.get('playlist_type') or "Playlist"
        track_count = latest_playlist.get('track_count', 0)

        release_info = {
            "artist_name": latest_playlist.get('user', {}).get('username'),
            "title": latest_playlist.get('title'),
            "url": latest_playlist.get('permalink_url'),
            "release_date": release_date,
            "cover_url": latest_playlist.get('artwork_url'),
            "features": None,
            "track_count": track_count,
            "duration": None,
            "repost": False,
            "genres": [],
            "release_type": "Album" if track_count >= 7 else "EP" if track_count >= 2 else "Single"
        }

        return release_info

    except Exception as e:
        raise ValueError(f"Playlist info fetch failed: {e}")

def get_soundcloud_likes_info(artist_url):
    try:
        user_id = extract_soundcloud_user_id(artist_url)
        url = f"https://api-v2.soundcloud.com/users/{user_id}/likes?client_id={CLIENT_ID}&limit=5"

        response = requests.get(url, timeout=10)
        response.raise_for_status()

        data = response.json()
        likes = []

        for item in data.get("collection", []):
            if item.get("type") == "track-like":
                track = item.get("track")
                if not track:
                    continue
                likes.append({
                    "track_id": track.get("id"),
                    "title": track.get("title"),
                    "artist_name": track.get("user", {}).get("username"),
                    "url": track.get("permalink_url"),
                    "release_date": track.get("created_at"),
                    "cover_url": track.get("artwork_url"),
                    "features": None,
                    "track_count": 1,
                    "duration": str(round(track.get("duration", 0) / 1000)) + "s",
                    "genres": [track.get("genre")] if track.get("genre") else [],
                    "repost": False
                })

        return likes
    except Exception as e:
        logging.error(f"SoundCloud likes fetch failed: {e}")
        return None

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
        response = requests.get(tracks_url, headers=HEADERS, timeout=10)
        response.raise_for_status()

        data = response.json()


        # Some responses return 'collection', not raw list
        tracks = data.get('collection', data if isinstance(data, list) else [])

        if not tracks:
            raise ValueError("No tracks found")

        return process_track(tracks[0])
    except Exception as e:
        raise ValueError(f"Artist release fetch failed: {e}")

# --- Utility Functions ---

def format_duration(milliseconds):
    """Convert ms to mm:ss format."""
    seconds = int(milliseconds / 1000)
    return f"{seconds // 60}:{seconds % 60:02d}"

def extract_features(title):
    """Extract featured artists from track titles."""
    patterns = [
        r"\(feat\.?\s*([^)]+)\)",
        r"\[feat\.?\s*([^\]]+)\]",
        r"ft\.?\s*([^\-–]+)",
        r"w/ ?(.+?)(?:\)|$)"
    ]
    features = set()

    for pattern in patterns:
        matches = re.findall(pattern, title, re.IGNORECASE)
        for match in matches:
            cleaned = re.sub(r"[()\[\]feat\.?|ft\.?|w/]", '', match)
            for sep in ['/', '&', ',', ' and ']:
                cleaned = cleaned.replace(sep, ',')
            features.update([name.strip() for name in cleaned.split(',') if name.strip()])

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
        response = requests.get(likes_url, headers=HEADERS, timeout=10)
        response.raise_for_status()

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
    try:
        user_id = extract_soundcloud_user_id(artist_url)
        url = f"https://api-v2.soundcloud.com/users/{user_id}/reposts?client_id={CLIENT_ID}&limit=5"

        response = requests.get(url, timeout=10)
        response.raise_for_status()

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
                    "features": None,
                    "track_count": 1,
                    "duration": str(round(track.get("duration", 0) / 1000)) + "s",
                    "genres": [track.get("genre")] if track.get("genre") else [],
                    "repost": True
                })

        return reposts
    except Exception as e:
        logging.error(f"SoundCloud repost fetch failed: {e}")
        return None