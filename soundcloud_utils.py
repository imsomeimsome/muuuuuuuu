import os
import requests
import re
from urllib.parse import urlparse
from dotenv import load_dotenv
from datetime import datetime

# Load environment variables
load_dotenv()
CLIENT_ID = os.getenv("SOUNDCLOUD_CLIENT_ID")

# --- Core URL Handling ---

def clean_soundcloud_url(url):
    """Normalize and verify SoundCloud URLs."""
    try:
        if 'on.soundcloud.com' in url:
            response = requests.head(url, allow_redirects=True, timeout=10)
            url = response.url

        parsed = urlparse(url)
        if 'soundcloud.com' not in parsed.netloc:
            raise ValueError("Invalid SoundCloud domain")

        response = requests.get(url, timeout=10)
        response.raise_for_status()

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

def get_artist_info(username):
    """Get complete artist metadata."""
    try:
        resolve_url = f"https://api-v2.soundcloud.com/resolve?url=https://soundcloud.com/{username}&client_id={CLIENT_ID}"
        response = requests.get(resolve_url, timeout=10)
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
    """Get most recent release date."""
    try:
        username = extract_soundcloud_username(artist_url)
        artist_info = get_artist_info(username)
        artist_id = artist_info['id']

        tracks_url = f"https://api-v2.soundcloud.com/users/{artist_id}/tracks?client_id={CLIENT_ID}&limit=1&order=created_at"
        response = requests.get(tracks_url, timeout=10)
        response.raise_for_status()

        tracks = response.json()
        if not tracks:
            return None

        return tracks[0].get('created_at', '')[:10]
    except Exception as e:
        print(f"Error getting last release: {e}")
        return None

def get_release_info(url):
    """Universal release info fetcher for tracks/playlists/artists."""
    try:
        clean_url = clean_soundcloud_url(url)
        resolve_url = f"https://api-v2.soundcloud.com/resolve?url={clean_url}&client_id={CLIENT_ID}"
        response = requests.get(resolve_url, timeout=10)
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

# --- Data Processing ---

def process_track(track_data):
    """Convert track data to standardized format."""
    return {
        'type': 'track',
        'artist_name': track_data['user']['username'],
        'title': track_data['title'],
        'url': track_data['permalink_url'],
        'release_date': track_data.get('created_at', '')[:10],
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
        tracks_url = f"https://api-v2.soundcloud.com/users/{artist_data['id']}/tracks?client_id={CLIENT_ID}&limit=1"
        response = requests.get(tracks_url, timeout=10)
        response.raise_for_status()

        tracks = response.json()
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
    """Get display name for database storage."""
    try:
        username = extract_soundcloud_username(url)
        return get_artist_info(username)['name']
    except Exception as e:
        print(f"Error getting artist name: {e}")
        return "Unknown Artist"

def get_artist_name_by_url(url):
    """Get artist name from any SoundCloud URL."""
    return get_soundcloud_artist_name(url)

def get_soundcloud_artist_id(url):
    """Resolve SoundCloud artist URL to numeric artist ID."""
    try:
        username = extract_soundcloud_username(url)
        return get_artist_info(username)['id']
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
