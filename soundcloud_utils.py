import os
import requests
import re
from urllib.parse import urlparse
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()
CLIENT_ID = os.getenv("SOUNDCLOUD_CLIENT_ID")

# --- Core URL Handling ---
def clean_soundcloud_url(url):
    """Normalize and verify SoundCloud URLs with error handling."""
    try:
        # Follow redirects for shortened URLs
        if 'on.soundcloud.com' in url:
            response = requests.head(url, allow_redirects=True, timeout=10)
            url = response.url

        parsed = urlparse(url)
        if 'soundcloud.com' not in parsed.netloc:
            raise ValueError("Invalid SoundCloud domain")

        # Extract canonical URL from page metadata
        response = requests.get(url, timeout=10)
        if response.status_code != 200:
            raise ValueError(f"Failed to fetch URL (HTTP {response.status_code})")

        match = re.search(r'<link rel="canonical" href="([^"]+)"', response.text)
        if match:
            return match.group(1)
        
        return url
    except Exception as e:
        raise ValueError(f"URL validation failed: {str(e)}")

def extract_soundcloud_id(url):
    """Get consistent artist/track identifier from any SoundCloud URL."""
    try:
        clean_url = clean_soundcloud_url(url)
        parsed = urlparse(clean_url)
        path_segments = [p for p in parsed.path.strip('/').split('/') if p]
        
        if len(path_segments) < 1:
            raise ValueError("No path segments in URL")
            
        # Handle different URL types
        if 'sets' in path_segments:  # Playlist
            return f"playlist_{path_segments[-1]}"
        elif 'tracks' in path_segments:  # Single track
            return f"track_{path_segments[-1]}"
        else:  # Assume artist profile
            return f"artist_{path_segments[0]}"
    except Exception as e:
        raise ValueError(f"ID extraction failed: {str(e)}")

# --- Artist Data Fetching ---
def get_artist_info(artist_id):
    """Get complete artist metadata with error handling."""
    try:
        resolve_url = f"https://api-v2.soundcloud.com/resolve?url=https://soundcloud.com/{artist_id}&client_id={CLIENT_ID}"
        response = requests.get(resolve_url, timeout=10)
        
        if response.status_code != 200:
            raise ValueError(f"API error: {response.status_code}")
            
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
        raise ValueError(f"Artist info fetch failed: {str(e)}")

# --- Release Data Fetching ---
def get_last_release_date(artist_url):
    """Get most recent release date with full error handling."""
    try:
        artist_id = extract_soundcloud_id(artist_url)
        tracks_url = f"https://api-v2.soundcloud.com/users/{artist_id}/tracks?client_id={CLIENT_ID}&limit=1&order=created_at"
        response = requests.get(tracks_url, timeout=10)
        
        if response.status_code != 200:
            raise ValueError(f"API error: {response.status_code}")
            
        tracks = response.json()
        if not tracks:
            return None
            
        return tracks[0].get('created_at', '')[:10]  # YYYY-MM-DD format
    except Exception as e:
        print(f"Error getting last release: {str(e)}")
        return None

def get_release_info(url):
    """Universal release info fetcher for tracks/playlists/artists."""
    try:
        clean_url = clean_soundcloud_url(url)
        resolve_url = f"https://api-v2.soundcloud.com/resolve?url={clean_url}&client_id={CLIENT_ID}"
        response = requests.get(resolve_url, timeout=10)
        
        if response.status_code != 200:
            raise ValueError(f"API error: {response.status_code}")
            
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
        raise ValueError(f"Release info fetch failed: {str(e)}")

# --- Data Processing ---
def process_track(track_data):
    """Convert raw track data to standardized format."""
    return {
        'type': 'track',
        'artist_name': track_data['user']['username'],
        'title': track_data['title'],
        'url': track_data['permalink_url'],
        'release_date': track_data.get('created_at', '')[:10],
        'cover_url': track_data.get('artwork_url', track_data['user'].get('avatar_url', '')),
        'duration': format_duration(track_data.get('duration', 0)),
        'features': extract_features(track_data['title']),
        'genres': [track_data.get('genre', '')],
        'repost': track_data.get('repost', False),
        'track_count': 1
    }

def process_playlist(playlist_data):
    """Convert playlist data to standardized format."""
    return {
        'type': 'playlist',
        'artist_name': playlist_data['user']['username'],
        'title': playlist_data['title'],
        'url': playlist_data['permalink_url'],
        'release_date': playlist_data.get('created_at', '')[:10],
        'cover_url': playlist_data.get('artwork_url', playlist_data['user'].get('avatar_url', '')),
        'duration': sum(t.get('duration', 0) for t in playlist_data['tracks']) // 1000,
        'features': ', '.join(set(
            feat for t in playlist_data['tracks'] 
            for feat in extract_features(t['title']).split(', ') 
            if feat != 'None'
        )),
        'genres': list(set(t.get('genre', '') for t in playlist_data['tracks'])),
        'repost': False,
        'track_count': len(playlist_data['tracks'])
    }

def get_artist_release(artist_data):
    """Get latest release from artist profile."""
    tracks_url = f"https://api-v2.soundcloud.com/users/{artist_data['id']}/tracks?client_id={CLIENT_ID}&limit=1"
    response = requests.get(tracks_url, timeout=10)
    
    if response.status_code != 200 or not response.json():
        raise ValueError("No tracks found for artist")
        
    return process_track(response.json()[0])

def format_duration(milliseconds):
    """Convert ms to mm:ss format."""
    seconds = int(milliseconds / 1000)
    return f"{seconds // 60}:{seconds % 60:02d}"

def extract_features(title):
    """Advanced feature extraction from track titles."""
    patterns = [
        r"\((feat\.? [^)]+)\)",
        r"\[feat\.? [^\]]+\]",
        r"ft\.? [^\-–]+",
        r"w/ ?(.+?)(?:\)|$)"
    ]
    
    features = set()
    for pattern in patterns:
        matches = re.findall(pattern, title, re.IGNORECASE)
        for match in matches:
            # Clean and split features
            cleaned = re.sub(r"[\(\)\[\]feat\.?|ft\.?|w/]", '', match)
            for sep in ['/', '&', ',', ' and ']:
                cleaned = cleaned.replace(sep, ',')
            features.update([name.strip() for name in cleaned.split(',') if name.strip()])
    
    return ", ".join(sorted(features)) if features else "None"

# --- Bot Integration Helpers ---
def get_soundcloud_artist_name(url):
    """Get display name for database storage."""
    try:
        artist_id = extract_soundcloud_id(url)
        return get_artist_info(artist_id)['name']
    except:
        return "Unknown Artist"

def get_soundcloud_release_info(url):
    """Main function for release checking."""
    try:
        return get_release_info(url)
    except Exception as e:
        print(f"SoundCloud Error: {str(e)}")
        return None

def get_artist_name_by_url(url):
    """Get artist name from any SoundCloud URL (wrapper for compatibility)."""
    try:
        artist_id = extract_soundcloud_id(url)
        return get_artist_info(artist_id)['name']
    except Exception as e:
        print(f"Error getting artist name: {str(e)}")
        return "Unknown Artist"
