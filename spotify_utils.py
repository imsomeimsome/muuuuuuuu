import os
from urllib.parse import urlparse
from dotenv import load_dotenv
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials

# Load environment variables
load_dotenv()
SPOTIFY_CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID")
SPOTIFY_CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET")

# === PATCHED: Enhanced exception handling and rate limiting ===

import time
from spotipy.exceptions import SpotifyException

def safe_spotify_call(callable_fn, *args, retries=3, delay=2, **kwargs):
    for attempt in range(retries):
        try:
            return callable_fn(*args, **kwargs)
        except SpotifyException as e:
            if e.http_status == 429 and 'Retry-After' in e.headers:
                wait = int(e.headers['Retry-After'])
                print(f"Rate limited by Spotify. Retrying in {wait} seconds...")
                time.sleep(wait)
            else:
                print(f"Spotify API error: {e}")
                break
        except Exception as e:
            print(f"Unexpected Spotify error: {e}")
            if attempt < retries - 1:
                time.sleep(delay)
    return None

# Spotify API client setup
spotify = spotipy.Spotify(auth_manager=SpotifyClientCredentials(
    client_id=SPOTIFY_CLIENT_ID,
    client_secret=SPOTIFY_CLIENT_SECRET
))

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
        artist = spotify.artist(artist_id)
        return artist["name"]
    except Exception as e:
        print(f"Error fetching artist name for {artist_id}: {e}")
        return "Unknown Artist"

def get_artist_info(artist_id):
    """Fetch artist info including genres and URL."""
    try:
        artist = spotify.artist(artist_id)
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
        albums = spotify.artist_albums(artist_id, album_type='album,single')
        artist_genres = spotify.artist(artist_id).get('genres', [])
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
        results = spotify.search(q=query, type='artist', limit=5)
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
        releases = spotify.artist_albums(artist_id, album_type='album,single', limit=1)
        if releases['items']:
            return releases['items'][0]['release_date']
        return "N/A"
    except Exception as e:
        print(f"Error fetching last release date for {artist_id}: {e}")
        return "N/A"

def get_latest_album_id(artist_id):
    """Get the latest album/single ID for an artist."""
    try:
        # Fetch multiple releases
        releases = spotify.artist_albums(
            artist_id,
            album_type='album,single',
            limit=10,
            country='US'
        )
        if not releases['items']:
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
        album = spotify.album(release_id)
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
                artist_genres = spotify.artist(artist_id).get('genres', [])
                genres.extend(artist_genres)
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
