import os
from urllib.parse import urlparse
from dotenv import load_dotenv
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials

# Load environment variables (works with Replit secrets)
load_dotenv()
SPOTIFY_CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID")
SPOTIFY_CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET")

# Spotify API client setup
spotify = spotipy.Spotify(auth_manager=SpotifyClientCredentials(
    client_id=SPOTIFY_CLIENT_ID,
    client_secret=SPOTIFY_CLIENT_SECRET
))

def extract_spotify_id(url):
    """Extract the Spotify artist or album ID from a URL."""
    parsed_url = urlparse(url)
    if "spotify.com" in parsed_url.netloc:
        if "/artist/" in parsed_url.path:
            return parsed_url.path.split("/artist/")[1].split("?")[0]
        elif "/album/" in parsed_url.path:
            return parsed_url.path.split("/album/")[1].split("?")[0]
    return None

def get_artist_name(artist_id):
    """Fetch the artist's display name by Spotify artist ID."""
    artist = spotify.artist(artist_id)
    return artist["name"]

def get_artist_info(artist_id):
    """Fetch artist info including genres and URL."""
    artist = spotify.artist(artist_id)
    return {
        'name': artist['name'],
        'genres': artist.get('genres', []),
        'url': artist['external_urls']['spotify'],
        'popularity': artist.get('popularity', 0)
    }

def get_last_release_date(artist_id):
    """Fetch the most recent release date for an artist."""
    releases = spotify.artist_albums(artist_id, album_type='album,single', limit=1)
    if releases['items']:
        return releases['items'][0]['release_date']
    return "N/A"

def get_release_info(release_id):
    """Fetch detailed release info for a Spotify album or single."""
    album = spotify.album(release_id)
    # Get main artist names (there can be multiple!)
    main_artists = [artist['name'] for artist in album['artists']]
    main_artist_ids = [artist['id'] for artist in album['artists']]
    artist_name = ', '.join(main_artists)
    title = album['name']
    release_date = album['release_date']
    cover_url = album['images'][0]['url'] if album['images'] else None
    track_count = album['total_tracks']
    # Total duration in minutes
    total_ms = sum(track['duration_ms'] for track in album['tracks']['items'])
    duration_min = round(total_ms / 60000, 1)
    # Collect featured artists across all tracks
    features = set()
    for track in album['tracks']['items']:
        for artist in track['artists']:
            if artist['name'] not in main_artists:
                features.add(artist['name'])
    features_str = ", ".join(sorted(features)) if features else "None"
    # Genres: combine album genres and all main artists' genres
    genres = album.get('genres', [])
    for artist_id in main_artist_ids:
        try:
            genres += spotify.artist(artist_id).get('genres', [])
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
        "duration": f"{duration_min} min",
        "features": features_str,
        "genres": genres,
        "repost": False  # Spotify doesn't have reposts, but your embed expects this key
    }

def get_artist_discography(artist_id):
    """Get full discography with genre tagging."""
    results = spotify.artist_albums(artist_id, album_type='album,single')
    artist_genres = spotify.artist(artist_id).get('genres', [])
    return [{
        'id': album['id'],
        'name': album['name'],
        'type': album['album_type'],
        'release_date': album['release_date'],
        'genres': artist_genres
    } for album in results['items']]

def search_artist(query):
    """Search artists with genre filters."""
    results = spotify.search(q=query, type='artist', limit=5)
    return [{
        'id': item['id'],
        'name': item['name'],
        'genres': item.get('genres', []),
        'popularity': item.get('popularity', 0)
    } for item in results['artists']['items']]

def get_latest_album_id(artist_id):
    """Get the latest album/single ID for an artist."""
    try:
        releases = spotify.artist_albums(
            artist_id,
            album_type='album,single',
            limit=1,
            country='US'  # Adjust based on your target market
        )
        if releases['items']:
            return releases['items'][0]['id']
        return None
    except Exception as e:
        print(f"Error getting latest album for {artist_id}: {str(e)}")
        return None

