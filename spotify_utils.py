import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import os

# Initialize Spotify API client
client_id = os.getenv("SPOTIFY_CLIENT_ID")
client_secret = os.getenv("SPOTIFY_CLIENT_SECRET")
spotify = spotipy.Spotify(auth_manager=SpotifyClientCredentials(client_id=client_id, client_secret=client_secret))

def track_spotify_artist(artist_id):
    """
    Track an artist's activities on Spotify.
    :param artist_id: Spotify artist ID
    :return: True if successful, False otherwise
    """
    try:
        artist = spotify.artist(artist_id)
        print(f"Tracking artist: {artist['name']}")
        # You can expand this function to fetch artist's albums, tracks, and playlists
        return True
    except spotipy.exceptions.SpotifyException as e:
        print(f"Error tracking artist: {e}")
        return False

def get_artist_releases(artist_id):
    """
    Fetch recent releases by an artist.
    :param artist_id: Spotify artist ID
    :return: List of releases
    """
    try:
        albums = spotify.artist_albums(artist_id, album_type='album,single', limit=10)
        return albums['items']
    except spotipy.exceptions.SpotifyException as e:
        print(f"Error fetching releases: {e}")
        return []

def get_artist_playlists(user_id):
    """
    Fetch playlists created by a user.
    :param user_id: Spotify user ID
    :return: List of playlists
    """
    try:
        playlists = spotify.user_playlists(user_id, limit=10)
        return playlists['items']
    except spotipy.exceptions.SpotifyException as e:
        print(f"Error fetching playlists: {e}")
        return []