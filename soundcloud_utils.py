import requests
import os

# Initialize SoundCloud API client
SOUNDCLOUD_CLIENT_ID = os.getenv("SOUNDCLOUD_CLIENT_ID")

def track_soundcloud_artist(artist_id):
    """
    Track an artist's activities on SoundCloud.
    :param artist_id: SoundCloud artist ID
    :return: True if successful, False otherwise
    """
    try:
        url = f"https://api.soundcloud.com/users/{artist_id}?client_id={SOUNDCLOUD_CLIENT_ID}"
        response = requests.get(url)
        response.raise_for_status()
        artist = response.json()
        print(f"Tracking artist: {artist['username']}")
        return True
    except requests.exceptions.RequestException as e:
        print(f"Error tracking artist: {e}")
        return False

def get_artist_tracks(artist_id):
    """
    Fetch recent tracks by an artist.
    :param artist_id: SoundCloud artist ID
    :return: List of tracks
    """
    try:
        url = f"https://api.soundcloud.com/users/{artist_id}/tracks?client_id={SOUNDCLOUD_CLIENT_ID}"
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching tracks: {e}")
        return []

def get_artist_likes(artist_id):
    """
    Fetch liked tracks by an artist.
    :param artist_id: SoundCloud artist ID
    :return: List of liked tracks
    """
    try:
        url = f"https://api.soundcloud.com/users/{artist_id}/favorites?client_id={SOUNDCLOUD_CLIENT_ID}"
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching likes: {e}")
        return []