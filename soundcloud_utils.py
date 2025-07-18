import requests
import os

# Initialize SoundCloud API client
SOUNDCLOUD_CLIENT_ID = os.getenv("SOUNDCLOUD_CLIENT_ID")
SOUNDCLOUD_CLIENT_SECRET = os.getenv("SOUNDCLOUD_CLIENT_SECRET")
REDIRECT_URI = "https://yourdomain.railway.app/callback"  # Replace with your app's redirect URI
SOUNDCLOUD_ACCESS_TOKEN = os.getenv("SOUNDCLOUD_ACCESS_TOKEN")  # Store the access token in your .env file


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

def get_artist_name_by_url(artist_url):
    """
    Get the name of a SoundCloud artist using their URL.
    :param artist_url: SoundCloud artist URL.
    :return: Artist name.
    """
    try:
        artist_id = artist_url.split("/")[-1]  # Extract artist ID from URL
        url = f"https://api.soundcloud.com/users/{artist_id}"
        headers = {"Authorization": f"OAuth {SOUNDCLOUD_ACCESS_TOKEN}"}
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        artist = response.json()
        return artist['username']
    except requests.exceptions.RequestException as e:
        print(f"Error fetching artist name: {e}")
        return None

def get_access_token():
    """
    Obtain an access token from SoundCloud using OAuth2.
    """
    token_url = "https://api.soundcloud.com/oauth2/token"

    authorization_code = os.getenv("SOUNDCLOUD_AUTHORIZATION_CODE")
    client_id = os.getenv("SOUNDCLOUD_CLIENT_ID")
    client_secret = os.getenv("SOUNDCLOUD_CLIENT_SECRET")
    redirect_uri = os.getenv("REDIRECT_URI")

    if not authorization_code:
        raise ValueError("Missing SOUNDCLOUD_AUTHORIZATION_CODE")
    if not all([client_id, client_secret, redirect_uri]):
        raise ValueError("One or more OAuth env vars are missing.")

    data = {
        "client_id": client_id,
        "client_secret": client_secret,
        "redirect_uri": redirect_uri,
        "grant_type": "authorization_code",
        "code": authorization_code,
    }

    response = requests.post(token_url, data=data)
    response.raise_for_status()
    token_data = response.json()

    access_token = token_data["access_token"]
    print("✅ Access token obtained.")
    return access_token
