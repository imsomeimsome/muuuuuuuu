import requests
import os

# Environment variables
SOUNDCLOUD_ACCESS_TOKEN = os.getenv("SOUNDCLOUD_ACCESS_TOKEN")
REDIRECT_URI = os.getenv("REDIRECT_URI")
SOUNDCLOUD_CLIENT_ID = os.getenv("SOUNDCLOUD_CLIENT_ID")
SOUNDCLOUD_CLIENT_SECRET = os.getenv("SOUNDCLOUD_CLIENT_SECRET")

# Shared headers for OAuth requests
HEADERS = {
    "Authorization": f"OAuth {SOUNDCLOUD_ACCESS_TOKEN}"
}

def track_soundcloud_artist(artist_id):
    """
    Track an artist's activities on SoundCloud.
    """
    try:
        url = f"https://api.soundcloud.com/users/{artist_id}"
        response = requests.get(url, headers=HEADERS)
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
    """
    try:
        url = f"https://api.soundcloud.com/users/{artist_id}/tracks"
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching tracks: {e}")
        return []

def get_artist_likes(artist_id):
    """
    Fetch liked tracks by an artist.
    """
    try:
        url = f"https://api.soundcloud.com/users/{artist_id}/favorites"
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching likes: {e}")
        return []

def get_artist_name_by_url(artist_url):
    """
    Get the name of a SoundCloud artist using their profile URL.
    """
    try:
        artist_slug = artist_url.rstrip("/").split("/")[-1]
        resolve_url = f"https://api.soundcloud.com/resolve?url={artist_url}"
        response = requests.get(resolve_url, headers=HEADERS)
        response.raise_for_status()
        artist = response.json()
        return artist.get("username", "Unknown Artist")
    except requests.exceptions.RequestException as e:
        print(f"Error fetching artist name: {e}")
        return None

def get_access_token():
    """
    Exchange authorization code for a long-lived access token.
    """
    token_url = "https://api.soundcloud.com/oauth2/token"
    authorization_code = os.getenv("SOUNDCLOUD_AUTHORIZATION_CODE")

    if not authorization_code:
        raise ValueError("Missing SOUNDCLOUD_AUTHORIZATION_CODE")
    if not all([SOUNDCLOUD_CLIENT_ID, SOUNDCLOUD_CLIENT_SECRET, REDIRECT_URI]):
        raise ValueError("One or more required env vars are missing.")

    data = {
        "client_id": SOUNDCLOUD_CLIENT_ID,
        "client_secret": SOUNDCLOUD_CLIENT_SECRET,
        "redirect_uri": REDIRECT_URI,
        "grant_type": "authorization_code",
        "code": authorization_code,
    }

    response = requests.post(token_url, data=data)
    response.raise_for_status()
    token_data = response.json()

    print("✅ Access token obtained.")
    return token_data["access_token"]
