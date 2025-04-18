import os
import requests
import re
from urllib.parse import urlparse, urlunparse
from dotenv import load_dotenv

load_dotenv()

CLIENT_ID = os.getenv("SOUNDCLOUD_CLIENT_ID")

def clean_soundcloud_url(url):
    """Expands shortlinks and resolves the canonical URL."""
    # Follow shortened on.soundcloud.com links
    if 'on.soundcloud.com' in url:
        res = requests.get(url, allow_redirects=True)
        url = res.url

    # Fetch the page HTML and extract the canonical URL
    res = requests.get(url)
    if res.status_code != 200:
        raise ValueError(f"Failed to fetch URL page: {res.status_code}")

    match = re.search(r'<link rel="canonical" href="([^"]+)"', res.text)
    if match:
        return match.group(1)
    else:
        raise ValueError("Could not determine canonical SoundCloud URL.")


def extract_soundcloud_id(url):
    """We use the clean URL as ID."""
    return clean_soundcloud_url(url)

def get_artist_name_by_url(url):
    clean_url = clean_soundcloud_url(url)
    resolve_url = f"https://api-v2.soundcloud.com/resolve?url={clean_url}&client_id={CLIENT_ID}"
    response = requests.get(resolve_url)
    if response.status_code == 403:
        raise ValueError("Access to this artist profile is forbidden — it may be private or restricted.")
    if response.status_code != 200:
        raise ValueError(f"Failed to resolve artist: {response.status_code}")
    data = response.json()
    if 'username' not in data:
        raise ValueError("Could not resolve artist name from URL.")
    return data['username']

def get_last_release_date(url):
    clean_url = clean_soundcloud_url(url)
    resolve_url = f"https://api-v2.soundcloud.com/resolve?url={clean_url}&client_id={CLIENT_ID}"
    response = requests.get(resolve_url)
    if response.status_code == 403:
        raise ValueError("Access to this artist profile is forbidden — it may be private or restricted.")
    if response.status_code != 200:
        raise ValueError(f"Failed to resolve user: {response.status_code}")
    user_id = response.json().get("id")
    if not user_id:
        raise ValueError("User ID could not be resolved.")

    tracks_url = f"https://api-v2.soundcloud.com/users/{user_id}/tracks?client_id={CLIENT_ID}&limit=1&order=created_at"
    tracks_response = requests.get(tracks_url)
    if tracks_response.status_code == 403:
        raise ValueError("Access to this user's tracks is forbidden — it may be private or restricted.")
    if tracks_response.status_code != 200:
        raise ValueError(f"Failed to fetch tracks: {tracks_response.status_code}")

    collection = tracks_response.json().get("collection")
    if not collection:
        return "Unknown"
    return collection[0].get("created_at", "Unknown")

def get_release_info(url):
    clean_url = clean_soundcloud_url(url)
    resolve_url = f"https://api-v2.soundcloud.com/resolve?url={clean_url}&client_id={CLIENT_ID}"
    response = requests.get(resolve_url)
    if response.status_code == 404:
        raise ValueError("Track or playlist not found — it may be private, invalid, or deleted.")
    if response.status_code == 403:
        raise ValueError("Access to this track or playlist is forbidden — it may be private or restricted.")
    if response.status_code != 200:
        raise ValueError(f"Failed to resolve track: {response.status_code}")

    data = response.json()
    kind = data.get('kind')
    if kind == 'track':
        return process_track(data)
    elif kind == 'playlist':
        return process_playlist(data)
    else:
        raise ValueError(f"Provided URL is not a track or playlist (got kind: '{kind}').")

def process_track(data):
    artist_name = data['user']['username']
    title = data['title']
    release_date = data.get('created_at', 'Unknown')
    cover_url = data.get('artwork_url') or data['user'].get('avatar_url') or ""
    duration_ms = data.get('duration', 0)
    duration_min = round(duration_ms / 60000, 1)
    repost = data.get('repost', False)
    features = extract_features_from_title(title)
    return {
        "artist_name": artist_name,
        "title": title,
        "url": data['permalink_url'],
        "release_date": release_date.split('T')[0] if release_date != 'Unknown' else "Unknown",
        "cover_url": cover_url,
        "track_count": 1,
        "duration": f"{duration_min} min",
        "features": features,
        "repost": repost
    }

def process_playlist(data):
    artist_name = data['user']['username']
    title = data['title']
    cover_url = data.get('artwork_url') or data['user'].get('avatar_url') or ""
    tracks = data.get('tracks', [])
    track_count = len(tracks)
    total_duration_ms = 0
    features_set = set()
    earliest_date = None
    for track in tracks:
        duration_ms = track.get('duration', 0)
        total_duration_ms += duration_ms
        track_title = track.get('title', '')
        track_features = extract_features_from_title(track_title)
        if track_features != "None":
            features_set.update([name.strip() for name in track_features.split(", ")])
        track_date = track.get('created_at')
        if track_date:
            if earliest_date is None or track_date < earliest_date:
                earliest_date = track_date

    duration_min = round(total_duration_ms / 60000, 1)
    features = ", ".join(sorted(features_set)) if features_set else "None"
    release_date = earliest_date.split('T')[0] if earliest_date else "Unknown"
    repost = False
    return {
        "artist_name": artist_name,
        "title": title,
        "url": data['permalink_url'],
        "release_date": release_date,
        "cover_url": cover_url,
        "track_count": track_count,
        "duration": f"{duration_min} min",
        "features": features,
        "repost": repost
    }

def extract_features_from_title(title):
    pattern = r"(?:feat\.|ft\.)\s*([^\(\)\[\]\-–;]+)"
    matches = re.findall(pattern, title, re.IGNORECASE)
    features_set = set()
    for match in matches:
        for sep in ['/', '&', ',', ' and ']:
            match = match.replace(sep, ',')
        features = [name.strip() for name in match.split(",") if name.strip()]
        features_set.update(features)
    return ", ".join(features_set) if features_set else "None"
