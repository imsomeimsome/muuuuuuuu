import os
from urllib.parse import urlparse
from dotenv import load_dotenv
import spotipy
import logging
from spotipy.oauth2 import SpotifyClientCredentials
import threading

# Load environment variables
load_dotenv()
SPOTIFY_CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID")
SPOTIFY_CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET")

# === PATCHED: Enhanced exception handling and rate limiting ===

import time
from spotipy.exceptions import SpotifyException

import asyncio

# Gather multiple Spotify credential sets (primary + backups)
SPOTIFY_KEYS = [
    (os.getenv("SPOTIFY_CLIENT_ID"), os.getenv("SPOTIFY_CLIENT_SECRET")),
    (os.getenv("SPOTIFY_CLIENT_ID_2"), os.getenv("SPOTIFY_CLIENT_SECRET_2")),
    (os.getenv("SPOTIFY_CLIENT_ID_3"), os.getenv("SPOTIFY_CLIENT_SECRET_3")),
    (os.getenv("SPOTIFY_CLIENT_ID_4"), os.getenv("SPOTIFY_CLIENT_SECRET_4")),
]

# Filter out incomplete pairs
SPOTIFY_KEYS = [(cid, sec) for cid, sec in SPOTIFY_KEYS if cid and sec]

spotify_key_manager = None  # will be initialized via init_spotify_key_manager

# Add telemetry + rate limit patterns
RATE_LIMIT_PATTERNS = [
    'rate/request limit',
    'retry will occur after',
    'rate limit',
    'exceeded',
    'over rate limit',
    'temporarily disabled'
]
TELEMETRY = {
    'calls': 0,
    'success': 0,
    'rate_limits': 0,
    'rotations': 0,
    'errors': 0,
    'invalid_credentials': 0,
}

# --- Tier 2 & 3 Monitoring State (Spotify) ---
from collections import deque
import statistics as _stats
import time as _time
SPOTIFY_DATA_ANOMALIES = deque(maxlen=50)  # (ts, kind, endpoint, detail)
SPOTIFY_ANOMALY_WINDOW_SEC = 120
SPOTIFY_ANOMALY_THRESHOLD = int(os.getenv('SPOTIFY_ANOMALY_THRESHOLD', '4'))
SPOTIFY_BATCH_STATE = {
    'start_time': None,
    'expected_total': None,
    'processed': 0,
    'latencies_ms': [],
    'median_baseline_ms': None,
    'watchdog_task': None,
    'last_progress_time': None,
    'rotated_this_run': False,
    'fail': 0,
    'success': 0,
    'consecutive_fail': 0,
}
SPOTIFY_CONSEC_FAIL_THRESHOLD = int(os.getenv('SPOTIFY_CONSEC_FAIL_THRESHOLD', '5'))
SPOTIFY_FAIL_RATE_THRESHOLD = float(os.getenv('SPOTIFY_FAIL_RATE_THRESHOLD', '0.8'))
SPOTIFY_MIN_SAMPLES_FOR_RATE = int(os.getenv('SPOTIFY_MIN_SAMPLES_FOR_RATE', '6'))
SPOTIFY_WATCHDOG_GRACE = int(os.getenv('SPOTIFY_WATCHDOG_GRACE', '180'))  # seconds inactivity
SPOTIFY_EXPECTED_DURATION_FACTOR = float(os.getenv('SPOTIFY_EXPECTED_DURATION_FACTOR', '1.6'))
SPOTIFY_EMA_ALPHA = 0.3

def record_spotify_anomaly(kind: str, endpoint: str, detail: str = ''):
    """Tier 2 structural anomaly tracker (missing keys, empty collections). Rotates key on clustering."""
    global SPOTIFY_DATA_ANOMALIES
    try:
        now = _time.time()
        SPOTIFY_DATA_ANOMALIES.append((now, kind, endpoint, detail))
        recent = [r for r in SPOTIFY_DATA_ANOMALIES if now - r[0] <= SPOTIFY_ANOMALY_WINDOW_SEC]
        if len(recent) >= SPOTIFY_ANOMALY_THRESHOLD:
            logging.warning(f"⚠️ Spotify anomaly cluster ({len(recent)}) in {SPOTIFY_ANOMALY_WINDOW_SEC}s – rotating credentials (kind={kind}).")
            if _attempt_rotation('spotify_data_anomaly_cluster'):
                logging.info("🔄 Rotated Spotify key due to anomaly cluster")
            SPOTIFY_DATA_ANOMALIES.clear()
    except Exception as e:
        logging.debug(f"record_spotify_anomaly failed: {e}")

def begin_spotify_release_batch(expected_total: int):
    """Initialize Tier 3 batch monitoring for a Spotify sweep."""
    st = SPOTIFY_BATCH_STATE
    st['start_time'] = _time.time()
    st['expected_total'] = expected_total
    st['processed'] = 0
    st['latencies_ms'] = []
    st['last_progress_time'] = st['start_time']
    st['rotated_this_run'] = False
    st['fail'] = 0
    st['success'] = 0
    st['consecutive_fail'] = 0
    # Cancel old watchdog
    task = st.get('watchdog_task')
    if task and not task.done():
        task.cancel()
    # Launch watchdog if event loop active
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            st['watchdog_task'] = loop.create_task(_spotify_batch_watchdog())
    except Exception:
        pass

def note_spotify_release_fetch(success: bool, context: str = '', latency_ms: float = None):
    """Record per-artist Spotify fetch outcome for Tier 3 heuristics."""
    st = SPOTIFY_BATCH_STATE
    if st['start_time'] is None:
        return
    if success:
        st['success'] += 1
        st['consecutive_fail'] = 0
    else:
        st['fail'] += 1
        st['consecutive_fail'] += 1
    if latency_ms is not None:
        st['latencies_ms'].append(latency_ms)
    if any(tag in context for tag in [':release_checked', ':no_release', ':exception']):
        st['processed'] += 1
        st['last_progress_time'] = _time.time()
    # Rotation heuristics (consecutive fail / fail rate)
    if not st['rotated_this_run']:
        if st['consecutive_fail'] >= SPOTIFY_CONSEC_FAIL_THRESHOLD:
            logging.warning("⚠️ Spotify consecutive failure threshold reached – rotating key")
            if _attempt_rotation('spotify_consecutive_failures'):
                st['rotated_this_run'] = True
        elif (st['processed'] >= SPOTIFY_MIN_SAMPLES_FOR_RATE and st['fail'] / max(1, st['processed']) >= SPOTIFY_FAIL_RATE_THRESHOLD):
            logging.warning("⚠️ Spotify batch high failure rate – rotating key")
            if _attempt_rotation('spotify_failure_rate'):
                st['rotated_this_run'] = True

def finalize_spotify_release_batch():
    st = SPOTIFY_BATCH_STATE
    if not st['start_time']:
        return
    try:
        duration = _time.time() - st['start_time']
        exp = st['expected_total'] or 0
        proc = st['processed']
        latencies = st['latencies_ms']
        median_lat = _stats.median(latencies) if latencies else None
        base = st['median_baseline_ms']
        if median_lat is not None:
            st['median_baseline_ms'] = median_lat if base is None else (SPOTIFY_EMA_ALPHA * median_lat + (1-SPOTIFY_EMA_ALPHA) * base)
        baseline = st['median_baseline_ms']
        if exp and proc < exp * 0.5:
            logging.warning(f"📉 Spotify batch processed only {proc}/{exp} ({proc/exp:.0%}) – potential abort; rotating")
            _attempt_rotation('spotify_processed_mismatch')
        if baseline and median_lat and median_lat > baseline * 2 and proc < exp:
            logging.warning(f"🐢 Spotify latency spike median {median_lat:.1f}ms > 2x baseline {baseline:.1f}ms – rotating")
            _attempt_rotation('spotify_latency_spike')
        logging.info(f"🧾 Spotify batch finalize: duration={duration:.1f}s processed={proc}/{exp} median_latency={median_lat:.1f if median_lat else 'n/a'}ms baseline={baseline:.1f if baseline else 'n/a'}ms fail={st['fail']} success={st['success']}")
    except Exception as e:
        logging.debug(f"finalize_spotify_release_batch error: {e}")
    finally:
        task = st.get('watchdog_task')
        if task and not task.done():
            task.cancel()
        st['start_time'] = None

def spotify_batch_in_progress():
    return SPOTIFY_BATCH_STATE['start_time'] is not None

async def _spotify_batch_watchdog():
    st = SPOTIFY_BATCH_STATE
    try:
        while spotify_batch_in_progress():
            await asyncio.sleep(30)
            if not spotify_batch_in_progress():
                break
            now = _time.time()
            if st['last_progress_time'] and (now - st['last_progress_time']) > SPOTIFY_WATCHDOG_GRACE:
                logging.warning("🕒 Spotify batch inactivity watchdog triggered – rotating key")
                if _attempt_rotation('spotify_watchdog_inactivity'):
                    st['last_progress_time'] = now
            # Overrun detection
            if st['expected_total'] and st['latencies_ms']:
                avg_lat = sum(st['latencies_ms'])/len(st['latencies_ms'])/1000.0
                elapsed = now - st['start_time']
                projected_max = avg_lat * st['expected_total'] * SPOTIFY_EXPECTED_DURATION_FACTOR
                if elapsed > projected_max and st['processed'] < st['expected_total']:
                    logging.warning("⏱️ Spotify batch duration overrun with incomplete progress – rotating key")
                    _attempt_rotation('spotify_watchdog_overrun')
                    break
    except asyncio.CancelledError:
        pass
    except Exception as e:
        logging.debug(f"Spotify watchdog error: {e}")

rotation_lock = threading.Lock()

class SpotifyKeyManager:
    def __init__(self, bot=None):
        self.bot = bot
        self.keys = SPOTIFY_KEYS
        self.index = 0
        self.key_cooldowns = {}  # index -> datetime when usable again
        if not self.keys:
            logging.error("❌ No Spotify credentials found in environment.")
        else:
            logging.info(f"✅ Loaded {len(self.keys)} Spotify credential set(s). Using index 0.")

    def get_current_key(self):
        if not self.keys:
            raise ValueError("No Spotify credentials configured.")
        return self.keys[self.index]

    def mark_rate_limited(self, minutes=15, seconds=None):
        """Put current key on cooldown. If seconds provided, override minutes."""
        from datetime import datetime, timezone, timedelta
        if seconds is not None:
            cooldown = timedelta(seconds=max(1, int(seconds)))
        else:
            cooldown = timedelta(minutes=minutes)
        self.key_cooldowns[self.index] = datetime.now(timezone.utc) + cooldown
        TELEMETRY['rate_limits'] += 1

    def rotate_key(self):
        from datetime import datetime, timezone
        # Removed internal lock to avoid deadlock (locking handled in _attempt_rotation)
        if not self.keys or len(self.keys) == 1:
            logging.warning("⚠️ Cannot rotate Spotify key (only one set configured).")
            return None
        start = self.index
        for _ in range(len(self.keys)):
            self.index = (self.index + 1) % len(self.keys)
            cooldown_until = self.key_cooldowns.get(self.index)
            if cooldown_until and datetime.now(timezone.utc) < cooldown_until:
                continue
            logging.info(f"🔄 Switching to Spotify key index {self.index}")
            return self.get_current_key()
        logging.error("❌ All Spotify keys are on cooldown.")
        return None

    async def log_key_rotation(self, old_index, new_index, reason):
        if not self.bot:
            return
        from datetime import datetime, timezone
        lines = []
        now = datetime.now(timezone.utc)
        for i, (cid, _) in enumerate(self.keys):
            if i == new_index:
                status = "▶️ Active"
            else:
                cd = self.key_cooldowns.get(i)
                if cd and cd > now:
                    status = f"⏳ Cooldown until {cd.isoformat()}"
                else:
                    status = "✅ Ready"
            lines.append(f"Key {i+1}: {status} (client_id={cid[:10]}…)")
        message = (
            "🔄 **Spotify Key Rotation**\n"
            f"Reason: {reason}\n"
            f"Switched from Key {old_index+1} ➜ Key {new_index+1}\n\n"
            + "\n".join(lines)
        )
        # Send to all guild log channels
        try:
            from database_utils import get_channel
            for guild in self.bot.guilds:
                channel_id = get_channel(str(guild.id), "logs")
                if channel_id:
                    ch = self.bot.get_channel(int(channel_id))
                    if ch:
                        await ch.send(message)
        except Exception as e:
            logging.error(f"Failed to send Spotify rotation log: {e}")

    def get_status_rows(self):
        """Return structured status info for all keys."""
        from datetime import datetime, timezone
        now = datetime.now(timezone.utc)
        rows = []
        for i, (cid, _sec) in enumerate(self.keys):
            cooldown_until = self.key_cooldowns.get(i)
            if i == self.index:
                state = "active"
            elif cooldown_until and cooldown_until > now:
                state = f"cooldown_until={cooldown_until.isoformat()}"
            else:
                state = "ready"
            rows.append({
                "index": i,
                "client_id_preview": cid[:12] + "…",
                "state": state
            })
        return rows

def _patch_rate_limit_handling():
    """Monkeypatch spotipy.Spotify._internal_call to enforce our rotation on rate limits.
    Handles both standard 429 and custom 'rate/request limit' messages that may be logged
    without re-raising in higher layers.
    """
    global spotify, spotify_key_manager
    if not spotify:
        return
    try:
        import types, re
        base_call = spotify._internal_call
        if getattr(spotify, '_rl_patched', False):
            return

        def patched_internal_call(self, method, url, payload=None, params=None, **kwargs):
            from spotipy.exceptions import SpotifyException as _SpEx
            try:
                return base_call(method, url, payload=payload, params=params, **kwargs)
            except _SpEx as e:  # Standard exception path
                msg_lc = str(e).lower()
                status = getattr(e, 'http_status', None)
                # Detect custom or standard rate limit conditions
                if status == 429 or 'rate/request limit' in msg_lc or 'retry will occur after' in msg_lc:
                    wait_seconds = None
                    m = re.search(r'after:?\s*(\d+)', msg_lc)
                    if m:
                        try: wait_seconds = int(m.group(1))
                        except ValueError: pass
                    logging.warning(f"⚠️ (patch) Spotify rate limit caught in _internal_call status={status} wait={wait_seconds}")
                    if spotify_key_manager:
                        spotify_key_manager.mark_rate_limited(seconds=wait_seconds if wait_seconds else None)
                    rotated = _attempt_rotation("rate_limit_internal_call")
                    if rotated:
                        logging.info("🔄 (patch) Rotated Spotify key. Retrying request once.")
                        try:
                            return base_call(method, url, payload=payload, params=params, **kwargs)
                        except _SpEx as e2:
                            logging.error(f"❌ (patch) Retry after rotation failed: {e2}")
                    # If cannot rotate or retry failed, re-raise to let safe_spotify_call handle backoff
                raise
        spotify._internal_call = types.MethodType(patched_internal_call, spotify)
        spotify._rl_patched = True
        logging.info("🛡️ Patched Spotify _internal_call for proactive rate limit rotation")
    except Exception as e:
        logging.error(f"Failed to patch Spotify internal call: {e}")

def _rebuild_spotify_client(client_id, client_secret):
    global spotify
    spotify = spotipy.Spotify(
        auth_manager=SpotifyClientCredentials(
            client_id=client_id,
            client_secret=client_secret
        )
    )
    logging.info(f"✅ Reinitialized Spotify client with key {client_id[:10]}…")
    _patch_rate_limit_handling()  # ensure monkeypatch applied after each rebuild

def validate_spotify_client():
    """Rebuild client if missing (e.g., after manual rotation issues)."""
    global spotify, spotify_key_manager
    if spotify is None and spotify_key_manager and spotify_key_manager.keys:
        try:
            cid, sec = spotify_key_manager.get_current_key()
            _rebuild_spotify_client(cid, sec)
        except Exception as e:
            logging.error(f"Failed to validate/rebuild Spotify client: {e}")

def init_spotify_key_manager(bot=None):
    """Call from bot startup to enable rotation + logging."""
    global spotify_key_manager
    if spotify_key_manager is None:
        spotify_key_manager = SpotifyKeyManager(bot)
        if spotify_key_manager.keys:
            cid, sec = spotify_key_manager.get_current_key()
            _rebuild_spotify_client(cid, sec)
    return spotify_key_manager

# Replace initial single-client construction with manager-based init guard
if 'spotify_key_manager' not in globals() or spotify_key_manager is None:
    try:
        # Fallback to original single credentials if manager not yet set up
        if SPOTIFY_KEYS:
            cid, sec = SPOTIFY_KEYS[0]
            spotify = spotipy.Spotify(
                auth_manager=SpotifyClientCredentials(
                    client_id=cid,
                    client_secret=sec
                )
            )
            _patch_rate_limit_handling()
        else:
            spotify = None
    except Exception as e:
        logging.error(f"Failed to initialize Spotify client: {e}")
        spotify = None

def _attempt_rotation(reason):
    """Try rotating Spotify credentials; return True if rotated."""
    global spotify_key_manager
    with rotation_lock:
        if not spotify_key_manager or not spotify_key_manager.keys:
            return False
        old = spotify_key_manager.index
        new_pair = spotify_key_manager.rotate_key()
        rotated = True if new_pair else False
        if not new_pair:
            return False
        cid, sec = new_pair
        _rebuild_spotify_client(cid, sec)
        try:
            # schedule async log if event loop running
            loop = asyncio.get_event_loop()
            if loop.is_running():
                asyncio.create_task(spotify_key_manager.log_key_rotation(old, spotify_key_manager.index, reason))
        except RuntimeError:
            pass
        if rotated:
            TELEMETRY['rotations'] += 1
        return rotated

def safe_spotify_call(callable_fn, *args, retries=3, delay=2, **kwargs):
    """Make a Spotify API call with error handling, rate limit + key rotation."""
    global spotify_key_manager
    validate_spotify_client()
    for attempt in range(retries):
        TELEMETRY['calls'] += 1
        try:
            logging.debug(f"Spotify call: {getattr(callable_fn,'__name__',str(callable_fn))} attempt={attempt+1}")
            result = callable_fn(*args, **kwargs)
            if result is not None:
                TELEMETRY['success'] += 1
            return result
        except SpotifyException as e:
            TELEMETRY['errors'] += 1
            status = getattr(e, "http_status", None)
            msg_lc = str(e).lower()
            headers = getattr(e, 'headers', {}) or {}
            # Unified pattern detection for custom messages even if status != 429
            is_pattern_limit = any(pat in msg_lc for pat in RATE_LIMIT_PATTERNS)
            # Standard 429 or heuristic 403 with pattern text (sometimes appears)
            if status in (429, 403) and (status == 429 or is_pattern_limit):
                wait = 0
                if headers and headers.get('Retry-After'):
                    try:
                        wait = int(headers['Retry-After'])
                    except ValueError:
                        wait = 5
                # Extract embedded numeric if present
                if not wait:
                    import re as _re
                    m = _re.search(r'after:?\s*(\d+)', msg_lc)
                    if m:
                        try: wait = int(m.group(1))
                        except ValueError: pass
                logging.warning(f"⚠️ Spotify rate limit detected status={status} wait={wait}s patterns={is_pattern_limit}")
                if spotify_key_manager:
                    spotify_key_manager.mark_rate_limited(seconds=wait if wait else None)
                if _attempt_rotation("rate_limit" + ("_pattern" if is_pattern_limit else "")):
                    logging.info("🔄 Rotated Spotify key due to rate limit; retrying")
                    continue
                backoff = min(60, wait or (attempt + 1) * delay)
                logging.info(f"⏳ Waiting {backoff}s then retrying same key (no rotation available)")
                time.sleep(backoff)
                continue
            # Handle custom verbose message without 429 status
            if is_pattern_limit:
                logging.warning("⚠️ Spotify rate limit (pattern-only) with status=" + str(status))
                if spotify_key_manager:
                    spotify_key_manager.mark_rate_limited(minutes=15)
                if _attempt_rotation("rate_limit_pattern_only"):
                    continue
                time.sleep(min(60, (attempt + 1) * delay))
                continue
            # Invalid client / auth
            if 'invalid_client' in msg_lc or status in (400, 401):
                TELEMETRY['invalid_credentials'] += 1
                logging.error("❌ Spotify invalid/expired credentials. Rotating...")
                if _attempt_rotation("invalid_client"):
                    continue
                raise
            # Access token expired
            if status == 401 and 'access token' in msg_lc:
                # Force rebuild (token might be stale)
                logging.warning("🔄 Access token expired; rebuilding client")
                validate_spotify_client()
                if _attempt_rotation("token_expired"):
                    continue
            # 5xx transient
            if status and 500 <= status < 600:
                logging.warning(f"⚠️ Spotify server error {status}. Retry {attempt+1}/{retries}")
                time.sleep(delay)
                continue
            logging.error(f"Spotify API error (no rotation): {e}")
            break
        except Exception as e:
            TELEMETRY['errors'] += 1
            logging.error(f"Unexpected Spotify error: {e}")
            time.sleep(delay)
            continue
    return None

def ping_spotify():
    """Lightweight call to keep token fresh & trigger rotation if failing."""
    try:
        res = safe_spotify_call(spotify.search, q="a", type="artist", limit=1)
        return bool(res)
    except Exception as e:
        logging.debug(f"Spotify ping failed: {e}")
        return False

def get_spotify_key_status():
    """Public helper to fetch current Spotify credential status."""
    global spotify_key_manager
    if not spotify_key_manager or not spotify_key_manager.keys:
        return {"loaded": False, "keys": []}
    return {
        "loaded": True,
        "active_index": spotify_key_manager.index,
        "total_keys": len(spotify_key_manager.keys),
        "keys": spotify_key_manager.get_status_rows()
    }

def manual_rotate_spotify_key(reason: str = "manual"):
    """Public helper to manually rotate Spotify credentials.
    Returns dict with keys: rotated(bool), active_index, total_keys, keys(list)."""
    global spotify_key_manager
    if not spotify_key_manager or not getattr(spotify_key_manager, 'keys', None):
        return {"rotated": False, "error": "No Spotify keys configured", "keys": []}
    if len(spotify_key_manager.keys) == 1:
        return {"rotated": False, "error": "Only one Spotify key configured", "keys": spotify_key_manager.get_status_rows()}
    old_index = spotify_key_manager.index
    rotated = _attempt_rotation(reason)
    return {
        "rotated": rotated,
        "old_index": old_index,
        "active_index": spotify_key_manager.index,
        "total_keys": len(spotify_key_manager.keys),
        "keys": spotify_key_manager.get_status_rows()
    }

def get_spotify_telemetry_snapshot():
    from datetime import datetime, timezone
    return {
        'timestamp': datetime.now(timezone.utc).isoformat(),
        'telemetry': TELEMETRY.copy(),
        'keys': get_spotify_key_status()
    }

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
        artist = safe_spotify_call(spotify.artist, artist_id)
        if artist:
            return artist["name"]
    except Exception as e:
        print(f"Error fetching artist name for {artist_id}: {e}")
    return "Unknown Artist"

def get_artist_info(artist_id):
    """Fetch artist info including genres and URL."""
    try:
        artist = safe_spotify_call(spotify.artist, artist_id)
        if not artist:
            return None
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
        albums = safe_spotify_call(spotify.artist_albums, artist_id, album_type='album,single')
        artist = safe_spotify_call(spotify.artist, artist_id)
        if not albums or not artist:
            return []
        artist_genres = artist.get('genres', [])
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
        results = safe_spotify_call(spotify.search, q=query, type='artist', limit=5)
        if not results:
            return []
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
        releases = safe_spotify_call(
            spotify.artist_albums,
            artist_id,
            album_type='album,single',
            limit=1
        )
        if releases and releases.get('items'):
            return releases['items'][0]['release_date']
        return "N/A"
    except Exception as e:
        print(f"Error fetching last release date for {artist_id}: {e}")
        return "N/A"

def get_latest_album_id(artist_id):
    """Get the latest album/single ID for an artist."""
    try:
        # Fetch multiple releases
        releases = safe_spotify_call(
            spotify.artist_albums,
            artist_id,
            album_type='album,single',
            limit=10,
            country='US'
        )
        if not releases or not releases.get('items'):
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
        album = safe_spotify_call(spotify.album, release_id)
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
                artist_info = safe_spotify_call(spotify.artist, artist_id)
                if artist_info:
                    genres.extend(artist_info.get('genres', []))
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

import logging

# Custom logging formatter for Railway logs
class RailwayLogFormatter(logging.Formatter):
    COLORS = {
        "DEBUG": "\033[90m",  # Gray
        "INFO": "\033[94m",  # Blue
        "WARNING": "\033[93m",  # Orange
        "ERROR": "\033[91m",  # Red
    }
    RESET = "\033[0m"

    def format(self, record):
        color = self.COLORS.get(record.levelname, self.RESET)
        record.msg = f"{color}{record.msg}{self.RESET}"
        return super().format(record)

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')
logging.getLogger().handlers[0].setFormatter(RailwayLogFormatter())