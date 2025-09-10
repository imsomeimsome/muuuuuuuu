import os
import requests
import re
import time
import sqlite3
import asyncio
import random
from urllib.parse import urlparse
from dotenv import load_dotenv
from datetime import datetime, timezone, timedelta
import logging
from utils import get_cache, set_cache, delete_cache
import json
from database_utils import DB_PATH, get_channel, save_api_key_state, load_api_key_state
from dateutil.parser import parse as isoparse
from functools import lru_cache
import threading
from collections import deque
import statistics

# At the top after imports
load_dotenv()

# Initialize global variables
CLIENT_ID = os.getenv("SOUNDCLOUD_CLIENT_ID")
key_manager = None

# --- Telemetry & Circuit Breaker ---
TELEMETRY = {
    'requests': 0,
    'success': 0,
    'client_errors': 0,
    'server_errors': 0,
    'rotations': 0,
    'refresh_attempts': 0,
    'html_soft_fail': 0,
    'circuit_breaker_tripped': 0
}
CIRCUIT_BREAKER_UNTIL = None  # datetime when breaker lifts
CIRCUIT_BREAKER_MIN = timedelta(minutes=10)

# Passive anomaly detection (data truncation / unexpected empties)
DATA_ANOMALIES = deque(maxlen=50)  # (ts, kind, endpoint, detail)
# Existing defaults kept but now overridable via env
DATA_ANOMALY_THRESHOLD = int(os.getenv('SC_DATA_ANOMALY_THRESHOLD', '4'))  # number of anomalies inside window to trigger rotation
DATA_ANOMALY_WINDOW_SEC = int(os.getenv('SC_DATA_ANOMALY_WINDOW_SEC', '120'))
ANOMALY_ROTATE_ENABLED = os.getenv('SC_DATA_ANOMALY_ROTATE', 'true').lower() == 'true'
ANOMALY_ROTATION_COOLDOWN_SEC = int(os.getenv('SC_ANOMALY_ROTATION_COOLDOWN', '300'))  # suppress further anomaly rotations for N sec
EMPTY_BASELINE_GRACE_SEC = int(os.getenv('SC_EMPTY_COLLECTION_BASELINE_GRACE', '43200'))  # 12h; ignore empty_collection anomalies until we have a baseline
_LAST_ANOMALY_ROTATION = None  # timestamp of last anomaly-triggered rotation
_LAST_NONEMPTY_BASELINE = {  # endpoint -> timestamp of last non-empty collection seen
    'playlists': None,
    'likes': None,
    'reposts': None,
}
# New configuration for empty collection anomaly handling
EMPTY_COLLECTION_ROTATE = os.getenv('SC_EMPTY_COLLECTION_ROTATE', 'false').lower() == 'true'  # default off to reduce noise
EMPTY_COLLECTION_STALE_SEC = int(os.getenv('SC_EMPTY_COLLECTION_STALE_SEC', '3600'))  # ignore empties if last non-empty older than this

def _update_nonempty_baseline(kind: str):
    try:
        from datetime import datetime as _dt, timezone as _tz
        if kind in _LAST_NONEMPTY_BASELINE:
            _LAST_NONEMPTY_BASELINE[kind] = _dt.now(_tz.utc)
    except Exception:
        pass

def record_data_anomaly(kind: str, endpoint: str, detail: str = ''):
    """Record an unexpected empty/truncated data response. If too many inside
    a short window, proactively rotate the key (treat as implicit rate limit).

    Improvements:
    - Configurable thresholds via env vars
    - Baseline-aware: do not treat initial empty collections as anomalies until a non-empty baseline was observed (avoids false positives for new / inactive users)
    - Rotation cooldown to avoid rapid churn
    - Ability to disable anomaly-triggered rotation entirely (SC_DATA_ANOMALY_ROTATE=false)
    """
    global _LAST_ANOMALY_ROTATION
    try:
        from datetime import datetime as _dt, timezone as _tz
        now = _dt.now(_tz.utc)

        # Baseline grace & suppression logic for empty collections
        if kind == 'empty_collection':
            ep_label = 'playlists' if 'playlist' in detail or 'playlists' in endpoint else (
                'likes' if 'like' in detail or 'likes' in endpoint else (
                'reposts' if 'repost' in detail or 'reposts' in endpoint else None))
            if ep_label:
                baseline_ts = _LAST_NONEMPTY_BASELINE.get(ep_label)
                # Suppress until we have a baseline
                if baseline_ts is None:
                    logging.debug(f"🟦 Suppressing empty_collection anomaly (no baseline) endpoint={ep_label}")
                    return
                # Suppress if baseline is stale (user simply inactive for long time)
                if (now - baseline_ts).total_seconds() > EMPTY_COLLECTION_STALE_SEC:
                    logging.debug(f"🟦 Suppressing empty_collection anomaly (baseline stale) endpoint={ep_label}")
                    return
                # Optionally suppress rotation impact entirely (still record debug)
                if not EMPTY_COLLECTION_ROTATE:
                    logging.debug(f"🟦 Ignoring empty_collection anomaly for rotation endpoint={ep_label}")
                    return
        # Deduplicate rapid identical anomalies (same kind+endpoint within 30s)
        if DATA_ANOMALIES:
            last_ts, last_kind, last_ep, last_detail = DATA_ANOMALIES[-1]
            if last_kind == kind and last_ep == endpoint and (now - last_ts).total_seconds() < 30:
                return

        DATA_ANOMALIES.append((now, kind, endpoint, detail))
        recent = [r for r in DATA_ANOMALIES if (now - r[0]).total_seconds() <= DATA_ANOMALY_WINDOW_SEC]

        if ANOMALY_ROTATE_ENABLED and len(recent) >= DATA_ANOMALY_THRESHOLD:
            # Check rotation cooldown
            if _LAST_ANOMALY_ROTATION and (now - _LAST_ANOMALY_ROTATION).total_seconds() < ANOMALY_ROTATION_COOLDOWN_SEC:
                logging.warning("⚠️ Anomaly threshold reached but rotation suppressed (cooldown active)")
                DATA_ANOMALIES.clear()
                return
            logging.warning(f"⚠️ Detected {len(recent)} SoundCloud data anomalies in {DATA_ANOMALY_WINDOW_SEC}s; rotating key proactively (kind={kind}).")
            if key_manager:
                try:
                    key_manager.mark_rate_limited()
                    new_key = key_manager.rotate_key(reason="data_anomaly")
                    if new_key:
                        global CLIENT_ID
                        CLIENT_ID = new_key
                        TELEMETRY['rotations'] += 1
                        _LAST_ANOMALY_ROTATION = now
                        logging.info("🔄 Proactive SoundCloud key rotation due to anomaly clustering")
                except Exception as e:
                    logging.error(f"Failed proactive rotation on anomaly: {e}")
            DATA_ANOMALIES.clear()
    except Exception as e:
        logging.debug(f"record_data_anomaly failed: {e}")

def circuit_breaker_active():
    """Return True only while the breaker window is still active; auto-clear when expired."""
    global CIRCUIT_BREAKER_UNTIL
    if not CIRCUIT_BREAKER_UNTIL:
        return False
    if datetime.now(timezone.utc) >= CIRCUIT_BREAKER_UNTIL:
        # Auto-clear expired breaker
        CIRCUIT_BREAKER_UNTIL = None
        return False
    return True

def trip_circuit_breaker(duration: timedelta = None, reason: str = ''):
    """Activate circuit breaker for given duration (default to minimum window)."""
    global CIRCUIT_BREAKER_UNTIL, TELEMETRY
    if duration is None:
        duration = CIRCUIT_BREAKER_MIN
    CIRCUIT_BREAKER_UNTIL = datetime.now(timezone.utc) + duration
    TELEMETRY['circuit_breaker_tripped'] += 1
    logging.error(f"🛑 SoundCloud circuit breaker tripped for {duration}. Reason: {reason}")

def reset_circuit_breaker():
    """Manually clear the circuit breaker (use sparingly)."""
    global CIRCUIT_BREAKER_UNTIL
    CIRCUIT_BREAKER_UNTIL = None
    logging.info("🔌 SoundCloud circuit breaker reset")

def get_circuit_breaker_status():
    return {
        'active': circuit_breaker_active(),
        'until': CIRCUIT_BREAKER_UNTIL.isoformat() if CIRCUIT_BREAKER_UNTIL else None,
        'seconds_remaining': (CIRCUIT_BREAKER_UNTIL - datetime.now(timezone.utc)).total_seconds() if CIRCUIT_BREAKER_UNTIL else 0
    }

# --- Batch Release Fetch Monitor (detect truncated runs / silent rate limits) ---
class _ReleaseFetchMonitor:
    def __init__(self):
        self.reset()
        self.consecutive_fail_threshold = int(os.getenv('SC_RUN_FAIL_THRESHOLD', '5'))
        self.failure_rate_threshold = float(os.getenv('SC_RUN_FAIL_RATE', '0.8'))
        self.min_samples_for_rate = int(os.getenv('SC_RUN_MIN_SAMPLES', '6'))
        self.rotated_this_run = False

    def reset(self):
        self.total = 0
        self.success = 0
        self.fail = 0
        self.consecutive_fail = 0
        self.rotated_this_run = False

    def note(self, success: bool, context: str = ''):
        self.total += 1
        if success:
            self.success += 1
            self.consecutive_fail = 0
        else:
            self.fail += 1
            self.consecutive_fail += 1

    def should_rotate(self):
        if self.rotated_this_run:
            return False
        # Hard trigger on consecutive failures
        if self.consecutive_fail >= self.consecutive_fail_threshold:
            return True
        # Soft trigger on high failure rate after enough samples
        if self.total >= self.min_samples_for_rate and self.fail / max(1, self.total) >= self.failure_rate_threshold:
            return True
        return False

    def mark_rotated(self):
        self.rotated_this_run = True

_release_monitor_singleton = _ReleaseFetchMonitor()

def get_release_monitor():
    return _release_monitor_singleton

class SoundCloudKeyManager:
    def __init__(self, bot=None):
        self.bot = bot
        self.api_keys = [
            os.getenv("SOUNDCLOUD_CLIENT_ID"),
            os.getenv("SOUNDCLOUD_CLIENT_ID_2"),
            os.getenv("SOUNDCLOUD_CLIENT_ID_3"),
        ]
        self.api_keys = [k for k in self.api_keys if k]
        self.current_key_index = 0
        self.key_cooldowns = {}          # idx -> datetime until reusable
        self.fail_counts = {}            # idx -> consecutive failures
        # Load persisted state if any
        try:
            persisted = load_api_key_state('soundcloud')
            if persisted:
                # Determine active index
                active_indices = [idx for idx, row in persisted.items() if row.get('active')]
                if active_indices:
                    active_idx = active_indices[0]
                    if active_idx < len(self.api_keys):
                        self.current_key_index = active_idx
                # Restore fail counts & cooldowns (only if within future)
                from datetime import datetime as _dt, timezone as _tz
                now = _dt.now(_tz.utc)
                for idx, row in persisted.items():
                    if idx < len(self.api_keys):
                        self.fail_counts[idx] = row.get('fail_count', 0)
                        cd_str = row.get('cooldown_until')
                        if cd_str:
                            try:
                                cd_dt = _dt.fromisoformat(cd_str.replace('Z', '+00:00'))
                                if cd_dt > now:
                                    self.key_cooldowns[idx] = cd_dt
                            except Exception:
                                pass
        except Exception as e:
            logging.warning(f"Failed restoring SoundCloud key state: {e}")
        if not self.api_keys:
            logging.error("❌ No SoundCloud API keys configured.")
        else:
            logging.info(f"✅ Loaded {len(self.api_keys)} SoundCloud key(s). Using index {self.current_key_index}.")
            self.persist_state()

    def persist_state(self):
        """Persist current rotation state to database."""
        snapshot = []
        for idx, key in enumerate(self.api_keys):
            cd = self.key_cooldowns.get(idx)
            snapshot.append({
                'index': idx,
                'key': key,
                'fail_count': self.fail_counts.get(idx, 0),
                'cooldown_until': cd.isoformat() if cd else None,
                'active': idx == self.current_key_index
            })
        save_api_key_state('soundcloud', snapshot)

    def stop_background_tasks(self):
        """Cancel internal background tasks (called on shutdown)."""
        task = getattr(self, '_auto_refresh_task', None)
        if task and not task.done():
            task.cancel()

    def get_current_key(self):
        if not self.api_keys:
            raise ValueError("No SoundCloud keys configured.")
        return self.api_keys[self.current_key_index]

    def _calc_cooldown(self, idx):
        """Adaptive cooldown: grows with consecutive failures."""
        base = 30   # seconds
        fails = self.fail_counts.get(idx, 0)
        # Exponential-ish backoff capped
        seconds = min(base * (2 ** fails), 60 * 60 * 6)  # cap at 6h
        return timedelta(seconds=seconds)

    def mark_rate_limited(self):
        """Mark current key as limited without rotating (used before rotate)."""
        idx = self.current_key_index
        self.fail_counts[idx] = self.fail_counts.get(idx, 0) + 1
        self.key_cooldowns[idx] = datetime.now(timezone.utc) + self._calc_cooldown(idx)
        self.persist_state()

    async def _log_rotation(self, old_index, new_index, reason, exhausted=False):
        if not self.bot:
            return
        try:
            now = datetime.now(timezone.utc)
            lines = []
            for i, key in enumerate(self.api_keys):
                if i == new_index and not exhausted:
                    state = "▶️ Active"
                else:
                    cd = self.key_cooldowns.get(i)
                    if cd and cd > now:
                        state = f"⏳ Cooldown until <t:{int(cd.timestamp())}:R>"
                    else:
                        state = "✅ Ready"
                preview = key[:10] + "…" if key else "N/A"
                lines.append(f"Key {i+1}: {state} ({preview})")
            header = "🛑 All SoundCloud keys exhausted" if exhausted else "🔄 SoundCloud Key Rotation"
            msg = f"{header}\nReason: {reason}\nFrom Key {old_index+1} ➜ Key {new_index+1 if not exhausted else 'N/A'}\n\n" + "\n".join(lines)
            from database_utils import get_channel
            for guild in self.bot.guilds:
                channel_id = get_channel(str(guild.id), "logs")
                if channel_id:
                    ch = self.bot.get_channel(int(channel_id))
                    if ch:
                        await ch.send(msg)
        except Exception as e:
            logging.error(f"Failed to log SoundCloud rotation: {e}")

    def rotate_key(self, reason="rate_limit"):
        """Rotate to next available key; schedule async log. Returns new key or None."""
        if len(self.api_keys) <= 1:
            logging.warning("⚠️ Only one SoundCloud key configured; cannot rotate.")
            return None
        old = self.current_key_index
        for _ in range(len(self.api_keys) - 1):
            nxt = (self.current_key_index + 1) % len(self.api_keys)
            cd = self.key_cooldowns.get(nxt)
            if cd and datetime.now(timezone.utc) < cd:
                # Skip keys still cooling down
                self.current_key_index = nxt
                continue
            self.current_key_index = nxt
            new_key = self.get_current_key()
            self.fail_counts[self.current_key_index] = 0
            logging.info(f"🔄 Rotated SoundCloud key {old+1} ➜ {self.current_key_index+1} ({new_key[:10]}…)")
            if self.bot:
                try:
                    asyncio.create_task(self._log_rotation(old, self.current_key_index, reason))
                except RuntimeError:
                    # Event loop not started yet
                    pass
            self.persist_state()
            return new_key
        # Exhausted
        logging.error("🛑 All SoundCloud keys on cooldown (rotation exhausted).")
        if self.bot:
            try:
                asyncio.create_task(self._log_rotation(old, old, reason, exhausted=True))
            except RuntimeError:
                pass
        self.persist_state()
        return None

    def get_status_rows(self):
        from datetime import datetime as _dt, timezone as _tz
        now = _dt.now(_tz.utc)
        rows = []
        for i, key in enumerate(self.api_keys):
            if i == self.current_key_index:
                state = 'active'
            else:
                cd = self.key_cooldowns.get(i)
                state = f"cooldown_until={cd.isoformat()}" if cd and cd > now else 'ready'
            rows.append({
                'index': i,
                'key_preview': (key or '')[:12] + '…',
                'state': state,
                'fail_count': self.fail_counts.get(i, 0)
            })
        return rows

    def start_background_tasks(self):
        if not self.bot:
            return
        loop = asyncio.get_event_loop()
        if not hasattr(self, '_auto_refresh_task'):
            self._auto_refresh_task = loop.create_task(self._auto_refresh_loop())

    async def _auto_refresh_loop(self):
        from datetime import datetime as _dt, timezone as _tz
        from soundcloud_utils import verify_client_id, refresh_client_id  # self import safe at runtime
        while True:
            try:
                # Sleep ~30m +/- 5m jitter
                base = 1800
                jitter = random.randint(-300, 300)
                await asyncio.sleep(max(60, base + jitter))
                # Skip if no keys
                if not self.api_keys:
                    continue
                # If active key is in cooldown pick a ready one
                now = _dt.now(_tz.utc)
                cd = self.key_cooldowns.get(self.current_key_index)
                if cd and cd > now:
                    rotated = self.rotate_key(reason="cooldown_active_auto")
                    if rotated:
                        logging.info("🔁 Auto-rotation due to active key cooldown.")
                # Validate current key
                if not verify_client_id():
                    logging.warning("⚠️ Active SoundCloud key appears invalid; attempting refresh/rotation")
                    refreshed = None
                    # Try rotation first
                    rotated = self.rotate_key(reason="auto_invalid")
                    if not rotated:
                        try:
                            refreshed = refresh_client_id()
                        except Exception:
                            refreshed = None
                    if refreshed:
                        logging.info("♻️ Auto-refreshed SoundCloud client_id")
                    self.persist_state()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logging.warning(f"SoundCloud auto-refresh loop error: {e}")

def init_key_manager(bot):
    """Initialize the key manager with bot reference."""
    global key_manager, CLIENT_ID
    key_manager = SoundCloudKeyManager(bot)
    CLIENT_ID = key_manager.get_current_key()
    # Start background maintenance tasks
    try:
        key_manager.start_background_tasks()
    except Exception as e:
        logging.warning(f"Failed starting SoundCloud key background tasks: {e}")
    return CLIENT_ID

def manual_rotate_soundcloud_key(reason: str = 'manual'):
    """Rotate SoundCloud key manually. Returns dict with rotation info."""
    global key_manager, CLIENT_ID
    if not key_manager or not getattr(key_manager, 'api_keys', None):
        return {'rotated': False, 'error': 'No SoundCloud keys configured'}
    if len(key_manager.api_keys) == 1:
        return {'rotated': False, 'error': 'Only one SoundCloud key configured', 'keys': key_manager.get_status_rows()}
    old = key_manager.current_key_index
    new_key = key_manager.rotate_key(reason=reason)
    if new_key:
        CLIENT_ID = new_key
        return {
            'rotated': True,
            'old_index': old,
            'active_index': key_manager.current_key_index,
            'total_keys': len(key_manager.api_keys),
            'keys': key_manager.get_status_rows()
        }
    return {'rotated': False, 'error': 'Rotation failed (all keys exhausted)', 'keys': key_manager.get_status_rows()}

def get_soundcloud_key_status():
    global key_manager
    if not key_manager or not getattr(key_manager, 'api_keys', None):
        return {'loaded': False, 'keys': []}
    return {
        'loaded': True,
        'active_index': key_manager.current_key_index,
        'total_keys': len(key_manager.api_keys),
        'keys': key_manager.get_status_rows()
    }

# Cache duration for repeated SoundCloud lookups
CACHE_TTL = 300  # 5 minutes
# Load environment variables

# --- Cache helper with jitter ---
import random as _random

def _jittered_ttl(base:int, jitter:int=60):
    if jitter <= 0:
        return base
    delta = _random.randint(-jitter, jitter)
    return max(30, base + delta)  # enforce minimum 30s

# Unified resolve logic (remove duplicate _cached_resolve layering)

def resolve_url(url):
    """Resolve a SoundCloud URL to its API data with single-layer caching + jitter."""
    url = clean_soundcloud_url(url)  # Normalize the URL
    cache_key = f"resolve:{url}"
    cached = get_cache(cache_key)
    if cached:
        try:
            return json.loads(cached)
        except Exception:
            delete_cache(cache_key)
    resolve_endpoint = f"https://api-v2.soundcloud.com/resolve?url={url}&client_id={CLIENT_ID}"
    for attempt in range(3):
        if circuit_breaker_active():
            logging.warning("⛔ Circuit breaker active - skipping resolve")
            return None
        response = safe_request(resolve_endpoint)
        if response and response.status_code == 200:
            try:
                data = response.json()
            except Exception:
                data = None
            if data:
                set_cache(cache_key, json.dumps(data), ttl=_jittered_ttl(3600, 120))
                return data
        time.sleep(1 + attempt * 0.5)
    return None

# Backwards compatibility shim (kept name, uses new resolve_url)
@lru_cache(maxsize=512)
def _cached_resolve(url: str):
    return resolve_url(url)

# Rate limit detection patterns (case-insensitive)
RATE_LIMIT_PATTERNS = [
    "rate/request limit",
    "retry will occur after",
    "application has reached a rate",
    "too many requests",
    "rate limit",
    "exceeded",  # new pattern
    "over rate limit",  # new pattern
    "temporarily disabled",  # new pattern
]

def _ensure_client_id_param(url: str, client_id: str) -> str:
    """Ensure the URL contains the correct client_id parameter, replacing or appending as needed."""
    if 'client_id=' in url:
        return re.sub(r'client_id=[^&]+', f'client_id={client_id}', url)
    separator = '&' if '?' in url else '?'
    return f"{url}{separator}client_id={client_id}"

def safe_request(url, headers=None, retries=3, timeout=10):
    """HTTP request with adaptive timeout, rotation, circuit breaker, soft-fail HTML detection."""
    global CLIENT_ID, key_manager, TELEMETRY, CIRCUIT_BREAKER_UNTIL
    if circuit_breaker_active():
        logging.debug("🔌 Circuit breaker active - skipping request")
        return None
    if not CLIENT_ID:
        raise ValueError("No SoundCloud CLIENT_ID available")
    original_url = url
    last_error_body = None
    timeouts = [5, 10, 15]  # adaptive timeouts
    for attempt in range(retries):
        TELEMETRY['requests'] += 1
        # Update batch progress timestamp to avoid false watchdog inactivity rotations
        try:
            st = globals().get('BATCH_STATE')
            if isinstance(st, dict):
                st['last_progress_time'] = time.time()
        except Exception:
            pass
        try:
            eff_timeout = timeouts[min(attempt, len(timeouts)-1)]
            url = _ensure_client_id_param(original_url, CLIENT_ID)
            response = requests.get(url, headers=headers or HEADERS, timeout=eff_timeout)
            status = response.status_code
            # Soft-fail HTML detection
            ct = response.headers.get('Content-Type','')
            text_snip_full = response.text or ''
            text_snip = text_snip_full[:160].lower()
            if status == 200 and ('text/html' in ct.lower() or text_snip.startswith('<!doctype') or text_snip.startswith('<html')):
                TELEMETRY['html_soft_fail'] += 1
                logging.warning("⚠️ Received HTML instead of JSON (soft fail) - treating as rate limit (status 200 -> 429 synthetic)")
                status = 429  # treat as rate limit to trigger rotation
            try:
                body_lower = text_snip_full.lower()
            except Exception:
                body_lower = ""
            # Header-based detection
            rl_remaining = response.headers.get('x-ratelimit-remaining') or response.headers.get('X-RateLimit-Remaining')
            rl_limit = response.headers.get('x-ratelimit-limit') or response.headers.get('X-RateLimit-Limit')
            header_exhausted = rl_remaining == '0' and rl_limit is not None
            rate_text_hit = any(pat in body_lower for pat in RATE_LIMIT_PATTERNS)
            is_rate_limited = status in (401, 429, 403) or rate_text_hit or header_exhausted
            if is_rate_limited:
                logging.warning(
                    f"🚫 SoundCloud rate limit indicator: status={response.status_code} url={original_url} "
                    f"patterns_hit={rate_text_hit} header_exhausted={header_exhausted} remaining={rl_remaining} limit={rl_limit} attempt={attempt+1}/{retries}")
                if key_manager:
                    prev_key_value = CLIENT_ID
                    prev_index = key_manager.current_key_index
                    key_manager.mark_rate_limited()
                    new_key = key_manager.rotate_key(reason="rate_limit")
                    TELEMETRY['rotations'] += 1
                    if new_key:
                        # Always switch even if same string (avoid false circuit breaker)
                        CLIENT_ID = new_key
                        logging.warning(
                            f"🔄 Rotated SoundCloud key {prev_index+1} -> {key_manager.current_key_index+1} "
                            f"({'same value' if new_key == prev_key_value else 'new value'})")
                        time.sleep(0.35)
                        # Retry with new / same key
                        continue
                    else:
                        trip_circuit_breaker(reason="all_keys_exhausted_or_rotation_failed")
                        return None
                else:
                    logging.error("❌ key_manager missing - cannot rotate; tripping circuit breaker")
                    trip_circuit_breaker(reason="no_key_manager")
                    return None
            if status == 200:
                TELEMETRY['success'] += 1
                return response
            if status == 404:
                TELEMETRY['client_errors'] += 1
                return response
            if 500 <= status < 600:
                TELEMETRY['server_errors'] += 1
                if attempt < retries - 1:
                    logging.warning(f"⚠️ SoundCloud server error {status}; retrying (attempt {attempt+2}/{retries})")
                    time.sleep(1 + attempt)
                    continue
                return None
            # Other client errors
            if 400 <= status < 500:
                TELEMETRY['client_errors'] += 1
            last_error_body = body_lower
            response.raise_for_status()
        except Exception as e:
            if attempt < retries - 1:
                logging.warning(f"❌ SoundCloud request exception: {e} (retry {attempt+2}/{retries})")
                time.sleep(0.5 + attempt * 0.5)
                continue
            logging.error(f"❌ SoundCloud request failed permanently: {e} url={original_url}")
            return None
    return None

# Global headers for all requests to avoid 403 errors
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Connection": "keep-alive",
}

# Try to refresh client_id automatically when unauthorized
def refresh_client_id():
    """Attempt to fetch a working SoundCloud client ID by scanning asset scripts. Returns new id or None."""
    global CLIENT_ID
    try:
        logging.info("🔄 Attempting to refresh SoundCloud client ID…")
        html = requests.get("https://soundcloud.com", headers=HEADERS, timeout=10).text
        # Collect candidate asset script URLs
        script_urls = set(re.findall(r'src="(https://[^"]+sndcdn\.com/[^"]+\.js)"', html))
        # Also check inline HTML for client_id
        patterns = [
            r'client_id\s*[:=]\s*"([A-Za-z0-9]{32})"',
            r'clientId\s*[:=]\s*"([A-Za-z0-9]{32})"',
            r'"client_id"\s*:\s*"([A-Za-z0-9]{32})"',
            r'"clientId"\s*:\s*"([A-Za-z0-9]{32})"',
        ]
        def _find_in_text(text: str):
            for pat in patterns:
                m = re.search(pat, text)
                if m:
                    return m.group(1)
            return None

        cid = _find_in_text(html)
        if cid:
            CLIENT_ID = cid
            logging.info(f"✅ Refreshed SoundCloud client ID: {CLIENT_ID}")
            return CLIENT_ID

        # Scan up to first 8 asset scripts for client_id
        for i, js_url in enumerate(list(script_urls)[:8], start=1):
            try:
                js = requests.get(js_url, headers=HEADERS, timeout=10).text
                cid = _find_in_text(js)
                if cid:
                    CLIENT_ID = cid
                    logging.info(f"✅ Refreshed SoundCloud client ID (from asset {i}): {CLIENT_ID}")
                    return CLIENT_ID
            except requests.RequestException:
                continue

        logging.error("❌ Failed to find a new SoundCloud client ID after scanning assets.")
        return None
    except requests.RequestException as e:
        logging.error(f"❌ Error refreshing SoundCloud client ID: {e}")
        return None

def verify_client_id():
    """Verify if the SoundCloud CLIENT_ID is valid by hitting a stable API endpoint."""
    global CLIENT_ID
    if not CLIENT_ID:
        logging.warning("⚠️ No SoundCloud CLIENT_ID configured.")
        return False
    test_urls = [
        f"https://api-v2.soundcloud.com/search/tracks?q=hello&limit=1&client_id={CLIENT_ID}",
        f"https://api-v2.soundcloud.com/search/users?q=hello&limit=1&client_id={CLIENT_ID}",
    ]
    for url in test_urls:
        try:
            resp = requests.get(url, headers=HEADERS, timeout=10)
            if resp.status_code == 200:
                logging.info("✅ SoundCloud CLIENT_ID is valid.")
                return True
            if resp.status_code in (401, 403):
                logging.warning(f"⚠️ SoundCloud CLIENT_ID unauthorized (HTTP {resp.status_code}).")
                return False
            # 404 and others can be endpoint-related; try next
            logging.debug(f"Client ID check got HTTP {resp.status_code} for {url}, retrying alternative endpoint…")
        except requests.RequestException as e:
            logging.debug(f"Client ID check error on {url}: {e}")
    logging.error("❌ SoundCloud CLIENT_ID verification failed on all test endpoints.")
    return False

# --- Core URL Handling ---

def extract_soundcloud_user_id(artist_url):
    """Fetch SoundCloud user ID from artist profile URL."""
    cache_key = f"sc_user_id:{artist_url}"
    cached = get_cache(cache_key)  # Use get_cache
    if cached:
        return cached
    try:
        res = safe_request(
            f"https://api-v2.soundcloud.com/resolve?url={artist_url}&client_id={CLIENT_ID}",
            headers=HEADERS,
        )
        if not res:
            raise ValueError("Request failed")
        data = res.json()
        user_id = data.get("id")
        if user_id:
            set_cache(cache_key, user_id, ttl=CACHE_TTL)  # Use set_cache
        return user_id
    except Exception as e:
        raise ValueError(f"Failed to extract user ID from URL: {e}")

def clean_soundcloud_url(url):
    """Normalize and verify SoundCloud URLs.
    Supports regular and shortened (on.soundcloud.com) redirect links.
    Only performs a HEAD + final GET fetch; raises ValueError if invalid.
    """
    try:
        if not url:
            raise ValueError("Empty URL")
        url = url.strip()
        # Prepend scheme if missing
        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url

        # First: allow both soundcloud.com and on.soundcloud.com for redirect expansion
        parsed_initial = urlparse(url)
        host = parsed_initial.netloc.lower()
        if host.endswith(':80') or host.endswith(':443'):
            host = host.split(':')[0]

        # Expand on.soundcloud.com short link BEFORE strict base validation
        if 'on.soundcloud.com' in host:
            try:
                head_resp = requests.head(url, headers=HEADERS, allow_redirects=True, timeout=10)
                # Some short links may require GET if HEAD filtered
                if head_resp.status_code in (403, 405) or 'soundcloud.com' not in head_resp.url:
                    get_resp = requests.get(url, headers=HEADERS, allow_redirects=True, timeout=10)
                    final_url = get_resp.url
                else:
                    final_url = head_resp.url
                url = final_url
                parsed_initial = urlparse(url)
                host = parsed_initial.netloc.lower()
                logging.debug(f"🔄 Expanded short SoundCloud URL to: {url}")
            except Exception as e:
                raise ValueError(f"Failed to expand short link: {e}")

        # Validate domain now
        if 'soundcloud.com' not in host:
            raise ValueError(f"Invalid SoundCloud domain: {host}")

        # Strip duplicate nested prefixes if somehow present
        while 'https://soundcloud.com/https://soundcloud.com/' in url:
            url = url.replace('https://soundcloud.com/https://soundcloud.com/', 'https://soundcloud.com/')

        # Fetch page to ensure existence (use GET not safe_request because this is HTML)
        page_resp = requests.get(url, headers=HEADERS, timeout=10)
        if page_resp.status_code == 404:
            raise ValueError("SoundCloud URL returned 404")
        page_resp.raise_for_status()
        logging.info(f"✅ Validated SoundCloud URL: {url}")

        # Canonical <link>
        match = re.search(r'<link rel="canonical" href="([^"]+)"', page_resp.text)
        canonical = match.group(1) if match else url
        return canonical
    except Exception as e:
        logging.error(f"❌ URL validation failed for {url}: {e}")
        raise ValueError(f"URL validation failed: {e}")

def expand_soundcloud_short_url(url: str) -> str:
    """Expand on.soundcloud.com short (Smart Link) URLs to canonical soundcloud.com URLs.
    Returns the final expanded URL or raises ValueError.
    """
    try:
        if 'on.soundcloud.com' not in url.lower():
            return url
        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url
        head_resp = requests.head(url, headers=HEADERS, allow_redirects=True, timeout=10)
        final_url = head_resp.url
        if (head_resp.status_code in (403, 405) or 'soundcloud.com' not in final_url) and head_resp.status_code != 301:
            # Fallback to GET if HEAD blocked
            get_resp = requests.get(url, headers=HEADERS, allow_redirects=True, timeout=10)
            final_url = get_resp.url
        if 'soundcloud.com' not in final_url:
            raise ValueError(f"Expansion did not resolve to soundcloud.com: {final_url}")
        return final_url
    except Exception as e:
        raise ValueError(f"Failed to expand short SoundCloud URL: {e}")

def extract_soundcloud_username(url):
    """Extract the username from a SoundCloud URL."""
    clean_url = clean_soundcloud_url(url)
    parsed = urlparse(clean_url)
    path_segments = [p for p in parsed.path.strip('/').split('/') if p]

    if not path_segments:
        raise ValueError("No path segments in URL")

    return path_segments[0]

# --- Artist Data Fetching ---

def get_artist_info(url_or_username):
    """Resolve a SoundCloud user from a full profile URL or username."""
    url_or_username = clean_soundcloud_url(url_or_username)  # Normalize the URL
    cache_key = f"sc_artist_info:{url_or_username}"
    cached = get_cache(cache_key)  # Use get_cache
    if cached:
        return json.loads(cached)

    try:
        # Extract username from URL if necessary
        if url_or_username.startswith("http"):
            username = extract_soundcloud_username(url_or_username)
        else:
            username = url_or_username

        # Build the resolve URL
        resolve_url = f"https://api-v2.soundcloud.com/resolve?url=https://soundcloud.com/{username}&client_id={CLIENT_ID}"
        response = safe_request(resolve_url, headers=HEADERS)

        # Handle invalid responses
        if not response or response.status_code != 200:
            raise ValueError(f"Failed to resolve SoundCloud user: {url_or_username}")

        data = response.json()

        # Ensure the response contains valid artist data
        if not data or data.get('kind') != 'user':
            raise ValueError(f"Invalid artist data for: {url_or_username}")

        # Extract artist information
        info = {
            'id': data.get('id', username),
            'name': data.get('username', 'Unknown Artist'),
            'url': data.get('permalink_url', f"https://soundcloud.com/{username}"),
            'track_count': data.get('track_count', 0),
            'avatar_url': data.get('avatar_url', ''),
            'followers': data.get('followers_count', 0)
        }

        # Cache the result
        set_cache(cache_key, json.dumps(info), ttl=CACHE_TTL)  # Use set_cache
        return info

    except Exception as e:
        logging.error(f"Error fetching artist info for {url_or_username}: {e}")
        return {'id': url_or_username, 'name': 'Unknown Artist', 'url': f"https://soundcloud.com/{url_or_username}"}
       
# --- Release Data Fetching ---

def get_last_release_date(artist_url):
    cache_key = f"sc_last_release:{artist_url}"
    cached = get_cache(cache_key)  # Use get_cache
    if cached:
        return cached
    try:
        artist_info = get_artist_info(artist_url)
        artist_id = artist_info['id']

        tracks_url = f"https://api-v2.soundcloud.com/users/{artist_id}/tracks?client_id={CLIENT_ID}&limit=5&order=created_at"
        response = safe_request(tracks_url, headers=HEADERS)
        if not response:
            return None
        
        tracks = response.json()
        if not tracks:
            return None

        latest_track = max(tracks, key=lambda t: t['created_at'])
        created = latest_track.get('created_at')
        if created:
            set_cache(cache_key, created, ttl=CACHE_TTL)  # Use set_cache
        return created
    except Exception as e:
        print(f"Error getting last release: {e}")
        return None

def get_release_info(url):
    """Universal release info fetcher for tracks/playlists/artists."""
    cache_key = f"sc_release:{url}"
    cached = get_cache(cache_key)  # Use get_cache
    if cached:
        return json.loads(cached)
    try:
        clean_url = clean_soundcloud_url(url)
        resolve_url = f"https://api-v2.soundcloud.com/resolve?url={clean_url}&client_id={CLIENT_ID}"
        response = safe_request(resolve_url, headers=HEADERS)
        if not response:
            raise ValueError("Request failed")

        data = response.json()

        if data['kind'] == 'track':
            info = {
                "title": data.get("title"),
                "artist_name": (data.get("user") or {}).get("username"),
                "url": data.get("permalink_url"),
                "upload_date": data.get("created_at"),
                "release_date": data.get("release_date") or data.get("created_at"),
                "cover_url": data.get("artwork_url"),
                "track_count": 1,
                "duration": format_duration(data.get("duration", 0)),
                "features": extract_track_features(data),
                "genres": [data.get("genre")] if data.get("genre") else [],
                "type": "track",
                "repost": False,
            }
        elif data['kind'] == 'playlist':
            info = process_playlist(data)
        elif data['kind'] == 'user':
            info = get_artist_release(data)
        else:
            raise ValueError("Unsupported content type")
    
        set_cache(cache_key, json.dumps(info), ttl=CACHE_TTL)  # Use set_cache
        return info
    except Exception as e:
        raise ValueError(f"Release info fetch failed: {e}")

def get_soundcloud_playlist_info(artist_url, force_refresh: bool = False):
    try:
        cache_key = f"playlists:{artist_url}"
        if not force_refresh:
            cached = get_cache(cache_key)
            if cached:
                return json.loads(cached) if isinstance(cached, str) else cached
        else:
            logging.debug(f"[SC] force_refresh=True bypassing playlist cache for {artist_url}")
        resolved = get_artist_info(artist_url)
        user_id = resolved.get("id")
        if not user_id:
            raise ValueError(f"Could not resolve user ID for {artist_url}")
        url = f"https://api-v2.soundcloud.com/users/{user_id}/playlists?client_id={CLIENT_ID}&limit=5"
        response = safe_request(url)
        if not response or response.status_code != 200:
            logging.warning(f"No playlists response for {artist_url}")
            return None
        try:
            data = response.json()
        except Exception:
            record_data_anomaly('invalid_json', url, 'playlists')
            return None
        playlists = data.get("collection", [])
        if not playlists:
            record_data_anomaly('empty_collection', url, 'playlists')
            logging.debug(f"Empty playlist collection for {artist_url}")
            return None
        _update_nonempty_baseline('playlists')
        latest_playlist = max(playlists, key=lambda p: p.get("created_at", ""))
        tracks = []
        genre_set = set()
        total_duration_ms = 0
        for index, track in enumerate(latest_playlist.get("tracks", [])):
            if isinstance(track, dict):
                tid = str(track.get("id"))
                ttitle = str(track.get("title"))
                tdur = track.get("duration") or 0
                tgenre = (track.get("genre") or "").strip()
                if tgenre:
                    genre_set.add(tgenre)
                total_duration_ms += tdur
                tracks.append({
                    "id": tid,
                    "title": ttitle,
                    "duration": tdur,
                    "genre": tgenre,
                    "order": index
                })
        playlist_duration = None
        if total_duration_ms > 0:
            playlist_duration = format_duration(total_duration_ms)
        if not playlist_duration and tracks:
            try:
                fallback_ms = sum(int(t.get("duration") or 0) for t in latest_playlist.get("tracks", []))
                if fallback_ms > 0:
                    playlist_duration = format_duration(fallback_ms)
                else:
                    playlist_duration = "0:00"
            except Exception:
                playlist_duration = None
        result = {
            "title": latest_playlist.get("title"),
            "artist_name": latest_playlist.get("user", {}).get("username"),
            "url": latest_playlist.get("permalink_url"),
            "release_date": latest_playlist.get("created_at"),
            "cover_url": latest_playlist.get("artwork_url"),
            "track_count": len(tracks),
            "tracks": tracks,
            "type": classify_sc_playlist(latest_playlist) if 'classify_sc_playlist' in globals() else "playlist",
            "genres": sorted(genre_set),
            "duration": playlist_duration,
            "features": None
        }
        if not result.get('url'):
            record_data_anomaly('missing_field', url, 'playlist_url')
        # Short TTL (~60s ± jitter) to prevent 2-cycle detection lag
        set_cache(cache_key, json.dumps(result), ttl=_jittered_ttl(60, 15))
        return result
    except Exception as e:
        logging.error(f"Error checking playlists: {e}")
        return None

def classify_sc_playlist(original: dict) -> str:
    """
    Classify a SoundCloud playlist strictly by SoundCloud's own type:
    - If set_type/playlist_type is 'album' or 'ep' -> return that
    - Otherwise treat as a regular 'playlist'
    Never infer album/ep by track count or title to avoid mislabeling.
    """
    try:
        if (original.get('kind') or '').lower() != 'playlist':
            return 'track'
        set_type = (original.get('set_type') or original.get('playlist_type') or '').strip().lower()
        # Map known set types; default to playlist
        mapping = {
            'album': 'album',
            'ep': 'ep',
            'single': 'track',        # rare on playlists; map to track if encountered
            'compilation': 'album'    # treat compilation as album for embeds
        }
        return mapping.get(set_type, 'playlist')
    except Exception:
        return 'playlist'

def get_soundcloud_likes_info(artist_url, force_refresh=False):
    """Fetch and process liked tracks/playlists from a SoundCloud user with playlist resolve batching.
    Adds anomaly detection for repeated empty collections.
    """
    try:
        cache_key = f"likes:{artist_url}"
        if not force_refresh:
            cached = get_cache(cache_key)
            if cached:
                logging.info(f"✅ Cache hit for likes: {artist_url}")
                return json.loads(cached)
        logging.info(f"⏳ Fetching likes for {artist_url}...")
        resolved = resolve_url(artist_url)
        if not resolved or "id" not in resolved:
            logging.warning(f"⚠️ Could not resolve SoundCloud user ID from {artist_url}")
            return []
        user_id = resolved["id"]
        url = f"https://api-v2.soundcloud.com/users/{user_id}/likes?client_id={CLIENT_ID}&limit=10"
        response = safe_request(url)
        if not response:
            logging.warning(f"⚠️ No response received for likes: {artist_url}")
            return []
        try:
            data = response.json()
        except Exception:
            record_data_anomaly('invalid_json', url, 'likes')
            return []
        if not data or "collection" not in data:
            record_data_anomaly('missing_collection', url, 'likes')
            return []
        if not data.get('collection'):
            record_data_anomaly('empty_collection', url, 'likes')
            return []
        _update_nonempty_baseline('likes')
        likes = []
        playlist_cache = {}
        for item in data.get("collection", []):
            original = item.get("track") or item.get("playlist")
            if not original:
                continue
            content_type = "track"
            tracks_data = None
            if original.get('kind') == 'playlist':
                # STRICT classification: only use SoundCloud's set_type/playlist_type
                content_type = classify_sc_playlist(original)
            like_date = item.get("created_at")
            if not like_date:
                continue
            track_release_date = original.get("created_at") or like_date
            genres = []
            if content_type == 'playlist' and tracks_data:
                unique_genres = { (t.get('genre') or '').strip() for t in tracks_data if t.get('genre') }
                genres = sorted(g for g in unique_genres if g)
            else:
                if original.get('genre'):
                    genres = [original.get('genre')]
            # Duration formatting
            duration = None
            if original.get('duration'):
                ms = original['duration']
                seconds = ms // 1000
                minutes = seconds // 60
                remaining_seconds = seconds % 60
                if minutes >= 60:
                    hours = minutes // 60
                    minutes = minutes % 60
                    duration = f"{hours}:{minutes:02d}:{remaining_seconds:02d}"
                else:
                    duration = f"{minutes}:{remaining_seconds:02d}"
            likes.append({
                "track_id": original.get("id"),
                "title": original.get("title"),
                "artist_name": original.get("user", {}).get("username"),
                "url": original.get("permalink_url"),
                "upload_date": original.get("created_at"),
                "release_date": original.get("release_date") or original.get("created_at"),
                "liked_date": like_date,
                "cover_url": original.get("artwork_url"),
                "features": extract_track_features(original),
                "track_count": original.get("track_count", 1),
                "duration": duration,
                "genres": genres,
                "content_type": content_type
            })
        set_cache(cache_key, json.dumps(likes), ttl=_jittered_ttl(60, 15))
        return likes
    except Exception as e:
        logging.error(f"Error fetching likes for {artist_url}: {e}")
        return []

def get_soundcloud_reposts_info(artist_url, force_refresh: bool = False):
    """Fetch and process reposts from a SoundCloud user with anomaly detection.
    force_refresh=True bypasses the cache to avoid 2-cycle delays."""
    try:
        cache_key = f"reposts:{artist_url}"
        if not force_refresh:
            cached = get_cache(cache_key)
            if cached:
                return json.loads(cached)
        resolved = resolve_url(artist_url)
        if not resolved or "id" not in resolved:
            logging.warning(f"⚠️ Could not resolve SoundCloud user ID from {artist_url}")
            return []
        user_id = resolved["id"]
        endpoints = [
            f"https://api-v2.soundcloud.com/users/{user_id}/reposts?client_id={CLIENT_ID}&limit=10",
            f"https://api-v2.soundcloud.com/users/{user_id}/track_reposts?client_id={CLIENT_ID}&limit=10",
            f"https://api-v2.soundcloud.com/stream/users/{user_id}/reposts?client_id={CLIENT_ID}&limit=10"
        ]
        response = None
        last_endpoint = None
        for endpoint in endpoints:
            last_endpoint = endpoint
            try:
                response = safe_request(endpoint)
                if response and response.status_code == 200:
                    break
            except Exception as e:
                logging.debug(f"Failed endpoint {endpoint}: {e}")
                continue
        if not response:
            record_data_anomaly('no_response', last_endpoint or 'unknown', 'reposts')
            logging.warning(f"⚠️ Could not fetch reposts from any endpoint for {artist_url}")
            return []
        try:
            data = response.json()
        except Exception:
            record_data_anomaly('invalid_json', last_endpoint or 'unknown', 'reposts')
            return []
        reposts = []
        coll = data.get('collection', [])
        if not coll:
            record_data_anomaly('empty_collection', last_endpoint or 'unknown', 'reposts')
            return []
        _update_nonempty_baseline('reposts')
        for item in coll:
            try:
                original = item.get("track") or item.get("playlist")
                if not original:
                    continue
                content_type = 'track'
                if original.get('kind') == 'playlist':
                    # STRICT classification: only use SoundCloud's set_type/playlist_type
                    content_type = classify_sc_playlist(original)
                repost_date = item.get("created_at")
                if not repost_date:
                    continue
                reposts.append({
                    "track_id": original.get("id"),
                    "title": original.get("title"),
                    "artist_name": original.get("user", {}).get("username"),
                    "url": original.get("permalink_url"),
                    "upload_date": original.get("created_at"),
                    "release_date": original.get("release_date") or original.get("created_at"),
                    "reposted_date": repost_date,
                    "cover_url": original.get("artwork_url"),
                    "features": extract_track_features(original),
                    "track_count": original.get("track_count", 1),
                    "duration": format_duration(original.get("duration", 0)),
                    "genres": [original.get("genre")] if original.get("genre") else [],
                    "content_type": content_type
                })
            except Exception as e:
                logging.warning(f"Error processing repost: {e}")
                continue
        # Short adaptive TTL (mirror likes ~60s) to reduce repost lag
        ttl = _jittered_ttl(60, 15) if 'playlist' not in artist_url else _jittered_ttl(70, 20)
        set_cache(cache_key, json.dumps(reposts), ttl=ttl)
        return reposts
    except Exception as e:
        logging.error(f"Error fetching reposts for {artist_url}: {e}")
        return []

# --- Data Processing ---

def process_track(track_data):
    """Convert track data to standardized format."""
    return {
        'type': 'track',
        'artist_name': track_data['user']['username'],
        'title': track_data['title'],
        'url': track_data['permalink_url'],
        'release_date': track_data.get('created_at', ''),
        'cover_url': track_data.get('artwork_url') or track_data['user'].get('avatar_url', ''),
        'duration': format_duration(track_data.get('duration', 0)),
        # Use robust extractor (publisher_metadata.artist) instead of title-only parsing
        'features': extract_track_features(track_data),
        'genres': [track_data.get('genre', '')] if track_data.get('genre') else [],
        'repost': track_data.get('repost', False),
        'track_count': 1
    }

def process_playlist(playlist_data):
    """Convert playlist data to standardized format."""
    total_duration = sum(t.get('duration', 0) for t in playlist_data['tracks'])
    features = set()
    genres = set()

    # Process each track's genres and tags
    for track in playlist_data['tracks']:
        # Add features from track titles
        features.update(extract_features(track['title']).split(', '))
        
        # Add direct genre
        if track.get('genre'):
            genres.add(track.get('genre'))
            
        # Add genre tags
        if track.get('tags'):
            genres.update([
                tag.strip() for tag in track.get('tags', '').split() 
                if 'genre:' in tag.lower() or 
                any(g in tag.lower() for g in ['rap', 'hip-hop', 'trap', 'edm', 'electronic', 'rock'])
            ])

    # Also check playlist-level genres/tags
    if playlist_data.get('genre'):
        genres.add(playlist_data.get('genre'))
    if playlist_data.get('tags'):
        genres.update([
            tag.strip() for tag in playlist_data.get('tags', '').split() 
            if 'genre:' in tag.lower() or 
            any(g in tag.lower() for g in ['rap', 'hip-hop', 'trap', 'edm', 'electronic', 'rock'])
        ])

    # Clean up sets
    features.discard('None')
    genres.discard('None')
    genres.discard('')

    return {
        'type': 'playlist',  # Do not auto-upgrade; semantic distinction preserved
        'artist_name': playlist_data['user']['username'],
        'title': playlist_data['title'],
        'url': playlist_data['permalink_url'],
        'release_date': playlist_data.get('created_at', ''),
        'cover_url': playlist_data.get('artwork_url') or playlist_data.get('avatar_url', ''),
        'duration': format_duration(total_duration),
        'features': ', '.join(sorted(features)) if features else None,
        'genres': sorted(list(genres)) if genres else ['Unknown'],  # Return list of genres or ['Unknown']
        'repost': False,
        'track_count': len(playlist_data['tracks'])
    }

def get_artist_release(artist_data):
    """Get latest track release for artist."""
    try:
        tracks_url = (
            f"https://api-v2.soundcloud.com/users/{artist_data['id']}/tracks"
            f"?client_id={CLIENT_ID}&limit=1&linked_partitioning=1&representation=full"
        )
        response = safe_request(tracks_url, headers=HEADERS)
        if not response:
            logging.warning("SoundCloud artist tracks request failed")
            return None

        data = response.json()


        # Some responses return 'collection', not raw list
        tracks = data.get('collection', data if isinstance(data, list) else [])

        if not tracks:
            return None

        return process_track(tracks[0])
    except Exception as e:
        logging.error(f"Artist release fetch failed: {e}")
        return None

# --- Utility Functions ---

def format_duration(ms):
    """Convert milliseconds to formatted duration string."""
    if not ms:
        return None
    seconds = ms // 1000
    minutes = seconds // 60
    seconds = seconds % 60
    if minutes >= 60:
        hours = minutes // 60
        minutes = minutes % 60
        return f"{hours}:{minutes:02d}:{seconds:02d}"
    return f"{minutes}:{seconds:02d}"

# Precompile feature extraction regex patterns
_FEATURE_PATTERNS = [
    # Only treat "with" as a feature when it’s inside () or []
    re.compile(r"\((?:feat|ft|with)\.?\s*([^)]+)\)", re.IGNORECASE),
    re.compile(r"\[(?:feat|ft|with)\.?\s*([^\]]+)\]", re.IGNORECASE),
    # Allow non-parenthesized feat/ft, but NOT "with"
    re.compile(r"(?:feat|ft)\.?\s+([^\-–()\[\]]+)", re.IGNORECASE),
    # Keep "w/" shorthand
    re.compile(r"w/\s*([^\-–()\[\]]+)", re.IGNORECASE),
]
_MAX_FEATURE_CHARS = 120

def extract_features(title):
    """Extract featured artists from track titles using precompiled patterns with trimming."""
    features = set()
    if not title:
        return "None"
    for pattern in _FEATURE_PATTERNS:
        matches = pattern.findall(title or '')
        for match in matches:
            cleaned = (match or '').strip()
            if not cleaned:
                continue
            # Split on common separators
            parts = re.split(r'\s*(?:/|&|,| and | x | × )\s*', cleaned, flags=re.IGNORECASE)
            for name in parts:
                n = (name or '').strip()
                if not n:
                    continue
                low = n.lower()
                if low in {'none', 'unknown', '-', 'n/a', 'na', 'various artists', 'va'}:
                    continue
                features.add(n)
    if not features:
        return "None"
    out = ", ".join(sorted(features))
    if len(out) > _MAX_FEATURE_CHARS:
        out = out[:_MAX_FEATURE_CHARS-3].rstrip(', ') + '...'
    return out

def extract_track_features(track: dict, main_artist: str | None = None):
    """
    Extract collaborators/features from a SoundCloud track payload.
    Prefers publisher_metadata.artist, falls back to title parsing.
    Returns a list of names excluding the main artist.
    """
    if not isinstance(track, dict):
        return None

    # Main artist to exclude
    main = (main_artist or (track.get('user') or {}).get('username') or '').strip()

    # 1) Prefer publisher_metadata.artist (e.g., "1649194610, icant2")
    artist_line = None
    pm = track.get('publisher_metadata') or {}
    if isinstance(pm, dict):
        artist_line = pm.get('artist')

    # 2) Other possible fields
    if not artist_line:
        artist_line = track.get('artist') or track.get('display_artist')

    names: list[str] = []
    if isinstance(artist_line, str):
        txt = artist_line.strip()
        if txt:
            parts = re.split(r'\s*(?:/|&|,| and | x | × )\s*', txt, flags=re.IGNORECASE)
            seen = set()
            for p in parts:
                name = (p or '').strip()
                if not name:
                    continue
                low = name.lower()
                if low in {'none', 'unknown', '-', 'n/a', 'na', 'various artists', 'va'}:
                    continue
                if main and low == main.lower():
                    continue
                if low not in seen:
                    seen.add(low)
                    names.append(name)

    # 3) Fallback: parse title "(feat ...)", "ft ...", "w/ ..." etc.
    if not names:
        try:
            extracted = extract_features(track.get('title', '') or '')
            if isinstance(extracted, list):
                names = [n for n in extracted if n]
            elif isinstance(extracted, str) and extracted.lower() != 'none':
                names = [p.strip() for p in extracted.split(',') if p.strip()]
        except Exception:
            names = []

    return names or None

# --- Bot Integration Helpers ---

def get_soundcloud_artist_name(url):
    """Get artist name from SoundCloud profile URL."""
    try:
        return get_artist_info(url)['name']
    except Exception as e:
        print(f"Error getting artist name: {e}")
        return "Unknown Artist"

def get_artist_name_by_url(url):
    """Get artist name from any SoundCloud URL."""
    return get_soundcloud_artist_name(url)

def get_soundcloud_artist_id(url):
    """Get the numeric artist ID from a SoundCloud URL."""
    try:
        artist_info = get_artist_info(url)
        return artist_info['id']
    except Exception as e:
        print(f"Error getting artist ID: {e}")
        return None

def get_soundcloud_release_info(url):
    """
    Resolve any SoundCloud URL (track / playlist / user profile) into a normalized release dict.
    Adds fallback for user profiles with empty initial track collections.
    """
    cache_key = f"sc_release:{url}"
    cached = get_cache(cache_key)
    if cached:
        try:
            return json.loads(cached)
        except Exception:
            pass  # Corrupt cache entry -> rebuild

    try:
        resolved = _cached_resolve(url)
        if not resolved:
            logging.warning(f"Resolve failed for {url}")
            return None

        kind = resolved.get("kind")
        info = None

        if kind == "track":
            info = process_track(resolved)

        elif kind == "playlist":
            # Ensure 'tracks' key present (sometimes truncated)
            if not resolved.get("tracks"):
                playlist_api = f"https://api-v2.soundcloud.com/playlists/{resolved.get('id')}?client_id={CLIENT_ID}"
                full_resp = safe_request(playlist_api, headers=HEADERS)
                if full_resp and full_resp.status_code == 200:
                    resolved = full_resp.json()
            info = process_playlist(resolved)

        elif kind == "user":
            # Try to get latest track(s)
            user_id = resolved.get("id")
            if user_id:
                tracks_url = (
                    f"https://api-v2.soundcloud.com/users/{user_id}/tracks"
                    f"?client_id={CLIENT_ID}&limit=5&linked_partitioning=1&representation=full"
                )
                resp = safe_request(tracks_url, headers=HEADERS)
                if resp and resp.status_code == 200:
                    data = resp.json()
                    tracks = data.get('collection', data if isinstance(data, list) else [])
                    if tracks:
                        # Pick newest by created_at
                        newest = max(tracks, key=lambda t: t.get('created_at', '') or '')
                        info = process_track(newest)
            if not info:
                logging.info(f"ℹ️ No tracks available after fallback for {url}")
                return None
        else:
            logging.warning(f"Unsupported content kind '{kind}' for {url}")
            return None

        if info:
            set_cache(cache_key, json.dumps(info), ttl=CACHE_TTL)
        return info

    except Exception as e:
        logging.error(f"Error fetching release info for {url}: {e}")
        return None

def extract_soundcloud_id(url):
    """Alias for extract_soundcloud_username, for compatibility."""
    return extract_soundcloud_username(url)

def seconds_to_minutes_seconds(seconds):
    """Helper to format duration."""
    minutes = seconds // 60
    seconds = seconds % 60
    return f"{minutes}:{str(seconds).zfill(2)}"

def safe_get(url, headers=None, retries=3):
    for attempt in range(retries):
        response = requests.get(url, headers=headers)
        if response.status_code == 429:
            retry_after = int(response.headers.get("Retry-After", 5))
            print(f"Rate limited. Sleeping for {retry_after} seconds...")
            time.sleep(retry_after)
            continue
        response.raise_for_status()
        return response
    return None

def get_soundcloud_likes(artist_url):
    """Backward-compatible wrapper returning likes list (uses likes_info)."""
    return get_soundcloud_likes_info(artist_url)

RATE_LIMIT_DELAY = 5  # Delay in seconds between requests

def rate_limited_request(url, headers=None):
    time.sleep(RATE_LIMIT_DELAY)  # Enforce delay between requests
    return safe_request(url, headers=headers)

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
        # Fixed mismatched bracket
        color = self.COLORS.get(record.levelname, self.RESET)
        record.msg = f"{color}{record.msg}{self.RESET}"
        return super().format(record)

# --- Release batch monitoring integration (silent rate limit / truncation) ---
# We already have _ReleaseFetchMonitor; add helpers to reset and automatic rotation logic.

def begin_soundcloud_release_batch():
    """Reset the per-run release fetch monitor. Call at start of a SoundCloud sweep."""
    try:
        mon = get_release_monitor()
        mon.reset()
    except Exception:
        pass

def note_soundcloud_release_fetch(success: bool, context: str = ""):
    """Record outcome of a single artist release fetch. If thresholds exceeded, rotate key automatically.
    success=True means the API calls succeeded (even if no new release)."""
    global CLIENT_ID, key_manager
    try:
        mon = get_release_monitor()
        mon.note(success, context)
        if mon.should_rotate() and key_manager:
            logging.warning("⚠️ SoundCloud release fetch failures threshold reached – rotating key (silent limit suspected)")
            key_manager.mark_rate_limited()
            new_key = key_manager.rotate_key(reason="batch_consecutive_failures")
            if new_key:
                CLIENT_ID = new_key
                TELEMETRY['rotations'] += 1
                mon.mark_rotated()
                logging.info("🔄 SoundCloud key rotated due to batch failure heuristic")
            else:
                logging.error("🛑 Unable to rotate SoundCloud key (all exhausted) – activating circuit breaker")
                trip_circuit_breaker(reason="batch_fail_rotation_exhausted")
    except Exception as e:
        logging.debug(f"note_soundcloud_release_fetch error: {e}")

# --- Missing simple wrappers expected by imports (provide lightweight adapters) ---

def get_soundcloud_reposts(artist_url):
    """Backward-compatible wrapper returning repost list (uses reposts_info)."""
    return get_soundcloud_reposts_info(artist_url)

def clear_cache(key):
    """Clear a specific cache key."""
    delete_cache(key)
    logging.info(f"✅ Cleared cache for key: {key}")

def clear_malformed_cache():
    """Clear cache entries with malformed URLs."""
    for key in get_all_cache_keys():  # Retrieve all cache keys
        if "https://soundcloud.com/https://soundcloud.com/" in key:
            delete_cache(key)
            logging.info(f"✅ Cleared malformed cache key: {key}")

def get_all_cache_keys():
    """Retrieve all cache keys from SQLite."""
    try:
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT key FROM cache")
            return [row[0] for row in cursor.fetchall()]
    except Exception as e:
        logging.error(f"❌ Error retrieving cache keys: {e}")
        return []
    
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')
# Support JSON logging toggle via env var LOG_JSON=1
if os.getenv('LOG_JSON') == '1':
    class _JSONFormatter(logging.Formatter):
        def format(self, record):
            import json as _json, time as _time
            payload = {
                'ts': _time.strftime('%Y-%m-%dT%H:%M:%SZ', _time.gmtime(record.created)),
                'level': record.levelname,
                'msg': record.getMessage(),
                'logger': record.name,
                'module': record.module
            }
            return _json.dumps(payload, ensure_ascii=False)
    logging.getLogger().handlers[0].setFormatter(_JSONFormatter())
else:
    logging.getLogger().handlers[0].setFormatter(RailwayLogFormatter())

def determine_release_type(playlist_data, tracks_data):
    """Determine release type with priority system.
    1. Check native SoundCloud kind
    2. Check title keywords
    3. Check track count as last resort
    """
    # 1. First check SoundCloud's native kind
    if (kind := playlist_data.get('kind')) == 'playlist':
        # Only override if explicit album/EP indicators exist
        title = playlist_data.get('title', '').lower()
        if any(kw in title for kw in ['album', 'lp', 'record']):
            return 'album'
        elif any(kw in title for kw in ['ep', 'extended play']):
            return 'EP'
        else:
            return 'playlist'  # Default to playlist if that's what SoundCloud says it is

    # 2. Check title keywords
    title_indicators = {
        'album': ['album', 'lp', 'record'],
        'EP': ['ep', 'extended play'],
        'mixtape': ['mixtape', 'mix tape'],
        'compilation': ['compilation', 'various artists', 'va']
    }
    
    for release_type, keywords in title_indicators.items():
        if any(keyword in title for keyword in keywords):
            return release_type.lower()

    # 3. Only use track count as last resort if no other indicators exist
    track_count = len(tracks_data) if tracks_data else 0
    if track_count >= 7:
        return 'deluxe' if 'deluxe' in title else 'album'
    elif track_count >= 2:
        return 'EP'
    
    return 'track'

# --- Telemetry accessors ---

def get_soundcloud_telemetry_snapshot():
    from datetime import datetime as _dt, timezone as _tz
    return {
        'timestamp': _dt.now(_tz.utc).isoformat(),
        'telemetry': TELEMETRY.copy(),
        'circuit_breaker': get_circuit_breaker_status(),
        'keys': get_soundcloud_key_status(),
    }

# Expose shutdown helper

def stop_soundcloud_background_tasks():
    global key_manager
    try:
        if key_manager:
            key_manager.stop_background_tasks()
    except Exception as e:
        logging.error(f"Failed stopping SoundCloud background tasks: {e}")

# --- Tier 2 & 3 Batch / Heuristic Monitoring State ---
BATCH_STATE = {
    'start_time': None,
    'expected_total': None,
    'processed': 0,
    'latencies_ms': [],
    'baseline_latency_ms': None,  # exponential moving average
    'watchdog_task': None,
    'last_progress_time': None,
}

BATCH_WATCHDOG_GRACE = 180  # seconds after which inactivity considered stall
BATCH_EXPECTED_FALLOUT_FACTOR = 1.6  # expected_total * avg_latency * factor -> max duration
EMA_ALPHA = 0.3

async def _soundcloud_batch_watchdog():
    """Detect stalled / abruptly halted SoundCloud release sweeps (Tier 3 heuristic)."""
    global BATCH_STATE, CLIENT_ID
    try:
        while True:
            await asyncio.sleep(30)
            if not BATCH_STATE['start_time']:
                continue
            now = time.time()
            # Inactivity detection
            if BATCH_STATE['last_progress_time'] and (now - BATCH_STATE['last_progress_time']) > BATCH_WATCHDOG_GRACE:
                logging.warning("🕒 SoundCloud batch watchdog: inactivity > grace threshold; rotating key (possible silent limit)")
                if key_manager:
                    key_manager.mark_rate_limited()
                    new_key = key_manager.rotate_key(reason="watchdog_inactivity")
                    if new_key:
                        CLIENT_ID = new_key
                # Reset to avoid repeated rotations
                BATCH_STATE['last_progress_time'] = now
            # Duration overrun detection
            if BATCH_STATE['expected_total'] and BATCH_STATE['latencies_ms']:
                avg_lat = sum(BATCH_STATE['latencies_ms']) / len(BATCH_STATE['latencies_ms']) / 1000.0
                elapsed = now - BATCH_STATE['start_time']
                projected_max = avg_lat * BATCH_STATE['expected_total'] * BATCH_EXPECTED_FALLOUT_FACTOR
                if elapsed > projected_max and BATCH_STATE['processed'] < BATCH_STATE['expected_total']:
                    logging.warning(f"⏱️ SoundCloud batch exceeded projected duration ({elapsed:.1f}s > {projected_max:.1f}s) with incomplete progress {BATCH_STATE['processed']}/{BATCH_STATE['expected_total']}; rotating key.")
                    if key_manager:
                        key_manager.mark_rate_limited()
                        new_key = key_manager.rotate_key(reason="watchdog_overrun")
                        if new_key:
                            CLIENT_ID = new_key
                    # Break after action to avoid repeat until next batch
                    break
    except asyncio.CancelledError:
        pass
    except Exception as e:
        logging.debug(f"Watchdog task error: {e}")

# Update begin function to accept expected_total
# ...existing code...

def begin_soundcloud_release_batch(expected_total: int = None):
    """Reset per-run monitor & establish batch state (Tier 3 heuristics)."""
    try:
        mon = get_release_monitor()
        mon.reset()
    except Exception:
        pass
    # Initialize batch state
    BATCH_STATE['start_time'] = time.time()
    BATCH_STATE['expected_total'] = expected_total
    BATCH_STATE['processed'] = 0
    BATCH_STATE['latencies_ms'] = []
    BATCH_STATE['last_progress_time'] = BATCH_STATE['start_time']
    # Cancel previous watchdog
    task = BATCH_STATE.get('watchdog_task')
    if (task and not task.done()):
        task.cancel()
    # Launch new watchdog if we have an event loop (bot started)
    try:
        if key_manager and key_manager.bot:
            loop = asyncio.get_event_loop()
            BATCH_STATE['watchdog_task'] = loop.create_task(_soundcloud_batch_watchdog())
    except Exception:
        pass

# Enhance note function with latency & processed counting
# ...existing code...

def note_soundcloud_release_fetch(success: bool, context: str = "", latency_ms: float = None):
    """Record outcome of a single artist release fetch. Adds latency & Tier 3 anomaly heuristics."""
    global CLIENT_ID, key_manager
    try:
        mon = get_release_monitor()
        mon.note(success, context)
        # Latency tracking
        if latency_ms is not None:
            BATCH_STATE['latencies_ms'].append(latency_ms)
        # Count processed when context indicates end of primary release attempt
        if any(tag in context for tag in [':release_checked', ':no_release_info', ':missing_release_date', ':exception']):
            BATCH_STATE['processed'] += 1
            BATCH_STATE['last_progress_time'] = time.time()
        # Rotation based on existing monitor
        if mon.should_rotate() and key_manager:
            logging.warning("⚠️ SoundCloud release fetch failures threshold reached – rotating key (silent limit suspected)")
            key_manager.mark_rate_limited()
            new_key = key_manager.rotate_key(reason="batch_consecutive_failures")
            if new_key:
                CLIENT_ID = new_key
                TELEMETRY['rotations'] += 1
                mon.mark_rotated()
                logging.info("🔄 SoundCloud key rotated due to batch failure heuristic")
            else:
                logging.error("🛑 Unable to rotate SoundCloud key (all exhausted) – activating circuit breaker")
                trip_circuit_breaker(reason="batch_fail_rotation_exhausted")
    except Exception as e:
        logging.debug(f"note_soundcloud_release_fetch error: {e}")


def finalize_soundcloud_release_batch():
    """Finalize batch; evaluate Tier 3 behavioral heuristics (processed mismatch, latency spike)."""
    global CLIENT_ID
    try:
        if not BATCH_STATE['start_time']:
            return
        duration = time.time() - BATCH_STATE['start_time']
        exp = BATCH_STATE['expected_total'] or 0
        proc = BATCH_STATE['processed']
        lat_list = BATCH_STATE['latencies_ms']
        median_lat = statistics.median(lat_list) if lat_list else None
        # Update EMA baseline
        if median_lat is not None:
            base = BATCH_STATE['baseline_latency_ms']
            if base is None:
                BATCH_STATE['baseline_latency_ms'] = median_lat
            else:
                BATCH_STATE['baseline_latency_ms'] = (EMA_ALPHA * median_lat) + (1-EMA_ALPHA) * base
        baseline = BATCH_STATE['baseline_latency_ms']
        # Processed mismatch (less than 50%)
        if exp and proc < exp * 0.5:
            logging.warning(f"📉 SoundCloud batch processed only {proc}/{exp} artists ({proc/exp:.0%}) – potential silent abort")
            if key_manager:
                key_manager.mark_rate_limited()
                new_key = key_manager.rotate_key(reason="processed_mismatch")
                if new_key:
                    CLIENT_ID = new_key
        # Latency spike heuristic
        if baseline and median_lat and median_lat > baseline * 2 and proc < exp:  # spike w/ incomplete batch
            logging.warning(f"🐢 SoundCloud batch latency spike: median {median_lat:.1f}ms > 2x baseline {baseline:.1f}ms with incomplete batch; rotating key")
            if key_manager:
                key_manager.mark_rate_limited()
                new_key = key_manager.rotate_key(reason="latency_spike")
                if new_key:
                    CLIENT_ID = new_key
        logging.info(f"🧾 SoundCloud batch finalize: duration={duration:.1f}s processed={proc}/{exp} median_latency={median_lat:.1f if median_lat else 'n/a'}ms baseline_latency={baseline:.1f if baseline else 'n/a'}ms")
    except Exception as e:
        logging.debug(f"finalize_soundcloud_release_batch error: {e}")
    finally:
        # Cancel watchdog
        task = BATCH_STATE.get('watchdog_task')
        if task and not task.done():
            task.cancel()
        BATCH_STATE['start_time'] = None