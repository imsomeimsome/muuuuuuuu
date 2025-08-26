import os, sqlite3, logging, json
from datetime import datetime, timezone, timedelta
from dateutil.parser import isoparse, parse as parse_datetime
from tables import get_connection, DB_PATH

# ---------- Helpers ----------

def normalize_date_str(date_str: str) -> str:
    """Ensure stored dates are ISO8601 with timezone (UTC)."""
    if not date_str:
        return None
    try:
        dt = isoparse(date_str)
    except Exception:
        try:
            dt = parse_datetime(date_str)
        except Exception:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat()

# ---------- Initialization ----------

def initialize_database():
    if not os.path.exists(os.path.dirname(DB_PATH)):
        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    # Tables created in tables.py; ensure file exists
    open(DB_PATH, 'a').close()

# ---------- User Functions ----------

def is_user_registered(user_id):
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM users WHERE user_id=?", (str(user_id),))
        return cur.fetchone() is not None

def add_user(user_id, username):
    now = datetime.now(timezone.utc).isoformat()
    try:
        with get_connection() as conn:
            conn.execute("REPLACE INTO users(user_id,username,registered_at) VALUES (?,?,?)", (str(user_id), username, now))
        return True
    except sqlite3.Error as e:
        logging.error(f"add_user failed: {e}")
        return False

def get_username(user_id):
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT username FROM users WHERE user_id=?", (str(user_id),))
        row = cur.fetchone()
        return row[0] if row else None

def get_user_registered_at(user_id):
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT registered_at FROM users WHERE user_id=?", (str(user_id),))
        row = cur.fetchone()
        return row[0] if row else None

# ---------- Artist Functions ----------

# Cache schema introspection for performance
_ARTISTS_HAS_CREATED_AT = None

def _artists_has_created_at():
    global _ARTISTS_HAS_CREATED_AT
    if _ARTISTS_HAS_CREATED_AT is not None:
        return _ARTISTS_HAS_CREATED_AT
    try:
        with get_connection() as conn:
            cur = conn.cursor()
            cur.execute("PRAGMA table_info(artists)")
            cols = [r[1] for r in cur.fetchall()]
            _ARTISTS_HAS_CREATED_AT = 'created_at' in cols
    except Exception:
        _ARTISTS_HAS_CREATED_AT = False
    return _ARTISTS_HAS_CREATED_AT

def add_artist(platform, artist_id, artist_name, artist_url, owner_id, guild_id=None, genres=None, last_release_date=None):
    """Insert/replace an artist row. Compatible with schemas with/without created_at column."""
    created_at_needed = _artists_has_created_at()
    cols = [
        'platform','artist_id','artist_name','artist_url','owner_id','guild_id','genres','last_release_date'
    ]
    vals = [
        platform, artist_id, artist_name, artist_url, str(owner_id), str(guild_id) if guild_id else None, json.dumps(genres or []), normalize_date_str(last_release_date)
    ]
    if created_at_needed:
        cols.append('created_at')
        vals.append(datetime.now(timezone.utc).isoformat())
    placeholders = ','.join(['?']*len(cols))
    col_list = ','.join(cols)
    sql = f"REPLACE INTO artists({col_list}) VALUES ({placeholders})"
    try:
        with get_connection() as conn:
            conn.execute(sql, vals)
    except sqlite3.IntegrityError as e:
        # Fallback: if created_at missing value assumption wrong, retry without
        if 'created_at' in str(e) and created_at_needed:
            logging.warning("Retrying add_artist without created_at column (schema mismatch)")
            try:
                with get_connection() as conn:
                    conn.execute(
                        "REPLACE INTO artists(platform,artist_id,artist_name,artist_url,owner_id,guild_id,genres,last_release_date) VALUES (?,?,?,?,?,?,?,?)",
                        (platform, artist_id, artist_name, artist_url, str(owner_id), str(guild_id) if guild_id else None, json.dumps(genres or []), normalize_date_str(last_release_date))
                    )
            except Exception as e2:
                logging.error(f"add_artist failed fallback: {e2}")
                raise
        else:
            logging.error(f"add_artist integrity error: {e}")
            raise
    except Exception as e:
        logging.error(f"add_artist failed: {e}")
        raise

def remove_artist(artist_id, owner_id):
    with get_connection() as conn:
        conn.execute("DELETE FROM artists WHERE artist_id=? AND owner_id=?", (artist_id, str(owner_id)))


def artist_exists(platform, artist_id, owner_id):
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM artists WHERE platform=? AND artist_id=? AND owner_id=?", (platform, artist_id, str(owner_id)))
        return cur.fetchone() is not None

# Like tracking helpers

def update_artist_last_like_date_to_now(artist_id, guild_id):
    update_last_like_date(artist_id, guild_id, datetime.now(timezone.utc).isoformat())


def reset_like_tracking_for_all():
    with get_connection() as conn:
        conn.execute("UPDATE artists SET last_like_date=NULL")


def get_all_artists(guild_id=None):
    with get_connection() as conn:
        cur = conn.cursor()
        if guild_id:
            cur.execute("SELECT platform, artist_id, artist_name, artist_url, owner_id, guild_id, genres, last_release_date, last_like_date, last_repost_date, last_playlist_date FROM artists WHERE guild_id=?", (str(guild_id),))
        else:
            cur.execute("SELECT platform, artist_id, artist_name, artist_url, owner_id, guild_id, genres, last_release_date, last_like_date, last_repost_date, last_playlist_date FROM artists")
        rows = cur.fetchall()
        cols = [c[0] for c in cur.description]
        result = []
        for r in rows:
            d = dict(zip(cols, r))
            if d.get('genres'):
                try:
                    d['genres'] = json.loads(d['genres'])
                except Exception:
                    d['genres'] = []
            result.append(d)
        return result


def get_artists_by_owner(owner_id):
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT platform, artist_id, artist_name, artist_url, owner_id, guild_id, genres, last_release_date FROM artists WHERE owner_id=?", (str(owner_id),))
        rows = cur.fetchall()
        cols = [c[0] for c in cur.description]
        out = []
        for r in rows:
            d = dict(zip(cols, r))
            if d.get('genres'):
                try: d['genres'] = json.loads(d['genres'])
                except: d['genres'] = []
            out.append(d)
        return out


def get_artist_by_id(artist_id, owner_id, guild_id):
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT * FROM artists WHERE artist_id=? AND owner_id=? AND guild_id=?", (artist_id, str(owner_id), str(guild_id)))
        row = cur.fetchone()
        if not row:
            return None
        cols = [c[0] for c in cur.description]
        d = dict(zip(cols, row))
        if d.get('genres'):
            try: d['genres'] = json.loads(d['genres'])
            except: d['genres'] = []
        return d


def get_artist_full_record(artist_id, owner_id):
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT * FROM artists WHERE artist_id=? AND owner_id=?", (artist_id, str(owner_id)))
        row = cur.fetchone()
        if not row: return None
        cols = [c[0] for c in cur.description]
        d = dict(zip(cols, row))
        if d.get('genres'):
            try: d['genres'] = json.loads(d['genres'])
            except: d['genres'] = []
        return d

# Date updates

def update_last_release_date(artist_id, owner_id, guild_id, new_date):
    with get_connection() as conn:
        conn.execute("UPDATE artists SET last_release_date=? WHERE artist_id=? AND owner_id=? AND guild_id=?", (normalize_date_str(new_date), artist_id, str(owner_id), str(guild_id)))


def update_last_like_date(artist_id, guild_id, new_date):
    with get_connection() as conn:
        conn.execute("UPDATE artists SET last_like_date=? WHERE artist_id=? AND guild_id=?", (normalize_date_str(new_date), artist_id, str(guild_id)))


def update_last_repost_date(artist_id, guild_id, new_date):
    with get_connection() as conn:
        conn.execute("UPDATE artists SET last_repost_date=? WHERE artist_id=? AND guild_id=?", (normalize_date_str(new_date), artist_id, str(guild_id)))


def update_last_playlist_date(artist_id, guild_id, new_date):
    with get_connection() as conn:
        conn.execute("UPDATE artists SET last_playlist_date=? WHERE artist_id=? AND guild_id=?", (normalize_date_str(new_date), artist_id, str(guild_id)))

# Counts

def get_global_artist_count():
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM artists")
        return cur.fetchone()[0]

# ---------- Channel Config ----------

def set_channel(guild_id, platform, channel_id):
    with get_connection() as conn:
        conn.execute("REPLACE INTO channels(guild_id, platform, channel_id) VALUES (?,?,?)", (str(guild_id), platform, str(channel_id)))


def get_channel(guild_id, platform):
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT channel_id FROM channels WHERE guild_id=? AND platform=?", (str(guild_id), platform))
        row = cur.fetchone()
        return row[0] if row else None

# ---------- Posted Content Tracking ----------

def is_already_posted_like(artist_id, guild_id, like_id):
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM posted_likes WHERE artist_id=? AND guild_id=? AND like_id=?", (artist_id, str(guild_id), like_id))
        return cur.fetchone() is not None

def mark_posted_like(artist_id, guild_id, like_id):
    with get_connection() as conn:
        conn.execute("REPLACE INTO posted_likes(artist_id, guild_id, like_id) VALUES (?,?,?)", (artist_id, str(guild_id), like_id))


def is_already_posted_repost(artist_id: str, guild_id: str, repost_id: str) -> bool:
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM posted_reposts WHERE artist_id=? AND guild_id=? AND repost_id=?", (artist_id, str(guild_id), repost_id))
        return cur.fetchone() is not None

def mark_posted_repost(artist_id: str, guild_id: str, repost_id: str):
    with get_connection() as conn:
        conn.execute("REPLACE INTO posted_reposts(artist_id, guild_id, repost_id) VALUES (?,?,?)", (artist_id, str(guild_id), repost_id))


def is_already_posted_playlist(artist_id, guild_id, playlist_id):
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM posted_playlists WHERE artist_id=? AND guild_id=? AND playlist_id=?", (artist_id, str(guild_id), playlist_id))
        return cur.fetchone() is not None

def mark_posted_playlist(artist_id, guild_id, playlist_id):
    with get_connection() as conn:
        conn.execute("REPLACE INTO posted_playlists(artist_id, guild_id, playlist_id) VALUES (?,?,?)", (artist_id, str(guild_id), playlist_id))


def store_playlist_state(artist_id, guild_id, playlist_id, tracks):
    with get_connection() as conn:
        conn.execute("REPLACE INTO playlist_states(artist_id, guild_id, playlist_id, tracks) VALUES (?,?,?,?)", (artist_id, str(guild_id), playlist_id, json.dumps(tracks)))


def get_playlist_state(artist_id, guild_id, playlist_id):
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT tracks FROM playlist_states WHERE artist_id=? AND guild_id=? AND playlist_id=?", (artist_id, str(guild_id), playlist_id))
        row = cur.fetchone()
        if not row: return None
        try: return json.loads(row[0])
        except: return None

# ---------- Release Stats ----------

def get_release_stats(user_id=None):
    with get_connection() as conn:
        cur = conn.cursor()
        if user_id:
            cur.execute("SELECT release_type, COUNT(*) FROM release_stats WHERE user_id=? GROUP BY release_type", (str(user_id),))
        else:
            cur.execute("SELECT release_type, COUNT(*) FROM release_stats GROUP BY release_type")
        rows = cur.fetchall()
        stats = {"albums":0,"eps":0,"singles":0,"deluxes":0}
        total = 0
        for rtype, cnt in rows:
            total += cnt
            key = rtype.lower()
            if key.startswith('album'): stats['albums'] += cnt
            elif key.startswith('ep'): stats['eps'] += cnt
            elif key.startswith('deluxe'): stats['deluxes'] += cnt
            else: stats['singles'] += cnt
        stats['total'] = total
        return stats

def add_release(user_id, artist_id, release_type, release_date):
    with get_connection() as conn:
        conn.execute("INSERT INTO release_stats(user_id, artist_id, release_type, release_date) VALUES (?,?,?,?)", (str(user_id), artist_id, release_type, normalize_date_str(release_date)))

# ---------- Activity Logging ----------

def log_activity(user_id, action, details=None, guild_id=None):
    with get_connection() as conn:
        conn.execute("INSERT INTO activity_logs(user_id, action, timestamp, details) VALUES (?,?,?,?)", (str(user_id), action, datetime.now(timezone.utc).isoformat(), json.dumps({"details":details, "guild_id":guild_id})))


def get_untrack_count(user_id):
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM activity_logs WHERE user_id=? AND action='untrack'", (str(user_id),))
        return cur.fetchone()[0]

# ---------- Platform Management (stubs for extensibility) ----------

def get_enabled_platforms():
    return ["spotify", "soundcloud"]

def get_platform_config(platform):
    return {}

# ---------- Legacy / Compatibility ----------

def log_untrack(user_id, artist_id):
    log_activity(user_id, 'untrack', artist_id)


def set_release_prefs(user_id, artist_id, release_type, state):
    # Could be stored in a future table; for now log
    log_activity(user_id, 'pref_change', {"artist":artist_id, "release_type":release_type, "state":state})


def get_release_prefs(user_id, artist_id):
    return {}


def import_artists_from_json(data, owner_id, guild_id):
    for entry in data:
        try:
            add_artist(entry['platform'], entry['artist_id'], entry.get('artist_name','Unknown'), entry.get('artist_url',''), owner_id, guild_id, entry.get('genres'), entry.get('last_release_date'))
        except Exception as e:
            logging.error(f"Failed importing artist {entry}: {e}")


def get_artist_by_identifier(identifier: str, owner_id: str):
    # Try direct ID
    rec = get_artist_full_record(identifier, owner_id)
    if rec: return rec
    # Try by name (case-insensitive)
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT * FROM artists WHERE LOWER(artist_name)=LOWER(?) AND owner_id=?", (identifier, str(owner_id)))
        row = cur.fetchone()
        if not row: return None
        cols = [c[0] for c in cur.description]
        d = dict(zip(cols,row))
        try: d['genres'] = json.loads(d['genres']) if d.get('genres') else []
        except: d['genres'] = []
        return d

# ---------- Maintenance Helpers ----------

def reset_old_like_dates():
    reset_like_tracking_for_all()


def reset_activity_tracking():
    with get_connection() as conn:
        conn.execute("DELETE FROM activity_logs")

# ---------- Lifecycle Records ----------

def record_bot_shutdown():
    log_activity('system', 'bot_shutdown')


def record_bot_startup():
    # Return last shutdown timestamp for catch-up logic
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT timestamp FROM activity_logs WHERE action='bot_shutdown' ORDER BY timestamp DESC LIMIT 1")
        row = cur.fetchone()
    log_activity('system', 'bot_startup')
    return row[0] if row else None


def get_downtime_duration():
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT timestamp FROM activity_logs WHERE action='bot_shutdown' ORDER BY timestamp DESC LIMIT 1")
        shut = cur.fetchone()
        cur.execute("SELECT timestamp FROM activity_logs WHERE action='bot_startup' ORDER BY timestamp DESC LIMIT 1")
        start = cur.fetchone()
    if not shut or not start:
        return None
    try:
        shut_dt = isoparse(shut[0])
        start_dt = isoparse(start[0])
        return start_dt - shut_dt
    except Exception:
        return None

# ---------- API Key State Persistence ----------

def save_api_key_state(platform: str, keys_state: list):
    """Persist API key manager state to DB.
    keys_state: list of dicts with keys: index, key, fail_count, cooldown_until, active
    Only stores a prefix of key for identification (not full secret)."""
    try:
        with get_connection() as conn:
            cur = conn.cursor()
            # Defensive: ensure table exists (early calls before create_all_tables)
            cur.execute("""CREATE TABLE IF NOT EXISTS api_keys (
                platform TEXT,
                key_index INTEGER,
                key_prefix TEXT,
                fail_count INTEGER,
                cooldown_until TEXT,
                active INTEGER,
                PRIMARY KEY(platform, key_index)
            )""")
            # Clear existing rows for platform
            cur.execute("DELETE FROM api_keys WHERE platform=?", (platform,))
            for st in keys_state:
                cur.execute(
                    "REPLACE INTO api_keys(platform, key_index, key_prefix, fail_count, cooldown_until, active) VALUES (?,?,?,?,?,?)",
                    (
                        platform,
                        st.get('index'),
                        (st.get('key') or '')[:12],
                        st.get('fail_count', 0),
                        st.get('cooldown_until'),
                        1 if st.get('active') else 0
                    )
                )
            conn.commit()
    except Exception as e:
        logging.error(f"Failed saving api key state for {platform}: {e}")


def load_api_key_state(platform: str):
    """Load persisted API key state. Returns dict index->row dict."""
    out = {}
    try:
        with get_connection() as conn:
            cur = conn.cursor()
            # Defensive: ensure table exists
            cur.execute("""CREATE TABLE IF NOT EXISTS api_keys (
                platform TEXT,
                key_index INTEGER,
                key_prefix TEXT,
                fail_count INTEGER,
                cooldown_until TEXT,
                active INTEGER,
                PRIMARY KEY(platform, key_index)
            )""")
            cur.execute("SELECT key_index, key_prefix, fail_count, cooldown_until, active FROM api_keys WHERE platform=?", (platform,))
            rows = cur.fetchall()
            for row in rows:
                idx, pref, fail_count, cooldown_until, active = row
                out[idx] = {
                    'key_prefix': pref,
                    'fail_count': fail_count or 0,
                    'cooldown_until': cooldown_until,
                    'active': bool(active)
                }
    except Exception as e:
        logging.error(f"Failed loading api key state for {platform}: {e}")
    return out

# ---------- Guild Feature Toggles ----------

def set_guild_feature(guild_id: str, feature: str, enabled: bool):
    """Enable/disable a feature (likes/reposts/playlists) for a guild."""
    feature = feature.lower()
    with get_connection() as conn:
        conn.execute("REPLACE INTO guild_features(guild_id, feature, enabled) VALUES (?,?,?)", (str(guild_id), feature, 1 if enabled else 0))


def is_feature_enabled(guild_id: str, feature: str) -> bool:
    """Return whether a feature is enabled for a guild. Defaults to True if unset."""
    feature = feature.lower()
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT enabled FROM guild_features WHERE guild_id=? AND feature=?", (str(guild_id), feature))
        row = cur.fetchone()
        if row is None:
            return True  # default enabled
        return bool(row[0])


def get_guild_features(guild_id: str):
    """Return dict of feature->enabled for a guild (defaults assumed True if missing)."""
    features = {"likes": True, "reposts": True, "playlists": True}
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT feature, enabled FROM guild_features WHERE guild_id=?", (str(guild_id),))
        for f, en in cur.fetchall():
            features[f] = bool(en)
    return features

# ---------- API Key Rotations ----------

def log_api_key_rotation(platform: str, old_index: int, new_index: int, reason: str, exhausted: bool = False):
    """Persist an API key rotation event."""
    try:
        with get_connection() as conn:
            conn.execute(
                "INSERT INTO api_key_rotations(platform, old_index, new_index, reason, exhausted, timestamp) VALUES (?,?,?,?,?,?)",
                (platform, old_index, new_index, reason, 1 if exhausted else 0, datetime.now(timezone.utc).isoformat())
            )
    except Exception as e:
        logging.error(f"Failed logging api key rotation: {e}")


def get_recent_api_key_rotations(platform: str, limit: int = 10):
    """Fetch recent API key rotation rows for telemetry."""
    try:
        with get_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                "SELECT platform, old_index, new_index, reason, exhausted, timestamp FROM api_key_rotations WHERE platform=? ORDER BY timestamp DESC LIMIT ?",
                (platform, limit)
            )
            rows = cur.fetchall()
            cols = [c[0] for c in cur.description]
            return [dict(zip(cols, r)) for r in rows]
    except Exception as e:
        logging.error(f"Failed fetching api key rotations: {e}")
        return []

# ---------- Release Check Timestamp (Spotify duplicate suppression) ----------

def _ensure_last_release_check_column():
    try:
        with get_connection() as conn:
            cur = conn.cursor()
            cur.execute("PRAGMA table_info(artists)")
            cols = [r[1] for r in cur.fetchall()]
            if 'last_release_check' not in cols:
                cur.execute("ALTER TABLE artists ADD COLUMN last_release_check TEXT")
                conn.commit()
    except Exception as e:
        logging.error(f"Failed ensuring last_release_check column: {e}")


def get_last_release_check(artist_id: str, owner_id: str, guild_id: str):
    _ensure_last_release_check_column()
    try:
        with get_connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT last_release_check FROM artists WHERE artist_id=? AND owner_id=? AND guild_id=?", (artist_id, str(owner_id), str(guild_id)))
            row = cur.fetchone()
            return row[0] if row and row[0] else None
    except Exception as e:
        logging.error(f"Failed fetching last_release_check for {artist_id}: {e}")
        return None


def update_last_release_check(artist_id: str, owner_id: str, guild_id: str, ts_iso: str):
    _ensure_last_release_check_column()
    try:
        with get_connection() as conn:
            conn.execute("UPDATE artists SET last_release_check=? WHERE artist_id=? AND owner_id=? AND guild_id=?", (ts_iso, artist_id, str(owner_id), str(guild_id)))
    except Exception as e:
        logging.error(f"Failed updating last_release_check for {artist_id}: {e}")