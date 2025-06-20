import sqlite3
from datetime import datetime, timezone
from dateutil.parser import isoparse
from spotify_utils import extract_spotify_id
from soundcloud_utils import extract_soundcloud_id

# --- Connection Helper ---
def get_connection():
    return sqlite3.connect("/data/artists.db")

# Ensure all dates stored with timezone info
def normalize_date_str(date_str: str) -> str:
    try:
        dt = isoparse(date_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.isoformat()
    except Exception:
        return date_str

# --- Table Initialization ---
def initialize_database():
    with get_connection() as conn:
        c = conn.cursor()
        c.execute('''
            CREATE TABLE IF NOT EXISTS artists (
                platform TEXT,
                artist_id TEXT,
                artist_name TEXT,
                artist_url TEXT,
                owner_id TEXT,
                guild_id TEXT,
                genres TEXT,
                last_release_date TEXT,
                PRIMARY KEY (artist_id, owner_id, guild_id)
            )
        ''')

        c.execute('''
            CREATE TABLE IF NOT EXISTS posted_likes (
                artist_id TEXT,
                guild_id TEXT,
                like_id TEXT,
                PRIMARY KEY (artist_id, guild_id, like_id)
            )
        ''')

        c.execute('''
            CREATE TABLE IF NOT EXISTS posted_reposts (
                artist_id TEXT,
                guild_id TEXT,
                repost_id TEXT,
                PRIMARY KEY (artist_id, guild_id, repost_id)
            )
        ''')

        conn.commit()

        # --- ADD THIS BLOCK: check if tracked_users exists, and add if not ---
    c.execute("PRAGMA table_info(artists);")
    columns = [col[1] for col in c.fetchall()]
    if 'tracked_users' not in columns:
            c.execute("ALTER TABLE artists ADD COLUMN tracked_users TEXT DEFAULT '';")

    
    # Release preferences table
    c.execute('''
        CREATE TABLE IF NOT EXISTS release_prefs (
            user_id TEXT,
            artist_id TEXT,
            albums BOOLEAN DEFAULT 1,
            singles BOOLEAN DEFAULT 1,
            eps BOOLEAN DEFAULT 1,
            reposts BOOLEAN DEFAULT 0,
            PRIMARY KEY (user_id, artist_id)
        )
    ''')

    # Channel configuration
    c.execute('''
        CREATE TABLE IF NOT EXISTS channels (
            guild_id TEXT,
            type TEXT CHECK(type IN ('spotify', 'soundcloud', 'logs', 'commands')),
            channel_id TEXT,
            PRIMARY KEY (guild_id, type)
        )
    ''')

    # Releases table
    c.execute('''
        CREATE TABLE IF NOT EXISTS releases (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT,
            artist_id TEXT,
            release_type TEXT,
            release_date TEXT
        )
    ''')

    # Users table
    c.execute('''
        CREATE TABLE IF NOT EXISTS users (
            user_id TEXT PRIMARY KEY,
            username TEXT,
            registered_at TEXT
        )
    ''')

    # Untrack logs
    c.execute('''
        CREATE TABLE IF NOT EXISTS untrack_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT,
            artist_id TEXT,
            timestamp TEXT
        )
    ''')


    conn.commit()
    conn.close()

def ensure_artists_table_has_unique_constraint():
    conn = get_connection()
    c = conn.cursor()

    c.execute("PRAGMA index_list(artists);")
    indexes = c.fetchall()
    has_unique = any("platform" in str(index) and "artist_id" in str(index) and "owner_id" in str(index) for index in indexes)

    if not has_unique:
        print("⚠️ Migrating 'artists' table to include UNIQUE(platform, artist_id, owner_id)...")
        c.execute('''
            CREATE TABLE IF NOT EXISTS artists_new (
                platform TEXT,
                artist_id TEXT,
                artist_name TEXT,
                artist_url TEXT,
                last_release_date TEXT,
                owner_id TEXT,
                tracked_users TEXT,
                genres TEXT,
                guild_id TEXT,
                UNIQUE(platform, artist_id, owner_id)
            );
        ''')

        c.execute('''
            INSERT INTO artists_new
            SELECT * FROM artists;
        ''')

        c.execute('DROP TABLE artists;')
        c.execute('ALTER TABLE artists_new RENAME TO artists;')

        conn.commit()
        print("✅ 'artists' table migrated successfully.")
    else:
        print("✅ 'artists' table already has UNIQUE constraint.")
    conn.close()

def initialize_posted_likes_table():
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS posted_likes (
                artist_id TEXT,
                guild_id TEXT,
                like_id TEXT,
                PRIMARY KEY (artist_id, guild_id, like_id)
            )
        ''')
        conn.commit()

# --- Artist Functions ---
def get_all_artists(guild_id=None):
    with get_connection() as conn:
        cursor = conn.cursor()
        if guild_id:
            cursor.execute("SELECT * FROM artists WHERE guild_id = ?", (guild_id,))
        else:
            cursor.execute("SELECT * FROM artists")

        columns = [column[0] for column in cursor.description]
        rows = cursor.fetchall()
        return [dict(zip(columns, row)) for row in rows]

def get_artists_by_owner(owner_id):
    conn = get_connection()
    c = conn.cursor()
    c.execute("SELECT * FROM artists WHERE owner_id = ?", (owner_id,))
    rows = c.fetchall()
    conn.close()
    return [{
        "platform": row[0],
        "artist_id": row[1],
        "artist_name": row[2],
        "artist_url": row[3],
        "last_release_date": row[4],
        "owner_id": row[5],
        "genres": row[6]
    } for row in rows]

def get_artist_by_id(artist_id, owner_id, guild_id):
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT * FROM artists
            WHERE artist_id = ? AND owner_id = ? AND guild_id = ?
        ''', (artist_id, owner_id, guild_id))
        return cursor.fetchone()

def get_artist_url(artist_id, owner_id):
    conn = get_connection()
    c = conn.cursor()
    c.execute("SELECT artist_url FROM artists WHERE artist_id = ? AND owner_id = ?", (artist_id, owner_id))
    row = c.fetchone()
    conn.close()
    if row:
        return row[0]
    else:
        return None

def update_last_release_date(artist_id, owner_id, guild_id, new_date):
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            UPDATE artists
            SET last_release_date = ?
            WHERE artist_id = ? AND owner_id = ? AND guild_id = ?
        ''', (normalize_date_str(new_date), artist_id, owner_id, guild_id))
        conn.commit()

def add_artist(platform, artist_id, artist_name, artist_url, owner_id, guild_id=None, genres=None, last_release_date=None):
    if guild_id is None:
        raise ValueError("guild_id must be provided when adding an artist")

    # Use timezone-aware datetime to avoid false release triggers
    if last_release_date is None:
        last_release_date = datetime.now(timezone.utc).isoformat()
    else:
        last_release_date = normalize_date_str(last_release_date)
    # sqlite doesn't support list objects directly; store genres as a comma
    # separated string for consistency
    if isinstance(genres, list):
        genres = ",".join(genres) if genres else None

    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            INSERT OR REPLACE INTO artists
            (platform, artist_id, artist_name, artist_url, owner_id, guild_id, genres, last_release_date)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (platform, artist_id, artist_name, artist_url, owner_id, guild_id, genres, last_release_date))
        conn.commit()

def remove_artist(artist_id, owner_id):
    conn = get_connection()
    c = conn.cursor()
    c.execute("DELETE FROM artists WHERE artist_id = ? AND owner_id = ?", (artist_id, owner_id))
    conn.commit()
    conn.close()

def artist_exists(platform, artist_id, owner_id):
    conn = get_connection()
    c = conn.cursor()
    c.execute('''
        SELECT 1 FROM artists WHERE platform = ? AND artist_id = ? AND owner_id = ?
    ''', (platform, artist_id, owner_id))
    result = c.fetchone()
    conn.close()
    return result is not None

def get_artist_full_record(artist_id, owner_id):
    conn = get_connection()
    c = conn.cursor()
    c.execute("SELECT * FROM artists WHERE artist_id = ? AND owner_id = ?", (artist_id, owner_id))
    row = c.fetchone()
    conn.close()
    if row:
        return {
            "platform": row[0],
            "artist_id": row[1],
            "artist_name": row[2],
            "artist_url": row[3],
            "last_release_date": row[4],
            "owner_id": row[5],
            "genres": row[6]
        }
    else:
        return None

# --- User Functions ---
def get_user_registered_at(user_id):
    conn = get_connection()
    c = conn.cursor()
    c.execute("SELECT registered_at FROM users WHERE user_id = ?", (str(user_id),))
    result = c.fetchone()
    conn.close()
    return result[0] if result else None

def get_global_artist_count():
    conn = get_connection()
    c = conn.cursor()
    c.execute("SELECT COUNT(DISTINCT artist_id) FROM artists")
    count = c.fetchone()[0]
    conn.close()
    return count

def is_user_registered(user_id):
    conn = get_connection()
    c = conn.cursor()
    c.execute("SELECT 1 FROM users WHERE user_id = ?", (str(user_id),))
    exists = c.fetchone() is not None
    conn.close()
    return exists

def add_user(user_id, username):
    conn = get_connection()
    c = conn.cursor()
    registered_at = datetime.now(timezone.utc).isoformat()
    try:
        c.execute(
            "INSERT INTO users (user_id, username, registered_at) VALUES (?, ?, ?)",
            (str(user_id), username, registered_at)
        )
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False
    finally:
        conn.close()

def get_username(user_id):
    conn = get_connection()
    c = conn.cursor()
    c.execute("SELECT username FROM users WHERE user_id = ?", (str(user_id),))
    row = c.fetchone()
    conn.close()
    return row[0] if row else "Unknown"

def get_user(user_id):
    conn = get_connection()
    c = conn.cursor()
    c.execute("SELECT * FROM users WHERE user_id = ?", (str(user_id),))
    row = c.fetchone()
    conn.close()
    if row:
        return {
            "user_id": row[0],
            "username": row[1],
            "registered_at": row[2]
        }
    return None

# --- Release Functions ---
def add_release(user_id, artist_id, release_type, release_date):
    conn = get_connection()
    c = conn.cursor()
    c.execute(
        "INSERT INTO releases (user_id, artist_id, release_type, release_date) VALUES (?, ?, ?, ?)",
        (str(user_id), artist_id, release_type, release_date)
    )
    conn.commit()
    conn.close()

def get_release_stats(user_id=None):
    conn = get_connection()
    c = conn.cursor()
    if user_id:
        c.execute("SELECT release_type, COUNT(*) FROM releases WHERE user_id = ? GROUP BY release_type", (str(user_id),))
    else:
        c.execute("SELECT release_type, COUNT(*) FROM releases GROUP BY release_type")
    stats = { "albums": 0, "eps": 0, "singles": 0, "deluxes": 0, "total": 0 }
    rows = c.fetchall()
    for release_type, count in rows:
        if release_type == "album":
            stats["albums"] += count
        elif release_type == "ep":
            stats["eps"] += count
        elif release_type == "single":
            stats["singles"] += count
        elif release_type == "deluxe":
            stats["deluxes"] += count
        stats["total"] += count
    conn.close()
    return stats

# --- Untrack Logs ---
def log_untrack(user_id, artist_id):
    conn = get_connection()
    c = conn.cursor()
    timestamp = datetime.now(timezone.utc).isoformat()
    c.execute(
        "INSERT INTO untrack_logs (user_id, artist_id, timestamp) VALUES (?, ?, ?)",
        (str(user_id), artist_id, timestamp)
    )
    conn.commit()
    conn.close()

def get_untrack_count(user_id):
    conn = get_connection()
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM untrack_logs WHERE user_id = ?", (str(user_id),))
    count = c.fetchone()[0]
    conn.close()
    return count

# --- Channel Config ---
def set_channel(guild_id, type, channel_id):
    conn = get_connection()
    c = conn.cursor()
    c.execute(
        "INSERT OR REPLACE INTO channels (guild_id, type, channel_id) VALUES (?, ?, ?)",
        (str(guild_id), type, str(channel_id))
    )
    conn.commit()
    conn.close()

def get_channel(guild_id, type):
    conn = get_connection()
    c = conn.cursor()
    c.execute(
        "SELECT channel_id FROM channels WHERE guild_id = ? AND type = ?",
        (str(guild_id), type)
    )
    result = c.fetchone()
    conn.close()
    return int(result[0]) if result else None

# --- Release Preferences ---
def set_release_prefs(user_id, artist_id, release_type, state):
    conn = get_connection()
    c = conn.cursor()
    c.execute(
        f'''INSERT OR REPLACE INTO release_prefs 
        (user_id, artist_id, {release_type})
        VALUES (?, ?, ?)''',
        (str(user_id), artist_id, 1 if state == "on" else 0)
    )
    conn.commit()
    conn.close()

def get_artist_by_identifier(identifier: str, owner_id: str):
    """Get artist by URL or ID, handling both Spotify and SoundCloud."""
    # Try to extract ID from URL
    if "spotify.com" in identifier:
        artist_id = extract_spotify_id(identifier)
    elif "soundcloud.com" in identifier:
        artist_id = extract_soundcloud_id(identifier)
    else:
        # Assume it's already an ID
        artist_id = identifier
    
    return get_artist_by_id(artist_id, owner_id)

def get_release_prefs(user_id, artist_id):
    conn = get_connection()
    c = conn.cursor()
    c.execute('SELECT * FROM release_prefs WHERE user_id = ? AND artist_id = ?', 
             (str(user_id), artist_id))
    row = c.fetchone()
    conn.close()
    return {
        'albums': bool(row[2]),
        'singles': bool(row[3]),
        'eps': bool(row[4]),
        'reposts': bool(row[5])
    } if row else None

def cleanup_duplicate_artists():
    conn = get_connection()
    c = conn.cursor()
    print("🧹 Cleaning up duplicate artists...")

    c.execute('''
        DELETE FROM artists
        WHERE rowid NOT IN (
            SELECT MIN(rowid)
            FROM artists
            GROUP BY platform, artist_id, owner_id
        )
    ''')

    conn.commit()
    conn.close()
    print("✅ Removed duplicate artist entries.")

import datetime

def import_artists_from_json(data, owner_id, guild_id):
    conn = get_connection()
    c = conn.cursor()
    imported = 0

    for entry in data:
        platform = entry.get("platform")
        artist_id = entry.get("artist_id")
        artist_name = entry.get("artist_name")
        artist_url = entry.get("artist_url")
        genres = entry.get("genres", [])
        genre_str = ",".join(genres) if genres else None

        now_timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()

        c.execute(
            '''
            INSERT OR IGNORE INTO artists
            (platform, artist_id, artist_name, artist_url, last_release_date, owner_id, tracked_users, genres, guild_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (platform, artist_id, artist_name, artist_url, now_timestamp, owner_id, '', genre_str, guild_id)
        )
        imported += 1

    conn.commit()
    conn.close()
    return imported

def is_already_posted_like(artist_id, guild_id, like_id):
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT 1 FROM posted_likes
            WHERE artist_id = ? AND guild_id = ? AND like_id = ?
        ''', (artist_id, guild_id, like_id))
        return cursor.fetchone() is not None

def mark_posted_like(artist_id, guild_id, like_id):
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            INSERT OR IGNORE INTO posted_likes (artist_id, guild_id, like_id)
            VALUES (?, ?, ?)
        ''', (artist_id, guild_id, like_id))
        conn.commit()

def mark_posted_repost(artist_id: str, guild_id: str, repost_id: str):
    conn = get_connection()
    c = conn.cursor()
    c.execute("""
        INSERT OR IGNORE INTO reposts (artist_id, guild_id, repost_id) VALUES (?, ?, ?)
    """, (artist_id, guild_id, repost_id))
    conn.commit()
    conn.close()

def is_already_posted_repost(artist_id: str, guild_id: str, repost_id: str) -> bool:
    conn = get_connection()
    c = conn.cursor()
    c.execute("""
        SELECT 1 FROM reposts WHERE artist_id=? AND guild_id=? AND repost_id=?
    """, (artist_id, guild_id, repost_id))
    result = c.fetchone()
    conn.close()
    return result is not None

# --- Initialize DB on module import ---
initialize_database()
