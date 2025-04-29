import sqlite3
from datetime import datetime
from spotify_utils import extract_spotify_id
from soundcloud_utils import extract_soundcloud_id

# --- Connection Helper ---
def get_connection():
    return sqlite3.connect("/data/artists.db")

# --- Table Initialization ---
def initialize_database():
    conn = get_connection()
    c = conn.cursor()

    # Main artists table with genres
    c.execute('''
CREATE TABLE IF NOT EXISTS artists (
            platform TEXT,
            artist_id TEXT,
            artist_name TEXT,
            artist_url TEXT,
            last_release_date TEXT,
            owner_id TEXT,
            tracked_users TEXT DEFAULT '',
            genres TEXT,
            PRIMARY KEY (artist_id, owner_id)

        )
    ''')

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

# --- Artist Functions ---
def get_all_artists():
    conn = get_connection()
    c = conn.cursor()
    c.execute("SELECT * FROM artists")
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

def get_artist_by_id(artist_id, owner_id):
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

def update_last_release_date(artist_id, owner_id, new_date):
    conn = get_connection()
    c = conn.cursor()
    c.execute(
        "UPDATE artists SET last_release_date = ? WHERE artist_id = ? AND owner_id = ?",
        (new_date, artist_id, owner_id)
    )
    conn.commit()
    conn.close()

def add_artist(platform, artist_id, artist_name, artist_url, owner_id, guild_id=None, genres=None):
    conn = get_connection()
    c = conn.cursor()
    c.execute(
        '''INSERT OR REPLACE INTO artists 
        (platform, artist_id, artist_name, artist_url, last_release_date, owner_id, tracked_users, genres, guild_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''',
        (platform, artist_id, artist_name, artist_url, None, owner_id, '', ",".join(genres) if genres else None, guild_id)
    )
    conn.commit()
    conn.close()


def remove_artist(artist_id, owner_id):
    conn = get_connection()
    c = conn.cursor()
    c.execute("DELETE FROM artists WHERE artist_id = ? AND owner_id = ?", (artist_id, owner_id))
    conn.commit()
    conn.close()

def artist_exists(artist_id, owner_id):
    conn = get_connection()
    c = conn.cursor()
    c.execute("SELECT 1 FROM artists WHERE artist_id = ? AND owner_id = ?", (artist_id, owner_id))
    exists = c.fetchone() is not None
    conn.close()
    return exists

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
    registered_at = datetime.utcnow().isoformat()
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
    timestamp = datetime.utcnow().isoformat()
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




# --- Initialize DB on module import ---
initialize_database()
