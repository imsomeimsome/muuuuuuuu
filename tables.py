from datetime import datetime, timezone
import sqlite3, os, json
import logging

DB_PATH = "/data/artists.db"

# Ensure directory exists
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

def get_connection():
    return sqlite3.connect(DB_PATH)

# Core schema definition (idempotent)
TABLE_DEFS = [
    ("users", """CREATE TABLE IF NOT EXISTS users (
        user_id TEXT PRIMARY KEY,
        username TEXT,
        registered_at TEXT
    )"""),
    ("artists", """CREATE TABLE IF NOT EXISTS artists (
        platform TEXT,
        artist_id TEXT,
        artist_name TEXT,
        artist_url TEXT,
        owner_id TEXT,
        guild_id TEXT,
        genres TEXT,
        last_release_date TEXT,
        last_like_date TEXT,
        last_repost_date TEXT,
        last_playlist_date TEXT,
        PRIMARY KEY(platform, artist_id, owner_id, COALESCE(guild_id,''))
    )"""),
    ("channels", """CREATE TABLE IF NOT EXISTS channels (
        guild_id TEXT,
        platform TEXT,
        channel_id TEXT,
        PRIMARY KEY(guild_id, platform)
    )"""),
    ("posted_likes", """CREATE TABLE IF NOT EXISTS posted_likes (
        artist_id TEXT,
        guild_id TEXT,
        like_id TEXT,
        PRIMARY KEY(artist_id, guild_id, like_id)
    )"""),
    ("posted_reposts", """CREATE TABLE IF NOT EXISTS posted_reposts (
        artist_id TEXT,
        guild_id TEXT,
        repost_id TEXT,
        PRIMARY KEY(artist_id, guild_id, repost_id)
    )"""),
    ("posted_playlists", """CREATE TABLE IF NOT EXISTS posted_playlists (
        artist_id TEXT,
        guild_id TEXT,
        playlist_id TEXT,
        PRIMARY KEY(artist_id, guild_id, playlist_id)
    )"""),
    ("playlist_states", """CREATE TABLE IF NOT EXISTS playlist_states (
        artist_id TEXT,
        guild_id TEXT,
        playlist_id TEXT,
        tracks TEXT,
        PRIMARY KEY(artist_id, guild_id, playlist_id)
    )"""),
    ("release_stats", """CREATE TABLE IF NOT EXISTS release_stats (
        user_id TEXT,
        artist_id TEXT,
        release_type TEXT,
        release_date TEXT
    )"""),
    ("activity_logs", """CREATE TABLE IF NOT EXISTS activity_logs (
        user_id TEXT,
        action TEXT,
        timestamp TEXT,
        details TEXT
    )"""),
    ("cache", """CREATE TABLE IF NOT EXISTS cache (
        key TEXT PRIMARY KEY,
        value TEXT,
        expires_at TEXT
    )"""),
    ("api_keys", """CREATE TABLE IF NOT EXISTS api_keys (
        platform TEXT,
        key_index INTEGER,
        key_prefix TEXT,
        fail_count INTEGER,
        cooldown_until TEXT,
        active INTEGER,
        PRIMARY KEY(platform, key_index)
    )"""),
    ("guild_features", """CREATE TABLE IF NOT EXISTS guild_features (
        guild_id TEXT,
        feature TEXT,
        enabled INTEGER,
        PRIMARY KEY(guild_id, feature)
    )"""),
    ("api_key_rotations", """CREATE TABLE IF NOT EXISTS api_key_rotations (
        platform TEXT,
        old_index INTEGER,
        new_index INTEGER,
        reason TEXT,
        exhausted INTEGER,
        timestamp TEXT
    )"""),
]

def drop_all_tables():
    with get_connection() as conn:
        cur = conn.cursor()
        for name, _ in TABLE_DEFS:
            cur.execute(f"DROP TABLE IF EXISTS {name}")
        conn.commit()

def create_all_tables():
    with get_connection() as conn:
        cur = conn.cursor()
        for _, ddl in TABLE_DEFS:
            cur.execute(ddl)
        # Add index for cache pruning performance
        try:
            cur.execute("CREATE INDEX IF NOT EXISTS idx_cache_expires_at ON cache(expires_at)")
        except Exception:
            pass
        conn.commit()
    _ensure_channels_created_at()

def _ensure_channels_created_at():
    try:
        conn = sqlite3.connect('/data/bot.db')
        cur = conn.cursor()
        # Check if created_at exists
        cur.execute("PRAGMA table_info(channels)")
        cols = [row[1] for row in cur.fetchall()]
        if 'created_at' not in cols:
            logging.info("🛠 Adding 'created_at' to channels table")
            cur.execute("ALTER TABLE channels ADD COLUMN created_at TEXT")
            conn.commit()
    except Exception as e:
        logging.error(f"❌ Failed ensuring channels.created_at: {e}")
    finally:
        try:
            conn.close()
        except Exception:
            pass

# Fresh init utility

def initialize_fresh_database():
    drop_all_tables()
    create_all_tables()

# Cache table (already created but helper used elsewhere)

def initialize_cache_table():
    with get_connection() as conn:
        conn.execute("CREATE TABLE IF NOT EXISTS cache (key TEXT PRIMARY KEY, value TEXT, expires_at TEXT)")
        conn.commit()

# No-op populate placeholder

def populate_default_data():
    pass

if __name__ == "__main__":
    # Run this to initialize a fresh database
    initialize_fresh_database()
    populate_default_data()