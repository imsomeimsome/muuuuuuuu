import sqlite3
from datetime import datetime, timezone, timedelta
from dateutil.parser import parse as parse_datetime
from dateutil.parser import isoparse
from tables import get_connection, DB_PATH
import os

# Ensure all dates stored with timezone info
def normalize_date_str(date_str: str) -> str:
    try:
        dt = isoparse(date_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.isoformat()
    except Exception:
        return date_str

def initialize_database():
    """Check if database exists and is properly initialized."""
    if not os.path.exists(DB_PATH):
        print("⚠️ Database not found. Run 'python tables.py' to initialize!")
        return
    
    # Check if we have the new schema
    try:
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='platform_configs'")
            if not cursor.fetchone():
                print("⚠️ Old database schema detected. Run 'python tables.py' to upgrade!")
                return
            print("✅ Database schema is up to date.")
    except Exception as e:
        print(f"❌ Database check failed: {e}")

# ===== USER FUNCTIONS =====

def is_user_registered(user_id):
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT 1 FROM users WHERE user_id = ?", (str(user_id),))
        return cursor.fetchone() is not None

def add_user(user_id, username):
    now = datetime.now(timezone.utc).isoformat()
    try:
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "INSERT INTO users (user_id, username, registered_at, updated_at) VALUES (?, ?, ?, ?)",
                (str(user_id), username, now, now)
            )
            conn.commit()
            log_activity(user_id, 'register', f'Username: {username}')
            return True
    except sqlite3.IntegrityError:
        return False

def get_username(user_id):
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT username FROM users WHERE user_id = ?", (str(user_id),))
        row = cursor.fetchone()
        return row[0] if row else "Unknown"

def get_user_registered_at(user_id):
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT registered_at FROM users WHERE user_id = ?", (str(user_id),))
        result = cursor.fetchone()
        return result[0] if result else None

# ===== ARTIST FUNCTIONS =====

def add_artist(platform, artist_id, artist_name, artist_url, owner_id, guild_id=None, genres=None, last_release_date=None):
    if guild_id is None:
        raise ValueError("guild_id must be provided when adding an artist")

    now = datetime.now(timezone.utc).isoformat()
    
    if last_release_date is None:
        last_release_date = now
    else:
        last_release_date = normalize_date_str(last_release_date)
    
    if isinstance(genres, list):
        genres = ",".join(genres) if genres else ""

    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            INSERT OR REPLACE INTO artists
            (platform, artist_id, artist_name, artist_url, owner_id, guild_id, 
             genres, last_release_date, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (platform, artist_id, artist_name, artist_url, owner_id, guild_id, 
              genres, last_release_date, now, now))
        conn.commit()
        
        log_activity(owner_id, 'track', f'Added {artist_name} ({platform})', guild_id)

def remove_artist(artist_id, owner_id):
    with get_connection() as conn:
        cursor = conn.cursor()
        # Get artist name for logging
        cursor.execute("SELECT artist_name, platform FROM artists WHERE artist_id = ? AND owner_id = ?", (artist_id, owner_id))
        result = cursor.fetchone()
        
        cursor.execute("DELETE FROM artists WHERE artist_id = ? AND owner_id = ?", (artist_id, owner_id))
        conn.commit()
        
        if result:
            log_activity(owner_id, 'untrack', f'Removed {result[0]} ({result[1]})')

def artist_exists(platform, artist_id, owner_id):
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT 1 FROM artists WHERE platform = ? AND artist_id = ? AND owner_id = ?
        ''', (platform, artist_id, owner_id))
        return cursor.fetchone() is not None

# Add this function to database_utils.py
def update_artist_last_like_date_to_now(artist_id, guild_id):
    """Set last_like_date to now to prevent posting old content."""
    now = datetime.now(timezone.utc).isoformat()
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE artists
            SET last_like_date = ?, updated_at = ?
            WHERE artist_id = ? AND guild_id = ?
        """, (now, now, artist_id, guild_id))
        conn.commit()
        print(f"✅ Set last_like_date to {now} for artist {artist_id}")

def reset_like_tracking_for_all():
    """Reset all like tracking to current time to prevent old content flood."""
    now = datetime.now(timezone.utc).isoformat()
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE artists
            SET last_like_date = ?, updated_at = ?
            WHERE platform = 'soundcloud'
        """, (now, now))
        affected = cursor.rowcount
        conn.commit()
        print(f"✅ Reset like tracking for {affected} SoundCloud artists")
        return affected
    
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
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT platform, artist_id, artist_name, artist_url, last_release_date, 
                   owner_id, genres, guild_id 
            FROM artists WHERE owner_id = ?
        """, (owner_id,))
        
        columns = ['platform', 'artist_id', 'artist_name', 'artist_url', 'last_release_date', 
                   'owner_id', 'genres', 'guild_id']
        rows = cursor.fetchall()
        return [dict(zip(columns, row)) for row in rows]

def get_artist_by_id(artist_id, owner_id, guild_id):
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT platform, artist_id, artist_name, artist_url, 
                   last_release_date, owner_id, genres, guild_id
            FROM artists
            WHERE artist_id = ? AND owner_id = ? AND guild_id = ?
        ''', (artist_id, owner_id, guild_id))
        row = cursor.fetchone()
        if row:
            columns = ['platform', 'artist_id', 'artist_name', 'artist_url', 
                      'last_release_date', 'owner_id', 'genres', 'guild_id']
            return dict(zip(columns, row))
        return None

def get_artist_full_record(artist_id, owner_id):
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT platform, artist_id, artist_name, artist_url, 
                   last_release_date, owner_id, genres, guild_id
            FROM artists 
            WHERE artist_id = ? AND owner_id = ?
        ''', (artist_id, owner_id))
        row = cursor.fetchone()
        if row:
            columns = ['platform', 'artist_id', 'artist_name', 'artist_url', 
                      'last_release_date', 'owner_id', 'genres', 'guild_id']
            return dict(zip(columns, row))
        return None

def update_last_release_date(artist_id, owner_id, guild_id, new_date):
    now = datetime.now(timezone.utc).isoformat()
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            UPDATE artists
            SET last_release_date = ?, updated_at = ?
            WHERE artist_id = ? AND owner_id = ? AND guild_id = ?
        ''', (normalize_date_str(new_date), now, artist_id, owner_id, guild_id))
        conn.commit()

def update_last_like_date(artist_id, guild_id, new_date):
    now = datetime.now(timezone.utc).isoformat()
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE artists
            SET last_like_date = ?, updated_at = ?
            WHERE artist_id = ? AND guild_id = ?
        """, (new_date, now, artist_id, guild_id))
        conn.commit()

def get_global_artist_count():
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(DISTINCT artist_id) FROM artists")
        return cursor.fetchone()[0]

# ===== CHANNEL CONFIG =====

def set_channel(guild_id, platform, channel_id):
    now = datetime.now(timezone.utc).isoformat()
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "INSERT OR REPLACE INTO channels (guild_id, platform, channel_id, created_at) VALUES (?, ?, ?, ?)",
            (str(guild_id), platform, str(channel_id), now)
        )
        conn.commit()

def get_channel(guild_id, platform):
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT channel_id FROM channels WHERE guild_id = ? AND platform = ?",
            (str(guild_id), platform)
        )
        result = cursor.fetchone()
        return int(result[0]) if result else None

# ===== POSTED CONTENT TRACKING =====

def is_already_posted_like(artist_id, guild_id, like_id):
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT 1 FROM posted_content
            WHERE artist_id = ? AND guild_id = ? AND content_type = 'like' AND content_id = ?
        ''', (artist_id, guild_id, like_id))
        return cursor.fetchone() is not None

def mark_posted_like(artist_id, guild_id, like_id):
    now = datetime.now(timezone.utc).isoformat()
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            INSERT OR IGNORE INTO posted_content 
            (artist_id, guild_id, platform, content_type, content_id, posted_at)
            VALUES (?, ?, 'soundcloud', 'like', ?, ?)
        ''', (artist_id, guild_id, like_id, now))
        conn.commit()

def is_already_posted_repost(artist_id: str, guild_id: str, repost_id: str) -> bool:
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT 1 FROM posted_content 
            WHERE artist_id=? AND guild_id=? AND content_type='repost' AND content_id=?
        """, (artist_id, guild_id, repost_id))
        return cursor.fetchone() is not None

def mark_posted_repost(artist_id: str, guild_id: str, repost_id: str):
    now = datetime.now(timezone.utc).isoformat()
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT OR IGNORE INTO posted_content 
            (artist_id, guild_id, platform, content_type, content_id, posted_at)
            VALUES (?, ?, 'soundcloud', 'repost', ?, ?)
        """, (artist_id, guild_id, repost_id, now))
        conn.commit()

# ===== RELEASE STATS =====

def get_release_stats(user_id=None):
    with get_connection() as conn:
        cursor = conn.cursor()
        if user_id:
            cursor.execute("SELECT release_type, COUNT(*) FROM releases WHERE user_id = ? GROUP BY release_type", (str(user_id),))
        else:
            cursor.execute("SELECT release_type, COUNT(*) FROM releases GROUP BY release_type")
        
        stats = {"albums": 0, "eps": 0, "singles": 0, "deluxes": 0, "total": 0}
        rows = cursor.fetchall()
        for release_type, count in rows:
            if release_type == "album":
                stats["albums"] += count
            elif release_type == "ep":
                stats["eps"] += count
            elif release_type == "single":
                stats["singles"] += count
            stats["total"] += count
        return stats

def add_release(user_id, artist_id, release_type, release_date):
    now = datetime.now(timezone.utc).isoformat()
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO releases (user_id, artist_id, platform, release_type, posted_at, guild_id) VALUES (?, ?, ?, ?, ?, ?)",
            (str(user_id), artist_id, 'unknown', release_type, now, 'unknown')
        )
        conn.commit()

# ===== ACTIVITY LOGGING =====

def log_activity(user_id, action, details=None, guild_id=None):
    """Log user activity."""
    now = datetime.now(timezone.utc).isoformat()
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO activity_logs (user_id, action, details, timestamp, guild_id) VALUES (?, ?, ?, ?, ?)",
            (str(user_id), action, details, now, guild_id)
        )
        conn.commit()

def get_untrack_count(user_id):
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM activity_logs WHERE user_id = ? AND action = 'untrack'", (str(user_id),))
        return cursor.fetchone()[0]

# ===== PLATFORM MANAGEMENT =====

def get_enabled_platforms():
    """Get list of enabled platforms."""
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT platform FROM platform_configs WHERE enabled = 1")
        return [row[0] for row in cursor.fetchall()]

def get_platform_config(platform):
    """Get configuration for a specific platform."""
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM platform_configs WHERE platform = ?", (platform,))
        row = cursor.fetchone()
        if row:
            columns = ['platform', 'enabled', 'api_endpoint', 'rate_limit', 'check_interval', 
                       'supports_likes', 'supports_reposts', 'supports_playlists', 'created_at', 'updated_at']
            return dict(zip(columns, row))
        return None

# ===== LEGACY COMPATIBILITY FUNCTIONS =====

def log_untrack(user_id, artist_id):
    log_activity(user_id, 'untrack', f'artist_id: {artist_id}')

def set_release_prefs(user_id, artist_id, release_type, state):
    # TODO: Implement with new schema
    pass

def get_release_prefs(user_id, artist_id):
    # TODO: Implement with new schema
    return None

def import_artists_from_json(data, owner_id, guild_id):
    # TODO: Implement with new schema
    return 0

def get_artist_by_identifier(identifier: str, owner_id: str):
    """Get artist by URL or ID, handling both Spotify and SoundCloud."""
    from spotify_utils import extract_spotify_id
    from soundcloud_utils import extract_soundcloud_id
    
    # Try to extract ID from URL
    if "spotify.com" in identifier:
        artist_id = extract_spotify_id(identifier)
    elif "soundcloud.com" in identifier:
        artist_id = extract_soundcloud_id(identifier)
    else:
        # Assume it's already an ID
        artist_id = identifier
    
    return get_artist_full_record(artist_id, owner_id)

# Add to database_utils.py
def reset_old_like_dates():
    """Reset very old like dates to 1 week ago."""
    one_week_ago = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()
    
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE artists 
            SET last_like_date = ? 
            WHERE platform = 'soundcloud' 
            AND (last_like_date IS NULL OR last_like_date < '2024-01-01')
        """, (one_week_ago,))
        affected = cursor.rowcount
        conn.commit()
        print(f"✅ Reset like tracking for {affected} artists to 1 week ago")
        return affected
    
def update_last_repost_date(artist_id, guild_id, new_date):
    now = datetime.now(timezone.utc).isoformat()
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE artists
            SET last_repost_date = ?, updated_at = ?
            WHERE artist_id = ? AND guild_id = ?
        """, (new_date, now, artist_id, guild_id))
        conn.commit()

# Add to database_utils.py and run once
def reset_activity_tracking():
    """Reset like and repost tracking to 1 hour ago to catch recent activity."""
    one_hour_ago = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
    
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE artists
            SET last_like_date = ?, last_repost_date = ?, updated_at = ?
            WHERE platform = 'soundcloud'
        """, (one_hour_ago, one_hour_ago, one_hour_ago))
        affected = cursor.rowcount
        conn.commit()
        print(f"✅ Reset activity tracking for {affected} artists to 1 hour ago")
        return affected

# Add to database_utils.py

def record_bot_shutdown():
    """Record when the bot goes down."""
    now = datetime.now(timezone.utc).isoformat()
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO activity_logs (user_id, action, details, timestamp, guild_id)
            VALUES ('system', 'bot_shutdown', ?, ?, NULL)
        """, (f"Bot shutdown at {now}", now))
        conn.commit()

def record_bot_startup():
    """Record when the bot starts up and return last shutdown time."""
    now = datetime.now(timezone.utc).isoformat()
    
    with get_connection() as conn:
        cursor = conn.cursor()
        
        # Get last shutdown time
        cursor.execute("""
            SELECT timestamp FROM activity_logs 
            WHERE user_id = 'system' AND action = 'bot_shutdown'
            ORDER BY timestamp DESC LIMIT 1
        """)
        result = cursor.fetchone()
        last_shutdown = result[0] if result else None
        
        # Record startup
        cursor.execute("""
            INSERT INTO activity_logs (user_id, action, details, timestamp, guild_id)
            VALUES ('system', 'bot_startup', ?, ?, NULL)
        """, (f"Bot started at {now}", now))
        conn.commit()
        
        return last_shutdown

def get_downtime_duration():
    """Get how long the bot was down."""
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT 
                (SELECT timestamp FROM activity_logs WHERE user_id = 'system' AND action = 'bot_shutdown' ORDER BY timestamp DESC LIMIT 1) as last_shutdown,
                (SELECT timestamp FROM activity_logs WHERE user_id = 'system' AND action = 'bot_startup' ORDER BY timestamp DESC LIMIT 1) as last_startup
        """)
        result = cursor.fetchone()
        
        if result and result[0] and result[1]:
            shutdown_time = parse_datetime(result[0])
            startup_time = parse_datetime(result[1])
            if shutdown_time and startup_time:
                return startup_time - shutdown_time
        return None
    
def update_last_playlist_date(artist_id, guild_id, new_date):
    """Update the last playlist date for an artist."""
    now = datetime.now(timezone.utc).isoformat()
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE artists
            SET last_playlist_date = ?, updated_at = ?
            WHERE artist_id = ? AND guild_id = ?
        """, (new_date, now, artist_id, guild_id))
        conn.commit()