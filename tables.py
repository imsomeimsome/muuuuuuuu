import sqlite3
import os
from datetime import datetime, timezone

DB_PATH = "/data/artists.db"

def get_connection():
    """Get database connection."""
    return sqlite3.connect(DB_PATH)

def drop_all_tables():
    """Drop all existing tables for clean slate."""
    print("🗑️ Dropping all existing tables...")
    
    tables_to_drop = [
        'artists', 'users', 'channels', 'releases', 'posted_content',
        'release_preferences', 'activity_logs', 'platform_configs',
        'posted_likes', 'reposts', 'release_prefs', 'untrack_logs'
    ]
    
    with get_connection() as conn:
        cursor = conn.cursor()
        for table in tables_to_drop:
            try:
                cursor.execute(f"DROP TABLE IF EXISTS {table}")
                print(f"✅ Dropped table: {table}")
            except Exception as e:
                print(f"⚠️ Could not drop {table}: {e}")
        conn.commit()

def create_all_tables():
    """Create all database tables with proper schema."""
    print("🏗️ Creating all database tables...")
    
    with get_connection() as conn:
        cursor = conn.cursor()
        
        # ===== CORE TABLES =====
        
        # Users table
        cursor.execute('''
            CREATE TABLE users (
                user_id TEXT PRIMARY KEY,
                username TEXT NOT NULL,
                registered_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
        ''')
        print("✅ Created table: users")
        
        # Artists table - expandable for new platforms
        cursor.execute('''
            CREATE TABLE artists (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                platform TEXT NOT NULL CHECK(platform IN ('spotify', 'soundcloud', 'youtube', 'bandcamp', 'apple_music')),
                artist_id TEXT NOT NULL,
                artist_name TEXT NOT NULL,
                artist_url TEXT NOT NULL,
                owner_id TEXT NOT NULL,
                guild_id TEXT NOT NULL,
                genres TEXT DEFAULT '',
                last_release_date TEXT,
                last_like_date TEXT,
                last_repost_date TEXT,
                last_playlist_date TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                UNIQUE(platform, artist_id, owner_id, guild_id),
                FOREIGN KEY (owner_id) REFERENCES users(user_id)
            )
        ''')
        print("✅ Created table: artists")
        
        # Channel configuration - expandable for new platforms
        cursor.execute('''
            CREATE TABLE channels (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                guild_id TEXT NOT NULL,
                platform TEXT NOT NULL CHECK(platform IN ('spotify', 'soundcloud', 'youtube', 'bandcamp', 'apple_music', 'logs', 'commands')),
                channel_id TEXT NOT NULL,
                created_at TEXT NOT NULL,
                UNIQUE(guild_id, platform)
            )
        ''')
        print("✅ Created table: channels")
        
        # ===== TRACKING TABLES =====
        
        # Posted content tracking (prevents duplicates)
        cursor.execute('''
            CREATE TABLE posted_content (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                artist_id TEXT NOT NULL,
                guild_id TEXT NOT NULL,
                platform TEXT NOT NULL,
                content_type TEXT NOT NULL CHECK(content_type IN ('release', 'like', 'repost', 'playlist')),
                content_id TEXT NOT NULL,
                posted_at TEXT NOT NULL,
                UNIQUE(artist_id, guild_id, platform, content_type, content_id)
            )
        ''')
        print("✅ Created table: posted_content")
        
        # Release tracking
        cursor.execute('''
            CREATE TABLE releases (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT NOT NULL,
                artist_id TEXT NOT NULL,
                platform TEXT NOT NULL,
                release_id TEXT,
                release_type TEXT NOT NULL CHECK(release_type IN ('album', 'single', 'ep', 'playlist', 'like', 'repost')),
                release_title TEXT,
                release_url TEXT,
                posted_at TEXT NOT NULL,
                guild_id TEXT NOT NULL,
                FOREIGN KEY (user_id) REFERENCES users(user_id)
            )
        ''')
        print("✅ Created table: releases")
        
        # Release preferences
        cursor.execute('''
            CREATE TABLE release_preferences (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT NOT NULL,
                artist_id TEXT NOT NULL,
                platform TEXT NOT NULL,
                albums BOOLEAN DEFAULT 1,
                singles BOOLEAN DEFAULT 1,
                eps BOOLEAN DEFAULT 1,
                playlists BOOLEAN DEFAULT 1,
                likes BOOLEAN DEFAULT 1,
                reposts BOOLEAN DEFAULT 0,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                UNIQUE(user_id, artist_id, platform),
                FOREIGN KEY (user_id) REFERENCES users(user_id)
            )
        ''')
        print("✅ Created table: release_preferences")
        
        # ===== LOGGING TABLES =====
        
        # Activity logs
        cursor.execute('''
            CREATE TABLE activity_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT NOT NULL,
                action TEXT NOT NULL CHECK(action IN ('track', 'untrack', 'register', 'channel_set', 'import', 'export', 'bot_startup', 'bot_shutdown')),
                details TEXT,
                timestamp TEXT NOT NULL,
                guild_id TEXT,
                FOREIGN KEY (user_id) REFERENCES users(user_id)
            )
        ''')
        print("✅ Created table: activity_logs")
        
        # ===== PLATFORM CONFIG =====
        
        # Platform configurations (for future expansion)
        cursor.execute('''
            CREATE TABLE platform_configs (
                platform TEXT PRIMARY KEY CHECK(platform IN ('spotify', 'soundcloud', 'youtube', 'bandcamp', 'apple_music')),
                enabled BOOLEAN DEFAULT 1,
                api_endpoint TEXT,
                rate_limit INTEGER DEFAULT 60,
                check_interval INTEGER DEFAULT 300,
                supports_likes BOOLEAN DEFAULT 0,
                supports_reposts BOOLEAN DEFAULT 0,
                supports_playlists BOOLEAN DEFAULT 1,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
        ''')
        print("✅ Created table: platform_configs")
        
        # ===== INDEXES FOR PERFORMANCE =====
        
        # Artists indexes
        cursor.execute('CREATE INDEX idx_artists_owner_guild ON artists(owner_id, guild_id)')
        cursor.execute('CREATE INDEX idx_artists_platform ON artists(platform)')
        cursor.execute('CREATE INDEX idx_artists_lookup ON artists(platform, artist_id)')
        
        # Posted content indexes
        cursor.execute('CREATE INDEX idx_posted_content_lookup ON posted_content(artist_id, guild_id, platform, content_type)')
        cursor.execute('CREATE INDEX idx_posted_content_unique ON posted_content(content_type, content_id)')
        
        # Releases indexes
        cursor.execute('CREATE INDEX idx_releases_user ON releases(user_id)')
        cursor.execute('CREATE INDEX idx_releases_artist ON releases(artist_id)')
        cursor.execute('CREATE INDEX idx_releases_date ON releases(posted_at)')
        
        # Activity logs indexes
        cursor.execute('CREATE INDEX idx_activity_logs_user ON activity_logs(user_id)')
        cursor.execute('CREATE INDEX idx_activity_logs_action ON activity_logs(action)')
        cursor.execute('CREATE INDEX idx_activity_logs_timestamp ON activity_logs(timestamp)')
        
        print("✅ Created all performance indexes")
        
        conn.commit()

def populate_default_data():
    """Insert default platform configurations."""
    print("📝 Populating default data...")
    
    now = datetime.now(timezone.utc).isoformat()
    
    platforms = [
        ('spotify', 1, 'https://api.spotify.com/v1', 100, 300, 0, 0, 1),
        ('soundcloud', 1, 'https://api-v2.soundcloud.com', 60, 300, 1, 1, 1),
        ('youtube', 0, 'https://www.googleapis.com/youtube/v3', 100, 300, 1, 0, 1),
        ('bandcamp', 0, '', 30, 600, 0, 0, 1),
        ('apple_music', 0, 'https://api.music.apple.com/v1', 100, 300, 0, 0, 1)
    ]
    
    with get_connection() as conn:
        cursor = conn.cursor()
        for platform_data in platforms:
            cursor.execute('''
                INSERT INTO platform_configs 
                (platform, enabled, api_endpoint, rate_limit, check_interval, supports_likes, supports_reposts, supports_playlists, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', platform_data + (now, now))
        
        conn.commit()
        print(f"✅ Added {len(platforms)} platform configurations")

def initialize_fresh_database():
    """Complete database initialization - drops everything and recreates."""
    print("🚀 Initializing fresh database...")
    
    # Ensure data directory exists
    os.makedirs('/data', exist_ok=True)
    
    # Drop all existing tables
    drop_all_tables()
    
    # Create all tables
    create_all_tables()
    
    # Populate default data
    populate_default_data()
    
    print("🎉 Fresh database initialization complete!")

def get_table_info():
    """Get information about all tables for debugging."""
    with get_connection() as conn:
        cursor = conn.cursor()
        
        # Get all table names
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()
        
        print("📊 Database Table Information:")
        print("=" * 50)
        
        for (table_name,) in tables:
            print(f"\n🗂️ Table: {table_name}")
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = cursor.fetchall()
            
            for col in columns:
                col_id, name, data_type, not_null, default, pk = col
                pk_str = " (PK)" if pk else ""
                null_str = " NOT NULL" if not_null else ""
                default_str = f" DEFAULT {default}" if default else ""
                print(f"  - {name}: {data_type}{pk_str}{null_str}{default_str}")
        
        print("\n" + "=" * 50)

# Add this to tables.py and run it once
def reset_like_tracking():
    """Reset like tracking to current time to prevent posting old content."""
    from database_utils import reset_like_tracking_for_all
    reset_like_tracking_for_all()

if __name__ == "__main__":
    # Run this to initialize a fresh database
    initialize_fresh_database()
    
    # Reset like tracking to prevent old content flood
    reset_like_tracking()
    
    get_table_info()

if __name__ == "__main__":
    # Run this to initialize a fresh database
    initialize_fresh_database()
    get_table_info()