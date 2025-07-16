import sqlite3

DB_PATH = "artists.db"

def initialize_database():
    """
    Initialize the database and create tables if they don't exist.
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS artists (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            platform TEXT NOT NULL,
            artist_id TEXT NOT NULL,
            artist_name TEXT NOT NULL,
            artist_url TEXT NOT NULL,
            owner_id INTEGER NOT NULL,
            guild_id TEXT,
            last_release_date TEXT NOT NULL,
            UNIQUE(platform, artist_id, owner_id)
        )
    """)
    conn.commit()
    conn.close()

def add_artist(platform, artist_id, artist_name, artist_url, owner_id, guild_id, last_release_date):
    """
    Add an artist to the database.
    :param platform: Platform name (e.g., 'spotify', 'soundcloud').
    :param artist_id: Artist ID.
    :param artist_name: Artist name.
    :param artist_url: Artist URL.
    :param owner_id: ID of the user tracking the artist.
    :param guild_id: ID of the guild where the artist is tracked.
    :param last_release_date: Last release date of the artist.
    :return: True if successful, False otherwise.
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO artists (platform, artist_id, artist_name, artist_url, owner_id, guild_id, last_release_date)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (platform, artist_id, artist_name, artist_url, owner_id, guild_id, last_release_date))
        conn.commit()
        conn.close()
        return True
    except sqlite3.IntegrityError:
        return False

def reset_artists():
    """
    Clear all tracked artists from the database.
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM artists")
    conn.commit()
    conn.close()

def artist_exists(platform, artist_id, owner_id):
    """
    Check if an artist is already tracked in the database.
    :param platform: Platform name (e.g., 'spotify', 'soundcloud').
    :param artist_id: Artist ID.
    :param owner_id: ID of the user tracking the artist.
    :return: True if the artist is already tracked, False otherwise.
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM artists WHERE platform = ? AND artist_id = ? AND owner_id = ?", (platform, artist_id, owner_id))
    count = cursor.fetchone()[0]
    conn.close()
    return count > 0