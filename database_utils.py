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
            UNIQUE(platform, artist_id)
        )
    """)
    conn.commit()
    conn.close()

def add_artist(platform, artist_id):
    """
    Add an artist to the database.
    :param platform: Platform name (e.g., 'spotify', 'soundcloud')
    :param artist_id: Artist ID
    :return: True if successful, False otherwise
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("INSERT INTO artists (platform, artist_id) VALUES (?, ?)", (platform, artist_id))
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