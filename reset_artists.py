import sqlite3

def reset_tables():
    conn = sqlite3.connect("artists.db")
    c = conn.cursor()
    try:
        print("Resetting 'artists' table...")
        c.execute("DELETE FROM artists")
        print("✅ Cleared 'artists' table.")

        print("Resetting 'reposts' table...")
        c.execute("DELETE FROM reposts")
        print("✅ Cleared 'reposts' table.")

        print("Resetting 'likes' table...")
        c.execute("DELETE FROM likes")
        print("✅ Cleared 'likes' table.")
    except Exception as e:
        print(f"⚠️ Error while resetting tables: {e}")
    finally:
        conn.commit()
        conn.close()

if __name__ == "__main__":
    reset_tables()
