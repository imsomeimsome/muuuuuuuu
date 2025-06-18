import sqlite3
import os

DB_PATH = "data.db"  # Adjust if your actual DB file is named differently

def reset_tables():
    if not os.path.exists(DB_PATH):
        print(f"❌ Database file '{DB_PATH}' does not exist.")
        return

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    tables = ["artists", "reposts", "likes"]

    try:
        for table in tables:
            print(f"🔁 Resetting '{table}' table...")

            # Check if table exists
            c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,))
            if not c.fetchone():
                print(f"⚠️ Table '{table}' does not exist — skipping.")
                continue

            # Delete all records
            c.execute(f"DELETE FROM {table}")
            print(f"✅ Cleared all records from '{table}'.")

            # Reset auto-increment (optional, if there's an integer PK)
            c.execute(f"DELETE FROM sqlite_sequence WHERE name='{table}'")

    except Exception as e:
        print(f"❌ Error during reset: {e}")
    finally:
        conn.commit()
        conn.close()
        print("✅ All done.")

if __name__ == "__main__":
    reset_tables()
