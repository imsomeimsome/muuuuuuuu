import os, sqlite3
from tables import drop_all_tables, create_all_tables, DB_PATH

def reset_tables():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    drop_all_tables()
    create_all_tables()
    print("✅ Database tables reset.")

if __name__ == "__main__":
    reset_tables()
