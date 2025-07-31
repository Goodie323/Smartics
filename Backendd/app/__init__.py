# Backend/init_db.py

from app.database.connection import create_table_if_not_exists

if __name__ == "__main__":
    create_table_if_not_exists()
    print("✅ Database initialized successfully.")
