import psycopg2

import os
from dotenv import load_dotenv

# Load env variables
load_dotenv()

# Get values from .env
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

# Optional URL format
# DB_URL = "postgresql://postgres:passwaord@localhost:5432/demodb"

try:
    # 🔹 Connect to PostgreSQL database
    connection = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )

    cursor = connection.cursor()
    print("✅ PostgreSQL Connection Successful!")

    # 🔹 Execute a test query
    cursor.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';"
    )

    tables = cursor.fetchall()

    print("📊 Tables in the database:")
    for table in tables:
        print(table[0])

    # 🔹 Close connection
    cursor.close()
    connection.close()
    print("🔒 Connection Closed.")

except Exception as e:
    print(f"❌ Error connecting to PostgreSQL: {e}")
