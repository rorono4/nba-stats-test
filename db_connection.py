import psycopg2 # type: ignore
import os
from dotenv import load_dotenv

# Carica le variabili d'ambiente dal file .env
load_dotenv()

# Funzione per connettersi al database
def connect_db():
    try:
        conn = psycopg2.connect(
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT")
        )
        print("✅ Connessione a PostgreSQL riuscita!")
        return conn
    except Exception as e:
        print("❌ Errore nella connessione al database:", e)
        return None

# Test di connessione
if __name__ == "__main__":
    conn = connect_db()
    if conn:
        conn.close()
        print("🔌 Connessione chiusa correttamente.")
