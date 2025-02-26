import psycopg2 # type: ignore
import os
import time
from dotenv import load_dotenv
from nba_api.stats.endpoints import leaguedashteamstats

# Carica le variabili d'ambiente
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
        return conn
    except Exception as e:
        print("❌ Errore nella connessione al database:", e)
        return None

# Funzione per inserire i dati NBA nel database
def insert_nba_data():
    conn = connect_db()
    if conn is None:
        return
    
    cursor = conn.cursor()

    # Ottenere i dati della NBA
    stats = leaguedashteamstats.LeagueDashTeamStats(season="2023-24", per_mode_detailed="PerGame")
    df = stats.get_data_frames()[0]

    for _, row in df.iterrows():
        cursor.execute("""
            INSERT INTO nba_team_stats (
                team_name, points, rebounds, offensive_rebounds, defensive_rebounds, assists,
                field_goals_made, field_goals_attempted, field_goal_percentage,
                three_points_made, three_points_attempted, three_point_percentage,
                free_throws_made, free_throws_attempted, free_throw_percentage
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                row["TEAM_NAME"], row["PTS"], row["REB"], row["OREB"], row["DREB"], row["AST"],
                row["FGM"], row["FGA"], row["FG_PCT"],
                row["FG3M"], row["FG3A"], row["FG3_PCT"],
                row["FTM"], row["FTA"], row["FT_PCT"]
            )
        )

    conn.commit()
    cursor.close()
    conn.close()
    print("✅ Dati NBA inseriti con successo nel database!")

# Eseguire l'inserimento
if __name__ == "__main__":
    insert_nba_data()
