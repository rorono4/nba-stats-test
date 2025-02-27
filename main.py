from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import psycopg2
import json
import os

app = FastAPI()

# ✅ Abilitare CORS per permettere al frontend di accedere all'API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Permette richieste da qualsiasi dominio (puoi specificare solo "http://localhost:5173" se preferisci)
    allow_credentials=True,
    allow_methods=["*"],  # Permette tutti i metodi (GET, POST, PUT, DELETE)
    allow_headers=["*"],  # Permette tutti gli headers
)


def get_db_connection():
    return psycopg2.connect(os.environ["DATABASE_URL"])


# # Connessione al database
# def get_db_connection():
#     return psycopg2.connect(
#         dbname="sport_stats",
#         user="postgres",
#         password="Bracalozz0!",  # Sostituisci con la tua password
#         host="localhost",
#         port="5432"
#     )

# Endpoint base
@app.get("/")
def home():
    return {"message": "API NBA attiva!"}

# Endpoint per ottenere tutti i team NBA con gestione degli errori
@app.get("/teams")
def get_teams():
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT team_name FROM nba_team_stats ORDER BY team_name;")
        teams = [row[0] for row in cur.fetchall()]
        cur.close()
        conn.close()
        return {"teams": teams}
    except Exception as e:
        return {"error": str(e)}  # 👈 Stampa l'errore come risposta JSON


# Endpoint per ottenere i dati di una squadra specifica
@app.get("/teams/{team_name}")
def get_team_stats(team_name: str):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM nba_team_stats WHERE team_name = %s;", (team_name,))
    team_data = cur.fetchone()
    cur.close()
    conn.close()

    if team_data:
        columns = ["id", "team_name", "points", "rebounds", "offensive_rebounds", "defensive_rebounds",
           "assists", "field_goals_made", "field_goals_attempted", "field_goal_percentage",
           "three_points_made", "three_points_attempted", "three_point_percentage",
           "free_throws_made", "free_throws_attempted", "free_throw_percentage",
           "team_rank_points", "team_rank_rebounds", "team_rank_off_rebounds", "team_rank_def_rebounds",
           "team_rank_assists", "team_rank_field_goals_made", "team_rank_field_goals_attempted",
           "team_rank_field_goal_percentage", "team_rank_three_points_made",
           "team_rank_three_points_attempted", "team_rank_three_point_percentage",
           "team_rank_free_throws_made", "team_rank_free_throws_attempted", "team_rank_free_throw_percentage",
           "prev_points_1", "prev_points_2", "prev_points_3", "prev_points_4", "prev_points_5",
           "prev_rebounds_1", "prev_rebounds_2", "prev_rebounds_3", "prev_rebounds_4", "prev_rebounds_5",
           "prev_assists_1", "prev_assists_2", "prev_assists_3", "prev_assists_4", "prev_assists_5",
           "prev_three_points_1", "prev_three_points_2", "prev_three_points_3", "prev_three_points_4", "prev_three_points_5"]

        team_dict = dict(zip(columns, team_data))
        return {"team_data": team_dict}
    
    return {"error": "Squadra non trovata"}

