from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import psycopg2
import os

app = FastAPI()

# ✅ Abilitare CORS per permettere al frontend di accedere all'API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Permette richieste da qualsiasi dominio
    allow_credentials=True,
    allow_methods=["*"],  # Permette tutti i metodi (GET, POST, PUT, DELETE)
    allow_headers=["*"],  # Permette tutti gli headers
)

# Funzione per connettersi al database
def get_db_connection():
    return psycopg2.connect(os.environ["DATABASE_URL"])

# Endpoint base
@app.get("/")
def home():
    return {"message": "API NBA attiva!"}


# 🏀 **Endpoint per ottenere tutte le squadre NBA**
@app.get("/teams")
def get_teams():
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT DISTINCT team_name FROM nba_team_stats ORDER BY team_name;")
        teams = [row[0] for row in cur.fetchall()]
        cur.close()
        conn.close()
        return {"teams": teams}
    except Exception as e:
        return {"error": str(e)}


# 🏀 **Endpoint per ottenere i dati di una squadra specifica**
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

#########################################################################
# 🏀 **Endpoint per ottenere i giocatori di una squadra specifica**
@app.get("/players/{team_name}")
def get_players(team_name: str):
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT id, player_position, player_number, player_name, minutes_average, points_average,
                   rebounds_average, assists_average, three_points_made_average,
                   prev_points_1, prev_points_2, prev_points_3, prev_points_4, prev_points_5, prev_points_6, prev_points_7,
                   prev_rebounds_1, prev_rebounds_2, prev_rebounds_3, prev_rebounds_4, prev_rebounds_5, prev_rebounds_6, prev_rebounds_7,
                   prev_assists_1, prev_assists_2, prev_assists_3, prev_assists_4, prev_assists_5, prev_assists_6, prev_assists_7,
                   prev_three_points_1, prev_three_points_2, prev_three_points_3, prev_three_points_4, prev_three_points_5, prev_three_points_6, prev_three_points_7,
                   prev_minutes_1, prev_minutes_2, prev_minutes_3, prev_plus_minus_1, prev_plus_minus_2, prev_plus_minus_3
            FROM nba_player_stats WHERE team_name = %s ORDER BY player_name;
        """, (team_name,))
        
        players = cur.fetchall()
        cur.close()
        conn.close()

        if players:
            columns = ["id", "player_position", "player_number", "player_name", "minutes_average", "points_average",
                       "rebounds_average", "assists_average", "three_points_made_average",
                       "prev_points_1", "prev_points_2", "prev_points_3", "prev_points_4", "prev_points_5", "prev_points_6", "prev_points_7",
                       "prev_rebounds_1", "prev_rebounds_2", "prev_rebounds_3", "prev_rebounds_4", "prev_rebounds_5", "prev_rebounds_6", "prev_rebounds_7",
                       "prev_assists_1", "prev_assists_2", "prev_assists_3", "prev_assists_4", "prev_assists_5", "prev_assists_6", "prev_assists_7",
                       "prev_three_points_1", "prev_three_points_2", "prev_three_points_3", "prev_three_points_4", "prev_three_points_5", "prev_three_points_6", "prev_three_points_7",
                       "prev_minutes_1", "prev_minutes_2", "prev_minutes_3", "prev_plus_minus_1", "prev_plus_minus_2", "prev_plus_minus_3"]

            player_list = [dict(zip(columns, player)) for player in players]
            return {"players": player_list}
        
        return {"error": "Nessun giocatore trovato per questa squadra"}
    
    except Exception as e:
        return {"error": str(e)}


@app.get("/opponents/{team_name}")
def get_opponent_stats(team_name: str):
    conn = get_db_connection()
    cur = conn.cursor()
    
    # Seleziona i dati dell'avversario di una squadra
    cur.execute("SELECT * FROM nba_opponent_stats WHERE team_name = %s;", (team_name,))
    opponent_data = cur.fetchone()
    
    cur.close()
    conn.close()

    if opponent_data:
        columns = ["id", "team_name", "points", "rebounds", "offensive_rebounds", "defensive_rebounds",
                   "assists", "field_goals_made", "field_goals_attempted", "field_goal_percentage",
                   "three_points_made", "three_points_attempted", "three_point_percentage",
                   "free_throws_made", "free_throws_attempted", "free_throw_percentage",
                   "team_rank_points", "team_rank_rebounds", "team_rank_off_rebounds", "team_rank_def_rebounds",
                   "team_rank_assists", "team_rank_field_goals_made", "team_rank_field_goals_attempted",
                   "team_rank_field_goal_percentage", "team_rank_three_points_made",
                   "team_rank_three_points_attempted", "team_rank_three_point_percentage",
                   "team_rank_free_throws_made", "team_rank_free_throws_attempted", "team_rank_free_throw_percentage"]
        
        opponent_dict = dict(zip(columns, opponent_data))
        return {"opponent_data": opponent_dict}
    
    return {"error": "Dati avversari non trovati"}




@app.get("/matchup/{team1}/{team2}")
def compare_teams(team1: str, team2: str):
    conn = get_db_connection()
    cur = conn.cursor()
    
    # Dati della prima squadra
    cur.execute("SELECT * FROM nba_team_stats WHERE team_name = %s;", (team1,))
    team1_data = cur.fetchone()
    
    # Dati della seconda squadra
    cur.execute("SELECT * FROM nba_team_stats WHERE team_name = %s;", (team2,))
    team2_data = cur.fetchone()
    
    cur.close()
    conn.close()

    if team1_data and team2_data:
        columns = ["id", "team_name", "points", "rebounds", "offensive_rebounds", "defensive_rebounds",
                   "assists", "field_goals_made", "field_goals_attempted", "field_goal_percentage",
                   "three_points_made", "three_points_attempted", "three_point_percentage",
                   "free_throws_made", "free_throws_attempted", "free_throw_percentage",
                   "team_rank_points", "team_rank_rebounds", "team_rank_off_rebounds", "team_rank_def_rebounds",
                   "team_rank_assists", "team_rank_field_goals_made", "team_rank_field_goals_attempted",
                   "team_rank_field_goal_percentage", "team_rank_three_points_made",
                   "team_rank_three_points_attempted", "team_rank_three_point_percentage",
                   "team_rank_free_throws_made", "team_rank_free_throws_attempted", "team_rank_free_throw_percentage"]

        team1_dict = dict(zip(columns, team1_data))
        team2_dict = dict(zip(columns, team2_data))

        return {"team1": team1_dict, "team2": team2_dict}
    
    return {"error": "Dati delle squadre non trovati"}