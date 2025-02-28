#START
import psycopg2 # type: ignore
import os
from dotenv import load_dotenv
from nba_api.stats.endpoints import playergamelog, playercareerstats
import pandas as pd
import time 
import numpy as np


# Carica le credenziali dal file .env
load_dotenv()

# Carica le variabili d'ambiente
load_dotenv()

# Connessione a PostgreSQL
conn = psycopg2.connect(
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT")
)
cur = conn.cursor()

######################
#PLAYERS
#Giocatori e relativi ID
hawks_players = {
     "Young": 1629027, "Daniels": 1630700, "Risacher": 1642258, "Okongwu": 1630168, "LeVert": 1627747,
     "Mann": 1629611, "Niang": 1627777, "Capela": 203991, "Krejčí": 1630249, "Nance Jr": 1626204,
     "Gueye": 1631243, "Mathews": 1629726, "Wallace": 1630811, "Barlow": 1631230, "Plowden": 1631342
}

celtics_players = {
     "Tatum": 1628369, "Brown": 1627759, "White": 1628401, "Porzingis": 204001, "Holiday": 201950,
     "Pritchard": 1630202, "Horford": 201143, "Hauser": 1630573, "Kornet": 1628436, "Craig": 1628470, "Queta": 1629674,
     "Walsh": 1641775, "Tillman": 1630214, "Peterson": 1641809
}

nets_players = {
     "Thomas": 1630560, "Johnson": 1629661, "Russell": 1626156, "Claxton": 1629651,
     "K.Johnson": 1630553, "Wilson": 1630592, "Z.Williams": 1630533, "Clowney": 1641730, "T.Martin": 1631213,
     "Evbuomwan": 1641787, "Sharpe": 1630549, "Watford": 1630570, "Whitehead": 1641727, "Hayes": 1630165
}

hornets_players = {
     "LaMelo Ball": 1630163, "Miles Bridges": 1628970, "Green": 1630182, "Mark Williams": 1631109, "Smith Jr": 1641733, 
     "Nurkic": 203994, "Diabate": 1631217, "Okogie": 1629006, "Curry": 203552, "KJ Simpson": 1642354,
     "Jeffries": 1629610, "Wong": 1631209, "Salaun": 1642275, "Gibson": 201959, "E.Payton": 203901
}

bulls_players = {
     "Coby White": 1629632, "Vučević": 202696, "Dosunmu": 1630245, "Giddey": 1630581, "Patrick Williams": 1630172,
     "Lonzo Ball": 1628366, "Jalen Smith": 1630188, "Huerter": 1628989, "Tre Jones": 1630200, "Buzelis": 1641824,
     "Terry": 1631207, "Philips": 1641763, "Horton-Tucker": 1629659, "Collins": 1628380, "J.Carter": 1628975
}

cavs_players = {
     "Mitchell": 1628378, "Garland": 1629636, "Mobley": 1630596, "Allen": 1628386, "Wade": 1629731,
     "Strus": 1629622, "Hunter": 1629631, "Okoro": 1630171, "Ty Jerome": 1629660, "Merrill": 1630241,
     "Javonte Green": 1629750, "Porter": 1641854, "Tristan Thompson": 202684
}

mavs_players = {
     "Davis": 203076, "Irving": 202681, "Thompson": 202691, "Washington": 1629023, "Gafford": 1629655, #"Lively": 1641726,
     "Christie": 1631108, "Marshall": 1630230, "Caleb Martin": 1628997, "Dinwiddie": 203915,
     "Hardy": 1630702, "Exum": 203957, "Prosper": 1641765, "Edwards": 1630556, "Powell": 203939, "Brandon Williams": 1630314
}

nuggets_players = {
     "Jokic": 203999, "Porter Jr": 1629008, "Murray": 1627750, "Westbrook": 201566, "Gordon": 203932,
     "Braun": 1631128, "Watson": 1631212, "Strawther": 1631124, "DeAndre Jordan": 201599, "Šarić": 203967,
     "Nnaji": 1630192, "Pickett": 1629618, "Tyson": 1641816
}

pistons_players = {
     "Cunningham": 1630595, "Harris": 202699, "Thompson": 1641709, "Hardaway Jr": 203501, "Duren": 1631105,
     "Schroder": 203471, "Beasley": 1627736, "Stewart": 1630191, "Fontecchio": 1631323, "Holland": 1641842,
     "Sasser": 1631204, "Waters III": 1630322
}

warriors_players = {
     "Stephen Curry": 201939, "Draymond Green": 203110, "Jimmy Butler": 202710, "Kuminga": 1630228, "Post": 1642366,
     "Hield": 1627741, "Podziemski": 1641764, "Jackson-Davis": 1631218, "Moody": 1630541, "Looney": 1626172, "Payton II": 1627780,
     "Gui Santos": 1630611, "Spencer": 1630311
}

rockets_players = {
     "VanVleet": 1627832, "Green": 1630224, "Brooks": 1628415, "Sengun": 1630578, "Smith": 1631095,
     "A.Thompson": 1641708, "Eason": 1631106, "Whitmore": 1641715, "Adams": 203500, "Aaron Holiday": 1628988,
     "Tate": 1630256, "Sheppard": 1642263, "Landale": 1629111
}

pacers_players = {
     "Haliburton": 1630169, "Siakam": 1627783, "Mathurin": 1631097, "Turner": 1626167, "Nembhard": 1629614,
     "Nesmith": 1630174, "Sheppard": 1641767, "Toppin": 1630167, "McConnell": 204456, "Walker": 1641716,
     "Bryant": 1628418, "Furphy": 1642277
}

clippers_players = {
    "Harden": 201935, "Zubac": 1627826, "Norman Powell": 1626181, "Leonard": 202695, "Derrick Jones Jr": 1627884,
    "Bogdanovic": 203992, "Simmons": 1627732, "Dunn": 1627739, "Coffey": 1629599, "Eubanks": 1629234, "Batum": 201587,
    "Beauchamp": 1630699, "Mills": 201988, "Kai Jones": 1630539
}

lakers_players = {
    "LeBron James": 2544, "Doncic": 1629029, "Reaves": 1630559, "Hayes": 1629637, "Hachimura": 1629060, "Finney-Smith": 1627827, 
    "Knecht": 1642261, "J.Goodwin": 1630692, "Vanderbilt": 1629020, "Vincent": 1629216, "Milton": 1629003, "Len": 203458,
    "Koloko": 1631132, "Morris": 202693, "Jemison": 1641998
}

grizzlies_players = {
    "Morant": 1629630, "Jackson Jr": 1628991, "Bane": 1630217, "Edey": 1641744, "Wells": 1642377,
    "Aldama": 1630583, "Kennard": 1628379, "Pippen Jr": 1630590, "GG Jackson": 1641713, "Clarke": 1629634,
    "Huff": 1630643, "Bagley III": 1628963, "V.Williams": 1631246, "Konchar": 1629723 ,"Johnny Davis": 1631098
}

heat_players = {
    "Herro": 1629639, "Adebayo": 1628389, "Wiggins": 203952, "Rozier": 1626179, "Jovic": 1631107,
    "Duncan Robinson": 1629130, "Jaquez Jr": 1631170, "Highsmith": 1629312, "Ware": 1642276, "Burks": 202692,
    "Davion Mitchell": 1630558, "Larsson": 1641796, "Kyle Anderson": 203937, "Love": 201567
}

bucks_players = {
    "Antetokounmpo": 203507, "Lillard": 203081, "Kuzma": 1628398, "Brook Lopez": 201572, "Andre Jackson": 1641748,
    "Prince": 1627752, "Portis": 1626171, "AJ Green": 1631260, "Trent Jr": 1629018, "Kevin Porter Jr": 1629645,
    "Connaughton": 1626192, "Sims": 1630579, "Rollins": 1631157
}

timberwolves_players = {
    "Edwards": 1630162, "Randle": 203944, "Gobert": 203497, "McDaniels": 1630183, "Conley": 201144,
    "Reid": 1629675, "DiVincenzo": 1628978, "Alexander-Walker": 1629638, "Dillingham": 1642265, "Minott": 1631169,
    "Clark": 1641740, "Shannon": 1630545, "Ingles": 204060, "Garza": 1630568
}

pelicans_players = {
    "Murphy III": 1630530, "McCollum": 203468, "Zion": 1629627, "Missi": 1642274, "Alvarado": 1630631, #"Herb Jones": 1630529,
    "Bruce Brown": 1628971, "Boston": 1630527, "Hawkins": 1641722, "Rob-Earl": 1630526,
    "Olynyk": 203482, "Matkovic": 1631255, "Cain": 1631288, "Reeves": 1641810
}

knicks_players = {
    "Brunson": 1628973, "K.A.Towns": 1626157, "Anunoby": 1628384, "Hart": 1628404, "Bridges": 1628969,
    "McBride": 1630540, "Achiuwa": 1630173, "Payne": 1626166, "Hukporti": 1630574, "Delon Wright": 1626153,
    "Shamet": 1629013, "Robinson": 1629011
}

thunder_players = {
    "Shai Gilgeous-Alexander": 1628983, "Jalen Williams": 1631114, "Holmgren": 1631096, "Hartenstein": 1628392,
    "Dort": 1629652, "Wallace": 1641717, "Aaron Wiggins": 1630598, "Joe": 1630198, "Caruso": 1627936,
    "Ajay Mitchell": 1642349, "Kenrich Williams": 1629026, "Jaylin Williams": 1631119, "Dieng": 1631172,
    "Dillon Jones": 1641794
}

magic_players = {
    "Banchero": 1631094, "F.Wagner": 1630532, "Suggs": 1630591, "Caldwell-Pope": 203484, "Bitadze": 1629048,
    "Da Silva": 1641783, "W.Carter Jr": 1628976, "Black": 1641710, "C.Anthony": 1630175, "G.Harris": 203914,
    "Isaac": 1628371, "Houstan": 1631216, "Jett Howard": 1641724, "Queen": 1630243, "Joseph": 202709
}

sixers_players = {
    "Maxey": 1630178, "Embiid": 203954, "Paul George": 202331, "Oubre": 1626162, "Grimes": 1629656,
    "Yabusele": 1627824, "Justin Edwards": 1642348, "Gordon": 201569, "Lowry": 200768, "Drummond": 203083,
    "Council IV": 1641741, "Jared Butler": 1630215, "Dowtin": 1630288, "Bona": 1641737, "Roddy": 1631223
}

suns_players = {
    "Booker": 1626164, "Durant": 201142, "Beal": 203078, "Tyus Jones": 1626145, "Richards": 1630208,
    "Allen": 1628960, "O'Neale": 1626220, "Dunn": 1642346, "Cody Martin": 1628998, "Micic": 203995,
    "Plumlee": 203486, "Ighodaro": 1642345, "Morris": 1628420, "Bol": 1629626
}

blazers_players = {
    "Simons": 1629014, "Grant": 203924, "Camara": 1641739, "Ayton": 1629028, "Avdija": 1630166,
    "Sharpe": 1631101, "Henderson": 1630703, "Clingan": 1642270, "R.Williams III": 1629057, "Banton": 1630625,
    "Kris Murray": 1631200, "Jabari Walker": 1631133, "Rupert": 1641712, "Thybulle": 1629680
}

kings_players = {
    "LaVine": 203897, "Sabonis": 1627734, "DeRozan": 201942, "Monk": 1628370, "Murray": 1631099,
    "Ellis": 1631165, "Lyles": 1626168, "Valančiūnas": 202685, "LaRavia": 1631222, "McDermott": 203926,
    "Crowder": 203109, "Devin Carter": 1642269, "Fultz": 1628365
}

spurs_players = { #"Wembanyama": 1641705
    "Fox": 1628368, "Chris Paul": 101108, "Vassell": 1630170, "Barnes": 203084, "Biyombo": 282687,
    "Castle": 1642264, "K.Johnson": 1629640, "Champagnie": 1630577, "Sochan": 1631110, "Bassey": 1629646,
    "Mamukelashvili": 1630572, "Wesley": 1631104, "Branham": 1631103, "McLaughlin": 1629162, "Baldwin jr": 1631116 
}

raptors_players = {
    "RJ Barrett": 1629628, "Ingram": 1627742, "Poeltl": 1627751, "S.Barnes": 1630567, "Quickley": 1630193,
    "Dick": 1641711, "Agbaji": 1630534, "Walter": 1642266, "Boucher": 1628449, "Shead": 1642347,
    "Mogbo": 1642367, "Battle": 1642419, "O.Robinson": 1631115, "Tucker": 200782
}

jazz_players = {
    "Markkanen": 1628374, "Sexton": 1629012, "K.George": 1641718, "Kessler": 1631117, "Collins": 1628381,
    "Clarkson": 203903, "Collier": 1642268, "Filipowski": 1642271, "Juzang": 1630548, "Sensabaugh": 1641729,
    "Mykhailiuk": 1629004, "Cody Williams": 1642262, "Potter": 1630695, "KJ Martin": 1630231, "Springer": 1630531
}

wizards_players = {
    "Coulibaly": 1641731, "Poole": 1629673, "Middleton": 203114, "Sarr": 1642259, "Smart": 203935,
    "Brogdon": 1627763, "Carrington": 1642267, "Kispert": 1630557, "K.George": 1642273, "Champagnie": 1630551,
    "Holmes": 1626158, "Vukcevic": 1641774, "AJ Johnson": 1642358
    }


##################
# 7 PRECEDENTI P,R,A,3 PLAYERS

from requests.exceptions import ReadTimeout

from nba_api.stats.endpoints import commonplayerinfo

def get_player_info(player_id):
    """
    Recupera il numero di maglia e la posizione del giocatore usando NBA API.
    Se non disponibile, restituisce 0 e "Unknown".
    """
    try:
        player_info = commonplayerinfo.CommonPlayerInfo(player_id=player_id)
        df = player_info.get_data_frames()[0]

        player_number = df["JERSEY"].iloc[0]  # Numero di maglia
        player_position = df["POSITION"].iloc[0]  # Posizione (es. "G", "F", "C")

        # Se i dati sono nulli, impostiamo valori di default
        if pd.isna(player_number) or player_number == '':
            player_number = 0
        if pd.isna(player_position) or player_position == '':
            player_position = "Unknown"

        return int(player_number), str(player_position)

    except Exception as e:
        print(f"Errore nel recupero info per il giocatore {player_id}: {e}")
        return 0, "Unknown"  # Se fallisce, valori di default


def get_last_games(player_id, season="2024-25", max_retries=3):
    for attempt in range(max_retries):
        try:
            log = playergamelog.PlayerGameLog(player_id=player_id, season=season)
            df = log.get_data_frames()[0]
            last_7_games = df.head(7)
            last_3_games = df.head(3)
            
            time.sleep(1)

            return {
                "PTS": last_7_games["PTS"].tolist(),
                "REB": last_7_games["REB"].tolist(),
                "AST": last_7_games["AST"].tolist(),
                "3PM": last_7_games["FG3M"].tolist(),
                "MIN": last_3_games["MIN"].tolist(),
                "PLUS_MINUS": last_3_games["PLUS_MINUS"].tolist(),
            }
        except ReadTimeout:
            print(f"Timeout per il giocatore {player_id}. Tentativo {attempt + 1} di {max_retries}.")
            time.sleep(5)  # Pausa prima di riprovare
        except Exception as e:
            print(f"Errore per il giocatore {player_id}: {e}")
            break

    return {"PTS": [], "REB": [], "AST": [], "3PM": [], "MIN": [], "PLUS_MINUS": []}

import time
from requests.exceptions import ReadTimeout

def get_season_averages(player_id, season="2024-25", max_retries=3):
    for attempt in range(max_retries):
        try:
            stats = playercareerstats.PlayerCareerStats(player_id=player_id, timeout=60)
            df = stats.get_data_frames()[0]
    
            # Filtra i dati per la stagione 2024-25
            season_stats = df[df['SEASON_ID'] == season]
    
            if not season_stats.empty:
                games_played = season_stats['GP'].iloc[0]
                avg_pts = season_stats['PTS'].iloc[0] / games_played if games_played > 0 else 0
                avg_reb = season_stats['REB'].iloc[0] / games_played if games_played > 0 else 0
                avg_ast = season_stats['AST'].iloc[0] / games_played if games_played > 0 else 0
                avg_min = season_stats['MIN'].iloc[0] / games_played if games_played > 0 else 0
                avg_3pm = season_stats['FG3M'].iloc[0] / games_played if games_played > 0 else 0

                return {
                    "PTS": round(avg_pts, 1),
                    "REB": round(avg_reb, 1),
                    "AST": round(avg_ast, 1),
                    "MIN": round(avg_min, 1),
                    "3PM": round(avg_3pm, 1)
                }
            else:
                return {"PTS": 0, "REB": 0, "AST": 0, "MIN": 0, "3PM": 0}
        
        except ReadTimeout:
            print(f"Timeout per il giocatore {player_id}. Tentativo {attempt + 1} di {max_retries}.")
            time.sleep(10)  # Aspetta 10 secondi prima di riprovare
        except Exception as e:
            print(f"Errore per il giocatore {player_id}: {e}")
            break
    
    return {"PTS": 0, "REB": 0, "AST": 0, "MIN": 0, "3PM": 0}


def align_previous_data(stats):
    # Sostituisci i valori mancanti con 0
    def fill_missing(lst, length):
        return [val if val != "" else 0 for val in lst] + [0] * (length - len(lst))

    stats["PTS"] = fill_missing(stats["PTS"], 7)
    stats["REB"] = fill_missing(stats["REB"], 7)
    stats["AST"] = fill_missing(stats["AST"], 7)
    stats["3PM"] = fill_missing(stats["3PM"], 7)
    stats["MIN"] = fill_missing(stats["MIN"], 3)
    stats["PLUS_MINUS"] = fill_missing(stats["PLUS_MINUS"], 3)

    return stats


def process_players(players, team_name):
    for player_name, player_id in players.items():
        print(f"Elaborazione dati per {player_name} nella squadra {team_name}...")

        # 📌 Recupera posizione e numero di maglia
        player_number, player_position = get_player_info(player_id)

        # 📌 Recupera le statistiche
        stats = get_last_games(player_id)
        stats = align_previous_data(stats)
        season_averages = get_season_averages(player_id)

        # 📌 Query per inserire nel database, aggiunto `team_name`
        query = """
            INSERT INTO nba_player_stats (
                team_name, player_number, player_position, player_name, minutes_average, 
                points_average, rebounds_average, assists_average, three_points_made_average, 
                prev_points_1, prev_points_2, prev_points_3, prev_points_4, prev_points_5, 
                prev_points_6, prev_points_7, prev_rebounds_1, prev_rebounds_2, prev_rebounds_3, 
                prev_rebounds_4, prev_rebounds_5, prev_rebounds_6, prev_rebounds_7, prev_assists_1, 
                prev_assists_2, prev_assists_3, prev_assists_4, prev_assists_5, prev_assists_6, 
                prev_assists_7, prev_three_points_1, prev_three_points_2, prev_three_points_3, 
                prev_three_points_4, prev_three_points_5, prev_three_points_6, prev_three_points_7, 
                prev_minutes_1, prev_minutes_2, prev_minutes_3, prev_plus_minus_1, prev_plus_minus_2, 
                prev_plus_minus_3
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                      %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        # 📌 Costruisci il pacchetto dati con `team_name`
        data = (
            team_name, player_number, player_position, player_name, 
            season_averages["MIN"], season_averages["PTS"], season_averages["REB"], 
            season_averages["AST"], season_averages["3PM"],
            *stats["PTS"], *stats["REB"], *stats["AST"], *stats["3PM"], *stats["MIN"], *stats["PLUS_MINUS"]
        )

        # 📌 Esegui l'inserimento nel database
        cur.execute(query, data)

    conn.commit()
    time.sleep(3)
    print(f"✅ Dati dei giocatori per {team_name} aggiornati nel database!")


# Chiamata alla funzione con `team_name`
process_players(hawks_players, "Atlanta Hawks")
process_players(celtics_players, "Boston Celtics")
process_players(nets_players, "Brooklyn Nets")
process_players(hornets_players, "Charlotte Hornets")
process_players(bulls_players, "Chicago Bulls")
process_players(cavs_players, "Cleveland Cavaliers")
process_players(mavs_players, "Dallas Mavericks")
process_players(nuggets_players, "Denver Nuggets")
process_players(pistons_players, "Detroit Pistons")
process_players(warriors_players, "Golden State Warriors")
process_players(rockets_players, "Houston Rockets")
process_players(pacers_players, "Indiana Pacers")
process_players(clippers_players, "Los Angeles Clippers")
process_players(lakers_players, "Los Angeles Lakers")
process_players(grizzlies_players, "Memphis Grizzlies")
process_players(heat_players, "Miami Heat")
process_players(bucks_players, "Milwaukee Bucks")
process_players(timberwolves_players, "Minnesota Timberwolves")
process_players(pelicans_players, "New Orleans Pelicans")
process_players(knicks_players, "New York Knicks")
process_players(thunder_players, "Oklahoma City Thunder")
process_players(magic_players, "Orlando Magic")
process_players(sixers_players, "Philadelphia 76ers")
process_players(suns_players, "Phoenix Suns")
process_players(blazers_players, "Portland Trail Blazers")
process_players(kings_players, "Sacramento Kings")
process_players(spurs_players, "San Antonio Spurs")
process_players(raptors_players, "Toronto Raptors")
process_players(jazz_players, "Utah Jazz")
process_players(wizards_players, "Washington Wizards")

print("Dati dei giocatori aggiornati!")


##############
#5 PRECEDENTI

from nba_api.stats.endpoints import teamgamelog
from requests.exceptions import ReadTimeout

# Dizionario con gli ID delle squadre e le righe del foglio
teams = {
    "Atlanta Hawks": 1610612737, "Boston Celtics": 1610612738, "Brooklyn Nets": 1610612751, 
    "Charlotte Hornets": 1610612766, "Chicago Bulls": 1610612741, "Cleveland Cavaliers": 1610612739, 
    "Dallas Mavericks": 1610612742, "Denver Nuggets": 1610612743, "Detroit Pistons": 1610612765, 
    "Golden State Warriors": 1610612744, "Houston Rockets": 1610612745, "Indiana Pacers": 1610612754, 
    "Los Angeles Clippers": 1610612746, "Los Angeles Lakers": 1610612747, "Memphis Grizzlies": 1610612763,
    "Miami Heat": 1610612748, "Milwaukee Bucks": 1610612749, "Minnesota Timberwolves": 1610612750,
    "New Orleans Pelicans": 1610612740, "New York Knicks": 1610612752, "Oklahoma City Thunder": 1610612760,
    "Orlando Magic": 1610612753, "Philadelphia 76ers": 1610612755, "Phoenix Suns": 1610612756, 
    "Portland Trail Blazers": 1610612757, "Sacramento Kings": 1610612758, "San Antonio Spurs": 1610612759,
    "Toronto Raptors": 1610612761, "Utah Jazz": 1610612762, "Washington Wizards": 1610612764
}

teams_opp = teams  # Stesso elenco di squadre per gli avversari

teams_rank = teams  # Anche qui, stessi ID per i ranking

import concurrent.futures

# # 1️⃣ Funzione per ottenere le ultime 5 partite di una squadra
def get_last_team_stats(team_id, season="2024-25", max_retries=2):
    for attempt in range(max_retries):
        try:
            log = teamgamelog.TeamGameLog(team_id=team_id, season=season, timeout=5)  # Timeout ridotto a 10 sec
            df = log.get_data_frames()[0]

            # Prendere le ultime 5 partite
            last_5_games = df.head(5)

            # Estrarre i dati rilevanti
            points = last_5_games["PTS"].tolist()
            rebounds = last_5_games["REB"].tolist()
            assists = last_5_games["AST"].tolist()
            three_pointers = last_5_games["FG3M"].tolist()

            return points, rebounds, assists, three_pointers

        except ReadTimeout:
            print(f"⚠️ Timeout per il team {team_id}. Tentativo {attempt + 1} di {max_retries}.")
            time.sleep(5)  # Aspetta 5 secondi prima di riprovare
        except Exception as e:
            print(f"❌ Errore per il team {team_id}: {e}")
            break  # Esce se l'errore non è un timeout

    return [], [], [], []  # Se fallisce, restituisce liste vuote


# 2️⃣ Funzione per inserire i dati nel database
def insert_last_team_stats(team_name, team_id):
    points, rebounds, assists, three_pointers = get_last_team_stats(team_id=team_id)

    if not points:  
        print(f"⚠️ Nessun dato per {team_name}. Skipping...")
        return

    query = """
    UPDATE nba_team_stats 
    SET 
        prev_points_1 = %s, prev_points_2 = %s, prev_points_3 = %s, prev_points_4 = %s, prev_points_5 = %s,
        prev_rebounds_1 = %s, prev_rebounds_2 = %s, prev_rebounds_3 = %s, prev_rebounds_4 = %s, prev_rebounds_5 = %s,
        prev_assists_1 = %s, prev_assists_2 = %s, prev_assists_3 = %s, prev_assists_4 = %s, prev_assists_5 = %s,
        prev_three_points_1 = %s, prev_three_points_2 = %s, prev_three_points_3 = %s, prev_three_points_4 = %s, prev_three_points_5 = %s
    WHERE team_name = %s
    """

    data = (
        *points, *rebounds, *assists, *three_pointers, team_name
    )

    cur.execute(query, data)
    conn.commit()
    print(f"✅ Prec squadra {team_name} aggiornati!")


for team_name, team_id in teams_rank.items():
    insert_last_team_stats(team_name, team_id)
    time.sleep(2)  # Aspetta 2 secondi tra ogni squadra per evitare limiti API

print("🏀 Precedenti squadre NBA caricati nel database!")

##############
# 5 PREC OPP
# Dati delle squadre e righe in cui inserire i dati

from nba_api.stats.endpoints import leaguegamefinder
import pandas as pd
import time

import time
from requests.exceptions import ReadTimeout

def get_opponent_stats(team_id, season="2024-25", max_retries=3):
    for attempt in range(max_retries):
        try:
            gamefinder = leaguegamefinder.LeagueGameFinder(season_nullable=season, league_id_nullable="00", season_type_nullable="Regular Season")
            games = gamefinder.get_data_frames()[0]

            # Filtrare le partite giocate dalla squadra
            team_games = games[games['TEAM_ID'] == team_id]

            # Recuperare le partite degli avversari
            opponent_games = games[games['TEAM_ID'] != team_id]
            opponent_games = opponent_games[opponent_games['GAME_ID'].isin(team_games['GAME_ID'])]

            # Prendere solo le ultime 5 partite
            latest_opponent_games = opponent_games.sort_values(by='GAME_DATE', ascending=False).head(5)

            # Estrarre le statistiche
            points = latest_opponent_games['PTS'].tolist()
            rebounds = latest_opponent_games['REB'].tolist()
            assists = latest_opponent_games['AST'].tolist()
            fg3m = latest_opponent_games['FG3M'].tolist()

            return points, rebounds, assists, fg3m

        except ReadTimeout:
            print(f"⏳ Timeout per il team {team_id}. Tentativo {attempt + 1} di {max_retries}.")
            time.sleep(5)  # Aspetta 5 secondi prima di riprovare
        except Exception as e:
            print(f"❌ Errore per il team {team_id}: {e}")
            break  # Esce se c'è un errore non legato al timeout

    return [], [], [], []  # Se fallisce, restituisce liste vuote


# Funzione per ottenere i dati delle ultime 5 partite degli avversari
def insert_opponent_stats(team_name, team_id):
    points, rebounds, assists, fg3m = get_opponent_stats(team_id=team_id)

    if not points:  
        print(f"⚠️ Nessun dato per {team_name}. Skipping...")
        return

    query = """
    UPDATE nba_opponent_stats 
    SET 
        prev_points_1 = %s, prev_points_2 = %s, prev_points_3 = %s, prev_points_4 = %s, prev_points_5 = %s,
        prev_rebounds_1 = %s, prev_rebounds_2 = %s, prev_rebounds_3 = %s, prev_rebounds_4 = %s, prev_rebounds_5 = %s,
        prev_assists_1 = %s, prev_assists_2 = %s, prev_assists_3 = %s, prev_assists_4 = %s, prev_assists_5 = %s,
        prev_three_points_1 = %s, prev_three_points_2 = %s, prev_three_points_3 = %s, prev_three_points_4 = %s, prev_three_points_5 = %s
    WHERE team_name = %s
    """

    data = (
        *points, *rebounds, *assists, *fg3m, team_name
    )

    cur.execute(query, data)
    conn.commit()
    print(f"✅ Prec avversari di {team_name} aggiornati!")


for team_name, team_id in teams_opp.items():
    insert_opponent_stats(team_name, team_id)
    time.sleep(2)  # Aspetta 5 secondi tra ogni squadra per evitare limiti API

print("🏀 Precedenti avversari caricati nel database!")

#############
#TEAM & TEAM RANK

from nba_api.stats.endpoints import leaguedashteamstats, leaguegamefinder
import time

# Categorie che ti interessano
categories = ["PTS", "REB", "OREB", "DREB", "AST", "FGM", "FGA", "FG_PCT", "FG3M", "FG3A", "FG3_PCT", "FTM", "FTA", "FT_PCT"]

# ✅ Funzione per aggiungere il suffisso SOLO in fase di VISUALIZZAZIONE (NON per il DB)
def add_rank_suffix(rank):
    if isinstance(rank, int):  # Applica solo ai numeri interi
        if rank % 10 == 1 and rank % 100 != 11:
            return f"{rank}st"
        elif rank % 10 == 2 and rank % 100 != 12:
            return f"{rank}nd"
        elif rank % 10 == 3 and rank % 100 != 13:
            return f"{rank}rd"
        else:
            return f"{rank}th"
    return rank  

# ✅ Funzione per ottenere le statistiche della squadra senza arrotondare prematuramente
def get_team_stats(season="2024-25", team_id=None):
    stats = leaguedashteamstats.LeagueDashTeamStats(season=season, per_mode_detailed="PerGame")
    df = stats.get_data_frames()[0]
    
    # Filtrare le statistiche per la squadra selezionata
    team_stats = df[df["TEAM_ID"] == team_id]

    # Estrarre solo le categorie desiderate
    stats_row = [float(team_stats[cat].iloc[0]) for cat in categories]  

    # Non arrotondiamo qui per evitare perdita di precisione
    return stats_row, [team_stats[cat + "_RANK"].iloc[0] for cat in categories]

# ✅ Funzione per formattare le percentuali con il simbolo '%'
def format_percentage(value):
    """ Trasforma i valori decimali in percentuali con un solo decimale e il simbolo '%' """
    return f"{value * 100:.1f}%" if isinstance(value, (int, float, np.float64, np.float32)) else value

# ✅ Funzione per arrotondare le statistiche a un decimale
def format_stat(value):
    return round(float(value), 1) if isinstance(value, (int, float, np.int64, np.float64, np.float32)) else value

# ✅ Funzione per formattare i rank con suffisso
def format_rank(rank):
    return add_rank_suffix(int(rank)) if isinstance(rank, (int, float, np.int64, np.float64)) else rank

# ✅ Funzione per aggiornare le statistiche della squadra nel database
def insert_team_stats(team_name, team_id):
    try:
        team_stats, team_ranks = get_team_stats(team_id=team_id)

        if not team_stats or not team_ranks:  
            print(f"⚠️ Nessun dato disponibile per {team_name}. Skipping...")
            return

        # ✅ Arrotonda i valori numerici a 1 decimale
        team_stats = [format_percentage(x) if cat in ["FG_PCT", "FG3_PCT", "FT_PCT"] else format_stat(x) 
                      for x, cat in zip(team_stats, categories)]

        # ✅ Converte i rank in numeri interi e poi in stringa con suffisso
        team_ranks = [format_rank(x) for x in team_ranks]

        # 🛠 DEBUG: Stampiamo i dati prima di inserirli nel database
        print(f"\n📊 Inserimento dati per {team_name}")
        print(f"Stats per DB: {team_stats}")  
        print(f"Ranks: {team_ranks}")  

        # ✅ QUERY SQL con UPDATE (nessuna duplicazione)
        query = """
        UPDATE nba_team_stats 
        SET 
            points = %s, rebounds = %s, offensive_rebounds = %s, defensive_rebounds = %s, assists = %s, 
            field_goals_made = %s, field_goals_attempted = %s, field_goal_percentage = %s,
            three_points_made = %s, three_points_attempted = %s, three_point_percentage = %s,
            free_throws_made = %s, free_throws_attempted = %s, free_throw_percentage = %s,
            team_rank_points = %s, team_rank_rebounds = %s, team_rank_off_rebounds = %s, 
            team_rank_def_rebounds = %s, team_rank_assists = %s, 
            team_rank_field_goals_made = %s, team_rank_field_goals_attempted = %s, team_rank_field_goal_percentage = %s,
            team_rank_three_points_made = %s, team_rank_three_points_attempted = %s, team_rank_three_point_percentage = %s,
            team_rank_free_throws_made = %s, team_rank_free_throws_attempted = %s, team_rank_free_throw_percentage = %s
        WHERE team_name = %s
        """

        # ✅ Conversione per il database (ora le percentuali hanno il simbolo % e sono stringhe)
        data = (*team_stats, *team_ranks, team_name)

        cur.execute(query, data)
        conn.commit()
        print(f"✅ Team e Rank {team_name} aggiornati!")

    except Exception as e:
        print(f"⚠️ Errore nell'inserimento dei dati per {team_name}: {e}")

# ✅ Loop per aggiornare tutti i team in ordine alfabetico
for team_name in sorted(teams.keys()):
    insert_team_stats(team_name, teams[team_name])
    time.sleep(2)

print("✅ Team stats e rank aggiornati correttamente!")



##############
# TEAM OPPONENT & RANK ESTESO A TUTTE LE SQUADRE
# Categorie di statistiche
categories = ["PTS", "REB", "OREB", "DREB", "AST", "FGM", "FGA", "FG_PCT", "FG3M", "FG3A", "FG3_PCT", "FTM", "FTA", "FT_PCT"]

# ✅ Funzione per ribaltare i rank degli avversari (perché nei team i valori più alti sono migliori, mentre per gli opponent è l'opposto)
def reverse_rank(rank, total_teams=30):
    return total_teams - rank + 1

# ✅ Funzione per ottenere statistiche medie degli avversari
def get_opponent_avg_stats(season="2024-25", team_id=None):
    gamefinder = leaguegamefinder.LeagueGameFinder(season_nullable=season, league_id_nullable="00", season_type_nullable="Regular Season")
    games = gamefinder.get_data_frames()[0]

    team_games = games[games['TEAM_ID'] == team_id]
    opponent_games = games[games['TEAM_ID'] != team_id]
    opponent_games = opponent_games[opponent_games['GAME_ID'].isin(team_games['GAME_ID'])]

    if opponent_games.empty:
        return {cat: 0 for cat in categories}

    return {cat: opponent_games[cat].mean() for cat in categories}  # RIMUOVE L'ARROTONDAMENTO! # ✅ Arrotondamento a un decimale

# ✅ Funzione per calcolare i rank ribaltati con suffisso
def calculate_reversed_ranks(opponent_stats):
    rank_data = {}
    for cat in categories:
        sorted_teams = sorted(opponent_stats, key=lambda x: x[cat], reverse=True)
        rank_data[cat] = {
            team['team_name']: add_rank_suffix(reverse_rank(i + 1))  
            for i, team in enumerate(sorted_teams)
        }
    return rank_data

# ✅ Raccolta dati di tutte le squadre
all_opponent_stats = []

for team_name, team_id in teams_opp.items():
    try:
        opponent_avg_stats = get_opponent_avg_stats(team_id=team_id)
        if not opponent_avg_stats:  
            print(f"⚠️ Nessun dato disponibile per {team_name}. Skipping...")
            continue
        
        opponent_avg_stats['team_name'] = team_name
        all_opponent_stats.append(opponent_avg_stats)

        print(f"✅ Dati opponent raccolti per {team_name}.")
        time.sleep(3)  

    except Exception as e:
        print(f"⚠️ Errore nell'elaborazione per {team_name}: {e}")

# ✅ Calcolo dei rank ribaltati
rank_data = calculate_reversed_ranks(all_opponent_stats)
print("🏆 Aggiornamento dei rank ribaltati completato!")

def format_percentage(value):
    """ Assicura che il valore sia trasformato in percentuale con un solo decimale senza arrotondare e aggiunge '%' """
    precise_value = float(value) * 100  # Converte in percentuale prima dell'arrotondamento
    return f"{precise_value:.1f}%" if isinstance(value, (int, float, np.float64, np.float32)) else value


def add_rank_suffix(rank):
    """ Aggiunge il suffisso ai rank (es. 1 → 1st, 2 → 2nd) """
    if isinstance(rank, int):  
        if rank % 10 == 1 and rank % 100 != 11:
            return f"{rank}st"
        elif rank % 10 == 2 and rank % 100 != 12:
            return f"{rank}nd"
        elif rank % 10 == 3 and rank % 100 != 13:
            return f"{rank}rd"
        else:
            return f"{rank}th"
    return rank  

def clean_rank(rank):
    """ Rimuove il suffisso dal rank per salvarlo come numero intero nel database """
    if isinstance(rank, str):
        return int(''.join(filter(str.isdigit, rank)))  # Converte '26th' → 26
    return rank  

def update_opponent_stats_and_rank(team_name, opponent_avg_stats, rank_data):
    try:
        # ✅ Formattiamo le statistiche correttamente
        team_stats = [format_percentage(opponent_avg_stats[cat]) if "PCT" in cat else round(opponent_avg_stats[cat], 1) for cat in categories]

        # ✅ Formattiamo i rank per il log (con suffisso) e per il DB (senza suffisso)
        team_ranks_for_log = [add_rank_suffix(rank_data[cat].get(team_name, None)) for cat in categories]
        team_ranks_for_db = [add_rank_suffix(rank_data[cat].get(team_name, None)) for cat in categories]

        print(f"\n📊 Inserimento dati per {team_name}")
        print(f"Stats per DB: {team_stats}")
        print(f"Ranks: {team_ranks_for_log}")  # Mostra i rank con suffisso nel terminale

        query = """
            UPDATE nba_opponent_stats 
            SET 
                points = %s, rebounds = %s, offensive_rebounds = %s, defensive_rebounds = %s, assists = %s, 
                field_goals_made = %s, field_goals_attempted = %s, field_goal_percentage = %s,
                three_points_made = %s, three_points_attempted = %s, three_point_percentage = %s,
                free_throws_made = %s, free_throws_attempted = %s, free_throw_percentage = %s,
                team_rank_points = %s, team_rank_rebounds = %s, team_rank_off_rebounds = %s, 
                team_rank_def_rebounds = %s, team_rank_assists = %s, 
                team_rank_field_goals_made = %s, team_rank_field_goals_attempted = %s, team_rank_field_goal_percentage = %s,
                team_rank_three_points_made = %s, team_rank_three_points_attempted = %s, team_rank_three_point_percentage = %s,
                team_rank_free_throws_made = %s, team_rank_free_throws_attempted = %s, team_rank_free_throw_percentage = %s
            WHERE team_name = %s
        """

        data = (*team_stats, *team_ranks_for_db, team_name)  # Rank senza suffisso per il database

        cur.execute(query, data)
        conn.commit()
        print(f"✅ Dati opponent e rank per {team_name} aggiornati!\n")

    except Exception as e:
        print(f"⚠️ Errore nell'inserimento dati per {team_name}: {e}")


# ✅ Aggiorniamo tutte le squadre
print("🏀 Aggiornamento delle statistiche e dei rank degli avversari in corso...")

for team in all_opponent_stats:
    try:
        team_name = team["team_name"]
        update_opponent_stats_and_rank(team_name, team, rank_data)
        time.sleep(3)  

    except Exception as e:
        print(f"⚠️ Errore nell'elaborazione per {team_name}: {e}")

print("🎯 Inserimento completo delle statistiche e dei rank degli avversari!")


cur.close()
conn.close()
print("🔌 Connessione a PostgreSQL chiusa!")