import http.client
import json
import time
import gspread
from google.oauth2.service_account import Credentials

# Configurazione Google Sheets
scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
creds = Credentials.from_service_account_file("chiavi_google_socc.json", scopes=scope)
gc = gspread.authorize(creds)
spreadsheet = gc.open("MS")
sheet = spreadsheet.worksheet("SOCC")

# Connessione all'API
conn = http.client.HTTPSConnection("api-football-v1.p.rapidapi.com")
headers = {
    'x-rapidapi-key': "80b9b5897fmsh30d7191130e8cbcp17829ajsnd6b8928d8ed9",
    'x-rapidapi-host': "api-football-v1.p.rapidapi.com"
}

# Dizionario che mappa gli ID delle squadre ai nomi
team_names = {
     499: "Atalanta", 500: "Bologna", 490: "Cagliari", 895: "Como",  511: "Empoli", 502: "Fiorentina", 495: "Genoa", 504: "Verona", 505: "Inter", 496: "Juventus", 
     487: "Lazio",867: "Lecce", 489: "AC Milan", 1579: "Monza", 492: "Napoli", 523: "Parma", 497: "AS Roma", 503: "Torino", 494: "Udinese", 517: "Venezia"
}

categories = ["WDL", "Goals Scored", "Expected Goals", "Cleansheets (%)", "Goalkeeper Saves", "Total Shots",
              "Shots on Goal", "Total Passes", "Ball Possession (%)", "Corner Kicks", "Fouls", "Offsides",
              "Yellow Cards", "Red Cards"]

#########################################################################################################
### TEAM & RANK

def calculate_points(wins, draws, losses):
    return wins * 3 + draws

def calculate_team_data(team_id):
    league_id = 135  # Serie A
    season = "2024"  # Stagione corrente

    stats_team = {key: 0 for key in categories}
    stats_opponents = {key: 0 for key in categories}
    team_wdl = {"win": 0, "draw": 0, "loss": 0}
    opponents_wdl = {"win": 0, "draw": 0, "loss": 0}
    played_matches = 0

    # Richiesta per ottenere le partite
    conn.request("GET", f"/v3/fixtures?team={team_id}&league={league_id}&season={season}", headers=headers)
    res = conn.getresponse()
    data = json.loads(res.read().decode("utf-8"))
    time.sleep(1)  # Pausa per evitare sovraccarico dell'API


    if "response" in data:
        for match in data["response"]:
            home_team = match['teams']['home']['id']
            away_team = match['teams']['away']['id']
            home_goals = match['goals']['home']
            away_goals = match['goals']['away']

            if home_goals is None or away_goals is None:
                continue

            played_matches += 1
            is_home = home_team == team_id

            team_goals = home_goals if is_home else away_goals
            opponents_goals = away_goals if is_home else home_goals
            stats_team["Goals Scored"] += team_goals
            stats_opponents["Goals Scored"] += opponents_goals

            if opponents_goals == 0:
                stats_team["Cleansheets (%)"] += 1
            if team_goals == 0:
                stats_opponents["Cleansheets (%)"] += 1

            if team_goals > opponents_goals:
                team_wdl["win"] += 1
                opponents_wdl["loss"] += 1
            elif team_goals < opponents_goals:
                team_wdl["loss"] += 1
                opponents_wdl["win"] += 1
            else:
                team_wdl["draw"] += 1
                opponents_wdl["draw"] += 1

            # Richiesta per statistiche dettagliate
            fixture_id = match['fixture']['id']
            conn.request("GET", f"/v3/fixtures/statistics?fixture={fixture_id}", headers=headers)
            stats_res = conn.getresponse()
            stats_data = json.loads(stats_res.read().decode("utf-8"))

            if "response" in stats_data and stats_data["response"]:
                for team_stats in stats_data["response"]:
                    team_id_in_stats = team_stats["team"]["id"]
                    target_stats = stats_team if team_id_in_stats == team_id else stats_opponents

                    for stat in team_stats["statistics"]:
                        stat_type = stat["type"]
                        stat_value = stat["value"]

                        if stat_type == "Shots on Goal" and stat_value:
                            target_stats["Shots on Goal"] += stat_value
                        elif stat_type == "Total Shots" and stat_value:
                            target_stats["Total Shots"] += stat_value
                        elif stat_type == "Fouls" and stat_value:
                            target_stats["Fouls"] += stat_value
                        elif stat_type == "Corner Kicks" and stat_value:
                            target_stats["Corner Kicks"] += stat_value
                        elif stat_type == "Offsides" and stat_value:
                            target_stats["Offsides"] += stat_value
                        elif stat_type == "Yellow Cards" and stat_value:
                            target_stats["Yellow Cards"] += stat_value
                        elif stat_type == "Red Cards" and stat_value:
                            target_stats["Red Cards"] += stat_value
                        elif stat_type == "Goalkeeper Saves" and stat_value:
                            target_stats["Goalkeeper Saves"] += stat_value
                        elif stat_type == "Total passes" and stat_value:
                            target_stats["Total Passes"] += stat_value
                        elif stat_type == "Ball Possession" and stat_value:
                            try:
                                # Rimuovi il simbolo % e convertilo in decimale (dividendo per 100)
                                possession = float(stat_value.strip('%')) / 100
                                
                                # Aggiungi il valore di possesso palla alla squadra o agli avversari
                                if team_id_in_stats == team_id:
                                    stats_team["Ball Possession (%)"] += possession
                                else:
                                    stats_opponents["Ball Possession (%)"] += possession
                            except (ValueError, AttributeError):
                                # Stampa un messaggio di errore per eventuali problemi nei dati
                                print(f"Errore nel parsing di Ball Possession: {stat_value}")
                        elif stat_type == "expected_goals" and stat_value:
                            target_stats["Expected Goals"] += float(stat_value)

    # Calcolo delle medie
    for stats in [stats_team, stats_opponents]:
        for key, value in stats.items():
            if key == "Ball Possession (%)":
                stats[key] = round(value / played_matches, 3)  # 3 cifre decimali per il possesso palla
            elif key == "Cleansheets (%)":
                stats[key] = round(value / played_matches, 4)  # Lascia 4 cifre per i cleansheet
            elif key not in ["WDL"]:
                stats[key] = round(value / played_matches, 1)

    stats_team["WDL"] = f"{team_wdl['win']}-{team_wdl['draw']}-{team_wdl['loss']}"
    stats_opponents["WDL"] = f"{opponents_wdl['win']}-{opponents_wdl['draw']}-{opponents_wdl['loss']}"

    team_points = calculate_points(team_wdl['win'], team_wdl['draw'], team_wdl['loss'])
    opponent_points = calculate_points(opponents_wdl['win'], opponents_wdl['draw'], opponents_wdl['loss'])

    return stats_team, stats_opponents, team_points, opponent_points

# Calcola la classifica per ogni categoria
def calculate_rankings(data, reverse=False):
    rankings = {}
    for category in categories:
        sorted_teams = sorted(data.items(), key=lambda x: x[1][category], reverse=not reverse)
        rankings[category] = {team: rank + 1 for rank, (team, _) in enumerate(sorted_teams)}
    return rankings

# Calcolo dei dati per tutte le squadre
team_data = {}
team_points_data = {}
opp_points_data = {}
for team_id in team_names:
    stats_team, stats_opponents, team_points, opponent_points = calculate_team_data(team_id)
    team_data[team_id] = {"team": stats_team, "opponents": stats_opponents}
    team_points_data[team_id] = team_points
    opp_points_data[team_id] = opponent_points
    time.sleep(2)  # Pausa per evitare di sovraccaricare l'API

# Calcola le classifiche per le categorie
team_rankings = calculate_rankings({team_id: data["team"] for team_id, data in team_data.items()})
opp_rankings = calculate_rankings({team_id: data["opponents"] for team_id, data in team_data.items()}, reverse=True)

# Classifiche per punti (WDL)
team_points_rankings = sorted(team_points_data.items(), key=lambda x: x[1], reverse=True)
opp_points_rankings = sorted(opp_points_data.items(), key=lambda x: x[1])  # Ribalta per Opponent

team_points_rank = {team_id: rank + 1 for rank, (team_id, _) in enumerate(team_points_rankings)}
opp_points_rank = {team_id: rank + 1 for rank, (team_id, _) in enumerate(opp_points_rankings)}


# Mappatura delle righe nel foglio Google
row_mapping = {
    499: (3, 6, 4, 7), 500: (13, 16, 14, 17), 490: (23, 26, 24, 27),
    895: (33, 36, 34, 37), 511: (43, 46, 44, 47), 502: (53, 56, 54, 57),
    495: (63, 66, 64, 67), 504: (73, 76, 74, 77), 505: (83, 86, 84, 87),
    496: (93, 96, 94, 97), 487: (103, 106, 104, 107), 867: (113, 116, 114, 117),
    489: (123, 126, 124, 127), 1579: (133, 136, 134, 137), 492: (143, 146, 144, 147),
    523: (153, 156, 154, 157), 497: (163, 166, 164, 167), 503: (173, 176, 174, 177),
    494: (183, 186, 184, 187), 517: (193, 196, 194, 197)
}

def convert_to_ordinal(n):
    if 11 <= (n % 100) <= 13:
        suffix = "th"
    else:
        suffix = {1: "st", 2: "nd", 3: "rd"}.get(n % 10, "th")
    return f"{n}{suffix}"

def prepare_updates(team_id, team_data, team_points_rank, opp_points_rank, team_rankings, opp_rankings):
    team_row, opp_row, team_rank_row, opp_rank_row = row_mapping[team_id]

    # Forza il WDL a essere trattato come testo aggiungendo un apostrofo all'inizio
    team_wdl = f"'{team_data[team_id]['team']['WDL']}"  # Es: '14-4-4
    opp_wdl = f"'{team_data[team_id]['opponents']['WDL']}"

    # Prepara i dati per le statistiche della squadra e avversarie
    team_values = [team_wdl] + [team_data[team_id]["team"][category] for category in categories if category != "WDL"]
    opp_values = [opp_wdl] + [team_data[team_id]["opponents"][category] for category in categories if category != "WDL"]

    # Prepara i dati per i ranking
    team_rank_values = [convert_to_ordinal(team_points_rank[team_id])] + [convert_to_ordinal(team_rankings[category][team_id]) for category in categories if category != "WDL"]
    opp_rank_values = [convert_to_ordinal(opp_points_rank[team_id])] + [convert_to_ordinal(opp_rankings[category][team_id]) for category in categories if category != "WDL"]

    return [
        (f"B{team_row}:O{team_row}", [team_values]),
        (f"B{opp_row}:O{opp_row}", [opp_values]),
        (f"B{team_rank_row}:O{team_rank_row}", [team_rank_values]),
        (f"B{opp_rank_row}:O{opp_rank_row}", [opp_rank_values])
    ]

def batch_update_google_sheets(sheet, team_data, team_points_rank, opp_points_rank, team_rankings, opp_rankings):
    updates = []
    for team_id in team_names:
        print(f"Elaborazione dati per la squadra: {team_names[team_id]}")  # Messaggio per monitorare i progressi
        updates.extend(prepare_updates(team_id, team_data, team_points_rank, opp_points_rank, team_rankings, opp_rankings))

    # Esegui gli aggiornamenti batch con pause per evitare il superamento del limite API
    for i, (range_name, values) in enumerate(updates):
        sheet.update(range_name, values)
        if (i + 1) % 10 == 0:  # Dopo ogni 10 aggiornamenti
            print("Pausa per evitare il superamento del limite API...")
            time.sleep(10)  # Pausa di 10 secondi per rispettare i limiti delle API

# Esegui gli aggiornamenti
batch_update_google_sheets(sheet, team_data, team_points_rank, opp_points_rank, team_rankings, opp_rankings)
print("Aggiornamento completato su Google Sheets.")

########################################################################################################################
## 7 PRECEDENTI

# Mappatura delle righe e colonne nel foglio Google
precedents_row_mapping = {
    499: (3, 6), 500: (13, 16), 490: (23, 26),
    895: (33, 36), 511: (43, 46), 502: (53, 56),
    495: (63, 66), 504: (73, 76), 505: (83, 86),
    496: (93, 96), 487: (103, 106), 867: (113, 116),
    489: (123, 126), 1579: (133, 136), 492: (143, 146),
    523: (153, 156), 497: (163, 166), 503: (173, 176),
    494: (183, 186), 517: (193, 196)
}

column_mapping = {
    "Goals Scored": ("S", "Y"),
    "expected_goals": ("S", "Y"),
    "Goalkeeper Saves": ("S", "Y"),
    "Total Shots": ("AB", "AH"),
    "Shots on Goal": ("AB", "AH"),
    "Corner Kicks": ("AB", "AH"),
    "Fouls": ("AK", "AQ"),
    "Yellow Cards": ("AK", "AQ"),
    "Offsides": ("AK", "AQ")
}

def get_last_7_matches(team_id):
    league_id = 135
    season = "2024"
    conn.request("GET", f"/v3/fixtures?team={team_id}&league={league_id}&season={season}&status=FT", headers=headers)
    res = conn.getresponse()
    data = json.loads(res.read().decode("utf-8"))
    matches = [match for match in data.get("response", []) if match['goals']['home'] is not None and match['goals']['away'] is not None]
    return matches[-7:]  # Ultimi 7 incontri

def extract_statistics_from_matches(matches, team_id):
    team_stats = {
        "Goals Scored": [],
        "expected_goals": [],
        "Goalkeeper Saves": [],
        "Total Shots": [],
        "Shots on Goal": [],
        "Corner Kicks": [],
        "Fouls": [],
        "Yellow Cards": [],
        "Offsides": []
    }
    opponent_stats = {
        "Goals Scored": [],
        "expected_goals": [],
        "Goalkeeper Saves": [],
        "Total Shots": [],
        "Shots on Goal": [],
        "Corner Kicks": [],
        "Fouls": [],
        "Yellow Cards": [],
        "Offsides": []
    }

    for match in matches:
        home_team = match['teams']['home']['id']
        away_team = match['teams']['away']['id']
        is_home = (home_team == team_id)

        # Goals scored extraction
        team_goals = match['goals']['home'] if is_home else match['goals']['away']
        opponent_goals = match['goals']['away'] if is_home else match['goals']['home']
        team_stats["Goals Scored"].append(team_goals)
        opponent_stats["Goals Scored"].append(opponent_goals)

        # Extract detailed match statistics
        fixture_id = match['fixture']['id']
        conn.request("GET", f"/v3/fixtures/statistics?fixture={fixture_id}", headers=headers)
        stats_res = conn.getresponse()
        stats_data = json.loads(stats_res.read().decode("utf-8"))

        if "response" in stats_data and stats_data["response"]:
            for stat_group in stats_data["response"]:
                if stat_group['team']['id'] == team_id:
                    target_stats = team_stats
                else:
                    target_stats = opponent_stats

                for stat in stat_group['statistics']:
                    stat_type = stat["type"]
                    stat_value = stat["value"]

                    if stat_type == "Shots on Goal" and stat_value is not None:
                        target_stats["Shots on Goal"].append(stat_value)
                    elif stat_type == "Total Shots" and stat_value is not None:
                        target_stats["Total Shots"].append(stat_value)
                    elif stat_type == "Fouls" and stat_value is not None:
                        target_stats["Fouls"].append(stat_value)
                    elif stat_type == "Corner Kicks" and stat_value is not None:
                        target_stats["Corner Kicks"].append(stat_value)
                    elif stat_type == "Offsides" and stat_value is not None:
                        target_stats["Offsides"].append(stat_value)
                    elif stat_type == "Yellow Cards" and stat_value is not None:
                        target_stats["Yellow Cards"].append(stat_value)
                    elif stat_type == "Goalkeeper Saves" and stat_value is not None:
                        target_stats["Goalkeeper Saves"].append(stat_value)
                    elif stat_type == "expected_goals" and stat_value is not None:
                        target_stats["expected_goals"].append(float(stat_value))

    # Fill missing values with 0 if necessary
    for key in team_stats:
        team_stats[key] = (team_stats[key] + [0] * (7 - len(team_stats[key])))[:7]
        opponent_stats[key] = (opponent_stats[key] + [0] * (7 - len(opponent_stats[key])))[:7]

    return team_stats, opponent_stats


def update_google_sheet_with_precedents(sheet, team_id, last_7_team_stats, last_7_opponent_stats):
    team_row, opp_row = precedents_row_mapping[team_id]
    requests = []

    # Prepara gli aggiornamenti per le colonne di precedenti
    for stat, (start_col, end_col) in column_mapping.items():
        if stat == "Goals Scored":
            target_row_team, target_row_opp = team_row, team_row + 3  # Gol fatti e subiti
        elif stat == "expected_goals":
            target_row_team, target_row_opp = team_row + 1, team_row + 4
        elif stat == "Goalkeeper Saves":
            target_row_team, target_row_opp = team_row + 2, team_row + 5
        elif stat == "Total Shots":
            target_row_team, target_row_opp = team_row, team_row + 3
        elif stat == "Shots on Goal":
            target_row_team, target_row_opp = team_row + 1, team_row + 4
        elif stat == "Corner Kicks":
            target_row_team, target_row_opp = team_row + 2, team_row + 5
        elif stat == "Fouls":
            target_row_team, target_row_opp = team_row, team_row + 3
        elif stat == "Yellow Cards":
            target_row_team, target_row_opp = team_row + 1, team_row + 4
        elif stat == "Offsides":
            target_row_team, target_row_opp = team_row + 2, team_row + 5

        team_values = last_7_team_stats[stat][::-1]  # Ordine inverso
        opp_values = last_7_opponent_stats[stat][::-1]  # Ordine inverso

        requests.append({
            'range': f"{start_col}{target_row_team}:{end_col}{target_row_team}",
            'values': [team_values]
        })
        requests.append({
            'range': f"{start_col}{target_row_opp}:{end_col}{target_row_opp}",
            'values': [opp_values]
        })

    # Esegui batch_update per tutte le richieste
    try:
        sheet.batch_update(requests)
        print(f"Batch aggiornamento completato per la squadra: {team_names[team_id]}")
    except Exception as e:
        print(f"Errore durante l'aggiornamento per {team_names[team_id]}: {str(e)}")


def main():
    for team_id, team_name in team_names.items():
        print(f"Estrazione precedenti per {team_name}...")
        matches = get_last_7_matches(team_id)

        # Estrazione delle statistiche richieste per i precedenti
        last_7_team_stats, last_7_opponent_stats = extract_statistics_from_matches(matches, team_id)

        # Debug: stampa i valori delle statistiche estratte per ogni categoria
        for stat in last_7_team_stats:
            print(f"{team_name} - {stat} (Team): {last_7_team_stats[stat]}")
            print(f"{team_name} - {stat} (Opponents): {last_7_opponent_stats[stat]}")

        # Aggiorna il foglio con i dati
        update_google_sheet_with_precedents(sheet, team_id, last_7_team_stats, last_7_opponent_stats)
        print(f"Dati precedenti aggiornati per {team_name}.")

        time.sleep(2)  # Pausa per evitare di superare i limiti dell'API

if __name__ == "__main__":
    main()
    print("Aggiornamento dei precedenti completato.")

##########################################################################################
## under / over

def get_all_season_matches(team_id):
    league_id = 135
    season = "2024"
    
    conn.request("GET", f"/v3/fixtures?team={team_id}&league={league_id}&season={season}&status=FT", headers=headers)
    res = conn.getresponse()
    data = json.loads(res.read().decode("utf-8"))

    matches = [match for match in data.get("response", []) if match['goals']['home'] is not None and match['goals']['away'] is not None]
    return matches  # Restituisce tutte le partite della stagione


def calculate_goal_outcomes(matches):
    over_1_5_count = 0
    over_2_5_count = 0
    both_teams_scored_count = 0
    total_matches = len(matches)

    for match in matches:
        home_goals = match['goals']['home']
        away_goals = match['goals']['away']
        total_goals = home_goals + away_goals

        if total_goals > 1.5:
            over_1_5_count += 1
        if total_goals > 2.5:
            over_2_5_count += 1
        if home_goals > 0 and away_goals > 0:
            both_teams_scored_count += 1

    over_1_5_percentage = round((over_1_5_count / total_matches) * 100, 2) if total_matches > 0 else 0
    over_2_5_percentage = round((over_2_5_count / total_matches) * 100, 2) if total_matches > 0 else 0
    both_teams_scored_percentage = round((both_teams_scored_count / total_matches) * 100, 2) if total_matches > 0 else 0

    return over_1_5_percentage, over_2_5_percentage, both_teams_scored_percentage

def update_google_sheet_goal_outcomes(sheet, team_id, percentages):
    over_1_5, over_2_5, both_teams_scored = [val / 100 for val in percentages]  # Converto in decimale

    # Correggiamo la riga dei risultati
    result_row = precedents_row_mapping[team_id][0] + 5  # Riga risultati corretta (8 per Atalanta, 18 per Bologna, ecc.)

    # Aggiorna le celle K, M e O
    updates = [
        {'range': f"K{result_row}", 'values': [[over_1_5]]},
        {'range': f"M{result_row}", 'values': [[over_2_5]]},
        {'range': f"O{result_row}", 'values': [[both_teams_scored]]}
    ]

    try:
        sheet.batch_update(updates)
        print(f"Percentuali O1.5/O2.5/Gol entrambe aggiornate per la squadra: {team_names[team_id]}")
        print(f"Aggiornato per {team_names[team_id]}, risultato stampato su K{result_row}")
    except Exception as e:
        print(f"Errore durante l'aggiornamento delle percentuali goal per {team_names[team_id]}: {str(e)}")


def update_goal_outcomes_for_all_teams():
    for team_id, team_name in team_names.items():
        print(f"Calcolo esiti goal per {team_name}...")
        matches = get_all_season_matches(team_id)  # Ottieni tutte le partite della stagione

        # Calcolo delle percentuali sugli esiti goal
        percentages = calculate_goal_outcomes(matches)

        # Aggiorna il foglio Google
        update_google_sheet_goal_outcomes(sheet, team_id, percentages)

        # Debug: Stampa il team aggiornato
        print(f"Aggiornato per {team_name}, risultato stampato su K{precedents_row_mapping[team_id][1] - 8}")

        time.sleep(2)  # Pausa aumentata per evitare limiti API

    print("Aggiornamento completato per tutte le squadre.")


if __name__ == "__main__":
    main()  # Aggiorna i precedenti
    update_goal_outcomes_for_all_teams()  # Calcola e aggiorna O1.5, O2.5 e Gol entrambe
    print("Aggiornamento finale completato.")
