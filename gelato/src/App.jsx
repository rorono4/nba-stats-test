import { useState, useEffect } from "react";
import axios from "axios";

function App() {
    const [teams, setTeams] = useState([]); // Lista squadre
    const [selectedTeam, setSelectedTeam] = useState(""); // Squadra selezionata
    const [teamData, setTeamData] = useState(null); // Dati squadra selezionata
    const [players, setPlayers] = useState([]); // Lista giocatori della squadra
    const [selectedPlayer, setSelectedPlayer] = useState(""); // Giocatore selezionato
    const [playerData, setPlayerData] = useState(null); // Dati del giocatore selezionato

    const API_BASE_URL = "https://nba-stats-test.onrender.com"; 

    // Recupera la lista delle squadre all'avvio
    useEffect(() => {
        axios.get(`${API_BASE_URL}/teams`)
            .then(response => setTeams(response.data.teams))
            .catch(error => console.error("Errore nel recupero squadre:", error));
    }, []);

    // Recupera i dati della squadra e la lista giocatori
    useEffect(() => {
        if (selectedTeam) {
            axios.get(`${API_BASE_URL}/teams/${encodeURIComponent(selectedTeam)}`)
                .then(response => setTeamData(response.data.team_data))
                .catch(error => console.error("Errore nel recupero dati squadra:", error));

            axios.get(`${API_BASE_URL}/players/${encodeURIComponent(selectedTeam)}`)
                .then(response => setPlayers(response.data.players))
                .catch(error => console.error("Errore nel recupero giocatori:", error));
        }
    }, [selectedTeam]);

    // Quando viene selezionato un giocatore, aggiorna i dati del giocatore
    useEffect(() => {
        if (selectedPlayer) {
            const playerInfo = players.find(player => player.player_name === selectedPlayer);
            setPlayerData(playerInfo);
        }
    }, [selectedPlayer, players]);

    return (
        <div>
            <h1>Benvenuto nel mio sito NBA</h1>

            {/* Dropdown per squadre */}
            <label>Seleziona una squadra: </label>
            <select value={selectedTeam} onChange={(e) => setSelectedTeam(e.target.value)}>
                <option value="">Seleziona una squadra</option>
                {teams.map((team, index) => (
                    <option key={index} value={team}>{team}</option>
                ))}
            </select>

            {/* Mostra dati squadra */}
            {teamData && (
                <div>
                    <h2>{teamData.team_name}</h2>
                    <p><strong>Punti:</strong> {teamData.points}</p>
                    <p><strong>Rimbalzi:</strong> {teamData.rebounds}</p>
                    <p><strong>Assist:</strong> {teamData.assists}</p>
                    <p><strong>Offensive Rebounds:</strong> {teamData.offensive_rebounds}</p>
                    <p><strong>Defensive Rebounds:</strong> {teamData.defensive_rebounds}</p>
                    <p><strong>Field Goals Made:</strong> {teamData.field_goals_made}</p>
                    <p><strong>Field Goals Attempted:</strong> {teamData.field_goals_attempted}</p>
                    <p><strong>Field Goal %:</strong> {teamData.field_goal_percentage}</p>
                    <p><strong>Three Points Made:</strong> {teamData.three_points_made}</p>
                    <p><strong>Three Points Attempted:</strong> {teamData.three_points_attempted}</p>
                    <p><strong>Three Point %:</strong> {teamData.three_point_percentage}</p>
                    <p><strong>Free Throws Made:</strong> {teamData.free_throws_made}</p>
                    <p><strong>Free Throws Attempted:</strong> {teamData.free_throws_attempted}</p>
                    <p><strong>Free Throw %:</strong> {teamData.free_throw_percentage}</p>
                </div>
            )}

            {/* Dropdown per giocatori */}
            {players.length > 0 && (
                <>
                    <label>Seleziona un giocatore: </label>
                    <select value={selectedPlayer} onChange={(e) => setSelectedPlayer(e.target.value)}>
                        <option value="">Seleziona un giocatore</option>
                        {players.map((player, index) => (
                            <option key={index} value={player.player_name}>{player.player_name}</option>
                        ))}
                    </select>
                </>
            )}

            {/* Mostra dati giocatore */}
            {playerData && (
                <div>
                    <h3>Statistiche di {playerData.player_name}</h3>
                    <p><strong>Minuti Medi:</strong> {playerData.minutes_average}</p>
                    <p><strong>Punti Medi:</strong> {playerData.points_average}</p>
                    <p><strong>Rimbalzi Medi:</strong> {playerData.rebounds_average}</p>
                    <p><strong>Assist Medi:</strong> {playerData.assists_average}</p>
                    <p><strong>Triple Segnate:</strong> {playerData.three_points_made_average}</p>
                    
                    <h3>Prestazioni Precedenti</h3>
                    <p><strong>Punti nelle ultime 5 partite:</strong> {playerData.prev_points_1}, {playerData.prev_points_2}, {playerData.prev_points_3}, {playerData.prev_points_4}, {playerData.prev_points_5}</p>
                    <p><strong>Rimbalzi nelle ultime 5 partite:</strong> {playerData.prev_rebounds_1}, {playerData.prev_rebounds_2}, {playerData.prev_rebounds_3}, {playerData.prev_rebounds_4}, {playerData.prev_rebounds_5}</p>
                    <p><strong>Assist nelle ultime 5 partite:</strong> {playerData.prev_assists_1}, {playerData.prev_assists_2}, {playerData.prev_assists_3}, {playerData.prev_assists_4}, {playerData.prev_assists_5}</p>
                    <p><strong>Triple segnate nelle ultime 5 partite:</strong> {playerData.prev_three_points_1}, {playerData.prev_three_points_2}, {playerData.prev_three_points_3}, {playerData.prev_three_points_4}, {playerData.prev_three_points_5}</p>
                </div>
            )}
        </div>
    );
}

export default App;
