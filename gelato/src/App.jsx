import { useState, useEffect } from "react";
import axios from "axios";

function App() {
    const [teams, setTeams] = useState([]); // Lista squadre
    const [selectedTeam, setSelectedTeam] = useState("Atlanta Hawks"); // Squadra iniziale
    const [teamData, setTeamData] = useState(null); // Dati squadra selezionata

    const API_BASE_URL = "https://backend.onrender.com"; // Sostituisci con il tuo URL API su Render

    // Recupera la lista delle squadre all'avvio
    useEffect(() => {
        axios.get(`${API_BASE_URL}/teams`)
            .then(response => setTeams(response.data.teams))
            .catch(error => console.error("Errore nel recupero squadre:", error));
    }, []);

    // Recupera i dati della squadra selezionata
    useEffect(() => {
        if (selectedTeam) {
            axios.get(`${API_BASE_URL}/teams/${encodeURIComponent(selectedTeam)}`)
                .then(response => setTeamData(response.data.team_data))
                .catch(error => console.error("Errore nel recupero dati squadra:", error));
        }
    }, [selectedTeam]); // Si aggiorna quando cambia la squadra selezionata

    return (
        <div>
            <h1>Benvenuto nel mio sito NBA</h1>

            {/* Dropdown per selezionare una squadra */}
            <label>Seleziona una squadra: </label>
            <select value={selectedTeam} onChange={(e) => setSelectedTeam(e.target.value)}>
                {teams.map(team => (
                    <option key={team} value={team}>{team}</option>
                ))}
            </select>

            {/* Mostra i dati della squadra selezionata */}
            {teamData ? (
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
                    
                    <h3>Classifiche</h3>
                    <p><strong>Rank Punti:</strong> {teamData.team_rank_points}</p>
                    <p><strong>Rank Rimbalzi:</strong> {teamData.team_rank_rebounds}</p>
                    <p><strong>Rank Assist:</strong> {teamData.team_rank_assists}</p>
                    <p><strong>Rank Field Goals Made:</strong> {teamData.team_rank_field_goals_made}</p>
                    <p><strong>Rank Field Goal %:</strong> {teamData.team_rank_field_goal_percentage}</p>
                    <p><strong>Rank Three Points Made:</strong> {teamData.team_rank_three_points_made}</p>
                    <p><strong>Rank Three Point %:</strong> {teamData.team_rank_three_point_percentage}</p>
                    <p><strong>Rank Free Throws Made:</strong> {teamData.team_rank_free_throws_made}</p>
                    <p><strong>Rank Free Throw %:</strong> {teamData.team_rank_free_throw_percentage}</p>
    
                    <h3>Precedenti Partite</h3>
                    <p><strong>Punti Precedenti:</strong> {teamData.prev_points_1}, {teamData.prev_points_2}, {teamData.prev_points_3}, {teamData.prev_points_4}, {teamData.prev_points_5}</p>
                    <p><strong>Rimbalzi Precedenti:</strong> {teamData.prev_rebounds_1}, {teamData.prev_rebounds_2}, {teamData.prev_rebounds_3}, {teamData.prev_rebounds_4}, {teamData.prev_rebounds_5}</p>
                    <p><strong>Assist Precedenti:</strong> {teamData.prev_assists_1}, {teamData.prev_assists_2}, {teamData.prev_assists_3}, {teamData.prev_assists_4}, {teamData.prev_assists_5}</p>
                    <p><strong>Triple Segnate Precedenti:</strong> {teamData.prev_three_points_1}, {teamData.prev_three_points_2}, {teamData.prev_three_points_3}, {teamData.prev_three_points_4}, {teamData.prev_three_points_5}</p>
                </div>
            ) : (
                <p>Caricamento dati...</p>
            )}
        </div>
    ); 
}

export default App;
