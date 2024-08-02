import requests
from bs4 import BeautifulSoup
import json
import pandas as pd
import string

# Fetch the data from the URL
url = 'https://fantasy.premierleague.com/api/bootstrap-static/'
response = requests.get(url)
#data = response.BeautifulSoup() ------- /** using beautiful soup for parsing the source data
#data = BeautifulSoup(response.text, 'html.parser')
#print({data})

# Parse the JSON response
data = response.json()

# Extract and filter team data
teams = data['teams']
filtered_teams = []
#arsenal = [team for team in teams if team['name'] == 'Arsenal']    # Filter for specifc team

# Loop through each team in the data and extract specific information
for team in teams:
    filtered_team = {
        "id": team["id"],
        "name": team["name"],
        "short_name": team["short_name"],
        "team_division": team["team_division"],
        "played":team["played"],
        "points": team["points"],
        "position": team["position"],
        "win": team["win"],
        "draw": team["draw"],
        "loss": team["loss"],
        "form": team["form"],
        "strength": team["strength"],
        "strength_overall_home": team["strength_overall_home"],
        "strength_overall_away": team["strength_overall_away"],
        "strength_attack_home": team["strength_attack_away"],
        "strength_attack_away": team["strength_attack_away"],
        "strength_defence_home": team["strength_defence_away"],
        "strength_defence_away": team["strength_defence_away"],
    }
    filtered_teams.append(filtered_team)
num_teams=len(teams)
print(f"count of teams:{num_teams}")

# Extract and filter player data
players = data['elements']
filtered_players = []
#fsmplayer 1= [player for player in players if player['first_name'] == 'Kai' or player ['second_name'] == 'Havertz']
PY = ["Johnstone", "Petrović", "Edouard", "Welbeck", "Coufal", "Garner", "Dunk", "Doku", "Ramses Becker", "Dalot Teixeira", "Højlund", "Pickford", "Hoelgebaum Pereira", "Trippier", "Martínez Romero", "Souček", "van Dijk", "Gvardiol", "Núñez Ribeiro", "Leno", "Kudus", "Ward-Prowse", "dos Santos Magalhães", "Hernandez", "Saliba", "Mateta",
"Rice", "Havertz", "Gordon", "Heung-min", "Saka", "Foden", "Palmer", "Haaland", "Salah"]
# List of names to exclude
exclusions = ["Wood-Gordon"]
"""PY_updated=[]

for name in PY:
    word =name.split()
    PY_updated.append(word)
    print (f"new player list: {PY_updated}")"""


fsmpl = [player for player in players if any(name in player['second_name'] for name in PY) and player['second_name'] not in exclusions]
#fsmpl = [player for player in players if any(name in player['second_name'] for sublist in PY_updated for name in sublist)]

num_fsmpl = len(fsmpl)
print(f"count of the players{num_fsmpl}")


for player in fsmpl:
    filtered_player = {
        "id": player["id"],
        "first_name": player["first_name"],
        "second_name": player["second_name"],
        "web_name": player["web_name"],
        "team": player["team"],
        "photo": player["photo"],
        "form": player["form"],
        "event_points": player["event_points"],
        "points_per_game": player["points_per_game"],
        "total_points": player["total_points"],
        "minutes": player["minutes"],
        "goals_scored": player["goals_scored"],
        "assists": player["assists"],
        "clean_sheets": player["clean_sheets"],
        "goals_conceded": player["goals_conceded"],
        "own_goals": player["own_goals"],
        "penalties_saved": player["penalties_saved"],
        "penalties_missed": player["penalties_missed"],
        "yellow_cards": player["yellow_cards"],
        "red_cards": player["red_cards"],
        "saves": player["saves"],
        "bonus": player["bonus"],
        "bps": player["bps"],
        "influence": player["influence"],
        "creativity": player["creativity"],
        "threat": player["threat"],
        "ict_index": player["ict_index"],
        "starts": player["starts"],
        "expected_goals": player["expected_goals"],
        "expected_assists": player["expected_assists"],
        "expected_goal_involvements": player["expected_goal_involvements"],
        "expected_goals_conceded": player["expected_goals_conceded"],
        "now_cost": player["now_cost"],
        "total_points": player["total_points"],
        "minutes": player["minutes"],
        "goals_scored": player["goals_scored"],
        "assists": player["assists"],
        "clean_sheets": player["clean_sheets"],
        "yellow_cards": player["yellow_cards"],
        "red_cards": player["red_cards"],
        "chance_of_playing_next_round": player["chance_of_playing_next_round"],
        "chance_of_playing_this_round": player["chance_of_playing_this_round"],
    }
    filtered_players.append(filtered_player)

# Print the filtered data for teams
print("Filtered Teams:")
for team in filtered_teams:
    print(team)

# Print the filtered data for players
print("\nFiltered Players:")
for player in filtered_players:
    print(player)

# Convert the list to a DataFrame
df_teams = pd.DataFrame(filtered_teams)
df_players = pd.DataFrame(filtered_players)
print(f"Task completed for teams: {df_teams}, Task completed for players: {df_players}")

df_teams.to_csv("FPL_teams.csv", index=False)
df_players.to_csv("FPL_players.csv", index=False)
print("Finished processing and saved both files to csv")

"""# Define the Excel writer object
    with pd.ExcelWriter('multiple_sheets.xlsx') as writer:
    # Write each DataFrame to a different sheet
    df1.to_excel(writer, sheet_name='Sheet1', index=False)
    df2.to_excel(writer, sheet_name='Sheet2', index=False)"""