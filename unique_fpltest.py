import requests
from bs4 import BeautifulSoup
import json
import pandas as pd
import string


# Read csv for teams and players
Fsm_players = pd.read_csv("FPL_players.csv", usecols=["id"])
Fsm_teams = pd.read_csv("FPL_teams.csv", usecols=["id", "name"])


# Extract IDs and name
player_id = Fsm_players.iloc[:, :]
team_id = Fsm_teams.iloc[:, 0]
# Create a copy of Fsm_teams
team = Fsm_teams.copy()
#team = Fsm_teams.iloc[:, 1]

# Create DataFrames for teams & players
df_player_id = pd.DataFrame(player_id)
df_team_id = pd.DataFrame(team_id)
df_team = pd.DataFrame(team)
print(f"{df_player_id}{df_team_id}{df_team}")

# Define dictionary to store player fixtures DataFrames
player_fixtures_dict = {}
df_fix_col = ['id', 'code', 'team_h', 'team_h_score', 'team_a', 'team_a_score', 'event', 'finished', 'minutes', 'provisional_start_time', 'kickoff_time', 'event_name', 'is_home', 'difficulty']


for element_id in df_player_id['id']:
    url = f"https://fantasy.premierleague.com/api/element-summary/{element_id}/"
    response = requests.get(url)
    data = response.json()

    # Extract and filter fixtures data
    fixtures = data['fixtures']

    # Create DataFrame for current player's fixtures
    df_fixtures = pd.DataFrame(fixtures, columns=df_fix_col)
    #df_fixtures.set_index('event_name', inplace=True)

    # Replace team IDs with team names
    def replace_team_ids(df_fixtures, df_team):
        df_fixtures['team_h'] = df_fixtures['team_h'].map(df_team.set_index('id') ['name'])
        df_fixtures['team_a'] = df_fixtures['team_a'].map(df_team.set_index('id') ['name'])
        return df_fixtures

    df_fixtures.set_index('event_name', inplace=True)
    df_fixtures = replace_team_ids(df_fixtures.copy(), df_team.copy())

    # Add player's fixture DataFrame to the dictionary
    player_fixtures_dict[element_id] = df_fixtures

    # Print the transformed DataFrame (optional)
    print(df_fixtures.shape)

# Access DataFrame for a specific player (optional)
player_42_fixtures = player_fixtures_dict[339]
print(f"shape 2 {player_42_fixtures.shape}")

# Export to CSV (optional, modify path as needed)
player_42_fixtures.to_csv("fsm_fixtures.csv", index=False)
print("DF exported to csv")