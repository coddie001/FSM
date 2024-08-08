"""/

1. Load 23_24 players into pandas df - done

2. Fetch all players for 23_24 season sorted by atleast players with 1 point both from API and pandas - done

3. sort player for 24_25 season - done

4. Define function - done

3. Create tables for all players and season - done

4. Load PD 23_24 players info DB (Assign ID and status) - done

5. Load PD 24_25 players into DB (Assign ID and status) - pending

/"""

import requests
from bs4 import BeautifulSoup
import json
import pandas as pd
import string
import sqlite3


# Load 23_24 players into pandas df


ls_players = pd.read_csv('FPL_season_23_24.csv')

df_ls_players = pd.DataFrame(ls_players)

print(df_ls_players)

num_of_scores = []

df_scores = df_ls_players.iloc[:,3]
for scores in df_scores:
    scores = df_scores [df_scores > 0]
    num_of_scores = len (scores)
print(num_of_scores)


# Fetch the 24&25 season player data from the URL
url = 'https://fantasy.premierleague.com/api/bootstrap-static/'
response = requests.get(url)

# Parse the JSON response
data = response.json()

# Extract and filter player data
players = data['elements']
filtered_players = [
    {
        "id": player["id"],
        "first_name": player["first_name"],
        "second_name": player["second_name"],
        "web_name": player["web_name"],
        "team": player["team"],
        "photo": player["photo"],
        "points_per_game": player["points_per_game"],
        "total_points": player["total_points"],
        "goals_scored": player["goals_scored"],
        "goals_conceded": player["goals_conceded"],
        "own_goals": player["own_goals"],
        "penalties_saved": player["penalties_saved"],
        "penalties_missed": player["penalties_missed"],
        "saves": player["saves"],
        "bonus": player["bonus"],
        "bps": player["bps"],
        "starts": player["starts"],
        "assists": player["assists"],
        "clean_sheets": player["clean_sheets"],
        "yellow_cards": player["yellow_cards"],
        "red_cards": player["red_cards"],
    }
    for player in players if player['total_points'] != 0 
]

df_player_ls_col = [
    'id', 'first_name','team', 'second_name', 'web_name', 'photo',
    'points_per_game']

df_filtered = pd.DataFrame(filtered_players, columns=df_player_ls_col)
#df_filtered.set_index('id', inplace=True)
df = df_filtered.reset_index(drop=True)

count = len(df_filtered)
print(f"Count of players in DataFrame:{count}")


##################################################### Analyze past season performance

season_count = 0

all_filtered_season = []

df_season_ls_col = ['element_id','season_name', 'total_points', 'start_cost', 'end_cost', 'goals_scored','goals_conceded', 'own_goals', 
    'penalties_saved', 'penalties_missed', 'saves', 'bonus', 'bps', 'starts', 'assists', 'clean_sheets', 'yellow_cards', 'red_cards']

for element_id in df_filtered['id']:
    url = f"https://fantasy.premierleague.com/api/element-summary/{element_id}/"
    response = requests.get(url)
    data = response.json()
    
    history_past = [
        season for season in data['history_past']
        if season['season_name'] == '2023/24'
    ]

    filtered_season = [
        {
            "element_id": element_id,
            "season_name": season["season_name"],
            "total_points": season["total_points"],
            "start_cost": season["start_cost"],
            "end_cost": season["end_cost"],
            "goals_scored": season["goals_scored"],
            "goals_conceded": season["goals_conceded"],
            "own_goals": season["own_goals"],
            "penalties_saved": season["penalties_saved"],
            "penalties_missed": season["penalties_missed"],
            "saves": season["saves"],
            "bonus": season["bonus"],
            "bps": season["bps"],
            "starts": season["starts"],
            "assists": season["assists"],
            "clean_sheets": season["clean_sheets"],
            "yellow_cards": season["yellow_cards"],
            "red_cards": season["red_cards"],
        }
        for season in history_past
    ]

    if len(filtered_season) > 0:
        season_count += 1
        all_filtered_season.extend(filtered_season)

    #print(filtered_season)

print(f"Number of element IDs that had a season of 23/24: {season_count}")

df_season = pd.DataFrame(all_filtered_season, columns=df_season_ls_col)


def gen_players_23_24(df_filtered, df_season):
    df_players_ls = pd.merge(df_filtered, df_season,left_on='id', right_on='element_id')
    return df_players_ls

# Generate merged DataFrame

df_players_23_24 = gen_players_23_24(df_filtered, df_season)
print(df_players_23_24)


def gen_players_24_25(players):
    players_24_25 = [
        {
            "id": player["id"],
            "first_name": player["first_name"],
            "second_name": player["second_name"],
            "web_name": player["web_name"],
            "team": player["team"],
            "photo": player["photo"],
            "points_per_game": player["points_per_game"],
            "total_points": player["total_points"],
            "goals_scored": player["goals_scored"],
            "goals_conceded": player["goals_conceded"],
            "own_goals": player["own_goals"],
            "penalties_saved": player["penalties_saved"],
            "penalties_missed": player["penalties_missed"],
            "saves": player["saves"],
            "bonus": player["bonus"],
            "bps": player["bps"],
            "starts": player["starts"],
            "assists": player["assists"],
            "clean_sheets": player["clean_sheets"],
            "yellow_cards": player["yellow_cards"],
            "red_cards": player["red_cards"],
        }
        for player in players
    ]
    return players_24_25

players_24_25 = gen_players_24_25(players)

df_players_24_25 = pd.DataFrame(players_24_25)

count = len(players_24_25)
print(f"Count of players_24_25: {count}")
print (df_players_24_25)

############################################################ Map df_players_23_24 to team and season foreign keys to assign their corresponding values

# Connect to the SQLite database
conn = sqlite3.connect('/users/mac/Downloads/Data/players.db')
cursor = conn.cursor()

# Ensure foreign keys are enabled
conn.execute("PRAGMA foreign_keys = ON")

# Fetch or create a snapshot entry
cursor.execute('INSERT INTO Snapshots DEFAULT VALUES')
snapshot_id = cursor.lastrowid

# Fetch the team and season mappings
team_mapping = pd.read_sql_query("SELECT team_name, team_id FROM Teams", conn)
season_mapping = pd.read_sql_query("SELECT season_name, season_id FROM Seasons", conn)

# Convert to dictionaries for easy mapping
#team_dict = dict(zip(team_mapping['team_id'], team_mapping['team_name']))
season_dict = dict(zip(season_mapping['season_name'], season_mapping['season_id']))

# Map 'team' and 'season_name' to 'team_id' and 'season_id'
df_players_23_24 = df_players_23_24.rename(columns={'team':'team_id'})
#df_players_23_24.rename(columns = {'team':'team_id'})
#df_players_23_24['team_id'] = df_players_23_24['team'].map(team_dict)
df_players_23_24['season_id'] = df_players_23_24['season_name'].map(season_dict)

print (f"list of teams and seasons:{df_players_23_24['team_id']}")

# Define the columns for All_Players
all_players_columns = [
    'id', 'element_id', 'team_id', 'season_id', 'first_name', 'second_name', 
    'web_name', 'photo', 'points_per_game', 'total_points', 'start_cost', 'end_cost', 'goals_scored', 
    'goals_conceded', 'own_goals', 'penalties_saved', 'penalties_missed', 'saves', 'bonus', 'bps', 
    'starts', 'assists', 'clean_sheets', 'yellow_cards', 'red_cards'
]

# Rename 'id' to 'fpl_player_id' for the insertion
# df_players_23_24.rename(columns={'id': 'fpl_player_id'}, inplace=True)

# Insert data into All_Players

# Add the snapshot_id to the dataframe
df_players_23_24['snapshot_id'] = snapshot_id

# Insert data into All_Players
df_players_23_24[all_players_columns + ['snapshot_id']].to_sql('All_Players', conn, if_exists='append', index=False)

# df_players_23_24[all_players_columns].to_sql('All_Players', conn, if_exists='append', index=False)

conn.commit()
conn.close()

##################################################### Import 24_25 Players to All_players DB

"""conn = sqlite3.connect('/users/mac/Downloads/Data/players.db')
cursor = conn.cursor()

# Check if the table exists
cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='All_Players';")
table_exists = cursor.fetchone()

if table_exists:
    # Table exists, insert data
    try:
        df_players_23_24.to_sql('All_Players', conn, if_exists='append', index=False)
        print("Data inserted successfully.")
    except Exception as e:
        print(f"Error occurred while inserting data: {e}")
    
    # Verify the data in the database
    cursor.execute("SELECT * FROM All_Players;")
    rows = cursor.fetchall()
    print("Data in 'All_Players' table after insertion:")
    for row in rows:
        print(row)
else:
    print("Table 'All_Players' does not exist.")

# Commit and close the connection
conn.commit()
conn.close()"""