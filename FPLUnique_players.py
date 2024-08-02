
""" Read the fixtures for each unique players
------------------
https://fantasy.premierleague.com/api/element-summary/47/ - done

Read csv for teams and players - done

store the element IDs in players with an array - done

Load URL with array - done

Define function to generate fixtures for fsm players - done
- create a new dataframe - done
- return data from the URL for each of the player IDs in players.csv - done
- Transform the these coulumns team_h in progress"""

import requests
from bs4 import BeautifulSoup
import json
import pandas as pd
import string


# Read csv for teams and players
Fsm_players = pd.read_csv("FPL_players.csv", usecols = ["id"])
Fsm_teams = pd.read_csv ("FPL_teams.csv", usecols =["id", "name"])


# Extract IDs and name
player_id = Fsm_players.iloc[:,:]
team_id = Fsm_teams.iloc[:,0]
team = Fsm_teams.iloc[:,1]

# Create DFs for teams & players
df_player_id= pd.DataFrame(player_id)
df_team_id= pd.DataFrame(team_id)
df_team=pd.DataFrame(team)
print (f"{df_player_id}{df_team_id}{df_team}")

# Define player element ID
all_player_fix=[]
player_fixtures_dict = {}
#filtered_fixs = []
df_fix_col=['id','code','team_h','team_h_score', 'team_a','team_a_score','event','finished','minutes','provisional_start_time','kickoff_time','event_name','is_home','difficulty']

for element_id in df_player_id['id']:
	url = f"https://fantasy.premierleague.com/api/element-summary/{element_id}/"
	response = requests.get(url)
	#print(response.text)
	data = response.json()
	#print(f"{data}")

	# Extract and filter fixtures data
	fixtures = data['fixtures']
	all_player_fix.extend(fixtures)
	#for fixture in fixtures:
		 #all_player_fix.append(fixture)
		 #print(f"{all_player_fix}")
	
	# create dataframes for players
	df_fixtures = pd.DataFrame(all_player_fix, columns=df_fix_col)
	df_fixtures.set_index('event_name', inplace=True)
	#print(df_fixtures)
	#player_fixtures_dict[element_id] = df_fixtures

	# Transform dataframes for players
	def replace_team_ids(df_fixtures, df_team):
		df_fixtures['team_h'] = df_fixtures['team_h'].map(df_team ['name'])
		df_fixtures['team_a'] = df_fixtures['team_a'].map(df_team ['name'])
		return df_fixtures

	# Apply transformation (outside the function)
	df_fixtures = replace_team_ids(df_fixtures.copy(), df_team.copy())  # Avoid modifying original DataFrame
	player_fixtures_dict[element_id] = df_fixtures

	# Print the transformed DataFrame (optional)
	#print(df_fixtures)
	print(df_fixtures.shape)

#acess data frame for each players
player_42_fixtures = player_fixtures_dict[339]
print(player_42_fixtures.shape) 
#player_42_fixtures.to_csv("fsm_fixtures.csv", index=False)
print("DF exported to csv")

"""set_index('id')

df_fixture=pd.DataFrame(fixtures, columns= df_fix_col)
	df_fixture.set_index('event_name', inplace=True)
	print(len(f"{df_fixture}"))
	player_fix_dfs.append(df_fixture)
	#print(df_fixtures.describe())

		 

def fix_dico(all_player_fix):
	fix_dico =df.dict["all_player_fix":{all_player_fix}, index=element_id]
	
	filtered_fix = {
	        "id": fixture["id"],
	      	"code": fixture["code"],
	      	"team_h": fixture["team_h"],
	      	"team_h_score": fixture["team_h_score"],
	      	"team_a": fixture["team_a"],
	      	"team_a_score": fixture["team_a_score"],
	      	"event": fixture["event"],
	      	"finished": fixture["finished"],
	      	"minutes": fixture["minutes"],
	      	"provisional_start_time": fixture["provisional_start_time"],
	      	"kickoff_time": fixture["kickoff_time"],
	      	"event_name": fixture["event_name"],
	      	"is_home": fixture["is_home"],
	      	"difficulty": fixture["difficulty"],
	    }
	    filtered_fixs.append(filtered_fix)
#print(f"fixtures:{filtered_fixs}")

# Frame new data from fixtures for 1 person
#df_fixtures= pd.DataFrame(all_player_fix)
#print (f"{df_fixtures}")

#df_fixtures.to_csv("FPL_fixtures.csv", index=False)

# Fetch the data from the URL for 1 player
#url = 'https://fantasy.premierleague.com/api/element-summary/{element_id}/'
#response = requests.get(url)
#data = response.BeautifulSoup() ------- /** using beautiful soup for parsing the source data
#data = BeautifulSoup(response.text, 'html.parser')

# Parse the JSON response

whats the problem
-----------------
i want to match the values of ether to a map of where






team_id and team 

ether = df.cols('team_h',[0,:]) and df.cols('team_a', ,[0,:])



ntv_a =[]
ntv_h =[]

for 'team_h' and 'team_a' in df_fixtures(df_team, df_team_id): 
	global ntv_a, ntv_h
	for ntv_a and ntv_h in team_h and team_a:
		df_team.iloc[:,0] = df_team_id.iloc[:,0]
		ntv_a.append('df_team')
		ntv_h.append('df_team')
	return ntv_a, ntv_h
df_fixtures.assign(['ntv_a', ntv] (Area=lambda df: df.Length*df.Height))
df_fixtures.assign(['ntv_h', ntv] (Area=lambda df: df.Length*df.Height))"""