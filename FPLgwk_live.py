"""
Get gwk information

use event namne to query all the event IDs

Return information tag for all ongoing events

Sample data structure {"elements":[]}

URL= 'https://fantasy.premierleague.com/api/event/{eventID}/live/'

"""
import requests
from bs4 import BeautifulSoup
import json
import pandas as pd
import string

# Read csv for teams and players
Fsm_players = pd.read_csv("FPL_players.csv", usecols=["id"])

# Fetch from the bootstrap of general information and filter for event ID
for element_id in Fsm_players['id']:
    url = f"https://fantasy.premierleague.com/api/element-summary/{element_id}/"
    response = requests.get(url)
    data = response.json()

for fixture in data['fixtures']:
   event_id = fixture['event']
   #print(event_id)

url = f"https://fantasy.premierleague.com/api/event/{event_id}/live/"
event = requests.get(url)
event_data = event.json()
print(event_data)
   




