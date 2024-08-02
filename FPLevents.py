"""
Get event information

Fetch event IDs for all gameweeks

filter status by event IDs and event_name

sample data structure

{
  "status": [],
  "leagues": ""
}

url='https://fantasy.premierleague.com/api/event-status/'

"""

import requests
from bs4 import BeautifulSoup
import json
import pandas as pd
import string


url='https://fantasy.premierleague.com/api/event-status/'

response= requests.get(url)

data=response.json()

print(f"here is something: {data}")