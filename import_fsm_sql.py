import sqlite3
import pandas as pd

# Connect to the SQLite database
conn = sqlite3.connect('players.db')
cursor = conn.cursor()

# Generate season names and insert them
base_year_beg = 2023
base_year_end = 24
num_seasons = 10

season_names = []

for i in range(num_seasons):
    season_start = base_year_beg + i
    season_end_x = base_year_end + i
    season_name = f"{season_start}/{season_end_x}"
    season_names.append(season_name)

print(season_names)

# Check if the table exists
cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='Seasons';")
table_exists = cursor.fetchone()

if table_exists:
    # Table exists, insert data
    df_seasons = pd.DataFrame({'season_name': season_names})
    try:
        df_seasons.to_sql('Seasons', conn, if_exists='append', index=False)
        print("Data inserted successfully.")
    except Exception as e:
        print(f"Error occurred while inserting data: {e}")
    
    # Verify the data in the database
    cursor.execute("SELECT * FROM seasons;")
    rows = cursor.fetchall()
    print("Data in 'Seasons' table after insertion:")
    for row in rows: print(row)
else:
    print("Table 'Seasons' does not exist.")

# Commit and close the connection
conn.commit()
conn.close()
