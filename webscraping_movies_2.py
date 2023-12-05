

import requests
import sqlite3
import pandas as pd
from bs4 import BeautifulSoup
import os

# Assign variables 
url = 'https://web.archive.org/web/20230902185655/https://en.everybodywiki.com/100_Most_Highly-Ranked_Films'
db_name = 'Movies.db'
table_name = 'Rotten_Tom_top_25'
csv_path = os.getcwd() + '/data/rt_top_25_films.csv'

# Create dataframe to store scraped data
df = pd.DataFrame(columns=["Rotten Tomatoes Rank","Film","Year"])
count = 0

# get data from webpage
html_page = requests.get(url).text
data = BeautifulSoup(html_page, 'html.parser')

# access information in tables with bs4
tables = data.find_all('tbody')
rows = tables[0].find_all('tr')

# loop through rows in table and get relevant details(Rotten toms rank, film, year)
for row in rows:
        col = row.find_all('td')
        if len(col) != 0 and col[3].contents[0] != 'unranked':
            # add details to dict for later use
            try:
                film_dict = {'Rotten Tomatoes Rank': int(col[3].contents[0]), \
                        'Film': col[1].contents[0],\
                        'Year': int(col[2].contents[0])}
            except ValueError:
                break

            # save dict to df & concatinate dfs 
            df1 = pd.DataFrame(film_dict, index=[0])
            df = pd.concat([df,df1], ignore_index=True)
            count += 1

# order by rank, only selected results past year 2000
df = df[df['Year'] >= 2000].reset_index(drop=True)
df = df.sort_values('Rotten Tomatoes Rank', ascending=True, ignore_index=True)
df = df.head(25)

# save data to csv
df.to_csv(csv_path)

# initialize db and save data
with sqlite3.connect(db_name) as db:
    df.to_sql(table_name, db, if_exists='replace', index=False)
