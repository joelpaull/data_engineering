import requests
import sqlite3
import pandas as pd
from bs4 import BeautifulSoup
import os
from datetime import datetime

def convert_to_billions(num):
    num = num.replace(',', '')
    num = round(int(num)/1000, 2)
    return num

def timestamp(message):
    now = datetime.now().replace(microsecond=0)
    print(f'{now}   {message}')

timestamp('Retreiving Data...')

# Assign varibles 
url = 'https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'
db = 'World_Economies.db'
table = 'Countries_by_GDP'
csv_path = os.getcwd() + '/data/Countries_by_GDP.csv'
columns = ['Country', 'GDP_USD_billion']

# Create df to store data
df = pd.DataFrame(columns=columns)

# get data from webpage
html_page = requests.get(url).text

# Create bs4 tree object holding html data
data = BeautifulSoup(html_page, 'html.parser')

# Access relevant info in webpage by collecting all tables & selecting first tables rows
tables = data.find_all('tbody')
rows = tables[2].find_all('tr')

for row in rows:

    # Collect data from row
    col = row.find_all('td')
    country = row.find_all('a')
    if len(col) != 0 and len(country):
        try:
            USD_billion = convert_to_billions(col[2].contents[0])
            data_dict = {'Country': country[0].contents[0],
                        'GDP_USD_billion': USD_billion}
        except ValueError:
            continue

        # save data to df 
        df_to_add = pd.DataFrame(data_dict, index=[0])
        df = pd.concat([df, df_to_add], ignore_index=True)

timestamp('Saving Data to .csv & Database...')

# save df to csv & database
df.to_csv(csv_path)
with sqlite3.connect(db) as db:
    df.to_sql(table, db, if_exists='replace', index=False)

timestamp('Data Successfully Saved')
