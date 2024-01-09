import requests
import pandas as pd
import urllib3
import sqlite3
import os
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from config import CLIENT_ID, CLIENT_SECRET, REFRESH_TOKEN
conn = sqlite3.connect('data/strava/db_dashboard_running.sqlite')

auth_url = "https://www.strava.com/oauth/token"
activites_url = "https://www.strava.com/api/v3/athlete/activities"

payload = {
    'client_id': CLIENT_ID,
    'client_secret': CLIENT_SECRET,
    'refresh_token': REFRESH_TOKEN,
    'grant_type': "refresh_token",
    'f': 'json'
}

print("Requesting Token...\n")
res = requests.post(auth_url, data=payload, verify=False)
access_token = res.json()['access_token']

print("Access Token = {}\n".format(access_token))

# check if a folder exists
if not os.path.exists('./data/strava'):
    os.makedirs('./data/strava')

header = {'Authorization': 'Bearer ' + access_token}
page_num = 1
all_activities = []
while True:

    param = {'per_page': 200, 'page': page_num}
    activities = requests.get(activites_url, params=param, headers=header).json()

    if all_activities:
        all_activities.extend(activities)
    else:
        all_activities = activities

    print(len(activities))
    if len(activities) == 0:
        print("no more activities...")
        break

    page_num += 1

df = pd.DataFrame.from_records(all_activities)
df.to_csv('./data/strava/strava.csv')
conn.close()
