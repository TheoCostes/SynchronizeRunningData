import datetime
import os
from garminconnect import (
    Garmin
)
from config import GARMIN_EMAIL, GARMIN_PWD


def init_api():
    api_garmin = Garmin(GARMIN_EMAIL, GARMIN_PWD)
    api_garmin.login()

    return api_garmin


api = init_api()
start_date = datetime.date(2020, 1, 1)
end_date = datetime.date.today()

activities = api.get_activities_by_date(
                start_date.isoformat(), end_date.isoformat(), 'running')
print(f'activities : {len(activities)}')

# check if a folder exists
if not os.path.exists('./data/garmin'):
    os.makedirs('./data/garmin')

for i, activity in enumerate(activities):
    print(i)
    activity_id = activity["activityId"]
    gpx_data = api.download_activity(
                        activity_id, dl_fmt=api.ActivityDownloadFormat.CSV
                    )
    output_file = f"./data/garmin/{str(activity_id)}.csv"
    with open(output_file, "wb") as fb:
        fb.write(gpx_data)

print("Finished....")
