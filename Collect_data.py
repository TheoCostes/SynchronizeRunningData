import pandas as pd

from src.api import GarminAPI, StravaAPI
from config import GARMIN_EMAIL, GARMIN_PWD, CLIENT_ID, CLIENT_SECRET, REFRESH_TOKEN


class Collector:
    """
    This class is responsible for managing the Garmin and Strava APIs.
    It initializes the APIs and collects data from them.
    """

    def __init__(self):
        """
        Initialize the Garmin and Strava APIs.
        """
        self.garmin_api = GarminAPI(GARMIN_EMAIL, GARMIN_PWD)
        self.strava_api = StravaAPI(CLIENT_ID, CLIENT_SECRET, REFRESH_TOKEN)

    def collect_data(self):
        """
        Collect data from the Garmin and Strava APIs.
        Save the collected data.
        """
        print("starting to collect data...")
        #garmin_activities = self.garmin_api.get_activities()
        print("garmin activities fetched...")
        #self.garmin_api.save_data(garmin_activities)
        print("garmin data collected and saved...")
        df_lap, df_activities = self.strava_api.collect_data()
        print(type(df_activities))
        transformerActivities = TransformerActivities(df_activities)
        df_activities = transformerActivities.transform_data()

        df_lap.to_csv('data/strava/strava_laps.csv', mode='a', header=False)
        df_activities.to_csv('data/strava/strava.csv', mode='w', header=True, index=False)
        print("strava data collected and saved...")
        print("finished collecting data...")


class TransformerActivities:

    def __init__(self, df_activities):
        self.df_activities = df_activities
        self.columns = ['id', 'athlete', 'name', 'type', 'start_date_local', 'distance', 'moving_time',
                        'total_elevation_gain', 'type',
                        'sport_type', 'workout_type', 'start_date_local',
                        'timezone', 'utc_offset', 'achievement_count', 'kudos_count', 'comment_count',
                        'average_speed',
                        'max_speed', 'average_cadence', 'average_watts', 'max_watts',
                        'weighted_average_watts', 'kilojoules', 'device_watts', 'has_heartrate',
                        'average_heartrate', 'max_heartrate', 'elev_high', 'elev_low',
                        'upload_id_str', 'external_id', 'pr_count',
                        'total_photo_count', 'suffer_score'
                        ]

    def clean_data(self):
        self.df_activities = self.df_activities[self.columns]
        self.df_activities = self.df_activities.drop_duplicates(subset=['id'])

    def add_feature(self):
        self.df_activities['numero_semaine_prepa'] = self.df_activities['name'].apply(lambda x: x.split(' - ')[0])
        self.df_activities['numero_seance_semaine'] = self.df_activities['name'].apply(lambda x: x.split(' - ')[1])
        self.df_activities['type_seance'] = self.df_activities['name'].apply(lambda x: x.split(' - ')[3])
        self.df_activities['prepa_name'] = self.df_activities['name'].apply(lambda x: x.split(' - ')[2])

        #convert average_speed to km/h and round to 2 decimals and also to pace
        self.df_activities['average_speed'] = self.df_activities['average_speed'] * 3.6
        self.df_activities['average_speed'] = self.df_activities['average_speed'].round(2)
        self.df_activities['average_pace'] = 60 / self.df_activities['average_speed']

        #convert moving_time to minutes and to hour
        self.df_activities['moving_time_minute'] = self.df_activities['moving_time'] / 60
        self.df_activities['moving_time_hour'] = self.df_activities['moving_time_minute'] / 60

    def transform_data(self):
        self.clean_data()
        self.add_feature()
        return self.df_activities


if __name__ == "__main__":
    collector = Collector()
    collector.collect_data()