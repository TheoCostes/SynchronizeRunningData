import pandas as pd
import boto3
from io import StringIO
import os

from src.api import GarminAPI, StravaAPI
try :
    # if local :
    from config import (GARMIN_EMAIL, GARMIN_PWD,
                        CLIENT_ID, CLIENT_SECRET, REFRESH_TOKEN,
                        AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
except:
    GARMIN_EMAIL = os.environ.get("GARMIN_EMAIL")
    GARMIN_PWD = os.environ.get("GARMIN_PWD")
    CLIENT_ID = os.environ.get("CLIENT_ID")
    CLIENT_SECRET = os.environ.get("CLIENT_SECRET")
    REFRESH_TOKEN = os.environ.get("REFRESH_TOKEN")
    AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID")
    AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")


def tryconvert(value, index, default):
    try:
        return value.split(' - ')[index]
    except IndexError:
        return default


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
        self.s3_bucket = "datarunning"
        self.s3_key_id = "strava_id.csv"
        self.s3_key_activities = "strava.csv"
        self.s3_key_lap = "strava_laps.csv"
        self.S3 = boto3.client(
            "s3",
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        )
        self.old_strava_id = self.get_old_strava_id()
        self.old_strava_activities = self.get_old_strava_activities()
        self.old_strava_lap = self.get_old_strava_lap()

    def get_old_strava_id(self):
        try:
            response = self.S3.get_object(Bucket=self.s3_bucket, Key=self.s3_key_id)
            existing_data = pd.read_csv(response['Body'])
            return existing_data
        except Exception as e:
            print("ERROR :", e)
            existing_data = pd.DataFrame()
            return existing_data


    def get_old_strava_activities(self):
        try:
            response = self.S3.get_object(Bucket=self.s3_bucket, Key=self.s3_key_activities)
            existing_data = pd.read_csv(response['Body'])
            return existing_data
        except Exception as e:
            print("ERROR :", e)
            existing_data = pd.DataFrame()
            return existing_data

    def get_old_strava_lap(self):
        try:
            response = self.S3.get_object(Bucket=self.s3_bucket, Key=self.s3_key_lap)
            existing_data = pd.read_csv(response['Body'])
            return existing_data
        except Exception as e:
            print("ERROR :", e)
            existing_data = pd.DataFrame()
            return existing_data

    def concat_and_save_strava_id(self, df_id):
        if df_id.empty:
            return
        updated_data = pd.concat([self.old_strava_id, df_id], ignore_index=True)
        updated_data = updated_data.drop_duplicates(subset=['id'])
        csv_buffer = StringIO()
        updated_data.to_csv(csv_buffer, index=False)
        self.S3.put_object(Body=csv_buffer.getvalue(), Bucket=self.s3_bucket, Key=self.s3_key_id)

    def concat_and_save_strava_activities(self, df_activities):
        if df_activities.empty:
            return
        updated_data = pd.concat([self.old_strava_activities, df_activities], ignore_index=True)
        updated_data = updated_data.drop_duplicates(subset=['id'])
        csv_buffer = StringIO()
        updated_data.to_csv(csv_buffer, index=False)
        self.S3.put_object(Body=csv_buffer.getvalue(), Bucket=self.s3_bucket, Key=self.s3_key_activities)

    def concat_and_save_strava_lap(self, df_lap):
        if df_lap.empty:
            return
        updated_data = pd.concat([self.old_strava_lap, df_lap], ignore_index=True)
        updated_data = updated_data.drop_duplicates(subset=['id'])
        csv_buffer = StringIO()
        updated_data.to_csv(csv_buffer, index=False)
        self.S3.put_object(Body=csv_buffer.getvalue(), Bucket=self.s3_bucket, Key=self.s3_key_lap)

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
        df_lap, df_activities, df_id = self.strava_api.collect_data(list(self.old_strava_id["id"].unique()))

        print(type(df_activities))
        transformerActivities = TransformerActivities(df_activities)
        df_activities = transformerActivities.transform_data()

        transformerLap = TransformerLap(df_lap)
        df_lap = transformerLap.transform_data()

        self.concat_and_save_strava_activities(df_activities)
        self.concat_and_save_strava_lap(df_lap)
        self.concat_and_save_strava_id(df_id)

        print("strava data collected and saved...")
        print("finished collecting data...")




class TransformerActivities:

    def __init__(self, df_activities):
        self.df_activities = df_activities
        self.columns = ['id', 'athlete', 'name', 'type', 'start_date_local', 'distance', 'moving_time',
                        'total_elevation_gain','sport_type', 'workout_type',
                        'timezone', 'utc_offset', 'achievement_count', 'kudos_count', 'comment_count',
                        'average_speed','max_speed', 'average_cadence', 'average_watts', 'max_watts',
                        'weighted_average_watts', 'kilojoules', 'device_watts', 'has_heartrate',
                        'average_heartrate', 'max_heartrate', 'elev_high', 'elev_low',
                        'upload_id_str', 'external_id', 'pr_count',
                        'total_photo_count', 'suffer_score'
                        ]

    def clean_data(self):
        print(self.df_activities.columns)
        print(self.df_activities)
        try:
            self.df_activities = self.df_activities[self.columns]
        except KeyError as e:
            print(e)
            list_columns_missing = str(e) \
                .split("]")[0] \
                .split("[")[1] \
                .replace("'", "")\
                .split(", ")
            for col in list_columns_missing:
                self.df_activities[col] = None
            self.df_activities = self.df_activities[self.columns]

        self.df_activities = self.df_activities.drop_duplicates(subset=['id'])

    def add_feature(self):
        self.df_activities['numero_semaine_prepa'] = self.df_activities['name'].apply(
            lambda x: tryconvert(x, 0, "None"))
        self.df_activities['numero_seance_semaine'] = self.df_activities['name'].apply(
            lambda x: tryconvert(x, 1, "None"))
        self.df_activities['type_seance'] = self.df_activities['name'].apply(lambda x: tryconvert(x, 3, "None"))
        self.df_activities['prepa_name'] = self.df_activities['name'].apply(lambda x: tryconvert(x, 2, "None"))

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

# create a class to manage the transformation lap data

class TransformerLap:
    def __init__(self, df_lap):
        self.df_lap = df_lap
        self.columns = ['id', 'activity', 'athlete', 'lap_index', 'split', 'start_index', 'end_index',
                        'moving_time', 'start_date_local', 'distance', 'average_speed', 'max_speed',
                        'total_elevation_gain', 'average_cadence','average_watts', 'average_heartrate',
                        'max_heartrate', 'pace_zone']

    def clean_data(self):
        try:
            self.df_lap = self.df_lap[self.columns]
        except KeyError as e:
            print(e)
            list_columns_missing = str(e) \
                .split("]")[0] \
                .split("[")[1] \
                .replace("'", "")\
                .split(", ")
            for col in list_columns_missing:
                self.df_lap[col] = None
            self.df_lap = self.df_lap[self.columns]
        self.df_lap = self.df_lap.drop_duplicates(subset=['id'])

    def transform_data(self):
        self.clean_data()
        return self.df_lap

if __name__ == "__main__":
    collector = Collector()
    collector.collect_data()