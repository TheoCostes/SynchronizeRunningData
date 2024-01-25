import datetime
import os
import requests
import pandas as pd

from garminconnect import Garmin
from config import GARMIN_EMAIL, GARMIN_PWD, CLIENT_ID, CLIENT_SECRET, REFRESH_TOKEN


class GarminAPI:
    """
    This class is responsible for interacting with the Garmin API.
    It initializes the API, fetches activities, and saves them.
    """

    def __init__(self, email, password):
        """
        Args:
            email (str): The email to login to the Garmin API.
            password (str): The password to login to the Garmin API.
        """
        self.api = self.init_api(email, password)
        self.start_date = datetime.date(2020, 1, 1)
        self.end_date = datetime.date.today()

    def init_api(self, email, password):
        """
        Login to the Garmin API with the given email and password.

        Args:
            email (str): The email to login to the Garmin API.
            password (str): The password to login to the Garmin API.

        Returns:
            Garmin: The initialized Garmin API.
        """
        api = Garmin(email, password)
        api.login()
        return api

    def get_activities(self):
        """
        Fetch activities from the Garmin API between the start and end dates.

        Returns:
            list: The list of activities fetched from the Garmin API.
        """
        activities = self.api.get_activities_by_date(
            self.start_date.isoformat(), self.end_date.isoformat(), "running"
        )
        print(f"activities : {len(activities)}")
        return activities

    def save_data(self, activities):
        """
        Save the given activities in CSV format.
        If the directory for saving does not exist, create it.

        Args:
            activities (list): The list of activities to save.
        """
        if not os.path.exists("../data/garmin"):
            os.makedirs("../data/garmin")

        for i, activity in enumerate(activities):
            activity_id = activity["activityId"]
            activity_date = activity["startTimeLocal"]
            gpx_data = self.api.download_activity(
                activity_id, dl_fmt=self.api.ActivityDownloadFormat.CSV
            )
            output_file = (
                f"../../data/garmin/{str(activity_id)}_{str(activity_date)}.csv"
            )
            with open(output_file, "wb") as fb:
                fb.write(gpx_data)


class StravaAPI:
    """
    This class is responsible for interacting with the Strava API.
    It initializes the API, fetches activities, and saves them.
    """

    def __init__(self, client_id, client_secret, refresh_token):
        """
        Args:
            client_id (str): The client ID for the Strava API.
            client_secret (str): The client secret for the Strava API.
            refresh_token (str): The refresh token for the Strava API.
        """
        self.auth_url = "https://www.strava.com/oauth/token"
        self.activities_url = "https://www.strava.com/api/v3/athlete/activities"
        self.access_token = self.get_access_token(client_id, client_secret, refresh_token)

    def get_access_token(self, client_id, client_secret, refresh_token):
        """
        Fetch the access token from the Strava API using the given client ID, client secret, and refresh token.

        Args:
            client_id (str): The client ID for the Strava API.
            client_secret (str): The client secret for the Strava API.
            refresh_token (str): The refresh token for the Strava API.

        Returns:
            str: The access token from the Strava API.
        """
        payload = {
            "client_id": client_id,
            "client_secret": client_secret,
            "refresh_token": refresh_token,
            "grant_type": "refresh_token",
            "f": "json",
        }

        print("Requesting Token...\n")
        res = requests.post(self.auth_url, data=payload, verify=False)
        access_token = res.json()["access_token"]
        return access_token

    def get_activities(self, page_num=1):
        """
        Fetch activities from the Strava API for the given page number.

        Args:
            page_num (int, optional): The page number to fetch activities from. Defaults to 1.

        Returns:
            list: The list of activities fetched from the Strava API.
        """
        header = {"Authorization": "Bearer " + self.access_token}
        param = {"per_page": 200, "page": page_num}
        activities = requests.get(
            self.activities_url, params=param, headers=header
        ).json()
        return activities

    def save_data(self):
        """
        Save activities fetched from the Strava API in CSV format.
        If the directory for saving does not exist, create it.
        """
        if not os.path.exists("../data/strava"):
            os.makedirs("../data/strava")

        page_num = 1
        all_activities = []
        while True:
            activities = self.get_activities(page_num)
            if all_activities:
                all_activities.extend(activities)
            else:
                all_activities = activities
            if len(activities) == 0:
                print("no more activities...")
                break
            page_num += 1

        df = pd.DataFrame.from_records(all_activities)
        df.to_csv("./data/strava/strava.csv")


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
        garmin_activities = self.garmin_api.get_activities()
        self.garmin_api.save_data(garmin_activities)

        self.strava_api.save_data()