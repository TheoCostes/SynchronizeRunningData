import datetime
import os
import requests
import pandas as pd

from garminconnect import Garmin
# from config import GARMIN_EMAIL, GARMIN_PWD, CLIENT_ID, CLIENT_SECRET, REFRESH_TOKEN

os.chdir("../")

GARMIN_EMAIL = os.environ.get("GARMIN_EMAIL")
GARMIN_PWD = os.environ.get("GARMIN_PWD")

CLIENT_ID = os.environ.get("CLIENT_ID")
CLIENT_SECRET = os.environ.get("CLIENT_SECRET")
REFRESH_TOKEN = os.environ.get("REFRESH_TOKEN")


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
        # print(email, password)
        # self.api = self.init_api(email, password)
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
        if not os.path.exists("./data/garmin"):
            os.makedirs("./data/garmin")

        for i, activity in enumerate(activities):
            activity_id = activity["activityId"]
            activity_date = activity["startTimeLocal"]
            output_file = (
                f"./data/garmin/{str(activity_id)}_{str(activity_date)}.csv"
            )
            file_exists = os.path.isfile(output_file)
            if not file_exists:
                gpx_data = self.api.download_activity(
                    activity_id, dl_fmt=self.api.ActivityDownloadFormat.CSV
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

    def get_activity_laps(self, activity_id):
        """
        Fetch the laps for the given activity ID from the Strava API.
        :param activity_id:
        :return:
        """
        url = f"https://www.strava.com/api/v3/activities/{activity_id}/laps"
        headers = {"Authorization": f"Bearer {self.access_token}"}

        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            return response.json()
        else:
            return None

    def activity_traitement(self, activity):
        """
        Traitement des données de la lap (suppression de sous dictionnaires)
        :param lap:
        :return:lap
        """
        activity.pop('map', None)
        activity.pop('start_latlng', None)
        activity.pop('end_latlng', None)
        activity["athlete"] = activity["athlete"]["id"]

        return activity

    def lap_traitement(self, laps):
        """
        Traitement des données de la lap (suppression de sous dictionnaires)
        :param laps:
        :return:laps
        """
        for lap in laps:
            lap["activity"] = lap["activity"]["id"]
            lap["athlete"] = lap["athlete"]["id"]
        return laps

    def collect_data(self, id_list):
        """
        Save activities fetched from the Strava API in CSV format.
        If the directory for saving does not exist, create it.
        """
        if not os.path.exists("./data/strava"):
            os.makedirs("./data/strava")

        page_num = 1
        all_activities = []
        all_activities_by_laps = []
        print("fetching strava activities...")
        erreur = False
        while True:
            activities = self.get_activities(page_num)
            for i, el in enumerate(activities):
                print(el)
                if el["id"] not in id_list:
                    try:
                        id_list.append(el["id"])
                        print(i)
                        all_activities.append(self.activity_traitement(el))
                        all_activities_by_laps.extend(
                            self.lap_traitement(
                                self.get_activity_laps(el["id"])
                            )
                        )
                    except Exception as e:
                        print('error : plus de forfait api')
                        erreur = True
                        break
            if erreur:
                break
            if len(activities) == 0:
                print("no more activities...")
                break
            page_num += 1

        df_activities = pd.DataFrame(all_activities)

        df_lap = pd.DataFrame(all_activities_by_laps)

        df_id = pd.DataFrame(id_list, columns=["id"])
        return df_lap, df_activities, df_id


