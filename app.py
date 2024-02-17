import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
# Importez plotly pour des graphiques interactifs si nécessaire
import plotly.express as px
import os
import boto3
import datetime

try :
    from config import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
except:
    AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID")
    AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")

s3_bucket = "datarunning"
s3_key_id = "strava_id.csv"
s3_key_activities = "strava.csv"
s3_key_lap = "strava_laps.csv"

s3 = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
)

response = s3.get_object(Bucket=s3_bucket, Key=s3_key_activities)
data = pd.read_csv(response['Body'])

# convert the column to datetime
data['date'] = data['start_date_local'].apply(
    lambda x : x.split("T")[0]
)
data['date'] = pd.to_datetime(data['date'], format='%Y-%m-%d')
data['start_date_local'] = pd.to_datetime(data['start_date_local'], format='ISO8601')

date_max = datetime.datetime(data['date'].max().year, data['date'].max().month, data['date'].max().day)
date_min = datetime.datetime(data['date'].min().year, data['date'].min().month, data['date'].min().day)

# Définition des filtres
st.sidebar.header('Choisir une prépa de course à pied')
type_seance = st.sidebar.multiselect('Prépa', options=data['prepa_name'].unique())

# create a slider for the date
st.sidebar.header('Choisir une période')
periode = st.sidebar.slider(
    'Période',
    min_value=date_min,
    max_value=date_max,
    value=(date_min, date_max)
)

# Filtrage des données en fonction des sélections
filtered_data = data[data['prepa_name'].isin(type_seance) & (data['date'] >= periode[0]) & (data['date'] <= periode[1])]

st.dataframe(filtered_data)

# Dashboard de KPIs
st.header('KPIs')
st.markdown(f"Volume Hebdomadaire d'Entraînement: {round(filtered_data['total_weekly_volume'].mean(),2)} km")
st.markdown(f"Distance Totale: {round(filtered_data['distance'].sum(),2)} km")
# Ajoutez d'autres KPIs selon les besoins

# Graphiques
st.header('Graphiques')

# Volume Hebdomadaire d'Entraînement
st.subheader('Volume Hebdomadaire d\'Entraînement')
fig, ax = plt.subplots()
ax.plot(filtered_data['date'], filtered_data['total_weekly_volume'])
st.pyplot(fig)

# Distribution des activités par type de séance
st.subheader('Distribution des Activités par Type de Séance')
fig = px.pie(filtered_data, names='type_seance', values='nombre_seances')
st.plotly_chart(fig)

# Evolution de la vitesse moyenne
st.subheader('Evolution de la Vitesse Moyenne')
fig, ax = plt.subplots()
ax.plot(filtered_data['date'], filtered_data['vitesse_moyenne'])
st.pyplot(fig)

# Ajoutez d'autres graphiques selon les besoins

# Résumés Hebdomadaires et Alertes
st.header('Résumés Hebdomadaires et Alertes')
# Implémentez la logique pour résumer les données hebdomadaires et générer des alertes

# Note: Ce code est un exemple basique pour démarrer. Vous devrez ajuster les noms des colonnes,
# le chargement des données, et la complexité des graphiques selon vos besoins spécifiques.
