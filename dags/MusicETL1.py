from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from datetime import datetime, timedelta
import pandas as pd

import spotipy
from spotipy.oauth2 import SpotifyOAuth, SpotifyClientCredentials
import applemusicpy

import psycopg2
from sqlalchemy import create_engine


# Authentication =======================================================

# Apple

with open('/opt/airflow/dags/AuthKey_H4JC8DUB9B.p8', 'r') as file:
    secret_key = file.read()

key_id = 'H4JC8DUB9B'
team_id = 'R5743U27B6'
am = applemusicpy.AppleMusic(secret_key, key_id, team_id)

# Spotify

SPOTIPY_CLIENT_ID = '235c5de559b94e1fad86246b7c922ba1'
SPOTIPY_CLIENT_SECRET = '6dbda14ae4a24338bdf3b4c81974d722'

auth_manager = SpotifyClientCredentials(
    client_id=SPOTIPY_CLIENT_ID, client_secret=SPOTIPY_CLIENT_SECRET)
sp = spotipy.Spotify(auth_manager=auth_manager)

# Function Definitions ====================================================

# Get playlist top chart from apple music


def am_playlist(id):

    playlist_features_list = ['title', 'artist', 'id']

    playlist_df = pd.DataFrame(columns=playlist_features_list)

    playlist = am.playlist(id)["data"]

    features_title = []
    features_artist = []
    features_id = []

    for track in playlist:
        for i in range(len(track['relationships']['tracks']['data'])):
            title = track['relationships']['tracks']['data'][i]['attributes']['name']
            artist = track['relationships']['tracks']['data'][i]['attributes']['artistName']
            id = track['relationships']['tracks']['data'][i]['id']

            features_title.append(title)
            features_artist.append(artist)
            features_id.append(id)

    df = pd.DataFrame({
        'title': features_title,
        'artist': features_artist,
        'id': features_id
    })

    return df

# Get artist info


def artist_info(lookup):

    try:
        artist = sp.search(lookup)
        artist_uri = artist['tracks']['items'][0]['album']['artists'][0]['uri']
        track_uri = artist['tracks']['items'][0]['uri']

        available_markets = len(
            artist['tracks']['items'][0]['available_markets'])
        release_date = artist['tracks']['items'][0]['album']['release_date']

        artist = sp.artist(artist_uri)
        total_followers = artist['followers']['total']
        genres = artist['genres']
        popularity = artist['popularity']

        audio_features = sp.audio_features(track_uri)[0]

        acousticness = audio_features['acousticness']
        danceability = audio_features['danceability']
        duration_ms = audio_features['duration_ms']
        energy = audio_features['energy']
        instrumentalness = audio_features['instrumentalness']
        key = audio_features['key']
        liveness = audio_features['liveness']
        loudness = audio_features['loudness']
        speechiness = audio_features['speechiness']
        tempo = audio_features['tempo']
        time_signature = audio_features['time_signature']
        valence = audio_features['valence']

        return available_markets, release_date, total_followers, genres, popularity, acousticness, danceability, duration_ms, energy, instrumentalness, key, liveness, loudness, speechiness, tempo, time_signature, valence

    except:
        return [None]*17


def DataExtractor(conn_string):
    # Use function am_playlist to get top chart
    top_chart_1 = am_playlist('pl.d25f5d1181894928af76c85c967f8f31')
    top_chart_2 = am_playlist('pl.abe8ba42278f4ef490e3a9fc5ec8e8c5')
    top_chart_3 = am_playlist('pl.b7ae3e0a28e84c5c96c4284b6a6c70af')
    top_chart_4 = am_playlist('pl.5ee8333dbe944d9f9151e97d92d1ead9')
    top_chart_5 = am_playlist('pl.78ee237116dc46f1a5a3d8916b4a28fc')
    top_chart_6 = am_playlist('pl.0d70b7c9be8e4e0b95ebbf5578aaf7a2')
    top_chart_7 = am_playlist('pl.91f43a875d694022ba4b338fce6a8c48')
    top_chart_8 = am_playlist('pl.f4d106fed2bd41149aaacabb233eb5eb')
    top_chart_9 = am_playlist('pl.e5afef10eb2544d0a38880fd8e2c9a27')
    top_chart_10 = am_playlist('pl.b82441d4e0c0435ca7383f7ea1172349')

    top_chart = pd.concat([top_chart_1, top_chart_2, top_chart_3, top_chart_4, top_chart_5,
                          top_chart_6, top_chart_7, top_chart_8, top_chart_9, top_chart_10], ignore_index=True)

    top_chart = top_chart.drop_duplicates(subset=['id'], keep="first")

    # Pass artist to function artist_info
    top_chart['lookup'] = top_chart['title'] + " " + top_chart['artist']
    top_chart['available_markets'], top_chart['release_date'], top_chart['total_followers'], top_chart['genres'], top_chart['popularity'], top_chart['acousticness'], top_chart['danceability'], top_chart['duration_ms'], top_chart['energy'], top_chart[
        'instrumentalness'], top_chart['key'], top_chart['liveness'], top_chart['loudness'], top_chart['speechiness'], top_chart['tempo'], top_chart['time_signature'], top_chart['valence'] = zip(*top_chart['lookup'].map(artist_info))
    # top_chart.to_csv('Mantap.csv')
    top_chart.rename({'_': 'num', 'id': 'track_id',
                      'mode': 'music_mode', 'key': 'music_key'}, axis=1, inplace=True)

    db = create_engine(conn_string)
    conn = db.connect()

    top_chart.to_sql("extractor", con=conn, if_exists='append', index=False)

    conn = psycopg2.connect(conn_string)
    conn.autocommit = True

    print(top_chart)


# DAG ================================================================================

conn = 'postgresql://airflow:airflow@host.docker.internal/etl_pipeline'

default_args = {
    'owner': 'Tugas_RekDat',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id="ETL_Pipeline",
    default_args=default_args,
    description="Tugas Rekayasa Data",
    start_date=datetime(2022, 11, 27),
    schedule=" 0 0 * * * ",
    catchup=False
) as dag:

    Extracter = PythonOperator(
        task_id='Extract-dataAndTransform',
        python_callable=DataExtractor,
        op_kwargs={
            'conn_string': conn
        }        # python_callable=testing
    )

    Transforming = PostgresOperator(
        task_id='Transform-dataChecking',
        postgres_conn_id='ETLid',
        sql="""
            DELETE FROM extractor WHERE artist IS NULL;
            DELETE FROM extractor a USING (
            SELECT MIN(ctid) as ctid, track_id
                FROM extractor 
                GROUP BY track_id HAVING COUNT(*) > 1
            ) b
            WHERE a.track_id = b.track_id 
            AND a.ctid <> b.ctid
        """
    )

    LOAD = PostgresOperator(
        task_id='Load-neededData',
        postgres_conn_id='ETLid',
        sql="""
            INSERT INTO clean_data(artist, title, genres, release_date)
            SELECT artist, title, genres, release_date FROM extractor ON CONLIFT DO NOTHING ;
        
        """
    )

    EXPORT = PostgresOperator(
        task_id='Export-toCSV',
        postgres_conn_id='ETLid',
        sql="""
            COPY clean_data TO 'D:\Codes\Airflow\data\Dataset.csv'  WITH DELIMITER ',' CSV HEADER;
        """
    )

    Extracter >> Transforming >> LOAD >> EXPORT
