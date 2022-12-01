FROM apache/airflow:2.4.3
RUN pip install --user --upgrade pip
RUN pip install spotipy
RUN pip install apple-music-python