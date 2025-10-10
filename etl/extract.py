import os 
import json 
import spotipy
from dotenv import load_dotenv
from datetime import datetime
from spotipy.oauth2 import SpotifyOAuth

load_dotenv()


def date_time():
    date = datetime.now()
    return date.strftime(f'%Y-%m-%d')

def spotify_connection(scope):
    sp = spotipy.Spotify(auth_manager=SpotifyOAuth(
        client_id= os.getenv("SPOTIPY_CLIENT_ID"),
        client_secret= os.getenv("SPOTIPY_CLIENT_SECRET"),
        redirect_uri= os.getenv("REDIRECT_URI"),
        scope= scope
    ))
    return sp

def recently_played(scope):
    today = date_time()
    sp = spotify_connection(scope)
    results = sp.current_user_recently_played()
    os.makedirs('./data/raw/recently_played', exist_ok=True)
    with open(f'./data/raw/recently_played/{today}_recently_played.json', 'w') as file:
        file.write(json.dumps(results, file, indent=2))

def top_tracks(scope):
    today = date_time()
    sp = spotify_connection(scope)
    results = sp.current_user_top_tracks()
    os.makedirs('./data/raw/top_tracks', exist_ok=True)
    with open(f'./data/raw/top_tracks/{today}_top_tracks.json', 'w') as file:
        file.write(json.dumps(results, file, indent=2))

def top_artists(scope):
    today = date_time()
    sp = spotify_connection(scope)
    results = sp.current_user_top_artists()
    os.makedirs('./data/raw/top_artists', exist_ok=True)
    with open(f'./data/raw/top_artists/{today}_top_artists.json', 'w') as file:
        file.write(json.dumps(results, file, indent=2))
