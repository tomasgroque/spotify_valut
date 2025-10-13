import os 
import time
import json
import pyarrow
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv

# PATH CONFIG
RAW_PATH = "./data/raw"
PROCESSED_PATH = "./data/processed"

def load_json_files():
    # Iterate over JSON files in RAW_PATH folders and yield loaded data.
    for subfolder in ['recently_played', 'top_tracks', 'top_artists']:
        folder_path = os.path.join(RAW_PATH, subfolder)
        for file_name in os.listdir(folder_path):
            if file_name.endswith(".json"):
                full_path = os.path.join(folder_path, file_name)
                with open(full_path, 'r') as f:
                    data = json.load(f)

                yield subfolder, data

def flatten_recently_played(json_data):
    # Extract track, artist, and play info from recently_played structure.
    plays, tracks, artists = [], [], []

    for item in json_data.get('items', []):
        track = item['track']
        primary_artist = track['artists'][0]

        plays.append({
            "track_id": track['id'],
            "played_at": item['played_at'],
            "duration_ms": track['duration_ms'],
            "context": item.get('context', {}).get('type') if item.get('context') else None
        })

        tracks.append({
            "track_id": track['id'],
            "name": track['name'],
            "album": track["album"]["name"],
            "artist_id": primary_artist['id']
        })

        artists.append({
            "artist_id": primary_artist['id'],
            "name": primary_artist['name']
        })

    return(
        pd.DataFrame(plays).drop_duplicates(),
        pd.DataFrame(tracks).drop_duplicates(),
        pd.DataFrame(artists).drop_duplicates()
    )
    

def flatten_top_tracks(json_data):
    # Flatten data from top_tracks endpoint.
    tracks, artists = [], []

    for track in json_data.get('items', []):
        primary_artist = track['artists'][0]

        tracks.append({
            "track_id": track['id'],
            "name": track['name'],
            "album": track["album"]["name"],
            "artist_id": primary_artist['id'],
            "duration_ms": track["duration_ms"],
            "popularity": track["popularity"],
            "explicit": track["explicit"]
        })

        artists.append({
            "atist_id": primary_artist['id'],
            "name": primary_artist['name']
        })
    
    return(
        pd.DataFrame(tracks).drop_duplicates(),
        pd.DataFrame(artists).drop_duplicates()
    )

def flatten_top_artists(json_data):
    artists = []

    for artist in json_data.get('items', []):
        artists.append({
            'artist_id': artist['id'],
            'name': artist['name'],
            'genres': ",".join(artist.get("genres", [])),
            "popularity": artist["popularity"],
            "followers": artist["followers"]["total"],
            "url": artist["external_urls"]["spotify"]
        })
    
    return pd.DataFrame(artists).drop_duplicates()


def clean_data(df_tracks, df_artists, df_plays):
    # Normalize column names 
    for df in [df_tracks, df_artists, df_plays]:
        df.columns = df.columns.str.lower().str.strip()
    
    # Fill nulls / handle missing data
    if 'genres' in df_artists.columns:
        df_artists['genres'] = df_artists['genres'].fillna('unknown')
    if 'context' in df_plays.columns:
        df_plays['context'] = df_plays['context'].fillna('unknown')

    # Convert timestamps & durations
    if 'played_at' in df_plays.columns:
        df_plays['played_at'] = pd.to_datetime(df_plays['played_at'], errors='coerce')
    if 'duration_ms' in df_plays.columns:
        df_plays['duration_s'] = df_plays['duration_ms'] / 1000

    # Drop invalid or missing IDs    
    df_tracks = df_tracks.dropna(subset=['track_id'])
    df_artists = df_artists.dropna(subset=['artist_id'])
    df_plays = df_plays.dropna(subset=['track_id', 'played_at'])

    # Final deduplication just in case
    df_tracks = df_tracks.drop_duplicates(subset=['track_id'])
    df_artists = df_artists.drop_duplicates(subset=['artist_id'])
    df_plays = df_plays.drop_cuplicates(subset=['track_id', 'played_at'])

    return df_tracks, df_artists, df_plays

def save_output(df_tracks, df_artists, df_plays):
    os.makedirs(PROCESSED_PATH, exist_ok=True)
    benchmarks = []

    datasets = {
        'tracks': df_tracks,
        'artists': df_artists,
        'plays': df_plays
    }

    for name, df in datasets.items():
        # CSV
        start = time.time()
        csv_path = f"{PROCESSED_PATH}/{name}.csv"
        df.to_csv(csv_path, index=False)
        csv_time = round(time.time() - start, 4)
        csv_size = os.path.getsize(csv_path) / 1024

        # Parquet
        start = time.time()
        pq_path = f"{PROCESSED_PATH}/{name}.parquet"
        df.to_parquet(pq_path, index=False)
        pq_time = round(time.time() - start, 4)
        pq_size = os.path.getsize(pq_path) / 1024

        benchmarks.append({
            'datset': name,
            'csv_time_s': csv_time,
            'csv_size_kb': csv_size,
            'parquet_time_s': pq_time,
            'parquet_size_kb': pq_size
        })

    # Save benchmark results
    with open(f"{PROCESSED_PATH}/benchmarks.txt", "w") as f:
        for b in benchmarks:
            f.write(str(b) + "\n") 

    return benchmarks