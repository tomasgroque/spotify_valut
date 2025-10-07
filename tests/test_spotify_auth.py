import os 
import json
import spotipy
from dotenv import load_dotenv
from spotipy.oauth2 import SpotifyOAuth

load_dotenv()

scope = 'user-read-recently-played user-top-read'

sp = spotipy.Spotify(auth_manager=SpotifyOAuth(
    client_id= os.getenv("SPOTIPY_CLIENT_ID "),
    client_secret= os.getenv("SPOTIPY_CLIENT_SECRET"),
    redirect_uri= os.getenv("SPOTIPY_REDIRECT_URI"),
    scope= scope
))

results = sp.current_user_recently_played(limit=5)

#Locating songs manually
'''
print("\nYour last 5 played tracks:")
print(results.keys())
first = results['items'][0]
print(first.keys())
track = first['track']
print(track.keys())
print(track['name'])
print(track['artists'][0]['name'])
print(track['album']['name'])
print(first['played_at'])
'''

# formatting the data into JSON
# print(json.dumps(results['items'][0], indent=2))
for item in results['items']:
    track = item.get('track', {})
    print(f"{track.get('name')} — {track.get('artists',[{}])[0].get('name')} at {item.get('played_at')}")