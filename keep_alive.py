from flask import Flask, jsonify
import os
from threading import Thread

# Lazy imports inside function to avoid circulars during early startup

def build_telemetry_snapshot():
    try:
        from soundcloud_utils import get_soundcloud_telemetry_snapshot, get_soundcloud_key_status
        from spotify_utils import get_spotify_key_status
        from database_utils import get_all_artists
        sc_tel = get_soundcloud_telemetry_snapshot()
        sc_keys = get_soundcloud_key_status()
        sp_keys = get_spotify_key_status()
        artists = get_all_artists()
        total = len(artists)
        spotify_ct = sum(1 for a in artists if a.get('platform') == 'spotify')
        sc_ct = sum(1 for a in artists if a.get('platform') == 'soundcloud')
        return {
            'ok': True,
            'spotify': sp_keys,
            'soundcloud': {
                'keys': sc_keys,
                'telemetry': sc_tel
            },
            'artists': {
                'total': total,
                'spotify': spotify_ct,
                'soundcloud': sc_ct
            }
        }
    except Exception as e:
        return {'ok': False, 'error': str(e)}

app = Flask('')

@app.route('/')
def home():
    return "Bot is running!"

@app.route('/telemetry')
def telemetry():
    return jsonify(build_telemetry_snapshot())

def run():
    app.run(host='0.0.0.0', port=8080)

def keep_alive():
    t = Thread(target=run)
    t.start()
