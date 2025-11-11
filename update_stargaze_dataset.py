# update_stargaze_dataset.py
# Simple script to fetch 7-day hourly forecast from Open-Meteo,
# compute sun/moon features with Skyfield, and save/merge to CSV.
# Edit LAT and LON below to your observing location before first run.

import requests, os, math
from datetime import datetime, timedelta, timezone
import pandas as pd
from skyfield.api import load, Topos
from skyfield import almanac

# ===== CONFIGURE YOUR LOCATION =====
LAT, LON = 16.0471, 108.2068   # change to your location (latitude, longitude)
FORECAST_DAYS = 7
CSV_PATH = "stargaze_dataset.csv"
# ==================================

def fetch_open_meteo_hourly(lat, lon, days=7):
    start = datetime.now(timezone.utc)
    end = start + timedelta(days=days)
    start_iso = start.replace(minute=0, second=0, microsecond=0).isoformat()
    end_iso = end.isoformat()
    url = (
        "https://api.open-meteo.com/v1/forecast?"
        f"latitude={lat}&longitude={lon}&hourly=cloudcover,precipitation_probability,visibility,temperature_2m,windspeed_10m"
        f"&timezone=UTC&start_date={start_iso[:10]}&end_date={end_iso[:10]}"
    )
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    data = r.json()['hourly']
    df = pd.DataFrame({
        'datetime_utc': pd.to_datetime(data['time']),
        'cloud': data['cloudcover'],
        'precip_prob': data['precipitation_probability'],
        'visibility_m': data['visibility'],
        'temp_c': data['temperature_2m'],
        'wind_m_s': data['windspeed_10m']
    })
    return df

def add_astronomy_features(df, lat, lon):
    ts = load.timescale()
    eph = load('de421.bsp')
    location = Topos(latitude_degrees=lat, longitude_degrees=lon)
    sun = eph['sun']
    moon = eph['moon']
    sun_alts, moon_illums, moon_alts = [], [], []
    for t_utc in df['datetime_utc']:
        t = ts.utc(t_utc.year, t_utc.month, t_utc.day, t_utc.hour, t_utc.minute, t_utc.second)
        astrometric = (eph['earth'] + location).at(t)
        sun_alt = astrometric.observe(sun).apparent().altaz()[0].degrees
        moon_alt = astrometric.observe(moon).apparent().altaz()[0].degrees
        moon_phase = almanac.phase(eph, t)
        illum = (1 + math.cos(moon_phase)) / 2
        sun_alts.append(sun_alt)
        moon_alts.append(moon_alt)
        moon_illums.append(illum)
    df = df.copy()
    df['sun_alt_deg'] = sun_alts
    df['moon_alt_deg'] = moon_alts
    df['moon_illum'] = moon_illums
    df['is_night'] = df['sun_alt_deg'] <= -12.0
    df['visibility_km'] = df['visibility_m'] / 1000.0
    return df

def load_existing(path):
    if os.path.exists(path):
        return pd.read_csv(path, parse_dates=['datetime_utc'])
    return None

def save_merged(path, new_df):
    existing = load_existing(path)
    if existing is None:
        out = new_df
    else:
        combined = pd.concat([existing, new_df], ignore_index=True)
        combined = combined.sort_values('datetime_utc').drop_duplicates(subset=['datetime_utc'], keep='last')
        out = combined.reset_index(drop=True)
    out.to_csv(path, index=False)
    print("Saved", len(out), "rows to", path)

if __name__ == "__main__":
    print("Fetching forecast...")
    df = fetch_open_meteo_hourly(LAT, LON, FORECAST_DAYS)
    print(f"Fetched {len(df)} hourly rows.")
    print("Adding astronomy features...")
    df2 = add_astronomy_features(df, LAT, LON)
    print("Merging and saving CSV...")
    save_merged(CSV_PATH, df2)
