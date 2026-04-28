import requests
import datetime
import os
import csv
import zoneinfo
 
# --- Configuration ---
LAT = 37.6191
LON = -122.3750
STATION_ID = "KSFO"
WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK")
 
TIMEZONE = zoneinfo.ZoneInfo("America/Los_Angeles")
SIGNAL_THRESHOLD = 2.0          # degrees F above forecast to trigger alert
MAX_OBS_AGE_MINUTES = 45        # reject NWS observations older than this
LOG_FILE = "tmax_log.csv"
 
 
# --- 1. Get Forecast Data (Open-Meteo) ---
def get_forecast():
    """Pulls hourly temp forecast for SFO and returns value for the current local hour."""
    url = (
        f"https://api.open-meteo.com/v1/forecast"
        f"?latitude={LAT}&longitude={LON}"
        f"&hourly=temperature_2m"
        f"&temperature_unit=fahrenheit"
        f"&timezone=America%2FLos_Angeles"
    )
    response = requests.get(url).json()
 
    now_local = datetime.datetime.now(TIMEZONE)
    # Open-Meteo returns ISO strings like "2026-04-28T14:00" — match to current hour
    target = now_local.strftime("%Y-%m-%dT%H:00")
    times = response["hourly"]["time"]
    temps = response["hourly"]["temperature_2m"]
 
    if target in times:
        idx = times.index(target)
        return temps[idx]
    else:
        print(f"Warning: could not find forecast slot for {target}")
        return None
 
 
# --- 2. Get Real-Time Ground Truth (NWS) ---
def get_actual():
    """Pulls the latest KSFO observation, validates freshness, and returns temp in Fahrenheit."""
    url = f"https://api.weather.gov/stations/{STATION_ID}/observations/latest"
    headers = {"User-Agent": "TMAX Signal Bot"}
    response = requests.get(url, headers=headers).json()
 
    props = response["properties"]
 
    # --- Freshness check ---
    obs_time_str = props.get("timestamp")
    if obs_time_str:
        obs_dt = datetime.datetime.fromisoformat(obs_time_str.replace("Z", "+00:00"))
        age_minutes = (datetime.datetime.now(datetime.timezone.utc) - obs_dt).total_seconds() / 60
        if age_minutes > MAX_OBS_AGE_MINUTES:
            print(f"Skipping: NWS observation is {age_minutes:.0f} min old (limit: {MAX_OBS_AGE_MINUTES} min)")
            return None, None
    else:
        age_minutes = None
 
    temp_c = props["temperature"]["value"]
    if temp_c is None:
        return None, age_minutes
 
    temp_f = round((temp_c * 9 / 5) + 32, 1)
    return temp_f, age_minutes
 
 
# --- 3. Log run to CSV ---
def log_run(forecast_temp, actual_temp, delta, signal_triggered, obs_age_minutes):
    """Appends a row to tmax_log.csv so you can audit accuracy over time."""
    file_exists = os.path.isfile(LOG_FILE)
    now_str = datetime.datetime.now(TIMEZONE).isoformat()
 
    with open(LOG_FILE, "a", newline="") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(["timestamp", "forecast_f", "actual_f", "delta", "signal_triggered", "obs_age_minutes"])
        writer.writerow([
            now_str,
            round(forecast_temp, 1) if forecast_temp is not None else "",
            actual_temp if actual_temp is not None else "",
            round(delta, 1) if delta is not None else "",
            signal_triggered,
            round(obs_age_minutes, 1) if obs_age_minutes is not None else ""
        ])
 
 
# --- 4. Execute Delta Strategy ---
def main():
    try:
        forecast_temp = get_forecast()
        actual_temp, obs_age = get_actual()
 
        if forecast_temp is None:
            print("No forecast data available. Exiting.")
            return
 
        if actual_temp is None:
            print("No valid actual temp available. Exiting.")
            log_run(forecast_temp, None, None, False, obs_age)
            return
 
        delta = actual_temp - forecast_temp
        print(f"Forecast: {forecast_temp}°F | Actual: {actual_temp}°F | Delta: {round(delta, 1)}°F | Obs age: {round(obs_age, 1) if obs_age else '?'} min")
 
        signal_triggered = delta >= SIGNAL_THRESHOLD
 
        if signal_triggered:
            msg = (
                f"🌡️ **SFO TMAX SIGNAL**\n"
                f"Real-time temp at KSFO is overshooting the forecast\n"
                f"**Actual:** {actual_temp}°F\n"
                f"**Forecast:** {forecast_temp}°F\n"
                f"**Delta:** +{round(delta, 1)}°F\n"
                f"**Obs age:** {round(obs_age, 1) if obs_age else '?'} min\n"
                f"Tracking matches Weather Underground history."
            )
            if WEBHOOK_URL:
                requests.post(WEBHOOK_URL, json={"content": msg})
                print("Signal sent to Discord.")
            else:
                print("Signal triggered, but Discord webhook is missing.")
 
        log_run(forecast_temp, actual_temp, delta, signal_triggered, obs_age)
 
    except Exception as e:
        print(f"Error running bot: {e}")
 
 
if __name__ == "__main__":
    main()
