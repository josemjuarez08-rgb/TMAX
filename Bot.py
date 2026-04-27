import requests
import datetime
import os

# --- Configuration ---
# Coordinates for San Francisco International Airport (SFO)
LAT = 37.6191
LON = -122.3750
# SFO NWS Weather Station (Matches Weather Underground KSFO History)
STATION_ID = "KSFO" 
# Your Discord Webhook URL (passed securely from GitHub)
WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK")

# --- 1. Get Forecast Data (Open-Meteo) ---
def get_forecast():
    # Pulls the free, high-resolution hourly forecast for SFO
    url = f"https://api.open-meteo.com/v1/forecast?latitude={LAT}&longitude={LON}&hourly=temperature_2m&temperature_unit=fahrenheit&timezone=America%2FLos_Angeles"
    response = requests.get(url).json()
    
    # Matches the forecast to the current hour
    current_hour = datetime.datetime.now().hour
    temps = response["hourly"]["temperature_2m"][:24] 
    return temps[current_hour]

# --- 2. Get Real-Time Ground Truth (NWS API / Wunderground Source) ---
def get_actual():
    # Pulls the exact temperature right now at KSFO
    url = f"https://api.weather.gov/stations/{STATION_ID}/observations/latest"
    headers = {"User-Agent": "TMAX Signal Bot"}
    response = requests.get(url, headers=headers).json()
    
    # NWS returns Celsius; converting to Fahrenheit
    temp_c = response["properties"]["temperature"]["value"]
    if temp_c is None:
        return None
    temp_f = (temp_c * 9/5) + 32
    return round(temp_f, 1)

# --- 3. Execute Delta Strategy ---
def main():
    try:
        forecast_temp = get_forecast()
        actual_temp = get_actual()
        
        if actual_temp is None:
            print("No real-time data available from SFO right now.")
            return

        # Calculate the edge
        delta = actual_temp - forecast_temp
        print(f"Forecast: {forecast_temp}°F | Actual: {actual_temp}°F | Delta: {round(delta, 1)}°F")
        
        # Signal Generation: If the actual temperature is heating up 2+ degrees faster than forecasted
        if delta >= 2.0:
            msg = f"🔥 **SFO TMAX SIGNAL** 🔥\nReal-time temp at KSFO is overshooting the forecast!\nActual: **{actual_temp}°F**\nForecast: **{forecast_temp}°F**\nDelta: **+{round(delta, 1)}°F**\n*Tracking matches Weather Underground history.*"
            
            if WEBHOOK_URL:
                requests.post(WEBHOOK_URL, json={"content": msg})
                print("Signal sent to Discord!")
            else:
                print("Signal triggered, but Discord webhook is missing.")
    except Exception as e:
        print(f"Error running bot: {e}")

if __name__ == "__main__":
    main()
