import requests
import datetime
import os
import csv
import json
import zoneinfo
 
# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
LAT = 37.6191
LON = -122.3750
STATION_ID = "KSFO"
WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK")
LOG_FILE = "tmax_log.csv"
LAST_SIGNAL_FILE = "last_signal.json"   # tracks last alert to prevent duplicates
TIMEZONE = zoneinfo.ZoneInfo("America/Los_Angeles")
 
# --- Trading window (PT) ---
# Only fire signals when you can actually act on them.
# SFO temps peak 1–4 PM PT. Morning signals (9–11 AM) have the most edge
# because the market hasn't priced in the day's trajectory yet.
SIGNAL_WINDOW_START_HOUR = 9    # 9 AM PT
SIGNAL_WINDOW_END_HOUR = 15     # 3 PM PT
 
# --- Contract threshold ---
# SET THIS to match the exact Robinhood contract strike for today.
# e.g. if the market is "Will SFO high exceed 60°F?" set this to 60.
# Override via GitHub Actions env var so you don't touch code each day.
CONTRACT_THRESHOLD = int(os.environ.get("CONTRACT_THRESHOLD", "60"))
 
# Fire signal when projected high is within this many degrees of threshold
SIGNAL_PROXIMITY = 2.0
 
# Minimum change in daily max (°F) before re-alerting — prevents spam
DEDUP_MIN_CHANGE = 1.0
 
 
# ---------------------------------------------------------------------------
# 1. Get all today's observations (LST-aware window)
# ---------------------------------------------------------------------------
def get_todays_observations():
    """
    Returns list of (timestamp_utc, temp_f) for all KSFO obs since midnight LST.
 
    CRITICAL: NWS Daily Climate Reports use LOCAL STANDARD TIME year-round.
    Midnight LST = 08:00 UTC always, even during DST (Mar–Nov).
    During DST this means the day starts at 1:00 AM PDT — not midnight PDT.
    """
    now_utc = datetime.datetime.now(datetime.timezone.utc)
    window_start = now_utc.replace(hour=8, minute=0, second=0, microsecond=0)
    if now_utc < window_start:
        window_start -= datetime.timedelta(days=1)
 
    url = f"https://api.weather.gov/stations/{STATION_ID}/observations"
    headers = {"User-Agent": "TMAX Signal Bot"}
    params = {"start": window_start.strftime("%Y-%m-%dT%H:%M:%SZ"), "limit": 100}
 
    response = requests.get(url, headers=headers, params=params, timeout=10).json()
 
    obs = []
    for feature in response.get("features", []):
        props = feature["properties"]
        temp_c = props["temperature"]["value"]
        ts = props.get("timestamp", "")
        if temp_c is None or not ts:
            continue
        temp_f = round((temp_c * 9 / 5) + 32, 1)
        ts_dt = datetime.datetime.fromisoformat(ts.replace("Z", "+00:00"))
        obs.append((ts_dt, temp_f))
 
    # Sort oldest → newest
    obs.sort(key=lambda x: x[0])
    return obs
 
 
# ---------------------------------------------------------------------------
# 2. Project end-of-day high using rate of warming
# ---------------------------------------------------------------------------
def project_daily_high(obs: list):
    """
    Uses the observed warming rate over the last 2 hours to project
    where the daily high will land by peak time (2 PM PT).
 
    Returns:
        daily_max_so_far (float)
        projected_high (float or None)
        obs_age_minutes (float)
        warming_rate_per_hour (float or None)
    """
    if not obs:
        return None, None, None, None
 
    now_utc = datetime.datetime.now(datetime.timezone.utc)
    daily_max = max(t for _, t in obs)
 
    # Obs age = how old is the most recent reading
    latest_ts, latest_temp = obs[-1]
    obs_age_minutes = (now_utc - latest_ts).total_seconds() / 60
 
    # Warming rate: compare last reading to one from ~2 hours ago
    projected_high = None
    warming_rate = None
 
    two_hours_ago = now_utc - datetime.timedelta(hours=2)
    earlier_obs = [(ts, t) for ts, t in obs if ts <= two_hours_ago]
 
    if earlier_obs:
        ref_ts, ref_temp = earlier_obs[-1]
        elapsed_hours = (latest_ts - ref_ts).total_seconds() / 3600
        if elapsed_hours > 0:
            warming_rate = (latest_temp - ref_temp) / elapsed_hours
 
            # Project to 2 PM PT (peak SFO temp time)
            now_local = datetime.datetime.now(TIMEZONE)
            peak_local = now_local.replace(hour=14, minute=0, second=0, microsecond=0)
            hours_to_peak = (peak_local - now_local).total_seconds() / 3600
 
            if hours_to_peak > 0 and warming_rate > 0:
                projected_high = round(latest_temp + (warming_rate * hours_to_peak), 1)
            else:
                # Past peak — projection is just the current max
                projected_high = daily_max
 
    return daily_max, projected_high, obs_age_minutes, warming_rate
 
 
# ---------------------------------------------------------------------------
# 3. NWS official forecast high (cross-reference only)
# ---------------------------------------------------------------------------
def get_nws_forecast_high():
    headers = {"User-Agent": "TMAX Signal Bot"}
    grid = requests.get(
        f"https://api.weather.gov/points/{LAT},{LON}",
        headers=headers, timeout=10
    ).json()
    forecast_url = grid["properties"]["forecast"]
    forecast = requests.get(forecast_url, headers=headers, timeout=10).json()
    for period in forecast["properties"]["periods"][:3]:
        if period["isDaytime"]:
            return period["temperature"]
    return None
 
 
# ---------------------------------------------------------------------------
# 4. Deduplication — don't re-fire if nothing meaningful changed
# ---------------------------------------------------------------------------
def load_last_signal():
    if os.path.isfile(LAST_SIGNAL_FILE):
        with open(LAST_SIGNAL_FILE) as f:
            return json.load(f)
    return {"daily_max": None, "date": None}
 
 
def save_last_signal(daily_max):
    today = datetime.datetime.now(TIMEZONE).date().isoformat()
    with open(LAST_SIGNAL_FILE, "w") as f:
        json.dump({"daily_max": daily_max, "date": today}, f)
 
 
def should_fire(daily_max):
    """Returns True only if daily_max has moved enough since last alert."""
    last = load_last_signal()
    today = datetime.datetime.now(TIMEZONE).date().isoformat()
 
    # Reset dedup each day
    if last["date"] != today:
        return True
 
    if last["daily_max"] is None:
        return True
 
    return abs(daily_max - last["daily_max"]) >= DEDUP_MIN_CHANGE
 
 
# ---------------------------------------------------------------------------
# 5. Trading window gate
# ---------------------------------------------------------------------------
def in_trading_window():
    now_local = datetime.datetime.now(TIMEZONE)
    return SIGNAL_WINDOW_START_HOUR <= now_local.hour < SIGNAL_WINDOW_END_HOUR
 
 
# ---------------------------------------------------------------------------
# 6. CSV logging
# ---------------------------------------------------------------------------
def log_run(data: dict):
    file_exists = os.path.isfile(LOG_FILE)
    fieldnames = [
        "timestamp", "contract_threshold",
        "nws_forecast_high", "daily_max_so_far",
        "projected_high", "warming_rate_per_hour",
        "obs_age_minutes", "in_window",
        "signal_triggered", "notes",
    ]
    with open(LOG_FILE, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        writer.writerow(data)
 
 
# ---------------------------------------------------------------------------
# 7. Main
# ---------------------------------------------------------------------------
def main():
    now_local = datetime.datetime.now(TIMEZONE)
    now_str = now_local.isoformat()
    notes = []
 
    try:
        obs = get_todays_observations()
        daily_max, projected_high, obs_age, warming_rate = project_daily_high(obs)
 
        nws_high = None
        try:
            nws_high = get_nws_forecast_high()
        except Exception as e:
            notes.append(f"NWS forecast error: {e}")
 
        window_open = in_trading_window()
 
        if obs_age is not None and obs_age > 45:
            notes.append(f"Stale obs: {obs_age:.0f} min old")
 
        # Signal fires when projected high (or current max) is within
        # SIGNAL_PROXIMITY of the fixed contract threshold
        signal_value = projected_high if projected_high is not None else daily_max
        signal_triggered = (
            signal_value is not None
            and window_open
            and (CONTRACT_THRESHOLD - signal_value) <= SIGNAL_PROXIMITY
            and should_fire(daily_max)
        )
 
        # --- Console output ---
        print(f"\n{'='*58}")
        print(f"  TMAX Bot  |  {now_str}")
        print(f"{'='*58}")
        print(f"  Contract threshold   : >{CONTRACT_THRESHOLD}°F")
        print(f"  NWS forecast high    : {nws_high}°F  (reference only)")
        print(f"  Daily max so far     : {daily_max}°F")
        print(f"  Projected day high   : {projected_high}°F")
        print(f"  Warming rate         : {f'{warming_rate:+.1f}°F/hr' if warming_rate is not None else '?'}")
        print(f"  Latest obs age       : {f'{obs_age:.0f} min' if obs_age is not None else '?'}")
        print(f"  Trading window open  : {'YES' if window_open else 'NO'} ({SIGNAL_WINDOW_START_HOUR} AM–{SIGNAL_WINDOW_END_HOUR-12} PM PT)")
        print(f"  Signal               : {'🔴 YES' if signal_triggered else '⚪ no'}")
        if notes:
            print(f"  Notes                : {'; '.join(notes)}")
        print(f"{'='*58}\n")
 
        # --- Discord alert ---
        if signal_triggered and signal_value is not None:
            gap = CONTRACT_THRESHOLD - signal_value
            gap_str = f"already {abs(gap):.1f}°F above" if gap < 0 else f"{gap:.1f}°F below"
            rate_str = f"{warming_rate:+.1f}°F/hr" if warming_rate is not None else "unknown"
            msg = (
                f"🎯 **SFO TMAX SIGNAL** — Contract >{CONTRACT_THRESHOLD}°F\n"
                f"**Daily max so far:** {daily_max}°F\n"
                f"**Projected day high:** {projected_high}°F ({gap_str} threshold)\n"
                f"**Warming rate:** {rate_str}\n"
                f"**NWS forecast:** {nws_high}°F\n"
                f"**Obs age:** {f'{obs_age:.0f} min' if obs_age is not None else '?'}\n"
                f"_Window: {SIGNAL_WINDOW_START_HOUR} AM–{SIGNAL_WINDOW_END_HOUR-12} PM PT | "
                f"Settlement: NWS CLI report ~6 AM PT tomorrow_"
            )
            if WEBHOOK_URL:
                requests.post(WEBHOOK_URL, json={"content": msg}, timeout=10)
                print("Signal sent to Discord.")
                save_last_signal(daily_max)
            else:
                print("DISCORD_WEBHOOK not set.")
 
        log_run({
            "timestamp": now_str,
            "contract_threshold": CONTRACT_THRESHOLD,
            "nws_forecast_high": nws_high if nws_high is not None else "",
            "daily_max_so_far": daily_max if daily_max is not None else "",
            "projected_high": projected_high if projected_high is not None else "",
            "warming_rate_per_hour": round(warming_rate, 2) if warming_rate is not None else "",
            "obs_age_minutes": round(obs_age, 1) if obs_age is not None else "",
            "in_window": window_open,
            "signal_triggered": signal_triggered,
            "notes": "; ".join(notes),
        })
 
    except Exception as e:
        print(f"Fatal error: {e}")
        log_run({
            "timestamp": now_str,
            "contract_threshold": CONTRACT_THRESHOLD,
            "nws_forecast_high": "", "daily_max_so_far": "",
            "projected_high": "", "warming_rate_per_hour": "",
            "obs_age_minutes": "", "in_window": False,
            "signal_triggered": False, "notes": f"Fatal: {e}",
        })
 
 
if __name__ == "__main__":
    main()
