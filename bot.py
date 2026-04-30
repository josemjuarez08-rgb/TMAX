import requests
import datetime
import os
import csv
import json
import zoneinfo
import re

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
LAT = 37.6191
LON = -122.3750
STATION_ID = "KSFO"
WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK")
LOG_FILE = "tmax_log.csv"
LAST_SIGNAL_FILE = "last_signal.json"
TIMEZONE = zoneinfo.ZoneInfo("America/Los_Angeles")

# ---------------------------------------------------------------------------
# 0. Fetch Live Strikes from Kalshi (Robinhood's Backend Provider)
# ---------------------------------------------------------------------------
def get_live_thresholds():
    """
    Dynamically fetches today's live SFO strikes from Kalshi's public API.
    SFO High Temp Event Tickers are formatted as: KXHIGHTSFO-YYMMM DD
    Example: KXHIGHTSFO-26APR30
    """
    today = datetime.datetime.now(TIMEZONE).date()
    # Format date to match Kalshi's ticker logic (e.g., 26APR30)
    date_str = today.strftime("%y%b%d").upper() 
    event_ticker = f"KXHIGHTSFO-{date_str}"
    
    url = "https://trading-api.kalshi.com/trade-api/v2/markets"
    params = {"event_ticker": event_ticker}
    headers = {"accept": "application/json"}
    
    try:
        response = requests.get(url, params=params, headers=headers, timeout=10)
        if response.status_code != 200:
            print(f"API Error fetching live markets: {response.status_code}")
            return []
            
        markets = response.json().get("markets", [])
        strikes = []
        
        for m in markets:
            subtitle = m.get("subtitle", "")
            # Subtitles look like "65° to 66°" or "67° or above".
            # We extract the first integer. We skip "below" contracts because 
            # Robinhood lists them as "Over X" binary options.
            match = re.search(r"(\d+)", subtitle)
            if match and "below" not in subtitle.lower():
                strikes.append(int(match.group(1)))
                
        # Return a clean, sorted list from lowest to highest
        return sorted(list(set(strikes)))
    
    except Exception as e:
        print(f"Fatal error fetching thresholds: {e}")
        return []

AVAILABLE_THRESHOLDS = get_live_thresholds()

# Trading window: only fire actionable signals during this range (PT)
SIGNAL_WINDOW_START_HOUR = 9    # 9 AM PT
SIGNAL_WINDOW_END_HOUR = 15     # 3 PM PT

# Pre-market check window: before this hour, run breach detection instead
PREMARKET_END_HOUR = 9          # anything before 9 AM is pre-market

# Fire when projected/current high is within this many degrees of threshold
SIGNAL_PROXIMITY = 2.0

# Min change in daily max before re-alerting (prevents spam)
DEDUP_MIN_CHANGE = 1.0


# ---------------------------------------------------------------------------
# 1. Fetch today's observations (DST-aware LST window)
# ---------------------------------------------------------------------------
def get_todays_observations():
    """
    Returns sorted list of (timestamp_utc, temp_f) since midnight LST.

    CRITICAL: NWS settlement window uses LOCAL STANDARD TIME year-round.
    Midnight LST = 08:00 UTC always — even during DST (Mar–Nov).
    During DST, this means the day starts at 1:00 AM PDT, not midnight PDT.
    Overnight readings between midnight–1 AM PDT ARE counted by NWS.
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

    obs.sort(key=lambda x: x[0])
    return obs


# ---------------------------------------------------------------------------
# 2. Auto-select the lowest viable contract threshold
# ---------------------------------------------------------------------------
def select_viable_threshold(daily_max: float, thresholds: list):
    """
    Returns the lowest threshold from AVAILABLE_THRESHOLDS that:
      - Has NOT already been breached by overnight/current temps
      - Is still tradeable (there is room to move)

    Returns (viable_threshold, breached_thresholds).
    If all thresholds are breached, returns (None, all_thresholds).
    """
    breached = [t for t in thresholds if daily_max >= t]
    viable = [t for t in thresholds if daily_max < t]
    return (viable[0] if viable else None), breached


# ---------------------------------------------------------------------------
# 3. Project end-of-day high using warming rate
# ---------------------------------------------------------------------------
def project_daily_high(obs: list):
    """
    Uses 2-hour warming rate to project where the daily high will land by
    peak time (2 PM PT). Returns:
        daily_max, projected_high, obs_age_minutes, warming_rate_per_hour
    """
    if not obs:
        return None, None, None, None

    now_utc = datetime.datetime.now(datetime.timezone.utc)
    daily_max = max(t for _, t in obs)
    latest_ts, latest_temp = obs[-1]
    obs_age_minutes = (now_utc - latest_ts).total_seconds() / 60

    projected_high = None
    warming_rate = None

    two_hours_ago = now_utc - datetime.timedelta(hours=2)
    earlier_obs = [(ts, t) for ts, t in obs if ts <= two_hours_ago]

    if earlier_obs:
        ref_ts, ref_temp = earlier_obs[-1]
        elapsed_hours = (latest_ts - ref_ts).total_seconds() / 3600
        if elapsed_hours > 0:
            warming_rate = (latest_temp - ref_temp) / elapsed_hours
            now_local = datetime.datetime.now(TIMEZONE)
            peak_local = now_local.replace(hour=14, minute=0, second=0, microsecond=0)
            hours_to_peak = (peak_local - now_local).total_seconds() / 3600

            if hours_to_peak > 0 and warming_rate > 0:
                projected_high = round(latest_temp + (warming_rate * hours_to_peak), 1)
            else:
                projected_high = daily_max  # past peak, use actual max

    return daily_max, projected_high, obs_age_minutes, warming_rate


# ---------------------------------------------------------------------------
# 4. NWS official forecast high (reference only)
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
# 5. Deduplication
# ---------------------------------------------------------------------------
WARNING_STATE_FILE = "warning_state.json"

def load_warning_state():
    if os.path.isfile(WARNING_STATE_FILE):
        with open(WARNING_STATE_FILE) as f:
            return json.load(f)
    return {"date": None, "highest_breached": None}

def save_warning_state(highest_breached):
    today = datetime.datetime.now(TIMEZONE).date().isoformat()
    with open(WARNING_STATE_FILE, "w") as f:
        json.dump({"date": today, "highest_breached": highest_breached}, f)

def load_last_signal():
    if os.path.isfile(LAST_SIGNAL_FILE):
        with open(LAST_SIGNAL_FILE) as f:
            return json.load(f)
    return {"daily_max": None, "date": None}

def save_last_signal(daily_max, threshold):
    today = datetime.datetime.now(TIMEZONE).date().isoformat()
    with open(LAST_SIGNAL_FILE, "w") as f:
        json.dump({"daily_max": daily_max, "threshold": threshold, "date": today}, f)

def should_fire(daily_max, threshold):
    last = load_last_signal()
    today = datetime.datetime.now(TIMEZONE).date().isoformat()
    if last["date"] != today:
        return True
    if last.get("threshold") != threshold:
        return True  # threshold changed (e.g. bot auto-selected new strike)
    if last["daily_max"] is None:
        return True
    return abs(daily_max - last["daily_max"]) >= DEDUP_MIN_CHANGE


# ---------------------------------------------------------------------------
# 6. CSV logging
# ---------------------------------------------------------------------------
def log_run(data: dict):
    file_exists = os.path.isfile(LOG_FILE)
    fieldnames = [
        "timestamp", "available_thresholds", "selected_threshold",
        "breached_thresholds", "nws_forecast_high",
        "daily_max_so_far", "projected_high", "warming_rate_per_hour",
        "obs_age_minutes", "mode", "signal_triggered", "notes",
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

        if obs_age is not None and obs_age > 45:
            notes.append(f"Stale obs: {obs_age:.0f} min old")

        # --- Auto-select viable threshold ---
        selected_threshold, breached = select_viable_threshold(
            daily_max if daily_max is not None else 0,
            AVAILABLE_THRESHOLDS
        )

        is_premarket = now_local.hour < PREMARKET_END_HOUR
        in_window = SIGNAL_WINDOW_START_HOUR <= now_local.hour < SIGNAL_WINDOW_END_HOUR
        mode = "premarket" if is_premarket else ("in_window" if in_window else "out_of_window")

        signal_value = projected_high if projected_high is not None else daily_max

        # -----------------------------------------------------------------------
        # PRE-MARKET BREACH CHECK
        # Runs before 9 AM. If overnight temps have already killed one or more
        # thresholds, fire an immediate Discord warning so you can react before
        # the trading window opens.
        # -----------------------------------------------------------------------
        if is_premarket and breached:
            highest_breached = breached[-1]
            warn_state = load_warning_state()
            today_str = now_local.date().isoformat()
            
            # Only trigger if it's a new day OR a new, higher threshold was breached
            if warn_state["date"] != today_str or warn_state.get("highest_breached") != highest_breached:
                breach_msg_parts = [
                    f"⚠️ **SFO PRE-MARKET BREACH ALERT**",
                    f"Overnight temps have already blown past threshold(s): **{', '.join(f'>{t}°F' for t in breached)}**",
                    f"**Overnight high so far:** {daily_max}°F",
                ]
                if selected_threshold:
                    breach_msg_parts.append(
                        f"✅ **Next viable strike:** >{selected_threshold}°F — contracts still tradeable"
                    )
                else:
                    breach_msg_parts.append(
                        "❌ **All configured thresholds are breached.** Update AVAILABLE_THRESHOLDS with higher strikes."
                    )
                breach_msg_parts.append(
                    f"_Update `AVAILABLE_THRESHOLDS` in GitHub Actions vars if needed._"
                )
                breach_msg = "\n".join(breach_msg_parts)

                print(f"\n{'='*58}")
                print(f"  PRE-MARKET BREACH DETECTED  |  {now_str}")
                print(f"  Breached: {breached}")
                print(f"  Next viable: {selected_threshold}")
                print(f"{'='*58}\n")

                if WEBHOOK_URL:
                    requests.post(WEBHOOK_URL, json={"content": breach_msg}, timeout=10)
                    print("Pre-market breach alert sent to Discord.")
                    save_warning_state(highest_breached)

        # -----------------------------------------------------------------------
        # IN-WINDOW SIGNAL (9 AM – 3 PM PT)
        # Only fires when there is a viable threshold and the trajectory is close.
        # -----------------------------------------------------------------------
        signal_triggered = False
        if (
            in_window
            and selected_threshold is not None
            and signal_value is not None
            and (selected_threshold - signal_value) <= SIGNAL_PROXIMITY
            and should_fire(daily_max, selected_threshold)
        ):
            signal_triggered = True
            gap = selected_threshold - signal_value
            direction = "above" if gap < 0 else "within"
            rate_str = f"{warming_rate:+.1f}°F/hr" if warming_rate is not None else "?"

            msg = (
                f"🎯 **SFO TMAX SIGNAL** — Contract >{selected_threshold}°F\n"
                f"**Daily max so far:** {daily_max}°F\n"
                f"**Projected day high:** {signal_value}°F "
                f"({direction} {abs(gap):.1f}°F of threshold)\n"
                f"**Warming rate:** {rate_str}\n"
                f"**NWS forecast:** {nws_high}°F\n"
                f"**Obs age:** {f'{obs_age:.0f} min' if obs_age is not None else '?'}\n"
                f"_Breached thresholds (skip these): {', '.join(f'>{t}°F' for t in breached) if breached else 'none'}_\n"
                f"_Settlement: NWS CLI report ~6 AM PT tomorrow_"
            )
            if WEBHOOK_URL:
                requests.post(WEBHOOK_URL, json={"content": msg}, timeout=10)
                print("Signal sent to Discord.")
                save_last_signal(daily_max, selected_threshold)
            else:
                print("DISCORD_WEBHOOK not set.")

        # --- Console summary ---
        print(f"\n{'='*58}")
        print(f"  TMAX Bot  |  {now_str}  |  mode: {mode}")
        print(f"{'='*58}")
        print(f"  Available thresholds  : {AVAILABLE_THRESHOLDS}")
        print(f"  Breached (overnight)  : {breached if breached else 'none'}")
        print(f"  Selected threshold    : {'>' + str(selected_threshold) + '°F' if selected_threshold else 'NONE — all breached'}")
        print(f"  NWS forecast high     : {nws_high}°F")
        print(f"  Daily max so far      : {daily_max}°F")
        print(f"  Projected day high    : {projected_high}°F")
        print(f"  Warming rate          : {f'{warming_rate:+.1f}°F/hr' if warming_rate is not None else '?'}")
        print(f"  Obs age               : {f'{obs_age:.0f} min' if obs_age is not None else '?'}")
        print(f"  Signal triggered      : {'🔴 YES' if signal_triggered else '⚪ no'}")
        if notes:
            print(f"  Notes                 : {'; '.join(notes)}")
        print(f"{'='*58}\n")

        log_run({
            "timestamp": now_str,
            "available_thresholds": str(AVAILABLE_THRESHOLDS),
            "selected_threshold": selected_threshold if selected_threshold else "",
            "breached_thresholds": str(breached) if breached else "",
            "nws_forecast_high": nws_high if nws_high is not None else "",
            "daily_max_so_far": daily_max if daily_max is not None else "",
            "projected_high": projected_high if projected_high is not None else "",
            "warming_rate_per_hour": round(warming_rate, 2) if warming_rate is not None else "",
            "obs_age_minutes": round(obs_age, 1) if obs_age is not None else "",
            "mode": mode,
            "signal_triggered": signal_triggered,
            "notes": "; ".join(notes),
        })

    except Exception as e:
        print(f"Fatal error: {e}")
        log_run({
            "timestamp": now_str,
            "available_thresholds": str(AVAILABLE_THRESHOLDS),
            "selected_threshold": "", "breached_thresholds": "",
            "nws_forecast_high": "", "daily_max_so_far": "",
            "projected_high": "", "warming_rate_per_hour": "",
            "obs_age_minutes": "", "mode": "error",
            "signal_triggered": False, "notes": f"Fatal: {e}",
        })


if __name__ == "__main__":
    main()
