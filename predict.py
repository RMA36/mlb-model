"""
YRFI/NRFI v4 Daily Prediction Pipeline (Standalone)

Architecture:
  Stage 1: Two-model LightGBM (top1_run + bot1_run), ~52 features each
  Stage 2: Market-anchored calibration: p_cal = mkt_fair + k*(p_raw - model_mean)
  Stage 3: Selectivity filter: sub-model agreement + Kelly > 1% threshold

Runs as GitHub Actions daily or locally.

Usage:
    python predict.py                     # today's games
    python predict.py --date 2026-06-15   # specific date
"""

import argparse
import json
import logging
import os
import subprocess
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import lightgbm as lgb
import numpy as np
import pandas as pd
import requests

ROOT = Path(__file__).resolve().parent
MODEL_DIR = ROOT / "models" / "v4"
PREDICTIONS_DIR = ROOT / "predictions"

logger = logging.getLogger(__name__)

# ── v4 Configuration ───────────────────────────────────────────────────

CALIBRATION_K = 0.90
DEFAULT_MODEL_MEAN = 0.485

KELLY_FRACTION = 0.25
MAX_BET_PCT = 0.05
MAX_GAME_PCT = 0.08
MIN_KELLY_PCT = 0.01
BANKROLL = 1000.0

# League averages (fallback for unknown players)
LEAGUE_AVG_K_RATE = 0.225
LEAGUE_AVG_BB_RATE = 0.085
LEAGUE_AVG_HR_RATE = 0.035
LEAGUE_AVG_WOBA = 0.310
LEAGUE_AVG_OBP = 0.310
LEAGUE_AVG_STRIKE_RATE = 0.34

# PA probability weights for lineup positions 1-6
PA_PROB_WEIGHTS = [1.0, 1.0, 1.0, 0.75, 0.40, 0.18]

# Default PA probabilities for positions 4, 5, 6
DEFAULT_PA_PROB_4 = 0.75
DEFAULT_PA_PROB_5 = 0.40
DEFAULT_PA_PROB_6 = 0.18

MIN_PA_FOR_BATTER_STATS = 20
REST_DAYS_CAP = 30

MLB_API = "https://statsapi.mlb.com"

# Odds master file from companion tracker repo
ODDS_MASTER_REPO = "RMA36/mlb-odds-tracker-2026"
ODDS_MASTER_PATH = "data/2026/yrfi_master_2026.parquet"
ODDS_CACHE = ROOT / ".cache" / "yrfi_master_2026.parquet"

# ── Team mappings ──────────────────────────────────────────────────────

FULL_NAME_TO_ABBR = {
    "Arizona Diamondbacks": "ARI", "Atlanta Braves": "ATL",
    "Baltimore Orioles": "BAL", "Boston Red Sox": "BOS",
    "Chicago Cubs": "CHC", "Chicago White Sox": "CHW",
    "Cincinnati Reds": "CIN", "Cleveland Guardians": "CLE",
    "Colorado Rockies": "COL", "Detroit Tigers": "DET",
    "Houston Astros": "HOU", "Kansas City Royals": "KCR",
    "Los Angeles Angels": "LAA", "Los Angeles Dodgers": "LAD",
    "Miami Marlins": "MIA", "Milwaukee Brewers": "MIL",
    "Minnesota Twins": "MIN", "New York Mets": "NYM",
    "New York Yankees": "NYY", "Oakland Athletics": "OAK", "Athletics": "OAK",
    "Philadelphia Phillies": "PHI", "Pittsburgh Pirates": "PIT",
    "San Diego Padres": "SDP", "San Francisco Giants": "SFG",
    "Seattle Mariners": "SEA", "St. Louis Cardinals": "STL",
    "Tampa Bay Rays": "TBR", "Texas Rangers": "TEX",
    "Toronto Blue Jays": "TOR", "Washington Nationals": "WSN",
}

ABBR_TO_FULL = {v: k for k, v in FULL_NAME_TO_ABBR.items()}

STATCAST_ABBR = {
    "AZ": "ARI", "TB": "TBR", "CWS": "CHW", "KC": "KCR",
    "SD": "SDP", "SF": "SFG", "WSH": "WSN", "ATH": "OAK",
}


def normalize_abbr(abbr):
    return STATCAST_ABBR.get(abbr, abbr)


# ── Odds helpers ──────────────────────────────────────────────────────


def american_to_implied(o):
    if o > 0:
        return 100 / (o + 100)
    return abs(o) / (abs(o) + 100)


def american_to_decimal(o):
    if o > 0:
        return o / 100 + 1
    if o < 0:
        return 100 / abs(o) + 1
    return 1.0


def download_odds_master():
    """Download odds master from GitHub using gh CLI or GITHUB_TOKEN."""
    ODDS_CACHE.parent.mkdir(parents=True, exist_ok=True)

    # Check if cache is fresh (< 1 hour old)
    if ODDS_CACHE.exists():
        age_hours = (time.time() - os.path.getmtime(ODDS_CACHE)) / 3600
        if age_hours < 1.0:
            logger.info("Using cached odds (%.0f min old)", age_hours * 60)
            return ODDS_CACHE

    # Method 1: gh CLI (works locally with authenticated user)
    try:
        result = subprocess.run(
            ["gh", "api", f"repos/{ODDS_MASTER_REPO}/contents/{ODDS_MASTER_PATH}",
             "--jq", ".download_url"],
            capture_output=True, text=True, timeout=30)
        if result.returncode == 0 and result.stdout.strip():
            urllib.request.urlretrieve(result.stdout.strip(), ODDS_CACHE)
            logger.info("Downloaded odds via gh CLI")
            return ODDS_CACHE
    except FileNotFoundError:
        pass  # gh not available

    # Method 2: GITHUB_TOKEN env var (works in GitHub Actions)
    token = os.environ.get("GH_TOKEN") or os.environ.get("GITHUB_TOKEN")
    if token:
        url = f"https://api.github.com/repos/{ODDS_MASTER_REPO}/contents/{ODDS_MASTER_PATH}"
        req = urllib.request.Request(url, headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github.raw+json",
        })
        with urllib.request.urlopen(req) as resp:
            ODDS_CACHE.write_bytes(resp.read())
        logger.info("Downloaded odds via GITHUB_TOKEN")
        return ODDS_CACHE

    if ODDS_CACHE.exists():
        logger.warning("Cannot refresh odds — using stale cache")
        return ODDS_CACHE

    logger.error("No way to download odds (no gh CLI, no GITHUB_TOKEN)")
    return None


def load_daily_odds(date_str):
    """Load YRFI/NRFI odds for a specific date from the YRFI master file."""
    odds_path = download_odds_master()
    if odds_path is None or not odds_path.exists():
        return pd.DataFrame()

    fi = pd.read_parquet(odds_path)
    if fi.empty:
        logger.warning("No data in YRFI master")
        return pd.DataFrame()

    # Use commence_time (UTC) → ET for true game date
    fi["commence_utc"] = pd.to_datetime(fi["commence_time"], utc=True)
    fi["commence_et"] = fi["commence_utc"].dt.tz_convert("US/Eastern")
    fi["game_day"] = fi["commence_et"].dt.strftime("%Y-%m-%d")
    fi = fi[fi["game_day"] == date_str].copy()

    if fi.empty:
        all_days = pd.read_parquet(odds_path)
        all_days["commence_utc"] = pd.to_datetime(all_days["commence_time"], utc=True)
        all_days["commence_et"] = all_days["commence_utc"].dt.tz_convert("US/Eastern")
        all_days["game_day"] = all_days["commence_et"].dt.strftime("%Y-%m-%d")
        avail = sorted(all_days["game_day"].unique())[-5:]
        logger.warning("No YRFI/NRFI odds for %s. Available: %s", date_str, avail)
        return pd.DataFrame()

    yrfi_rows = fi[fi["outcome_name"] == "Over"].copy()
    nrfi_rows = fi[fi["outcome_name"] == "Under"].copy()

    # Best odds per game (highest American odds = best for bettor)
    yrfi_best = (yrfi_rows.loc[yrfi_rows.groupby("game_id")["close_price"].idxmax()]
                 [["game_id", "close_price", "bookmaker"]]
                 .rename(columns={"close_price": "yrfi_best_odds",
                                  "bookmaker": "yrfi_best_book"}))
    nrfi_best = (nrfi_rows.loc[nrfi_rows.groupby("game_id")["close_price"].idxmax()]
                 [["game_id", "close_price", "bookmaker"]]
                 .rename(columns={"close_price": "nrfi_best_odds",
                                  "bookmaker": "nrfi_best_book"}))

    # Keep commence_time for precise game matching (handles doubleheaders)
    yrfi_agg = (yrfi_rows.groupby(["game_id", "home_team", "away_team", "commence_time"])
                .agg(yrfi_odds=("close_price", "median"),
                     yrfi_open=("open_price", "median"),
                     n_books=("bookmaker", "nunique"))
                .reset_index())
    nrfi_agg = (nrfi_rows.groupby(["game_id", "home_team", "away_team", "commence_time"])
                .agg(nrfi_odds=("close_price", "median"),
                     nrfi_open=("open_price", "median"))
                .reset_index())

    result = yrfi_agg.merge(nrfi_agg[["game_id", "nrfi_odds", "nrfi_open"]],
                            on="game_id", how="inner")
    result = result.merge(yrfi_best, on="game_id", how="left")
    result = result.merge(nrfi_best, on="game_id", how="left")

    result["home_team_abbr"] = result["home_team"].map(FULL_NAME_TO_ABBR)
    result["away_team_abbr"] = result["away_team"].map(FULL_NAME_TO_ABBR)

    # Parse commence_time to UTC datetime for matching against MLB API game times
    result["commence_utc"] = pd.to_datetime(result["commence_time"], utc=True)

    valid = (result["yrfi_odds"].abs() >= 100) & (result["nrfi_odds"].abs() >= 100)
    result = result[valid].copy()

    result["mkt_y_impl"] = result["yrfi_odds"].apply(american_to_implied)
    result["mkt_n_impl"] = result["nrfi_odds"].apply(american_to_implied)
    total = result["mkt_y_impl"] + result["mkt_n_impl"]
    result["mkt_y_fair"] = result["mkt_y_impl"] / total
    result["mkt_n_fair"] = result["mkt_n_impl"] / total
    result["yrfi_dec"] = result["yrfi_odds"].apply(american_to_decimal)
    result["nrfi_dec"] = result["nrfi_odds"].apply(american_to_decimal)
    result["yrfi_best_dec"] = result["yrfi_best_odds"].apply(american_to_decimal)
    result["nrfi_best_dec"] = result["nrfi_best_odds"].apply(american_to_decimal)

    logger.info("Loaded odds for %d games (%s)", len(result), date_str)
    return result


# ── MLB Stats API ──────────────────────────────────────────────────────


def fetch_schedule(date_str):
    r = requests.get(f"{MLB_API}/api/v1/schedule",
                     params={"sportId": 1, "date": date_str})
    r.raise_for_status()
    data = r.json()
    if not data.get("dates"):
        return []
    return data["dates"][0]["games"]


def fetch_game_detail(game_pk):
    r = requests.get(f"{MLB_API}/api/v1.1/game/{game_pk}/feed/live")
    r.raise_for_status()
    return r.json()


def extract_game_info(game_data):
    gd = game_data["gameData"]
    ld = game_data["liveData"]

    info = {
        "game_pk": gd["game"]["pk"],
        "home_team": normalize_abbr(gd["teams"]["home"]["abbreviation"]),
        "away_team": normalize_abbr(gd["teams"]["away"]["abbreviation"]),
        "venue": gd["venue"]["name"],
        "game_datetime": gd.get("datetime", {}).get("dateTime"),  # ISO UTC
        "status": gd.get("status", {}).get("detailedState", ""),
    }

    pp = gd.get("probablePitchers", {})
    info["home_starter_id"] = pp.get("home", {}).get("id")
    info["away_starter_id"] = pp.get("away", {}).get("id")
    info["home_starter_name"] = pp.get("home", {}).get("fullName", "TBD")
    info["away_starter_name"] = pp.get("away", {}).get("fullName", "TBD")

    players = gd.get("players", {})
    for side in ["home", "away"]:
        pid = info.get(f"{side}_starter_id")
        if pid:
            pdata = players.get(f"ID{pid}", {})
            info[f"{side}_starter_throws"] = pdata.get("pitchHand", {}).get("code", "R")

    bp = ld.get("boxscore", {}).get("teams", {})
    for side in ["away", "home"]:
        order = bp.get(side, {}).get("battingOrder", [])
        info[f"{side}_lineup"] = order[:6]
        batter_hands = []
        for bid in order[:6]:
            bdata = players.get(f"ID{bid}", {})
            batter_hands.append(bdata.get("batSide", {}).get("code", "R"))
        info[f"{side}_lineup_hands"] = batter_hands

    officials = ld.get("boxscore", {}).get("officials", [])
    for off in officials:
        if off.get("officialType") == "Home Plate":
            info["umpire"] = off.get("official", {}).get("fullName", "Unknown")
            break
    else:
        info["umpire"] = "Unknown"

    weather = gd.get("weather", {})
    info["temp"] = weather.get("temp", "72")
    info["humidity"] = weather.get("humidity", "50")
    info["wind"] = weather.get("wind", "0 mph")

    return info


# ── Weather (Open-Meteo) ──────────────────────────────────────────────


def load_stadium_coordinates():
    """Load lat/lon for each venue from stadium_metadata.csv."""
    csv_path = MODEL_DIR / "stadium_metadata.csv"
    if not csv_path.exists():
        return {}
    coords = {}
    import csv
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            name = row.get("stadium_name", "").strip()
            if name:
                coords[name] = {
                    "lat": float(row.get("latitude", 0)),
                    "lon": float(row.get("longitude", 0)),
                }
    return coords


def fetch_weather_for_games(game_infos, date_str):
    """Fetch weather from Open-Meteo for each unique venue on game day.

    Returns dict keyed by venue name with:
      temp_min (°F), temp_max (°F), precipitation_mm, humidity (%)
    And for games with known start times, hourly data at first pitch:
      hourly_precipitation_mm, hourly_temp (°F)
    """
    coords = load_stadium_coordinates()
    if not coords:
        logger.warning("No stadium coordinates — skipping weather fetch")
        return {}

    # Collect unique venues with their game times
    venues = {}
    for info in game_infos:
        venue = info.get("venue", "")
        if venue and venue in coords and venue not in venues:
            venues[venue] = {
                "lat": coords[venue]["lat"],
                "lon": coords[venue]["lon"],
                "game_datetime": info.get("game_datetime"),
            }

    if not venues:
        return {}

    weather = {}
    for venue, vinfo in venues.items():
        try:
            lat, lon = vinfo["lat"], vinfo["lon"]
            # Open-Meteo forecast API — free, no key, returns daily + hourly
            url = (
                f"https://api.open-meteo.com/v1/forecast?"
                f"latitude={lat}&longitude={lon}"
                f"&daily=temperature_2m_max,temperature_2m_min,precipitation_sum"
                f"&hourly=temperature_2m,precipitation,relative_humidity_2m,windspeed_10m"
                f"&temperature_unit=fahrenheit"
                f"&windspeed_unit=mph"
                f"&precipitation_unit=mm"
                f"&timezone=America%2FNew_York"
                f"&start_date={date_str}&end_date={date_str}"
            )
            resp = requests.get(url, timeout=10)
            resp.raise_for_status()
            data = resp.json()

            daily = data.get("daily", {})
            w = {
                "temp_max": daily.get("temperature_2m_max", [72])[0],
                "temp_min": daily.get("temperature_2m_min", [62])[0],
                "precipitation_mm": daily.get("precipitation_sum", [0])[0] or 0,
            }

            # If we have game start time, get hourly values at first pitch
            gdt = vinfo.get("game_datetime")
            hourly = data.get("hourly", {})
            if gdt and hourly.get("time"):
                try:
                    game_utc = datetime.fromisoformat(gdt.replace("Z", "+00:00"))
                    game_et = game_utc.astimezone(ZoneInfo("US/Eastern"))
                    target_hour = game_et.strftime("%Y-%m-%dT%H:00")
                    hours = hourly["time"]
                    if target_hour in hours:
                        idx = hours.index(target_hour)
                        precip_vals = hourly.get("precipitation", [])
                        temp_vals = hourly.get("temperature_2m", [])
                        humidity_vals = hourly.get("relative_humidity_2m", [])
                        if idx < len(precip_vals):
                            w["hourly_precipitation_mm"] = precip_vals[idx] or 0
                        if idx < len(temp_vals):
                            w["hourly_temp"] = temp_vals[idx]
                        if idx < len(humidity_vals):
                            w["hourly_humidity"] = humidity_vals[idx]
                        wind_vals = hourly.get("windspeed_10m", [])
                        if idx < len(wind_vals):
                            w["hourly_wind_mph"] = wind_vals[idx]
                except Exception:
                    pass

            weather[venue] = w
            time.sleep(0.2)  # polite rate limiting
        except Exception as exc:
            logger.warning("Weather fetch failed for %s: %s", venue, exc)

    logger.info("Fetched weather for %d venues", len(weather))
    return weather


# ── Feature computation ────────────────────────────────────────────────


def load_lookups():
    """Load compact lookup tables (pitchers, batters, umpires, parks)."""
    path = MODEL_DIR / "lookups.json"
    if not path.exists():
        logger.error("lookups.json not found at %s", path)
        return {}

    with open(path) as f:
        raw = json.load(f)

    pitcher_stats = {}
    for pid_str, d in raw.get("pitchers", {}).items():
        pid = int(pid_str)
        k = d.get("k", LEAGUE_AVG_K_RATE)
        bb = d.get("bb", LEAGUE_AVG_BB_RATE)
        hr = d.get("hr", LEAGUE_AVG_HR_RATE)
        fi_k = d.get("fik", k)
        fi_bb = d.get("fibb", bb)
        pitcher_stats[pid] = {
            "avg_velo": d.get("v", 93.0),
            "career_k_rate": k,
            "career_bb_rate": bb,
            "career_hr_rate": hr,
            "season_k_rate": d.get("sk", k),
            "season_bb_rate": d.get("sbb", bb),
            "season_hr_rate": d.get("shr", hr),
            "gb_rate": d.get("gb", 0.43),
            "whip_approx": d.get("whip", 1.30),
            "k_rate_vs_L": d.get("kL", k),
            "k_rate_vs_R": d.get("kR", k),
            "bb_rate_vs_L": d.get("bbL", bb),
            "bb_rate_vs_R": d.get("bbR", bb),
            "hr_rate_vs_L": d.get("hrL", hr),
            "hr_rate_vs_R": d.get("hrR", hr),
            "obpa_vs_L": d.get("obpL", 0.315),
            "obpa_vs_R": d.get("obpR", 0.315),
            "fi_k_rate": fi_k,
            "fi_bb_rate": fi_bb,
            "fi_hr_rate": d.get("fihr", hr),
            "fi_runs_per_start": d.get("fir", 0.5),
            "fi_starts": d.get("fis", 0),
            "fi_k_rate_vs_L": d.get("fkL", fi_k),
            "fi_k_rate_vs_R": d.get("fkR", fi_k),
            "recent_fi_k_rate": d.get("rfik", fi_k),
            "recent_fi_bb_rate": d.get("rfibb", fi_bb),
            "recent_fi_runs": d.get("rfir", 0.5),
            "last_game_date": d.get("lgd"),
        }

    batter_stats = {}
    for bid_str, d in raw.get("batters", {}).items():
        bid = int(bid_str)
        w = d.get("w", LEAGUE_AVG_WOBA)
        o = d.get("o", LEAGUE_AVG_OBP)
        batter_stats[bid] = {
            "woba": w,
            "obp": o,
            "woba_vs_L": d.get("wL", w),
            "woba_vs_R": d.get("wR", w),
            "obp_vs_L": d.get("oL", o),
            "obp_vs_R": d.get("oR", o),
        }

    # Umpire lookups: handle both old format (float) and new format (dict)
    umpire_stats = {}
    for name, val in raw.get("umpires", {}).items():
        if isinstance(val, dict):
            umpire_stats[name] = {
                "career_strike_rate": val.get("sr", LEAGUE_AVG_STRIKE_RATE),
                "consistency": val.get("con", 0.03),
                "games_count": val.get("gc", 0),
            }
        else:
            umpire_stats[name] = {
                "career_strike_rate": float(val),
                "consistency": 0.03,
                "games_count": 0,
            }

    # Park lookups: handle both old format (float) and new format (dict)
    stadium_meta = {}
    for venue, val in raw.get("parks", {}).items():
        if isinstance(val, dict):
            stadium_meta[venue] = {
                "park_factor_runs": val.get("pfr", 1.0),
                "park_factor_hr": val.get("pfh", 1.0),
                "elevation_ft": val.get("elev", 0),
                "is_dome": val.get("dome", 0),
            }
        else:
            stadium_meta[venue] = {
                "park_factor_runs": float(val),
                "park_factor_hr": 1.0,
                "elevation_ft": 0,
                "is_dome": 0,
            }

    logger.info("Lookups: %d pitchers, %d batters, %d umps, %d parks",
                len(pitcher_stats), len(batter_stats), len(umpire_stats), len(stadium_meta))

    return {
        "pitcher_stats": pitcher_stats,
        "batter_stats": batter_stats,
        "umpire_stats": umpire_stats,
        "stadium_meta": stadium_meta,
    }


def compute_features(game_info, lookups, date_str=None, weather=None):
    """Compute ALL 52 model features for a single game.

    Parameters
    ----------
    weather : dict or None
        Weather data for this venue from fetch_weather_for_games().
        Keys: temp_min, temp_max, precipitation_mm, hourly_precipitation_mm,
              hourly_temp, hourly_humidity.

    Matches the training pipeline exactly:
    - 30 pitcher features per side (career, season, FI, platoon, recent)
    - 7 lineup features per side (PA probs, OBP, weighted score, platoon)
    - 10 environment features
    - 3 umpire features
    - 3 context features
    """
    features = {}
    ps = lookups.get("pitcher_stats", {})
    bs = lookups.get("batter_stats", {})
    us = lookups.get("umpire_stats", {})
    stadium = lookups.get("stadium_meta", {})

    for side in ["home", "away"]:
        pid = game_info.get(f"{side}_starter_id")
        throws = game_info.get(f"{side}_starter_throws", "R")
        p = ps.get(pid, {})
        prefix = f"{side}_p_"

        # ── Career stats (expanding window) ──
        features[f"{prefix}avg_velo"] = p.get("avg_velo", 93.0)
        features[f"{prefix}career_k_rate"] = p.get("career_k_rate", LEAGUE_AVG_K_RATE)
        features[f"{prefix}career_bb_rate"] = p.get("career_bb_rate", LEAGUE_AVG_BB_RATE)
        features[f"{prefix}career_hr_rate"] = p.get("career_hr_rate", LEAGUE_AVG_HR_RATE)

        # ── Season stats ──
        features[f"{prefix}season_k_rate"] = p.get("season_k_rate", LEAGUE_AVG_K_RATE)
        features[f"{prefix}season_bb_rate"] = p.get("season_bb_rate", LEAGUE_AVG_BB_RATE)
        features[f"{prefix}season_hr_rate"] = p.get("season_hr_rate", LEAGUE_AVG_HR_RATE)

        # ── Ground ball / WHIP ──
        features[f"{prefix}gb_rate"] = p.get("gb_rate", 0.43)
        features[f"{prefix}whip_approx"] = p.get("whip_approx", 1.30)

        # ── Platoon splits ──
        features[f"{prefix}k_rate_vs_L"] = p.get("k_rate_vs_L", LEAGUE_AVG_K_RATE)
        features[f"{prefix}k_rate_vs_R"] = p.get("k_rate_vs_R", LEAGUE_AVG_K_RATE)
        features[f"{prefix}bb_rate_vs_L"] = p.get("bb_rate_vs_L", LEAGUE_AVG_BB_RATE)
        features[f"{prefix}bb_rate_vs_R"] = p.get("bb_rate_vs_R", LEAGUE_AVG_BB_RATE)
        features[f"{prefix}hr_rate_vs_L"] = p.get("hr_rate_vs_L", LEAGUE_AVG_HR_RATE)
        features[f"{prefix}hr_rate_vs_R"] = p.get("hr_rate_vs_R", LEAGUE_AVG_HR_RATE)
        features[f"{prefix}obpa_vs_L"] = p.get("obpa_vs_L", 0.315)
        features[f"{prefix}obpa_vs_R"] = p.get("obpa_vs_R", 0.315)

        # ── First-inning stats ──
        features[f"{prefix}fi_k_rate"] = p.get("fi_k_rate", LEAGUE_AVG_K_RATE)
        features[f"{prefix}fi_bb_rate"] = p.get("fi_bb_rate", LEAGUE_AVG_BB_RATE)
        features[f"{prefix}fi_hr_rate"] = p.get("fi_hr_rate", LEAGUE_AVG_HR_RATE)
        features[f"{prefix}fi_runs_per_start"] = p.get("fi_runs_per_start", 0.5)
        features[f"{prefix}fi_starts"] = p.get("fi_starts", 0)

        # FI platoon K rates
        features[f"{prefix}fi_k_rate_vs_L"] = p.get("fi_k_rate_vs_L", LEAGUE_AVG_K_RATE)
        features[f"{prefix}fi_k_rate_vs_R"] = p.get("fi_k_rate_vs_R", LEAGUE_AVG_K_RATE)

        # ── Recent FI stats (last 3 starts) ──
        features[f"{prefix}recent_fi_k_rate"] = p.get("recent_fi_k_rate", LEAGUE_AVG_K_RATE)
        features[f"{prefix}recent_fi_bb_rate"] = p.get("recent_fi_bb_rate", LEAGUE_AVG_BB_RATE)
        features[f"{prefix}recent_fi_runs"] = p.get("recent_fi_runs", 0.5)

        # ── Rest days ──
        rest = REST_DAYS_CAP  # default
        lgd = p.get("last_game_date")
        if lgd and date_str:
            try:
                last_dt = datetime.strptime(lgd, "%Y-%m-%d")
                game_dt = datetime.strptime(date_str, "%Y-%m-%d")
                rest = min((game_dt - last_dt).days, REST_DAYS_CAP)
                if rest < 1:
                    rest = 5  # fallback for same-day or negative
            except (ValueError, TypeError):
                rest = 5
        elif not lgd:
            rest = 5  # fallback for unknown
        features[f"{prefix}rest_days"] = rest

        # ── Composite platoon K rate (PA-probability weighted lineup hand composition) ──
        opp_side = "away" if side == "home" else "home"
        opp_hands = game_info.get(f"{opp_side}_lineup_hands", [])

        # PA-probability weighted handedness fractions (matches training)
        pa_w = PA_PROB_WEIGHTS[:len(opp_hands)]
        if pa_w:
            left_weight = sum(w for h, w in zip(opp_hands, pa_w) if h == "L")
            total_weight = sum(pa_w)
            pct_left = left_weight / total_weight if total_weight > 0 else 0.5
        else:
            pct_left = 0.5

        features[f"{prefix}platoon_k_rate"] = (
            pct_left * p.get("k_rate_vs_L", LEAGUE_AVG_K_RATE) +
            (1 - pct_left) * p.get("k_rate_vs_R", LEAGUE_AVG_K_RATE)
        )
        features[f"{prefix}fi_platoon_k_rate"] = (
            pct_left * p.get("fi_k_rate_vs_L", LEAGUE_AVG_K_RATE) +
            (1 - pct_left) * p.get("fi_k_rate_vs_R", LEAGUE_AVG_K_RATE)
        )

        # ── Opposing lineup features ──
        lineup_ids = game_info.get(f"{opp_side}_lineup", [])
        lineup_hands = game_info.get(f"{opp_side}_lineup_hands", [])

        wobas = []
        obps = []
        platoon_wobas = []
        platoon_obps = []
        for bid, hand in zip(lineup_ids, lineup_hands):
            b = bs.get(bid, {})
            wobas.append(b.get("woba", LEAGUE_AVG_WOBA))
            obps.append(b.get("obp", LEAGUE_AVG_OBP))
            if throws == "L":
                platoon_wobas.append(b.get("woba_vs_L", LEAGUE_AVG_WOBA))
                platoon_obps.append(b.get("obp_vs_L", LEAGUE_AVG_OBP))
            else:
                platoon_wobas.append(b.get("woba_vs_R", LEAGUE_AVG_WOBA))
                platoon_obps.append(b.get("obp_vs_R", LEAGUE_AVG_OBP))

        # Pad to 6 if shorter
        while len(wobas) < 6:
            wobas.append(LEAGUE_AVG_WOBA)
            obps.append(LEAGUE_AVG_OBP)
            platoon_wobas.append(LEAGUE_AVG_WOBA)
            platoon_obps.append(LEAGUE_AVG_OBP)

        pa_probs = PA_PROB_WEIGHTS[:6]

        # Top-3 OBP (used for PA probability grid lookup)
        top3_obp = np.mean(obps[:3])
        features[f"{opp_side}_lineup_top3_obp"] = float(top3_obp)

        # PA probabilities for positions 4, 5, 6 (Monte Carlo grid approximation)
        # The training grid uses OBP and pitcher K rate; the actual MC simulation
        # outcome depends only on OBP. We approximate with a simple formula
        # calibrated to match the MC grid output.
        pitcher_k = p.get("career_k_rate", LEAGUE_AVG_K_RATE)
        pa4, pa5, pa6 = _estimate_pa_probs(top3_obp, pitcher_k)
        features[f"{opp_side}_lineup_pa_prob_4"] = pa4
        features[f"{opp_side}_lineup_pa_prob_5"] = pa5
        features[f"{opp_side}_lineup_pa_prob_6"] = pa6

        # Use estimated PA probs for weighted score (matches training)
        actual_pa_probs = [1.0, 1.0, 1.0, pa4, pa5, pa6]
        features[f"{opp_side}_lineup_weighted_score"] = float(
            np.dot(actual_pa_probs[:len(wobas)], wobas[:6])
        )

        # Platoon lineup wOBA and OBP (PA-probability-weighted mean)
        total_pa_w = sum(actual_pa_probs[:6])
        features[f"{opp_side}_platoon_lineup_woba"] = float(
            np.dot(actual_pa_probs[:6], platoon_wobas[:6]) / total_pa_w
        )
        features[f"{opp_side}_platoon_lineup_obp"] = float(
            np.dot(actual_pa_probs[:6], platoon_obps[:6]) / total_pa_w
        )

    # ── Environment features ──
    venue = game_info.get("venue", "")
    park = stadium.get(venue, {})
    is_dome = park.get("is_dome", 0)
    wx = weather or {}

    try:
        temp = float(game_info.get("temp", 72))
    except (ValueError, TypeError):
        temp = 72.0
    try:
        humidity = float(game_info.get("humidity", 50))
    except (ValueError, TypeError):
        humidity = 50.0

    # Parse wind from MLB API string like "5 mph, In From CF"
    wind_str = game_info.get("wind", "0 mph")
    try:
        wind_mph = float(wind_str.split()[0])
    except (ValueError, IndexError):
        wind_mph = 0.0

    # Use Open-Meteo hourly values at first pitch when available
    if "hourly_temp" in wx:
        temp = wx["hourly_temp"]
    if "hourly_humidity" in wx:
        humidity = wx["hourly_humidity"]

    # temp_min: prefer Open-Meteo daily min (matches training), fall back to temp-10
    temp_min = wx.get("temp_min", temp - 10.0)

    # precipitation: prefer hourly at first pitch, then daily sum, then 0
    precipitation = wx.get("hourly_precipitation_mm", wx.get("precipitation_mm", 0.0))

    # Dome overrides (matches training pipeline)
    if is_dome:
        temp = 72.0
        temp_min = 72.0
        humidity = 50.0
        wind_mph = 0.0
        precipitation = 0.0

    features["env_temp_max"] = temp
    features["env_temp_min"] = temp_min
    features["env_humidity"] = humidity
    features["env_wind_mph"] = wind_mph
    features["env_precipitation"] = precipitation
    features["env_is_dome"] = float(is_dome)
    features["env_park_factor_runs"] = park.get("park_factor_runs", 100.0)
    features["env_park_factor_hr"] = park.get("park_factor_hr", 100.0)
    features["env_elevation_ft"] = park.get("elevation_ft", 0)

    # ── Umpire features ──
    ump = us.get(game_info.get("umpire", "Unknown"), {})
    features["ump_career_strike_rate"] = ump.get("career_strike_rate", LEAGUE_AVG_STRIKE_RATE)
    features["ump_consistency"] = ump.get("consistency", 0.03)
    features["ump_games_count"] = ump.get("games_count", 0)

    # ── Context features ──
    if date_str:
        try:
            gdate = datetime.strptime(date_str, "%Y-%m-%d")
            features["ctx_day_of_week"] = gdate.weekday()  # Monday=0, Sunday=6
            features["ctx_is_weekend"] = 1.0 if gdate.weekday() >= 5 else 0.0
            features["ctx_month"] = gdate.month
        except ValueError:
            features["ctx_day_of_week"] = 2  # default Tuesday
            features["ctx_is_weekend"] = 0.0
            features["ctx_month"] = 6  # default June
    else:
        features["ctx_day_of_week"] = 2
        features["ctx_is_weekend"] = 0.0
        features["ctx_month"] = 6

    return features


def _load_pa_grid():
    """Load the precomputed Monte Carlo PA probability grid.

    Returns dict with obp_grid, k_rate_grid, pa_probs (3D array).
    Falls back to None if file not found.
    """
    grid_path = MODEL_DIR / "pa_grid.json"
    if not grid_path.exists():
        logger.warning("pa_grid.json not found at %s — using fallback defaults", grid_path)
        return None
    with open(grid_path) as f:
        raw = json.load(f)
    return {
        "obp_grid": np.array(raw["obp_grid"]),
        "k_rate_grid": np.array(raw["k_rate_grid"]),
        "pa_probs": np.array(raw["pa_probs"]),
    }


# Module-level cache: loaded once on first call
_PA_GRID_CACHE = None


def _get_pa_grid():
    global _PA_GRID_CACHE
    if _PA_GRID_CACHE is None:
        _PA_GRID_CACHE = _load_pa_grid()
    return _PA_GRID_CACHE


def _estimate_pa_probs(top3_obp, pitcher_k_rate):
    """Look up PA probabilities for positions 4, 5, 6 from the MC grid.

    Uses bilinear interpolation over the same precomputed Monte Carlo grid
    that the training pipeline uses, ensuring exact feature parity.
    """
    grid = _get_pa_grid()
    if grid is None:
        # Fallback to defaults if grid not available
        return DEFAULT_PA_PROB_4, DEFAULT_PA_PROB_5, DEFAULT_PA_PROB_6

    obp_arr = grid["obp_grid"]
    k_arr = grid["k_rate_grid"]
    pa = grid["pa_probs"]   # shape (n_obp, n_k, 6)

    obp = np.clip(top3_obp, obp_arr[0], obp_arr[-1])
    k = np.clip(pitcher_k_rate, k_arr[0], k_arr[-1])

    # Find surrounding grid indices for bilinear interpolation
    i = np.searchsorted(obp_arr, obp) - 1
    i = np.clip(i, 0, len(obp_arr) - 2)
    j = np.searchsorted(k_arr, k) - 1
    j = np.clip(j, 0, len(k_arr) - 2)

    # Interpolation weights
    obp_frac = (obp - obp_arr[i]) / (obp_arr[i + 1] - obp_arr[i])
    k_frac = (k - k_arr[j]) / (k_arr[j + 1] - k_arr[j])

    # Bilinear interpolation over positions 4, 5, 6 (indices 3, 4, 5)
    v00 = pa[i, j]
    v10 = pa[i + 1, j]
    v01 = pa[i, j + 1]
    v11 = pa[i + 1, j + 1]
    interp = (v00 * (1 - obp_frac) * (1 - k_frac) +
              v10 * obp_frac * (1 - k_frac) +
              v01 * (1 - obp_frac) * k_frac +
              v11 * obp_frac * k_frac)

    return float(interp[3]), float(interp[4]), float(interp[5])


# ── Model ──────────────────────────────────────────────────────────────


def load_models():
    """Load pre-trained LGB models and metadata."""
    meta_path = MODEL_DIR / "metadata.json"
    if not meta_path.exists():
        logger.error("metadata.json not found at %s", meta_path)
        return None, None, None

    with open(meta_path) as f:
        meta = json.load(f)

    top = lgb.Booster(model_file=str(MODEL_DIR / "top1_model.txt"))
    bot = lgb.Booster(model_file=str(MODEL_DIR / "bot1_model.txt"))
    return top, bot, meta


def predict_game(features, top_model, bot_model, meta):
    top_vals = np.array([[features.get(f, 0) for f in meta["top1_features"]]])
    bot_vals = np.array([[features.get(f, 0) for f in meta["bot1_features"]]])
    p_top = top_model.predict(top_vals)[0]
    p_bot = bot_model.predict(bot_vals)[0]
    p_yrfi = 1 - (1 - p_top) * (1 - p_bot)
    return p_top, p_bot, p_yrfi


def calibrate(p_raw, mkt_y_fair, model_mean, k=CALIBRATION_K):
    return np.clip(mkt_y_fair + k * (p_raw - model_mean), 0.001, 0.999)


def kelly(win_prob, dec_odds, fraction=KELLY_FRACTION):
    payoff = dec_odds - 1
    if payoff <= 0:
        return 0.0
    raw = (win_prob * payoff - (1 - win_prob)) / payoff
    return max(0.0, raw * fraction)


# ── Main ──────────────────────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(description="YRFI/NRFI v4 daily predictions")
    parser.add_argument("--date", type=str, default=None)
    parser.add_argument("--bankroll", type=float, default=BANKROLL)
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--json-only", action="store_true",
                        help="Output JSON only (for GitHub Actions)")
    args = parser.parse_args()

    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(level=level, format="%(asctime)s  %(message)s")

    # Compound bankroll: size off starting bankroll + cumulative P&L
    if args.bankroll == BANKROLL:
        results_file = ROOT / "results" / "results.json"
        if results_file.exists():
            try:
                res = json.loads(results_file.read_text())
                cum_profit = res.get("cumulative", {}).get("profit", 0.0)
                compound = BANKROLL + cum_profit
                args.bankroll = max(compound, 100.0)  # floor at $100
                logger.info("Compound bankroll: $%.2f (base $%.0f + P&L $%+.2f)",
                            args.bankroll, BANKROLL, cum_profit)
            except Exception:
                pass

    ET = ZoneInfo("US/Eastern")
    date_str = args.date or datetime.now(ET).strftime("%Y-%m-%d")

    # Load everything
    top_model, bot_model, meta = load_models()
    if top_model is None:
        sys.exit(1)

    model_mean = meta.get("model_mean", DEFAULT_MODEL_MEAN)
    top_mean = meta.get("top_mean", 0.295)
    bot_mean = meta.get("bot_mean", 0.280)

    lookups = load_lookups()
    if not lookups:
        sys.exit(1)

    daily_odds = load_daily_odds(date_str)
    if daily_odds.empty:
        logger.error("No odds for %s — cannot generate predictions", date_str)
        sys.exit(1)

    games = fetch_schedule(date_str)
    if not games:
        logger.info("No games scheduled for %s", date_str)
        sys.exit(0)

    # Load already-predicted game_pks from earlier runs today
    out_path = PREDICTIONS_DIR / f"{date_str}.json"
    already_predicted = set()
    if out_path.exists():
        try:
            prev = json.loads(out_path.read_text())
            already_predicted = {g["game_pk"] for g in prev.get("games", [])}
            logger.info("Already predicted %d games from earlier run(s)", len(already_predicted))
        except Exception:
            pass

    # Pass 1: Collect eligible games and extract game info
    eligible_games = []
    skipped_already = 0
    skipped_no_starters = 0
    skipped_started = 0
    skipped_no_lineup = 0
    for g in games:
        gpk = g["gamePk"]
        try:
            if gpk in already_predicted:
                skipped_already += 1
                continue

            detail = fetch_game_detail(gpk)
            info = extract_game_info(detail)
            tag = f"{info['away_team']}@{info['home_team']}"

            if not info.get("home_starter_id") or not info.get("away_starter_id"):
                logger.info("  %d (%s): Starters TBD — skipping", gpk, tag)
                skipped_no_starters += 1
                continue

            status = info.get("status", "")
            if status in ("In Progress", "Final", "Game Over", "Completed Early"):
                logger.info("  %d (%s): %s — skipping", gpk, tag, status)
                skipped_started += 1
                continue

            home_lineup = info.get("home_lineup", [])
            away_lineup = info.get("away_lineup", [])
            if len(home_lineup) < 3 or len(away_lineup) < 3:
                logger.info("  %d (%s): Lineups not posted yet (home=%d, away=%d) — skipping",
                            gpk, tag, len(home_lineup), len(away_lineup))
                skipped_no_lineup += 1
                continue

            game_dt_str = info.get("game_datetime")
            if game_dt_str:
                game_dt = datetime.fromisoformat(game_dt_str.replace("Z", "+00:00"))
                now_utc = datetime.now(timezone.utc)
                if game_dt < now_utc:
                    logger.info("  %d (%s): Game time %s already passed — skipping",
                                gpk, tag,
                                game_dt.astimezone(ZoneInfo("US/Eastern")).strftime("%I:%M %p ET"))
                    skipped_started += 1
                    continue

            eligible_games.append((gpk, info))
            time.sleep(0.3)
        except Exception as exc:
            logger.warning("  %d: Error fetching game -- %s", gpk, exc)

    # Pass 2: Fetch weather for all eligible venues at once
    venue_weather = {}
    if eligible_games:
        try:
            venue_weather = fetch_weather_for_games(
                [info for _, info in eligible_games], date_str
            )
            logger.info("Weather data: %d venues", len(venue_weather))
        except Exception as exc:
            logger.warning("Weather fetch failed: %s — using approximations", exc)

    # Pass 3: Compute features and predict
    predictions = []
    for gpk, info in eligible_games:
        try:
            tag = f"{info['away_team']}@{info['home_team']}"
            wx = venue_weather.get(info.get("venue", ""))
            features = compute_features(info, lookups, date_str=date_str, weather=wx)
            p_top, p_bot, p_raw = predict_game(features, top_model, bot_model, meta)

            home = info["home_team"]
            away = info["away_team"]

            # Match odds: filter by team names, then pick closest commence time.
            # Team names narrow to 1 row (or 2 for doubleheaders), then
            # commence_time disambiguates which game within that pair.
            team_odds = daily_odds[
                (daily_odds["home_team_abbr"] == home) &
                (daily_odds["away_team_abbr"] == away)
            ]

            if team_odds.empty:
                logger.info("  %d (%s): No odds for this matchup — skipping", gpk, tag)
                continue

            if len(team_odds) == 1:
                odds_row = team_odds.iloc[0]
            else:
                # Doubleheader: multiple odds rows for same team pair — pick closest time
                if game_dt_str and "commence_utc" in team_odds.columns:
                    game_utc = pd.Timestamp(game_dt_str.replace("Z", "+00:00"))
                    time_diffs = (team_odds["commence_utc"] - game_utc).abs()
                    odds_row = team_odds.loc[time_diffs.idxmin()]
                else:
                    odds_row = team_odds.iloc[0]
            p_cal = calibrate(p_raw, odds_row["mkt_y_fair"], model_mean)

            top_dev = p_top - top_mean
            bot_dev = p_bot - bot_mean
            if top_dev > 0 and bot_dev > 0:
                agreement = "YRFI"
            elif top_dev < 0 and bot_dev < 0:
                agreement = "NRFI"
            else:
                agreement = "MIXED"

            edge_y = p_cal - odds_row["mkt_y_fair"]
            edge_n = (1 - p_cal) - odds_row["mkt_n_fair"]
            kelly_y = kelly(p_cal, odds_row["yrfi_dec"])
            kelly_n = kelly(1 - p_cal, odds_row["nrfi_dec"])

            bet_side = None
            bet_edge = 0
            bet_kelly = 0
            bet_dec = 1.0
            bet_odds = 0
            open_odds = 0
            best_odds = 0
            best_book = ""
            best_dec = 1.0

            if edge_y > edge_n and edge_y > 0:
                bet_side, bet_edge, bet_kelly = "YRFI", edge_y, kelly_y
                bet_dec, bet_odds = odds_row["yrfi_dec"], odds_row["yrfi_odds"]
                open_odds = odds_row["yrfi_open"]
                best_odds = odds_row.get("yrfi_best_odds", bet_odds)
                best_book = odds_row.get("yrfi_best_book", "")
                best_dec = odds_row.get("yrfi_best_dec", bet_dec)
            elif edge_n > 0:
                bet_side, bet_edge, bet_kelly = "NRFI", edge_n, kelly_n
                bet_dec, bet_odds = odds_row["nrfi_dec"], odds_row["nrfi_odds"]
                open_odds = odds_row["nrfi_open"]
                best_odds = odds_row.get("nrfi_best_odds", bet_odds)
                best_book = odds_row.get("nrfi_best_book", "")
                best_dec = odds_row.get("nrfi_best_dec", bet_dec)

            passes = False
            skip = ""
            if bet_side is None:
                skip = "no edge"
            elif agreement == "MIXED":
                skip = "mixed agreement"
            elif agreement != bet_side:
                skip = "agreement contradicts"
            elif bet_kelly < MIN_KELLY_PCT:
                skip = f"kelly < {MIN_KELLY_PCT:.0%}"
            else:
                passes = True

            stake = 0.0
            if passes:
                stake = min(args.bankroll * bet_kelly, args.bankroll * MAX_BET_PCT)
                stake = round(stake, 2)

            predictions.append({
                "game_pk": gpk,
                "date": date_str,
                "away_team": away,
                "home_team": home,
                "away_starter": info["away_starter_name"],
                "home_starter": info["home_starter_name"],
                "umpire": info.get("umpire", ""),
                "p_top": round(p_top, 4),
                "p_bot": round(p_bot, 4),
                "p_raw": round(p_raw, 4),
                "p_cal": round(float(p_cal), 4),
                "mkt_y_fair": round(odds_row["mkt_y_fair"], 4),
                "mkt_n_fair": round(odds_row["mkt_n_fair"], 4),
                "agreement": agreement,
                "bet_side": bet_side,
                "bet_edge": round(bet_edge, 4),
                "bet_kelly": round(bet_kelly, 4),
                "bet_odds": bet_odds,
                "open_odds": open_odds,
                "bet_dec": round(bet_dec, 3),
                "passes_filter": passes,
                "skip_reason": skip,
                "stake": stake,
                "n_books": int(odds_row["n_books"]),
                "best_odds": int(best_odds) if pd.notna(best_odds) else bet_odds,
                "best_book": str(best_book) if pd.notna(best_book) else "",
                "best_dec": round(float(best_dec), 3) if pd.notna(best_dec) else round(bet_dec, 3),
                "mkt_y_impl": round(float(odds_row["mkt_y_impl"]), 4),
                "mkt_n_impl": round(float(odds_row["mkt_n_impl"]), 4),
                "forecast_weather": {
                    "temp": round(wx.get("hourly_temp", wx.get("temp_max", 72))),
                    "humidity": round(wx.get("hourly_humidity", 50)),
                    "wind_mph": round(wx.get("hourly_wind_mph", 0)),
                    "precipitation_mm": round(wx.get("hourly_precipitation_mm", wx.get("precipitation_mm", 0)), 1),
                } if wx else None,
                "pick_factors": {
                    "away_fi_runs_per_start": round(features.get("away_fi_runs_per_start", 0), 2),
                    "home_fi_runs_per_start": round(features.get("home_fi_runs_per_start", 0), 2),
                    "away_fi_k_rate": round(features.get("away_fi_k_rate", 0), 3),
                    "home_fi_k_rate": round(features.get("home_fi_k_rate", 0), 3),
                    "away_recent_fi_runs": round(features.get("away_recent_fi_runs", 0), 2),
                    "home_recent_fi_runs": round(features.get("home_recent_fi_runs", 0), 2),
                    "away_avg_velo": round(features.get("away_avg_velo", 0), 1),
                    "home_avg_velo": round(features.get("home_avg_velo", 0), 1),
                    "away_platoon_k_rate": round(features.get("away_platoon_k_rate", 0), 3),
                    "home_platoon_k_rate": round(features.get("home_platoon_k_rate", 0), 3),
                    "away_lineup_weighted_score": round(features.get("away_lineup_weighted_score", 0), 3),
                    "home_lineup_weighted_score": round(features.get("home_lineup_weighted_score", 0), 3),
                    "away_platoon_lineup_woba": round(features.get("away_platoon_lineup_woba", 0), 3),
                    "home_platoon_lineup_woba": round(features.get("home_platoon_lineup_woba", 0), 3),
                    "umpire_strike_rate": round(features.get("ump_career_strike_rate", 0), 3),
                    "park_factor_runs": round(features.get("env_park_factor_runs", 100), 1),
                    "elevation_ft": round(features.get("env_elevation_ft", 0)),
                },
            })
        except Exception as exc:
            logger.warning("  %d: Error -- %s", gpk, exc)
            if args.verbose:
                import traceback
                traceback.print_exc()

    logger.info("Hourly check: %d new | %d already done | %d no starters | %d no lineup | %d started/past",
                len(predictions), skipped_already, skipped_no_starters, skipped_no_lineup, skipped_started)

    if not predictions:
        if skipped_already == len(games):
            logger.info("All %d games already predicted — nothing new to do", len(games))
        else:
            logger.info("No new games ready for prediction yet")
        # Still exit 0 so the workflow doesn't fail
        if args.json_only:
            print(json.dumps({"new_predictions": 0, "already_predicted": skipped_already,
                               "waiting_lineups": skipped_no_lineup, "waiting_starters": skipped_no_starters}))
        sys.exit(0)

    # Save predictions JSON — merge with earlier runs from same day
    PREDICTIONS_DIR.mkdir(parents=True, exist_ok=True)
    out_path = PREDICTIONS_DIR / f"{date_str}.json"

    # If file exists from an earlier run, merge (update existing games, add new ones)
    all_predictions = {}
    if out_path.exists():
        try:
            prev = json.loads(out_path.read_text())
            for g in prev.get("games", []):
                all_predictions[g["game_pk"]] = g
            logger.info("Merging with %d games from earlier run", len(all_predictions))
        except Exception:
            pass

    # Current run's predictions overwrite earlier ones for same game_pk
    for p in predictions:
        all_predictions[p["game_pk"]] = p

    merged = list(all_predictions.values())

    out_data = {
        "date": date_str,
        "model": "v4-lgb-two-model",
        "calibration_k": CALIBRATION_K,
        "model_mean": model_mean,
        "bankroll": BANKROLL,
        "bankroll_current": round(args.bankroll, 2),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "games": merged,
        "bets": [p for p in merged if p["passes_filter"]],
        "summary": {
            "total_games": len(merged),
            "total_bets": sum(1 for p in merged if p["passes_filter"]),
            "yrfi_bets": sum(1 for p in merged if p["passes_filter"] and p["bet_side"] == "YRFI"),
            "nrfi_bets": sum(1 for p in merged if p["passes_filter"] and p["bet_side"] == "NRFI"),
            "total_exposure": sum(p["stake"] for p in merged if p["passes_filter"]),
        },
    }
    out_path.write_text(json.dumps(out_data, indent=2, default=str))
    logger.info("Saved predictions to %s (%d games)", out_path, len(merged))

    # Print summary (unless JSON-only mode for CI)
    if args.json_only:
        print(json.dumps(out_data["summary"]))
        return

    print()
    print("=" * 95)
    print(f"  YRFI/NRFI v4 PREDICTIONS — {date_str}")
    print(f"  Model: LGB Two-Model + Market-Anchored Calibration (k={CALIBRATION_K})")
    print(f"  Filter: Sub-model Agreement + Kelly > {MIN_KELLY_PCT:.0%}")
    print(f"  Bankroll: ${args.bankroll:,.0f}")
    print("=" * 95)
    print()

    print("ALL GAMES:")
    print("%-4s %-25s %-14s %-14s %5s %5s %5s %5s %5s %5s %-6s" % (
        "#", "Matchup", "Away SP", "Home SP",
        "pTop", "pBot", "pRaw", "pCal", "MktY", "Edge", "Agree"))
    print("-" * 120)

    for i, p in enumerate(sorted(predictions, key=lambda x: abs(x["bet_edge"]), reverse=True), 1):
        matchup = f"{p['away_team']} @ {p['home_team']}"
        edge_str = "%+.1f%%" % (p["bet_edge"] * 100) if p["bet_side"] else " --"
        print("%-4d %-25s %-14s %-14s %4.1f%% %4.1f%% %4.1f%% %4.1f%% %4.1f%% %s %-6s" % (
            i, matchup,
            p["away_starter"][:13], p["home_starter"][:13],
            p["p_top"] * 100, p["p_bot"] * 100,
            p["p_raw"] * 100, p["p_cal"] * 100,
            p["mkt_y_fair"] * 100,
            edge_str, p["agreement"]))

    bets = [p for p in predictions if p["passes_filter"]]
    if bets:
        total_exposure = sum(b["stake"] for b in bets)

        print()
        print("=" * 95)
        print("  RECOMMENDED BETS (Agreement + Kelly > %.0f%%)" % (MIN_KELLY_PCT * 100))
        print("=" * 95)
        print()
        print("%-25s %-5s %6s %6s %6s %+8s %8s %6s" % (
            "Game", "Side", "Model", "Mkt", "Edge", "Odds", "Stake", "Kelly"))
        print("-" * 95)

        for b in sorted(bets, key=lambda x: x["bet_edge"], reverse=True):
            matchup = f"{b['away_team']} @ {b['home_team']}"
            model_pct = b["p_cal"] * 100 if b["bet_side"] == "YRFI" else (1 - b["p_cal"]) * 100
            mkt_pct = b["mkt_y_fair"] * 100 if b["bet_side"] == "YRFI" else b["mkt_n_fair"] * 100
            print("%-25s %-5s %5.1f%% %5.1f%% %5.1f%% %+8.0f $%7.2f %5.1f%%" % (
                matchup, b["bet_side"],
                model_pct, mkt_pct,
                b["bet_edge"] * 100,
                b["bet_odds"], b["stake"],
                b["bet_kelly"] * 100))

        print("-" * 95)
        print("Total: %d bets | Exposure: $%.2f (%.1f%% of bankroll) | Skipped: %d" % (
            len(bets), total_exposure, total_exposure / args.bankroll * 100,
            len(predictions) - len(bets)))
    else:
        print()
        print("No bets pass the selectivity filter today.")
        skipped = [p for p in predictions if not p["passes_filter"]]
        reasons = {}
        for p in skipped:
            r = p.get("skip_reason", "unknown")
            reasons[r] = reasons.get(r, 0) + 1
        if reasons:
            print("Skip reasons: %s" % ", ".join(
                f"{k}={v}" for k, v in sorted(reasons.items(), key=lambda x: -x[1])))


if __name__ == "__main__":
    main()
