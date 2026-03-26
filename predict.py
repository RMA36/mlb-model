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
MAX_DAILY_PCT = 0.20
MIN_KELLY_PCT = 0.01
BANKROLL = 1000.0

# League averages (fallback for unknown players)
LEAGUE_AVG_K_RATE = 0.225
LEAGUE_AVG_WOBA = 0.310
LEAGUE_AVG_OBP = 0.310
LEAGUE_AVG_STRIKE_RATE = 0.34

MLB_API = "https://statsapi.mlb.com"

# Odds master file from companion tracker repo
ODDS_MASTER_REPO = "RMA36/mlb-odds-tracker-2026"
ODDS_MASTER_PATH = "data/2026/odds_master_2026.parquet"
ODDS_CACHE = ROOT / ".cache" / "odds_master_2026.parquet"

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
    "New York Yankees": "NYY", "Oakland Athletics": "OAK",
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
    """Load YRFI/NRFI odds for a specific date from the odds master file."""
    odds_path = download_odds_master()
    if odds_path is None or not odds_path.exists():
        return pd.DataFrame()

    df = pd.read_parquet(odds_path)
    fi = df[df["market"] == "totals_1st_1_innings"].copy()
    if fi.empty:
        logger.warning("No totals_1st_1_innings market in odds master")
        return pd.DataFrame()

    # Use commence_time (UTC) → ET for true game date
    fi["commence_utc"] = pd.to_datetime(fi["commence_time"], utc=True)
    fi["commence_et"] = fi["commence_utc"].dt.tz_convert("US/Eastern")
    fi["game_day"] = fi["commence_et"].dt.strftime("%Y-%m-%d")
    fi = fi[fi["game_day"] == date_str].copy()

    if fi.empty:
        all_days = df[df["market"] == "totals_1st_1_innings"].copy()
        all_days["commence_utc"] = pd.to_datetime(all_days["commence_time"], utc=True)
        all_days["commence_et"] = all_days["commence_utc"].dt.tz_convert("US/Eastern")
        all_days["game_day"] = all_days["commence_et"].dt.strftime("%Y-%m-%d")
        avail = sorted(all_days["game_day"].unique())[-5:]
        logger.warning("No YRFI/NRFI odds for %s. Available: %s", date_str, avail)
        return pd.DataFrame()

    yrfi_rows = fi[fi["outcome_name"] == "Over"].copy()
    nrfi_rows = fi[fi["outcome_name"] == "Under"].copy()

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
        pitcher_stats[pid] = {
            "avg_velo": d.get("v", 93.0),
            "career_k_rate": d.get("k", LEAGUE_AVG_K_RATE),
            "k_rate_vs_L": d.get("kL", d.get("k", LEAGUE_AVG_K_RATE)),
            "k_rate_vs_R": d.get("kR", d.get("k", LEAGUE_AVG_K_RATE)),
            "fi_k_rate_vs_L": d.get("fkL", d.get("k", LEAGUE_AVG_K_RATE)),
            "fi_k_rate_vs_R": d.get("fkR", d.get("k", LEAGUE_AVG_K_RATE)),
        }

    batter_stats = {}
    for bid_str, d in raw.get("batters", {}).items():
        bid = int(bid_str)
        batter_stats[bid] = {
            "woba": d.get("w", LEAGUE_AVG_WOBA),
            "woba_vs_L": d.get("wL", d.get("w", LEAGUE_AVG_WOBA)),
            "woba_vs_R": d.get("wR", d.get("w", LEAGUE_AVG_WOBA)),
        }

    umpire_stats = {name: {"career_strike_rate": rate}
                    for name, rate in raw.get("umpires", {}).items()}

    stadium_meta = {venue: {"park_factor_runs": factor}
                    for venue, factor in raw.get("parks", {}).items()}

    logger.info("Lookups: %d pitchers, %d batters, %d umps, %d parks",
                len(pitcher_stats), len(batter_stats), len(umpire_stats), len(stadium_meta))

    return {
        "pitcher_stats": pitcher_stats,
        "batter_stats": batter_stats,
        "umpire_stats": umpire_stats,
        "stadium_meta": stadium_meta,
    }


def compute_features(game_info, lookups):
    """Compute model features for a single game."""
    features = {}
    ps = lookups.get("pitcher_stats", {})
    bs = lookups.get("batter_stats", {})
    us = lookups.get("umpire_stats", {})
    stadium = lookups.get("stadium_meta", {})

    for side in ["home", "away"]:
        pid = game_info.get(f"{side}_starter_id")
        throws = game_info.get(f"{side}_starter_throws", "R")
        p_stats = ps.get(pid, {})
        prefix = f"{side}_p_"

        features[f"{prefix}avg_velo"] = p_stats.get("avg_velo", 93.0)
        features[f"{prefix}career_k_rate"] = p_stats.get("career_k_rate", LEAGUE_AVG_K_RATE)

        opp_side = "away" if side == "home" else "home"
        opp_hands = game_info.get(f"{opp_side}_lineup_hands", [])
        pct_left = sum(1 for h in opp_hands if h == "L") / len(opp_hands) if opp_hands else 0.5

        k_vs_l = p_stats.get("k_rate_vs_L", LEAGUE_AVG_K_RATE)
        k_vs_r = p_stats.get("k_rate_vs_R", LEAGUE_AVG_K_RATE)
        features[f"{prefix}platoon_k_rate"] = pct_left * k_vs_l + (1 - pct_left) * k_vs_r

        fi_k_vs_l = p_stats.get("fi_k_rate_vs_L", LEAGUE_AVG_K_RATE)
        fi_k_vs_r = p_stats.get("fi_k_rate_vs_R", LEAGUE_AVG_K_RATE)
        features[f"{prefix}fi_platoon_k_rate"] = pct_left * fi_k_vs_l + (1 - pct_left) * fi_k_vs_r

        # Opposing lineup features
        lineup_ids = game_info.get(f"{opp_side}_lineup", [])
        lineup_hands = game_info.get(f"{opp_side}_lineup_hands", [])

        wobas = []
        platoon_wobas = []
        for bid, hand in zip(lineup_ids, lineup_hands):
            b = bs.get(bid, {})
            wobas.append(b.get("woba", LEAGUE_AVG_WOBA))
            if throws == "L":
                platoon_wobas.append(b.get("woba_vs_L", LEAGUE_AVG_WOBA))
            else:
                platoon_wobas.append(b.get("woba_vs_R", LEAGUE_AVG_WOBA))

        if wobas:
            pa_probs = [1.0, 1.0, 1.0, 0.75, 0.40, 0.18][:len(wobas)]
            features[f"{opp_side}_lineup_weighted_score"] = float(np.dot(pa_probs, wobas))
            features[f"{opp_side}_platoon_lineup_woba"] = np.mean(platoon_wobas)
        else:
            features[f"{opp_side}_lineup_weighted_score"] = LEAGUE_AVG_WOBA * 3.33
            features[f"{opp_side}_platoon_lineup_woba"] = LEAGUE_AVG_WOBA

    # Environment
    try:
        features["env_temp_max"] = float(game_info.get("temp", 72))
    except (ValueError, TypeError):
        features["env_temp_max"] = 72.0
    try:
        features["env_humidity"] = float(game_info.get("humidity", 50))
    except (ValueError, TypeError):
        features["env_humidity"] = 50.0

    venue = game_info.get("venue", "")
    park = stadium.get(venue, {})
    features["env_park_factor_runs"] = park.get("park_factor_runs", 1.0)

    # Umpire
    ump = us.get(game_info.get("umpire", "Unknown"), {})
    features["ump_career_strike_rate"] = ump.get("career_strike_rate", LEAGUE_AVG_STRIKE_RATE)

    return features


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

    # Process each game
    predictions = []
    skipped_already = 0
    skipped_no_starters = 0
    skipped_started = 0
    skipped_no_lineup = 0
    for g in games:
        gpk = g["gamePk"]
        try:
            # Skip games already predicted in an earlier hourly run
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

            # Skip games that have already started or are final
            status = info.get("status", "")
            if status in ("In Progress", "Final", "Game Over", "Completed Early"):
                logger.info("  %d (%s): %s — skipping", gpk, tag, status)
                skipped_started += 1
                continue

            # Skip games where lineups haven't been posted yet
            home_lineup = info.get("home_lineup", [])
            away_lineup = info.get("away_lineup", [])
            if len(home_lineup) < 3 or len(away_lineup) < 3:
                logger.info("  %d (%s): Lineups not posted yet (home=%d, away=%d) — skipping",
                            gpk, tag, len(home_lineup), len(away_lineup))
                skipped_no_lineup += 1
                continue

            # Skip games starting in the past (already commenced)
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

            features = compute_features(info, lookups)
            p_top, p_bot, p_raw = predict_game(features, top_model, bot_model, meta)

            home = info["home_team"]
            away = info["away_team"]

            # Match odds by team + commence time (handles doubleheaders)
            # MLB API game_datetime and odds commence_time are both UTC ISO strings
            team_mask = ((daily_odds["home_team_abbr"] == home) &
                         (daily_odds["away_team_abbr"] == away))
            team_odds = daily_odds[team_mask]

            if team_odds.empty:
                logger.info("  %d (%s): No odds match — skipping", gpk, tag)
                continue

            # If multiple games for same team pair (doubleheader), match by closest commence_time
            if len(team_odds) > 1 and game_dt_str:
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

            if edge_y > edge_n and edge_y > 0:
                bet_side, bet_edge, bet_kelly = "YRFI", edge_y, kelly_y
                bet_dec, bet_odds = odds_row["yrfi_dec"], odds_row["yrfi_odds"]
            elif edge_n > 0:
                bet_side, bet_edge, bet_kelly = "NRFI", edge_n, kelly_n
                bet_dec, bet_odds = odds_row["nrfi_dec"], odds_row["nrfi_odds"]

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
                "bet_dec": round(bet_dec, 3),
                "passes_filter": passes,
                "skip_reason": skip,
                "stake": stake,
                "n_books": int(odds_row["n_books"]),
            })

            time.sleep(0.3)
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
        "bankroll": args.bankroll,
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
        if total_exposure > args.bankroll * MAX_DAILY_PCT:
            scale = (args.bankroll * MAX_DAILY_PCT) / total_exposure
            for b in bets:
                b["stake"] = round(b["stake"] * scale, 2)
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
