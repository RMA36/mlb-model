"""
Daily Incremental Stats Updater

Downloads yesterday's Statcast data (~1 day, ~10MB), updates running
tallies per pitcher/batter/umpire, and rebuilds lookup tables.

This replicates the expanding-window computation from training with
a 1-day lag, running entirely on GitHub Actions (no laptop needed).

The bi-weekly full retrain overwrites these tallies from raw Statcast,
correcting any accumulated drift.

Usage:
    python update_daily_stats.py              # update with yesterday's data
    python update_daily_stats.py --date 2026-04-15
    python update_daily_stats.py --init       # initialize tallies from existing lookups
"""

import argparse
import csv
import json
import logging
import math
import sys
import time
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s")
logger = logging.getLogger(__name__)

ROOT = Path(__file__).parent
TALLIES_PATH = ROOT / "models" / "v4" / "tallies.json"
LOOKUPS_PATH = ROOT / "models" / "v4" / "lookups.json"
MODEL_DIR = ROOT / "models" / "v4"

STADIUM_CSV_LOCAL = Path("C:/Users/ander/Documents/Betting Models/MLB/data/stadium_metadata.csv")
STADIUM_CSV_REPO = ROOT / "models" / "v4" / "stadium_metadata.csv"

ET = ZoneInfo("America/New_York")

# wOBA weights (same as retrain_v4.py)
WOBA_WEIGHTS = {
    "single": 0.888, "double": 1.271, "triple": 1.616,
    "home_run": 2.101, "walk": 0.690, "hit_by_pitch": 0.722,
}

REACH_EVENTS = {"single", "double", "triple", "home_run", "walk", "hit_by_pitch"}

LEAGUE_AVG_K_RATE = 0.225
LEAGUE_AVG_BB_RATE = 0.080
LEAGUE_AVG_HR_RATE = 0.030
LEAGUE_AVG_WOBA = 0.310
LEAGUE_AVG_OBP = 0.310
LEAGUE_AVG_STRIKE_RATE = 0.34

MLB_API = "https://statsapi.mlb.com"


# ── Helper: ensure pitcher tally has all fields ───────────────────────────

def _ensure_pitcher_fields(t: dict) -> dict:
    """Add any missing fields to a pitcher tally (backward compat)."""
    defaults = {
        "pa": 0, "k": 0, "bb": 0, "hr": 0, "hits": 0, "hbp": 0,
        "gb": 0, "bip": 0,
        "pa_vs_L": 0, "k_vs_L": 0, "bb_vs_L": 0, "hr_vs_L": 0, "reach_vs_L": 0,
        "pa_vs_R": 0, "k_vs_R": 0, "bb_vs_R": 0, "hr_vs_R": 0, "reach_vs_R": 0,
        "fi_pa": 0, "fi_k": 0, "fi_bb": 0, "fi_hr": 0,
        "fi_runs": 0.0, "fi_starts": 0,
        "fi_pa_vs_L": 0, "fi_k_vs_L": 0, "fi_bb_vs_L": 0, "fi_bb_vs_R": 0,
        "fi_pa_vs_R": 0, "fi_k_vs_R": 0,
        "velo_sum": 0.0, "velo_n": 0,
        "season_pa": 0, "season_k": 0, "season_bb": 0, "season_hr": 0,
        "season_velo_sum": 0.0, "season_velo_n": 0, "current_season": 0,
        "recent_fi_games": [],  # list of last 3 FI game dicts
        "last_game_date": None,
    }
    for k, v in defaults.items():
        if k not in t:
            t[k] = v
    return t


def _ensure_batter_fields(t: dict) -> dict:
    """Add any missing fields to a batter tally (backward compat)."""
    defaults = {
        "pa": 0, "woba_num": 0.0,
        "hits": 0, "bb": 0, "hbp": 0,
        "pa_vs_L": 0, "woba_num_vs_L": 0.0,
        "hits_vs_L": 0, "bb_vs_L": 0, "hbp_vs_L": 0,
        "pa_vs_R": 0, "woba_num_vs_R": 0.0,
        "hits_vs_R": 0, "bb_vs_R": 0, "hbp_vs_R": 0,
    }
    for k, v in defaults.items():
        if k not in t:
            t[k] = v
    return t


def _ensure_umpire_fields(t: dict) -> dict:
    """Add any missing fields to an umpire tally (backward compat)."""
    defaults = {
        "total_pitches": 0, "called_strikes": 0,
        "n_games": 0, "sum_rate": 0.0, "sum_rate_sq": 0.0,
    }
    for k, v in defaults.items():
        if k not in t:
            t[k] = v
    return t


def _new_pitcher_tally() -> dict:
    return {
        "pa": 0, "k": 0, "bb": 0, "hr": 0, "hits": 0, "hbp": 0,
        "gb": 0, "bip": 0,
        "pa_vs_L": 0, "k_vs_L": 0, "bb_vs_L": 0, "hr_vs_L": 0, "reach_vs_L": 0,
        "pa_vs_R": 0, "k_vs_R": 0, "bb_vs_R": 0, "hr_vs_R": 0, "reach_vs_R": 0,
        "fi_pa": 0, "fi_k": 0, "fi_bb": 0, "fi_hr": 0,
        "fi_runs": 0.0, "fi_starts": 0,
        "fi_pa_vs_L": 0, "fi_k_vs_L": 0, "fi_bb_vs_L": 0, "fi_bb_vs_R": 0,
        "fi_pa_vs_R": 0, "fi_k_vs_R": 0,
        "velo_sum": 0.0, "velo_n": 0,
        "season_pa": 0, "season_k": 0, "season_bb": 0, "season_hr": 0,
        "season_velo_sum": 0.0, "season_velo_n": 0, "current_season": 0,
        "recent_fi_games": [],
        "last_game_date": None,
    }


def _new_batter_tally() -> dict:
    return {
        "pa": 0, "woba_num": 0.0,
        "hits": 0, "bb": 0, "hbp": 0,
        "pa_vs_L": 0, "woba_num_vs_L": 0.0,
        "hits_vs_L": 0, "bb_vs_L": 0, "hbp_vs_L": 0,
        "pa_vs_R": 0, "woba_num_vs_R": 0.0,
        "hits_vs_R": 0, "bb_vs_R": 0, "hbp_vs_R": 0,
    }


def _new_umpire_tally() -> dict:
    return {
        "total_pitches": 0, "called_strikes": 0,
        "n_games": 0, "sum_rate": 0.0, "sum_rate_sq": 0.0,
    }


# ── Statcast download (single day) ───────────────────────────────────────

def download_statcast_day(date_str: str) -> pd.DataFrame:
    """Download one day of Statcast data from Baseball Savant."""
    url = (
        f"https://baseballsavant.mlb.com/statcast_search/csv"
        f"?all=true&type=details"
        f"&game_date_gt={date_str}&game_date_lt={date_str}"
        f"&hfGT=R%7C"  # Regular season
    )
    logger.info("Downloading Statcast for %s...", date_str)
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": "Mozilla/5.0 (MLB-Model/1.0)"
        })
        with urllib.request.urlopen(req, timeout=60) as resp:
            import io
            data = resp.read().decode("utf-8")
            if len(data) < 100 or "error" in data.lower()[:200]:
                logger.warning("No Statcast data for %s (empty response)", date_str)
                return pd.DataFrame()
            df = pd.read_csv(io.StringIO(data), low_memory=False)
            logger.info("  Got %d pitches from %d games", len(df),
                        df["game_pk"].nunique() if "game_pk" in df.columns else 0)
            return df
    except Exception as e:
        logger.error("Failed to download Statcast for %s: %s", date_str, e)
        return pd.DataFrame()


# ── MLB Stats API for umpire data ────────────────────────────────────────

def fetch_umpire_assignments(date_str: str) -> dict:
    """Fetch HP umpire for each game from MLB Stats API."""
    url = f"{MLB_API}/api/v1/schedule?date={date_str}&sportId=1&hydrate=officials"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "MLB-Model/1.0"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read())

        umpires = {}
        for date_info in data.get("dates", []):
            for game in date_info.get("games", []):
                gpk = game["gamePk"]
                for official in game.get("officials", []):
                    if official.get("officialType") == "Home Plate":
                        name = official.get("official", {}).get("fullName", "")
                        if name:
                            umpires[gpk] = name
        return umpires
    except Exception as e:
        logger.warning("Failed to fetch umpire data for %s: %s", date_str, e)
        return {}


# ── Tally management ─────────────────────────────────────────────────────

def load_tallies() -> dict:
    """Load running tallies. Returns empty structure if none exist."""
    if TALLIES_PATH.exists():
        tallies = json.loads(TALLIES_PATH.read_text())
        # Ensure backward compat for all sub-dicts
        for pid, t in tallies.get("pitchers", {}).items():
            _ensure_pitcher_fields(t)
        for bid, t in tallies.get("batters", {}).items():
            _ensure_batter_fields(t)
        for name, t in tallies.get("umpires", {}).items():
            _ensure_umpire_fields(t)
        return tallies
    return {
        "last_updated": None,
        "dates_processed": [],
        "pitchers": {},
        "batters": {},
        "umpires": {},
    }


def save_tallies(tallies: dict):
    """Save running tallies."""
    TALLIES_PATH.parent.mkdir(parents=True, exist_ok=True)
    TALLIES_PATH.write_text(json.dumps(tallies, indent=2))


def _detect_season(date_str: str) -> int:
    """Extract year from YYYY-MM-DD date string."""
    return int(date_str[:4])


def update_pitcher_tallies(tallies: dict, statcast: pd.DataFrame, date_str: str = None):
    """Update pitcher running tallies from one day of Statcast."""
    if statcast.empty:
        return

    events = statcast[statcast["events"].notna()].copy()
    if events.empty:
        return

    current_season = _detect_season(date_str) if date_str else 0

    for pid, grp in events.groupby("pitcher"):
        pid_str = str(int(pid))
        if pid_str not in tallies["pitchers"]:
            tallies["pitchers"][pid_str] = _new_pitcher_tally()
        t = _ensure_pitcher_fields(tallies["pitchers"][pid_str])

        # Season reset check
        if current_season > 0 and t["current_season"] > 0 and current_season != t["current_season"]:
            t["season_pa"] = 0
            t["season_k"] = 0
            t["season_bb"] = 0
            t["season_hr"] = 0
            t["season_velo_sum"] = 0.0
            t["season_velo_n"] = 0
        if current_season > 0:
            t["current_season"] = current_season

        is_k = grp["events"].str.contains("strikeout", na=False)
        is_bb = grp["events"].str.contains("walk", na=False) & ~grp["events"].str.contains("strikeout", na=False)
        is_hr = grp["events"] == "home_run"
        is_hit = grp["events"].isin(["single", "double", "triple", "home_run"])
        is_hbp = grp["events"] == "hit_by_pitch"
        is_reach = grp["events"].isin(REACH_EVENTS)

        n_pa = len(grp)
        n_k = int(is_k.sum())
        n_bb = int(is_bb.sum())
        n_hr = int(is_hr.sum())

        t["pa"] += n_pa
        t["k"] += n_k
        t["bb"] += n_bb
        t["hr"] += n_hr
        t["hits"] += int(is_hit.sum())
        t["hbp"] += int(is_hbp.sum())

        # Season-level
        t["season_pa"] += n_pa
        t["season_k"] += n_k
        t["season_bb"] += n_bb
        t["season_hr"] += n_hr

        if "bb_type" in grp.columns:
            t["gb"] += int((grp["bb_type"] == "ground_ball").sum())
            t["bip"] += int(grp["bb_type"].notna().sum())

        # Platoon splits
        for hand in ["L", "R"]:
            hg = grp[grp["stand"] == hand] if "stand" in grp.columns else pd.DataFrame()
            if len(hg) == 0:
                continue
            t[f"pa_vs_{hand}"] += len(hg)
            t[f"k_vs_{hand}"] += int(hg["events"].str.contains("strikeout", na=False).sum())
            t[f"bb_vs_{hand}"] += int(
                (hg["events"].str.contains("walk", na=False) &
                 ~hg["events"].str.contains("strikeout", na=False)).sum()
            )
            t[f"hr_vs_{hand}"] += int((hg["events"] == "home_run").sum())
            t[f"reach_vs_{hand}"] += int(hg["events"].isin(REACH_EVENTS).sum())

        # First-inning stats
        fi = grp[grp["inning"] == 1] if "inning" in grp.columns else pd.DataFrame()
        if len(fi) > 0:
            fi_k_n = int(fi["events"].str.contains("strikeout", na=False).sum())
            fi_bb_n = int(
                (fi["events"].str.contains("walk", na=False) &
                 ~fi["events"].str.contains("strikeout", na=False)).sum()
            )
            fi_hr_n = int((fi["events"] == "home_run").sum())

            t["fi_pa"] += len(fi)
            t["fi_k"] += fi_k_n
            t["fi_bb"] += fi_bb_n
            t["fi_hr"] += fi_hr_n

            for hand in ["L", "R"]:
                fih = fi[fi["stand"] == hand] if "stand" in fi.columns else pd.DataFrame()
                if len(fih) == 0:
                    continue
                t[f"fi_pa_vs_{hand}"] += len(fih)
                t[f"fi_k_vs_{hand}"] += int(fih["events"].str.contains("strikeout", na=False).sum())
                t[f"fi_bb_vs_{hand}"] += int(
                    (fih["events"].str.contains("walk", na=False) &
                     ~fih["events"].str.contains("strikeout", na=False)).sum()
                )

        # last_game_date: update for each pitcher that appeared today
        if date_str:
            t["last_game_date"] = date_str

    # ── First-inning runs: per (pitcher, game_pk), max post_bat_score in inning==1 ──
    if "post_bat_score" in statcast.columns and "inning" in statcast.columns:
        fi_pitches = statcast[
            (statcast["inning"] == 1) &
            statcast["events"].notna() &
            statcast["post_bat_score"].notna()
        ]
        if not fi_pitches.empty:
            fi_runs_df = (
                fi_pitches.groupby(["pitcher", "game_pk"])["post_bat_score"]
                .max()
                .reset_index()
                .rename(columns={"post_bat_score": "fi_runs_game"})
            )
            # Also track fi_starts per (pitcher, game_pk) — one start per game
            fi_starts_df = fi_pitches.groupby("pitcher")["game_pk"].nunique().reset_index()
            fi_starts_df.columns = ["pitcher", "n_starts"]

            for _, row in fi_runs_df.iterrows():
                pid_str = str(int(row["pitcher"]))
                if pid_str in tallies["pitchers"]:
                    tallies["pitchers"][pid_str]["fi_runs"] += float(row["fi_runs_game"])

            for _, row in fi_starts_df.iterrows():
                pid_str = str(int(row["pitcher"]))
                if pid_str in tallies["pitchers"]:
                    tallies["pitchers"][pid_str]["fi_starts"] += int(row["n_starts"])

            # ── Recent FI games: track last 3 ──
            for (pid, gpk), sub in fi_pitches.groupby(["pitcher", "game_pk"]):
                pid_str = str(int(pid))
                if pid_str not in tallies["pitchers"]:
                    continue
                t = tallies["pitchers"][pid_str]
                fi_events = sub[sub["events"].notna()]
                game_dict = {
                    "pa": len(fi_events),
                    "k": int(fi_events["events"].str.contains("strikeout", na=False).sum()),
                    "bb": int(
                        (fi_events["events"].str.contains("walk", na=False) &
                         ~fi_events["events"].str.contains("strikeout", na=False)).sum()
                    ),
                    "runs": float(sub["post_bat_score"].max()),
                }
                recent = t.get("recent_fi_games", [])
                recent.append(game_dict)
                t["recent_fi_games"] = recent[-3:]  # keep last 3

    # ── Velocity (from all pitches, not just events) ──
    fb_types = ["FF", "SI", "FT", "FA"]
    if "pitch_type" in statcast.columns and "release_speed" in statcast.columns:
        fb = statcast[
            statcast["pitch_type"].isin(fb_types) &
            statcast["release_speed"].notna()
        ]
        for pid, grp in fb.groupby("pitcher"):
            pid_str = str(int(pid))
            if pid_str in tallies["pitchers"]:
                vsum = float(grp["release_speed"].sum())
                vn = len(grp)
                tallies["pitchers"][pid_str]["velo_sum"] += vsum
                tallies["pitchers"][pid_str]["velo_n"] += vn
                tallies["pitchers"][pid_str]["season_velo_sum"] += vsum
                tallies["pitchers"][pid_str]["season_velo_n"] += vn


def update_batter_tallies(tallies: dict, statcast: pd.DataFrame):
    """Update batter running tallies from one day of Statcast."""
    if statcast.empty:
        return

    events = statcast[statcast["events"].notna()].copy()
    if events.empty:
        return

    for bid, grp in events.groupby("batter"):
        bid_str = str(int(bid))
        if bid_str not in tallies["batters"]:
            tallies["batters"][bid_str] = _new_batter_tally()
        t = _ensure_batter_fields(tallies["batters"][bid_str])

        is_hit = grp["events"].isin(["single", "double", "triple", "home_run"])
        is_bb = grp["events"].str.contains("walk", na=False) & ~grp["events"].str.contains("strikeout", na=False)
        is_hbp = grp["events"] == "hit_by_pitch"

        t["pa"] += len(grp)
        t["hits"] += int(is_hit.sum())
        t["bb"] += int(is_bb.sum())
        t["hbp"] += int(is_hbp.sum())

        for event_type, weight in WOBA_WEIGHTS.items():
            t["woba_num"] += float((grp["events"] == event_type).sum()) * weight

        # Platoon splits (by pitcher hand)
        if "p_throws" in grp.columns:
            for hand in ["L", "R"]:
                hg = grp[grp["p_throws"] == hand]
                if len(hg) == 0:
                    continue
                t[f"pa_vs_{hand}"] += len(hg)
                t[f"hits_vs_{hand}"] += int(hg["events"].isin(
                    ["single", "double", "triple", "home_run"]).sum())
                t[f"bb_vs_{hand}"] += int(
                    (hg["events"].str.contains("walk", na=False) &
                     ~hg["events"].str.contains("strikeout", na=False)).sum()
                )
                t[f"hbp_vs_{hand}"] += int((hg["events"] == "hit_by_pitch").sum())
                for event_type, weight in WOBA_WEIGHTS.items():
                    t[f"woba_num_vs_{hand}"] += float((hg["events"] == event_type).sum()) * weight


def update_umpire_tallies(tallies: dict, date_str: str, statcast: pd.DataFrame):
    """Update umpire running tallies with per-game consistency tracking."""
    ump_assignments = fetch_umpire_assignments(date_str)
    if not ump_assignments:
        return

    game_pks = statcast["game_pk"].unique() if not statcast.empty else []

    for gpk in game_pks:
        ump_name = ump_assignments.get(gpk)
        if not ump_name:
            continue

        # Count called strikes from Statcast directly
        game_pitches = statcast[statcast["game_pk"] == gpk]
        if game_pitches.empty:
            continue

        total = len(game_pitches)
        called_strikes = 0
        if "description" in game_pitches.columns:
            called_strikes = int(game_pitches["description"].str.contains(
                "called_strike", case=False, na=False).sum())
        elif "type" in game_pitches.columns:
            called_strikes = int((game_pitches["type"] == "S").sum())

        if total > 0:
            if ump_name not in tallies["umpires"]:
                tallies["umpires"][ump_name] = _new_umpire_tally()
            t = _ensure_umpire_fields(tallies["umpires"][ump_name])

            game_rate = called_strikes / total

            t["total_pitches"] += total
            t["called_strikes"] += called_strikes
            t["n_games"] += 1
            t["sum_rate"] += game_rate
            t["sum_rate_sq"] += game_rate * game_rate


# ── Stadium metadata loader ──────────────────────────────────────────────

def load_stadium_metadata(csv_path: Path = None) -> dict:
    """
    Load stadium metadata from CSV. Returns dict keyed by stadium_name.
    Park factors are stored as integers (106 means +6% runs) and kept in
    that scale to match the training feature matrix.
    """
    if csv_path is None:
        csv_path = STADIUM_CSV_REPO if STADIUM_CSV_REPO.exists() else STADIUM_CSV_LOCAL
    if not csv_path.exists():
        logger.warning("Stadium metadata not found at %s", csv_path)
        return {}

    parks = {}
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            name = row.get("stadium_name", "").strip()
            if not name:
                continue
            roof = row.get("roof_type", "open").strip().lower()
            pfr_raw = float(row.get("park_factor_runs", 100))
            pfh_raw = float(row.get("park_factor_hr", 100))
            parks[name] = {
                "pfr": pfr_raw,  # integer scale (e.g. 106), matches training
                "pfh": pfh_raw,  # integer scale (e.g. 112), matches training
                "elev": int(float(row.get("elevation_ft", 0))),
                "dome": 1 if roof in ("dome", "retractable") else 0,
            }
    logger.info("Loaded %d park entries from %s", len(parks), csv_path)
    return parks


# ── Rebuild lookups from tallies ─────────────────────────────────────────

def rebuild_lookups(tallies: dict):
    """Convert running tallies into the lookup.json format used by predict.py."""

    # ── Pitcher lookup ──
    pitcher_lookup = {}
    for pid_str, t in tallies["pitchers"].items():
        t = _ensure_pitcher_fields(t)
        if t["pa"] < 20:
            continue

        pa = t["pa"]
        k_rate = t["k"] / pa if pa > 0 else LEAGUE_AVG_K_RATE
        bb_rate = t["bb"] / pa if pa > 0 else LEAGUE_AVG_BB_RATE
        hr_rate = t["hr"] / pa if pa > 0 else LEAGUE_AVG_HR_RATE
        avg_velo = t["velo_sum"] / t["velo_n"] if t["velo_n"] > 50 else 93.0
        gb_rate = t["gb"] / t["bip"] if t["bip"] > 10 else 0.45

        # WHIP approx = (hits + bb) / (outs/3) where outs = pa - hits - bb - hbp, clipped >= 1
        outs = max(pa - t["hits"] - t["bb"] - t["hbp"], 1)
        whip = (t["hits"] + t["bb"]) / (outs / 3.0)

        # Season rates
        spa = t.get("season_pa", 0)
        s_k_rate = t.get("season_k", 0) / spa if spa > 20 else k_rate
        s_bb_rate = t.get("season_bb", 0) / spa if spa > 20 else bb_rate
        s_hr_rate = t.get("season_hr", 0) / spa if spa > 20 else hr_rate

        d = {
            "v": round(avg_velo, 1),
            "k": round(k_rate, 4),
            "bb": round(bb_rate, 4),
            "hr": round(hr_rate, 4),
            "sk": round(s_k_rate, 4),
            "sbb": round(s_bb_rate, 4),
            "shr": round(s_hr_rate, 4),
            "gb": round(gb_rate, 4),
            "whip": round(whip, 3),
        }

        # Platoon splits
        for h in ["L", "R"]:
            pa_h = t[f"pa_vs_{h}"]
            d[f"k{h}"] = round(t[f"k_vs_{h}"] / pa_h, 4) if pa_h > 10 else round(k_rate, 4)
            d[f"bb{h}"] = round(t[f"bb_vs_{h}"] / pa_h, 4) if pa_h > 10 else round(bb_rate, 4)
            d[f"hr{h}"] = round(t[f"hr_vs_{h}"] / pa_h, 4) if pa_h > 10 else round(hr_rate, 4)
            d[f"obp{h}"] = round(t[f"reach_vs_{h}"] / pa_h, 4) if pa_h > 10 else round(
                (t["hits"] + t["bb"] + t["hbp"]) / pa if pa > 0 else 0.310, 4)

        # First-inning rates
        fi_pa = t["fi_pa"]
        fi_k = t["fi_k"] / fi_pa if fi_pa > 10 else k_rate
        fi_bb = t["fi_bb"] / fi_pa if fi_pa > 10 else bb_rate
        fi_hr = t["fi_hr"] / fi_pa if fi_pa > 10 else hr_rate
        fi_starts = t.get("fi_starts", 0)
        fi_runs_per_start = t["fi_runs"] / fi_starts if fi_starts > 3 else 0.5

        d["fik"] = round(fi_k, 4)
        d["fibb"] = round(fi_bb, 4)
        d["fihr"] = round(fi_hr, 4)
        d["fir"] = round(fi_runs_per_start, 3)
        d["fis"] = fi_starts

        for h in ["L", "R"]:
            fi_pa_h = t[f"fi_pa_vs_{h}"]
            d[f"fk{h}"] = round(t[f"fi_k_vs_{h}"] / fi_pa_h, 4) if fi_pa_h > 5 else round(fi_k, 4)

        # Recent FI stats (last 3 games)
        recent = t.get("recent_fi_games", [])
        if recent:
            r_pa = sum(g.get("pa", 0) for g in recent)
            r_k = sum(g.get("k", 0) for g in recent)
            r_bb = sum(g.get("bb", 0) for g in recent)
            r_runs = sum(g.get("runs", 0) for g in recent)
            n_recent = len(recent)
            d["rfik"] = round(r_k / r_pa, 4) if r_pa > 0 else round(fi_k, 4)
            d["rfibb"] = round(r_bb / r_pa, 4) if r_pa > 0 else round(fi_bb, 4)
            d["rfir"] = round(r_runs / n_recent, 3) if n_recent > 0 else round(fi_runs_per_start, 3)
        else:
            d["rfik"] = round(fi_k, 4)
            d["rfibb"] = round(fi_bb, 4)
            d["rfir"] = round(fi_runs_per_start, 3)

        # Last game date for rest days
        if t.get("last_game_date"):
            d["lgd"] = t["last_game_date"]

        pitcher_lookup[pid_str] = d

    # ── Batter lookup ──
    batter_lookup = {}
    for bid_str, t in tallies["batters"].items():
        t = _ensure_batter_fields(t)
        if t["pa"] < 20:
            continue

        pa = t["pa"]
        woba = t["woba_num"] / pa if pa > 0 else LEAGUE_AVG_WOBA
        obp = (t["hits"] + t["bb"] + t["hbp"]) / pa if pa > 0 else LEAGUE_AVG_OBP

        d = {
            "w": round(woba, 4),
            "o": round(obp, 4),
        }

        for h in ["L", "R"]:
            pa_h = t[f"pa_vs_{h}"]
            if pa_h >= 10:
                d[f"w{h}"] = round(t[f"woba_num_vs_{h}"] / pa_h, 4)
                d[f"o{h}"] = round(
                    (t[f"hits_vs_{h}"] + t[f"bb_vs_{h}"] + t[f"hbp_vs_{h}"]) / pa_h, 4)
            else:
                d[f"w{h}"] = round(woba, 4)
                d[f"o{h}"] = round(obp, 4)

        batter_lookup[bid_str] = d

    # ── Umpire lookup ──
    umpire_lookup = {}
    for name, t in tallies["umpires"].items():
        t = _ensure_umpire_fields(t)
        if t["total_pitches"] < 100:
            continue
        sr = t["called_strikes"] / t["total_pitches"]
        n_games = t.get("n_games", 0)

        # Consistency = std dev of per-game called strike rates
        con = 0.0
        if n_games >= 3:
            mean_rate = t["sum_rate"] / n_games
            variance = t["sum_rate_sq"] / n_games - mean_rate * mean_rate
            con = math.sqrt(max(variance, 0.0))

        umpire_lookup[name] = {
            "sr": round(sr, 4),
            "con": round(con, 4),
            "gc": n_games,
        }

    # ── Park lookup ──
    # Load fresh from stadium_metadata.csv if available, else preserve existing
    park_lookup = load_stadium_metadata()
    if not park_lookup and LOOKUPS_PATH.exists():
        existing = json.loads(LOOKUPS_PATH.read_text())
        park_lookup = existing.get("parks", {})

    lookups = {
        "pitchers": pitcher_lookup,
        "batters": batter_lookup,
        "umpires": umpire_lookup,
        "parks": park_lookup,
    }

    LOOKUPS_PATH.parent.mkdir(parents=True, exist_ok=True)
    LOOKUPS_PATH.write_text(json.dumps(lookups, indent=2))
    size_kb = LOOKUPS_PATH.stat().st_size / 1024

    logger.info("Lookups rebuilt: %d pitchers, %d batters, %d umpires, %d parks (%.1f KB)",
                len(pitcher_lookup), len(batter_lookup), len(umpire_lookup),
                len(park_lookup), size_kb)

    return lookups


# ── Initialize tallies from existing Statcast ────────────────────────────

def init_from_retrain():
    """
    Initialize tallies from raw Statcast data (full history).

    Processes game-by-game in date order for correct fi_runs tracking
    (needs post_bat_score) and recent_fi_games (last 3 per pitcher).
    Also initializes umpire consistency and park metadata.
    """
    from pathlib import Path as P

    bm_root = P("C:/Users/ander/Documents/Betting Models/MLB")

    tallies = {
        "last_updated": None,
        "dates_processed": [],
        "pitchers": {},
        "batters": {},
        "umpires": {},
    }

    # Load raw Statcast
    statcast_path = bm_root / "data" / "raw" / "statcast_raw_2023_2025.parquet"
    if not statcast_path.exists():
        logger.error("Statcast raw data not found at %s", statcast_path)
        logger.info("Run the full retrain locally first to initialize tallies.")
        return

    logger.info("Initializing tallies from raw Statcast (%s)...", statcast_path)
    sc = pd.read_parquet(statcast_path)
    logger.info("  Loaded %d pitches", len(sc))

    # Ensure game_date column exists and is string
    if "game_date" not in sc.columns:
        logger.error("Statcast data missing 'game_date' column")
        return
    sc["game_date"] = sc["game_date"].astype(str)

    # Sort by date for correct recent_fi tracking
    dates_sorted = sorted(sc["game_date"].unique())
    logger.info("  Processing %d dates from %s to %s", len(dates_sorted),
                dates_sorted[0], dates_sorted[-1])

    # Process date by date for proper fi_runs and recent_fi tracking
    for i, date_str in enumerate(dates_sorted):
        day_sc = sc[sc["game_date"] == date_str]
        if day_sc.empty:
            continue

        # Update pitcher tallies (with fi_runs, recent_fi, last_game_date, season tracking)
        update_pitcher_tallies(tallies, day_sc, date_str=date_str)

        # Update batter tallies
        update_batter_tallies(tallies, day_sc)

        if (i + 1) % 50 == 0:
            logger.info("  Processed %d / %d dates (%d pitchers, %d batters)",
                        i + 1, len(dates_sorted),
                        len(tallies["pitchers"]), len(tallies["batters"]))

    logger.info("  Pitcher/batter tallies complete: %d pitchers, %d batters",
                len(tallies["pitchers"]), len(tallies["batters"]))

    # ── Umpire tallies from umpire data file ──
    ump_path = bm_root / "data" / "raw" / "umpire_data_2021_2025.parquet"
    if not ump_path.exists():
        ump_path = bm_root / "data" / "raw" / "umpire_data_2023_2025.parquet"
    if ump_path.exists():
        ump_df = pd.read_parquet(ump_path)
        nc = "hp_umpire_name" if ("hp_umpire_name" in ump_df.columns and
                                   ump_df["hp_umpire_name"].notna().sum() > 0) else "umpire"
        tc = "total_called" if "total_called" in ump_df.columns else "total_calls"
        ump_df = ump_df[ump_df[nc].notna()].copy()

        for name, grp in ump_df.groupby(nc):
            total_calls = int(grp[tc].sum())
            called_strikes = int((grp["called_strike_rate"] * grp[tc]).sum())
            n_games = len(grp)

            # Per-game rates for consistency
            sum_rate = 0.0
            sum_rate_sq = 0.0
            for _, row in grp.iterrows():
                rate = float(row["called_strike_rate"])
                sum_rate += rate
                sum_rate_sq += rate * rate

            tallies["umpires"][name] = {
                "total_pitches": total_calls,
                "called_strikes": called_strikes,
                "n_games": n_games,
                "sum_rate": sum_rate,
                "sum_rate_sq": sum_rate_sq,
            }
        logger.info("  Umpire tallies: %d umpires from %s", len(tallies["umpires"]), ump_path)

    tallies["last_updated"] = datetime.now(timezone.utc).isoformat()
    tallies["dates_processed"] = ["init-from-statcast"]

    save_tallies(tallies)
    logger.info("Tallies initialized: %d pitchers, %d batters, %d umpires",
                len(tallies["pitchers"]), len(tallies["batters"]), len(tallies["umpires"]))

    # Rebuild lookups from fresh tallies (includes park metadata from CSV)
    rebuild_lookups(tallies)


# ── Main ─────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Daily incremental stats updater")
    parser.add_argument("--date", help="Date to process (YYYY-MM-DD). Default: yesterday")
    parser.add_argument("--init", action="store_true",
                        help="Initialize tallies from raw Statcast (run locally)")
    parser.add_argument("--json-only", action="store_true",
                        help="Output JSON summary only (for CI)")
    args = parser.parse_args()

    if args.init:
        init_from_retrain()
        return

    # Determine date to process
    if args.date:
        date_str = args.date
    else:
        yesterday = datetime.now(ET) - timedelta(days=1)
        date_str = yesterday.strftime("%Y-%m-%d")

    # Load existing tallies
    tallies = load_tallies()

    if tallies["last_updated"] is None:
        logger.error("No tallies found. Run with --init first (locally).")
        sys.exit(1)

    # Skip if already processed
    if date_str in tallies.get("dates_processed", []):
        logger.info("Date %s already processed — skipping", date_str)
        if args.json_only:
            print(json.dumps({"status": "skipped", "date": date_str}))
        return

    # Download one day of Statcast
    sc = download_statcast_day(date_str)

    if sc.empty:
        logger.info("No Statcast data for %s (off-day or data not yet available)", date_str)
        if args.json_only:
            print(json.dumps({"status": "no_data", "date": date_str}))
        return

    # Update tallies
    prev_pitchers = len(tallies["pitchers"])
    prev_batters = len(tallies["batters"])

    update_pitcher_tallies(tallies, sc, date_str=date_str)
    update_batter_tallies(tallies, sc)
    update_umpire_tallies(tallies, date_str, sc)

    new_pitchers = len(tallies["pitchers"]) - prev_pitchers
    new_batters = len(tallies["batters"]) - prev_batters

    tallies["last_updated"] = datetime.now(timezone.utc).isoformat()
    tallies["dates_processed"].append(date_str)
    save_tallies(tallies)

    logger.info("Tallies updated for %s: %d pitchers (+%d new), %d batters (+%d new)",
                date_str, len(tallies["pitchers"]), new_pitchers,
                len(tallies["batters"]), new_batters)

    # Rebuild lookups
    lookups = rebuild_lookups(tallies)

    if args.json_only:
        print(json.dumps({
            "status": "updated",
            "date": date_str,
            "games": int(sc["game_pk"].nunique()) if "game_pk" in sc.columns else 0,
            "pitches": len(sc),
            "pitchers": len(lookups["pitchers"]),
            "batters": len(lookups["batters"]),
            "umpires": len(lookups["umpires"]),
            "parks": len(lookups["parks"]),
            "new_pitchers": new_pitchers,
            "new_batters": new_batters,
        }))


if __name__ == "__main__":
    main()
