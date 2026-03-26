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
import json
import logging
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

ET = ZoneInfo("America/New_York")

# wOBA weights (same as retrain_v4.py)
WOBA_WEIGHTS = {
    "single": 0.888, "double": 1.271, "triple": 1.616,
    "home_run": 2.101, "walk": 0.690, "hit_by_pitch": 0.722,
}

LEAGUE_AVG_K_RATE = 0.225
LEAGUE_AVG_WOBA = 0.310
LEAGUE_AVG_STRIKE_RATE = 0.34

MLB_API = "https://statsapi.mlb.com"


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


def fetch_game_called_strikes(game_pk: int) -> dict | None:
    """Fetch called strike data for a game from MLB Stats API."""
    url = f"{MLB_API}/api/v1.1/game/{game_pk}/feed/live"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "MLB-Model/1.0"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read())

        total_pitches = 0
        called_strikes = 0
        for play in data.get("liveData", {}).get("plays", {}).get("allPlays", []):
            for event in play.get("playEvents", []):
                if event.get("isPitch"):
                    total_pitches += 1
                    desc = event.get("details", {}).get("description", "")
                    if "Called Strike" in desc:
                        called_strikes += 1

        if total_pitches > 0:
            return {"total_pitches": total_pitches, "called_strikes": called_strikes}
        return None
    except Exception:
        return None


# ── Tally management ─────────────────────────────────────────────────────

def load_tallies() -> dict:
    """Load running tallies. Returns empty structure if none exist."""
    if TALLIES_PATH.exists():
        return json.loads(TALLIES_PATH.read_text())
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


def update_pitcher_tallies(tallies: dict, statcast: pd.DataFrame):
    """Update pitcher running tallies from one day of Statcast."""
    if statcast.empty:
        return

    events = statcast[statcast["events"].notna()].copy()
    if events.empty:
        return

    for pid, grp in events.groupby("pitcher"):
        pid_str = str(int(pid))
        if pid_str not in tallies["pitchers"]:
            tallies["pitchers"][pid_str] = {
                "pa": 0, "k": 0, "bb": 0, "hr": 0, "hits": 0, "hbp": 0,
                "gb": 0, "bip": 0,
                "pa_vs_L": 0, "k_vs_L": 0, "pa_vs_R": 0, "k_vs_R": 0,
                "fi_pa": 0, "fi_k": 0,
                "fi_pa_vs_L": 0, "fi_k_vs_L": 0,
                "fi_pa_vs_R": 0, "fi_k_vs_R": 0,
                "velo_sum": 0.0, "velo_n": 0,
            }
        t = tallies["pitchers"][pid_str]

        is_k = grp["events"].str.contains("strikeout", na=False)
        is_bb = grp["events"].str.contains("walk", na=False)
        is_hr = grp["events"] == "home_run"
        is_hit = grp["events"].isin(["single", "double", "triple", "home_run"])
        is_hbp = grp["events"] == "hit_by_pitch"

        t["pa"] += len(grp)
        t["k"] += int(is_k.sum())
        t["bb"] += int(is_bb.sum())
        t["hr"] += int(is_hr.sum())
        t["hits"] += int(is_hit.sum())
        t["hbp"] += int(is_hbp.sum())

        if "bb_type" in grp.columns:
            t["gb"] += int((grp["bb_type"] == "ground_ball").sum())
            t["bip"] += int(grp["bb_type"].notna().sum())

        # Platoon splits
        for hand in ["L", "R"]:
            hg = grp[grp["stand"] == hand] if "stand" in grp.columns else pd.DataFrame()
            t[f"pa_vs_{hand}"] += len(hg)
            t[f"k_vs_{hand}"] += int(hg["events"].str.contains("strikeout", na=False).sum()) if len(hg) > 0 else 0

        # First-inning stats
        fi = grp[grp["inning"] == 1] if "inning" in grp.columns else pd.DataFrame()
        if len(fi) > 0:
            t["fi_pa"] += len(fi)
            t["fi_k"] += int(fi["events"].str.contains("strikeout", na=False).sum())
            for hand in ["L", "R"]:
                fih = fi[fi["stand"] == hand] if "stand" in fi.columns else pd.DataFrame()
                t[f"fi_pa_vs_{hand}"] += len(fih)
                t[f"fi_k_vs_{hand}"] += int(fih["events"].str.contains("strikeout", na=False).sum()) if len(fih) > 0 else 0

    # Velocity (from all pitches, not just events)
    fb_types = ["FF", "SI", "FT", "FA"]
    if "pitch_type" in statcast.columns and "release_speed" in statcast.columns:
        fb = statcast[
            statcast["pitch_type"].isin(fb_types) &
            statcast["release_speed"].notna()
        ]
        for pid, grp in fb.groupby("pitcher"):
            pid_str = str(int(pid))
            if pid_str in tallies["pitchers"]:
                tallies["pitchers"][pid_str]["velo_sum"] += float(grp["release_speed"].sum())
                tallies["pitchers"][pid_str]["velo_n"] += len(grp)


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
            tallies["batters"][bid_str] = {
                "pa": 0, "woba_num": 0.0,
                "pa_vs_L": 0, "woba_num_vs_L": 0.0,
                "pa_vs_R": 0, "woba_num_vs_R": 0.0,
            }
        t = tallies["batters"][bid_str]

        t["pa"] += len(grp)
        for event_type, weight in WOBA_WEIGHTS.items():
            t["woba_num"] += float((grp["events"] == event_type).sum()) * weight

        # Platoon splits (by pitcher hand)
        if "p_throws" in grp.columns:
            for hand in ["L", "R"]:
                hg = grp[grp["p_throws"] == hand]
                t[f"pa_vs_{hand}"] += len(hg)
                for event_type, weight in WOBA_WEIGHTS.items():
                    t[f"woba_num_vs_{hand}"] += float((hg["events"] == event_type).sum()) * weight


def update_umpire_tallies(tallies: dict, date_str: str, statcast: pd.DataFrame):
    """Update umpire running tallies."""
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
                tallies["umpires"][ump_name] = {"total_pitches": 0, "called_strikes": 0}
            tallies["umpires"][ump_name]["total_pitches"] += total
            tallies["umpires"][ump_name]["called_strikes"] += called_strikes


# ── Rebuild lookups from tallies ─────────────────────────────────────────

def rebuild_lookups(tallies: dict):
    """Convert running tallies into the lookup.json format used by predict.py."""
    pitcher_lookup = {}
    for pid_str, t in tallies["pitchers"].items():
        if t["pa"] < 20:
            continue

        k_rate = t["k"] / t["pa"] if t["pa"] > 0 else LEAGUE_AVG_K_RATE
        avg_velo = t["velo_sum"] / t["velo_n"] if t["velo_n"] > 50 else 93.0

        d = {"v": round(avg_velo, 1), "k": round(k_rate, 4)}

        for h in ["L", "R"]:
            pa_h = t[f"pa_vs_{h}"]
            d[f"k{h}"] = round(t[f"k_vs_{h}"] / pa_h, 4) if pa_h > 10 else round(k_rate, 4)

        fi_k = t["fi_k"] / t["fi_pa"] if t["fi_pa"] > 10 else k_rate
        for h in ["L", "R"]:
            fi_pa_h = t[f"fi_pa_vs_{h}"]
            d[f"fk{h}"] = round(t[f"fi_k_vs_{h}"] / fi_pa_h, 4) if fi_pa_h > 5 else round(fi_k, 4)

        pitcher_lookup[pid_str] = d

    batter_lookup = {}
    for bid_str, t in tallies["batters"].items():
        if t["pa"] < 20:
            continue

        woba = t["woba_num"] / t["pa"] if t["pa"] > 0 else LEAGUE_AVG_WOBA
        d = {"w": round(woba, 4)}

        for h in ["L", "R"]:
            pa_h = t[f"pa_vs_{h}"]
            if pa_h >= 10:
                d[f"w{h}"] = round(t[f"woba_num_vs_{h}"] / pa_h, 4)
            else:
                d[f"w{h}"] = round(woba, 4)
        batter_lookup[bid_str] = d

    umpire_lookup = {}
    for name, t in tallies["umpires"].items():
        if t["total_pitches"] > 100:
            rate = t["called_strikes"] / t["total_pitches"]
            umpire_lookup[name] = round(rate, 4)

    # Park factors: load from existing lookups (static, don't change)
    park_lookup = {}
    if LOOKUPS_PATH.exists():
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
    Initialize tallies from the full retrain's lookup tables.

    This bootstraps the daily updater from the last full retrain.
    We back-compute approximate tallies from the lookup rates.
    Better: if raw expanding caches exist, use those directly.
    """
    from pathlib import Path as P

    # Try to load from the Betting Models repo's expanding caches
    bm_root = P("C:/Users/ander/Documents/Betting Models/MLB")
    pitcher_cache = bm_root / "data" / "processed" / "_pitcher_expanding.parquet"
    batter_cache = bm_root / "data" / "processed" / "_batter_expanding.parquet"

    tallies = {
        "last_updated": None,
        "dates_processed": [],
        "pitchers": {},
        "batters": {},
        "umpires": {},
    }

    # Use retrain_v4.py's raw Statcast approach for maximum accuracy
    statcast_path = bm_root / "data" / "raw" / "statcast_raw_2023_2025.parquet"
    if not statcast_path.exists():
        logger.error("Statcast raw data not found at %s", statcast_path)
        logger.info("Run the full retrain locally first to initialize tallies.")
        return

    logger.info("Initializing tallies from raw Statcast (%s)...", statcast_path)
    sc = pd.read_parquet(statcast_path)
    logger.info("  Loaded %d pitches", len(sc))

    # Process as if all days at once
    update_pitcher_tallies(tallies, sc)
    update_batter_tallies(tallies, sc)

    # Umpire tallies from umpire data file
    # Try 2021-2025 file first (has populated umpire names), then 2023-2025
    ump_path = bm_root / "data" / "raw" / "umpire_data_2021_2025.parquet"
    if not ump_path.exists():
        ump_path = bm_root / "data" / "raw" / "umpire_data_2023_2025.parquet"
    if ump_path.exists():
        ump_df = pd.read_parquet(ump_path)
        # Try hp_umpire_name first, fall back to umpire column
        nc = "hp_umpire_name" if ("hp_umpire_name" in ump_df.columns and
                                   ump_df["hp_umpire_name"].notna().sum() > 0) else "umpire"
        tc = "total_called" if "total_called" in ump_df.columns else "total_calls"
        ump_df = ump_df[ump_df[nc].notna()]
        for name, grp in ump_df.groupby(nc):
            total_calls = int(grp[tc].sum())
            called_strikes = int((grp["called_strike_rate"] * grp[tc]).sum())
            tallies["umpires"][name] = {
                "total_pitches": total_calls,
                "called_strikes": called_strikes,
            }

    tallies["last_updated"] = datetime.now(timezone.utc).isoformat()
    tallies["dates_processed"] = ["init-from-statcast"]

    save_tallies(tallies)
    logger.info("Tallies initialized: %d pitchers, %d batters, %d umpires",
                len(tallies["pitchers"]), len(tallies["batters"]), len(tallies["umpires"]))

    # Rebuild lookups from fresh tallies
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

    update_pitcher_tallies(tallies, sc)
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
            "new_pitchers": new_pitchers,
            "new_batters": new_batters,
        }))


if __name__ == "__main__":
    main()
