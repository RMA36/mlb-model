"""
YRFI/NRFI v4 — Result Scoring

Checks final first-inning linescore for each predicted game,
marks bets as W/L, and maintains cumulative results.json.

Runs nightly via GitHub Actions after games finish (~1 AM ET)
or manually: python score_results.py --date 2026-06-15

Usage:
    python score_results.py              # score yesterday's predictions
    python score_results.py --date DATE  # score specific date
    python score_results.py --backfill   # rescore all unscored predictions
"""

import argparse
import json
import logging
import os
import subprocess
import time
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s")
logger = logging.getLogger(__name__)

ROOT = Path(__file__).parent
PREDICTIONS_DIR = ROOT / "predictions"
RESULTS_FILE = ROOT / "results" / "results.json"

ODDS_MASTER_REPO = "RMA36/mlb-odds-tracker-2026"
ODDS_MASTER_PATH = "data/2026/odds_master_2026.parquet"
ODDS_CACHE = ROOT / ".cache" / "odds_master_2026.parquet"

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

ET = ZoneInfo("America/New_York")


def fetch_linescore(game_pk: int) -> dict | None:
    """Fetch first-inning runs from MLB linescore API."""
    url = f"https://statsapi.mlb.com/api/v1.1/game/{game_pk}/feed/live"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "MLB-Model/1.0"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read())

        linescore = data.get("liveData", {}).get("linescore", {})
        innings = linescore.get("innings", [])
        status = data.get("gameData", {}).get("status", {}).get("abstractGameState", "")

        if status != "Final":
            return None  # Game not finished yet

        if not innings:
            return None

        first = innings[0]
        away_1st = first.get("away", {}).get("runs", 0)
        home_1st = first.get("home", {}).get("runs", 0)
        total_1st = away_1st + home_1st

        return {
            "away_1st_runs": away_1st,
            "home_1st_runs": home_1st,
            "total_1st_runs": total_1st,
            "yrfi": total_1st > 0,
            "game_status": status,
        }
    except Exception as e:
        logger.warning("  Failed to fetch linescore for %d: %s", game_pk, e)
        return None


def load_results() -> dict:
    """Load existing cumulative results."""
    if RESULTS_FILE.exists():
        return json.loads(RESULTS_FILE.read_text())
    return {
        "model": "v4-lgb-two-model",
        "updated_at": None,
        "bankroll_start": 1000,
        "daily": {},
        "cumulative": {
            "total_bets": 0,
            "wins": 0,
            "losses": 0,
            "profit": 0.0,
            "roi_pct": 0.0,
            "peak_profit": 0.0,
            "max_drawdown": 0.0,
        },
    }


def american_to_decimal(odds: float) -> float:
    """Convert American odds to decimal payout (e.g., +120 → 2.20, -130 → 1.769)."""
    if odds > 0:
        return 1 + odds / 100
    else:
        return 1 + 100 / abs(odds)


def american_to_implied(odds: float) -> float:
    """Convert American odds to implied probability."""
    if odds > 0:
        return 100 / (odds + 100)
    return abs(odds) / (abs(odds) + 100)


def download_odds_master():
    """Download odds master from GitHub using gh CLI or GITHUB_TOKEN."""
    ODDS_CACHE.parent.mkdir(parents=True, exist_ok=True)

    if ODDS_CACHE.exists():
        age_hours = (time.time() - os.path.getmtime(ODDS_CACHE)) / 3600
        if age_hours < 1.0:
            logger.info("Using cached odds (%.0f min old)", age_hours * 60)
            return ODDS_CACHE

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
        pass

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

    logger.warning("No way to download odds (no gh CLI, no GITHUB_TOKEN)")
    return None


def load_closing_odds(date_str: str) -> dict:
    """Load closing YRFI/NRFI odds for a date. Returns {(away_abbr, home_abbr): {yrfi: odds, nrfi: odds}}."""
    try:
        import pandas as pd
    except ImportError:
        logger.warning("pandas not available — skipping CLV")
        return {}

    odds_path = download_odds_master()
    if odds_path is None or not odds_path.exists():
        return {}

    df = pd.read_parquet(odds_path)
    fi = df[df["market"] == "totals_1st_1_innings"].copy()
    if fi.empty:
        return {}

    fi["commence_utc"] = pd.to_datetime(fi["commence_time"], utc=True)
    fi["commence_et"] = fi["commence_utc"].dt.tz_convert("US/Eastern")
    fi["game_day"] = fi["commence_et"].dt.strftime("%Y-%m-%d")
    fi = fi[fi["game_day"] == date_str].copy()
    if fi.empty:
        return {}

    fi["home_abbr"] = fi["home_team"].map(FULL_NAME_TO_ABBR)
    fi["away_abbr"] = fi["away_team"].map(FULL_NAME_TO_ABBR)

    yrfi = fi[fi["outcome_name"] == "Over"]
    nrfi = fi[fi["outcome_name"] == "Under"]

    yrfi_agg = (yrfi.groupby(["away_abbr", "home_abbr"])
                .agg(close_odds=("close_price", "median"))
                .reset_index())
    nrfi_agg = (nrfi.groupby(["away_abbr", "home_abbr"])
                .agg(close_odds=("close_price", "median"))
                .reset_index())

    closing = {}
    for _, row in yrfi_agg.iterrows():
        key = (row["away_abbr"], row["home_abbr"])
        closing[key] = {"yrfi": row["close_odds"]}
    for _, row in nrfi_agg.iterrows():
        key = (row["away_abbr"], row["home_abbr"])
        if key in closing:
            closing[key]["nrfi"] = row["close_odds"]
        else:
            closing[key] = {"nrfi": row["close_odds"]}

    logger.info("Loaded closing odds for %d games on %s", len(closing), date_str)
    return closing


def score_date(date_str: str, results: dict) -> int:
    """Score all bets for a given date. Returns number of newly scored bets."""
    pred_path = PREDICTIONS_DIR / f"{date_str}.json"
    if not pred_path.exists():
        logger.info("No predictions file for %s", date_str)
        return 0

    preds = json.loads(pred_path.read_text())
    bets = [g for g in preds.get("games", []) if g.get("passes_filter")]

    if not bets:
        logger.info("%s: No bets to score", date_str)
        return 0

    # Check if already scored
    if date_str in results["daily"]:
        existing = results["daily"][date_str]
        if existing.get("all_scored"):
            logger.info("%s: Already fully scored (%d bets)", date_str, len(existing["bets"]))
            return 0

    # Load closing odds for CLV calculation
    closing_odds = load_closing_odds(date_str)

    scored_bets = []
    newly_scored = 0

    for bet in bets:
        gpk = bet["game_pk"]
        ls = fetch_linescore(gpk)

        if ls is None:
            logger.warning("  %s: %s @ %s — game not final, skipping",
                           date_str, bet["away_team"], bet["home_team"])
            scored_bets.append({**bet, "result": "pending"})
            continue

        yrfi_hit = ls["yrfi"]
        if bet["bet_side"] == "YRFI":
            won = yrfi_hit
        else:  # NRFI
            won = not yrfi_hit

        dec_odds = american_to_decimal(bet["bet_odds"])
        pnl = bet["stake"] * (dec_odds - 1) if won else -bet["stake"]

        # CLV: compare entry odds to true closing odds
        game_key = (bet["away_team"], bet["home_team"])
        side_key = bet["bet_side"].lower()  # "yrfi" or "nrfi"
        close_line = closing_odds.get(game_key, {}).get(side_key)

        scored_bet = {
            **bet,
            "result": "W" if won else "L",
            "away_1st_runs": ls["away_1st_runs"],
            "home_1st_runs": ls["home_1st_runs"],
            "total_1st_runs": ls["total_1st_runs"],
            "pnl": round(pnl, 2),
            "dec_odds": round(dec_odds, 3),
        }

        if close_line is not None:
            close_impl = american_to_implied(close_line)
            entry_impl = american_to_implied(bet["bet_odds"])
            # Positive CLV = we got a better price than the closing line
            scored_bet["close_odds"] = round(close_line, 1)
            scored_bet["clv"] = round(entry_impl - close_impl, 4)

        scored_bets.append(scored_bet)
        newly_scored += 1
        tag = "✓ W" if won else "✗ L"
        logger.info("  %s @ %s: %s %s → %s (1st: %d-%d) → $%+.2f",
                     bet["away_team"], bet["home_team"],
                     bet["bet_side"], f"{bet['bet_odds']:+.0f}",
                     tag, ls["away_1st_runs"], ls["home_1st_runs"], pnl)

    all_scored = all(b["result"] != "pending" for b in scored_bets)
    day_wins = sum(1 for b in scored_bets if b["result"] == "W")
    day_losses = sum(1 for b in scored_bets if b["result"] == "L")
    day_pnl = sum(b.get("pnl", 0) for b in scored_bets if b["result"] in ("W", "L"))
    day_wagered = sum(b["stake"] for b in scored_bets if b["result"] in ("W", "L"))

    results["daily"][date_str] = {
        "bets": scored_bets,
        "all_scored": all_scored,
        "wins": day_wins,
        "losses": day_losses,
        "pnl": round(day_pnl, 2),
        "wagered": round(day_wagered, 2),
        "roi_pct": round(day_pnl / day_wagered * 100, 1) if day_wagered > 0 else 0,
    }

    if all_scored:
        logger.info("%s: %dW-%dL, P&L $%+.2f (%.1f%% ROI)",
                    date_str, day_wins, day_losses, day_pnl,
                    day_pnl / day_wagered * 100 if day_wagered else 0)

    return newly_scored


def rebuild_cumulative(results: dict):
    """Recompute cumulative stats from all daily results."""
    total_bets = 0
    wins = 0
    losses = 0
    profit = 0.0
    wagered = 0.0
    running_profit = 0.0
    peak = 0.0
    max_dd = 0.0

    for date_str in sorted(results["daily"]):
        day = results["daily"][date_str]
        total_bets += day["wins"] + day["losses"]
        wins += day["wins"]
        losses += day["losses"]
        profit += day["pnl"]
        wagered += day["wagered"]

        running_profit += day["pnl"]
        peak = max(peak, running_profit)
        dd = peak - running_profit
        max_dd = max(max_dd, dd)

    results["cumulative"] = {
        "total_bets": total_bets,
        "wins": wins,
        "losses": losses,
        "win_rate_pct": round(wins / total_bets * 100, 1) if total_bets else 0,
        "profit": round(profit, 2),
        "wagered": round(wagered, 2),
        "roi_pct": round(profit / wagered * 100, 1) if wagered else 0,
        "peak_profit": round(peak, 2),
        "max_drawdown": round(max_dd, 2),
    }
    results["updated_at"] = datetime.now(timezone.utc).isoformat()


def main():
    parser = argparse.ArgumentParser(description="Score YRFI/NRFI predictions")
    parser.add_argument("--date", help="Date to score (YYYY-MM-DD). Default: yesterday")
    parser.add_argument("--backfill", action="store_true", help="Rescore all unscored dates")
    parser.add_argument("--json-only", action="store_true", help="Output JSON summary only")
    args = parser.parse_args()

    results = load_results()

    if args.backfill:
        # Find all prediction files and score any that aren't fully scored
        dates = sorted(p.stem for p in PREDICTIONS_DIR.glob("*.json"))
        logger.info("Backfilling %d dates...", len(dates))
        total_scored = 0
        for d in dates:
            n = score_date(d, results)
            total_scored += n
        logger.info("Backfill complete: %d bets scored", total_scored)
    else:
        if args.date:
            date_str = args.date
        else:
            # Default to yesterday (games from yesterday should be final by now)
            yesterday = datetime.now(ET) - timedelta(days=1)
            date_str = yesterday.strftime("%Y-%m-%d")

        score_date(date_str, results)

    rebuild_cumulative(results)

    # Save results
    RESULTS_FILE.parent.mkdir(parents=True, exist_ok=True)
    RESULTS_FILE.write_text(json.dumps(results, indent=2, default=str))
    logger.info("Results saved to %s", RESULTS_FILE)

    # Print summary
    c = results["cumulative"]
    if args.json_only:
        print(json.dumps(c))
    else:
        print()
        print("=" * 50)
        print(f"  Cumulative: {c['total_bets']} bets | {c['wins']}W-{c['losses']}L ({c['win_rate_pct']}%)")
        print(f"  P&L: ${c['profit']:+.2f} | ROI: {c['roi_pct']}%")
        print(f"  Peak: ${c['peak_profit']:+.2f} | Max DD: ${c['max_drawdown']:.2f}")
        print("=" * 50)


if __name__ == "__main__":
    main()
