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
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s")
logger = logging.getLogger(__name__)

ROOT = Path(__file__).parent
PREDICTIONS_DIR = ROOT / "predictions"
RESULTS_FILE = ROOT / "results" / "results.json"

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

        scored_bets.append({
            **bet,
            "result": "W" if won else "L",
            "away_1st_runs": ls["away_1st_runs"],
            "home_1st_runs": ls["home_1st_runs"],
            "total_1st_runs": ls["total_1st_runs"],
            "pnl": round(pnl, 2),
            "dec_odds": round(dec_odds, 3),
        })
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
