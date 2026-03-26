"""
Production vs Training Pipeline Audit
======================================
Compares production-reconstructed features (from predict.py + lookups.json)
against ground-truth training features (feature_matrix.parquet).

Three levels:
  L1: Feature-by-feature comparison (last 50 Sep 2025 games)
  L2: Prediction-level comparison (same 50 games)
  L3: Bet-level comparison (full 2025 season)

Usage:
    python audit_production.py
"""

import json
import sys
import warnings
from datetime import datetime
from pathlib import Path

import lightgbm as lgb
import numpy as np
import pandas as pd
from scipy import stats

warnings.filterwarnings("ignore", category=FutureWarning)

# ── Paths ─────────────────────────────────────────────────────────────
ROOT = Path(__file__).resolve().parent
MODEL_DIR = ROOT / "models" / "v4"

DATA_BASE = Path(r"C:\Users\ander\Documents\Betting Models\MLB")
FEATURE_MATRIX = DATA_BASE / "data" / "processed" / "feature_matrix.parquet"
GAME_LOGS = DATA_BASE / "data" / "raw" / "game_logs_2023_2025.parquet"
STATCAST = DATA_BASE / "data" / "raw" / "statcast_raw_2023_2025.parquet"

# ── Import production pipeline ────────────────────────────────────────
sys.path.insert(0, str(ROOT))
from predict import (
    compute_features,
    load_lookups,
    load_models,
    predict_game,
    PA_PROB_WEIGHTS,
    LEAGUE_AVG_K_RATE,
    LEAGUE_AVG_BB_RATE,
    LEAGUE_AVG_HR_RATE,
    LEAGUE_AVG_WOBA,
    LEAGUE_AVG_OBP,
)


# ── Thresholds ────────────────────────────────────────────────────────
THRESHOLDS = {
    # Feature correlation thresholds
    "pitcher_career_corr": (0.95, 0.85),   # (PASS, WARN) — below WARN = FAIL
    "lineup_corr": (0.70, 0.50),
    "env_corr": (0.80, 0.60),
    "ump_corr": (0.85, 0.70),
    # Prediction thresholds
    "p_yrfi_corr": (0.95, 0.85),
    "p_yrfi_mae": (0.02, 0.04),            # (PASS, WARN) — above WARN = FAIL
    # Bet-level thresholds
    "auc_delta": (0.01, 0.02),
    "bet_agreement": (0.90, 0.80),          # (PASS, WARN)
}


def status(val, pass_thresh, warn_thresh, higher_is_better=True):
    """Return PASS/WARN/FAIL label."""
    if higher_is_better:
        if val >= pass_thresh:
            return "PASS"
        elif val >= warn_thresh:
            return "WARN"
        return "FAIL"
    else:
        if val <= pass_thresh:
            return "PASS"
        elif val <= warn_thresh:
            return "WARN"
        return "FAIL"


# ── Data Loading ──────────────────────────────────────────────────────

def load_data():
    """Load feature matrix, game logs, and statcast inning-1 data."""
    print("Loading data...")
    fm = pd.read_parquet(FEATURE_MATRIX)
    gl = pd.read_parquet(GAME_LOGS)
    print(f"  Feature matrix: {len(fm)} games, {len(fm.columns)} columns")
    print(f"  Game logs: {len(gl)} games")

    # Only load inning 1 data from statcast (for lineup reconstruction)
    print("  Loading statcast inning 1 data (this may take a moment)...")
    sc = pd.read_parquet(STATCAST, columns=[
        "game_pk", "game_date", "inning", "inning_topbot",
        "batter", "stand", "home_team", "away_team",
        "bat_score", "fld_score", "post_bat_score",
    ])
    sc = sc[sc["inning"] == 1].copy()
    print(f"  Statcast inning 1: {len(sc)} pitches")

    return fm, gl, sc


def build_game_info_from_historical(game_pk, game_date, gl_row, sc_game, stadium_meta):
    """Build a game_info dict mimicking MLB API output from historical data."""
    info = {
        "game_pk": game_pk,
        "home_team": gl_row["home_team"],
        "away_team": gl_row["away_team"],
        "home_starter_id": int(gl_row["home_starter_id"]) if pd.notna(gl_row["home_starter_id"]) else None,
        "away_starter_id": int(gl_row["away_starter_id"]) if pd.notna(gl_row["away_starter_id"]) else None,
        "umpire": gl_row.get("umpire", "Unknown"),
    }

    # Find venue from stadium metadata (team → venue mapping)
    home = gl_row["home_team"]
    venue = None
    for vname, vmeta in stadium_meta.items():
        # Stadium meta keys are venue names; we need team→venue
        # Actually lookups have parks keyed by venue name, so we need a team→venue map
        pass
    # We'll set venue below after building the team→venue map
    info["venue"] = venue or ""

    # Get pitcher handedness from statcast
    # Top of 1st: away batters face home pitcher → home pitcher is fielding
    # Bot of 1st: home batters face away pitcher → away pitcher is fielding
    top1 = sc_game[sc_game["inning_topbot"] == "Top"].copy()
    bot1 = sc_game[sc_game["inning_topbot"] == "Bot"].copy()

    # Extract lineup: unique batters in order of appearance, first 6
    def get_lineup(half_df):
        if half_df.empty:
            return [], []
        seen = set()
        lineup_ids = []
        lineup_hands = []
        for _, row in half_df.iterrows():
            bid = int(row["batter"])
            if bid not in seen:
                seen.add(bid)
                lineup_ids.append(bid)
                lineup_hands.append(row.get("stand", "R"))
            if len(lineup_ids) >= 6:
                break
        return lineup_ids, lineup_hands

    away_lineup, away_hands = get_lineup(top1)  # away bats in top 1st
    home_lineup, home_hands = get_lineup(bot1)  # home bats in bot 1st

    info["away_lineup"] = away_lineup
    info["away_lineup_hands"] = away_hands
    info["home_lineup"] = home_lineup
    info["home_lineup_hands"] = home_hands

    # Pitcher throws: infer from statcast (pitcher throws the same hand to all batters)
    # We don't have p_throws in statcast directly, but we can get it from the pitcher's
    # typical K splits in lookups. For the audit, we'll use a simple heuristic.
    # Actually, we DO have this: the stand column tells us batter hand, not pitcher hand.
    # We need to pass pitcher throws for platoon features. Let's default to "R" and
    # check if it matters (it only affects platoon lineup woba/obp selection).
    info["home_starter_throws"] = "R"  # will be overridden below
    info["away_starter_throws"] = "R"

    return info


STATCAST_TO_STD = {
    "AZ": "ARI", "TB": "TBR", "CWS": "CHW", "KC": "KCR",
    "SD": "SDP", "SF": "SFG", "WSH": "WSN", "ATH": "OAK",
}


def build_team_venue_map(lookups):
    """Build team abbreviation → venue name mapping from park lookups + stadium CSV.
    Maps BOTH standard and Statcast abbreviations to venue names."""
    csv_path = MODEL_DIR / "stadium_metadata.csv"
    if csv_path.exists():
        sdf = pd.read_csv(csv_path)
        mapping = {}
        for _, row in sdf.iterrows():
            team = row["team"]  # standard abbr (ARI, CHW, etc.)
            venue = row["stadium_name"]
            mapping[team] = venue
        # Also add reverse Statcast mappings
        for sc_abbr, std_abbr in STATCAST_TO_STD.items():
            if std_abbr in mapping:
                mapping[sc_abbr] = mapping[std_abbr]
        return mapping
    return {}


def infer_pitcher_throws(lookups, pid):
    """Infer pitcher handedness from platoon split asymmetry in lookups."""
    ps = lookups.get("pitcher_stats", {})
    p = ps.get(pid, {})
    if not p:
        return "R"
    # LHP typically has higher K rate vs R, RHP has higher K rate vs L
    k_vs_L = p.get("k_rate_vs_L", 0.225)
    k_vs_R = p.get("k_rate_vs_R", 0.225)
    # If K vs R >> K vs L, likely LHP (same-side advantage)
    # If K vs L >> K vs R, likely RHP
    # This is a rough heuristic; RHP face more RHB so their vs_R is more regressed
    # Actually the split is: same-hand batters are harder to K
    # RHP: higher K vs L (opposite hand), lower K vs R (same hand)
    # LHP: higher K vs R (opposite hand), lower K vs L (same hand)
    if k_vs_R > k_vs_L + 0.02:
        return "L"
    return "R"


# ── Level 1: Feature-by-Feature Comparison ────────────────────────────

def run_level1(fm, gl, sc, lookups, n_games=50):
    """Compare production features against ground truth for last N Sep 2025 games."""
    print("\n" + "=" * 70)
    print("LEVEL 1: Feature-by-Feature Comparison")
    print("=" * 70)

    # Get last N games from Sep 2025 (lookups contain career-end stats,
    # so late games minimize expanding-window vs career-end differences)
    fm_sorted = fm.sort_values("game_date")
    sep2025 = fm_sorted[
        (fm_sorted["game_date"] >= "2025-09-01") &
        (fm_sorted["game_date"] <= "2025-09-30")
    ]
    if len(sep2025) < n_games:
        print(f"  Only {len(sep2025)} Sep 2025 games, using all of them")
        sample = sep2025
    else:
        sample = sep2025.tail(n_games)
    print(f"  Using {len(sample)} games ({sample['game_date'].min()} to {sample['game_date'].max()})")

    # Build team→venue map
    team_venue = build_team_venue_map(lookups)

    # Load metadata for feature names
    with open(MODEL_DIR / "metadata.json") as f:
        meta = json.load(f)
    all_features = sorted(set(meta["top1_features"] + meta["bot1_features"]))

    # Also need pitcher throws from statcast p_throws column if available
    # Load a small statcast subset to get pitcher handedness
    print("  Loading pitcher handedness from statcast...")
    try:
        sc_throws = pd.read_parquet(STATCAST, columns=["pitcher", "p_throws"])
        pitcher_throws_map = sc_throws.drop_duplicates("pitcher").set_index("pitcher")["p_throws"].to_dict()
        print(f"  Found handedness for {len(pitcher_throws_map)} pitchers")
    except Exception:
        pitcher_throws_map = {}
        print("  Could not load pitcher handedness, using heuristic")

    # Compare features game by game
    gt_rows = []
    prod_rows = []
    game_pks_used = []

    gl_indexed = gl.set_index("game_pk")
    sc_indexed = sc.groupby("game_pk")

    for _, fm_row in sample.iterrows():
        gpk = fm_row["game_pk"]
        game_date = str(fm_row["game_date"])[:10]

        if gpk not in gl_indexed.index:
            continue

        gl_row = gl_indexed.loc[gpk]
        if isinstance(gl_row, pd.DataFrame):
            gl_row = gl_row.iloc[0]

        # Get statcast inning 1 for this game
        if gpk in sc_indexed.groups:
            sc_game = sc_indexed.get_group(gpk)
        else:
            continue

        # Build game_info
        game_info = build_game_info_from_historical(gpk, game_date, gl_row, sc_game, lookups.get("stadium_meta", {}))

        # Set venue from team→venue map
        home = gl_row["home_team"]
        game_info["venue"] = team_venue.get(home, "")

        # Set pitcher throws
        home_pid = game_info["home_starter_id"]
        away_pid = game_info["away_starter_id"]
        if home_pid and home_pid in pitcher_throws_map:
            game_info["home_starter_throws"] = pitcher_throws_map[home_pid]
        elif home_pid:
            game_info["home_starter_throws"] = infer_pitcher_throws(lookups, home_pid)
        if away_pid and away_pid in pitcher_throws_map:
            game_info["away_starter_throws"] = pitcher_throws_map[away_pid]
        elif away_pid:
            game_info["away_starter_throws"] = infer_pitcher_throws(lookups, away_pid)

        # Set weather (use feature matrix ground truth values for env features
        # to isolate non-env differences first — then we'll also test with approx weather)
        game_info["temp"] = str(fm_row.get("env_temp_max", 72))
        game_info["humidity"] = str(fm_row.get("env_humidity", 50))
        game_info["wind"] = f"{fm_row.get('env_wind_mph', 0)} mph"

        # Compute production features
        prod_feats = compute_features(game_info, lookups, date_str=game_date)

        # Extract ground truth
        gt_feats = {}
        for f in all_features:
            if f in fm_row.index:
                gt_feats[f] = fm_row[f]
            else:
                gt_feats[f] = np.nan

        gt_rows.append(gt_feats)
        prod_rows.append(prod_feats)
        game_pks_used.append(gpk)

    gt_df = pd.DataFrame(gt_rows, index=game_pks_used)
    prod_df = pd.DataFrame(prod_rows, index=game_pks_used)

    print(f"\n  Successfully compared {len(gt_df)} games")

    # ── Per-feature analysis ──
    results = []
    for f in all_features:
        if f not in gt_df.columns or f not in prod_df.columns:
            results.append({"feature": f, "status": "SKIP", "reason": "missing"})
            continue

        gt_vals = gt_df[f].values.astype(float)
        prod_vals = prod_df[f].values.astype(float)

        # Skip if all NaN
        mask = ~(np.isnan(gt_vals) | np.isnan(prod_vals))
        if mask.sum() < 10:
            results.append({"feature": f, "status": "SKIP", "reason": f"only {mask.sum()} valid"})
            continue

        gt_v = gt_vals[mask]
        prod_v = prod_vals[mask]

        # Correlation
        if np.std(gt_v) < 1e-10 or np.std(prod_v) < 1e-10:
            corr = 1.0 if np.allclose(gt_v, prod_v, atol=1e-6) else 0.0
        else:
            corr = np.corrcoef(gt_v, prod_v)[0, 1]

        mae = np.mean(np.abs(gt_v - prod_v))
        bias = np.mean(prod_v - gt_v)
        max_err = np.max(np.abs(gt_v - prod_v))
        gt_mean = np.mean(gt_v)
        gt_std = np.std(gt_v)
        rel_mae = mae / gt_std if gt_std > 1e-6 else mae

        # Categorize feature for threshold selection
        if f.startswith(("home_p_", "away_p_")):
            cat = "pitcher"
            pass_t, warn_t = THRESHOLDS["pitcher_career_corr"]
        elif "lineup" in f or "platoon_lineup" in f:
            cat = "lineup"
            pass_t, warn_t = THRESHOLDS["lineup_corr"]
        elif f.startswith("env_"):
            cat = "env"
            pass_t, warn_t = THRESHOLDS["env_corr"]
        elif f.startswith("ump_"):
            cat = "umpire"
            pass_t, warn_t = THRESHOLDS["ump_corr"]
        else:
            cat = "context"
            pass_t, warn_t = (0.99, 0.95)

        stat = status(corr, pass_t, warn_t, higher_is_better=True)

        results.append({
            "feature": f,
            "category": cat,
            "corr": corr,
            "mae": mae,
            "bias": bias,
            "max_err": max_err,
            "gt_mean": gt_mean,
            "rel_mae": rel_mae,
            "status": stat,
            "n": int(mask.sum()),
        })

    # ── Print results ──
    print(f"\n{'Feature':<40} {'Corr':>6} {'MAE':>8} {'Bias':>8} {'MaxErr':>8} {'Status':>6}")
    print("-" * 80)

    pass_count = warn_count = fail_count = skip_count = 0
    categories = {}

    for r in sorted(results, key=lambda x: (x.get("status", "SKIP") == "SKIP", x.get("category", ""), x["feature"])):
        if r["status"] == "SKIP":
            skip_count += 1
            continue

        cat = r["category"]
        if cat not in categories:
            categories[cat] = {"pass": 0, "warn": 0, "fail": 0, "corrs": []}

        s = r["status"]
        if s == "PASS":
            pass_count += 1
            categories[cat]["pass"] += 1
        elif s == "WARN":
            warn_count += 1
            categories[cat]["warn"] += 1
        else:
            fail_count += 1
            categories[cat]["fail"] += 1

        categories[cat]["corrs"].append(r["corr"])

        flag = "" if s == "PASS" else f" <-- {s}"
        print(f"  {r['feature']:<38} {r['corr']:>6.3f} {r['mae']:>8.4f} {r['bias']:>8.4f} {r['max_err']:>8.4f} {s:>6}{flag}")

    print(f"\n  Summary: {pass_count} PASS, {warn_count} WARN, {fail_count} FAIL, {skip_count} SKIP")

    print("\n  By category:")
    for cat, counts in sorted(categories.items()):
        avg_corr = np.mean(counts["corrs"]) if counts["corrs"] else 0
        print(f"    {cat:<12}: {counts['pass']}P/{counts['warn']}W/{counts['fail']}F  avg_corr={avg_corr:.3f}")

    return gt_df, prod_df, results


# ── Level 2: Prediction-Level Comparison ──────────────────────────────

def run_level2(gt_df, prod_df, meta):
    """Compare predictions from ground truth vs production features."""
    print("\n" + "=" * 70)
    print("LEVEL 2: Prediction-Level Comparison")
    print("=" * 70)

    top_model = lgb.Booster(model_file=str(MODEL_DIR / "top1_model.txt"))
    bot_model = lgb.Booster(model_file=str(MODEL_DIR / "bot1_model.txt"))

    top_feats = meta["top1_features"]
    bot_feats = meta["bot1_features"]

    # Ground truth predictions
    gt_top_vals = gt_df[top_feats].fillna(0).values
    gt_bot_vals = gt_df[bot_feats].fillna(0).values
    gt_p_top = top_model.predict(gt_top_vals)
    gt_p_bot = bot_model.predict(gt_bot_vals)
    gt_p_yrfi = 1 - (1 - gt_p_top) * (1 - gt_p_bot)

    # Production predictions
    prod_top_vals = prod_df[top_feats].fillna(0).values
    prod_bot_vals = prod_df[bot_feats].fillna(0).values
    prod_p_top = top_model.predict(prod_top_vals)
    prod_p_bot = bot_model.predict(prod_bot_vals)
    prod_p_yrfi = 1 - (1 - prod_p_top) * (1 - prod_p_bot)

    # Compare
    for name, gt_arr, prod_arr in [
        ("p_top", gt_p_top, prod_p_top),
        ("p_bot", gt_p_bot, prod_p_bot),
        ("p_yrfi", gt_p_yrfi, prod_p_yrfi),
    ]:
        corr = np.corrcoef(gt_arr, prod_arr)[0, 1]
        mae = np.mean(np.abs(gt_arr - prod_arr))
        bias = np.mean(prod_arr - gt_arr)
        max_err = np.max(np.abs(gt_arr - prod_arr))

        if name == "p_yrfi":
            stat = status(corr, *THRESHOLDS["p_yrfi_corr"])
            mae_stat = status(mae, *THRESHOLDS["p_yrfi_mae"], higher_is_better=False)
        else:
            stat = status(corr, 0.95, 0.85)
            mae_stat = status(mae, 0.02, 0.04, higher_is_better=False)

        print(f"\n  {name}:")
        print(f"    Correlation: {corr:.4f}  [{stat}]")
        print(f"    MAE:         {mae:.4f}  [{mae_stat}]")
        print(f"    Bias:        {bias:+.4f}")
        print(f"    Max Error:   {max_err:.4f}")
        print(f"    GT mean:     {np.mean(gt_arr):.4f}")
        print(f"    Prod mean:   {np.mean(prod_arr):.4f}")

    return gt_p_yrfi, prod_p_yrfi


# ── Level 3: Bet-Level Comparison (full 2025) ─────────────────────────

def run_level3(fm, gl, sc, lookups, meta):
    """Full 2025 season comparison: AUC, sub-model agreement, rank correlation."""
    print("\n" + "=" * 70)
    print("LEVEL 3: Bet-Level Comparison (Full 2025 Season)")
    print("=" * 70)

    fm_sorted = fm.sort_values("game_date")
    season_2025 = fm_sorted[
        (fm_sorted["game_date"] >= "2025-03-20") &
        (fm_sorted["game_date"] <= "2025-10-01")
    ]
    print(f"  2025 season: {len(season_2025)} games")

    # Load models
    top_model = lgb.Booster(model_file=str(MODEL_DIR / "top1_model.txt"))
    bot_model = lgb.Booster(model_file=str(MODEL_DIR / "bot1_model.txt"))
    top_feats = meta["top1_features"]
    bot_feats = meta["bot1_features"]
    model_mean = meta["model_mean"]

    # Ground truth predictions (directly from feature matrix)
    gt_top_vals = season_2025[top_feats].fillna(0).values
    gt_bot_vals = season_2025[bot_feats].fillna(0).values
    gt_p_top = top_model.predict(gt_top_vals)
    gt_p_bot = bot_model.predict(gt_bot_vals)
    gt_p_yrfi = 1 - (1 - gt_p_top) * (1 - gt_p_bot)

    # Production predictions (reconstruct features for each game)
    team_venue = build_team_venue_map(lookups)
    gl_indexed = gl.set_index("game_pk")
    sc_indexed = sc.groupby("game_pk")

    # Load pitcher throws
    try:
        sc_throws = pd.read_parquet(STATCAST, columns=["pitcher", "p_throws"])
        pitcher_throws_map = sc_throws.drop_duplicates("pitcher").set_index("pitcher")["p_throws"].to_dict()
    except Exception:
        pitcher_throws_map = {}

    prod_p_top_list = []
    prod_p_bot_list = []
    valid_mask = []

    print("  Reconstructing production features for each game...")
    n_skip = 0
    for i, (_, fm_row) in enumerate(season_2025.iterrows()):
        if (i + 1) % 500 == 0:
            print(f"    {i+1}/{len(season_2025)}...")

        gpk = fm_row["game_pk"]
        game_date = str(fm_row["game_date"])[:10]

        if gpk not in gl_indexed.index or gpk not in sc_indexed.groups:
            prod_p_top_list.append(np.nan)
            prod_p_bot_list.append(np.nan)
            valid_mask.append(False)
            n_skip += 1
            continue

        gl_row = gl_indexed.loc[gpk]
        if isinstance(gl_row, pd.DataFrame):
            gl_row = gl_row.iloc[0]
        sc_game = sc_indexed.get_group(gpk)

        game_info = build_game_info_from_historical(gpk, game_date, gl_row, sc_game, lookups.get("stadium_meta", {}))
        game_info["venue"] = team_venue.get(gl_row["home_team"], "")

        # Pitcher throws
        for side in ["home", "away"]:
            pid = game_info[f"{side}_starter_id"]
            if pid and pid in pitcher_throws_map:
                game_info[f"{side}_starter_throws"] = pitcher_throws_map[pid]
            elif pid:
                game_info[f"{side}_starter_throws"] = infer_pitcher_throws(lookups, pid)

        # Use ground truth weather to isolate computation differences
        game_info["temp"] = str(fm_row.get("env_temp_max", 72))
        game_info["humidity"] = str(fm_row.get("env_humidity", 50))
        game_info["wind"] = f"{fm_row.get('env_wind_mph', 0)} mph"

        feats = compute_features(game_info, lookups, date_str=game_date)

        top_vals = np.array([[feats.get(f, 0) for f in top_feats]])
        bot_vals = np.array([[feats.get(f, 0) for f in bot_feats]])
        prod_p_top_list.append(top_model.predict(top_vals)[0])
        prod_p_bot_list.append(bot_model.predict(bot_vals)[0])
        valid_mask.append(True)

    prod_p_top = np.array(prod_p_top_list)
    prod_p_bot = np.array(prod_p_bot_list)
    valid = np.array(valid_mask)
    prod_p_yrfi = 1 - (1 - prod_p_top) * (1 - prod_p_bot)

    print(f"  Valid: {valid.sum()}, Skipped: {n_skip}")

    # Actual outcomes
    actual_yrfi = season_2025["yrfi"].values

    # Filter to valid games
    gt_valid = gt_p_yrfi[valid]
    prod_valid = prod_p_yrfi[valid]
    actual_valid = actual_yrfi[valid]

    # ── AUC comparison ──
    from sklearn.metrics import roc_auc_score

    gt_auc = roc_auc_score(actual_valid, gt_valid)
    prod_auc = roc_auc_score(actual_valid, prod_valid)
    auc_delta = abs(gt_auc - prod_auc)

    auc_stat = status(auc_delta, *THRESHOLDS["auc_delta"], higher_is_better=False)
    print(f"\n  AUC (ground truth):  {gt_auc:.4f}")
    print(f"  AUC (production):    {prod_auc:.4f}")
    print(f"  AUC delta:           {auc_delta:.4f}  [{auc_stat}]")

    # ── Rank correlation ──
    rank_corr, _ = stats.spearmanr(gt_valid, prod_valid)
    print(f"\n  Spearman rank corr (p_yrfi): {rank_corr:.4f}")

    # ── Prediction correlation ──
    pearson_corr = np.corrcoef(gt_valid, prod_valid)[0, 1]
    mae = np.mean(np.abs(gt_valid - prod_valid))
    print(f"  Pearson corr (p_yrfi):       {pearson_corr:.4f}")
    print(f"  MAE (p_yrfi):                {mae:.4f}")

    # ── Sub-model agreement ──
    # "Agreement" = both pipelines pick the same side (YRFI if p>model_mean, NRFI otherwise)
    gt_pick = gt_valid > model_mean
    prod_pick = prod_valid > model_mean
    agreement = np.mean(gt_pick == prod_pick)
    agree_stat = status(agreement, *THRESHOLDS["bet_agreement"])
    print(f"\n  Directional agreement (YRFI/NRFI pick): {agreement:.1%}  [{agree_stat}]")

    # ── Edge agreement at typical thresholds ──
    for edge_thresh in [0.03, 0.05, 0.07]:
        gt_edge = gt_valid - model_mean
        prod_edge = prod_valid - model_mean
        gt_bets = np.abs(gt_edge) >= edge_thresh
        prod_bets = np.abs(prod_edge) >= edge_thresh
        both_bet = gt_bets & prod_bets
        if gt_bets.sum() > 0:
            overlap = both_bet.sum() / gt_bets.sum()
            # Among games where both pipelines bet, do they agree on side?
            if both_bet.sum() > 0:
                side_agree = np.mean((gt_edge[both_bet] > 0) == (prod_edge[both_bet] > 0))
            else:
                side_agree = 0
            print(f"  Edge>={edge_thresh:.0%}: GT bets={gt_bets.sum()}, Prod bets={prod_bets.sum()}, "
                  f"Overlap={overlap:.1%}, Side agree={side_agree:.1%}")

    return gt_auc, prod_auc


# ── Main ──────────────────────────────────────────────────────────────

def main():
    print("=" * 70)
    print("  PRODUCTION vs TRAINING PIPELINE AUDIT")
    print("  " + datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    print("=" * 70)

    # Load data
    fm, gl, sc = load_data()

    # Load production lookups
    print("\nLoading production lookups...")
    lookups = load_lookups()

    # Load metadata
    with open(MODEL_DIR / "metadata.json") as f:
        meta = json.load(f)

    # Level 1: Feature comparison (50 late Sep 2025 games)
    gt_df, prod_df, l1_results = run_level1(fm, gl, sc, lookups, n_games=50)

    # Level 2: Prediction comparison (same 50 games)
    gt_p, prod_p = run_level2(gt_df, prod_df, meta)

    # Level 3: Bet-level comparison (full 2025 season)
    gt_auc, prod_auc = run_level3(fm, gl, sc, lookups, meta)

    # ── Final Summary ──
    print("\n" + "=" * 70)
    print("  FINAL SUMMARY")
    print("=" * 70)

    l1_fails = sum(1 for r in l1_results if r.get("status") == "FAIL")
    l1_warns = sum(1 for r in l1_results if r.get("status") == "WARN")
    l1_pass = sum(1 for r in l1_results if r.get("status") == "PASS")

    print(f"\n  L1 Features:    {l1_pass} PASS / {l1_warns} WARN / {l1_fails} FAIL")
    print(f"  L2 p_yrfi corr: {np.corrcoef(gt_p, prod_p)[0,1]:.4f}")
    print(f"  L3 AUC delta:   {abs(gt_auc - prod_auc):.4f}")

    overall = "PASS" if l1_fails == 0 and abs(gt_auc - prod_auc) < 0.02 else "FAIL"
    if l1_warns > 5 or abs(gt_auc - prod_auc) >= 0.01:
        overall = "WARN" if overall != "FAIL" else "FAIL"

    print(f"\n  OVERALL: {overall}")
    print("=" * 70)


if __name__ == "__main__":
    main()
