"use client";

import { useEffect, useState, useCallback } from "react";

const REPO = "RMA36/mlb-model";
const BRANCH = "master";
const RAW = `https://raw.githubusercontent.com/${REPO}/${BRANCH}`;
const MLB_API = "https://statsapi.mlb.com/api/v1";

// ── Bookmaker display names ──────────────────────────────────────────
const BOOK_NAME: Record<string, string> = {
  draftkings: "DraftKings",
  fanduel: "FanDuel",
  betmgm: "BetMGM",
  caesars: "Caesars",
  pointsbetus: "PointsBet",
  betonlineag: "BetOnline",
  bovada: "Bovada",
  mybookieag: "MyBookie",
  betrivers: "BetRivers",
  unibet_us: "Unibet",
  williamhill_us: "WilliamHill",
  wynnbet: "WynnBet",
  superbook: "SuperBook",
  betparx: "BetParx",
  espnbet: "ESPN Bet",
  fliff: "Fliff",
  pinnacle: "Pinnacle",
  hardrockbet: "Hard Rock",
  fanatics: "Fanatics",
  bet365: "Bet365",
  betus: "BetUS",
  lowvig: "LowVig",
  betanysports: "BetAnySports",
};

// ── Venue roof types ─────────────────────────────────────────────────
const VENUE_ROOF: Record<string, "dome" | "retractable" | "open"> = {
  "Tropicana Field": "dome",
  "Chase Field": "retractable",
  "Minute Maid Park": "retractable",
  "loanDepot park": "retractable",
  "American Family Field": "retractable",
  "T-Mobile Park": "retractable",
  "Globe Life Field": "retractable",
  "Rogers Centre": "retractable",
  "Daikin Park": "retractable",
};

// ── Existing interfaces ──────────────────────────────────────────────

interface ForecastWeather {
  temp: number;
  humidity: number;
  wind_mph: number;
  precipitation_mm: number;
}

interface PickFactors {
  away_fi_runs_per_start: number;
  home_fi_runs_per_start: number;
  away_fi_k_rate: number;
  home_fi_k_rate: number;
  away_recent_fi_runs: number;
  home_recent_fi_runs: number;
  away_avg_velo: number;
  home_avg_velo: number;
  away_platoon_k_rate: number;
  home_platoon_k_rate: number;
  away_lineup_weighted_score: number;
  home_lineup_weighted_score: number;
  away_platoon_lineup_woba: number;
  home_platoon_lineup_woba: number;
  umpire_strike_rate: number;
  park_factor_runs: number;
  elevation_ft: number;
}

interface Bet {
  game_pk: number;
  date: string;
  away_team: string;
  home_team: string;
  away_starter: string;
  home_starter: string;
  umpire: string;
  p_cal: number;
  p_top?: number;
  p_bot?: number;
  p_raw?: number;
  mkt_y_fair: number;
  mkt_n_fair: number;
  agreement: string;
  bet_side: string;
  bet_edge: number;
  bet_kelly: number;
  bet_odds: number;
  bet_dec: number;
  passes_filter: boolean;
  skip_reason: string;
  stake: number;
  n_books: number;
  result?: string;
  pnl?: number;
  away_1st_runs?: number;
  home_1st_runs?: number;
  total_1st_runs?: number;
  forecast_weather?: ForecastWeather | null;
  open_odds?: number;
  close_odds?: number;
  clv?: number;
  best_odds?: number;
  best_book?: string;
  mkt_y_impl?: number;
  mkt_n_impl?: number;
  pick_factors?: PickFactors;
}

interface DayPredictions {
  date: string;
  generated_at: string;
  games: Bet[];
  bets: Bet[];
  summary: {
    total_games: number;
    total_bets: number;
    yrfi_bets: number;
    nrfi_bets: number;
    total_exposure: number;
  };
}

interface DayResults {
  bets: Bet[];
  all_scored: boolean;
  wins: number;
  losses: number;
  pnl: number;
  wagered: number;
  roi_pct: number;
}

interface Results {
  daily: Record<string, DayResults>;
  cumulative: {
    total_bets: number;
    wins: number;
    losses: number;
    win_rate_pct: number;
    profit: number;
    wagered: number;
    roi_pct: number;
    peak_profit: number;
    max_drawdown: number;
  };
  updated_at: string;
}

// ── Today's Games interfaces ─────────────────────────────────────────

interface LineupPlayer {
  id: number;
  name: string;
  batSide: string;
  position: string;
}

interface PitcherStats {
  velocity: number | null;
  kRate: number | null;
}

interface TodaysGame {
  gamePk: number;
  gameTime: string;
  gameState: string;
  awayTeam: string;
  homeTeam: string;
  awayAbbrev: string;
  homeAbbrev: string;
  awayTeamId: number;
  homeTeamId: number;
  awayScore: number | null;
  homeScore: number | null;
  currentInning: string | null;
  venue: string;
  roofType: "dome" | "retractable" | "open";
  weather: { condition: string; temp: string; wind: string } | null;
  umpire: string | null;
  awayStarter: { name: string; throws: string; stats: PitcherStats } | null;
  homeStarter: { name: string; throws: string; stats: PitcherStats } | null;
  awayLineup: LineupPlayer[];
  homeLineup: LineupPlayer[];
  firstInningAway: number | null;
  firstInningHome: number | null;
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
type Lookups = Record<string, any>;

// ── Helpers ──────────────────────────────────────────────────────────

function formatOdds(odds: number): string {
  return odds > 0 ? `+${odds}` : `${odds}`;
}

function formatGameTime(iso: string): string {
  const d = new Date(iso);
  return d.toLocaleTimeString("en-US", {
    hour: "numeric",
    minute: "2-digit",
    timeZone: "America/New_York",
  }) + " ET";
}

function formatDateHeading(dateStr: string): string {
  const d = new Date(dateStr + "T12:00:00");
  return d.toLocaleDateString("en-US", {
    weekday: "long",
    month: "long",
    day: "numeric",
    year: "numeric",
    timeZone: "America/New_York",
  });
}

function roofLabel(type: "dome" | "retractable" | "open"): string {
  if (type === "dome") return "Dome";
  if (type === "retractable") return "Retractable Roof";
  return "Open Air";
}

function teamLogoUrl(teamId: number): string {
  return `https://www.mlbstatic.com/team-logos/team-cap-on-dark/${teamId}.svg`;
}

function decToAmerican(dec: number): number {
  if (dec >= 2.0) return Math.round((dec - 1) * 100);
  if (dec <= 1.0) return -9999;
  return Math.round(-100 / (dec - 1));
}

function fairProbToAmerican(prob: number): number {
  if (prob <= 0 || prob >= 1) return 0;
  return decToAmerican(1 / prob);
}

// ── Data fetching ────────────────────────────────────────────────────

async function fetchJSON<T>(path: string): Promise<T | null> {
  try {
    const res = await fetch(`${RAW}/${path}`, { cache: "no-store" });
    if (!res.ok) return null;
    return await res.json();
  } catch {
    return null;
  }
}

async function fetchMLBSchedule(date: string): Promise<TodaysGame[]> {
  try {
    const url = `${MLB_API}/schedule?date=${date}&sportId=1&hydrate=team,weather,venue,probablePitchers,officials,linescore`;
    const res = await fetch(url, { cache: "no-store" });
    if (!res.ok) return [];
    const data = await res.json();
    const dates = data.dates || [];
    if (dates.length === 0) return [];

    const games: TodaysGame[] = [];
    for (const g of dates[0].games) {
      const venue = g.venue?.name || "Unknown";
      const roofType = VENUE_ROOF[venue] || "open";

      let umpire: string | null = null;
      if (g.officials) {
        const hp = g.officials.find(
          // eslint-disable-next-line @typescript-eslint/no-explicit-any
          (o: any) => o.officialType === "Home Plate"
        );
        if (hp) umpire = hp.official.fullName;
      }

      let weather = null;
      if (g.weather && roofType !== "dome") {
        weather = {
          condition: g.weather.condition || "",
          temp: g.weather.temp || "",
          wind: g.weather.wind || "",
        };
      }

      const awayStarter = g.teams?.away?.probablePitcher
        ? {
            name: g.teams.away.probablePitcher.fullName,
            throws: "",
            stats: { velocity: null, kRate: null } as PitcherStats,
          }
        : null;

      const homeStarter = g.teams?.home?.probablePitcher
        ? {
            name: g.teams.home.probablePitcher.fullName,
            throws: "",
            stats: { velocity: null, kRate: null } as PitcherStats,
          }
        : null;

      // Determine game state and inning
      const detailedState = g.status?.detailedState || "Scheduled";
      const isLive = detailedState === "In Progress" || detailedState === "Manager Challenge";
      const isFinal = detailedState === "Final" || detailedState === "Game Over" || detailedState === "Completed Early";
      let currentInning: string | null = null;
      if (isLive && g.linescore) {
        const half = g.linescore.inningHalf === "Top" ? "Top" : "Bot";
        const ord = g.linescore.currentInningOrdinal || g.linescore.currentInning;
        currentInning = `${half} ${ord}`;
      }

      const awayScore = (isLive || isFinal) ? (g.teams?.away?.score ?? null) : null;
      const homeScore = (isLive || isFinal) ? (g.teams?.home?.score ?? null) : null;

      games.push({
        gamePk: g.gamePk,
        gameTime: g.gameDate,
        gameState: detailedState,
        awayTeam: g.teams?.away?.team?.name || "TBD",
        homeTeam: g.teams?.home?.team?.name || "TBD",
        awayAbbrev: g.teams?.away?.team?.abbreviation || "???",
        homeAbbrev: g.teams?.home?.team?.abbreviation || "???",
        awayTeamId: g.teams?.away?.team?.id || 0,
        homeTeamId: g.teams?.home?.team?.id || 0,
        awayScore,
        homeScore,
        currentInning,
        venue,
        roofType,
        weather,
        umpire,
        awayStarter,
        homeStarter,
        awayLineup: [],
        homeLineup: [],
        firstInningAway: null,
        firstInningHome: null,
      });
    }

    return games.sort(
      (a, b) => new Date(a.gameTime).getTime() - new Date(b.gameTime).getTime()
    );
  } catch {
    return [];
  }
}

async function fetchGameDetails(
  gamePk: number
): Promise<{
  awayLineup: LineupPlayer[];
  homeLineup: LineupPlayer[];
  awayStarterThrows: string;
  homeStarterThrows: string;
  awayStarterName: string | null;
  homeStarterName: string | null;
  awayScore: number | null;
  homeScore: number | null;
  currentInning: string | null;
  firstInningAway: number | null;
  firstInningHome: number | null;
} | null> {
  try {
    const res = await fetch(
      `${MLB_API}.1/game/${gamePk}/feed/live`,
      { cache: "no-store" }
    );
    if (!res.ok) return null;
    const data = await res.json();
    const box = data.liveData?.boxscore;
    if (!box) return null;

    const gdPlayers = data.gameData?.players || {};

    const extractLineup = (side: "away" | "home"): LineupPlayer[] => {
      const team = box.teams?.[side];
      const order: number[] = team?.battingOrder || [];
      if (order.length === 0) return [];

      return order.map((pid: number) => {
        const p = team.players?.[`ID${pid}`];
        const gdp = gdPlayers[`ID${pid}`];
        return {
          id: pid,
          name: p?.person?.fullName || `Player ${pid}`,
          batSide: gdp?.batSide?.code || "?",
          position: p?.position?.abbreviation || "",
        };
      });
    };

    const awayPitcherId = data.gameData?.probablePitchers?.away?.id;
    const homePitcherId = data.gameData?.probablePitchers?.home?.id;
    const awayPitcherGd = awayPitcherId ? gdPlayers[`ID${awayPitcherId}`] : null;
    const homePitcherGd = homePitcherId ? gdPlayers[`ID${homePitcherId}`] : null;

    // Get inning info from linescore
    const linescore = data.liveData?.linescore;
    let currentInning: string | null = null;
    if (linescore?.currentInning) {
      const half = linescore.inningHalf === "Top" ? "Top" : "Bot";
      const ord = linescore.currentInningOrdinal || linescore.currentInning;
      currentInning = `${half} ${ord}`;
    }

    // Extract 1st inning runs
    const innings = linescore?.innings || [];
    const firstInning = innings.length > 0 ? innings[0] : null;
    const firstInningAway = firstInning?.away?.runs ?? null;
    const firstInningHome = firstInning?.home?.runs ?? null;

    return {
      awayLineup: extractLineup("away"),
      homeLineup: extractLineup("home"),
      awayStarterThrows: awayPitcherGd?.pitchHand?.code || "",
      homeStarterThrows: homePitcherGd?.pitchHand?.code || "",
      awayStarterName: awayPitcherGd?.fullName || null,
      homeStarterName: homePitcherGd?.fullName || null,
      awayScore: linescore?.teams?.away?.runs ?? null,
      homeScore: linescore?.teams?.home?.runs ?? null,
      currentInning,
      firstInningAway,
      firstInningHome,
    };
  } catch {
    return null;
  }
}

function enrichWithLookups(
  games: TodaysGame[],
  lookups: Lookups | null
): TodaysGame[] {
  if (!lookups) return games;
  const pitchers = lookups.pitchers || {};

  return games.map((g) => {
    const enrichStarter = (
      starter: TodaysGame["awayStarter"],
      lineup: LineupPlayer[]
    ): TodaysGame["awayStarter"] => {
      if (!starter) return null;
      void pitchers;
      void lineup;
      return starter;
    };

    return {
      ...g,
      awayStarter: enrichStarter(g.awayStarter, g.awayLineup),
      homeStarter: enrichStarter(g.homeStarter, g.homeLineup),
    };
  });
}

// ── Components ───────────────────────────────────────────────────────

function StatCard({
  label,
  value,
  sub,
  color,
}: {
  label: string;
  value: string;
  sub?: string;
  color?: string;
}) {
  return (
    <div className="rounded-lg border border-[var(--card-border)] bg-[var(--card)] p-4">
      <div className="text-xs uppercase tracking-wider text-[var(--text-muted)]">
        {label}
      </div>
      <div className={`mt-1 text-2xl font-bold ${color || ""}`}>{value}</div>
      {sub && (
        <div className="mt-0.5 text-xs text-[var(--text-muted)]">{sub}</div>
      )}
    </div>
  );
}

function BetCard({ bet, isLive, gameTime, gameState }: { bet: Bet; isLive?: boolean; gameTime?: string; gameState?: string }) {
  const isYrfi = bet.bet_side === "YRFI";
  const modelPct = isYrfi ? bet.p_cal * 100 : (1 - bet.p_cal) * 100;
  const mktPct = isYrfi
    ? (bet.mkt_y_impl ?? bet.mkt_y_fair) * 100
    : (bet.mkt_n_impl ?? bet.mkt_n_fair) * 100;
  const hasResult = bet.result === "W" || bet.result === "L";
  const isFinal = gameState === "Final" || gameState === "Game Over" || gameState === "Completed Early";
  const isPreGame = gameState && !isFinal && !isLive && gameState !== "Postponed" && gameState !== "Suspended";

  // Build "why this pick" reasons from actual model features
  const reasons: string[] = [];
  const f = bet.pick_factors;
  if (f) {
    if (isYrfi) {
      // YRFI: highlight weak pitching, strong lineups, high park factors
      const awayFiRuns = f.away_fi_runs_per_start;
      const homeFiRuns = f.home_fi_runs_per_start;
      if (awayFiRuns >= 0.5) reasons.push(`${bet.away_starter} allows ${awayFiRuns.toFixed(2)} runs/1st inn`);
      if (homeFiRuns >= 0.5) reasons.push(`${bet.home_starter} allows ${homeFiRuns.toFixed(2)} runs/1st inn`);
      if (f.away_recent_fi_runs >= 0.6) reasons.push(`${bet.away_starter} trending worse: ${f.away_recent_fi_runs.toFixed(2)} runs/1st inn last 3 starts`);
      if (f.home_recent_fi_runs >= 0.6) reasons.push(`${bet.home_starter} trending worse: ${f.home_recent_fi_runs.toFixed(2)} runs/1st inn last 3 starts`);
      if (f.away_fi_k_rate < 0.18) reasons.push(`${bet.away_starter} low 1st-inn K rate (${(f.away_fi_k_rate * 100).toFixed(0)}%)`);
      if (f.home_fi_k_rate < 0.18) reasons.push(`${bet.home_starter} low 1st-inn K rate (${(f.home_fi_k_rate * 100).toFixed(0)}%)`);
      if (f.away_lineup_weighted_score >= 0.33) reasons.push(`${bet.away_team} lineup wOBA: ${f.away_lineup_weighted_score.toFixed(3)}`);
      if (f.home_lineup_weighted_score >= 0.33) reasons.push(`${bet.home_team} lineup wOBA: ${f.home_lineup_weighted_score.toFixed(3)}`);
      if (f.park_factor_runs >= 105) reasons.push(`Hitter-friendly park (${f.park_factor_runs.toFixed(0)} park factor)`);
      if (f.elevation_ft >= 4000) reasons.push(`High elevation (${f.elevation_ft.toLocaleString()} ft)`);
    } else {
      // NRFI: highlight strong pitching, weak lineups, pitcher-friendly park
      const awayK = f.away_fi_k_rate;
      const homeK = f.home_fi_k_rate;
      if (awayK >= 0.25) reasons.push(`${bet.away_starter} high 1st-inn K rate (${(awayK * 100).toFixed(0)}%)`);
      if (homeK >= 0.25) reasons.push(`${bet.home_starter} high 1st-inn K rate (${(homeK * 100).toFixed(0)}%)`);
      if (f.away_fi_runs_per_start <= 0.3) reasons.push(`${bet.away_starter} allows just ${f.away_fi_runs_per_start.toFixed(2)} runs/1st inn`);
      if (f.home_fi_runs_per_start <= 0.3) reasons.push(`${bet.home_starter} allows just ${f.home_fi_runs_per_start.toFixed(2)} runs/1st inn`);
      if (f.away_avg_velo >= 95) reasons.push(`${bet.away_starter} throws hard (${f.away_avg_velo.toFixed(1)} mph)`);
      if (f.home_avg_velo >= 95) reasons.push(`${bet.home_starter} throws hard (${f.home_avg_velo.toFixed(1)} mph)`);
      if (f.away_platoon_k_rate >= 0.25) reasons.push(`${bet.away_starter} dominant platoon K rate (${(f.away_platoon_k_rate * 100).toFixed(0)}%)`);
      if (f.home_platoon_k_rate >= 0.25) reasons.push(`${bet.home_starter} dominant platoon K rate (${(f.home_platoon_k_rate * 100).toFixed(0)}%)`);
      if (f.away_lineup_weighted_score <= 0.30) reasons.push(`${bet.away_team} lineup weak (${f.away_lineup_weighted_score.toFixed(3)} wOBA)`);
      if (f.home_lineup_weighted_score <= 0.30) reasons.push(`${bet.home_team} lineup weak (${f.home_lineup_weighted_score.toFixed(3)} wOBA)`);
      if (f.park_factor_runs <= 95) reasons.push(`Pitcher-friendly park (${f.park_factor_runs.toFixed(0)} park factor)`);
    }
    if (f.umpire_strike_rate >= 0.35 && isYrfi) reasons.push(`Generous umpire (${(f.umpire_strike_rate * 100).toFixed(1)}% strike rate) — may suppress scoring`);
    if (f.umpire_strike_rate <= 0.32 && isYrfi) reasons.push(`Tight umpire (${(f.umpire_strike_rate * 100).toFixed(1)}% strike rate) — more walks expected`);
    if (f.umpire_strike_rate >= 0.35 && !isYrfi) reasons.push(`Generous umpire (${(f.umpire_strike_rate * 100).toFixed(1)}% strike rate) — favors pitchers`);
    if (f.umpire_strike_rate <= 0.32 && !isYrfi) reasons.push(`Tight umpire (${(f.umpire_strike_rate * 100).toFixed(1)}% strike rate) — but lineups overcome it`);
  }
  // Weather factors (available even without pick_factors)
  if (bet.forecast_weather) {
    const wx = bet.forecast_weather;
    if (wx.temp >= 85 && isYrfi) reasons.push(`Hot weather (${wx.temp}°F) — ball carries farther`);
    if (wx.temp <= 45) reasons.push(`Cold weather (${wx.temp}°F) — affects grip and ball flight`);
    if (wx.wind_mph >= 15) reasons.push(`Windy conditions (${wx.wind_mph} mph)`);
    if (wx.precipitation_mm > 0) reasons.push(`Precipitation expected (${wx.precipitation_mm}mm)`);
  }

  return (
    <div
      className={`rounded-lg border bg-[var(--card)] p-4 ${
        hasResult
          ? bet.result === "W"
            ? "border-green-600/40"
            : "border-red-600/40"
          : "border-[var(--card-border)]"
      }`}
    >
      <div className="flex items-center justify-between">
        <div>
          <span className="text-sm text-[var(--text-muted)]">
            {bet.away_team} @ {bet.home_team}
          </span>
          <div className="mt-1 flex items-center gap-2">
            <span
              className={`rounded px-2 py-0.5 text-sm font-bold ${
                isYrfi
                  ? "bg-red-900/50 text-red-300"
                  : "bg-blue-900/50 text-blue-300"
              }`}
            >
              {bet.bet_side}
            </span>
            <span className="font-mono text-lg font-bold">
              {formatOdds(bet.bet_odds)}
            </span>
          </div>
          {bet.best_odds != null && bet.best_book && bet.best_odds !== bet.bet_odds && (
            <div className="mt-0.5 flex items-center gap-1.5 text-xs">
              <span className="text-[var(--text-muted)]">Best:</span>
              <span className="font-mono font-medium text-[var(--green)]">
                {formatOdds(bet.best_odds)}
              </span>
              <span className="rounded bg-[var(--card-border)] px-1.5 py-0.5 text-[10px] font-semibold tracking-wide">
                {BOOK_NAME[bet.best_book] ?? bet.best_book}
              </span>
            </div>
          )}
        </div>
        <div className="text-right">
          {hasResult ? (
            <div>
              <span
                className={`text-2xl font-bold ${
                  bet.result === "W"
                    ? "text-[var(--green)]"
                    : "text-[var(--red)]"
                }`}
              >
                {bet.result === "W" ? "✓" : "✗"}{" "}
                {bet.pnl !== undefined && (
                  <span className="text-lg">
                    ${bet.pnl > 0 ? "+" : ""}
                    {bet.pnl.toFixed(0)}
                  </span>
                )}
              </span>
              {bet.total_1st_runs !== undefined && (
                <div className="text-xs text-[var(--text-muted)]">
                  1st inn: {bet.away_1st_runs}-{bet.home_1st_runs}
                </div>
              )}
            </div>
          ) : isLive ? (
            <span className="rounded bg-yellow-900/40 px-2 py-0.5 text-xs text-yellow-300 animate-pulse">
              LIVE
            </span>
          ) : isPreGame && gameTime ? (
            <span className="text-xs text-[var(--text-muted)]">
              {formatGameTime(gameTime)}
            </span>
          ) : (
            <span className="rounded bg-yellow-900/40 px-2 py-0.5 text-xs text-yellow-300">
              PENDING
            </span>
          )}
        </div>
      </div>

      <div className="mt-3 grid grid-cols-2 sm:grid-cols-4 gap-2 text-xs">
        <div>
          <span className="text-[var(--text-muted)]">Model</span>
          <div className="font-mono font-medium">{modelPct.toFixed(1)}%</div>
        </div>
        <div>
          <span className="text-[var(--text-muted)]">Market</span>
          <div className="font-mono font-medium">{mktPct.toFixed(1)}%</div>
        </div>
        <div>
          <span className="text-[var(--text-muted)]">Edge</span>
          <div className="font-mono font-medium text-[var(--green)]">
            +{(bet.bet_edge * 100).toFixed(1)}%
          </div>
        </div>
        <div>
          <span className="text-[var(--text-muted)]">Stake</span>
          <div className="font-mono font-medium">{(bet.bet_kelly * 100).toFixed(1)}%</div>
        </div>
      </div>

      {/* Why this pick */}
      <div className="mt-3 border-t border-[var(--card-border)] pt-2">
        <div className="text-[10px] uppercase tracking-wider text-[var(--text-muted)] mb-1">Why this pick</div>
        <ul className="space-y-0.5 text-xs text-[var(--text-muted)]">
          {reasons.map((r, i) => (
            <li key={i} className="flex gap-1.5">
              <span className="text-[var(--green)] mt-0.5">•</span>
              <span>{r}</span>
            </li>
          ))}
        </ul>
      </div>

      <div className="mt-2 text-xs text-[var(--text-muted)]">
        {bet.away_starter} vs {bet.home_starter}
        {bet.umpire && ` · HP: ${bet.umpire}`}
      </div>
    </div>
  );
}

function AccordionSection({
  title,
  defaultOpen,
  children,
}: {
  title: string;
  defaultOpen?: boolean;
  children: React.ReactNode;
}) {
  const [open, setOpen] = useState(defaultOpen ?? false);

  return (
    <div className="border-t border-[var(--card-border)]">
      <button
        onClick={() => setOpen(!open)}
        className="flex w-full items-center justify-between px-4 py-2 text-xs font-medium uppercase tracking-wider text-[var(--text-muted)] hover:text-[var(--text)] transition-colors"
      >
        <span>{title}</span>
        <span className="text-[10px]">{open ? "▼" : "▶"}</span>
      </button>
      {open && <div className="px-4 pb-3">{children}</div>}
    </div>
  );
}


function GameCard({ game, bets, forecast }: { game: TodaysGame; bets?: Bet[]; forecast?: ForecastWeather }) {
  const hasAwayLineup = game.awayLineup.length > 0;
  const hasHomeLineup = game.homeLineup.length > 0;
  const hasLineups = hasAwayLineup && hasHomeLineup;

  const isFinal = game.gameState === "Final" || game.gameState === "Game Over" || game.gameState === "Completed Early";
  const isLive = game.gameState === "In Progress" || game.gameState === "Manager Challenge";
  const isPostponed = game.gameState === "Postponed" || game.gameState === "Suspended";
  const isDelayed = game.gameState === "Delayed" || game.gameState === "Delayed Start" || game.gameState === "Delay";
  const isPreGame = !isFinal && !isLive && !isPostponed && !isDelayed;

  const awayWins = isFinal && game.awayScore !== null && game.homeScore !== null && game.awayScore > game.homeScore;
  const homeWins = isFinal && game.awayScore !== null && game.homeScore !== null && game.homeScore > game.awayScore;

  return (
    <div className="rounded-lg border border-[var(--card-border)] bg-[var(--card)] overflow-hidden">
      {/* Teams row with status centered above @ */}
      <div className="flex items-center px-4 pt-3 pb-3">
        {/* Away side */}
        <div className="flex items-center gap-2 flex-1">
          <img
            src={teamLogoUrl(game.awayTeamId)}
            alt={game.awayAbbrev}
            className="h-7 w-7"
            onError={(e) => { (e.target as HTMLImageElement).style.display = "none"; }}
          />
          <span className={`text-sm font-semibold ${awayWins ? "text-[var(--text)]" : game.awayScore !== null ? "text-[var(--text-muted)]" : "text-[var(--text)]"}`}>
            {game.awayAbbrev}
          </span>
          {game.awayScore !== null && (
            <span className={`text-lg font-bold tabular-nums ${awayWins ? "text-[var(--text)]" : "text-[var(--text-muted)]"}`}>
              {game.awayScore}
            </span>
          )}
        </div>

        {/* Center: status */}
        <div className="flex flex-col items-center mx-3">
          {isPostponed && (
            <span className="rounded bg-orange-900/60 px-2 py-0.5 text-xs font-bold text-orange-300">
              PPD
            </span>
          )}
          {isDelayed && (
            <span className="rounded bg-yellow-900/60 px-2 py-0.5 text-xs font-bold text-yellow-300 animate-pulse">
              Delayed
            </span>
          )}
          {isPreGame && (
            <span className="text-xs text-[var(--text-muted)]">
              {formatGameTime(game.gameTime)}
            </span>
          )}
          {isLive && (
            <span className="rounded bg-green-900/60 px-2 py-0.5 text-xs font-bold text-green-300 animate-pulse">
              {game.currentInning || "LIVE"}
            </span>
          )}
          {isFinal && (
            <span className="text-xs font-medium text-[var(--text-muted)]">
              Final
            </span>
          )}
        </div>

        {/* Home side */}
        <div className="flex items-center gap-2 flex-1 justify-end">
          {game.homeScore !== null && (
            <span className={`text-lg font-bold tabular-nums ${homeWins ? "text-[var(--text)]" : "text-[var(--text-muted)]"}`}>
              {game.homeScore}
            </span>
          )}
          <span className={`text-sm font-semibold ${homeWins ? "text-[var(--text)]" : game.homeScore !== null ? "text-[var(--text-muted)]" : "text-[var(--text)]"}`}>
            {game.homeAbbrev}
          </span>
          <img
            src={teamLogoUrl(game.homeTeamId)}
            alt={game.homeAbbrev}
            className="h-7 w-7"
            onError={(e) => { (e.target as HTMLImageElement).style.display = "none"; }}
          />
        </div>
      </div>

      {/* Venue + Umpire */}
      <div className="border-t border-[var(--card-border)] px-4 py-2 text-xs text-[var(--text-muted)]">
        {game.venue} · {roofLabel(game.roofType)}
        {game.umpire && ` · HP: ${game.umpire}`}
      </div>

      {/* Weather - always visible */}
      {game.roofType === "dome" ? (
        <div className="border-t border-[var(--card-border)] px-4 py-2 text-xs text-[var(--text-muted)]">
          Indoor — no weather impact
        </div>
      ) : forecast ? (
        <div className="border-t border-[var(--card-border)] px-4 py-2 text-xs">
          <span className="text-[var(--text)]">
            {forecast.temp}°F · {forecast.humidity}% humidity
          </span>
          {forecast.wind_mph > 0 && (
            <span className="text-[var(--text-muted)]">
              {" "}· {forecast.wind_mph} mph wind
            </span>
          )}
          {forecast.precipitation_mm > 0 && (
            <span className="text-[var(--text-muted)]">
              {" "}· {forecast.precipitation_mm}mm precip
            </span>
          )}
          {game.roofType === "retractable" && (
            <span className="text-[var(--text-muted)]">
              {" "}(retractable roof)
            </span>
          )}
          <span className="text-[var(--text-muted)] ml-1">— forecast</span>
        </div>
      ) : game.weather ? (
        <div className="border-t border-[var(--card-border)] px-4 py-2 text-xs">
          <span className="text-[var(--text)]">
            {game.weather.temp}°F · {game.weather.condition}
          </span>
          {game.weather.wind && (
            <span className="text-[var(--text-muted)]">
              {" "}· {game.weather.wind}
            </span>
          )}
          {game.roofType === "retractable" && (
            <span className="text-[var(--text-muted)]">
              {" "}(retractable roof)
            </span>
          )}
        </div>
      ) : null}

      {/* Pitching Matchup & Lineups - combined accordion */}
      <AccordionSection title="Pitching & Lineups">
        {/* Pitching Matchup */}
        <div className="flex items-center gap-2 text-sm">
          <div className="flex-1">
            {game.awayStarter ? (
              <span>
                {game.awayStarter.throws && (
                  <span className="text-[var(--text-muted)]">
                    {game.awayStarter.throws}HP{" "}
                  </span>
                )}
                <span className="font-medium">{game.awayStarter.name}</span>
              </span>
            ) : (
              <span className="text-[var(--text-muted)]">TBD</span>
            )}
          </div>
          <span className="text-xs text-[var(--text-muted)]">vs</span>
          <div className="flex-1 text-right">
            {game.homeStarter ? (
              <span>
                <span className="font-medium">{game.homeStarter.name}</span>
                {game.homeStarter.throws && (
                  <span className="text-[var(--text-muted)]">
                    {" "}{game.homeStarter.throws}HP
                  </span>
                )}
              </span>
            ) : (
              <span className="text-[var(--text-muted)]">TBD</span>
            )}
          </div>
        </div>

        {/* Lineups */}
        {hasLineups ? (
          <div className="mt-3 grid grid-cols-2 gap-4 text-xs border-t border-[var(--card-border)] pt-3">
            <div>
              <div className="mb-1 font-medium text-[var(--text-muted)]">{game.awayAbbrev}</div>
              {game.awayLineup.map((p, i) => (
                <div key={p.id} className="flex justify-between py-0.5">
                  <span>
                    <span className="text-[var(--text-muted)] w-4 inline-block">
                      {i + 1}.
                    </span>{" "}
                    {p.name}
                  </span>
                  <span className="text-[var(--text-muted)] ml-1">
                    ({p.batSide})
                  </span>
                </div>
              ))}
            </div>
            <div>
              <div className="mb-1 font-medium text-[var(--text-muted)]">{game.homeAbbrev}</div>
              {game.homeLineup.map((p, i) => (
                <div key={p.id} className="flex justify-between py-0.5">
                  <span>
                    <span className="text-[var(--text-muted)] w-4 inline-block">
                      {i + 1}.
                    </span>{" "}
                    {p.name}
                  </span>
                  <span className="text-[var(--text-muted)] ml-1">
                    ({p.batSide})
                  </span>
                </div>
              ))}
            </div>
          </div>
        ) : (
          <div className="mt-3 pt-3 border-t border-[var(--card-border)] text-center text-xs text-[var(--text-muted)]">
            Lineups not yet announced — typically available ~2hrs before first
            pitch
          </div>
        )}
      </AccordionSection>

      {/* Picks accordion - only if there are bets for this game */}
      {bets && bets.length > 0 && (
        <AccordionSection title={`Picks (${bets.length})`}>
          <div className="space-y-2">
            {bets.map((bet, i) => {
              const isYrfi = bet.bet_side === "YRFI";
              const hasFirst = game.firstInningAway !== null && game.firstInningHome !== null;
              const firstTotal = (game.firstInningAway ?? 0) + (game.firstInningHome ?? 0);
              const betWon = isYrfi ? firstTotal > 0 : firstTotal === 0;
              return (
                <div key={i} className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-1 sm:gap-0 text-sm">
                  <div className="flex items-center gap-2">
                    <span
                      className={`rounded px-2 py-0.5 text-xs font-bold ${
                        isYrfi
                          ? "bg-red-900/50 text-red-300"
                          : "bg-blue-900/50 text-blue-300"
                      }`}
                    >
                      {bet.bet_side}
                    </span>
                    <span className="font-mono font-medium">
                      {formatOdds(bet.bet_odds)}
                    </span>
                  </div>
                  <div className="flex items-center gap-2 sm:gap-3 text-xs text-[var(--text-muted)]">
                    <span>
                      Edge{" "}
                      <span className="text-[var(--green)] font-mono">
                        +{(bet.bet_edge * 100).toFixed(1)}%
                      </span>
                    </span>
                    <span>
                      Stake{" "}
                      <span className="font-mono font-medium text-[var(--text)]">
                        {(bet.bet_kelly * 100).toFixed(1)}%
                      </span>
                    </span>
                    {!isPreGame && hasFirst && (
                      <span className={`font-bold ${betWon ? "text-[var(--green)]" : "text-[var(--red)]"}`}>
                        1st: {game.firstInningAway}-{game.firstInningHome}{" "}
                        {betWon ? "W" : "L"}
                      </span>
                    )}
                    {!isPreGame && !hasFirst && (
                      <span className="text-yellow-300 animate-pulse">Live</span>
                    )}
                  </div>
                </div>
              );
            })}
          </div>
        </AccordionSection>
      )}
    </div>
  );
}

function PnlChart({ daily }: { daily: Record<string, DayResults> }) {
  const dates = Object.keys(daily).sort();
  if (dates.length === 0) return null;

  let running = 0;
  const points = dates.map((d) => {
    running += daily[d].pnl;
    return { date: d, pnl: running };
  });

  const maxPnl = Math.max(...points.map((p) => p.pnl), 0);
  const minPnl = Math.min(...points.map((p) => p.pnl), 0);
  const range = maxPnl - minPnl || 1;

  const W = 800;
  const H = 200;
  const PAD = 30;
  const plotW = W - PAD * 2;
  const plotH = H - PAD * 2;

  const toX = (i: number) =>
    PAD + (i / Math.max(points.length - 1, 1)) * plotW;
  const toY = (v: number) => PAD + plotH - ((v - minPnl) / range) * plotH;

  const pathD = points
    .map(
      (p, i) =>
        `${i === 0 ? "M" : "L"} ${toX(i).toFixed(1)} ${toY(p.pnl).toFixed(1)}`
    )
    .join(" ");

  const zeroY = toY(0);
  const lastPnl = points[points.length - 1].pnl;

  return (
    <div className="rounded-lg border border-[var(--card-border)] bg-[var(--card)] p-4">
      <h3 className="mb-2 text-sm font-medium text-[var(--text-muted)]">
        Cumulative P&L
      </h3>
      <svg
        viewBox={`0 0 ${W} ${H}`}
        className="w-full"
        style={{ maxHeight: 250 }}
      >
        <line
          x1={PAD}
          y1={zeroY}
          x2={W - PAD}
          y2={zeroY}
          stroke="#333"
          strokeDasharray="4"
        />
        <text
          x={PAD - 4}
          y={zeroY + 4}
          textAnchor="end"
          fill="#666"
          fontSize="10"
        >
          $0
        </text>
        <path
          d={pathD}
          fill="none"
          stroke={lastPnl >= 0 ? "var(--green)" : "var(--red)"}
          strokeWidth="2.5"
        />
        <text
          x={toX(points.length - 1) + 6}
          y={toY(lastPnl) + 4}
          fill={lastPnl >= 0 ? "var(--green)" : "var(--red)"}
          fontSize="11"
          fontWeight="bold"
        >
          ${lastPnl >= 0 ? "+" : ""}
          {lastPnl.toFixed(0)}
        </text>
        <text
          x={PAD - 4}
          y={PAD + 4}
          textAnchor="end"
          fill="#666"
          fontSize="10"
        >
          ${maxPnl.toFixed(0)}
        </text>
        <text
          x={PAD - 4}
          y={H - PAD + 4}
          textAnchor="end"
          fill="#666"
          fontSize="10"
        >
          ${minPnl.toFixed(0)}
        </text>
      </svg>
    </div>
  );
}

function OddsSpectrum({ p_cal, mktYFair, mktNFair, betSide, passesFilter }: {
  p_cal: number;
  mktYFair: number;
  mktNFair: number;
  betSide: string | null;
  passesFilter: boolean;
}) {
  // Since YRFI/NRFI are inverses, use a single probability-based spectrum
  // Left = NRFI favored (low YRFI prob), Right = YRFI favored (high YRFI prob)
  // Range: 20% to 80% YRFI probability
  const MIN_P = 0.20;
  const MAX_P = 0.80;

  const probToPos = (p: number): number => {
    const clamped = Math.max(MIN_P, Math.min(MAX_P, p));
    return ((clamped - MIN_P) / (MAX_P - MIN_P)) * 100;
  };

  const bePos = probToPos(p_cal); // Model breakeven = p_cal
  const mktPos = probToPos(mktYFair); // Market implied YRFI prob

  // Breakeven American odds for display
  const yrfiBreakeven = fairProbToAmerican(p_cal);
  const nrfiBreakeven = fairProbToAmerican(1 - p_cal);
  // Apply ~6.5% vig to fair probs to approximate actual book lines
  const VIG = 1.065;
  const yrfiMarket = fairProbToAmerican(Math.min(mktYFair * VIG, 0.99));
  const nrfiMarket = fairProbToAmerican(Math.min(mktNFair * VIG, 0.99));

  // Edge: model thinks YRFI prob is higher than market → YRFI edge (bePos > mktPos)
  // Model thinks YRFI prob is lower than market → NRFI edge (bePos < mktPos)
  const hasYrfiEdge = p_cal > mktYFair;
  const hasNrfiEdge = p_cal < mktYFair;
  const edgeLeft = Math.min(bePos, mktPos);
  const edgeWidth = Math.abs(bePos - mktPos);

  const isYrfiBet = passesFilter && betSide === "YRFI";
  const isNrfiBet = passesFilter && betSide === "NRFI";
  const edgeColor = hasYrfiEdge ? "bg-red-900/50" : hasNrfiEdge ? "bg-blue-900/50" : "";
  const activeBetGlow = (isYrfiBet || isNrfiBet) ? "ring-1 ring-inset ring-white/20" : "";

  return (
    <div className="mt-2">
      {/* YRFI odds scale above */}
      {(() => {
        const yrfiMarkers = [
          { p: 0.25, label: "+300" },
          { p: 0.333, label: "+200" },
          { p: 0.40, label: "+150" },
          { p: 0.50, label: "±100" },
          { p: 0.60, label: "-150" },
          { p: 0.667, label: "-200" },
          { p: 0.75, label: "-300" },
        ];
        return (
          <div className="relative h-4 mb-0.5">
            <div className="absolute right-0 text-[9px] sm:text-[10px] font-bold text-red-300">YRFI</div>
            {yrfiMarkers.map(({ p, label }) => {
              const pos = probToPos(p);
              if (pos <= 2 || pos >= 92) return null;
              return (
                <div
                  key={label}
                  className="absolute text-[8px] sm:text-[9px] text-red-300/50 font-mono"
                  style={{ left: `${pos}%`, transform: "translateX(-50%)", top: "0" }}
                >
                  {label}
                </div>
              );
            })}
          </div>
        );
      })()}
      {/* Bar */}
      <div className="relative h-7 sm:h-8 bg-[var(--card-border)] rounded overflow-hidden">
        {/* Edge zone — from relevant market line to model */}
        {edgeWidth > 0.5 && (passesFilter ? (
          <div
            className={`absolute top-0 bottom-0 ${edgeColor} ${activeBetGlow}`}
            style={hasYrfiEdge
              ? { left: `calc(${mktPos}% + 3px)`, width: `calc(${bePos - mktPos}% - 3px)` }
              : { left: `${bePos}%`, width: `calc(${mktPos - bePos}% - 3px)` }
            }
          />
        ) : (
          <div
            className={`absolute top-[45%] h-[10%] rounded ${edgeColor}`}
            style={hasYrfiEdge
              ? { left: `calc(${mktPos}% + 3px)`, width: `calc(${bePos - mktPos}% - 3px)` }
              : { left: `${bePos}%`, width: `calc(${mktPos - bePos}% - 3px)` }
            }
          />
        ))}
        {/* Tick marks at odds positions */}
        {[0.25, 0.333, 0.40, 0.50, 0.60, 0.667, 0.75].map((p) => (
          <div
            key={p}
            className={`absolute top-0 bottom-0 w-px ${p === 0.5 ? "bg-white/15" : "bg-white/8"}`}
            style={{ left: `${probToPos(p)}%` }}
          />
        ))}
        {/* NRFI approx market line */}
        <div
          className="absolute top-0 bottom-0 w-0.5 bg-blue-400/60"
          style={{ left: `${mktPos}%`, transform: "translateX(-3px)" }}
          title={`≈NRFI Mkt: ${formatOdds(nrfiMarket)}`}
        />
        {/* YRFI approx market line */}
        <div
          className="absolute top-0 bottom-0 w-0.5 bg-red-400/60"
          style={{ left: `${mktPos}%`, transform: "translateX(3px)" }}
          title={`≈YRFI Mkt: ${formatOdds(yrfiMarket)}`}
        />
        {/* Model breakeven marker (slider) */}
        <div
          className="absolute top-1 bottom-1 w-2 sm:w-2.5 rounded-sm bg-amber-400"
          style={{ left: `${bePos}%`, transform: "translateX(-50%)" }}
          title={`Model: YRFI ${(p_cal * 100).toFixed(0)}%`}
        />
      </div>
      {/* NRFI odds scale below */}
      {(() => {
        // NRFI odds are inverse: at p_yrfi=0.25, NRFI is -300; at p_yrfi=0.75, NRFI is +300
        const nrfiMarkers = [
          { p: 0.25, label: "-300" },
          { p: 0.333, label: "-200" },
          { p: 0.40, label: "-150" },
          { p: 0.50, label: "±100" },
          { p: 0.60, label: "+150" },
          { p: 0.667, label: "+200" },
          { p: 0.75, label: "+300" },
        ];
        return (
          <div className="relative h-4 mt-0.5">
            <div className="absolute left-0 text-[9px] sm:text-[10px] font-bold text-blue-300">NRFI</div>
            {nrfiMarkers.map(({ p, label }) => {
              const pos = probToPos(p);
              if (pos <= 8 || pos >= 98) return null;
              return (
                <div
                  key={label}
                  className="absolute text-[8px] sm:text-[9px] text-blue-300/50 font-mono"
                  style={{ left: `${pos}%`, transform: "translateX(-50%)", top: "0" }}
                >
                  {label}
                </div>
              );
            })}
          </div>
        );
      })()}
      {/* BE / Mkt labels */}
      <div className="flex justify-between mt-1 text-[10px] sm:text-xs">
        <div className="text-blue-300 font-mono">
          <span className="text-[var(--text-muted)] text-[9px] sm:text-[10px]">BE </span>
          {formatOdds(nrfiBreakeven)}
          <span className="text-[var(--text-muted)] mx-1">|</span>
          <span className="text-[var(--text-muted)] text-[9px] sm:text-[10px]">≈Mkt </span>
          {formatOdds(nrfiMarket)}
        </div>
        <div className="text-red-300 font-mono">
          <span className="text-[var(--text-muted)] text-[9px] sm:text-[10px]">BE </span>
          {formatOdds(yrfiBreakeven)}
          <span className="text-[var(--text-muted)] mx-1">|</span>
          <span className="text-[var(--text-muted)] text-[9px] sm:text-[10px]">≈Mkt </span>
          {formatOdds(yrfiMarket)}
        </div>
      </div>
      {/* Legend */}
      <div className="flex items-center gap-3 text-[9px] sm:text-[10px] text-[var(--text-muted)] mt-1">
        <span><span className="inline-block w-2 h-2 bg-amber-400 mr-1 align-middle rounded-sm" /> Model</span>
        <span><span className="inline-block w-3 h-0.5 bg-blue-400/60 mr-1 align-middle" /> ≈NRFI Mkt</span>
        <span><span className="inline-block w-3 h-0.5 bg-red-400/60 mr-1 align-middle" /> ≈YRFI Mkt</span>
      </div>
    </div>
  );
}

function BreakevenCard({ game }: { game: Bet }) {
  const borderColor = game.passes_filter
    ? game.bet_side === "YRFI"
      ? "border-l-red-500"
      : "border-l-blue-500"
    : "border-l-transparent";

  const agreementColor =
    game.agreement === "YRFI"
      ? "bg-red-900/50 text-red-300"
      : game.agreement === "NRFI"
        ? "bg-blue-900/50 text-blue-300"
        : "bg-gray-700/50 text-gray-300";

  return (
    <div className={`rounded-lg border border-[var(--card-border)] bg-[var(--card)] p-3 sm:p-4 border-l-4 ${borderColor}`}>
      {/* Header */}
      <div className="flex items-center justify-between mb-2">
        <div>
          <div className="text-sm font-semibold">
            {game.away_team} @ {game.home_team}
          </div>
          <div className="text-xs text-[var(--text-muted)]">
            {game.away_starter} vs {game.home_starter}
          </div>
        </div>
        <div className="flex items-center gap-2">
          <span className={`rounded px-2 py-0.5 text-[10px] sm:text-xs font-bold ${agreementColor}`}>
            {game.agreement}
          </span>
          {game.passes_filter ? (
            <span className="text-xs font-bold text-[var(--green)]">
              +{(game.bet_edge * 100).toFixed(1)}%
            </span>
          ) : (
            <span className="text-[10px] sm:text-xs text-[var(--text-muted)]">
              {game.skip_reason}
            </span>
          )}
        </div>
      </div>

      {/* Spectrum */}
      <OddsSpectrum
        p_cal={game.p_cal}
        mktYFair={game.mkt_y_fair}
        mktNFair={game.mkt_n_fair}
        betSide={game.bet_side}
        passesFilter={game.passes_filter}
      />

      {/* Bet details if qualifying */}
      {game.passes_filter && (
        <div className="mt-2 flex items-center gap-2 text-xs">
          <span className={`rounded px-2 py-0.5 font-bold ${
            game.bet_side === "YRFI" ? "bg-red-900/50 text-red-300" : "bg-blue-900/50 text-blue-300"
          }`}>
            {game.bet_side}
          </span>
          <span className="font-mono font-medium">{formatOdds(game.bet_odds)}</span>
          <span className="text-[var(--text-muted)]">
            Stake <span className="font-mono font-medium text-[var(--text)]">{(game.bet_kelly * 100).toFixed(1)}%</span>
          </span>
        </div>
      )}
    </div>
  );
}

function CalendarView({ daily }: { daily: Record<string, DayResults> }) {
  const dates = Object.keys(daily).sort();
  if (dates.length === 0) return null;

  const firstDate = new Date(dates[0] + "T12:00:00");
  const lastDate = new Date(dates[dates.length - 1] + "T12:00:00");

  const [calMonth, setCalMonth] = useState({
    year: lastDate.getFullYear(),
    month: lastDate.getMonth(),
  });

  const minMonth = { year: firstDate.getFullYear(), month: firstDate.getMonth() };
  const maxMonth = { year: lastDate.getFullYear(), month: lastDate.getMonth() };

  const canPrev = calMonth.year > minMonth.year || (calMonth.year === minMonth.year && calMonth.month > minMonth.month);
  const canNext = calMonth.year < maxMonth.year || (calMonth.year === maxMonth.year && calMonth.month < maxMonth.month);

  const daysInMonth = new Date(calMonth.year, calMonth.month + 1, 0).getDate();
  const firstDayOfWeek = new Date(calMonth.year, calMonth.month, 1).getDay();

  const monthLabel = new Date(calMonth.year, calMonth.month).toLocaleDateString("en-US", {
    month: "long",
    year: "numeric",
  });

  const navigateMonth = (dir: -1 | 1) => {
    setCalMonth((prev) => {
      let m = prev.month + dir;
      let y = prev.year;
      if (m < 0) { m = 11; y--; }
      if (m > 11) { m = 0; y++; }
      return { year: y, month: m };
    });
  };

  const cells: (string | null)[] = [];
  for (let i = 0; i < firstDayOfWeek; i++) cells.push(null);
  for (let d = 1; d <= daysInMonth; d++) {
    const dateStr = `${calMonth.year}-${String(calMonth.month + 1).padStart(2, "0")}-${String(d).padStart(2, "0")}`;
    cells.push(dateStr);
  }

  return (
    <div className="rounded-lg border border-[var(--card-border)] bg-[var(--card)] p-4 mt-4">
      <div className="flex items-center justify-between mb-3">
        <button
          onClick={() => navigateMonth(-1)}
          disabled={!canPrev}
          className="px-2 py-1 text-sm text-[var(--text-muted)] hover:text-[var(--text)] disabled:opacity-30"
        >
          &lt;
        </button>
        <h3 className="text-sm font-medium">{monthLabel}</h3>
        <button
          onClick={() => navigateMonth(1)}
          disabled={!canNext}
          className="px-2 py-1 text-sm text-[var(--text-muted)] hover:text-[var(--text)] disabled:opacity-30"
        >
          &gt;
        </button>
      </div>
      <div className="grid grid-cols-7 gap-1 text-center text-xs">
        {["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"].map((d) => (
          <div key={d} className="py-1 text-[var(--text-muted)] font-medium">
            {d}
          </div>
        ))}
        {cells.map((dateStr, i) => {
          if (!dateStr) return <div key={`empty-${i}`} />;
          const day = daily[dateStr];
          const dayNum = parseInt(dateStr.split("-")[2]);
          const hasBets = !!day;
          const pnl = day?.pnl ?? 0;
          const bgClass = !hasBets
            ? ""
            : pnl > 0
              ? "bg-green-900/40"
              : pnl < 0
                ? "bg-red-900/40"
                : "bg-[var(--card-border)]";
          const textColor = !hasBets
            ? "text-[var(--text-muted)]"
            : pnl > 0
              ? "text-[var(--green)]"
              : pnl < 0
                ? "text-[var(--red)]"
                : "text-[var(--text)]";

          return (
            <div
              key={dateStr}
              className={`rounded p-1 ${bgClass} ${hasBets ? "cursor-pointer hover:ring-1 hover:ring-[var(--text-muted)]" : ""}`}
              onClick={() => {
                if (hasBets) {
                  const el = document.getElementById(`day-${dateStr}`);
                  el?.scrollIntoView({ behavior: "smooth", block: "center" });
                }
              }}
            >
              <div className="text-[10px] text-[var(--text-muted)]">{dayNum}</div>
              {hasBets && (
                <div className={`text-[10px] font-bold tabular-nums ${textColor}`}>
                  {pnl >= 0 ? "+" : ""}${pnl.toFixed(0)}
                </div>
              )}
            </div>
          );
        })}
      </div>
    </div>
  );
}

function BankrollChart({ daily }: { daily: Record<string, DayResults> }) {
  const dates = Object.keys(daily).sort();
  if (dates.length === 0) return null;

  let running = 1000;
  const points = dates.map((d) => {
    running += daily[d].pnl;
    return { date: d, bankroll: running };
  });
  points.unshift({ date: "", bankroll: 1000 });

  const maxVal = Math.max(...points.map((p) => p.bankroll));
  const minVal = Math.min(...points.map((p) => p.bankroll));
  const range = maxVal - minVal || 1;

  const W = 800;
  const H = 200;
  const PAD = 40;
  const plotW = W - PAD * 2;
  const plotH = H - PAD * 2;

  const toX = (i: number) => PAD + (i / Math.max(points.length - 1, 1)) * plotW;
  const toY = (v: number) => PAD + plotH - ((v - minVal) / range) * plotH;

  const pathD = points
    .map((p, i) => `${i === 0 ? "M" : "L"} ${toX(i).toFixed(1)} ${toY(p.bankroll).toFixed(1)}`)
    .join(" ");

  const lastVal = points[points.length - 1].bankroll;

  return (
    <div className="rounded-lg border border-[var(--card-border)] bg-[var(--card)] p-4">
      <h3 className="mb-2 text-sm font-medium text-[var(--text-muted)]">Bankroll Equity Curve</h3>
      <svg viewBox={`0 0 ${W} ${H}`} className="w-full" style={{ maxHeight: 250 }}>
        <line x1={PAD} y1={toY(1000)} x2={W - PAD} y2={toY(1000)} stroke="#333" strokeDasharray="4" />
        <text x={PAD - 4} y={toY(1000) + 4} textAnchor="end" fill="#666" fontSize="10">$1000</text>
        <path d={pathD} fill="none" stroke={lastVal >= 1000 ? "var(--green)" : "var(--red)"} strokeWidth="2.5" />
        <text x={toX(points.length - 1) + 6} y={toY(lastVal) + 4} fill={lastVal >= 1000 ? "var(--green)" : "var(--red)"} fontSize="11" fontWeight="bold">
          ${lastVal.toFixed(0)}
        </text>
        <text x={PAD - 4} y={PAD + 4} textAnchor="end" fill="#666" fontSize="10">${maxVal.toFixed(0)}</text>
        <text x={PAD - 4} y={H - PAD + 4} textAnchor="end" fill="#666" fontSize="10">${minVal.toFixed(0)}</text>
      </svg>
    </div>
  );
}

function CalibrationDriftChart({ daily }: { daily: Record<string, DayResults> }) {
  const dates = Object.keys(daily).sort();
  // Flatten all scored bets with dates
  const allBets: { date: string; p_cal: number; actual: number }[] = [];
  for (const d of dates) {
    for (const b of daily[d].bets) {
      if (b.result === "W" || b.result === "L") {
        const actual = (b.total_1st_runs !== undefined && b.total_1st_runs > 0) ? 1 : 0;
        allBets.push({ date: d, p_cal: b.p_cal, actual });
      }
    }
  }
  if (allBets.length < 30) return null;

  // Rolling 30-bet window
  const windowSize = 30;
  const points: { idx: number; predicted: number; actual: number }[] = [];
  for (let i = windowSize; i <= allBets.length; i++) {
    const window = allBets.slice(i - windowSize, i);
    const avgPredicted = window.reduce((s, b) => s + b.p_cal, 0) / window.length;
    const avgActual = window.reduce((s, b) => s + b.actual, 0) / window.length;
    points.push({ idx: i, predicted: avgPredicted, actual: avgActual });
  }

  const W = 800;
  const H = 200;
  const PAD = 40;
  const plotW = W - PAD * 2;
  const plotH = H - PAD * 2;

  const allVals = points.flatMap((p) => [p.predicted, p.actual]);
  const maxV = Math.max(...allVals, 0.6);
  const minV = Math.min(...allVals, 0.3);
  const range = maxV - minV || 0.1;

  const toX = (i: number) => PAD + (i / Math.max(points.length - 1, 1)) * plotW;
  const toY = (v: number) => PAD + plotH - ((v - minV) / range) * plotH;

  const predPath = points.map((p, i) => `${i === 0 ? "M" : "L"} ${toX(i).toFixed(1)} ${toY(p.predicted).toFixed(1)}`).join(" ");
  const actPath = points.map((p, i) => `${i === 0 ? "M" : "L"} ${toX(i).toFixed(1)} ${toY(p.actual).toFixed(1)}`).join(" ");

  const lastPred = points[points.length - 1].predicted;
  const lastAct = points[points.length - 1].actual;
  const gap = Math.abs(lastPred - lastAct);

  return (
    <div className="rounded-lg border border-[var(--card-border)] bg-[var(--card)] p-4">
      <h3 className="mb-2 text-sm font-medium text-[var(--text-muted)]">
        Calibration Drift (Rolling {windowSize}-bet)
      </h3>
      {gap > 0.05 && (
        <div className="mb-2 rounded bg-yellow-900/30 px-3 py-1.5 text-xs text-yellow-300">
          Divergence warning: {(gap * 100).toFixed(1)}% gap between predicted and actual YRFI rate
        </div>
      )}
      <svg viewBox={`0 0 ${W} ${H}`} className="w-full" style={{ maxHeight: 250 }}>
        <path d={predPath} fill="none" stroke="#3b82f6" strokeWidth="2" />
        <path d={actPath} fill="none" stroke="#f97316" strokeWidth="2" />
        <text x={W - PAD} y={toY(lastPred) - 6} textAnchor="end" fill="#3b82f6" fontSize="10">
          Predicted {(lastPred * 100).toFixed(1)}%
        </text>
        <text x={W - PAD} y={toY(lastAct) + 14} textAnchor="end" fill="#f97316" fontSize="10">
          Actual {(lastAct * 100).toFixed(1)}%
        </text>
        <text x={PAD - 4} y={PAD + 4} textAnchor="end" fill="#666" fontSize="10">{(maxV * 100).toFixed(0)}%</text>
        <text x={PAD - 4} y={H - PAD + 4} textAnchor="end" fill="#666" fontSize="10">{(minV * 100).toFixed(0)}%</text>
      </svg>
      <div className="mt-2 flex gap-4 text-xs text-[var(--text-muted)]">
        <span><span className="inline-block w-3 h-0.5 bg-blue-500 mr-1 align-middle" /> Predicted YRFI rate</span>
        <span><span className="inline-block w-3 h-0.5 bg-orange-500 mr-1 align-middle" /> Actual YRFI rate</span>
      </div>
    </div>
  );
}

// ── Results Day Card (expandable) ────────────────────────────────────

function ResultsDayCard({ date, day, pnlColor }: { date: string; day: DayResults; pnlColor: string }) {
  const [expanded, setExpanded] = useState(false);

  return (
    <div
      id={`day-${date}`}
      className="rounded-lg border border-[var(--card-border)] bg-[var(--card)] overflow-hidden"
    >
      <button
        onClick={() => setExpanded(!expanded)}
        className="w-full px-4 py-3 text-left hover:bg-[#1a1a1a] transition-colors"
      >
        <div className="flex items-center justify-between">
          <div>
            <div className="text-xs sm:text-sm font-medium">{formatDateHeading(date)}</div>
            <div className="mt-0.5 text-xs text-[var(--text-muted)]">
              {day.bets.length} bet{day.bets.length !== 1 ? "s" : ""}
              {day.all_scored ? "" : " · scoring in progress"}
            </div>
          </div>
          <div className="flex items-center gap-3">
            <div className="text-right">
              <div className="flex items-center gap-3">
                <span className="text-sm font-semibold">
                  {day.wins}W-{day.losses}L
                </span>
                <span className={`text-sm font-bold tabular-nums ${pnlColor}`}>
                  ${day.pnl >= 0 ? "+" : ""}{day.pnl.toFixed(0)}
                </span>
              </div>
              {day.wagered > 0 && (
                <div className="mt-0.5 text-xs text-[var(--text-muted)]">
                  {day.roi_pct >= 0 ? "+" : ""}{day.roi_pct.toFixed(1)}% ROI · ${day.wagered.toFixed(0)} wagered
                </div>
              )}
            </div>
            <span className="text-[10px] text-[var(--text-muted)]">{expanded ? "▼" : "▶"}</span>
          </div>
        </div>
      </button>

      {expanded && day.bets.length > 0 && (
        <div className="border-t border-[var(--card-border)] px-4 py-2">
          <table className="w-full text-xs">
            <thead>
              <tr className="text-[var(--text-muted)] uppercase tracking-wider">
                <th className="py-1 text-left font-medium">Pick</th>
                <th className="py-1 text-left font-medium">1st Inn</th>
                <th className="py-1 text-right font-medium">Edge</th>
                <th className="py-1 text-right font-medium">P&L</th>
              </tr>
            </thead>
            <tbody>
              {day.bets.map((bet, i) => {
                const isYrfi = bet.bet_side === "YRFI";
                const hasScore = bet.away_1st_runs !== undefined && bet.home_1st_runs !== undefined;
                const resultColor = bet.result === "W" ? "text-[var(--green)]" : bet.result === "L" ? "text-[var(--red)]" : "text-yellow-300";
                return (
                  <tr key={i} className="border-t border-[var(--card-border)]/50">
                    <td className="py-1.5">
                      <div className="flex items-center gap-2">
                        <span className={`rounded px-1.5 py-0.5 text-[10px] font-bold ${isYrfi ? "bg-red-900/50 text-red-300" : "bg-blue-900/50 text-blue-300"}`}>
                          {bet.bet_side}
                        </span>
                        <span className="text-[var(--text-muted)]">
                          {bet.away_team} @ {bet.home_team}
                        </span>
                      </div>
                    </td>
                    <td className="py-1.5">
                      {hasScore ? (
                        <span className={`font-mono ${resultColor}`}>
                          {bet.away_1st_runs}-{bet.home_1st_runs}{" "}
                          <span className="font-bold">{bet.result === "W" ? "W" : bet.result === "L" ? "L" : "?"}</span>
                        </span>
                      ) : (
                        <span className="text-yellow-300">Pending</span>
                      )}
                    </td>
                    <td className="py-1.5 text-right font-mono text-[var(--green)]">
                      +{(bet.bet_edge * 100).toFixed(1)}%
                    </td>
                    <td className={`py-1.5 text-right font-mono font-bold ${resultColor}`}>
                      {bet.pnl !== undefined
                        ? `${bet.pnl >= 0 ? "+" : ""}$${bet.pnl.toFixed(2)}`
                        : "—"}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}

// ── Page ─────────────────────────────────────────────────────────────

function todayET(): string {
  return new Date().toLocaleDateString("en-CA", {
    timeZone: "America/New_York",
  });
}

type TabId = "games" | "breakeven" | "predictions" | "results" | "risk";

export default function Home() {
  const [activeTab, setActiveTab] = useState<TabId>("games");

  // Predictions tab state
  const [predictions, setPredictions] = useState<DayPredictions | null>(null);
  const [results, setResults] = useState<Results | null>(null);
  const [loading, setLoading] = useState(false);
  const [selectedDate, setSelectedDate] = useState(todayET());

  // Results tab state
  const [resultsLimit, setResultsLimit] = useState(10);
  const [resultsLoading, setResultsLoading] = useState(false);

  // Today's Games tab state
  const [games, setGames] = useState<TodaysGame[]>([]);
  const [gamesLoading, setGamesLoading] = useState(true);
  const [gamesError, setGamesError] = useState<string | null>(null);
  const [lastRefresh, setLastRefresh] = useState<Date | null>(null);
  const [gameBets, setGameBets] = useState<Map<number, Bet[]>>(new Map());
  const [predsGeneratedAt, setPredsGeneratedAt] = useState<string | null>(null);
  const [gameForecasts, setGameForecasts] = useState<Map<number, ForecastWeather>>(new Map());
  const [analysisGames, setAnalysisGames] = useState<Bet[]>([]);

  // Load predictions when predictions tab is active or date changes
  useEffect(() => {
    if (activeTab !== "predictions") return;
    async function load() {
      setLoading(true);
      const [preds, res] = await Promise.all([
        fetchJSON<DayPredictions>(`predictions/${selectedDate}.json`),
        fetchJSON<Results>("results/results.json"),
      ]);
      setPredictions(preds);
      setResults(res);
      setLoading(false);
    }
    load();
  }, [selectedDate, activeTab]);

  // Load results when results or risk tab is active
  useEffect(() => {
    if (activeTab !== "results" && activeTab !== "risk") return;
    if (results) return; // already loaded
    async function load() {
      setResultsLoading(true);
      const res = await fetchJSON<Results>("results/results.json");
      setResults(res);
      setResultsLoading(false);
    }
    load();
  }, [activeTab, results]);

  // Load today's games
  const loadGames = useCallback(async () => {
    setGamesLoading(true);
    setGamesError(null);
    try {
      const today = todayET();
      const schedule = await fetchMLBSchedule(today);

      if (schedule.length === 0) {
        setGames([]);
        setGamesLoading(false);
        setLastRefresh(new Date());
        return;
      }

      // Fetch game details (lineups, pitcher hand) in parallel
      const detailResults = await Promise.allSettled(
        schedule.map((g) => fetchGameDetails(g.gamePk))
      );

      const enriched = schedule.map((g, i) => {
        const detail =
          detailResults[i].status === "fulfilled"
            ? detailResults[i].value
            : null;
        if (!detail) return g;

        // Use game feed pitcher names as fallback when schedule API didn't have them
        const awayStarter = g.awayStarter
          ? { ...g.awayStarter, throws: detail.awayStarterThrows }
          : detail.awayStarterName
            ? { name: detail.awayStarterName, throws: detail.awayStarterThrows, stats: { velocity: null, kRate: null } as PitcherStats }
            : null;

        const homeStarter = g.homeStarter
          ? { ...g.homeStarter, throws: detail.homeStarterThrows }
          : detail.homeStarterName
            ? { name: detail.homeStarterName, throws: detail.homeStarterThrows, stats: { velocity: null, kRate: null } as PitcherStats }
            : null;

        return {
          ...g,
          awayLineup: detail.awayLineup,
          homeLineup: detail.homeLineup,
          awayStarter,
          homeStarter,
          // Only use game feed scores for live/final games (pregame linescore returns 0)
          awayScore: g.awayScore !== null ? (detail.awayScore ?? g.awayScore) : null,
          homeScore: g.homeScore !== null ? (detail.homeScore ?? g.homeScore) : null,
          currentInning: detail.currentInning ?? g.currentInning,
          firstInningAway: detail.firstInningAway,
          firstInningHome: detail.firstInningHome,
        };
      });

      // Try to enrich with lookups (pitcher stats)
      const lookups = await fetchJSON<Lookups>("models/v4/lookups.json");
      const final = enrichWithLookups(enriched, lookups);

      setGames(final);
      setLastRefresh(new Date());

      // Fetch today's predictions to show picks on game cards
      const todayPreds = await fetchJSON<DayPredictions>(`predictions/${today}.json`);
      if (todayPreds?.bets) {
        const byGame = new Map<number, Bet[]>();
        for (const b of todayPreds.bets) {
          const existing = byGame.get(b.game_pk) || [];
          existing.push(b);
          byGame.set(b.game_pk, existing);
        }
        setGameBets(byGame);
        if (todayPreds.generated_at) setPredsGeneratedAt(todayPreds.generated_at);
      }
      // Build forecast weather map from all predicted games
      if (todayPreds?.games) {
        const forecasts = new Map<number, ForecastWeather>();
        for (const g of todayPreds.games) {
          if (g.forecast_weather) forecasts.set(g.game_pk, g.forecast_weather);
        }
        setGameForecasts(forecasts);
        setAnalysisGames(todayPreds.games);
      }
    } catch {
      setGamesError("Failed to load games. Try refreshing.");
    } finally {
      setGamesLoading(false);
    }
  }, []);

  useEffect(() => {
    if (activeTab !== "games" && activeTab !== "breakeven" && activeTab !== "predictions") return;
    loadGames();

    // Auto-refresh every 5 minutes
    const interval = setInterval(loadGames, 5 * 60 * 1000);
    return () => clearInterval(interval);
  }, [activeTab, loadGames]);

  // Build a map of live first-inning scores from the Games tab data
  const liveFirstInning = new Map(
    games.map((g) => [g.gamePk, { away: g.firstInningAway, home: g.firstInningHome, inning: g.currentInning, gameState: g.gameState, gameTime: g.gameTime }])
  );

  // Merge results into predictions if available
  const betsWithResults: Bet[] = (() => {
    if (!predictions) return [];
    const bets = predictions.bets || [];
    const scored = results?.daily?.[selectedDate]?.bets;
    const scoredMap = scored ? new Map(scored.map((b) => [b.game_pk, b])) : new Map();

    return bets.map((b) => {
      const s = scoredMap.get(b.game_pk);
      if (s && (s.result === "W" || s.result === "L")) {
        return { ...b, ...s };
      }
      // Use live game data for unscored bets
      const live = liveFirstInning.get(b.game_pk);
      if (live && live.away !== null && live.home !== null) {
        const totalFirst = live.away + live.home;
        const isYrfi = b.bet_side === "YRFI";
        const won = isYrfi ? totalFirst > 0 : totalFirst === 0;
        return {
          ...b,
          result: won ? "W" as const : "L" as const,
          pnl: won ? (b.bet_dec - 1) * b.stake : -b.stake,
          away_1st_runs: live.away,
          home_1st_runs: live.home,
          total_1st_runs: totalFirst,
        };
      }
      return b;
    });
  })();

  const cum = results?.cumulative;
  const availableDates = results
    ? Object.keys(results.daily).sort().reverse()
    : [];

  const tabClass = (tab: TabId) =>
    `px-3 sm:px-4 py-2 text-xs sm:text-sm font-medium rounded-t-lg transition-colors whitespace-nowrap ${
      activeTab === tab
        ? "bg-[var(--card)] text-[var(--text)] border border-b-0 border-[var(--card-border)]"
        : "text-[var(--text-muted)] hover:text-[var(--text)]"
    }`;

  return (
    <main className="mx-auto max-w-4xl px-4 py-8">
      {/* Header */}
      <div className="mb-2 inline-flex items-end gap-3">
        <img
          src="/logo.png"
          alt="Betty"
          className="h-28 w-auto"
        />
        <p className="text-xs font-medium tracking-wide uppercase bg-gradient-to-r from-red-400 via-white to-blue-400 bg-clip-text text-transparent pb-1">
          A 5 Big Guys Model
        </p>
      </div>

      {/* Tab Bar */}
      <div className="mb-6 flex gap-1 border-b border-[var(--card-border)]">
        <button className={tabClass("games")} onClick={() => setActiveTab("games")}>
          Games
        </button>
        <button
          className={tabClass("breakeven")}
          onClick={() => setActiveTab("breakeven")}
        >
          Edge
        </button>
        <button
          className={tabClass("predictions")}
          onClick={() => setActiveTab("predictions")}
        >
          Picks
        </button>
        <button
          className={tabClass("results")}
          onClick={() => setActiveTab("results")}
        >
          Results
        </button>
        <button
          className={tabClass("risk")}
          onClick={() => setActiveTab("risk")}
        >
          Risk
        </button>
      </div>

      {/* ── TODAY'S GAMES TAB ──────────────────────────────────────── */}
      {activeTab === "games" && (
        <div>
          {/* Date heading + refresh */}
          <div className="mb-4 flex flex-col sm:flex-row sm:items-center justify-between gap-2">
            <h2 className="text-base sm:text-lg font-semibold">
              {formatDateHeading(todayET())}
            </h2>
            <div className="flex items-center gap-2">
              {lastRefresh && (
                <span className="text-xs text-[var(--text-muted)]">
                  Updated{" "}
                  {lastRefresh.toLocaleTimeString("en-US", {
                    hour: "numeric",
                    minute: "2-digit",
                    timeZone: "America/New_York",
                  })}
                </span>
              )}
              <button
                onClick={loadGames}
                disabled={gamesLoading}
                className="rounded border border-[var(--card-border)] bg-[var(--card)] px-3 py-1 text-xs hover:bg-[#1a1a1a] disabled:opacity-50"
              >
                {gamesLoading ? "Loading..." : "Refresh"}
              </button>
            </div>
          </div>

          {/* Loading */}
          {gamesLoading && games.length === 0 && (
            <div className="py-12 text-center text-[var(--text-muted)]">
              Loading today&apos;s games...
            </div>
          )}

          {/* Error */}
          {gamesError && (
            <div className="rounded-lg border border-red-600/40 bg-[var(--card)] p-8 text-center">
              <div className="text-[var(--red)]">{gamesError}</div>
              <button
                onClick={loadGames}
                className="mt-3 rounded border border-[var(--card-border)] bg-[var(--card)] px-4 py-1.5 text-sm hover:bg-[#1a1a1a]"
              >
                Retry
              </button>
            </div>
          )}

          {/* No games */}
          {!gamesLoading && !gamesError && games.length === 0 && (
            <div className="rounded-lg border border-[var(--card-border)] bg-[var(--card)] p-8 text-center">
              <div className="text-lg font-medium">
                No MLB games scheduled today
              </div>
            </div>
          )}

          {/* Game cards */}
          {games.length > 0 && (
            <div>
              <div className="mb-3 text-sm text-[var(--text-muted)]">
                {games.length} game{games.length !== 1 ? "s" : ""} scheduled
              </div>
              <div className="grid gap-3 sm:gap-4 sm:grid-cols-2">
                {games.map((g) => (
                  <GameCard key={g.gamePk} game={g} bets={gameBets.get(g.gamePk)} forecast={gameForecasts.get(g.gamePk)} />
                ))}
              </div>
            </div>
          )}
        </div>
      )}

      {/* ── BREAKEVEN ODDS TAB ─────────────────────────────────────── */}
      {activeTab === "breakeven" && (
        <div>
          {gamesLoading && analysisGames.length === 0 && (
            <div className="py-12 text-center text-[var(--text-muted)]">
              Loading analysis...
            </div>
          )}

          {!gamesLoading && analysisGames.length === 0 && (
            <div className="rounded-lg border border-[var(--card-border)] bg-[var(--card)] p-8 text-center">
              <div className="text-lg font-medium">
                Awaiting lineup data
              </div>
              <div className="mt-1 text-sm text-[var(--text-muted)]">
                Predictions update throughout the day as lineups are posted
              </div>
            </div>
          )}

          {(analysisGames.length > 0 || games.length > 0) && (() => {
            const analyzedPks = new Set(analysisGames.map((g) => g.game_pk));
            const pendingGames = games.filter((g) => !analyzedPks.has(g.gamePk));
            return (
            <div>
              <div className="mb-3 text-sm text-[var(--text-muted)]">
                {analysisGames.length} game{analysisGames.length !== 1 ? "s" : ""} analyzed
                {pendingGames.length > 0 && ` · ${pendingGames.length} pending`}
                {" · "}
                {analysisGames.filter((g) => g.passes_filter).length} qualifying bet{analysisGames.filter((g) => g.passes_filter).length !== 1 ? "s" : ""}
              </div>
              <div className="grid gap-3">
                {[...analysisGames]
                  .sort((a, b) => {
                    // Qualifying bets first, then by edge descending
                    if (a.passes_filter !== b.passes_filter) return a.passes_filter ? -1 : 1;
                    return b.bet_edge - a.bet_edge;
                  })
                  .map((g) => (
                    <BreakevenCard key={g.game_pk} game={g} />
                  ))}
                {pendingGames.map((g) => (
                  <div key={g.gamePk} className="rounded-lg border border-[var(--card-border)] bg-[var(--card)] p-4 flex items-center justify-between">
                    <div>
                      <div className="text-sm font-medium">{g.awayAbbrev} @ {g.homeAbbrev}</div>
                      <div className="text-xs text-[var(--text-muted)]">
                        {g.awayStarter?.name ?? "TBD"} vs {g.homeStarter?.name ?? "TBD"}
                      </div>
                    </div>
                    <div className="flex items-center gap-2">
                      <span className="text-xs text-[var(--text-muted)]">{formatGameTime(g.gameTime)}</span>
                      <span className="rounded bg-yellow-900/40 px-2 py-0.5 text-xs text-yellow-300">
                        PENDING
                      </span>
                    </div>
                  </div>
                ))}
              </div>
            </div>
            );
          })()}
        </div>
      )}

      {/* ── PREDICTIONS TAB ────────────────────────────────────────── */}
      {activeTab === "predictions" && (
        <div>
          {/* Date Picker */}
          <div className="mb-6 flex flex-wrap items-center gap-2 sm:gap-3">
            <input
              type="date"
              value={selectedDate}
              onChange={(e) => setSelectedDate(e.target.value)}
              className="rounded border border-[var(--card-border)] bg-[var(--card)] px-3 py-1.5 text-sm text-[var(--text)]"
            />
            <button
              onClick={() => setSelectedDate(todayET())}
              className="rounded border border-[var(--card-border)] bg-[var(--card)] px-3 py-1.5 text-sm hover:bg-[#1a1a1a]"
            >
              Today
            </button>
            {availableDates.length > 0 && (
              <select
                value={selectedDate}
                onChange={(e) => setSelectedDate(e.target.value)}
                className="rounded border border-[var(--card-border)] bg-[var(--card)] px-3 py-1.5 text-sm text-[var(--text)]"
              >
                <option value="">Recent dates...</option>
                {availableDates.slice(0, 30).map((d) => (
                  <option key={d} value={d}>
                    {d}
                    {results?.daily[d] &&
                      ` (${results.daily[d].wins}W-${results.daily[d].losses}L)`}
                  </option>
                ))}
              </select>
            )}
          </div>

          {/* Loading */}
          {loading && (
            <div className="py-12 text-center text-[var(--text-muted)]">
              Loading...
            </div>
          )}

          {/* No predictions */}
          {!loading && !predictions && (
            <div className="rounded-lg border border-[var(--card-border)] bg-[var(--card)] p-8 text-center">
              <div className="text-lg font-medium">
                No predictions for {selectedDate}
              </div>
              <div className="mt-1 text-sm text-[var(--text-muted)]">
                Predictions appear once lineups are posted (typically 1-3 hours
                before first pitch)
              </div>
            </div>
          )}

          {/* Predictions with no bets */}
          {!loading && predictions && betsWithResults.length === 0 && (
            <div className="rounded-lg border border-[var(--card-border)] bg-[var(--card)] p-8 text-center">
              <div className="text-lg font-medium">
                {predictions.summary.total_games} games analyzed — no qualifying
                bets
              </div>
              <div className="mt-1 text-sm text-[var(--text-muted)]">
                Selectivity filter requires sub-model agreement + Kelly &gt; 1%
              </div>
            </div>
          )}

          {/* Bet cards */}
          {!loading && betsWithResults.length > 0 && (() => {
            const wins = betsWithResults.filter((b) => b.result === "W").length;
            const losses = betsWithResults.filter((b) => b.result === "L").length;
            const pending = betsWithResults.filter((b) => b.result !== "W" && b.result !== "L").length;
            const dayPnl = betsWithResults.reduce((sum, b) => sum + (b.pnl ?? 0), 0);
            const hasAnyResult = wins + losses > 0;

            return (
              <div>
                {/* Daily Tracker */}
                <div className="mb-4 rounded-lg border border-[var(--card-border)] bg-[var(--card)] px-4 py-3">
                  <div className="flex items-center justify-between">
                    <div className="flex items-center gap-4">
                      <div>
                        <div className="text-xs uppercase tracking-wider text-[var(--text-muted)]">Record</div>
                        <div className="text-lg font-bold">
                          {wins}W-{losses}L
                        </div>
                      </div>
                      {pending > 0 && (
                        <div>
                          <div className="text-xs uppercase tracking-wider text-[var(--text-muted)]">Pending</div>
                          <div className="text-lg font-bold text-yellow-300">{pending}</div>
                        </div>
                      )}
                    </div>
                    {hasAnyResult && (
                      <div className="text-right">
                        <div className="text-xs uppercase tracking-wider text-[var(--text-muted)]">Daily P&L</div>
                        <div className={`text-lg font-bold tabular-nums ${dayPnl >= 0 ? "text-[var(--green)]" : "text-[var(--red)]"}`}>
                          ${dayPnl >= 0 ? "+" : ""}{dayPnl.toFixed(2)}
                        </div>
                      </div>
                    )}
                  </div>
                </div>

                <div className="mb-3 flex items-center justify-between">
                  <h2 className="text-lg font-semibold">
                    {betsWithResults.length} Pick
                    {betsWithResults.length !== 1 ? "s" : ""}
                  </h2>
                  {predictions && (
                    <span className="text-xs text-[var(--text-muted)]">
                      {predictions.summary.total_games} games analyzed
                    </span>
                  )}
                </div>
                <div className="grid gap-3">
                  {betsWithResults.map((b) => {
                    const live = liveFirstInning.get(b.game_pk);
                    const gameIsLive = !b.result && (live?.gameState === "In Progress" || live?.gameState === "Manager Challenge");
                    return (
                      <BetCard
                        key={b.game_pk}
                        bet={b}
                        isLive={gameIsLive}
                        gameTime={live?.gameTime}
                        gameState={live?.gameState}
                      />
                    );
                  })}
                </div>
              </div>
            );
          })()}
        </div>
      )}

      {/* ── RESULTS TAB ───────────────────────────────────────────── */}
      {activeTab === "results" && (
        <div>
          {/* Loading */}
          {resultsLoading && (
            <div className="py-12 text-center text-[var(--text-muted)]">
              Loading results...
            </div>
          )}

          {/* No results */}
          {!resultsLoading && !results && (
            <div className="rounded-lg border border-[var(--card-border)] bg-[var(--card)] p-8 text-center">
              <div className="text-lg font-medium">No results available</div>
            </div>
          )}

          {/* Results content */}
          {!resultsLoading && results && (() => {
            const allDates = Object.keys(results.daily).sort().reverse();
            const totalDays = allDates.length;
            const shownDates = allDates.slice(0, resultsLimit);

            return (
              <>
                {/* Cumulative Stats */}
                {cum && cum.total_bets > 0 && (
                  <div className="mb-6">
                    <div className="mb-3 grid grid-cols-2 gap-2 sm:gap-3 sm:grid-cols-4">
                      <StatCard
                        label="Record"
                        value={`${cum.wins}W-${cum.losses}L`}
                        sub={`${cum.win_rate_pct}% win rate`}
                      />
                      <StatCard
                        label="Profit"
                        value={`$${cum.profit >= 0 ? "+" : ""}${cum.profit.toFixed(0)}`}
                        sub={`$${cum.wagered.toFixed(0)} wagered`}
                        color={
                          cum.profit >= 0
                            ? "text-[var(--green)]"
                            : "text-[var(--red)]"
                        }
                      />
                      <StatCard
                        label="ROI"
                        value={`${cum.roi_pct >= 0 ? "+" : ""}${cum.roi_pct.toFixed(1)}%`}
                        sub={`${cum.total_bets} total bets`}
                        color={
                          cum.roi_pct >= 0
                            ? "text-[var(--green)]"
                            : "text-[var(--red)]"
                        }
                      />
                      <StatCard
                        label="Max Drawdown"
                        value={`$${cum.max_drawdown.toFixed(0)}`}
                        sub={`Peak: $+${cum.peak_profit.toFixed(0)}`}
                      />
                    </div>
                    <PnlChart daily={results.daily} />
                    <CalendarView daily={results.daily} />
                  </div>
                )}

                {/* Daily results list */}
                {totalDays === 0 ? (
                  <div className="rounded-lg border border-[var(--card-border)] bg-[var(--card)] p-8 text-center">
                    <div className="text-lg font-medium">No daily results yet</div>
                    <div className="mt-1 text-sm text-[var(--text-muted)]">
                      Results will appear once today&apos;s games are scored
                    </div>
                  </div>
                ) : (
                  <>
                    <div className="mb-3 text-sm text-[var(--text-muted)]">
                      Showing {Math.min(shownDates.length, totalDays)} of {totalDays} days
                    </div>
                    <div className="grid gap-3">
                      {shownDates.map((date) => {
                        const day = results.daily[date];
                        const pnlColor = day.pnl >= 0 ? "text-[var(--green)]" : "text-[var(--red)]";
                        return (
                          <ResultsDayCard key={date} date={date} day={day} pnlColor={pnlColor} />
                        );
                      })}
                    </div>

                    {/* Pagination */}
                    {shownDates.length < totalDays && (
                      <div className="mt-4 flex items-center justify-center gap-3">
                        <button
                          onClick={() => setResultsLimit((prev) => prev + 10)}
                          className="rounded border border-[var(--card-border)] bg-[var(--card)] px-4 py-1.5 text-sm hover:bg-[#1a1a1a]"
                        >
                          Show 10 More
                        </button>
                        <button
                          onClick={() => setResultsLimit(totalDays)}
                          className="rounded border border-[var(--card-border)] bg-[var(--card)] px-4 py-1.5 text-sm hover:bg-[#1a1a1a]"
                        >
                          Show All
                        </button>
                      </div>
                    )}
                  </>
                )}
              </>
            );
          })()}
        </div>
      )}

      {/* ── RISK TAB ────────────────────────────────────────────── */}
      {activeTab === "risk" && (
        <div>
          {resultsLoading && (
            <div className="py-12 text-center text-[var(--text-muted)]">Loading risk data...</div>
          )}

          {!resultsLoading && !results && (
            <div className="rounded-lg border border-[var(--card-border)] bg-[var(--card)] p-8 text-center">
              <div className="text-lg font-medium">No results available</div>
            </div>
          )}

          {!resultsLoading && results && (() => {
            const dates = Object.keys(results.daily).sort();
            let running = 0;
            let peak = 0;
            let maxDrawdown = 0;
            let maxDdStart = "";
            let maxDdEnd = "";
            let currentStreak = 0;
            let streakType: "W" | "L" | null = null;

            for (const d of dates) {
              const dayPnl = results.daily[d].pnl;
              running += dayPnl;
              if (running > peak) peak = running;
              const dd = peak - running;
              if (dd > maxDrawdown) {
                maxDrawdown = dd;
                maxDdEnd = d;
                // Find drawdown start (peak date)
                let peakRunning = 0;
                for (const pd of dates) {
                  peakRunning += results.daily[pd].pnl;
                  if (peakRunning >= peak) { maxDdStart = pd; break; }
                }
              }
              // Streak
              if (dayPnl > 0) {
                if (streakType === "W") currentStreak++;
                else { streakType = "W"; currentStreak = 1; }
              } else if (dayPnl < 0) {
                if (streakType === "L") currentStreak++;
                else { streakType = "L"; currentStreak = 1; }
              }
            }

            const currentBankroll = 1000 + running;

            return (
              <>
                {/* Drawdown & Bankroll Stats */}
                <div className="mb-6 grid grid-cols-2 gap-2 sm:gap-3 sm:grid-cols-3">
                  <StatCard
                    label="Current Bankroll"
                    value={`$${currentBankroll.toFixed(0)}`}
                    sub={`$1000 base ${running >= 0 ? "+" : ""}$${running.toFixed(0)} P&L`}
                    color={running >= 0 ? "text-[var(--green)]" : "text-[var(--red)]"}
                  />
                  <StatCard
                    label="Max Drawdown"
                    value={`-$${maxDrawdown.toFixed(0)}`}
                    sub={maxDdStart && maxDdEnd ? `${maxDdStart} to ${maxDdEnd}` : "N/A"}
                    color="text-[var(--red)]"
                  />
                  <StatCard
                    label="Current Streak"
                    value={`${currentStreak}${streakType || ""}`}
                    sub={streakType === "W" ? "winning days" : streakType === "L" ? "losing days" : ""}
                    color={streakType === "W" ? "text-[var(--green)]" : streakType === "L" ? "text-[var(--red)]" : ""}
                  />
                </div>

                {/* Bankroll Equity Curve */}
                <BankrollChart daily={results.daily} />

                {/* Edge-Level Performance */}
                {(() => {
                  // Bucket all bets by edge level
                  const allBets: Bet[] = [];
                  for (const d of dates) {
                    for (const b of results.daily[d].bets) {
                      if (b.result === "W" || b.result === "L") allBets.push(b);
                    }
                  }
                  if (allBets.length === 0) return null;

                  const edgeBuckets = [
                    { label: "1-3%", min: 0.01, max: 0.03 },
                    { label: "3-5%", min: 0.03, max: 0.05 },
                    { label: "5-8%", min: 0.05, max: 0.08 },
                    { label: "8-12%", min: 0.08, max: 0.12 },
                    { label: "12%+", min: 0.12, max: Infinity },
                  ];

                  const bucketData = edgeBuckets.map(({ label, min, max }) => {
                    const bets = allBets.filter((b) => b.bet_edge >= min && b.bet_edge < max);
                    const wins = bets.filter((b) => b.result === "W").length;
                    const losses = bets.length - wins;
                    const pnl = bets.reduce((s, b) => s + (b.pnl ?? 0), 0);
                    const wagered = bets.reduce((s, b) => s + (b.stake ?? 0), 0);
                    const roi = wagered > 0 ? (pnl / wagered) * 100 : 0;
                    const avgEdge = bets.length > 0 ? bets.reduce((s, b) => s + b.bet_edge, 0) / bets.length * 100 : 0;
                    return { label, count: bets.length, wins, losses, pnl, roi, avgEdge, wagered };
                  }).filter((b) => b.count > 0);

                  return (
                    <div className="mt-4 rounded-lg border border-[var(--card-border)] bg-[var(--card)] p-4">
                      <h3 className="mb-3 text-sm font-semibold uppercase tracking-wider text-[var(--text-muted)]">
                        Performance by Edge Level
                      </h3>
                      <div className="overflow-x-auto">
                        <table className="w-full text-xs">
                          <thead>
                            <tr className="text-[var(--text-muted)] uppercase tracking-wider border-b border-[var(--card-border)]">
                              <th className="py-2 text-left font-medium">Edge</th>
                              <th className="py-2 text-right font-medium">Bets</th>
                              <th className="py-2 text-right font-medium">Record</th>
                              <th className="py-2 text-right font-medium">Win%</th>
                              <th className="py-2 text-right font-medium">P&L</th>
                              <th className="py-2 text-right font-medium">ROI</th>
                              <th className="py-2 text-right font-medium">Avg Edge</th>
                            </tr>
                          </thead>
                          <tbody>
                            {bucketData.map((b) => (
                              <tr key={b.label} className="border-t border-[var(--card-border)]/50">
                                <td className="py-2 font-medium">{b.label}</td>
                                <td className="py-2 text-right font-mono">{b.count}</td>
                                <td className="py-2 text-right font-mono">{b.wins}W-{b.losses}L</td>
                                <td className="py-2 text-right font-mono">
                                  {((b.wins / b.count) * 100).toFixed(0)}%
                                </td>
                                <td className={`py-2 text-right font-mono font-bold ${b.pnl >= 0 ? "text-[var(--green)]" : "text-[var(--red)]"}`}>
                                  {b.pnl >= 0 ? "+" : ""}${b.pnl.toFixed(0)}
                                </td>
                                <td className={`py-2 text-right font-mono ${b.roi >= 0 ? "text-[var(--green)]" : "text-[var(--red)]"}`}>
                                  {b.roi >= 0 ? "+" : ""}{b.roi.toFixed(1)}%
                                </td>
                                <td className="py-2 text-right font-mono text-[var(--green)]">
                                  +{b.avgEdge.toFixed(1)}%
                                </td>
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </div>
                    </div>
                  );
                })()}

                {/* YRFI vs NRFI Performance */}
                {(() => {
                  const allBets: Bet[] = [];
                  for (const d of dates) {
                    for (const b of results.daily[d].bets) {
                      if (b.result === "W" || b.result === "L") allBets.push(b);
                    }
                  }
                  if (allBets.length === 0) return null;

                  const sideData = (["YRFI", "NRFI"] as const).map((side) => {
                    const bets = allBets.filter((b) => b.bet_side === side);
                    const wins = bets.filter((b) => b.result === "W").length;
                    const losses = bets.length - wins;
                    const pnl = bets.reduce((s, b) => s + (b.pnl ?? 0), 0);
                    const wagered = bets.reduce((s, b) => s + (b.stake ?? 0), 0);
                    const roi = wagered > 0 ? (pnl / wagered) * 100 : 0;
                    const avgEdge = bets.length > 0 ? bets.reduce((s, b) => s + b.bet_edge, 0) / bets.length * 100 : 0;
                    return { label: side, count: bets.length, wins, losses, pnl, roi, avgEdge };
                  }).filter((b) => b.count > 0);

                  return (
                    <div className="mt-4 rounded-lg border border-[var(--card-border)] bg-[var(--card)] p-4">
                      <h3 className="mb-3 text-sm font-semibold uppercase tracking-wider text-[var(--text-muted)]">
                        YRFI vs NRFI
                      </h3>
                      <div className="overflow-x-auto">
                        <table className="w-full text-xs">
                          <thead>
                            <tr className="text-[var(--text-muted)] uppercase tracking-wider border-b border-[var(--card-border)]">
                              <th className="py-2 text-left font-medium">Side</th>
                              <th className="py-2 text-right font-medium">Bets</th>
                              <th className="py-2 text-right font-medium">Record</th>
                              <th className="py-2 text-right font-medium">Win%</th>
                              <th className="py-2 text-right font-medium">P&L</th>
                              <th className="py-2 text-right font-medium">ROI</th>
                              <th className="py-2 text-right font-medium">Avg Edge</th>
                            </tr>
                          </thead>
                          <tbody>
                            {sideData.map((b) => (
                              <tr key={b.label} className="border-t border-[var(--card-border)]/50">
                                <td className="py-2">
                                  <span className={`rounded px-1.5 py-0.5 text-[10px] font-bold ${b.label === "YRFI" ? "bg-red-900/50 text-red-300" : "bg-blue-900/50 text-blue-300"}`}>
                                    {b.label}
                                  </span>
                                </td>
                                <td className="py-2 text-right font-mono">{b.count}</td>
                                <td className="py-2 text-right font-mono">{b.wins}W-{b.losses}L</td>
                                <td className="py-2 text-right font-mono">
                                  {((b.wins / b.count) * 100).toFixed(0)}%
                                </td>
                                <td className={`py-2 text-right font-mono font-bold ${b.pnl >= 0 ? "text-[var(--green)]" : "text-[var(--red)]"}`}>
                                  {b.pnl >= 0 ? "+" : ""}${b.pnl.toFixed(0)}
                                </td>
                                <td className={`py-2 text-right font-mono ${b.roi >= 0 ? "text-[var(--green)]" : "text-[var(--red)]"}`}>
                                  {b.roi >= 0 ? "+" : ""}{b.roi.toFixed(1)}%
                                </td>
                                <td className="py-2 text-right font-mono text-[var(--green)]">
                                  +{b.avgEdge.toFixed(1)}%
                                </td>
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </div>
                    </div>
                  );
                })()}

                {/* CLV Summary */}
                {(() => {
                  const allBets: Bet[] = [];
                  for (const d of dates) {
                    for (const b of results.daily[d].bets) {
                      if ((b.result === "W" || b.result === "L") && b.clv !== undefined) allBets.push(b);
                    }
                  }
                  if (allBets.length === 0) return null;

                  const avgClv = allBets.reduce((s, b) => s + (b.clv ?? 0), 0) / allBets.length * 100;
                  const yrfiBets = allBets.filter((b) => b.bet_side === "YRFI");
                  const nrfiBets = allBets.filter((b) => b.bet_side === "NRFI");
                  const yrfiClv = yrfiBets.length > 0 ? yrfiBets.reduce((s, b) => s + (b.clv ?? 0), 0) / yrfiBets.length * 100 : 0;
                  const nrfiClv = nrfiBets.length > 0 ? nrfiBets.reduce((s, b) => s + (b.clv ?? 0), 0) / nrfiBets.length * 100 : 0;
                  const posClv = allBets.filter((b) => (b.clv ?? 0) > 0).length;

                  return (
                    <div className="mt-4 rounded-lg border border-[var(--card-border)] bg-[var(--card)] p-4">
                      <h3 className="mb-3 text-sm font-semibold uppercase tracking-wider text-[var(--text-muted)]">
                        Closing Line Value (CLV)
                      </h3>
                      <div className="mb-3 grid grid-cols-2 gap-2 sm:grid-cols-4">
                        <div>
                          <div className="text-xs text-[var(--text-muted)]">Avg CLV</div>
                          <div className={`text-lg font-bold font-mono ${avgClv > 0 ? "text-[var(--green)]" : "text-[var(--red)]"}`}>
                            {avgClv >= 0 ? "+" : ""}{avgClv.toFixed(2)}%
                          </div>
                        </div>
                        <div>
                          <div className="text-xs text-[var(--text-muted)]">Beat Close</div>
                          <div className="text-lg font-bold font-mono">
                            {posClv}/{allBets.length}
                          </div>
                        </div>
                        <div>
                          <div className="text-xs text-[var(--text-muted)]">YRFI CLV</div>
                          <div className={`text-lg font-bold font-mono ${yrfiClv > 0 ? "text-[var(--green)]" : yrfiBets.length > 0 ? "text-[var(--red)]" : ""}`}>
                            {yrfiBets.length > 0 ? `${yrfiClv >= 0 ? "+" : ""}${yrfiClv.toFixed(2)}%` : "—"}
                          </div>
                        </div>
                        <div>
                          <div className="text-xs text-[var(--text-muted)]">NRFI CLV</div>
                          <div className={`text-lg font-bold font-mono ${nrfiClv > 0 ? "text-[var(--green)]" : nrfiBets.length > 0 ? "text-[var(--red)]" : ""}`}>
                            {nrfiBets.length > 0 ? `${nrfiClv >= 0 ? "+" : ""}${nrfiClv.toFixed(2)}%` : "—"}
                          </div>
                        </div>
                      </div>
                      <div className="text-xs text-[var(--text-muted)]">
                        Positive CLV = entry odds beat the closing line. Based on {allBets.length} bet{allBets.length !== 1 ? "s" : ""} with closing data.
                      </div>
                    </div>
                  );
                })()}

                {/* Rolling ROI */}
                {(() => {
                  const allBets: Bet[] = [];
                  for (const d of dates) {
                    for (const b of results.daily[d].bets) {
                      if (b.result === "W" || b.result === "L") allBets.push(b);
                    }
                  }
                  if (allBets.length < 10) return null;

                  const windowSize = Math.min(30, allBets.length);
                  const rollingPoints: { idx: number; roi: number }[] = [];
                  for (let i = windowSize - 1; i < allBets.length; i++) {
                    const window = allBets.slice(i - windowSize + 1, i + 1);
                    const pnl = window.reduce((s, b) => s + (b.pnl ?? 0), 0);
                    const wag = window.reduce((s, b) => s + (b.stake ?? 0), 0);
                    rollingPoints.push({ idx: i, roi: wag > 0 ? (pnl / wag) * 100 : 0 });
                  }

                  if (rollingPoints.length < 2) return null;

                  const minRoi = Math.min(...rollingPoints.map((p) => p.roi));
                  const maxRoi = Math.max(...rollingPoints.map((p) => p.roi));
                  const range = maxRoi - minRoi || 1;
                  const w = 600;
                  const h = 120;
                  const pad = { top: 10, bottom: 20, left: 0, right: 0 };
                  const plotW = w - pad.left - pad.right;
                  const plotH = h - pad.top - pad.bottom;

                  const points = rollingPoints.map((p, i) => {
                    const x = pad.left + (i / (rollingPoints.length - 1)) * plotW;
                    const y = pad.top + plotH - ((p.roi - minRoi) / range) * plotH;
                    return `${x},${y}`;
                  });

                  const zeroY = pad.top + plotH - ((0 - minRoi) / range) * plotH;

                  return (
                    <div className="mt-4 rounded-lg border border-[var(--card-border)] bg-[var(--card)] p-4">
                      <h3 className="mb-3 text-sm font-semibold uppercase tracking-wider text-[var(--text-muted)]">
                        Rolling {windowSize}-Bet ROI
                      </h3>
                      <svg viewBox={`0 0 ${w} ${h}`} className="w-full" preserveAspectRatio="none">
                        {/* Zero line */}
                        {minRoi < 0 && maxRoi > 0 && (
                          <line x1={pad.left} x2={w - pad.right} y1={zeroY} y2={zeroY}
                            stroke="var(--text-muted)" strokeWidth="0.5" strokeDasharray="4,4" />
                        )}
                        <polyline points={points.join(" ")} fill="none"
                          stroke={rollingPoints[rollingPoints.length - 1].roi >= 0 ? "var(--green)" : "var(--red)"}
                          strokeWidth="2" />
                      </svg>
                      <div className="flex justify-between text-xs text-[var(--text-muted)]">
                        <span>Bet {windowSize}</span>
                        <span className={`font-mono ${rollingPoints[rollingPoints.length - 1].roi >= 0 ? "text-[var(--green)]" : "text-[var(--red)]"}`}>
                          Current: {rollingPoints[rollingPoints.length - 1].roi >= 0 ? "+" : ""}{rollingPoints[rollingPoints.length - 1].roi.toFixed(1)}%
                        </span>
                        <span>Bet {allBets.length}</span>
                      </div>
                    </div>
                  );
                })()}

                {/* Calibration Table */}
                {(() => {
                  const allBets: Bet[] = [];
                  for (const d of dates) {
                    for (const b of results.daily[d].bets) {
                      if (b.result === "W" || b.result === "L") allBets.push(b);
                    }
                  }
                  if (allBets.length === 0) return null;

                  const calBuckets = [
                    { label: "50-55%", min: 0.50, max: 0.55 },
                    { label: "55-60%", min: 0.55, max: 0.60 },
                    { label: "60-65%", min: 0.60, max: 0.65 },
                    { label: "65-70%", min: 0.65, max: 0.70 },
                    { label: "70%+", min: 0.70, max: 1.0 },
                  ];

                  const bucketData = calBuckets.map(({ label, min, max }) => {
                    const bets = allBets.filter((b) => {
                      const modelProb = b.bet_side === "YRFI" ? b.p_cal : 1 - b.p_cal;
                      return modelProb >= min && modelProb < max;
                    });
                    const wins = bets.filter((b) => b.result === "W").length;
                    const predicted = bets.length > 0
                      ? bets.reduce((s, b) => s + (b.bet_side === "YRFI" ? b.p_cal : 1 - b.p_cal), 0) / bets.length * 100
                      : 0;
                    const actual = bets.length > 0 ? (wins / bets.length) * 100 : 0;
                    const diff = actual - predicted;
                    return { label, count: bets.length, predicted, actual, diff };
                  }).filter((b) => b.count > 0);

                  return (
                    <div className="mt-4 rounded-lg border border-[var(--card-border)] bg-[var(--card)] p-4">
                      <h3 className="mb-3 text-sm font-semibold uppercase tracking-wider text-[var(--text-muted)]">
                        Model Calibration
                      </h3>
                      <div className="overflow-x-auto">
                        <table className="w-full text-xs">
                          <thead>
                            <tr className="text-[var(--text-muted)] uppercase tracking-wider border-b border-[var(--card-border)]">
                              <th className="py-2 text-left font-medium">Model Prob</th>
                              <th className="py-2 text-right font-medium">Bets</th>
                              <th className="py-2 text-right font-medium">Predicted</th>
                              <th className="py-2 text-right font-medium">Actual</th>
                              <th className="py-2 text-right font-medium">Diff</th>
                            </tr>
                          </thead>
                          <tbody>
                            {bucketData.map((b) => (
                                <tr key={b.label} className="border-t border-[var(--card-border)]/50">
                                  <td className="py-2 font-medium">{b.label}</td>
                                  <td className="py-2 text-right font-mono">{b.count}</td>
                                  <td className="py-2 text-right font-mono">{b.predicted.toFixed(1)}%</td>
                                  <td className="py-2 text-right font-mono">{b.actual.toFixed(1)}%</td>
                                  <td className="py-2 text-right font-mono font-bold" style={{ color: b.diff >= 0 ? "var(--green)" : "var(--red)" }}>
                                    {b.diff >= 0 ? "+" : ""}{b.diff.toFixed(1)}%
                                  </td>
                                </tr>
                            ))}
                          </tbody>
                        </table>
                      </div>
                    </div>
                  );
                })()}

                {/* Stake Efficiency */}
                {(() => {
                  const allBets: Bet[] = [];
                  for (const d of dates) {
                    for (const b of results.daily[d].bets) {
                      if (b.result === "W" || b.result === "L") allBets.push(b);
                    }
                  }
                  if (allBets.length === 0) return null;

                  const kellyBuckets = [
                    { label: "<2%", min: 0, max: 0.02 },
                    { label: "2-3%", min: 0.02, max: 0.03 },
                    { label: "3-5%", min: 0.03, max: 0.05 },
                    { label: "5%+", min: 0.05, max: Infinity },
                  ];

                  const bucketData = kellyBuckets.map(({ label, min, max }) => {
                    const bets = allBets.filter((b) => b.bet_kelly >= min && b.bet_kelly < max);
                    const wins = bets.filter((b) => b.result === "W").length;
                    const losses = bets.length - wins;
                    const pnl = bets.reduce((s, b) => s + (b.pnl ?? 0), 0);
                    const wagered = bets.reduce((s, b) => s + (b.stake ?? 0), 0);
                    const roi = wagered > 0 ? (pnl / wagered) * 100 : 0;
                    return { label, count: bets.length, wins, losses, pnl, roi };
                  }).filter((b) => b.count > 0);

                  return (
                    <div className="mt-4 rounded-lg border border-[var(--card-border)] bg-[var(--card)] p-4">
                      <h3 className="mb-3 text-sm font-semibold uppercase tracking-wider text-[var(--text-muted)]">
                        Stake Efficiency (by Kelly %)
                      </h3>
                      <div className="overflow-x-auto">
                        <table className="w-full text-xs">
                          <thead>
                            <tr className="text-[var(--text-muted)] uppercase tracking-wider border-b border-[var(--card-border)]">
                              <th className="py-2 text-left font-medium">Kelly</th>
                              <th className="py-2 text-right font-medium">Bets</th>
                              <th className="py-2 text-right font-medium">Record</th>
                              <th className="py-2 text-right font-medium">Win%</th>
                              <th className="py-2 text-right font-medium">P&L</th>
                              <th className="py-2 text-right font-medium">ROI</th>
                            </tr>
                          </thead>
                          <tbody>
                            {bucketData.map((b) => (
                              <tr key={b.label} className="border-t border-[var(--card-border)]/50">
                                <td className="py-2 font-medium">{b.label}</td>
                                <td className="py-2 text-right font-mono">{b.count}</td>
                                <td className="py-2 text-right font-mono">{b.wins}W-{b.losses}L</td>
                                <td className="py-2 text-right font-mono">
                                  {((b.wins / b.count) * 100).toFixed(0)}%
                                </td>
                                <td className={`py-2 text-right font-mono font-bold ${b.pnl >= 0 ? "text-[var(--green)]" : "text-[var(--red)]"}`}>
                                  {b.pnl >= 0 ? "+" : ""}${b.pnl.toFixed(0)}
                                </td>
                                <td className={`py-2 text-right font-mono ${b.roi >= 0 ? "text-[var(--green)]" : "text-[var(--red)]"}`}>
                                  {b.roi >= 0 ? "+" : ""}{b.roi.toFixed(1)}%
                                </td>
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </div>
                    </div>
                  );
                })()}

                {/* Calibration Drift */}
                <div className="mt-4">
                  <CalibrationDriftChart daily={results.daily} />
                </div>
              </>
            );
          })()}
        </div>
      )}

      {/* Footer */}
      <div className="mt-12 border-t border-[var(--card-border)] pt-4 text-center text-xs text-[var(--text-muted)]">
        {(() => {
          const ts =
            activeTab === "results" || activeTab === "risk"
              ? results?.updated_at
              : activeTab === "predictions"
                ? predictions?.generated_at
                : predsGeneratedAt; // covers "games" and "breakeven"
          if (!ts) return null;
          const d = new Date(ts);
          return (
            <>
              Last updated{" "}
              {d.toLocaleDateString("en-US", {
                month: "short",
                day: "numeric",
                year: "numeric",
              })}{" "}
              {d.toLocaleTimeString("en-US", {
                hour: "numeric",
                minute: "2-digit",
                timeZoneName: "short",
              })}
            </>
          );
        })()}
      </div>
    </main>
  );
}
