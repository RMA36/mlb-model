"use client";

import { useEffect, useState, useCallback } from "react";

const REPO = "RMA36/mlb-model";
const BRANCH = "master";
const RAW = `https://raw.githubusercontent.com/${REPO}/${BRANCH}`;
const MLB_API = "https://statsapi.mlb.com/api/v1";

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

interface Bet {
  game_pk: number;
  date: string;
  away_team: string;
  home_team: string;
  away_starter: string;
  home_starter: string;
  umpire: string;
  p_cal: number;
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
      const isLive = detailedState === "In Progress" || detailedState === "Manager Challenge" || detailedState === "Delayed";
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

function BetCard({ bet }: { bet: Bet }) {
  const isYrfi = bet.bet_side === "YRFI";
  const modelPct = isYrfi ? bet.p_cal * 100 : (1 - bet.p_cal) * 100;
  const mktPct = isYrfi ? bet.mkt_y_fair * 100 : bet.mkt_n_fair * 100;
  const hasResult = bet.result === "W" || bet.result === "L";

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
          ) : (
            <span className="rounded bg-yellow-900/40 px-2 py-0.5 text-xs text-yellow-300">
              PENDING
            </span>
          )}
        </div>
      </div>

      <div className="mt-3 grid grid-cols-4 gap-2 text-xs">
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
          <div className="font-mono font-medium">${bet.stake.toFixed(0)}</div>
        </div>
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


function GameCard({ game }: { game: TodaysGame }) {
  const hasAwayLineup = game.awayLineup.length > 0;
  const hasHomeLineup = game.homeLineup.length > 0;
  const hasLineups = hasAwayLineup && hasHomeLineup;

  const isFinal = game.gameState === "Final" || game.gameState === "Game Over" || game.gameState === "Completed Early";
  const isLive = game.gameState === "In Progress" || game.gameState === "Manager Challenge";
  const isPreGame = !isFinal && !isLive;

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

// ── Page ─────────────────────────────────────────────────────────────

function todayET(): string {
  return new Date().toLocaleDateString("en-CA", {
    timeZone: "America/New_York",
  });
}

type TabId = "games" | "predictions" | "results";

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

  // Load results when results tab is active
  useEffect(() => {
    if (activeTab !== "results") return;
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
        };
      });

      // Try to enrich with lookups (pitcher stats)
      const lookups = await fetchJSON<Lookups>("models/v4/lookups.json");
      const final = enrichWithLookups(enriched, lookups);

      setGames(final);
      setLastRefresh(new Date());
    } catch {
      setGamesError("Failed to load games. Try refreshing.");
    } finally {
      setGamesLoading(false);
    }
  }, []);

  useEffect(() => {
    if (activeTab !== "games") return;
    loadGames();

    // Auto-refresh every 5 minutes
    const interval = setInterval(loadGames, 5 * 60 * 1000);
    return () => clearInterval(interval);
  }, [activeTab, loadGames]);

  // Merge results into predictions if available
  const betsWithResults: Bet[] = (() => {
    if (!predictions) return [];
    const bets = predictions.bets || [];
    if (!results?.daily?.[selectedDate]) return bets;

    const scored = results.daily[selectedDate].bets;
    const scoredMap = new Map(scored.map((b) => [b.game_pk, b]));

    return bets.map((b) => {
      const s = scoredMap.get(b.game_pk);
      if (s && (s.result === "W" || s.result === "L")) {
        return { ...b, ...s };
      }
      return b;
    });
  })();

  const cum = results?.cumulative;
  const availableDates = results
    ? Object.keys(results.daily).sort().reverse()
    : [];

  const tabClass = (tab: TabId) =>
    `px-4 py-2 text-sm font-medium rounded-t-lg transition-colors ${
      activeTab === tab
        ? "bg-[var(--card)] text-[var(--text)] border border-b-0 border-[var(--card-border)]"
        : "text-[var(--text-muted)] hover:text-[var(--text)]"
    }`;

  return (
    <main className="mx-auto max-w-4xl px-4 py-8">
      {/* Header */}
      <div className="mb-6">
        <h1 className="text-3xl font-bold">MLB Model</h1>
      </div>

      {/* Tab Bar */}
      <div className="mb-6 flex gap-1 border-b border-[var(--card-border)]">
        <button className={tabClass("games")} onClick={() => setActiveTab("games")}>
          Today&apos;s Games
        </button>
        <button
          className={tabClass("predictions")}
          onClick={() => setActiveTab("predictions")}
        >
          Predictions
        </button>
        <button
          className={tabClass("results")}
          onClick={() => setActiveTab("results")}
        >
          Results
        </button>
      </div>

      {/* ── TODAY'S GAMES TAB ──────────────────────────────────────── */}
      {activeTab === "games" && (
        <div>
          {/* Date heading + refresh */}
          <div className="mb-4 flex items-center justify-between">
            <h2 className="text-lg font-semibold">
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
              <div className="grid gap-4 md:grid-cols-2">
                {games.map((g) => (
                  <GameCard key={g.gamePk} game={g} />
                ))}
              </div>
            </div>
          )}
        </div>
      )}

      {/* ── PREDICTIONS TAB ────────────────────────────────────────── */}
      {activeTab === "predictions" && (
        <div>
          {/* Date Picker */}
          <div className="mb-6 flex items-center gap-3">
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
          {!loading && betsWithResults.length > 0 && (
            <div>
              <div className="mb-3 flex items-center justify-between">
                <h2 className="text-lg font-semibold">
                  {betsWithResults.length} Pick
                  {betsWithResults.length !== 1 ? "s" : ""} — {selectedDate}
                </h2>
                {predictions && (
                  <span className="text-xs text-[var(--text-muted)]">
                    {predictions.summary.total_games} games analyzed
                  </span>
                )}
              </div>
              <div className="grid gap-3">
                {betsWithResults.map((b) => (
                  <BetCard key={b.game_pk} bet={b} />
                ))}
              </div>
            </div>
          )}
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
                    <div className="mb-3 grid grid-cols-2 gap-3 sm:grid-cols-4">
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
                          <div
                            key={date}
                            className="rounded-lg border border-[var(--card-border)] bg-[var(--card)] px-4 py-3"
                          >
                            <div className="flex items-center justify-between">
                              <div>
                                <div className="text-sm font-medium">{formatDateHeading(date)}</div>
                                <div className="mt-0.5 text-xs text-[var(--text-muted)]">
                                  {day.bets.length} bet{day.bets.length !== 1 ? "s" : ""}
                                  {day.all_scored ? "" : " · scoring in progress"}
                                </div>
                              </div>
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
                            </div>
                          </div>
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

      {/* Footer */}
      <div className="mt-12 border-t border-[var(--card-border)] pt-4 text-center text-xs text-[var(--text-muted)]">
        {results?.updated_at && (
          <>
            Last updated{" "}
            {new Date(results.updated_at).toLocaleDateString("en-US", {
              timeZone: "America/New_York",
              month: "short",
              day: "numeric",
              year: "numeric",
            })}
          </>
        )}
      </div>
    </main>
  );
}
