"use client";

import { useEffect, useState } from "react";

const REPO = "RMA36/mlb-model";
const BRANCH = "master";
const RAW = `https://raw.githubusercontent.com/${REPO}/${BRANCH}`;

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
  // Scoring fields (from results)
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

function formatOdds(odds: number): string {
  return odds > 0 ? `+${odds}` : `${odds}`;
}

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
  const mktPct = isYrfi
    ? bet.mkt_y_fair * 100
    : bet.mkt_n_fair * 100;
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

  const toX = (i: number) => PAD + (i / Math.max(points.length - 1, 1)) * plotW;
  const toY = (v: number) => PAD + plotH - ((v - minPnl) / range) * plotH;

  const pathD = points
    .map((p, i) => `${i === 0 ? "M" : "L"} ${toX(i).toFixed(1)} ${toY(p.pnl).toFixed(1)}`)
    .join(" ");

  const zeroY = toY(0);
  const lastPnl = points[points.length - 1].pnl;

  return (
    <div className="rounded-lg border border-[var(--card-border)] bg-[var(--card)] p-4">
      <h3 className="mb-2 text-sm font-medium text-[var(--text-muted)]">
        Cumulative P&L
      </h3>
      <svg viewBox={`0 0 ${W} ${H}`} className="w-full" style={{ maxHeight: 250 }}>
        {/* Zero line */}
        <line
          x1={PAD}
          y1={zeroY}
          x2={W - PAD}
          y2={zeroY}
          stroke="#333"
          strokeDasharray="4"
        />
        <text x={PAD - 4} y={zeroY + 4} textAnchor="end" fill="#666" fontSize="10">
          $0
        </text>

        {/* P&L line */}
        <path
          d={pathD}
          fill="none"
          stroke={lastPnl >= 0 ? "var(--green)" : "var(--red)"}
          strokeWidth="2.5"
        />

        {/* End label */}
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

        {/* Top/bottom labels */}
        <text x={PAD - 4} y={PAD + 4} textAnchor="end" fill="#666" fontSize="10">
          ${maxPnl.toFixed(0)}
        </text>
        <text x={PAD - 4} y={H - PAD + 4} textAnchor="end" fill="#666" fontSize="10">
          ${minPnl.toFixed(0)}
        </text>
      </svg>
    </div>
  );
}

async function fetchJSON<T>(path: string): Promise<T | null> {
  try {
    const res = await fetch(`${RAW}/${path}`, { cache: "no-store" });
    if (!res.ok) return null;
    return await res.json();
  } catch {
    return null;
  }
}

function todayET(): string {
  return new Date().toLocaleDateString("en-CA", {
    timeZone: "America/New_York",
  });
}

export default function Home() {
  const [predictions, setPredictions] = useState<DayPredictions | null>(null);
  const [results, setResults] = useState<Results | null>(null);
  const [loading, setLoading] = useState(true);
  const [selectedDate, setSelectedDate] = useState(todayET());

  useEffect(() => {
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
  }, [selectedDate]);

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

  return (
    <main className="mx-auto max-w-4xl px-4 py-8">
      {/* Header */}
      <div className="mb-8">
        <h1 className="text-3xl font-bold">MLB YRFI/NRFI</h1>
        <p className="text-sm text-[var(--text-muted)]">
          v4 LightGBM Two-Model · Market-Anchored Calibration · Quarter-Kelly
        </p>
      </div>

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
              color={cum.profit >= 0 ? "text-[var(--green)]" : "text-[var(--red)]"}
            />
            <StatCard
              label="ROI"
              value={`${cum.roi_pct >= 0 ? "+" : ""}${cum.roi_pct.toFixed(1)}%`}
              sub={`${cum.total_bets} total bets`}
              color={cum.roi_pct >= 0 ? "text-[var(--green)]" : "text-[var(--red)]"}
            />
            <StatCard
              label="Max Drawdown"
              value={`$${cum.max_drawdown.toFixed(0)}`}
              sub={`Peak: $+${cum.peak_profit.toFixed(0)}`}
            />
          </div>
          {results && <PnlChart daily={results.daily} />}
        </div>
      )}

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
          <div className="text-lg font-medium">No predictions for {selectedDate}</div>
          <div className="mt-1 text-sm text-[var(--text-muted)]">
            Predictions appear once lineups are posted (typically 1-3 hours before first pitch)
          </div>
        </div>
      )}

      {/* Predictions with no bets */}
      {!loading && predictions && betsWithResults.length === 0 && (
        <div className="rounded-lg border border-[var(--card-border)] bg-[var(--card)] p-8 text-center">
          <div className="text-lg font-medium">
            {predictions.summary.total_games} games analyzed — no qualifying bets
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

      {/* Footer */}
      <div className="mt-12 border-t border-[var(--card-border)] pt-4 text-center text-xs text-[var(--text-muted)]">
        YRFI/NRFI v4 · LGB Two-Model + Market-Anchored Calibration (k=0.90) ·
        Quarter-Kelly (0.25×)
        {results?.updated_at && (
          <>
            {" · "}Results updated{" "}
            {new Date(results.updated_at).toLocaleDateString("en-US", {
              timeZone: "America/New_York",
            })}
          </>
        )}
      </div>
    </main>
  );
}
