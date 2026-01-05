import { normalizeText } from "./normalize";

type GameRow = {
  name?: unknown;
  playing?: unknown;
};

type KeywordStat = {
  keyword: string;
  count: number;
  avgPlayers: number;
};

export function analyzeKeywords(rows: GameRow[]): KeywordStat[] {
  const stats = new Map<string, { count: number; totalPlayers: number }>();

  for (const row of rows) {
    const name = row?.name;
    const playing = typeof row?.playing === "number" && Number.isFinite(row.playing)
      ? row.playing
      : 0;

    // Count keyword once per title
    const keywords = new Set(normalizeText(name));

    for (const kw of keywords) {
      const entry = stats.get(kw) ?? { count: 0, totalPlayers: 0 };
      entry.count += 1;
      entry.totalPlayers += playing;
      stats.set(kw, entry);
    }
  }

  return Array.from(stats.entries())
    .map(([keyword, v]) => ({
      keyword,
      count: v.count,
      avgPlayers: v.count ? Math.round(v.totalPlayers / v.count) : 0,
    }))
    .sort((a, b) => b.count - a.count);
}