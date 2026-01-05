import { normalizeText, ALLOW_SHORT } from "./normalize";
import { buildNgrams } from "./ngrams";

type GameRow = {
  name?: unknown;
  playing?: unknown;
};

export type KeywordStat = {
  keyword: string;     // unigram or n-gram phrase
  count: number;       // number of titles it appears in
  avgPlayers: number;  // average playing across those titles
};

export function analyzeKeywords(rows: GameRow[], opts?: {
  minN?: 1 | 2 | 3;
  maxN?: 1 | 2 | 3;
  // optionally: drop very rare n-grams
  minCount?: number;
}): KeywordStat[] {
  const minN = opts?.minN ?? 1;
  const maxN = opts?.maxN ?? 3;
  const minCount = opts?.minCount ?? 1;

  const stats = new Map<string, { count: number; totalPlayers: number }>();

  for (const row of rows) {
    const playing =
      typeof row?.playing === "number" && Number.isFinite(row.playing) ? row.playing : 0;

    const tokens = normalizeText(row?.name);

    // Build 1-3 grams from title tokens
    const grams = buildNgrams(tokens, {
      minN,
      maxN,
      joiner: " ",
      minTokenLen: 3,
      allowShortTokens: ALLOW_SHORT,
    });

    // Count each gram once per title
    const unique = new Set(grams);

    for (const kw of unique) {
      const entry = stats.get(kw) ?? { count: 0, totalPlayers: 0 };
      entry.count += 1;
      entry.totalPlayers += playing;
      stats.set(kw, entry);
    }
  }

  const out: KeywordStat[] = Array.from(stats.entries())
    .map(([keyword, v]) => ({
      keyword,
      count: v.count,
      avgPlayers: v.count ? Math.round(v.totalPlayers / v.count) : 0,
    }))
    .filter((r) => r.count >= minCount);

  return out;
}