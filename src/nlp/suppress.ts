import type { KeywordStat } from "./keywordStats";

export function suppressDominatedUnigrams(
  stats: KeywordStat[],
  opts?: { threshold?: number; minPhraseCount?: number }
): KeywordStat[] {
  const threshold = opts?.threshold ?? 0.8;
  const minPhraseCount = opts?.minPhraseCount ?? 3;

  const unigram = new Map<string, KeywordStat>();
  const phrases: KeywordStat[] = [];

  for (const s of stats) {
    if (s.keyword.includes(" ")) phrases.push(s);
    else unigram.set(s.keyword, s);
  }

  // Track the strongest dominating phrase per unigram
  const bestRatio = new Map<string, number>();

  for (const p of phrases) {
    if (p.count < minPhraseCount) continue;
    const tokens = p.keyword.split(" ").filter(Boolean);

    for (const t of tokens) {
      const u = unigram.get(t);
      if (!u || u.count === 0) continue;

      const ratio = p.count / u.count;
      const prev = bestRatio.get(t) ?? 0;
      if (ratio > prev) bestRatio.set(t, ratio);
    }
  }

  const toDrop = new Set<string>();
  for (const [t, ratio] of bestRatio.entries()) {
    if (ratio >= threshold) toDrop.add(t);
  }

  return stats.filter((s) => !(toDrop.has(s.keyword) && !s.keyword.includes(" ")));
}