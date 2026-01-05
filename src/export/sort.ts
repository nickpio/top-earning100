import type { KeywordStat } from "./exporters";

export function sortKeywordStats(rows: KeywordStat[]): KeywordStat[] {
  return [...rows].sort((a, b) => {
    if (b.count !== a.count) return b.count - a.count;
    if (b.avgPlayers !== a.avgPlayers) return b.avgPlayers - a.avgPlayers;
    return a.keyword.localeCompare(b.keyword);
  });
}