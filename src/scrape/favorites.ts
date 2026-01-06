import { fetchJsonWithRetry } from "./http";
import { RateLimiter } from "./limiter";

type FavoritesCountResponse = { favoritesCount: number };

export async function fetchFavoritesCountBatch(
  universeIds: number[],
  opts?: { concurrency?: number; minIntervalMs?: number }
): Promise<Map<number, number>> {
  const concurrency = Math.min(Math.max(opts?.concurrency ?? 4, 1), 10);
  const limiter = new RateLimiter(opts?.minIntervalMs ?? 250);

  const uniq = Array.from(new Set(universeIds)).filter((n) => Number.isFinite(n) && n > 0);
  const out = new Map<number, number>();
  let idx = 0;

  async function worker() {
    while (idx < uniq.length) {
      const id = uniq[idx++];

      await limiter.wait();

      const url = `https://games.roblox.com/v1/games/${id}/favorites/count`;
      try {
        const json = await fetchJsonWithRetry<FavoritesCountResponse>(
          url,
          {
            headers: {
              accept: "application/json",
              "accept-language": "en-US,en;q=0.9",
              "user-agent":
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:146.0) Gecko/20100101 Firefox/146.0",
            },
          },
          {
            maxRetries: 7,
            baseDelayMs: 800,
            maxDelayMs: 45_000,
            retryOnStatuses: [429, 500, 502, 503, 504],
          }
        );

        out.set(id, Number(json.favoritesCount ?? 0));
      } catch {
        // don’t kill run; mark unknown as 0 or leave missing
        out.set(id, 0);
      }
    }
  }

  await Promise.all(Array.from({ length: concurrency }, () => worker()));
  return out;
}