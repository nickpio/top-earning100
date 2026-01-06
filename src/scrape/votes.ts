import { fetchJsonWithRetry } from "./http";
import { RateLimiter } from "./limiter";

export type VoteInfo = {
  upVotes: number;
  downVotes: number;
};

type VotesResponse = {
  data: Array<{
    id: number;        // universeId
    upVotes: number;
    downVotes: number;
  }>;
};

function chunk<T>(arr: T[], size: number): T[][] {
  const out: T[][] = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}

export async function fetchVotesBatch(
  universeIds: number[],
  opts?: { batchSize?: number; concurrency?: number; minIntervalMs?: number }
): Promise<Map<number, VoteInfo>> {
  const batchSize = Math.min(Math.max(opts?.batchSize ?? 50, 1), 100);
  const concurrency = Math.min(Math.max(opts?.concurrency ?? 2, 1), 5);
  const limiter = new RateLimiter(opts?.minIntervalMs ?? 250);

  const uniq = Array.from(new Set(universeIds)).filter((n) => Number.isFinite(n) && n > 0);
  const batches = chunk(uniq, batchSize);

  const out = new Map<number, VoteInfo>();
  let idx = 0;

  async function worker() {
    while (idx < batches.length) {
      const my = idx++;
      const ids = batches[my];

      await limiter.wait();

      const url = `https://games.roblox.com/v1/games/votes?universeIds=${ids.join(",")}`;
      const json = await fetchJsonWithRetry<VotesResponse>(
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
          baseDelayMs: 600,
          maxDelayMs: 30_000,
          retryOnStatuses: [429, 500, 502, 503, 504],
        }
      );

      for (const row of json.data ?? []) {
        out.set(row.id, {
          upVotes: Number(row.upVotes ?? 0),
          downVotes: Number(row.downVotes ?? 0),
        });
      }
    }
  }

  await Promise.all(Array.from({ length: concurrency }, () => worker()));
  return out;
}