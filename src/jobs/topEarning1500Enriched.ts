import * as fs from "node:fs";
import * as path from "node:path";
import { fetchTopEarningUniverseIds } from "../scrape/topEarning";
import { fetchGameDetailsByUniverseIds } from "../scrape/gameDetails";
import { fetchGamePassStatsBatch } from "../scrape/gamePassesBatch";
import { fetchPaidAccessFromPlaceDetails } from "../scrape/paidAccess";
import { loadGamePassCache, saveGamePassCache, splitUniverseIdsByCacheFreshness, getCached, setCached } from "../cache/gamePassCache";
import { fetchVotesBatch } from "../scrape/votes";
import { fetchFavoritesCountBatch } from "../scrape/favorites";
import { loadFavoritesCache, saveFavoritesCache, splitByFreshness, getCachedFavorites, setCachedFavorites } from "../cache/favoritesCache";

function getArg(flag: string): string | undefined {
  const i = process.argv.indexOf(flag);
  return i >= 0 ? process.argv[i + 1] : undefined;
}

function todayISO() {
  return new Date().toISOString().slice(0, 10);
}

function ageDays(createdIso?: string): number | null {
  if (!createdIso) return null;
  const t = Date.parse(createdIso);
  if (!Number.isFinite(t)) return null;
  const diffMs = Date.now() - t;
  return diffMs >= 0 ? Math.floor(diffMs / (1000 * 60 * 60 * 24)) : null;
}

type Row = {
  rank: number;
  universeId: number;

  name: string;
  description: string;

  playing: number;
  visits: number;
  likes: number | null;
  dislikes: number | null;
  favorites: number | null;
  developerName: string | null;

  gameAgeDays: number | null;

  paidAccess: boolean | null;
  paidAccessPrice: number | null;

  gamePassCount: number | null;
  avgGamePassPrice: number | null;
};

async function main() {
  const limit = 1500;

  const { items } = await fetchTopEarningUniverseIds({
    limit,
    country: "all",
    device: "computer",
    throttleMs: 150,
  });

  const universeIds = items.map((x) => x.universeId);

  const details = await fetchGameDetailsByUniverseIds(universeIds, {
    batchSize: 50,
    concurrency: 1,     // ✅ start conservative
    minIntervalMs: 350, // ✅ ~3 req/sec max
  });

  // --- votes (daily, batched)
const votesById = await fetchVotesBatch(universeIds, {
  batchSize: 50,
  concurrency: 2,
  minIntervalMs: 250,
});

// --- favorites (weekly cache)
const favoritesCache = loadFavoritesCache();
const FAVORITES_CACHE_DAYS = 7;

const { staleOrMissing: staleOrMissingFavs } = splitByFreshness(favoritesCache, universeIds, FAVORITES_CACHE_DAYS);

if (staleOrMissingFavs.length > 0) {
  const fetchedFavs = await fetchFavoritesCountBatch(staleOrMissingFavs, {
    concurrency: 4,
    minIntervalMs: 250,
  });

  for (const [id, count] of fetchedFavs.entries()) {
    setCachedFavorites(favoritesCache, id, count);
  }
  saveFavoritesCache(favoritesCache);
}
  const byId = new Map(details.map((d) => [d.universeId, d]));

  // Game pass stats (heavy): use cache to minimize API calls
  const cache = loadGamePassCache();

  // 7 days freshness window (weekly refresh)
  const PASS_CACHE_DAYS = 7;

  const { staleOrMissing } = splitUniverseIdsByCacheFreshness(cache, universeIds, PASS_CACHE_DAYS);

  // ✅ Daily: only fetch for missing/stale (usually small once cache is built)
  const passStatsFetched = await fetchGamePassStatsBatch(staleOrMissing, {
    concurrency: 4,
    minIntervalMs: 250,
  });

  // merge fetched stats into cache
  for (const [universeId, stats] of passStatsFetched.entries()) {
    setCached(cache, universeId, stats);
  }

  saveGamePassCache(cache);

  const rows: Row[] = [];
  for (const it of items) {
    const d = byId.get(it.universeId);

    // Paid access best-effort:
    let paidAccess: boolean | null = null;
    let paidAccessPrice: number | null = null;

    if (typeof d?.isPaidAccess === "boolean") {
      paidAccess = d.isPaidAccess;
    }
    if (typeof d?.price === "number" && Number.isFinite(d.price)) {
      paidAccessPrice = d.price;
      paidAccess = d.price > 0;
    }

    // fallback to place details if still unknown and rootPlaceId exists
    if ((paidAccess == null || paidAccessPrice == null) && typeof d?.rootPlaceId === "number") {
      const fallback = await fetchPaidAccessFromPlaceDetails(d.rootPlaceId);
      // only apply if fallback learned something
      if (fallback.paidAccess != null) paidAccess = fallback.paidAccess;
      if (fallback.paidAccessPrice != null) paidAccessPrice = fallback.paidAccessPrice;
    }

    const cached = getCached(cache, it.universeId, PASS_CACHE_DAYS);
    const passStats = cached ?? null;

    const v = votesById.get(it.universeId);
    const favorites = getCachedFavorites(favoritesCache, it.universeId, FAVORITES_CACHE_DAYS);
    rows.push({
      rank: it.rank,
      universeId: it.universeId,

      name: d?.name ?? "",
      description: d?.description ?? "",

      playing: d?.playing ?? 0,
      visits: d?.visits ?? 0,
      likes: v?.upVotes ?? null,
      dislikes: v?.downVotes ?? null,
      favorites: favorites,

      developerName: d?.creatorName ?? null,

      gameAgeDays: ageDays(d?.created),

      paidAccess,
      paidAccessPrice,

      gamePassCount: passStats?.gamePassCount ?? null,
      avgGamePassPrice: passStats?.avgGamePassPrice ?? null,
    });
  }

  const outDir = getArg("--outDir") ?? path.resolve(process.cwd(), "reports");
  fs.mkdirSync(outDir, { recursive: true });

  const outPath = path.join(outDir, `${todayISO()}_top-earning_top${limit}_enriched.json`);
  fs.writeFileSync(outPath, JSON.stringify(rows, null, 2), "utf8");

  console.log(`Wrote ${rows.length} rows -> ${outPath}`);
  console.log(rows.slice(0, 2));
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});