import * as fs from "node:fs";
import * as path from "node:path";

type CacheEntry = { fetchedAt: string; favoritesCount: number };
type CacheFile = { version: 1; entries: Record<string, CacheEntry> };

const DEFAULT_PATH = path.resolve(process.cwd(), "data", "favoritesCache.json");

function ensureDir(p: string) {
  fs.mkdirSync(p, { recursive: true });
}

export function loadFavoritesCache(filePath = DEFAULT_PATH): CacheFile {
  try {
    const raw = fs.readFileSync(filePath, "utf8");
    const parsed = JSON.parse(raw) as CacheFile;
    if (!parsed || parsed.version !== 1 || typeof parsed.entries !== "object") {
      return { version: 1, entries: {} };
    }
    return parsed;
  } catch {
    return { version: 1, entries: {} };
  }
}

export function saveFavoritesCache(cache: CacheFile, filePath = DEFAULT_PATH) {
  ensureDir(path.dirname(filePath));
  fs.writeFileSync(filePath, JSON.stringify(cache, null, 2), "utf8");
}

function isFresh(entry: CacheEntry | undefined, maxAgeDays: number): boolean {
  if (!entry?.fetchedAt) return false;
  const t = Date.parse(entry.fetchedAt);
  if (!Number.isFinite(t)) return false;
  const ageMs = Date.now() - t;
  return ageMs >= 0 && ageMs <= maxAgeDays * 24 * 60 * 60 * 1000;
}

export function getCachedFavorites(cache: CacheFile, universeId: number, maxAgeDays: number): number | null {
  const entry = cache.entries[String(universeId)];
  return isFresh(entry, maxAgeDays) ? entry.favoritesCount : null;
}

export function setCachedFavorites(cache: CacheFile, universeId: number, favoritesCount: number) {
  cache.entries[String(universeId)] = {
    fetchedAt: new Date().toISOString(),
    favoritesCount,
  };
}

export function splitByFreshness(cache: CacheFile, universeIds: number[], maxAgeDays: number) {
  const staleOrMissing: number[] = [];
  for (const id of universeIds) {
    const entry = cache.entries[String(id)];
    if (!isFresh(entry, maxAgeDays)) staleOrMissing.push(id);
  }
  return { staleOrMissing };
}