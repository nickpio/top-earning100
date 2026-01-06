// src/transform/pruneGameData.ts

export type EnrichedGameRow = {
    universeId: number;
  
    name?: string;
    description?: string;
  
    playing?: number;
    visits?: number;
    favorites: number;
    likes: number;
    dislikes: number;
  
    developerName?: string | null;
  
    gameAgeDays?: number | null;
  
    paidAccess?: boolean | null;
    paidAccessPrice?: number | null;
  
    gamePassCount?: number | null;
    avgGamePassPrice?: number | null;
  };
  
  export type PrunedGameRow = {
    universeId: number;
  
    name: string;
    developer: string | null;
  
    ageDays: number | null;
  
    players: number;
    visits: number;
    favorites: number;
    likes: number;
    dislikes: number;
  
    paidAccess: boolean | null;
    paidAccessPrice: number | null;
  
    gamePassCount: number | null;
    avgGamePassPrice: number | null;
  };
  
  function safeInt(n: unknown, fallback = 0): number {
    return typeof n === "number" && Number.isFinite(n) ? Math.floor(n) : fallback;
  }
  
  function safeNullableInt(n: unknown): number | null {
    return typeof n === "number" && Number.isFinite(n) ? Math.floor(n) : null;
  }
  
  export function pruneGameData(rows: EnrichedGameRow[]): PrunedGameRow[] {
    return rows.map((r) => ({
      universeId: r.universeId,
  
      name: typeof r.name === "string" ? r.name : "",
      developer: typeof r.developerName === "string" ? r.developerName : null,
  
      ageDays: safeNullableInt(r.gameAgeDays),
  
      players: safeInt(r.playing, 0),
      visits: safeInt(r.visits, 0),
      favorites: safeInt(r.favorites, 0),
      likes: safeInt(r.likes, 0 ),
      dislikes: safeInt(r.dislikes, 0),
  
      paidAccess: typeof r.paidAccess === "boolean" ? r.paidAccess : null,
      paidAccessPrice: safeNullableInt(r.paidAccessPrice),
  
      gamePassCount: safeNullableInt(r.gamePassCount),
      avgGamePassPrice: safeNullableInt(r.avgGamePassPrice),
    }));
  }