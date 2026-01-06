from __future__ import annotations

from ast import Pass
import json
import math
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

import pandas as pd

# -- Parameters
@dataclass
class EDRParams:
    alpha: float = 20.0
    base_rate: float = 0.01
    gamma: float = 0.02

    pcr_floor: float = 0.001
    pcr_cap: float = 0.05

    engagement_scale: float = 50.0
    engagement_cap: float = 1.5

DATE_RE = re.compile(fr"(\d{4}-\d{2}-\d{2})")

# -- Run discovery
def discover_pruned_run_files(runs_dir: Union[str, Path]) -> List[Tuple[str, Path]]:

    runs_dir = Path(runs_dir)
    if not runs_dir.exists():
        raise FileNotFoundError(f"Runs dir not found: {runs_dir}")

    files: List[Tuple[str, Path]] = []

    for fp in runs_dir.glob("*/pruned/*.json"):
        parts = fp.parts
        m = DATE_RE.search(str(fp))
        if not m:
            continue
        date_str = m.group(1)
        files.append((date_str, fp))
    
    files.sort(key=lambda x: x[0])
    return files

def load_pruned_file(path: Path, snapshot_date: str) -> pd.DataFrame:



    with open(path, "r", encoding="utf-8") as f:
        obj = json.load(f)
    
    if isinstance(obj, dict) and "data" in obj and isinstance(obj["data"], list):
        rows = obj["data"]
    elif isinstance(obj, dict) and all(isinstance(v, dict) for v in obj.values()):
        rows = list(obj.values())
    elif isinstance(obj, list):
        rows = obj
    else:
        raise ValueError(f"Unsupported JSON shape in {path}")
    
    df = pd.DataFrame(rows)
    df["snapshot_date"] = pd.to_datetime(snapshot_date).date()

    if "universeId" not in df.columns:
        if "universe_id" in df.columns:
            df["universeId"] = df["universe_id"]
        elif "id" in df.columns:
            df["universeId"] = df["id"]
    
    return df

# -- Feature helpers

def _safe_div(a: pd.Series, b: pd.Series) -> pd.Series:
    b2 = b.replace({0: pd.NA})
    return (a / b2).fillna(0.0)

def _extract_prices(x: Any) -> List[float]:
    if not x or not isinstance(x, list):
        return []
    out = []
    for it in x:
        if isinstance(it, dict) and it.get("price") is not None:
            try:
                out.append(float(it["price"]))
            except Exception:
                pass
    return out

def add_ccu(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if "avg_ccu" in df.columns:
        df["avg_ccu"] = df["avg_ccu"].fillna(0).astype(float)
        return df
    for c in ("players", "playing", "ccu", "concurrentPlayers"):
        if c in df.columns:
            df["avg_ccu"] = df[c].fillna(0).astype(float)
            return df
    df["avg_ccu"] = 0.0
    return df

def add_monetization(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    if "monetization_count" not in df.columns:
        if "num_gamepasses" in df.columns or "num_devproducts" in df.columns:
            df["monetization_count"] = (
                df.get("num_gamepasses", 0).fillna(0).astype(float)
                + df.get("num_devproducts", 0).fillna(0).astype(float)
            )
        else:
            gp = df.get("game_passes", pd.Series([None] * len(df)))
            dp = df.get("dev_products", pd.Series([None] * len(df)))
            df["monetization_count"] = gp.apply(lambda v: len(v) if isinstance(v, list) else 0) + dp.apply(lambda v: len(v) if isinstance(v, list) else 0)

    gp_prices = df.get("game_passes", pd.Series([None] * len(df))).apply(_extract_prices)
    dp_prices = df.get("dev_products", pd.Series([None] * len(df))).apply(_extract_prices)
    all_prices = gp_prices + dp_prices

    def median(prices: List[float]) -> float:
        if not prices:
            return 0.0
        s = sorted(prices)
        mid = len(s)//2
        return float(s[mid]) if len(s) % 2 == 1 else float((s[mid-1] + s[mid]) / 2)
    
    def dispersion(prices: List[float]) -> float:
        if not prices:
            return 0.0
        m = sum(prices) / len(prices)
        if m <= 0:
            return 0.0
        var = sum((p - m) ** 2 for p in prices) / len(prices)
        return float(math.sqrt(var) / m)
    
    df["median_price"] = all_prices.apply(median)
    df["price_dispersion"] = all_prices.apply(dispersion)

    return df

def add_engagement(df: pd.DataFrame, params: EDRParams) -> pd.DataFrame:
    df = df.copy()
    visits = df.get("visits", 0).fillna(0).astype(float)
    favorites = df.get("favorites", 0).fillna(0).astype(float)
    likes = df.get("likes", 0).fillna(0).astype(float)

    fav_rate = _safe_div(favorites, visits)
    like_rate = _safe_div(likes, visits)
    raw = 0.5 * (fav_rate + like_rate)

    df["engagement_score"] = (raw * params.engagement_scale).clip(0.0, params.engagement_cap)
    return df


# -----------------------------
# EDR computation
# -----------------------------

def compute_edr_for_snapshots(df: pd.DataFrame, params: EDRParams) -> pd.DataFrame:
    df = add_ccu(df)
    df = add_monetization(df)
    df = add_engagement(df, params)

    df["dau_est"] = (params.alpha * df["avg_ccu"]).clip(lower=0.0)

    # PCR v1: base_rate * log(1 + monetization_count)
    df["pcr"] = (
        params.base_rate * (1.0 + df["monetization_count"]).apply(lambda x: math.log(x))
    ).clip(lower=params.pcr_floor, upper=params.pcr_cap)

    # ASPU proxy
    df["aspu"] = (df["median_price"] * (1.0 + df["price_dispersion"])).clip(lower=0.0)

    df["spend_revenue"] = df["dau_est"] * df["pcr"] * df["aspu"]
    df["premium_revenue"] = params.gamma * df["dau_est"] * df["engagement_score"]
    df["edr_raw"] = (df["spend_revenue"] + df["premium_revenue"]).clip(lower=0.0)

    return df


# -- Build from runs/
def build_edr_history_from_runs(
    runs_dir: Union[str, Path],
    params: Optional[EDRParams] = None,
) -> pd.DataFrame:
    params = params or EDRParams()
    run_files = discover_pruned_run_files(runs_dir)

    if not run_files:
        raise FileNotFoundError(f"No pruned JSON files found under {runs_dir}")

    frames = []
    for date_str, fp in run_files:
        df_day = load_pruned_file(fp, snapshot_date=date_str)
        df_day = compute_edr_for_snapshots(df_day, params)
        frames.append(df_day)

    history = pd.concat(frames, ignore_index=True)

    # Minimal output
    keep = [
        "snapshot_date", "universeId", "name", "developer",
        "avg_ccu", "visits", "favorites", "likes",
        "monetization_count", "median_price", "price_dispersion",
        "engagement_score", "dau_est", "pcr", "aspu",
        "spend_revenue", "premium_revenue", "edr_raw",
    ]
    keep = [c for c in keep if c in history.columns]
    return history[keep].copy()


if __name__ == "__main__":
    history = build_edr_history_from_runs(
        runs_dir="runs",
        params=EDRParams(alpha=20.0, base_rate=0.01, gamma=0.02),
    )
    history.to_parquet("edr_history.parquet", index=False)
    print("Wrote edr_history.parquet", len(history))