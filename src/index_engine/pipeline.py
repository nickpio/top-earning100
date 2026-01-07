# src/index_engine/pipeline.py
from __future__ import annotations

from pathlib import Path
from typing import Optional, cast, List

import pandas as pd

from .parameters import EDRParams, RollingParams, RebalanceParams, StorageParams
from .io_runs import load_pruned_file  # <- your loader: (path: Path) -> pd.DataFrame
from .edr_model import compute_edr_daily
from .rolling_features import compute_rolling_features
from .rebalance import rebalance_weekly
from .report import write_weekly_report
from .index_level import build_index_level_series, write_index_level_exports


def _as_df(obj: object) -> pd.DataFrame:
    if isinstance(obj, pd.DataFrame):
        return obj
    if isinstance(obj, pd.Series):
        return obj.to_frame()
    return pd.DataFrame()


def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def _find_pruned_files(runs_dir: Path) -> list[Path]:
    """
    Finds pruned json files under runs/YYYY-MM-DD/pruned/*.json
    Returns sorted list of files (stable order).
    """
    if not runs_dir.exists():
        return []
    files = sorted(runs_dir.glob("*/pruned/*.json"))
    return files


def update_snapshots_from_runs(
    runs_dir: str,
    storage: StorageParams,
    edr_params: EDRParams,
) -> pd.DataFrame:
    """
    Reads all runs under runs_dir, computes per-day snapshots with EDR, and appends to snapshots.parquet.
    Rebuild approach: load all files each run (simple + consistent).
    """
    runs_path = Path(runs_dir)
    pruned_files = _find_pruned_files(runs_path)

    rows: list[pd.DataFrame] = []
    for f in pruned_files:
        # Expect path like runs/2026-01-05/pruned/...json
        run_date = f.parent.parent.name  # YYYY-MM-DD
        df_day = load_pruned_file(f, run_date)
        df_day = _as_df(df_day).copy()

        df_day["snapshot_date"] = run_date

        # Compute EDR + derived columns
        df_day = compute_edr_daily(df_day, edr_params)

        rows.append(df_day)

    if rows:
        snapshots = pd.concat(rows, ignore_index=True)
    else:
        snapshots = pd.DataFrame()

    # Normalize snapshot_date type
    if not snapshots.empty and "snapshot_date" in snapshots.columns:
        snapshots["snapshot_date"] = pd.to_datetime(snapshots["snapshot_date"], errors="coerce").dt.date.astype(str)

    out_dir = Path(storage.index_data_dir)
    _ensure_dir(out_dir)

    snap_path = out_dir / storage.snapshots_file
    snapshots.to_parquet(snap_path, index=False)
    return snapshots

def _series_dt_date(df: pd.DataFrame, col: str) -> pd.Series:
    """Return a date (python date) Series aligned to df.index; missing -> NaT."""
    if col not in df.columns:
        return pd.Series([pd.NaT] * len(df), index=df.index, dtype="datetime64[ns]")
    raw = pd.to_datetime(df[col], errors="coerce")
    # normalize to midnight then convert to date via .dt.date (still Series)
    s = pd.Series(raw, index=df.index).dt.normalize()
    return cast(pd.Series, s)

def rebuild_features(
    snapshots: pd.DataFrame,
    storage: StorageParams,
    rolling_params: RollingParams,
) -> pd.DataFrame:
    """
    Recomputes rolling features from snapshots and writes features.parquet.
    """
    feats = compute_rolling_features(_as_df(snapshots), rolling_params)

    out_dir = Path(storage.index_data_dir)
    _ensure_dir(out_dir)

    feat_path = out_dir / storage.features_file
    _as_df(feats).to_parquet(feat_path, index=False)
    return _as_df(feats)


def _read_parquet_if_exists(path: Path) -> pd.DataFrame:
    if path.exists():
        return cast(pd.DataFrame, pd.read_parquet(path))
    return pd.DataFrame()


def _write_latest_copy(src: Path, dst: Path) -> None:
    dst.write_text(src.read_text(encoding="utf-8"), encoding="utf-8")

def export_rebalance_outputs(
    result_membership_obj: object,
    ranked_universe_obj: object,
    snapshots_obj: object,
    exports_root: Path,
    exports_day: Path,
) -> pd.DataFrame:
    """
    Strict-safe exporter.

    Writes:
      exports/<YYYY-MM-DD>/rte100.csv
      exports/<YYYY-MM-DD>/rte100.json
      exports/rte100_latest.csv
      exports/rte100_latest.json

    Returns export_df (DataFrame).
    """
    exports_root.mkdir(parents=True, exist_ok=True)
    exports_day.mkdir(parents=True, exist_ok=True)

    result_membership = _as_df(result_membership_obj).copy()
    ranked_universe = _as_df(ranked_universe_obj).copy()
    snapshots = _as_df(snapshots_obj).copy()

    if result_membership.empty:
        return pd.DataFrame()

    # --- rebalance date (ISO) ---
    if "rebalance_date" not in result_membership.columns:
        raise ValueError("membership missing 'rebalance_date'")

    reb_raw = result_membership["rebalance_date"].iloc[0]
    reb_date_iso = str(pd.to_datetime(reb_raw, errors="coerce").date())

    # --- prepare snapshots (latest row per universeId as-of rebalance date) ---
    latest_snap = pd.DataFrame({"universeId": []})
    if (not snapshots.empty) and ("universeId" in snapshots.columns) and ("snapshot_date" in snapshots.columns):
        snapshots = snapshots.copy()
        snapshots["snapshot_date"] = _series_dt_date(snapshots, "snapshot_date").dt.date  # Series[date]

        asof = pd.to_datetime(reb_date_iso).date()
        mask = cast(pd.Series, snapshots["snapshot_date"] <= asof)
        snaps_asof = snapshots.loc[mask].copy()

        if not snaps_asof.empty:
            snaps_asof = snaps_asof.sort_values(by=["universeId", "snapshot_date"], kind="stable")
            # groupby/tail can be typed weirdly; cast it back
            latest_snap = cast(
                pd.DataFrame,
                snaps_asof.groupby("universeId", as_index=False).tail(1),
            )

    # --- merge membership + latest snapshot ---
    export_df = cast(
        pd.DataFrame,
        result_membership.merge(latest_snap, on="universeId", how="left", suffixes=("", "_snap")),
    )

    # --- merge ranked features (optional) ---
    if (not ranked_universe.empty) and ("universeId" in ranked_universe.columns):
        wanted: List[str] = ["universeId", "score", "edr_7d_mean", "edr_mom", "edr_14d_vol", "coverage_7d"]
        cols: List[str] = [c for c in wanted if c in ranked_universe.columns]
        if len(cols) > 1:
            export_df = cast(
                pd.DataFrame,
                export_df.merge(ranked_universe.loc[:, cols], on="universeId", how="left"),
            )

    # --- column selection ---
    preferred: List[str] = [
        "rebalance_date", "rank", "universeId", "name", "developer",
        "weight",
        "score", "edr_7d_mean", "edr_mom", "edr_14d_vol", "coverage_7d",
        "avg_ccu", "visits", "favorites", "likes",
        "monetization_count", "median_price", "price_dispersion",
        "engagement_score", "edr_raw",
    ]
    cols_out: List[str] = [c for c in preferred if c in export_df.columns]
    export_df = export_df.loc[:, cols_out].copy()

    # normalize rebalance_date column to ISO strings
    if "rebalance_date" in export_df.columns:
        export_df["rebalance_date"] = export_df["rebalance_date"].apply(
            lambda x: str(pd.to_datetime(x, errors="coerce").date())
        )

    # sort by rank
    if "rank" in export_df.columns:
        raw_rank = pd.to_numeric(export_df["rank"], errors="coerce")
        export_df["rank"] = pd.Series(raw_rank, index=export_df.index)
        by_cols: List[str] = ["rank"]
        export_df = export_df.sort_values(by=by_cols, kind="stable").reset_index(drop=True)

    # --- write files ---
    dated_csv = exports_day / "rte100.csv"
    dated_json = exports_day / "rte100.json"

    export_df.to_csv(dated_csv, index=False)
    export_df.to_json(dated_json, orient="records", indent=2)

    print(f"[index_engine] Exported: {dated_csv}")
    print(f"[index_engine] Exported: {dated_json}")

    return export_df
def run_weekly_rebalance(
    features: pd.DataFrame,
    rebalance_date: str,
    storage: StorageParams,
    rebalance_params: RebalanceParams,
) -> None:
    """
    Runs rebalance for a specific date, writes membership.parquet, and writes exports:
      exports/<YYYY-MM-DD>/*  (dated snapshot)
      exports/*              (latest copies)
    """
    out_dir = Path(storage.index_data_dir)
    _ensure_dir(out_dir)

    exports_root = out_dir / "exports"
    exports_day = exports_root / rebalance_date
    _ensure_dir(exports_root)
    _ensure_dir(exports_day)

    membership_path = out_dir / storage.membership_file
    membership_all = _read_parquet_if_exists(membership_path)

    # Keep 'prior' for entrants/exits if you want "vs last rebalance"
    prior = membership_all.copy()

    result = rebalance_weekly(
        features=_as_df(features),
        rebalance_date=rebalance_date,
        params=rebalance_params,
        prior_membership=prior if not prior.empty else None,
    )

    # Append/save membership history
    new_membership = _as_df(result.membership).copy()
    if membership_all.empty:
        membership_all = new_membership
    else:
        membership_all = pd.concat([membership_all, new_membership], ignore_index=True)

    membership_all.to_parquet(membership_path, index=False)

    # --- Export constituents: dated + latest ---
    snapshots_path = out_dir / storage.snapshots_file
    snapshots = _read_parquet_if_exists(snapshots_path)

    export_df = export_rebalance_outputs(
    result_membership_obj=result.membership,
    ranked_universe_obj=result.ranked,
    snapshots_obj=snapshots,
    exports_root=exports_root,
    exports_day=exports_day,
)

    # --- Weekly report: to "Weekly Reports" folder ---
    weekly_reports_dir = exports_root / "Weekly Reports"
    _ensure_dir(weekly_reports_dir)
    write_weekly_report(
        exports_dir=str(weekly_reports_dir),
        rebalance_date=rebalance_date,
        export_df=_as_df(export_df),
        membership_history=_as_df(membership_all),
    )

    # --- Index level: dated folder only ---
    index_ts = build_index_level_series(
        snapshots=_as_df(snapshots),
        membership_history=_as_df(membership_all),
        base_level=1000.0,
        eps=1.0,
    )

    write_index_level_exports(_as_df(index_ts), exports_dir=str(exports_day))

    print(f"[index_engine] Rebalance complete: {rebalance_date}")
    print(f"[index_engine] Exports: {exports_day}")
    print(f"[index_engine] Weekly reports: {weekly_reports_dir}")


def run_pipeline(
    runs_dir: str,
    rebalance_date: Optional[str],
    edr_params: EDRParams,
    rolling_params: RollingParams,
    rebalance_params: RebalanceParams,
    storage: StorageParams,
) -> None:
    """
    Orchestrates:
      1) snapshots rebuild from runs/
      2) rolling feature rebuild
      3) optional weekly rebalance (and export/report/index level)
    """
    snapshots = update_snapshots_from_runs(runs_dir=runs_dir, storage=storage, edr_params=edr_params)
    features = rebuild_features(snapshots=snapshots, storage=storage, rolling_params=rolling_params)

    if rebalance_date is not None:
        # Normalize to ISO date string
        reb_date_iso = str(pd.to_datetime(rebalance_date, errors="coerce").date())
        run_weekly_rebalance(
            features=features,
            rebalance_date=reb_date_iso,
            storage=storage,
            rebalance_params=rebalance_params,
        )