from __future__ import annotations

from pathlib import Path
from typing import Optional, List, cast

import pandas as pd


def _as_df(obj: object) -> pd.DataFrame:
    """
    Narrow unknown pandas-ish objects to a DataFrame.
    basedpyright strict sometimes widens .copy() or slicing to DataFrame|Series|Unknown.
    """
    if isinstance(obj, pd.DataFrame):
        return obj
    if isinstance(obj, pd.Series):
        # Convert a Series to a 1-col DataFrame (rare in our flow, but safe)
        return obj.to_frame()
    return pd.DataFrame()


def _series_float(df: pd.DataFrame, col: str, default: float = 0.0) -> pd.Series:
    if col in df.columns:
        raw = pd.to_numeric(df[col], errors="coerce")
        s = pd.Series(raw, index=df.index, dtype="float64").fillna(default)
        return cast(pd.Series, s)
    return pd.Series([default] * len(df), index=df.index, dtype="float64")


def _series_str(df: pd.DataFrame, col: str, default: str = "") -> pd.Series:
    if col in df.columns:
        s = pd.Series(df[col], index=df.index).astype("string").fillna(default)
        return cast(pd.Series, s)
    return pd.Series([default] * len(df), index=df.index, dtype="string")


def _fmt_pct(x: float, digits: int = 2) -> str:
    try:
        return f"{float(x) * 100.0:.{digits}f}%"
    except Exception:
        return "—"


def _fmt_num(x: float, digits: int = 2) -> str:
    try:
        v = float(x)
        av = abs(v)
        if av >= 1_000_000:
            return f"{v / 1_000_000:.{digits}f}M"
        if av >= 1_000:
            return f"{v / 1_000:.{digits}f}K"
        return f"{v:.{digits}f}"
    except Exception:
        return "—"


def _safe_sort_by_rank(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "rank" in out.columns:
        raw = pd.to_numeric(out["rank"], errors="coerce")
        out["rank"] = pd.Series(raw, index=out.index)
        by_cols: List[str] = ["rank"]
        out = out.sort_values(by=by_cols, kind="stable")
    return out


def _to_markdown_table(df: pd.DataFrame, cols: List[str], n: int) -> str:
    existing: List[str] = [c for c in cols if c in df.columns]
    if not existing:
        return "_(no columns available)_"
    view = df.loc[:, existing].head(n)
    return view.to_markdown(index=False)


def write_weekly_report(
    exports_dir: str,
    rebalance_date: str,
    export_df: pd.DataFrame,
    membership_history: Optional[pd.DataFrame] = None,
) -> Path:
    out_dir = Path(exports_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    df = _as_df(export_df).copy()
    df = _safe_sort_by_rank(df)

    # Concentration
    w = _series_float(df, "weight", 0.0)
    top5 = float(w.head(5).sum())
    top10 = float(w.head(10).sum())

    lines: List[str] = []
    lines.append(f"# RTE100 Weekly Report — {rebalance_date}\n")

    lines.append("## Summary\n")
    lines.append(f"- Constituents: **{len(df)}**")
    lines.append(f"- Top 5 concentration: **{_fmt_pct(top5)}**")
    lines.append(f"- Top 10 concentration: **{_fmt_pct(top10)}**\n")

    # Top 10
    lines.append("## Top 10\n")
    top_view = df.copy()

    if "weight" in top_view.columns:
        top_view["weight"] = _series_float(top_view, "weight", 0.0).map(lambda v: _fmt_pct(float(v), 2))

    for col in ("edr_7d_mean", "edr_raw"):
        if col in top_view.columns:
            top_view[col] = _series_float(top_view, col, 0.0).map(lambda v: _fmt_num(float(v), 2))

    for col in ("edr_mom", "edr_14d_vol"):
        if col in top_view.columns:
            top_view[col] = _series_float(top_view, col, 0.0).map(lambda v: _fmt_num(float(v), 3))

    top_cols: List[str] = ["rank", "name", "developer", "weight", "edr_7d_mean", "edr_mom", "edr_14d_vol"]
    lines.append(_to_markdown_table(top_view, top_cols, n=10))
    lines.append("")

    # Entrants / exits
    mh = _as_df(membership_history) if membership_history is not None else pd.DataFrame()

    if not mh.empty:
        if "rebalance_date" in mh.columns:
            mh = mh.copy()
            mh["rebalance_date"] = pd.to_datetime(mh["rebalance_date"], errors="coerce").dt.date.astype(str)

        prior_date: Optional[str] = None
        if "rebalance_date" in mh.columns:
            date_list = mh["rebalance_date"].dropna().astype(str).tolist()
            dates = sorted({d for d in date_list if d != rebalance_date})
            if dates:
                prior_date = dates[-1]

        if prior_date is not None:
            prior = _as_df(mh[mh["rebalance_date"] == prior_date]).copy()

            curr_ids = set(_series_str(df, "universeId", "").tolist())
            prior_ids = set(_series_str(prior, "universeId", "").tolist())

            entrant_ids = curr_ids - prior_ids
            exit_ids = prior_ids - curr_ids

            lines.append(f"\n## Changes vs {prior_date}\n")

            if entrant_ids and "universeId" in df.columns:
                entrants = _as_df(df[df["universeId"].isin(list(entrant_ids))]).copy()
                entrants = _safe_sort_by_rank(entrants)

                ent_view = entrants.copy()
                if "weight" in ent_view.columns:
                    ent_view["weight"] = _series_float(ent_view, "weight", 0.0).map(lambda v: _fmt_pct(float(v), 2))

                lines.append("### New entrants\n")
                ent_cols: List[str] = ["rank", "name", "developer", "weight", "universeId"]
                lines.append(_to_markdown_table(ent_view, ent_cols, n=25))
                lines.append("")

            if exit_ids and "universeId" in prior.columns:
                exits = _as_df(prior[prior["universeId"].isin(list(exit_ids))]).copy()
                exits = _safe_sort_by_rank(exits)

                lines.append("### Exits\n")
                exit_cols: List[str] = ["rank", "universeId"]
                if "weight" in exits.columns:
                    exit_cols.append("weight")
                lines.append(_to_markdown_table(exits, exit_cols, n=25))
                lines.append("")

    # Data quality
    lines.append("\n## Data quality\n")
    for col in ("monetization_count", "edr_7d_mean", "score"):
        if col in df.columns:
            raw = pd.to_numeric(df[col], errors="coerce")
            s = pd.Series(raw, index=df.index)  # force Series
            missing = int(s.isna().sum())
            lines.append(f"- Missing `{col}`: **{missing}/{len(df)}**")
        else:
            lines.append(f"- Missing `{col}`: **{len(df)}/{len(df)}** (column absent)")

    md = "\n".join(lines).strip() + "\n"

    report_path = out_dir / f"rte100_report_{rebalance_date}.md"
    report_path.write_text(md, encoding="utf-8")

    return report_path