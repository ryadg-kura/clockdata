import os
from pathlib import Path
import pandas as pd


def load_gold(gold_path: str) -> pd.DataFrame | None:
    path = Path(gold_path)
    if not path.exists():
        return None
    parquet_files = list(path.glob("*.parquet"))
    if not parquet_files:
        return None
    df = pd.read_parquet(gold_path)
    df["day"] = df["hour"].str[:10]
    df["datetime"] = pd.to_datetime(df["hour"], format="%Y-%m-%d %H")
    return df


def get_devices(df: pd.DataFrame) -> list[str]:
    return sorted(df["device_id"].unique().tolist())


def filter_device(df: pd.DataFrame, device_id: str | None) -> pd.DataFrame:
    if device_id is None:
        return df
    return df[df["device_id"] == device_id]


def compute_kpis(df: pd.DataFrame) -> dict:
    steps_df = df.drop_duplicates(subset=["device_id", "day"])
    return {
        "avg_bpm": round(float(df["avg_bpm"].mean()), 1),
        "max_bpm": int(df["max_bpm"].max()),
        "min_bpm": int(df["min_bpm"].min()),
        "total_steps": int(steps_df["total_steps"].sum()),
    }


def get_bpm_history(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df.sort_values("datetime")
        [["datetime", "avg_bpm", "max_bpm", "min_bpm"]]
        .reset_index(drop=True)
    )


def get_steps_per_day(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df.drop_duplicates(subset=["device_id", "day"])
        .groupby("day", as_index=False)["total_steps"]
        .sum()
        .sort_values("day")
        .reset_index(drop=True)
    )
