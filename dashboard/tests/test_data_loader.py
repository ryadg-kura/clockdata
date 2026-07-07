import os
import sys
import tempfile

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from data_loader import (
    compute_kpis,
    filter_device,
    get_bpm_history,
    get_devices,
    get_steps_per_day,
    load_analysis,
    load_gold,
)

SCHEMA = pa.schema([
    ("device_id", pa.string()),
    ("hour", pa.string()),
    ("avg_bpm", pa.float64()),
    ("max_bpm", pa.int32()),
    ("min_bpm", pa.int32()),
    ("total_steps", pa.int64()),
])


@pytest.fixture
def gold_dir():
    with tempfile.TemporaryDirectory() as d:
        table = pa.table(
            {
                "device_id": ["dev1", "dev1", "dev2"],
                "hour": ["2024-04-26 08", "2024-04-26 09", "2024-04-26 08"],
                "avg_bpm": [72.0, 75.0, 80.0],
                "max_bpm": pa.array([85, 88, 95], type=pa.int32()),
                "min_bpm": pa.array([60, 62, 70], type=pa.int32()),
                "total_steps": pa.array([5000, 5000, 3000], type=pa.int64()),
            },
            schema=SCHEMA,
        )
        pq.write_table(table, os.path.join(d, "part-00000.parquet"))
        open(os.path.join(d, "_SUCCESS"), "w").close()
        yield d


@pytest.fixture
def loaded_df(gold_dir):
    return load_gold(gold_dir)


def test_load_gold_returns_none_for_nonexistent_dir():
    assert load_gold("/tmp/does_not_exist_clockdata_xyz") is None


def test_load_gold_returns_none_for_empty_dir():
    with tempfile.TemporaryDirectory() as d:
        assert load_gold(d) is None


def test_load_gold_adds_day_and_datetime_columns(loaded_df):
    assert loaded_df is not None
    assert "day" in loaded_df.columns
    assert "datetime" in loaded_df.columns
    assert loaded_df["day"].iloc[0] == "2024-04-26"
    assert pd.api.types.is_datetime64_any_dtype(loaded_df["datetime"])


def test_get_devices_returns_sorted_list(loaded_df):
    assert get_devices(loaded_df) == ["dev1", "dev2"]


def test_filter_device_filters_by_id(loaded_df):
    result = filter_device(loaded_df, "dev1")
    assert set(result["device_id"].unique()) == {"dev1"}
    assert len(result) == 2


def test_filter_device_returns_all_when_none(loaded_df):
    result = filter_device(loaded_df, None)
    assert len(result) == 3


def test_compute_kpis_deduplicates_steps(loaded_df):
    kpis = compute_kpis(loaded_df)
    assert kpis["total_steps"] == 8000
    assert kpis["max_bpm"] == 95
    assert kpis["min_bpm"] == 60
    assert round(kpis["avg_bpm"], 1) == round((72.0 + 75.0 + 80.0) / 3, 1)


def test_get_bpm_history_is_sorted_by_datetime(loaded_df):
    hist = get_bpm_history(loaded_df)
    assert list(hist.columns) == ["datetime", "avg_bpm", "max_bpm", "min_bpm"]
    assert hist["datetime"].is_monotonic_increasing


def test_get_steps_per_day_deduplicates_and_sums_by_day(loaded_df):
    result = get_steps_per_day(loaded_df)
    assert list(result.columns) == ["day", "total_steps"]
    assert len(result) == 1
    assert result["total_steps"].iloc[0] == 8000


def test_load_analysis_returns_none_for_nonexistent_dir():
    assert load_analysis("/tmp/does_not_exist_clockdata_analysis_xyz") is None


def test_load_analysis_returns_none_for_empty_dir():
    with tempfile.TemporaryDirectory() as d:
        assert load_analysis(d) is None


def test_load_analysis_reads_jsonl_and_returns_question_answer(tmp_path):
    jsonl = (
        '{"question":"Q1","answer":"A1"}\n'
        '{"question":"Q2","answer":"A2"}\n'
    )
    (tmp_path / "part-00000.json").write_text(jsonl, encoding="utf-8")
    (tmp_path / "_SUCCESS").write_text("")
    df = load_analysis(str(tmp_path))
    assert df is not None
    assert list(df.columns) == ["question", "answer"]
    assert len(df) == 2
    assert df["question"].iloc[0] == "Q1"
    assert df["answer"].iloc[1] == "A2"
