#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""Tests for ``SASMetadata``: typing a ``.sas7bdat`` result from its column metadata."""

from __future__ import annotations

import datetime
import io
from unittest.mock import patch

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from airflow.providers.sas.driver.metadata import _CHARACTER, _DATE, _DATETIME, _NUMERIC, SASMetadata

from unit.sas.fakes import (
    COLUMN_FORMATS,
    COLUMN_TYPES,
    DATE32,
    EPOCH_DAYS_2020_01_01,
    _FakeReader,
    _raw_frame,
)


# ---------------------------------------------------------------------------
# Column kinds from the SAS metadata
# ---------------------------------------------------------------------------
@pytest.mark.parametrize(
    ("fmt", "expected"),
    [
        ("DATE9.", _DATE),
        ("date9.", _DATE),
        (b"DATE9.", _DATE),
        ("YYMMDD10.", _DATE),
        ("DDMMYY8.", _DATE),
        ("E8601DA10.", _DATE),
        ("NLDATE20.", _DATE),
        ("DATETIME21.2", _DATETIME),
        ("DATEAMPM22.", _DATETIME),
        ("E8601DT19.", _DATETIME),
        ("NLDATM30.", _DATETIME),
        ("TIME8.", _NUMERIC),
        ("COMMA12.2", _NUMERIC),
        ("BEST12.", _NUMERIC),
        ("", _NUMERIC),
    ],
)
def test_column_kind_from_format(fmt, expected):
    assert SASMetadata().column_kind("AMOUNT", "d", fmt) is expected


def test_column_kind_of_character_override_and_name_heuristic():
    meta = SASMetadata()
    assert meta.column_kind("NAME", "s", "DATE9.") is _CHARACTER
    assert meta.column_kind("START_DATE", "d", None) is _DATE, (
        "no FORMAT metadata at all: the name suffix decides"
    )
    assert meta.column_kind("START_DATE", "d", "") is _NUMERIC, (
        "FORMAT metadata without a format: the name is not looked at"
    )

    override = SASMetadata(date_columns={"datdep"})
    assert override.column_kind("DATDEP", "d", "BEST12.") is _DATE
    assert override.column_kind("START_DATE", "d", None) is _NUMERIC, (
        "an explicit date_columns set turns the heuristic off"
    )


def test_column_kinds_cover_the_sas_formats_pandas_converts():
    # pandas' own lists (read_sas(convert_dates=True)), the source of ours.
    from pandas.io.sas.sas_constants import sas_date_formats, sas_datetime_formats

    meta = SASMetadata()
    for name in sas_datetime_formats:
        assert meta.column_kind("X", "d", f"{name}20.") is _DATETIME, name
    for name in sas_date_formats:
        assert meta.column_kind("X", "d", f"{name}10.") is _DATE, name


# ---------------------------------------------------------------------------
# normalize_dataframe: stable dtypes per SAS column kind
# ---------------------------------------------------------------------------
def test_normalize_dataframe_dtypes_follow_sas_metadata():
    df = SASMetadata().normalize_dataframe(
        _raw_frame(), column_types=COLUMN_TYPES, column_formats=COLUMN_FORMATS
    )

    assert isinstance(df["NAME"].dtype, pd.StringDtype)
    assert df["AMOUNT"].dtype == np.dtype("float64")
    assert df["DATDEP"].dtype == DATE32
    assert df["EMPTY_DATE"].dtype == DATE32, "an all-missing date column keeps its type"
    assert pd.api.types.is_datetime64_any_dtype(df["TS"])


def test_normalize_dataframe_values():
    df = SASMetadata().normalize_dataframe(
        _raw_frame(), column_types=COLUMN_TYPES, column_formats=COLUMN_FORMATS
    )

    # Character: right-stripped, padded dot is missing, unpadded dot and blank are kept.
    assert df["NAME"].tolist()[:2] == ["Alice", "Bob"]
    assert pd.isna(df["NAME"].iloc[2])
    assert df["NAME"].iloc[3] == "."
    assert df["NAME"].iloc[4] == ""

    # Date: days since 1960-01-01 become dates.
    assert df["DATDEP"].iloc[0] == datetime.date(2020, 1, 1)
    assert pd.isna(df["DATDEP"].iloc[1])
    assert df["DATDEP"].iloc[2] == datetime.date(1960, 1, 1)

    # Datetime: seconds since 1960-01-01T00:00:00.
    assert df["TS"].iloc[0] == pd.Timestamp("1960-01-01 00:00:00")
    assert df["TS"].iloc[2] == pd.Timestamp("1960-01-02 00:00:00.5")
    assert pd.isna(df["TS"].iloc[1])

    # Plain numeric stays float with NaN for missing.
    assert df["AMOUNT"].iloc[0] == 1.5
    assert np.isnan(df["AMOUNT"].iloc[1])


def test_normalize_dataframe_accepts_dates_already_converted_by_pandas():
    """``read_sas(convert_dates=True)`` already yields datetime64 for known formats."""
    raw = pd.DataFrame(
        {
            "DATDEP": pd.to_datetime(["2020-01-01", None, "2021-06-30"]),
            "TS": pd.to_datetime(["2020-01-01 10:00:00", None, "2021-06-30 00:00:00"]),
        }
    )
    df = SASMetadata().normalize_dataframe(
        raw,
        column_types={"DATDEP": "d", "TS": "d"},
        column_formats={"DATDEP": "DATE9.", "TS": "DATETIME21."},
    )
    assert df["DATDEP"].dtype == DATE32
    assert df["DATDEP"].iloc[2] == datetime.date(2021, 6, 30)
    assert pd.api.types.is_datetime64_any_dtype(df["TS"])
    assert df["TS"].iloc[0] == pd.Timestamp("2020-01-01 10:00:00")


def test_normalize_dataframe_explicit_date_columns_override_format():
    raw = pd.DataFrame({"DATDEP": [EPOCH_DAYS_2020_01_01, 0.0]})
    df = SASMetadata(date_columns={"datdep"}).normalize_dataframe(
        raw, column_types={"DATDEP": "d"}, column_formats={"DATDEP": "BEST12."}
    )
    assert df["DATDEP"].dtype == DATE32


def test_normalize_dataframe_suffix_heuristic_only_without_format_metadata():
    raw = pd.DataFrame({"START_DATE": [EPOCH_DAYS_2020_01_01, np.nan]})

    without_metadata = SASMetadata().normalize_dataframe(raw.copy())
    assert without_metadata["START_DATE"].dtype == DATE32

    with_metadata = SASMetadata().normalize_dataframe(
        raw.copy(), column_types={"START_DATE": "d"}, column_formats={"START_DATE": ""}
    )
    assert with_metadata["START_DATE"].dtype == np.dtype("float64")


def test_normalize_dataframe_is_stable_across_runs_and_chunks():
    """Same metadata, different content (here: all missing) → same dtypes."""
    meta = SASMetadata()
    full = meta.normalize_dataframe(_raw_frame(), column_types=COLUMN_TYPES, column_formats=COLUMN_FORMATS)
    empty = meta.normalize_dataframe(
        pd.DataFrame({col: [np.nan] * 2 for col in COLUMN_TYPES}).astype({"NAME": object}),
        column_types=COLUMN_TYPES,
        column_formats=COLUMN_FORMATS,
    )
    assert full.dtypes.to_dict() == empty.dtypes.to_dict()
    assert full.iloc[1:2].dtypes.to_dict() == full.dtypes.to_dict()


def test_normalized_dataframe_round_trips_through_parquet():
    df = SASMetadata().normalize_dataframe(
        _raw_frame(), column_types=COLUMN_TYPES, column_formats=COLUMN_FORMATS
    )

    buffer = io.BytesIO()
    df.to_parquet(buffer)
    buffer.seek(0)
    schema = pq.read_schema(buffer)

    assert pa.types.is_string(schema.field("NAME").type) or pa.types.is_large_string(
        schema.field("NAME").type
    )
    assert schema.field("AMOUNT").type == pa.float64()
    assert schema.field("DATDEP").type == pa.date32()
    assert schema.field("EMPTY_DATE").type == pa.date32()
    assert pa.types.is_timestamp(schema.field("TS").type)

    buffer.seek(0)
    assert pq.read_metadata(buffer).num_rows == 5


def test_parse_sas7bdat_chunks_types_every_chunk_from_the_file_metadata():
    typed = SASMetadata().normalize_dataframe(
        _raw_frame(), column_types=COLUMN_TYPES, column_formats=COLUMN_FORMATS
    )
    reader = _FakeReader(_raw_frame(), chunksize=2)

    with patch("pandas.io.sas.sas7bdat.SAS7BDATReader", return_value=reader) as read_sas:
        chunks = list(SASMetadata().parse_sas7bdat_chunks(io.BytesIO(), "iso-8859-15", 2))

    assert read_sas.call_args.kwargs["chunksize"] == 2
    assert read_sas.call_args.kwargs["convert_dates"] is False
    assert [len(c) for c in chunks] == [2, 2, 1]
    for chunk in chunks:
        assert chunk.dtypes.to_dict() == typed.dtypes.to_dict()
        assert chunk.index.tolist() == list(range(len(chunk)))
    # The second chunk holds only missing dates and still carries the date type.
    assert chunks[1]["EMPTY_DATE"].dtype == DATE32
    assert pd.concat(chunks, ignore_index=True).equals(typed)
    assert reader.closed


# ---------------------------------------------------------------------------
# dtype_backend, as passed by SqlToS3Operator(read_kwargs={"dtype_backend": "pyarrow"})
# ---------------------------------------------------------------------------
def test_dtype_backend_gives_a_stable_parquet_schema_and_nullable_floats():
    arrow = SASMetadata().normalize_dataframe(
        _raw_frame(),
        column_types=COLUMN_TYPES,
        column_formats=COLUMN_FORMATS,
        dtype_backend="pyarrow",
    )
    buffer = io.BytesIO()
    arrow.to_parquet(buffer)
    buffer.seek(0)
    assert pq.read_schema(buffer).field("AMOUNT").type == pa.float64(), (
        "integral floats do not become integers"
    )

    nullable = SASMetadata().normalize_dataframe(
        _raw_frame(),
        column_types=COLUMN_TYPES,
        column_formats=COLUMN_FORMATS,
        dtype_backend="numpy_nullable",
    )
    assert nullable["AMOUNT"].dtype == pd.Float64Dtype()


def test_normalize_dataframe_pyarrow_backend_in_one_pass():
    df = SASMetadata().normalize_dataframe(
        _raw_frame(),
        column_types=COLUMN_TYPES,
        column_formats=COLUMN_FORMATS,
        dtype_backend="pyarrow",
    )

    assert df["NAME"].dtype == pd.ArrowDtype(pa.string())
    assert df["AMOUNT"].dtype == pd.ArrowDtype(pa.float64())
    assert df["DATDEP"].dtype == DATE32
    assert df["TS"].dtype == pd.ArrowDtype(pa.timestamp("us"))
    assert df["NAME"].tolist()[:2] == ["Alice   ", "Bob"] or df["NAME"].tolist()[:2] == [
        "Alice",
        "Bob",
    ]
    assert pd.isna(df["NAME"].iloc[2])
    assert df["NAME"].iloc[3] == "."
    with pytest.raises(ValueError, match="Unsupported dtype_backend"):
        SASMetadata().normalize_dataframe(_raw_frame(), dtype_backend="polars")


def test_to_records_gives_plain_python_with_none_for_every_missing_kind():
    typed = SASMetadata().normalize_dataframe(
        _raw_frame(), column_types=COLUMN_TYPES, column_formats=COLUMN_FORMATS
    )
    rows = SASMetadata.to_records(typed)

    assert rows[0] == (
        "Alice",
        1.5,
        datetime.date(2020, 1, 1),
        datetime.datetime(1960, 1, 1),
        None,
    )
    assert rows[1] == ("Bob", None, None, None, None)
    assert type(rows[0][1]) is float
    assert type(rows[0][3]) is datetime.datetime
    assert SASMetadata.to_records(typed.iloc[0:0]) == []

    arrow = SASMetadata.normalize_dataframe(
        SASMetadata(),
        _raw_frame(),
        column_types=COLUMN_TYPES,
        column_formats=COLUMN_FORMATS,
        dtype_backend="pyarrow",
    )
    assert SASMetadata.to_records(arrow)[:2] == rows[:2]
