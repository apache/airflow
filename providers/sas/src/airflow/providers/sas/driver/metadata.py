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
"""Typing of SAS results: the dtype of each column kind, from the ``.sas7bdat`` column metadata."""

from __future__ import annotations

import re
from abc import ABC, abstractmethod
from dataclasses import dataclass
from functools import cache
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Generator, Mapping

    from pandas import DataFrame, Series
    from pandas.io.sas.sas7bdat import SAS7BDATReader


@dataclass(frozen=True)
class _DtypeBackend:
    """
    The pandas dtype of each SAS column kind for one ``dtype_backend``.

    :meth:`named` resolves the backend name once, so the typing code never
    branches on it.  Dates are ``date32[pyarrow]`` for every backend (see
    :meth:`SASMetadata.normalize_dataframe`).
    """

    character: Any
    numeric: Any
    datetime: Any

    @classmethod
    @cache
    def named(cls, dtype_backend: str | None) -> _DtypeBackend:
        """Return the backend for a ``dtype_backend`` value of ``pandas.read_sql``."""
        match dtype_backend:
            case None:
                return cls("string", "float64", "datetime64[us]")
            case "numpy_nullable":
                return cls("string", "Float64", "datetime64[us]")
            case "pyarrow":
                import pandas as pd
                import pyarrow as pa

                return cls(
                    pd.ArrowDtype(pa.string()),
                    pd.ArrowDtype(pa.float64()),
                    pd.ArrowDtype(pa.timestamp("us")),
                )
            case _:
                raise ValueError(f"Unsupported dtype_backend: {dtype_backend!r}")


_FORMAT_WIDTH_RE = re.compile(r"[0-9.]+$")


class _ColumnKind(ABC):
    """
    A kind of SAS column, and how a raw ``read_sas`` series becomes it.

    The kinds are stateless, one instance each: :meth:`SASMetadata.column_kind`
    picks the instance from the column metadata and
    :meth:`SASMetadata.normalize_dataframe` calls its :meth:`convert`, so
    neither branches on the kind.  A kind that a SAS FORMAT selects lists
    those formats as stored in the ``.sas7bdat`` metadata, without width or
    decimals: exact names in ``FORMATS`` (from the SAS Formats reference,
    the same lists pandas uses for ``convert_dates``) and, for the NLS and
    width variants such as ``NLDATEW`` or ``YYMMDDN``, ``FORMAT_PREFIXES``.
    """

    FORMATS: frozenset[str] = frozenset()
    FORMAT_PREFIXES: tuple[str, ...] = ()

    def matches(self, format_name: str) -> bool:
        """Whether *format_name* (upper case, no width or decimals) selects this kind."""
        return format_name in self.FORMATS or format_name.startswith(self.FORMAT_PREFIXES)

    @abstractmethod
    def convert(self, series: Series, backend: _DtypeBackend) -> Series:
        """Return *series* in this kind's dtype for *backend*."""


class _CharacterColumn(_ColumnKind):
    def convert(self, series: Series, backend: _DtypeBackend) -> Series:
        """
        Give a character column its string dtype and drop padded missing dots.

        One dtype conversion (to the backend's character dtype, so the
        column is never converted twice), one ``rstrip`` (SAS pads to the declared
        length; pandas' parser strips the raw bytes already, the pass is
        kept for readers that do not), then the padded SAS missing marker
        (whitespace followed by a dot) becomes missing.  On pandas 3 these
        are pyarrow string kernels and cost a few milliseconds per 100k
        rows; the date and datetime kinds are where typing time goes.
        """
        text = series.astype(backend.character).str.rstrip()
        padded_dot = ((text.str.strip() == ".") & (text != ".")).fillna(False)
        return text.where(~padded_dot.astype(bool))


class _NumericColumn(_ColumnKind):
    def convert(self, series: Series, backend: _DtypeBackend) -> Series:
        """``float64`` (or the backend's twin); a TIME format stays numeric."""
        import pandas as pd
        from pandas.api.types import is_datetime64_any_dtype, is_numeric_dtype

        if is_datetime64_any_dtype(series):
            # pandas already applied a date format we do not classify.
            return series
        if is_numeric_dtype(series):
            return series.astype(backend.numeric)
        return pd.to_numeric(series, errors="coerce").astype(backend.numeric)


class _DateColumn(_ColumnKind):
    """Days since 1960-01-01, ``date32[pyarrow]`` for every backend."""

    FORMATS: frozenset[str] = frozenset(
        {
            "DATE", "DAY", "DDMMYY", "DOWNAME", "JULDAY", "JULIAN", "MMDDYY",
            "MMYY", "MMYYC", "MMYYD", "MMYYP", "MMYYS", "MMYYN", "MONNAME",
            "MONTH", "MONYY", "QTR", "QTRR", "NENGO", "WEEKDATE", "WEEKDATX",
            "WEEKDAY", "WEEKV", "WORDDATE", "WORDDATX", "YEAR", "YYMM", "YYMMC",
            "YYMMD", "YYMMP", "YYMMS", "YYMMN", "YYMON", "YYMMDD", "YYQ", "YYQC",
            "YYQD", "YYQP", "YYQS", "YYQN", "YYQR", "YYQRC", "YYQRD", "YYQRP",
            "YYQRS", "YYQRN", "YYMMDDP", "YYMMDDC", "E8601DA", "YYMMDDN",
            "MMDDYYC", "MMDDYYS", "MMDDYYD", "YYMMDDS", "B8601DA", "DDMMYYN",
            "YYMMDDD", "DDMMYYB", "DDMMYYP", "MMDDYYP", "YYMMDDB", "MMDDYYN",
            "DDMMYYC", "DDMMYYD", "DDMMYYS", "MINGUO", "NLDATE",
        }
    )  # fmt: skip
    FORMAT_PREFIXES: tuple[str, ...] = (
        "DATE",
        "DAY",
        "DDMMYY",
        "DOWNAME",
        "JULDAY",
        "JULIAN",
        "MMDDYY",
        "MMYY",
        "MONNAME",
        "MONTH",
        "MONYY",
        "QTR",
        "QTRR",
        "WEEKDATE",
        "WEEKDATX",
        "WEEKDAY",
        "WORDDATE",
        "WORDDATX",
        "YEAR",
        "YYMM",
        "YYMMDD",
        "YYMON",
        "YYQ",
        "YYQR",
        "NLDATE",
    )

    # Column-name suffixes that mark a date when no FORMAT metadata is
    # available at all (DataFrames that did not come from a ``.sas7bdat``).
    NAME_SUFFIXES: tuple[str, ...] = ("_DATE", "_DT", "_DTE", "_DAT", "BDT")

    def convert(self, series: Series, backend: _DtypeBackend) -> Series:
        import numpy as np
        import pandas as pd
        from pandas.api.types import is_datetime64_any_dtype, is_numeric_dtype

        if is_datetime64_any_dtype(series):
            # pandas converted a DATE format itself: values are midnight, so
            # the cast is exact; fall back to a floor when a caller declared a
            # DATETIME column as a date through ``date_columns``.
            stamps = series
        elif is_numeric_dtype(series):
            # Days since 1960-01-01 as floats.  Plain numpy arithmetic is
            # about five times faster than ``to_timedelta`` here.
            values = series.to_numpy(dtype="float64", na_value=np.nan)
            missing = np.isnan(values)
            days = np.where(missing, 0, np.floor(values)).astype("int64")
            dates = np.datetime64("1960-01-01", "D") + days.astype("timedelta64[D]")
            dates[missing] = np.datetime64("NaT")
            stamps = pd.Series(dates.astype("datetime64[s]"), index=series.index)
        else:
            # Already typed (e.g. date32) or unknown objects: leave untouched.
            return series
        try:
            import pyarrow as pa
        except ImportError:
            return stamps.dt.floor("D").dt.date
        date32 = pd.ArrowDtype(pa.date32())
        try:
            return stamps.astype(date32)
        except (ValueError, TypeError):
            return stamps.dt.floor("D").astype(date32)


class _DatetimeColumn(_ColumnKind):
    """Seconds since 1960-01-01T00:00:00, microsecond timestamps."""

    FORMATS: frozenset[str] = frozenset(
        {
            "DATETIME", "DTWKDATX", "B8601DN", "B8601DT", "B8601DX", "B8601DZ",
            "B8601LX", "E8601DN", "E8601DT", "E8601DX", "E8601DZ", "E8601LX",
            "DATEAMPM", "DTDATE", "DTMONYY", "DTYEAR", "TOD", "MDYAMPM", "NLDATM",
        }
    )  # fmt: skip
    FORMAT_PREFIXES: tuple[str, ...] = ("DATETIME", "DATEAMPM", "NLDATM")

    def convert(self, series: Series, backend: _DtypeBackend) -> Series:
        import numpy as np
        import pandas as pd
        from pandas.api.types import is_datetime64_any_dtype, is_numeric_dtype

        if is_datetime64_any_dtype(series):
            # Pin the unit: pandas picks ns or us depending on the input, and
            # microseconds are what Parquet readers agree on.
            stamps = series.astype("datetime64[us]")
        elif is_numeric_dtype(series):
            # Seconds since 1960-01-01T00:00:00 as floats, rounded to the
            # microsecond.  ``to_timedelta`` on floats is ~45x slower.
            values = series.to_numpy(dtype="float64", na_value=np.nan)
            missing = np.isnan(values)
            micros = np.where(missing, 0, np.rint(values * 1e6)).astype("int64")
            datetimes = np.datetime64("1960-01-01T00:00:00", "us") + micros.astype("timedelta64[us]")
            datetimes[missing] = np.datetime64("NaT")
            stamps = pd.Series(datetimes, index=series.index)
        else:
            return series
        return stamps.astype(backend.datetime)


_CHARACTER = _CharacterColumn()
_NUMERIC = _NumericColumn()
_DATE = _DateColumn()
_DATETIME = _DatetimeColumn()
# Datetime is asked first: several of its formats (DATETIME, DATEAMPM) start
# with the DATE prefix.
_FORMATTED_KINDS: tuple[_ColumnKind, ...] = (_DATETIME, _DATE)


class SASMetadata:
    """
    Type SAS results from the column metadata of the ``.sas7bdat`` file.

    Turns the raw ``pandas.read_sas()`` output into a DataFrame with a
    stable, documented dtype per SAS column kind, driven by the column
    type and FORMAT metadata stored in the file itself (see
    :meth:`normalize_dataframe`), and that DataFrame into DB-API rows and
    ``cursor.description`` tuples.
    """

    def __init__(self, date_columns: set[str] | None = None) -> None:
        # Explicit set of column names (upper-cased) that contain SAS date
        # values encoded as floats (days-since-1960).  Overrides the FORMAT
        # metadata; when ``None`` and no FORMAT metadata is available, the
        # name-suffix heuristic is used as fallback.
        self.date_columns: set[str] | None = (
            {c.upper() for c in date_columns} if date_columns is not None else None
        )

    def column_kind(self, name: str, sas_type: str = "d", fmt: str | bytes | None = None) -> _ColumnKind:
        """
        Return the kind of the column *name* from its SAS metadata.

        A character column is character.  A numeric one is a date when
        :attr:`date_columns` lists it, else what its FORMAT selects (width
        and decimals ignored, so ``"DATE9."``, ``"YYMMDD10."`` and
        ``"DATETIME21.2"`` classify by their name), else, only when no
        FORMAT metadata is available at all, a date when its name ends in
        one of the date suffixes.  Anything else is numeric, TIME formats
        included (seconds since midnight).

        :param name: Column name, any case.
        :param sas_type: ``"s"`` (character) or ``"d"`` (numeric), as in
            ``SAS7BDATReader.column_types``.
        :param fmt: The column's SAS FORMAT; ``""`` when the file has FORMAT
            metadata and this column has none, ``None`` when there is no
            FORMAT metadata at all.
        """
        if sas_type == "s":
            return _CHARACTER
        key = name.upper()
        if self.date_columns is not None and key in self.date_columns:
            return _DATE
        if fmt is not None:
            if isinstance(fmt, bytes):
                fmt = fmt.decode(errors="replace")
            format_name = _FORMAT_WIDTH_RE.sub("", fmt.strip().upper())
            return next(
                (kind for kind in _FORMATTED_KINDS if kind.matches(format_name)),
                _NUMERIC,
            )
        if self.date_columns is None and key.endswith(_DATE.NAME_SUFFIXES):
            return _DATE
        return _NUMERIC

    # -----------------------------------------------------------------------
    # DataFrame typing
    # -----------------------------------------------------------------------
    def normalize_dataframe(
        self,
        df: DataFrame,
        column_types: Mapping[str, str | bytes] | None = None,
        column_formats: Mapping[str, str | bytes | None] | None = None,
        dtype_backend: str | None = None,
    ) -> DataFrame:
        """
        Give a raw ``read_sas`` DataFrame one stable dtype per SAS column kind.

        The dtype depends only on the column metadata, never on the
        values, so the same table yields the same schema on every run,
        an all-missing column keeps its type, and any slice of the
        result (see ``SASHook.get_df_by_chunks``) has the same dtypes
        as the whole.  Writing the result with ``DataFrame.to_parquet``
        therefore gives a stable Parquet schema without any casting in
        the caller.

        ==================  ======================  =========================
        SAS column          pandas dtype            Parquet type
        ==================  ======================  =========================
        character           ``string``              string
        numeric             ``float64``             double
        numeric, DATE fmt   ``date32[pyarrow]``     date32
        numeric, DATETIME   ``datetime64[us]``      timestamp
        ==================  ======================  =========================

        Character columns are right-stripped (SAS pads to the declared
        length) and a padded SAS missing indicator (``"       ."``, a
        dot preceded by whitespace) becomes missing.  A bare ``"."`` is a
        legitimate value and is kept.  A blank character value is the SAS
        missing value and arrives from ``pandas.read_sas`` as missing
        already (``blank_missing=True``).  Missing numeric values are ``NaN``,
        missing dates ``NA``, missing datetimes ``NaT``.

        The kind of each column comes from :meth:`column_kind`, which also
        says how a numeric column is recognized as a date or datetime.

        :param df: DataFrame as returned by ``pandas.read_sas``.
        :param column_types: SAS column type per column name, ``"s"``
            (character) or ``"d"`` (numeric), as exposed by
            ``SAS7BDATReader.column_types``.  Inferred from the dtype
            when omitted.
        :param column_formats: SAS FORMAT per column name, as exposed by
            ``SAS7BDATReader.column_formats``.  ``None`` means "no
            metadata available" and enables the name-suffix heuristic.
        :param dtype_backend: ``None`` for the dtypes in the table above,
            ``"pyarrow"`` for their Arrow-backed twins (``string[pyarrow]``,
            ``double[pyarrow]``, ``timestamp[us][pyarrow]``) or
            ``"numpy_nullable"`` (``Float64`` for numeric columns).  Chosen here so
            every column is converted exactly once.
        """
        from pandas.api.types import is_string_dtype

        backend = _DtypeBackend.named(dtype_backend)

        types = {
            str(name).upper(): (ctype.decode() if isinstance(ctype, bytes) else str(ctype)).lower()
            for name, ctype in (column_types or {}).items()
        }
        formats = (
            None
            if column_formats is None
            else {str(name).upper(): fmt for name, fmt in column_formats.items()}
        )

        for col in df.columns:
            key = str(col).upper()
            sas_type = types.get(key) or ("s" if is_string_dtype(df[col]) else "d")
            # "" (metadata, no FORMAT) is not None (no metadata at all).
            fmt = None if formats is None else formats.get(key, "")
            df[col] = self.column_kind(key, sas_type, fmt).convert(df[col], backend)
        return df

    @staticmethod
    def _reader(file: Any, encoding: str, chunksize: int | None = None) -> SAS7BDATReader:
        """
        Open the pandas reader of a ``.sas7bdat`` with the dates left as SAS numbers.

        ``pandas.read_sas`` always converts DATE and DATETIME columns itself,
        DATETIME at millisecond precision; with ``convert_dates=False`` the
        numbers reach the column kinds, which keep microseconds.
        """
        from pandas.io.sas.sas7bdat import SAS7BDATReader

        return SAS7BDATReader(file, convert_dates=False, encoding=encoding, chunksize=chunksize)

    def parse_sas7bdat(self, file: Any, encoding: str, dtype_backend: str | None = None) -> DataFrame:
        """
        Read a ``.sas7bdat`` from a file object into a typed DataFrame.

        The pandas reader is opened directly so that the column type and
        FORMAT metadata stored in the file are available; they drive
        :meth:`normalize_dataframe`.  *file* only needs to be a synchronous,
        forward-readable file object (an SFTP handle, a local file, a
        ``BytesIO``).
        """
        # The reader is a context manager: leaving the block closes it.
        with self._reader(file, encoding) as reader:
            df = reader.read()
            column_types, column_formats = self._column_metadata(reader)
        return self.normalize_dataframe(
            df,
            column_types=column_types,
            column_formats=column_formats,
            dtype_backend=dtype_backend,
        )

    def parse_sas7bdat_chunks(
        self,
        file: Any,
        encoding: str,
        chunksize: int,
        dtype_backend: str | None = None,
    ) -> Generator[DataFrame, None, None]:
        """
        Read a ``.sas7bdat`` from a file object in typed chunks of *chunksize* rows.

        The reader parses the file page by page as it is read, so only one
        chunk is held in memory.  Every chunk is typed from the same file
        metadata, so all chunks carry the same dtypes, an all-missing chunk
        included, and each chunk is indexed from 0.
        """
        with self._reader(file, encoding, chunksize=chunksize) as reader:
            column_types, column_formats = self._column_metadata(reader)
            for chunk in reader:
                yield self.normalize_dataframe(
                    chunk.reset_index(drop=True),
                    column_types=column_types,
                    column_formats=column_formats,
                    dtype_backend=dtype_backend,
                )

    @staticmethod
    def _column_metadata(reader: Any) -> tuple[dict[str, Any], dict[str, Any]]:
        """Return the SAS type and FORMAT of every column, by column name."""
        # ``column_types`` is a method on pandas' SAS7BDATReader,
        # ``column_formats`` a plain list attribute.
        column_types = reader.column_types
        if callable(column_types):
            column_types = column_types()
        return (
            dict(zip(reader.column_names, column_types, strict=False)),
            dict(zip(reader.column_names, reader.column_formats, strict=False)),
        )

    @staticmethod
    def to_records(df: DataFrame) -> list[tuple[Any, ...]]:
        """
        Convert a typed DataFrame into DB-API rows.

        Plain Python values, ``None`` for every kind of missing (``NaN``,
        ``NA``, ``NaT``), ``datetime.datetime`` for datetimes and
        ``datetime.date`` for dates, so the rows serialise as JSON (XCom,
        deferred mode).  The conversion is done per column, once, and the
        rows are assembled with ``zip``: no Python call per cell.
        """
        import pandas as pd
        from pandas.api.types import is_datetime64_any_dtype

        columns: list[list[Any]] = []
        for col in df.columns:
            series = df[col]
            dtype = series.dtype
            # pandas reports Arrow date32 as a datetime kind too, so Arrow
            # dtypes are inspected on their pyarrow type: only timestamps
            # become datetimes, dates stay ``datetime.date``.
            if isinstance(dtype, pd.ArrowDtype):
                is_timestamp = str(dtype.pyarrow_dtype).startswith("timestamp")
            else:
                is_timestamp = is_datetime64_any_dtype(dtype)
            if is_timestamp:
                stamps = series.astype("datetime64[us]").dt.to_pydatetime()
                columns.append([None if ts is pd.NaT else ts for ts in stamps])
            else:
                columns.append(series.astype(object).where(series.notna(), None).tolist())
        return list(zip(*columns, strict=False)) if columns else []

    @staticmethod
    def build_description(
        df: DataFrame,
    ) -> list[tuple[str, str, None, None, None, None, None]]:
        """Build DB-API 2.0 ``cursor.description`` tuples from DataFrame columns."""
        return [(str(col), str(df[col].dtype), None, None, None, None, None) for col in df.columns]
