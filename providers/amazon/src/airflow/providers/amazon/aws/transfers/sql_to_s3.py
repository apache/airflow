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
from __future__ import annotations

import enum
import gzip
import importlib.util
import io
import warnings
from collections import namedtuple
from collections.abc import Iterable, Mapping, Sequence
from typing import TYPE_CHECKING, Any, Literal, cast

from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.common.compat.sdk import (
    AirflowException,
    AirflowOptionalProviderFeatureException,
    BaseHook,
    BaseOperator,
)

if TYPE_CHECKING:
    import jinja2
    import pandas as pd
    import polars as pl

    from airflow.providers.common.sql.hooks.sql import DbApiHook
    from airflow.sdk import Context


class FILE_FORMAT(enum.Enum):
    """Possible file formats."""

    CSV = enum.auto()
    JSON = enum.auto()
    PARQUET = enum.auto()


FileOptions = namedtuple("FileOptions", ["mode", "suffix", "function", "polars_function"])

FILE_OPTIONS_MAP = {
    FILE_FORMAT.CSV: FileOptions("r+", ".csv", "to_csv", "write_csv"),
    FILE_FORMAT.JSON: FileOptions("r+", ".json", "to_json", "write_json"),
    FILE_FORMAT.PARQUET: FileOptions("rb+", ".parquet", "to_parquet", "write_parquet"),
}


class SqlToS3Operator(BaseOperator):
    """
    Saves data from a specific SQL query into a file in S3.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:SqlToS3Operator`

    :param query: the sql query to be executed. If you want to execute a file, place the absolute path of it,
        ending with .sql extension. (templated)
    :param s3_bucket: bucket where the data will be stored. (templated)
    :param s3_key: desired key for the file. It includes the name of the file. (templated)
    :param replace: whether or not to replace the file in S3 if it previously existed
    :param sql_conn_id: reference to a specific database.
    :param sql_hook_params: Extra config params to be passed to the underlying hook.
        Should match the desired hook constructor params.
    :param parameters: (optional) the parameters to render the SQL query with.
    :param read_kwargs: arguments to include in DataFrame when reading from SQL (supports both pandas and polars).
    :param df_type: the type of DataFrame to use ('pandas' or 'polars'). Defaults to 'pandas'.
    :param aws_conn_id: reference to a specific S3 connection
    :param verify: Whether or not to verify SSL certificates for S3 connection.
        By default SSL certificates are verified.
        You can provide the following values:

        - ``False``: do not validate SSL certificates. SSL will still be used
                (unless use_ssl is False), but SSL certificates will not be verified.
        - ``path/to/cert/bundle.pem``: A filename of the CA cert bundle to uses.
                You can specify this argument if you want to use a different
                CA cert bundle than the one used by botocore.
    :param file_format: the destination file format, only string 'csv', 'json' or 'parquet' is accepted.
    :param max_rows_per_file: (optional) argument to set destination file number of rows limit, if source data
        is larger than that, it will be dispatched into multiple files.
        Will be ignored if ``groupby_kwargs`` argument is specified.
    :param df_kwargs: arguments to include in DataFrame ``.to_parquet()``, ``.to_json()`` or ``.to_csv()``,
        or in ``.write_parquet()``, ``.write_json()`` or ``.write_csv()`` when ``df_type`` is ``'polars'``.
    :param groupby_kwargs: argument to include in DataFrame ``groupby()``.
    :param page_size: (optional) number of rows per page. The query is then paged the way
        ``GenericTransfer`` pages it, one query per page through ``paginated_sql_statement_clause``,
        and every page is written as a file of its own (``<key>_0``, ``<key>_1``, ...), so only one
        page is held in memory at a time. Cannot be combined with ``max_rows_per_file`` or
        ``groupby_kwargs``.
    :param paginated_sql_statement_clause: SQL statement clause used for pagination, formatted with the
        query, the page size and the offset (default ``"{} LIMIT {} OFFSET {}"``). Only used with
        ``page_size``.
    """

    template_fields: Sequence[str] = (
        "s3_bucket",
        "s3_key",
        "query",
        "sql_conn_id",
        "aws_conn_id",
        "page_size",
        "paginated_sql_statement_clause",
    )
    template_ext: Sequence[str] = (".sql",)
    template_fields_renderers = {
        "query": "sql",
        "df_kwargs": "json",
        "pd_kwargs": "json",
        "read_kwargs": "json",
    }

    def __init__(
        self,
        *,
        query: str,
        s3_bucket: str,
        s3_key: str,
        sql_conn_id: str,
        sql_hook_params: dict | None = None,
        parameters: None | Mapping[str, Any] | list | tuple = None,
        read_kwargs: dict | None = None,
        read_pd_kwargs: dict | None = None,
        df_type: Literal["pandas", "polars"] = "pandas",
        replace: bool = False,
        aws_conn_id: str | None = "aws_default",
        verify: bool | str | None = None,
        file_format: Literal["csv", "json", "parquet"] = "csv",
        max_rows_per_file: int = 0,
        df_kwargs: dict | None = None,
        pd_kwargs: dict | None = None,
        groupby_kwargs: dict | None = None,
        page_size: int | None = None,
        paginated_sql_statement_clause: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.query = query
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.sql_conn_id = sql_conn_id
        self.aws_conn_id = aws_conn_id
        self.verify = verify
        self.replace = replace
        self.parameters = parameters
        self.max_rows_per_file = max_rows_per_file
        self.groupby_kwargs = groupby_kwargs or {}
        self.sql_hook_params = sql_hook_params
        self.df_type = df_type
        self.page_size = page_size
        self.paginated_sql_statement_clause = paginated_sql_statement_clause or "{} LIMIT {} OFFSET {}"

        if read_pd_kwargs is not None:
            warnings.warn(
                "The 'read_pd_kwargs' parameter is deprecated. Use 'read_kwargs' instead.",
                AirflowProviderDeprecationWarning,
                stacklevel=2,
            )
        self.read_kwargs = read_kwargs if read_kwargs is not None else read_pd_kwargs or {}

        if pd_kwargs is not None:
            warnings.warn(
                "The 'pd_kwargs' parameter is deprecated. Use 'df_kwargs' instead.",
                AirflowProviderDeprecationWarning,
                stacklevel=2,
            )

        self.df_kwargs = df_kwargs if df_kwargs is not None else pd_kwargs or {}

        if "path_or_buf" in self.df_kwargs:
            raise AirflowException("The argument path_or_buf is not allowed, please remove it")

        if self.max_rows_per_file and self.groupby_kwargs:
            raise AirflowException(
                "SqlToS3Operator arguments max_rows_per_file and groupby_kwargs "
                "can not be both specified. Please choose one."
            )

        if self.page_size is not None and (self.max_rows_per_file or self.groupby_kwargs):
            raise ValueError(
                "SqlToS3Operator argument page_size can not be combined with "
                "max_rows_per_file or groupby_kwargs: every page is already written as a file of its own."
            )

        try:
            self.file_format = FILE_FORMAT[file_format.upper()]
        except KeyError:
            raise AirflowException(f"The argument file_format doesn't support {file_format} value.")

    @staticmethod
    def _fix_dtypes(df: pd.DataFrame, file_format: FILE_FORMAT) -> None:
        """
        Mutate DataFrame to set dtypes for float columns containing NaN values.

        Set dtype of object to str to allow for downstream transformations.
        """
        try:
            import numpy as np
            import pandas as pd
        except ImportError as e:
            raise AirflowOptionalProviderFeatureException(e)

        for col in df:
            if df[col].dtype.name == "object" and file_format == FILE_FORMAT.PARQUET:
                # if the type wasn't identified or converted, change it to a string so if can still be
                # processed.
                df[col] = cast("pd.Series", df[col].astype(str))  # type: ignore[call-overload]

            if "float" in df[col].dtype.name and df[col].hasnans:
                # inspect values to determine if dtype of non-null values is int or float
                notna_series: Any = df[col].dropna().values
                if np.equal(notna_series, notna_series.astype(int)).all():
                    # set to dtype that retains integers and supports NaNs
                    # The type ignore can be removed here if https://github.com/numpy/numpy/pull/23690
                    # is merged and released as currently NumPy does not consider None as valid for x/y.
                    df[col] = np.where(df[col].isnull(), None, df[col])  # type: ignore[call-overload]
                    df[col] = cast("pd.Series", df[col].astype(pd.Int64Dtype()))  # type: ignore[call-overload]
                elif np.isclose(notna_series, notna_series.astype(int)).all():
                    # set to float dtype that retains floats and supports NaNs
                    # The type ignore can be removed here if https://github.com/numpy/numpy/pull/23690
                    # is merged and released
                    df[col] = np.where(df[col].isnull(), None, df[col])  # type: ignore[call-overload]
                    df[col] = cast("pd.Series", df[col].astype(pd.Float64Dtype()))  # type: ignore[call-overload]

    @staticmethod
    def _strip_suffixes(
        path: str,
    ) -> str:
        suffixes = [".json.gz", ".csv.gz", ".json", ".csv", ".parquet"]
        for suffix in sorted(suffixes, key=len, reverse=True):
            if path.endswith(suffix):
                return path[: -len(suffix)]
        return path

    def _ensure_parquet_engine(self) -> None:
        if self.df_type != "pandas":
            return
        engines: tuple[str, ...]
        if ("dtype_backend", "pyarrow") in self.read_kwargs.items():
            # pandas builds the frame through pyarrow itself here, so fastparquet cannot stand in
            # for it even when the output format is Parquet.
            trigger, engines = 'read_kwargs={"dtype_backend": "pyarrow"}', ("pyarrow",)
        elif self.file_format == FILE_FORMAT.PARQUET:
            trigger, engines = 'file_format="parquet"', ("pyarrow", "fastparquet")
        else:
            return
        if any(importlib.util.find_spec(engine) for engine in engines):
            return

        raise AirflowOptionalProviderFeatureException(
            f"{trigger} needs {' or '.join(engines)}. Install apache-airflow-providers-amazon[parquet]."
        )

    def get_paginated_sql(self, offset: int) -> str:
        """Format the paginated SQL statement using the current format."""
        return self.paginated_sql_statement_clause.format(self.query, self.page_size, offset)

    def render_template_fields(
        self,
        context: Context,
        jinja_env: jinja2.Environment | None = None,
    ) -> None:
        super().render_template_fields(context=context, jinja_env=jinja_env)
        # A templated page_size renders as a string; the paging decision needs the number.
        if self.page_size is not None:
            self.page_size = int(self.page_size)
            if self.page_size < 0:
                raise ValueError(f"SqlToS3Operator argument page_size can not be negative: {self.page_size}")

    def execute(self, context: Context) -> None:
        self._ensure_parquet_engine()
        sql_hook = self._get_hook()
        s3_conn = S3Hook(aws_conn_id=self.aws_conn_id, verify=self.verify)
        pages = (
            self._pages(sql_hook)
            if self.page_size
            else self._partition_dataframe(df=self._read_dataframe(sql_hook, self.query))
        )
        for suffix, df in pages:
            self._upload_dataframe(s3_conn, df, suffix)
            del df  # release the page before the next one is read; the loop variable would hold it

    def _pages(self, sql_hook: DbApiHook) -> Iterable[tuple[str, pd.DataFrame | pl.DataFrame]]:
        """
        Read the query one page at a time, each page keyed by its number.

        The first page is always yielded, so the key exists even for an empty result; a later empty
        page only tells that the previous one was the last. A page is released before the next is read.
        """
        if TYPE_CHECKING:
            assert self.page_size is not None  # execute() pages only when page_size is set
        offset = 0
        page = 0
        while True:
            paginated_sql = self.get_paginated_sql(offset)
            self.log.info("Executing: \n %s", paginated_sql)
            df = self._read_dataframe(sql_hook, paginated_sql)
            if page == 0 or len(df):
                yield str(page), df
            if len(df) < self.page_size:
                self.log.info("Last page read; ending transfer after %d page(s).", page + 1)
                return
            del df
            offset += self.page_size
            page += 1

    def _read_dataframe(self, sql_hook: DbApiHook, sql: str) -> pd.DataFrame | pl.DataFrame:
        """Run *sql* through the hook's DataFrame API and apply the pandas dtype fixes."""
        df = sql_hook.get_df(sql=sql, parameters=self.parameters, df_type=self.df_type, **self.read_kwargs)
        self.log.info("Data from SQL obtained")
        # Only apply dtype fixes to pandas DataFrames since Polars doesn't have the same NaN/None inconsistencies as panda
        if ("dtype_backend", "pyarrow") not in self.read_kwargs.items() and self.df_type == "pandas":
            self._fix_dtypes(df, self.file_format)  # type: ignore[arg-type]
        return df

    def _upload_dataframe(self, s3_conn: S3Hook, df: pd.DataFrame | pl.DataFrame, suffix: str) -> None:
        """Write *df* in the configured file format and upload it as ``<key>_<suffix>``."""
        file_options = FILE_OPTIONS_MAP[self.file_format]
        write_function = file_options.polars_function if self.df_type == "polars" else file_options.function
        buf = io.BytesIO()
        self.log.info("Writing data to in-memory buffer")
        clean_key = self._strip_suffixes(self.s3_key)
        object_key = (
            f"{clean_key}_{suffix}{file_options.suffix}" if suffix else f"{clean_key}{file_options.suffix}"
        )

        if self.file_format != FILE_FORMAT.PARQUET and self.df_kwargs.get("compression") == "gzip":
            object_key += ".gz"
            df_kwargs = {k: v for k, v in self.df_kwargs.items() if k != "compression"}
            with gzip.GzipFile(fileobj=buf, mode="wb", filename=object_key) as gz:
                getattr(df, write_function)(gz, **df_kwargs)
        else:
            if self.file_format == FILE_FORMAT.PARQUET:
                getattr(df, write_function)(buf, **self.df_kwargs)
            else:
                text_buf = io.TextIOWrapper(buf, encoding="utf-8", write_through=True)
                getattr(df, write_function)(text_buf, **self.df_kwargs)
                text_buf.flush()

        buf.seek(0)

        self.log.info("Uploading data to S3")
        s3_conn.load_file_obj(file_obj=buf, key=object_key, bucket_name=self.s3_bucket, replace=self.replace)

    def _partition_dataframe(
        self, df: pd.DataFrame | pl.DataFrame
    ) -> Iterable[tuple[str, pd.DataFrame | pl.DataFrame]]:
        """Partition dataframe using pandas or polars groupby() method."""
        try:
            import secrets
            import string

            import numpy as np
            import pandas as pd
            import polars as pl
        except ImportError:
            pass

        # if max_rows_per_file argument is specified, a temporary column with a random unusual name will be
        # added to the dataframe. This column is used to dispatch the dataframe into smaller ones using groupby()
        random_column_name = None
        if self.max_rows_per_file and not self.groupby_kwargs:
            random_column_name = "".join(secrets.choice(string.ascii_letters) for _ in range(20))
            self.groupby_kwargs = {"by": random_column_name}

        if random_column_name:
            if isinstance(df, pd.DataFrame):
                df[random_column_name] = np.arange(len(df)) // self.max_rows_per_file
            elif isinstance(df, pl.DataFrame):
                df = df.with_columns(
                    (pl.int_range(pl.len()) // self.max_rows_per_file).alias(random_column_name)
                )

        if not self.groupby_kwargs:
            yield "", df
            return

        if isinstance(df, pd.DataFrame):
            for group_label in (grouped_df := df.groupby(**self.groupby_kwargs)).groups:
                group_df = grouped_df.get_group(group_label)
                if random_column_name:
                    group_df = group_df.drop(random_column_name, axis=1, errors="ignore")
                yield (
                    cast("str", group_label[0] if isinstance(group_label, tuple) else group_label),
                    group_df.reset_index(drop=True),
                )
        elif isinstance(df, pl.DataFrame):
            for group_label, group_df_in in df.group_by(**self.groupby_kwargs):  # type: ignore[assignment]
                group_df2 = group_df_in.drop(random_column_name) if random_column_name else group_df_in
                yield (
                    cast("str", group_label[0] if isinstance(group_label, tuple) else group_label),
                    group_df2,
                )

    def _get_hook(self) -> DbApiHook:
        self.log.debug("Get connection for %s", self.sql_conn_id)
        conn = BaseHook.get_connection(self.sql_conn_id)
        hook = conn.get_hook(hook_params=self.sql_hook_params)
        if not callable(getattr(hook, "get_df", None)):
            raise AirflowException("This hook is not supported. The hook class must have get_df method.")
        return hook
