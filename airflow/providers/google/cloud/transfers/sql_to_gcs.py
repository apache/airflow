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
"""Base operator for SQL to GCS operators."""
from __future__ import annotations

import abc
import json
import os
from tempfile import NamedTemporaryFile
from typing import TYPE_CHECKING, Sequence

import pyarrow as pa
import pyarrow.parquet as pq
import unicodecsv as csv

from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class BaseSQLToGCSOperator(BaseOperator):
    """
    Copy data from SQL to Google Cloud Storage in JSON, CSV, or Parquet format.

    :param sql: The SQL to execute.
    :param bucket: The bucket to upload to.
    :param filename: The filename to use as the object name when uploading
        to Google Cloud Storage. A ``{}`` should be specified in the filename
        to allow the operator to inject file numbers in cases where the
        file is split due to size.
    :param schema_filename: If set, the filename to use as the object name
        when uploading a .json file containing the BigQuery schema fields
        for the table that was dumped from the database.
    :param approx_max_file_size_bytes: This operator supports the ability
        to split large table dumps into multiple files (see notes in the
        filename param docs above). This param allows developers to specify the
        file size of the splits. Check https://cloud.google.com/storage/quotas
        to see the maximum allowed file size for a single object.
    :param export_format: Desired format of files to be exported. (json, csv or parquet)
    :param stringify_dict: Whether to dump Dictionary type objects
        (such as JSON columns) as a string. Applies only to CSV/JSON export format.
    :param field_delimiter: The delimiter to be used for CSV files.
    :param null_marker: The null marker to be used for CSV files.
    :param gzip: Option to compress file for upload (does not apply to schemas).
    :param schema: The schema to use, if any. Should be a list of dict or
        a str. Pass a string if using Jinja template, otherwise, pass a list of
        dict. Examples could be seen: https://cloud.google.com/bigquery/docs
        /schemas#specifying_a_json_schema_file
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :param parameters: a parameters dict that is substituted at query runtime.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :param upload_metadata: whether to upload the row count metadata as blob metadata
    :param exclude_columns: set of columns to exclude from transmission
    :param partition_columns: list of columns to use for file partitioning. In order to use
        this parameter, you must sort your dataset by partition_columns. Do this by
        passing an ORDER BY clause to the sql query. Files are uploaded to GCS as objects
        with a hive style partitioning directory structure (templated).
    :param write_on_empty: Optional parameter to specify whether to write a file if the
        export does not return any rows. Default is False so we will not write a file
        if the export returns no rows.
    """

    template_fields: Sequence[str] = (
        "sql",
        "bucket",
        "filename",
        "schema_filename",
        "schema",
        "parameters",
        "impersonation_chain",
        "partition_columns",
    )
    template_ext: Sequence[str] = (".sql",)
    template_fields_renderers = {"sql": "sql"}
    ui_color = "#a0e08c"

    def __init__(
        self,
        *,
        sql: str,
        bucket: str,
        filename: str,
        schema_filename: str | None = None,
        approx_max_file_size_bytes: int = 1900000000,
        export_format: str = "json",
        stringify_dict: bool = False,
        field_delimiter: str = ",",
        null_marker: str | None = None,
        gzip: bool = False,
        schema: str | list | None = None,
        parameters: dict | None = None,
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: str | None = None,
        impersonation_chain: str | Sequence[str] | None = None,
        upload_metadata: bool = False,
        exclude_columns: set | None = None,
        partition_columns: list | None = None,
        write_on_empty: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        if exclude_columns is None:
            exclude_columns = set()

        self.sql = sql
        self.bucket = bucket
        self.filename = filename
        self.schema_filename = schema_filename
        self.approx_max_file_size_bytes = approx_max_file_size_bytes
        self.export_format = export_format.lower()
        self.stringify_dict = stringify_dict
        self.field_delimiter = field_delimiter
        self.null_marker = null_marker
        self.gzip = gzip
        self.schema = schema
        self.parameters = parameters
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain
        self.upload_metadata = upload_metadata
        self.exclude_columns = exclude_columns
        self.partition_columns = partition_columns
        self.write_on_empty = write_on_empty

    def execute(self, context: Context):
        if self.partition_columns:
            self.log.info(
                f"Found partition columns: {','.join(self.partition_columns)}. "
                "Assuming the SQL statement is properly sorted by these columns in "
                "ascending or descending order."
            )

        self.log.info("Executing query")
        cursor = self.query()

        # If a schema is set, create a BQ schema JSON file.
        if self.schema_filename:
            self.log.info("Writing local schema file")
            schema_file = self._write_local_schema_file(cursor)

            # Flush file before uploading
            schema_file["file_handle"].flush()

            self.log.info("Uploading schema file to GCS.")
            self._upload_to_gcs(schema_file)
            schema_file["file_handle"].close()

        counter = 0
        files = []
        total_row_count = 0
        total_files = 0
        self.log.info("Writing local data files")
        for file_to_upload in self._write_local_data_files(cursor):

            # Flush file before uploading
            file_to_upload["file_handle"].flush()

            self.log.info("Uploading chunk file #%d to GCS.", counter)
            self._upload_to_gcs(file_to_upload)

            self.log.info("Removing local file")
            file_to_upload["file_handle"].close()

            # Metadata to be outputted to Xcom
            total_row_count += file_to_upload["file_row_count"]
            total_files += 1
            files.append(
                {
                    "file_name": file_to_upload["file_name"],
                    "file_mime_type": file_to_upload["file_mime_type"],
                    "file_row_count": file_to_upload["file_row_count"],
                }
            )

            counter += 1

        file_meta = {
            "bucket": self.bucket,
            "total_row_count": total_row_count,
            "total_files": total_files,
            "files": files,
        }

        return file_meta

    def convert_types(self, schema, col_type_dict, row) -> list:
        """Convert values from DBAPI to output-friendly formats."""
        return [
            self.convert_type(value, col_type_dict.get(name), stringify_dict=self.stringify_dict)
            for name, value in zip(schema, row)
        ]

    def _write_local_data_files(self, cursor):
        """
        Takes a cursor, and writes results to a local file.

        :return: A dictionary where keys are filenames to be used as object
            names in GCS, and values are file handles to local files that
            contain the data for the GCS objects.
        """
        org_schema = list(map(lambda schema_tuple: schema_tuple[0], cursor.description))
        schema = [column for column in org_schema if column not in self.exclude_columns]

        col_type_dict = self._get_col_type_dict()
        file_no = 0
        file_mime_type = self._get_file_mime_type()
        file_to_upload, tmp_file_handle = self._get_file_to_upload(file_mime_type, file_no)

        if self.export_format == "csv":
            csv_writer = self._configure_csv_file(tmp_file_handle, schema)
        if self.export_format == "parquet":
            parquet_schema = self._convert_parquet_schema(cursor)
            parquet_writer = self._configure_parquet_file(tmp_file_handle, parquet_schema)

        prev_partition_values = None
        curr_partition_values = None
        for row in cursor:
            if self.partition_columns:
                row_dict = dict(zip(schema, row))
                curr_partition_values = tuple(
                    [row_dict.get(partition_column, "") for partition_column in self.partition_columns]
                )

                if prev_partition_values is None:
                    # We haven't set prev_partition_values before. Set to current
                    prev_partition_values = curr_partition_values

                elif prev_partition_values != curr_partition_values:
                    # If the partition values differ, write the current local file out
                    # Yield first before we write the current record
                    file_no += 1

                    if self.export_format == "parquet":
                        parquet_writer.close()

                    file_to_upload["partition_values"] = prev_partition_values
                    yield file_to_upload
                    file_to_upload, tmp_file_handle = self._get_file_to_upload(file_mime_type, file_no)
                    if self.export_format == "csv":
                        csv_writer = self._configure_csv_file(tmp_file_handle, schema)
                    if self.export_format == "parquet":
                        parquet_writer = self._configure_parquet_file(tmp_file_handle, parquet_schema)

                    # Reset previous to current after writing out the file
                    prev_partition_values = curr_partition_values

            # Incrementing file_row_count after partition yield ensures all rows are written
            file_to_upload["file_row_count"] += 1

            # Proceed to write the row to the localfile
            if self.export_format == "csv":
                row = self.convert_types(schema, col_type_dict, row)
                if self.null_marker is not None:
                    row = [value if value is not None else self.null_marker for value in row]
                csv_writer.writerow(row)
            elif self.export_format == "parquet":
                row = self.convert_types(schema, col_type_dict, row)
                if self.null_marker is not None:
                    row = [value if value is not None else self.null_marker for value in row]
                row_pydic = {col: [value] for col, value in zip(schema, row)}
                tbl = pa.Table.from_pydict(row_pydic, parquet_schema)
                parquet_writer.write_table(tbl)
            else:
                row = self.convert_types(schema, col_type_dict, row)
                row_dict = dict(zip(schema, row))

                tmp_file_handle.write(
                    json.dumps(row_dict, sort_keys=True, ensure_ascii=False).encode("utf-8")
                )

                # Append newline to make dumps BigQuery compatible.
                tmp_file_handle.write(b"\n")

            # Stop if the file exceeds the file size limit.
            fppos = tmp_file_handle.tell()
            tmp_file_handle.seek(0, os.SEEK_END)
            file_size = tmp_file_handle.tell()
            tmp_file_handle.seek(fppos, os.SEEK_SET)

            if file_size >= self.approx_max_file_size_bytes:
                file_no += 1

                if self.export_format == "parquet":
                    parquet_writer.close()

                file_to_upload["partition_values"] = curr_partition_values
                yield file_to_upload
                file_to_upload, tmp_file_handle = self._get_file_to_upload(file_mime_type, file_no)
                if self.export_format == "csv":
                    csv_writer = self._configure_csv_file(tmp_file_handle, schema)
                if self.export_format == "parquet":
                    parquet_writer = self._configure_parquet_file(tmp_file_handle, parquet_schema)

        if self.export_format == "parquet":
            parquet_writer.close()
        # Last file may have 0 rows, don't yield if empty
        # However, if it is the first file and self.write_on_empty is True, then yield to write an empty file
        if file_to_upload["file_row_count"] > 0 or (file_no == 0 and self.write_on_empty):
            file_to_upload["partition_values"] = curr_partition_values
            yield file_to_upload

    def _get_file_to_upload(self, file_mime_type, file_no):
        """Returns a dictionary that represents the file to upload"""
        tmp_file_handle = NamedTemporaryFile(delete=True)
        return (
            {
                "file_name": self.filename.format(file_no),
                "file_handle": tmp_file_handle,
                "file_mime_type": file_mime_type,
                "file_row_count": 0,
            },
            tmp_file_handle,
        )

    def _get_file_mime_type(self):
        if self.export_format == "csv":
            file_mime_type = "text/csv"
        elif self.export_format == "parquet":
            file_mime_type = "application/octet-stream"
        else:
            file_mime_type = "application/json"
        return file_mime_type

    def _configure_csv_file(self, file_handle, schema):
        """Configure a csv writer with the file_handle and write schema
        as headers for the new file.
        """
        csv_writer = csv.writer(file_handle, encoding="utf-8", delimiter=self.field_delimiter)
        csv_writer.writerow(schema)
        return csv_writer

    def _configure_parquet_file(self, file_handle, parquet_schema):
        parquet_writer = pq.ParquetWriter(file_handle.name, parquet_schema)
        return parquet_writer

    def _convert_parquet_schema(self, cursor):
        type_map = {
            "INTEGER": pa.int64(),
            "FLOAT": pa.float64(),
            "NUMERIC": pa.float64(),
            "BIGNUMERIC": pa.float64(),
            "BOOL": pa.bool_(),
            "STRING": pa.string(),
            "BYTES": pa.binary(),
            "DATE": pa.date32(),
            "DATETIME": pa.date64(),
            "TIMESTAMP": pa.timestamp("s"),
        }

        columns = [field[0] for field in cursor.description]
        bq_fields = [self.field_to_bigquery(field) for field in cursor.description]
        bq_types = [bq_field.get("type") if bq_field is not None else None for bq_field in bq_fields]
        pq_types = [type_map.get(bq_type, pa.string()) for bq_type in bq_types]
        parquet_schema = pa.schema(zip(columns, pq_types))
        return parquet_schema

    @abc.abstractmethod
    def query(self):
        """Execute DBAPI query."""

    @abc.abstractmethod
    def field_to_bigquery(self, field) -> dict[str, str]:
        """Convert a DBAPI field to BigQuery schema format."""

    @abc.abstractmethod
    def convert_type(self, value, schema_type, **kwargs):
        """Convert a value from DBAPI to output-friendly formats."""

    def _get_col_type_dict(self):
        """Return a dict of column name and column type based on self.schema if not None."""
        schema = []
        if isinstance(self.schema, str):
            schema = json.loads(self.schema)
        elif isinstance(self.schema, list):
            schema = self.schema
        elif self.schema is not None:
            self.log.warning("Using default schema due to unexpected type. Should be a string or list.")

        col_type_dict = {}
        try:
            col_type_dict = {col["name"]: col["type"] for col in schema}
        except KeyError:
            self.log.warning(
                "Using default schema due to missing name or type. Please "
                "refer to: https://cloud.google.com/bigquery/docs/schemas"
                "#specifying_a_json_schema_file"
            )
        return col_type_dict

    def _write_local_schema_file(self, cursor):
        """
        Takes a cursor, and writes the BigQuery schema for the results to a
        local file system. Schema for database will be read from cursor if
        not specified.

        :return: A dictionary where key is a filename to be used as an object
            name in GCS, and values are file handles to local files that
            contains the BigQuery schema fields in .json format.
        """
        if self.schema:
            self.log.info("Using user schema")
            schema = self.schema
        else:
            self.log.info("Starts generating schema")
            schema = [
                self.field_to_bigquery(field)
                for field in cursor.description
                if field[0] not in self.exclude_columns
            ]

        if isinstance(schema, list):
            schema = json.dumps(schema, sort_keys=True)

        self.log.info("Using schema for %s", self.schema_filename)
        self.log.debug("Current schema: %s", schema)

        tmp_schema_file_handle = NamedTemporaryFile(delete=True)
        tmp_schema_file_handle.write(schema.encode("utf-8"))
        schema_file_to_upload = {
            "file_name": self.schema_filename,
            "file_handle": tmp_schema_file_handle,
            "file_mime_type": "application/json",
        }
        return schema_file_to_upload

    def _upload_to_gcs(self, file_to_upload):
        """Upload a file (data split or schema .json file) to Google Cloud Storage."""
        hook = GCSHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            impersonation_chain=self.impersonation_chain,
        )
        is_data_file = file_to_upload.get("file_name") != self.schema_filename
        metadata = None
        if is_data_file and self.upload_metadata:
            metadata = {"row_count": file_to_upload["file_row_count"]}

        object_name = file_to_upload.get("file_name")
        if is_data_file and self.partition_columns:
            # Add partition column values to object_name
            partition_values = file_to_upload.get("partition_values")
            head_path, tail_path = os.path.split(object_name)
            partition_subprefix = [
                f"{col}={val}" for col, val in zip(self.partition_columns, partition_values)
            ]
            object_name = os.path.join(head_path, *partition_subprefix, tail_path)

        hook.upload(
            self.bucket,
            object_name,
            file_to_upload.get("file_handle").name,
            mime_type=file_to_upload.get("file_mime_type"),
            gzip=self.gzip if is_data_file else False,
            metadata=metadata,
        )
