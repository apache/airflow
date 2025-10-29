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
"""This module contains an operator to move data from an S3 bucket to Hive."""

from __future__ import annotations

import bz2
import gzip
import os
import shutil
import tempfile
from collections.abc import Sequence
from tempfile import NamedTemporaryFile, TemporaryDirectory
from typing import TYPE_CHECKING, Any

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.apache.hive.hooks.hive import HiveCliHook
from airflow.providers.common.compat.sdk import BaseOperator

if TYPE_CHECKING:
    from airflow.providers.common.compat.sdk import Context


class S3ToHiveOperator(BaseOperator):
    """
    Moves data from S3 to Hive.

    The operator downloads a file from S3, stores the file locally
    before loading it into a Hive table.

    If the ``create`` or ``recreate`` arguments are set to ``True``,
    a ``CREATE TABLE`` and ``DROP TABLE`` statements are generated.
    Hive data types are inferred from the cursor's metadata from.

    Note that the table generated in Hive uses ``STORED AS textfile``
    which isn't the most efficient serialization format. If a
    large amount of data is loaded and/or if the tables gets
    queried considerably, you may want to use this operator only to
    stage the data into a temporary table before loading it into its
    final destination using a ``HiveOperator``.

    :param s3_key: The key to be retrieved from S3. (templated)
    :param field_dict: A dictionary of the fields name in the file
        as keys and their Hive types as values
    :param hive_table: target Hive table, use dot notation to target a
        specific database. (templated)
    :param delimiter: field delimiter in the file
    :param create: whether to create the table if it doesn't exist
    :param recreate: whether to drop and recreate the table at every
        execution
    :param partition: target partition as a dict of partition columns
        and values. (templated)
    :param headers: whether the file contains column names on the first
        line
    :param check_headers: whether the column names on the first line should be
        checked against the keys of field_dict
    :param wildcard_match: whether the s3_key should be interpreted as a Unix
        wildcard pattern
    :param aws_conn_id: source s3 connection
    :param verify: Whether or not to verify SSL certificates for S3 connection.
        By default SSL certificates are verified.
        You can provide the following values:

        - ``False``: do not validate SSL certificates. SSL will still be used
                 (unless use_ssl is False), but SSL certificates will not be
                 verified.
        - ``path/to/cert/bundle.pem``: A filename of the CA cert bundle to uses.
                 You can specify this argument if you want to use a different
                 CA cert bundle than the one used by botocore.
    :param hive_cli_conn_id: Reference to the
        :ref:`Hive CLI connection id <howto/connection:hive_cli>`.
    :param input_compressed: Boolean to determine if file decompression is
        required to process headers
    :param tblproperties: TBLPROPERTIES of the hive table being created
    :param select_expression: S3 Select expression
    """

    template_fields: Sequence[str] = ("s3_key", "partition", "hive_table")
    template_ext: Sequence[str] = ()
    ui_color = "#a0e08c"

    def __init__(
        self,
        *,
        s3_key: str,
        field_dict: dict,
        hive_table: str,
        delimiter: str = ",",
        create: bool = True,
        recreate: bool = False,
        partition: dict | None = None,
        headers: bool = False,
        check_headers: bool = False,
        wildcard_match: bool = False,
        aws_conn_id: str | None = "aws_default",
        verify: bool | str | None = None,
        hive_cli_conn_id: str = "hive_cli_default",
        input_compressed: bool = False,
        tblproperties: dict | None = None,
        select_expression: str | None = None,
        hive_auth: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.s3_key = s3_key
        self.field_dict = field_dict
        self.hive_table = hive_table
        self.delimiter = delimiter
        self.create = create
        self.recreate = recreate
        self.partition = partition
        self.headers = headers
        self.check_headers = check_headers
        self.wildcard_match = wildcard_match
        self.hive_cli_conn_id = hive_cli_conn_id
        self.aws_conn_id = aws_conn_id
        self.verify = verify
        self.input_compressed = input_compressed
        self.tblproperties = tblproperties
        self.select_expression = select_expression
        self.hive_auth = hive_auth

        if self.check_headers and not (self.field_dict is not None and self.headers):
            raise AirflowException("To check_headers provide field_dict and headers")

    def execute(self, context: Context):
        # Downloading file from S3
        s3_hook = S3Hook(aws_conn_id=self.aws_conn_id, verify=self.verify)
        hive_hook = HiveCliHook(hive_cli_conn_id=self.hive_cli_conn_id, auth=self.hive_auth)
        self.log.info("Downloading S3 file")

        if self.wildcard_match:
            if not s3_hook.check_for_wildcard_key(self.s3_key):
                raise AirflowException(f"No key matches {self.s3_key}")
            s3_key_object = s3_hook.get_wildcard_key(self.s3_key)
        elif s3_hook.check_for_key(self.s3_key):
            s3_key_object = s3_hook.get_key(self.s3_key)

        else:
            raise AirflowException(f"The key {self.s3_key} does not exists")

        if TYPE_CHECKING:
            assert s3_key_object

        _, file_ext = os.path.splitext(s3_key_object.key)
        if self.select_expression and self.input_compressed and file_ext.lower() != ".gz":
            raise AirflowException("GZIP is the only compression format Amazon S3 Select supports")

        with (
            TemporaryDirectory(prefix="tmps32hive_") as tmp_dir,
            NamedTemporaryFile(mode="wb", dir=tmp_dir, suffix=file_ext) as f,
        ):
            self.log.info("Dumping S3 key %s contents to local file %s", s3_key_object.key, f.name)
            if self.select_expression:
                option = {}
                if self.headers:
                    option["FileHeaderInfo"] = "USE"
                if self.delimiter:
                    option["FieldDelimiter"] = self.delimiter

                input_serialization: dict[str, Any] = {"CSV": option}
                if self.input_compressed:
                    input_serialization["CompressionType"] = "GZIP"

                content = s3_hook.select_key(
                    bucket_name=s3_key_object.bucket_name,
                    key=s3_key_object.key,
                    expression=self.select_expression,
                    input_serialization=input_serialization,
                )
                f.write(content.encode("utf-8"))
            else:
                s3_key_object.download_fileobj(f)
            f.flush()

            if self.select_expression or not self.headers:
                self.log.info("Loading file %s into Hive", f.name)
                hive_hook.load_file(
                    f.name,
                    self.hive_table,
                    field_dict=self.field_dict,
                    create=self.create,
                    partition=self.partition,
                    delimiter=self.delimiter,
                    recreate=self.recreate,
                    tblproperties=self.tblproperties,
                )
            else:
                # Decompressing file
                if self.input_compressed:
                    self.log.info("Uncompressing file %s", f.name)
                    fn_uncompressed = uncompress_file(f.name, file_ext, tmp_dir)
                    self.log.info("Uncompressed to %s", fn_uncompressed)
                    # uncompressed file available now so deleting
                    # compressed file to save disk space
                    f.close()
                else:
                    fn_uncompressed = f.name

                # Testing if header matches field_dict
                if self.check_headers:
                    self.log.info("Matching file header against field_dict")
                    header_list = self._get_top_row_as_list(fn_uncompressed)
                    if not self._match_headers(header_list):
                        raise AirflowException("Header check failed")

                # Deleting top header row
                self.log.info("Removing header from file %s", fn_uncompressed)
                headless_file = self._delete_top_row_and_compress(fn_uncompressed, file_ext, tmp_dir)
                self.log.info("Headless file %s", headless_file)
                self.log.info("Loading file %s into Hive", headless_file)
                hive_hook.load_file(
                    headless_file,
                    self.hive_table,
                    field_dict=self.field_dict,
                    create=self.create,
                    partition=self.partition,
                    delimiter=self.delimiter,
                    recreate=self.recreate,
                    tblproperties=self.tblproperties,
                )

    def _get_top_row_as_list(self, file_name):
        with open(file_name) as file:
            header_line = file.readline().strip()
            return header_line.split(self.delimiter)

    def _match_headers(self, header_list):
        if not header_list:
            raise AirflowException("Unable to retrieve header row from file")
        field_names = self.field_dict.keys()
        if len(field_names) != len(header_list):
            self.log.warning(
                "Headers count mismatch File headers:\n %s\nField names: \n %s\n", header_list, field_names
            )
            return False
        test_field_match = all(h1.lower() == h2.lower() for h1, h2 in zip(header_list, field_names))
        if test_field_match:
            return True
        self.log.warning(
            "Headers do not match field names File headers:\n %s\nField names: \n %s\n",
            header_list,
            field_names,
        )
        return False

    @staticmethod
    def _delete_top_row_and_compress(input_file_name, output_file_ext, dest_dir):
        # When output_file_ext is not defined, file is not compressed
        open_fn = open
        if output_file_ext.lower() == ".gz":
            open_fn = gzip.GzipFile
        elif output_file_ext.lower() == ".bz2":
            open_fn = bz2.BZ2File

        _, fn_output = tempfile.mkstemp(suffix=output_file_ext, dir=dest_dir)
        with open(input_file_name, "rb") as f_in, open_fn(fn_output, "wb") as f_out:
            f_in.seek(0)
            next(f_in)
            for line in f_in:
                f_out.write(line)
        return fn_output


def uncompress_file(input_file_name, file_extension, dest_dir):
    """Uncompress gz and bz2 files."""
    if file_extension.lower() not in (".gz", ".bz2"):
        raise NotImplementedError(
            f"Received {file_extension} format. Only gz and bz2 files can currently be uncompressed."
        )
    if file_extension.lower() == ".gz":
        fmodule = gzip.GzipFile
    elif file_extension.lower() == ".bz2":
        fmodule = bz2.BZ2File
    with (
        fmodule(input_file_name, mode="rb") as f_compressed,
        NamedTemporaryFile(dir=dest_dir, mode="wb", delete=False) as f_uncompressed,
    ):
        shutil.copyfileobj(f_compressed, f_uncompressed)
    return f_uncompressed.name
