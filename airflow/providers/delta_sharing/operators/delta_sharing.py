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
#
"""This module contains Delta Sharing operators."""
import json
import os.path
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Sequence
from urllib.parse import urlparse

import requests

from airflow import AirflowException
from airflow.models import BaseOperator
from airflow.providers.delta_sharing.hooks.delta_sharing import DeltaSharingHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class DeltaSharingLocalDownloadOperator(BaseOperator):
    """
    :param share: name of the share in which check will be performed.
        This field will be templated.
    :param schema: name of the schema (database) in which check will be performed.
        This field will be templated.
    :param table: name of the table to check.
        This field will be templated.
    :param location: name of directory where downloaded data will be stored.
        This field will be templated.
    :param limit: optional limit on the number of records to return.
    :param predicates: optional list of strings that will be ANDed to build a filter expression.
        This field will be templated.
    :param save_partitioned: if True, data will be saved by partitions. if False, all data files will be
       saved into a top-level directory.
    :param save_metadata:  if True, metadata will be shared into a ``_metadata/<version>.json`` file
    :param save_stats: if True, per-file statistics will be saved into a ``_stats/<data_file_name>.json``
    :param overwrite_existing: Overwrite existing files.  False by default. If file with the same name exists
        and has the same size as returned in file metadata, then it won't be re-downloaded.
    :param num_parallel_downloads: number of parallel downloads. Default is 5.
    :param delta_sharing_conn_id: Reference to the
        :ref:`Delta Sharing connection <howto/connection:delta_sharing>`.
        By default and in the common case this will be ``delta_sharing_default``. To use
        token based authentication, provide the bearer token in the password field for the
        connection and put the base URL in the ``host`` field.
    :param timeout_seconds: The timeout for this run. By default a value of 0 is used
        which means to have no timeout.
        This field will be templated.
    :param retry_limit: Amount of times retry if the Delta Sharing backend is
        unreachable. Its value must be greater than or equal to 1.
    :param retry_delay: Number of seconds for initial wait between retries (it
            might be a floating point number).
    :param retry_args: An optional dictionary with arguments passed to ``tenacity.Retrying`` class.
    """

    template_fields: Sequence[str] = (
        'share',
        'schema',
        'table',
        'predicates',
        'location',
    )

    # Used in airflow.models.BaseOperator
    # Delta Sharing brand color (blue) under white text
    ui_color = '#1CB1C2'
    ui_fgcolor = '#fff'

    def __init__(
        self,
        *,
        share: str,
        schema: str,
        table: str,
        location: str,
        limit: Optional[int] = None,
        predicates: Optional[List[str]] = None,
        save_partitioned: bool = True,
        save_metadata: bool = True,
        save_stats: bool = True,
        overwrite_existing: bool = False,
        num_parallel_downloads: int = 5,
        delta_sharing_conn_id: str = 'delta_sharing_default',
        timeout_seconds: int = 180,
        retry_limit: int = 3,
        retry_delay: float = 2.0,
        retry_args: Optional[Dict[Any, Any]] = None,
        **kwargs,
    ) -> None:
        """Creates a new ``DeltaSharingDownloadToLocalOperator``."""
        super().__init__(**kwargs)
        self.share = share
        self.schema = schema
        self.table = table
        self.location = location
        if limit is not None and limit < 0:
            raise ValueError(f"limit should be greater or equal to 0, got {limit}")
        self.limit = limit
        self.predicates = predicates
        self.save_partitioned = save_partitioned
        self.save_stats = save_stats
        self.save_metadata = save_metadata
        self.overwrite_existing = overwrite_existing
        if num_parallel_downloads < 1:
            raise ValueError(
                "num_parallel_downloads should be greater or equal to 1," f" got {num_parallel_downloads}"
            )
        self.num_parallel_downloads = num_parallel_downloads

        self.hook = DeltaSharingHook(
            delta_sharing_conn_id=delta_sharing_conn_id,
            retry_args=retry_args,
            retry_delay=retry_delay,
            retry_limit=retry_limit,
            timeout_seconds=timeout_seconds,
        )

    def _get_output_file_path(self, metadata: Dict[str, Any], file: Dict[str, Any]) -> str:
        chunks = urlparse(file['url'])
        file_name = chunks.path.split('/')[-1]
        file_parts = [self.location]
        # add partition parts if table is partitioned
        partition_values = file.get('partitionValues', {})
        if self.save_partitioned and len(partition_values) > 0:
            partitions = metadata.get('partitionColumns', [])
            for part in partitions:
                part_value = partition_values.get(part)
                if part_value is None:
                    self.log.warning(f"There is no value for partition '{part}'")
                    part_value = "__null__"
                file_parts.append(f"{part}={part_value}")

        os.makedirs(os.path.join(*file_parts), exist_ok=True)
        file_parts.append(file_name)
        return os.path.join(*file_parts)

    def _download_one(self, metadata: Dict[str, Any], file: Dict[str, Any]):
        dest_file_path = self._get_output_file_path(metadata, file)
        file_size = file['size']
        if os.path.exists(dest_file_path):
            stat = os.stat(dest_file_path)
            if file_size == stat.st_size and not self.overwrite_existing:
                self.log.info(
                    f"File {dest_file_path} already exists, and has the same size. " "Skipping download..."
                )
                return

        # download file from URL into a given path
        with requests.get(file['url'], stream=True) as r:
            r.raise_for_status()
            with open(dest_file_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=16384):
                    f.write(chunk)

        stat = os.stat(dest_file_path)
        if file_size != stat.st_size:
            raise RuntimeError(
                f"Error when downloading file to {dest_file_path}. "
                f"Saved size: {stat.st_size}, expected size: {file_size}"
            )

        # write statistics
        stats_data = file.get('stats', '')
        if self.save_stats and stats_data != '':
            stats_dir = os.path.join(self.location, "_stats")
            os.makedirs(stats_dir, exist_ok=True)
            stats_file_name = os.path.join(stats_dir, os.path.basename(dest_file_path) + ".json")
            with open(stats_file_name, "w") as f2:
                f2.write(stats_data)

        return

    def execute(self, context: 'Context'):
        results = self.hook.query_table(
            self.share, self.schema, self.table, limit=self.limit, predicates=self.predicates
        )
        self.log.debug(f"Version: {results.version}")
        self.log.debug(f"Protocol: {results.protocol}")
        self.log.debug(f"Metadata: {results.metadata}")
        self.log.debug(f"Files: count={len(results.files)}")
        # write files
        if len(results.files) > 0:
            has_errors = False
            with ThreadPoolExecutor(max_workers=self.num_parallel_downloads) as executor:
                future_to_file = {
                    executor.submit(lambda x: self._download_one(results.metadata, x), file): file
                    for file in results.files
                }
                for future in as_completed(future_to_file):
                    file = future_to_file[future]
                    try:
                        _ = future.result()
                    except Exception as exc:
                        has_errors = True
                        self.log.warning("Exception when downloading from '%s': %s", file['url'], exc)
            if has_errors:
                raise AirflowException(
                    "Some Delta Sharing files weren't downloaded correctly. " "Check logs for details"
                )

        # write metadata - for each version
        if self.save_metadata:
            metadata_dir = os.path.join(self.location, "_metadata")
            os.makedirs(metadata_dir, exist_ok=True)
            metadata_file_name = os.path.join(metadata_dir, str(results.version) + ".json")
            with open(metadata_file_name, "w") as f:
                json.dump(results.metadata, f)
