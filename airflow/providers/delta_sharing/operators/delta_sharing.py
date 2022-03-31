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
"""This module contains Databricks operators."""
import os.path
import time
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Sequence, Union
from urllib.parse import urlparse

from airflow import AirflowException
from airflow.models import BaseOperator
from airflow.providers.delta_sharing.hooks.delta_sharing import DeltaSharingHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class DeltaSharingDownloadToLocalOperator(BaseOperator):
    """
    :param share: name of the share in which check will be performed
    :param schema: name of the schema (database) in which check will be performed
    :param table: name of the table to check
    :param delta_sharing_conn_id: Reference to the
        :ref:`Databricks connection <howto/connection:delta_sharing>`.
        By default and in the common case this will be ``delta_sharing_default``. To use
        token based authentication, provide the bearer token in the password field for the
        connection and put the base URL in the ``host`` field.
    :param timeout_seconds: The timeout for this run. By default a value of 0 is used
        which means to have no timeout.
        This field will be templated.
    :param databricks_retry_limit: Amount of times retry if the Databricks backend is
        unreachable. Its value must be greater than or equal to 1.
    :param databricks_retry_delay: Number of seconds to wait between retries (it
            might be a floating point number).
    :param databricks_retry_args: An optional dictionary with arguments passed to ``tenacity.Retrying`` class.
    """

    template_fields: Sequence[str] = ('share', 'schema', 'table', 'predicates',)

    # Used in airflow.models.BaseOperator
    # Databricks brand color (blue) under white text
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
        partitioned: bool = True,
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
        if limit < 0:
            raise AirflowException(f"limit should be greater or equal to 0, got {limit}")
        self.limit = limit
        self.predicates = predicates
        self.partitioned = partitioned
        self.save_stats = save_stats
        self.save_metadata = save_metadata
        self.overwrite_existing = overwrite_existing
        if num_parallel_downloads < 1:
            raise AirflowException("num_parallel_downloads should be greater or equal to 1,"
                                   f" got {num_parallel_downloads}")
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
        if self.partitioned and len(partition_values):
            partitions = metadata.get('partitionColumns', [])
            for part in partitions:
                part_value = partition_values.get(part)
                if part_value is None:
                    self.log.warning(f"There is no value for partition '{part}'")
                    part_value="__null__"
                file_parts.append(f"{part}={part_value}")

        os.makedirs(os.path.join(*file_parts), exist_ok=True)
        file_parts.append(file_name)
        return os.path.join(*file_parts)

    def _download_one(self, metadata: Dict[str, Any], file: Dict[str, Any]) -> bool:
        dest_file_path = self._get_output_file_path(metadata, file)
        file_size = file['size']
        if os.path.exists(dest_file_path):
            stat = os.stat(dest_file_path)
            if file_size == stat.st_size and not self.overwrite_existing:
                self.log.info(f"File {dest_file_path} already exists, and has the same size. "
                              "Skipping downloading...")
                return True

        # TODO: download file from URL into given path

        # TODO: write statistics

        return True

    def execute(self, context: 'Context'):
        results = self.hook.query_table(self.share, self.schema, self.table, limit=self.limit,
                                        predicates=self.predicates)
        self.log.info(f"Version: {results.version}")
        self.log.info(f"Protocol: {results.protocol}")
        self.log.info(f"Metadata: {results.metadata}")
        self.log.info(f"Files: count={len(results.files)}")
        [self.log.info(file) for file in results.files]
        # TODO: write files (use multithreading...)
        # TODO: write metadata - for each version


