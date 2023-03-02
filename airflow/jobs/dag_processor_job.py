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

import os
from datetime import timedelta

from airflow.dag_processing.manager import DagFileProcessorManager
from airflow.jobs.base_job import BaseJob


class DagProcessorJob(BaseJob):
    """
    :param dag_directory: Directory where DAG definitions are kept. All
        files in file_paths should be under this directory
    :param max_runs: The number of times to parse and schedule each file. -1
        for unlimited.
    :param processor_timeout: How long to wait before timing out a DAG file processor
    :param dag_ids: if specified, only schedule tasks with these DAG IDs
    :param pickle_dags: whether to pickle DAGs.
    :param async_mode: Whether to start agent in async mode
    """

    __mapper_args__ = {"polymorphic_identity": "DagProcessorJob"}

    def __init__(
        self,
        dag_directory: os.PathLike,
        max_runs: int,
        processor_timeout: timedelta,
        dag_ids: list[str] | None,
        pickle_dags: bool,
        *args,
        **kwargs,
    ):
        self.processor = DagFileProcessorManager(
            dag_directory=dag_directory,
            max_runs=max_runs,
            processor_timeout=processor_timeout,
            dag_ids=dag_ids,
            pickle_dags=pickle_dags,
        )
        super().__init__(*args, **kwargs)

    def _execute(self) -> None:
        self.log.info("Starting the Dag Processor Job")
        try:
            self.processor.start()
        except Exception:
            self.log.exception("Exception when executing DagProcessorJob")
            raise
        finally:
            self.processor.terminate()
            self.processor.end()
