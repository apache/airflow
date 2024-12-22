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

from airflow import settings
from airflow.configuration import conf
from airflow.dag_processing.bundles.local import LocalDagBundle


class DagsFolderDagBundle(LocalDagBundle):
    """A bundle for the DAGs folder."""

    def __init__(self, refresh_interval: int | None = None, **kwargs):
        if refresh_interval is None:
            refresh_interval = conf.getint("scheduler", "dag_dir_list_interval")

        super().__init__(
            local_folder=settings.DAGS_FOLDER,
            refresh_interval=refresh_interval,
            **kwargs,
        )
