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

from airflow.configuration import conf
from airflow.plugins_manager import AirflowPlugin
from airflow.providers.openlineage.plugins.listener import get_openlineage_listener
from airflow.providers.openlineage.plugins.macros import lineage_parent_id, lineage_run_id


def _is_disabled() -> bool:
    return (
        conf.getboolean("openlineage", "disabled", fallback=False)
        or os.getenv("OPENLINEAGE_DISABLED", "false").lower() == "true"
        or (
            conf.get("openlineage", "transport", fallback="") == ""
            and conf.get("openlineage", "config_path", fallback="") == ""
            and os.getenv("OPENLINEAGE_URL", "") == ""
            and os.getenv("OPENLINEAGE_CONFIG", "") == ""
        )
    )


class OpenLineageProviderPlugin(AirflowPlugin):
    """
    Listener that emits numerous Events.

    OpenLineage Plugin provides listener that emits OL events on DAG start,
    complete and failure and TaskInstances start, complete and failure.
    """

    name = "OpenLineageProviderPlugin"
    if not _is_disabled():
        macros = [lineage_run_id, lineage_parent_id]
        listeners = [get_openlineage_listener()]
