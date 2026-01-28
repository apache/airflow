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

from airflow.configuration import conf
from airflow.plugins_manager import AirflowPlugin

is_disabled = conf.getboolean("informatica", "disabled", fallback=False)
# Conditional imports - only load expensive dependencies when plugin is enabled
if not is_disabled:
    from airflow.lineage.hook import HookLineageReader
    from airflow.providers.informatica.plugins.listener import get_informatica_listener


class InformaticaProviderPlugin(AirflowPlugin):
    """
    Listener that emits numerous Events.

    Informatica Plugin provides listener that emits OL events on DAG start,
    complete and failure and TaskInstances start, complete and failure.
    """

    name: str = "InformaticaProviderPlugin"
    if not is_disabled:
        listeners: list = [get_informatica_listener()]
        hook_lineage_readers: list = [HookLineageReader]
    else:
        listeners: list = []

    def __init__(self) -> None:
        """Initialize InformaticaProviderPlugin."""
        super().__init__()
