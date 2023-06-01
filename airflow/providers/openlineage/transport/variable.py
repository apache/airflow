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

from airflow.models import Variable
from airflow.plugins_manager import AirflowPlugin, plugins
from airflow.utils.log.logging_mixin import LoggingMixin
from openlineage.client.run import DatasetEvent, JobEvent, RunEvent
from openlineage.client.serde import Serde
from openlineage.client.transport import Transport


class VariableTransport(Transport, LoggingMixin):
    """This transport sends OpenLineage events to Variables.
    Key schema is <DAG_ID>.<TASK_ID>.event.<EVENT_TYPE>.
    It's made to be used in system tests, stored data read by OpenLineageTestOperator.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        ...

    def emit(self, event: RunEvent | DatasetEvent | JobEvent):
        from airflow.providers.openlineage.plugins.openlineage import OpenLineageProviderPlugin

        plugin: AirflowPlugin | None = next(  # type: ignore[assignment]
            filter(lambda x: isinstance(x, OpenLineageProviderPlugin), plugins)  # type: ignore[arg-type]
        )
        if not plugin:
            raise RuntimeError("OpenLineage listener should be set up here")

        listener = plugin.listeners[0]  # type: ignore
        ti = listener.current_ti  # type: ignore

        key = f"{ti.dag_id}.{ti.task_id}.event.{event.eventType.value.lower()}"  # type: ignore[union-attr]
        str_event = Serde.to_json(event)
        Variable.set(key=key, value=str_event)
