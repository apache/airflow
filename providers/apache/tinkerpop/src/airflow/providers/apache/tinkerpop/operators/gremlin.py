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

from typing import TYPE_CHECKING, Any

from airflow.providers.apache.tinkerpop.hooks.gremlin import GremlinHook
from airflow.providers.common.compat.sdk import BaseOperator

if TYPE_CHECKING:
    from airflow.providers.common.compat.sdk import Context


class GremlinOperator(BaseOperator):
    """
    Execute a Gremlin query.

    :param query: The Gremlin query to execute.
    :param gremlin_conn_id: The connection ID to use when connecting to Gremlin. Defaults to "gremlin_default".
    """

    template_fields = ("query",)

    def __init__(self, query: str, gremlin_conn_id: str = "gremlin_default", **kwargs) -> None:
        super().__init__(**kwargs)
        self.query = query
        self.gremlin_conn_id = gremlin_conn_id

    def execute(self, context: Context) -> Any:
        hook = GremlinHook(conn_id=self.gremlin_conn_id)
        # Note: the hook method is defined as run() in our hook implementation.
        # If you prefer, you can add an alias run_query = run in your hook.
        try:
            return hook.run(self.query)
        finally:
            if hasattr(hook, "client") and hook.client:
                hook.client.close()  # Ensure client cleanup
