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

from collections.abc import Callable, Sequence
from operator import itemgetter
from typing import TYPE_CHECKING, Any

from airflow.providers.common.compat.sdk import AirflowException, BaseSensorOperator
from airflow.providers.neo4j.hooks.neo4j import Neo4jHook

if TYPE_CHECKING:
    from airflow.sdk import Context


class Neo4jSensor(BaseSensorOperator):
    """
    Executes a Cypher query in Neo4j until the returned value satisfies a condition.

    The query runs repeatedly at the defined poke interval until:
      * A callable provided in ``failure`` evaluates to True, which raises an exception.
      * A callable provided in ``success`` evaluates to True, which marks success.
      * Otherwise, the truthiness of the selected value determines success.

    Example
    -------
    .. code-block:: python

        wait_person_exists = Neo4jSensor(
            task_id="wait_person_exists",
            neo4j_conn_id="neo4j_default",
            cypher="MATCH (p:Person) RETURN count(p) > 0",
        )

    :param neo4j_conn_id: Connection ID to use for connecting to Neo4j.
    :param cypher: Cypher statement to execute. (Templated)
    :param parameters: Query parameters. (Templated)
    :param success: Callable that receives the selected value and returns a boolean.
    :param failure: Callable that receives the selected value; if it returns True, an error is raised.
    :param selector: Function that extracts a single value from the first row of the result.
    :param fail_on_empty: When True, raises if the query returns no rows.
    """

    template_fields: Sequence[str] = ("cypher", "parameters")
    template_fields_renderers = {"cypher": "sql", "parameters": "json"}

    def __init__(
        self,
        *,
        neo4j_conn_id: str = "neo4j_default",
        cypher: str,
        parameters: dict[str, Any] | None = None,
        success: Callable[[Any], bool] | None = None,
        failure: Callable[[Any], bool] | None = None,
        selector: Callable[[tuple[Any, ...]], Any] = itemgetter(0),
        fail_on_empty: bool = False,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.neo4j_conn_id = neo4j_conn_id
        self.cypher = cypher
        self.parameters = parameters
        self.success = success
        self.failure = failure
        self.selector = selector
        self.fail_on_empty = fail_on_empty

    @staticmethod
    def _row_to_tuple(record: Any) -> tuple[Any, ...]:
        if record is None:
            return ()
        if hasattr(record, "values"):
            try:
                return tuple(record.values())
            except Exception:
                pass
        if isinstance(record, dict):
            return tuple(record.values())
        if isinstance(record, (list, tuple)):
            return tuple(record)
        return (record,)

    def poke(self, context: Context) -> bool:
        hook = Neo4jHook(conn_id=self.neo4j_conn_id)
        self.log.info("Executing Cypher: %s (parameters=%s)", self.cypher, self.parameters)
        rows = hook.run(self.cypher, self.parameters)

        if not rows:
            if self.fail_on_empty:
                raise AirflowException("No rows returned, raising as per parameter 'fail_on_empty=True'")
            return False

        first_row = self._row_to_tuple(rows[0])

        if not callable(self.selector):
            raise AirflowException(f"Parameter 'selector' is not callable: {self.selector!r}")

        value = self.selector(first_row)

        if self.failure is not None:
            if callable(self.failure):
                if self.failure(value):
                    raise AirflowException(f"Failure criteria met: failure({value!r}) returned True")
            else:
                raise AirflowException(f"Parameter 'failure' is not callable: {self.failure!r}")

        if self.success is not None:
            if callable(self.success):
                return bool(self.success(value))
            raise AirflowException(f"Parameter 'success' is not callable: {self.success!r}")

        return bool(value)
