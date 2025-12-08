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

from unittest import mock

import pytest

from airflow.models.dag import DAG
from airflow.providers.common.compat.sdk import AirflowException, timezone
from airflow.providers.neo4j.sensors.neo4j import Neo4jSensor

DEFAULT_DATE = timezone.datetime(2015, 1, 1)
TEST_DAG_ID = "unit_test_neo4j_dag"


class TestNeo4jSensor:
    def setup_method(self):
        args = {"owner": "airflow", "start_date": DEFAULT_DATE}
        self.dag = DAG(TEST_DAG_ID, schedule=None, default_args=args)

    @mock.patch("airflow.providers.neo4j.sensors.neo4j.Neo4jHook")
    def test_neo4j_sensor_smoke_test(self, mock_neo4j_hook):
        mock_neo4j_hook_conn = mock_neo4j_hook.return_value
        mock_neo4j_hook_conn.run.return_value = [{"person_count": 50}]

        cypher = "MATCH (p:Person) RETURN COUNT(p) AS person_count"

        sensor = Neo4jSensor(task_id="neo4j_sensor_check", neo4j_conn_id="neo4j_default", cypher=cypher)

        assert sensor.poke(mock.MagicMock()) is True
        mock_neo4j_hook.assert_called_once_with(conn_id="neo4j_default")
        mock_neo4j_hook_conn.run.assert_called_once_with(cypher, None)

    @mock.patch("airflow.providers.neo4j.sensors.neo4j.Neo4jHook")
    def test_neo4j_sensor_poke_empty_default(self, mock_neo4j_hook):
        mock_neo4j_hook_conn = mock_neo4j_hook.return_value
        mock_neo4j_hook_conn.run.return_value = []

        cypher = "MATCH (n:NoSuchLabel) RETURN n.id AS id"

        sensor = Neo4jSensor(
            task_id="neo4j_sensor_check",
            neo4j_conn_id="neo4j_default",
            cypher=cypher,
            parameters=None,
        )
        assert sensor.poke(mock.MagicMock()) is False

    @mock.patch("airflow.providers.neo4j.sensors.neo4j.Neo4jHook")
    def test_neo4j_sensor_poke_empty_false(self, mock_neo4j_hook):
        mock_neo4j_hook_conn = mock_neo4j_hook.return_value
        mock_neo4j_hook_conn.run.return_value = []

        cypher = "MATCH (n:NoSuchLabel) RETURN n.id AS id"

        sensor = Neo4jSensor(
            task_id="neo4j_sensor_check",
            neo4j_conn_id="neo4j_default",
            cypher=cypher,
            parameters=None,
            fail_on_empty=False,
        )
        assert sensor.poke(mock.MagicMock()) is False

    @mock.patch("airflow.providers.neo4j.sensors.neo4j.Neo4jHook")
    def test_neo4j_sensor_poke_empty_true(self, mock_neo4j_hook):
        mock_neo4j_hook_conn = mock_neo4j_hook.return_value
        mock_neo4j_hook_conn.run.return_value = []

        cypher = "MATCH (n:NoSuchLabel) RETURN n.id AS id"

        sensor = Neo4jSensor(
            task_id="neo4j_sensor_check",
            neo4j_conn_id="neo4j_default",
            cypher=cypher,
            parameters=None,
            fail_on_empty=True,
        )
        with pytest.raises(AirflowException):
            sensor.poke(mock.MagicMock())

    @mock.patch("airflow.providers.neo4j.sensors.neo4j.Neo4jHook")
    def test_neo4j_sensor_poke_default_true_value(self, mock_neo4j_hook):
        mock_neo4j_hook_conn = mock_neo4j_hook.return_value
        mock_neo4j_hook_conn.run.return_value = [{"value": 1}]

        cypher = "RETURN 1 AS value"

        sensor = Neo4jSensor(
            task_id="neo4j_sensor_check",
            neo4j_conn_id="neo4j_default",
            cypher=cypher,
            parameters=None,
        )

        assert sensor.poke(mock.MagicMock()) is True

    @mock.patch("airflow.providers.neo4j.sensors.neo4j.Neo4jHook")
    def test_neo4j_sensor_poke_default_false_value(self, mock_neo4j_hook):
        mock_neo4j_hook_conn = mock_neo4j_hook.return_value
        mock_neo4j_hook_conn.run.return_value = [{"value": 0}]

        cypher = "RETURN 0 AS value"

        sensor = Neo4jSensor(
            task_id="neo4j_sensor_check",
            neo4j_conn_id="neo4j_default",
            cypher=cypher,
            parameters=None,
        )

        assert sensor.poke(mock.MagicMock()) is False

    @mock.patch("airflow.providers.neo4j.sensors.neo4j.Neo4jHook")
    def test_neo4j_sensor_poke_failure_precedence(self, mock_neo4j_hook):
        mock_neo4j_hook_conn = mock_neo4j_hook.return_value
        mock_neo4j_hook_conn.run.return_value = [{"value": 10}]

        success = mock.MagicMock(side_effect=lambda v: v == 10)
        failure = mock.MagicMock(side_effect=lambda v: v == 10)

        cypher = "RETURN 10 AS value"

        sensor = Neo4jSensor(
            task_id="neo4j_sensor_check",
            neo4j_conn_id="neo4j_default",
            cypher=cypher,
            success=success,
            failure=failure,
        )

        with pytest.raises(AirflowException):
            sensor.poke(mock.MagicMock())

        failure.assert_called_once()
        success.assert_not_called()

    @mock.patch("airflow.providers.neo4j.sensors.neo4j.Neo4jHook")
    def test_neo4j_sensor_poke_failure_non_callable(self, mock_neo4j_hook):
        mock_neo4j_hook_conn = mock_neo4j_hook.return_value
        mock_neo4j_hook_conn.run.return_value = [{"value": 10}]

        cypher = "RETURN 10 AS value"

        sensor = Neo4jSensor(
            task_id="neo4j_sensor_check", neo4j_conn_id="neo4j_default", cypher=cypher, failure="value = 10"
        )
        with pytest.raises(AirflowException) as ctx:
            sensor.poke(mock.MagicMock())

        assert str(ctx.value) == "Parameter 'failure' is not callable: 'value = 10'"

    @mock.patch("airflow.providers.neo4j.sensors.neo4j.Neo4jHook")
    def test_neo4j_sensor_poke_failure_default(self, mock_neo4j_hook):
        mock_neo4j_hook_conn = mock_neo4j_hook.return_value
        mock_neo4j_hook_conn.run.return_value = [{"value": 1}]

        cypher = "RETURN 1 AS value"

        sensor = Neo4jSensor(
            task_id="neo4j_sensor_check",
            neo4j_conn_id="neo4j_default",
            cypher=cypher,
            failure=lambda x: x == 0,
        )

        assert sensor.poke(mock.MagicMock()) is True

    @mock.patch("airflow.providers.neo4j.sensors.neo4j.Neo4jHook")
    def test_neo4j_sensor_poke_success_true(self, mock_neo4j_hook):
        mock_neo4j_hook_conn = mock_neo4j_hook.return_value
        mock_neo4j_hook_conn.run.return_value = [{"value": 15}]

        cypher = "RETURN 15 AS value"

        sensor = Neo4jSensor(
            task_id="neo4j_sensor_check",
            neo4j_conn_id="neo4j_default",
            cypher=cypher,
            success=lambda x: x > 10,
        )
        assert sensor.poke(mock.MagicMock()) is True

    @mock.patch("airflow.providers.neo4j.sensors.neo4j.Neo4jHook")
    def test_neo4j_sensor_poke_success_false(self, mock_neo4j_hook):
        mock_neo4j_hook_conn = mock_neo4j_hook.return_value
        mock_neo4j_hook_conn.run.return_value = [{"value": 10}]

        cypher = "RETURN 10 AS value"

        sensor = Neo4jSensor(
            task_id="neo4j_sensor_check",
            neo4j_conn_id="neo4j_default",
            cypher=cypher,
            success=lambda x: x > 10,
        )
        assert sensor.poke(mock.MagicMock()) is False

    @mock.patch("airflow.providers.neo4j.sensors.neo4j.Neo4jHook")
    def test_neo4j_sensor_poke_success_non_callable(self, mock_neo4j_hook):
        mock_neo4j_hook_conn = mock_neo4j_hook.return_value
        mock_neo4j_hook_conn.run.return_value = [{"value": 10}]

        cypher = "RETURN 10 AS value"

        sensor = Neo4jSensor(
            task_id="neo4j_sensor_check",
            neo4j_conn_id="neo4j_default",
            cypher=cypher,
            success="value = 10",
        )
        with pytest.raises(AirflowException) as ctx:
            sensor.poke(mock.MagicMock())

        assert str(ctx.value) == "Parameter 'success' is not callable: 'value = 10'"

    @mock.patch("airflow.providers.neo4j.sensors.neo4j.Neo4jHook")
    def test_neo4j_sensor_poke_selector_default(self, mock_neo4j_hook):
        mock_neo4j_hook_conn = mock_neo4j_hook.return_value
        mock_neo4j_hook_conn.run.return_value = [{"first_name": "John", "last_name": "Doe"}]

        cypher = "MATCH (n:Person{id:'John Doe'}) RETURN n.first_name AS first_name, n.last_name AS last_name"

        sensor = Neo4jSensor(
            task_id="neo4j_sensor_check",
            neo4j_conn_id="neo4j_default",
            cypher=cypher,
            success=lambda x: x == "John",
        )
        assert sensor.poke(mock.MagicMock()) is True

    @mock.patch("airflow.providers.neo4j.sensors.neo4j.Neo4jHook")
    def test_neo4j_sensor_poke_selector_custom(self, mock_neo4j_hook):
        mock_neo4j_hook_conn = mock_neo4j_hook.return_value
        mock_neo4j_hook_conn.run.return_value = [{"first_name": "John", "last_name": "Doe"}]

        cypher = "MATCH (n:Person{id:'John Doe'}) RETURN n.first_name AS first_name, n.last_name AS last_name"

        sensor = Neo4jSensor(
            task_id="neo4j_sensor_check",
            neo4j_conn_id="neo4j_default",
            cypher=cypher,
            success=lambda x: x == "Doe",
            selector=lambda x: x[1],
        )
        assert sensor.poke(mock.MagicMock()) is True

    @mock.patch("airflow.providers.neo4j.sensors.neo4j.Neo4jHook")
    def test_neo4j_sensor_poke_selector_non_callable(self, mock_neo4j_hook):
        mock_neo4j_hook_conn = mock_neo4j_hook.return_value
        mock_neo4j_hook_conn.run.return_value = [{"first_name": "John", "last_name": "Doe"}]

        cypher = "MATCH (n:Person{id:'John Doe'}) RETURN n.first_name AS first_name, n.last_name AS last_name"

        sensor = Neo4jSensor(
            task_id="neo4j_sensor_check",
            neo4j_conn_id="neo4j_default",
            cypher=cypher,
            success=lambda x: x == "John",
            selector="first_name",
        )
        with pytest.raises(AirflowException) as ctx:
            sensor.poke(mock.MagicMock())

        assert str(ctx.value) == "Parameter 'selector' is not callable: 'first_name'"

    @mock.patch("airflow.providers.neo4j.sensors.neo4j.Neo4jHook")
    def test_neo4j_sensor_poke_templated_parameters(self, mock_neo4j_hook):
        mock_neo4j_hook_conn = mock_neo4j_hook.return_value
        mock_neo4j_hook_conn.run.return_value = [{"c": 100}]

        cypher = "MATCH (n:$node_label) RETURN COUNT(n) as total_person"

        sensor = Neo4jSensor(
            task_id="neo4j_sensor_check",
            neo4j_conn_id="neo4j_default",
            cypher=cypher,
            parameters={"node_label": "{{ target_node_label }}"},
            success=lambda x: x == 100,
        )

        sensor.render_template_fields(context={"target_node_label": "Person"})

        assert sensor.parameters == {"node_label": "Person"}
        assert sensor.poke(context=mock.MagicMock()) is True

        mock_neo4j_hook_conn.run.assert_called_once_with(
            cypher,
            {"node_label": "Person"},
        )
