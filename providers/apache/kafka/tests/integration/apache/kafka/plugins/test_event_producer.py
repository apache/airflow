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

import json
import logging
import os
import subprocess
import time
import uuid

import pytest

from airflow.providers.apache.kafka.hooks.client import KafkaAdminClientHook
from airflow.providers.apache.kafka.hooks.consume import KafkaConsumerHook
from airflow.sdk import Connection
from airflow.utils.state import State

from tests_common.test_utils.integration_setup import (
    serialize_and_get_dags,
    start_scheduler,
    terminate_process,
    unpause_trigger_dag_and_get_run_id,
    wait_for_dag_run,
)

log = logging.getLogger(__name__)

client_config = {
    "bootstrap.servers": "broker:29092",
    "group.id": "kafka-event-producer-integration-test",
    "enable.auto.commit": False,
    "auto.offset.reset": "earliest",
}


def _wait_for_assignment(consumer, timeout: float = 10.0) -> None:
    """Block until the consumer has been assigned partitions by the broker."""
    deadline = time.monotonic() + timeout
    while not consumer.assignment():
        consumer.poll(0.5)
        if time.monotonic() > deadline:
            raise TimeoutError("Kafka consumer did not receive partition assignment in time")


@pytest.mark.integration("kafka")
@pytest.mark.backend("postgres")
class TestEventProducer:
    test_dir = os.path.dirname(os.path.abspath(__file__))
    dag_folder = os.path.join(test_dir, "dags")

    KAFKA_CONFIG_ID = "kafka_default"
    # This config id lives in the metadata DB only.
    DB_KAFKA_CONFIG_ID = "kafka_event_producer_db"
    # Use a unique topic per run to avoid errors on a re-run, in case
    # the previous teardown hasn't finished with the topic deletion.
    TOPIC = f"airflow.events.itest.{uuid.uuid4().hex[:8]}"

    @classmethod
    def setup_class(cls):
        # The pytest plugin strips AIRFLOW__*__* env vars (including the JWT secret set
        # by Breeze). Both the scheduler and api-server subprocesses must share the same
        # secret; otherwise each generates its own random key and token verification fails.
        os.environ["AIRFLOW__API_AUTH__JWT_SECRET"] = "test-secret-key-for-testing"
        os.environ["AIRFLOW__API_AUTH__JWT_ISSUER"] = "airflow"

        os.environ["AIRFLOW__SCHEDULER__STANDALONE_DAG_PROCESSOR"] = "False"
        os.environ["AIRFLOW__SCHEDULER__PROCESSOR_POLL_INTERVAL"] = "2"

        os.environ["AIRFLOW__CORE__DAGS_FOLDER"] = f"{cls.dag_folder}"
        os.environ["AIRFLOW__CORE__LOAD_EXAMPLES"] = "False"
        os.environ["AIRFLOW__CORE__UNIT_TEST_MODE"] = "False"

        os.environ["AIRFLOW__KAFKA_EVENT_PRODUCER__DAG_RUN_EVENTS_ENABLED"] = "True"
        os.environ["AIRFLOW__KAFKA_EVENT_PRODUCER__TASK_INSTANCE_EVENTS_ENABLED"] = "True"
        os.environ["AIRFLOW__KAFKA_EVENT_PRODUCER__TOPIC"] = cls.TOPIC
        os.environ["AIRFLOW__KAFKA_EVENT_PRODUCER__SOURCE"] = "dev-breeze"

        # Shared Kafka connection: used by the event producer plugin, the topic
        # creation/deletion, and the consumer.
        kafka_default_conn = Connection(
            conn_id=cls.KAFKA_CONFIG_ID,
            conn_type="kafka",
            extra=json.dumps(client_config),
        )
        os.environ["AIRFLOW_CONN_KAFKA_DEFAULT"] = kafka_default_conn.as_json()

        reset_command = ["airflow", "db", "reset", "--yes"]
        subprocess.run(reset_command, check=True, env=os.environ.copy())

        migrate_command = ["airflow", "db", "migrate"]
        subprocess.run(migrate_command, check=True, env=os.environ.copy())

        cls.dags = serialize_and_get_dags(dag_folder=cls.dag_folder)

        cls._admin = KafkaAdminClientHook(kafka_config_id=cls.KAFKA_CONFIG_ID)
        # Tuple positions are (name, partition, replication).
        cls._admin.create_topic([(cls.TOPIC, 1, 1)])

    @classmethod
    def teardown_class(cls):
        try:
            cls._admin.delete_topic([cls.TOPIC])
            time.sleep(2)  # let the broker finish the async delete
        except Exception as exc:
            log.warning("teardown: failed to delete topic %r: %s", cls.TOPIC, exc)

    @pytest.fixture
    def start_components(self):
        scheduler_process = None
        apiserver_process = None
        try:
            scheduler_process, apiserver_process = start_scheduler()
            yield scheduler_process, apiserver_process
        finally:
            terminate_process(scheduler_process)
            terminate_process(apiserver_process)

    @pytest.fixture
    def kafka_connection_in_metadata_db(self):
        """Give the plugin a Kafka connection that only the metadata DB has.

        setup_class exports AIRFLOW_CONN_KAFKA_DEFAULT, and the environment secrets backend
        answers from that variable without opening a DB session. This test needs the lookup
        to reach the metastore backend instead, and that only happens for a connection the
        environment does not have.

        The topic is separate too. The other test counts the messages on the class topic,
        and the runs here would add to that count.
        """
        topic = f"airflow.events.itest.{uuid.uuid4().hex[:8]}"
        self._admin.create_topic([(topic, 1, 1)])

        connection = Connection(
            conn_id=self.DB_KAFKA_CONFIG_ID,
            conn_type="kafka",
            extra=json.dumps(client_config),
        )
        subprocess.run(
            ["airflow", "connections", "add", self.DB_KAFKA_CONFIG_ID, "--conn-json", connection.as_json()],
            check=True,
            env=os.environ.copy(),
        )
        os.environ["AIRFLOW__KAFKA_EVENT_PRODUCER__KAFKA_CONFIG_ID"] = self.DB_KAFKA_CONFIG_ID
        os.environ["AIRFLOW__KAFKA_EVENT_PRODUCER__TOPIC"] = topic
        try:
            yield
        finally:
            os.environ["AIRFLOW__KAFKA_EVENT_PRODUCER__TOPIC"] = self.TOPIC
            os.environ.pop("AIRFLOW__KAFKA_EVENT_PRODUCER__KAFKA_CONFIG_ID", None)
            subprocess.run(
                ["airflow", "connections", "delete", self.DB_KAFKA_CONFIG_ID],
                check=False,
                env=os.environ.copy(),
            )
            try:
                self._admin.delete_topic([topic])
            except Exception as exc:
                log.warning("teardown: failed to delete topic %r: %s", topic, exc)

    @pytest.mark.execution_timeout(300)
    def test_scheduler_starts_every_queued_dag_run(self, kafka_connection_in_metadata_db):
        """The scheduler starts every queued Dag run, not only the first one.

        Both runs are triggered before the scheduler starts, so a single pass of
        _start_queued_dagruns has more than one row to get through. That pass is holding a
        DB session. If building the Kafka producer closes it, the rows the pass has not
        reached yet are detached, and the scheduler exits.
        """
        dag_id = "demo_dag"
        run_ids = [unpause_trigger_dag_and_get_run_id(dag_id=dag_id) for _ in range(2)]

        scheduler_process, apiserver_process = start_scheduler()
        try:
            states = [wait_for_dag_run(dag_id=dag_id, run_id=run_id, max_wait_time=60) for run_id in run_ids]
            scheduler_exit_code = scheduler_process.poll()
        finally:
            terminate_process(scheduler_process)
            terminate_process(apiserver_process)

        assert scheduler_exit_code is None, (
            f"the scheduler exited while starting queued Dag runs, exit code {scheduler_exit_code}"
        )
        assert states == [State.SUCCESS, State.SUCCESS], (
            f"the scheduler did not finish both queued Dag runs, final states: {states}"
        )

    @pytest.mark.execution_timeout(90)
    def test_dag_run_produces_event_messages(self, start_components):
        consumer = KafkaConsumerHook(topics=[self.TOPIC], kafka_config_id=self.KAFKA_CONFIG_ID).get_consumer()
        _wait_for_assignment(consumer)

        dag_id = "demo_dag"
        run_id = unpause_trigger_dag_and_get_run_id(dag_id=dag_id)

        state = wait_for_dag_run(dag_id=dag_id, run_id=run_id, max_wait_time=90)
        assert state == State.SUCCESS, f"Dag run did not complete successfully. Final state: {state}."

        expected_events = [
            "dag_run.running",
            "task_instance.running",
            "task_instance.success",
            "dag_run.success",
        ]

        try:
            messages = consumer.consume(num_messages=10, timeout=2.0)
        finally:
            consumer.close()

        topic_msg_bodies: list[dict] = []
        for message in messages:
            topic_msg_bodies.append(json.loads(message.value()))

        assert len(topic_msg_bodies) == len(expected_events)

        for body in topic_msg_bodies:
            assert body["dag_id"] == dag_id
            assert body["run_id"] == run_id
            if body["event"].startswith("task_instance."):
                assert body["task_id"] == "task1"

        received_events = [body["event"] for body in topic_msg_bodies]
        assert sorted(received_events) == sorted(expected_events)
