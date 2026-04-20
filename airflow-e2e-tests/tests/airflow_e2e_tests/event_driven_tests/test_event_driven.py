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
"""E2E tests for the event-driven DAG pattern using Apache Kafka.

The producer DAG sends 8 valid orders + 1 malformed message to the ``fizz_buzz``
Kafka topic. The consumer DAG is scheduled on an Asset with a Kafka AssetWatcher,
so each message triggers a separate consumer DAG run (9 total).
"""

from __future__ import annotations

import time

from airflow_e2e_tests.e2e_test_utils.clients import AirflowClient

PRODUCER_DAG_ID = "event_driven_producer"
CONSUMER_DAG_ID = "event_driven_consumer"
# 8 valid orders + 1 malformed message
EXPECTED_CONSUMER_RUNS = 9
EXPECTED_FIZZ_BUZZ_OFFSET = 9
EXPECTED_DLQ_OFFSET = 1


def _parse_topic_offset(raw_output: str, topic: str) -> int:
    """Parse ``kafka-get-offsets`` output (``topic:partition:offset``) and return the offset."""
    for line in raw_output.strip().splitlines():
        parts = line.strip().split(":")
        if len(parts) == 3 and parts[0] == topic:
            return int(parts[2])
    raise ValueError(f"Could not find offset for topic '{topic}' in output:\n{raw_output}")


class TestEventDrivenDag:
    """Test the Kafka-based event-driven producer/consumer DAG pair."""

    airflow_client = AirflowClient()

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _wait_for_kafka_consumer_group(
        self, compose_instance, group_id: str, timeout: int = 60, check_interval: int = 3
    ):
        """Poll until the Kafka consumer group is registered, indicating the trigger is active."""
        start = time.monotonic()
        while time.monotonic() - start < timeout:
            stdout, _ = compose_instance.exec_in_container(
                command=[
                    "kafka-consumer-groups",
                    "--bootstrap-server",
                    "broker:29092",
                    "--list",
                ],
                service_name="broker",
            )
            output = stdout.decode() if isinstance(stdout, bytes) else stdout
            if group_id in output:
                return
            time.sleep(check_interval)
        raise TimeoutError(f"Kafka consumer group '{group_id}' not registered within {timeout}s")

    def _wait_for_consumer_dag_runs(
        self, expected_count: int, timeout: int = 600, check_interval: int = 10
    ) -> list[dict]:
        """Poll until *expected_count* consumer DAG runs reach a terminal state."""
        start = time.monotonic()
        while time.monotonic() - start < timeout:
            response = self.airflow_client.list_dag_runs(CONSUMER_DAG_ID)
            runs = response.get("dag_runs", [])
            terminal_runs = [r for r in runs if r["state"] in ("success", "failed")]
            if len(terminal_runs) >= expected_count:
                # Return only the most recent expected_count runs to avoid
                # assertion failures if extra runs exist from retries or prior state.
                terminal_runs.sort(
                    key=lambda r: (
                        r.get("end_date") or "",
                        r.get("start_date") or "",
                        r.get("logical_date") or "",
                        r.get("dag_run_id") or "",
                    ),
                    reverse=True,
                )
                return terminal_runs[:expected_count]
            time.sleep(check_interval)

        # Timed out — gather diagnostics
        response = self.airflow_client.list_dag_runs(CONSUMER_DAG_ID)
        runs = response.get("dag_runs", [])
        states = {r["dag_run_id"]: r["state"] for r in runs}
        raise TimeoutError(
            f"Expected {expected_count} terminal consumer DAG runs within {timeout}s, "
            f"but only found {len([r for r in runs if r['state'] in ('success', 'failed')])}. "
            f"Run states: {states}"
        )

    def _get_topic_offset(self, compose_instance, topic: str) -> int:
        """Return the current end-offset of *topic* via ``kafka-get-offsets`` inside the broker."""
        stdout, _ = compose_instance.exec_in_container(
            command=[
                "kafka-get-offsets",
                "--bootstrap-server",
                "broker:29092",
                "--topic",
                topic,
            ],
            service_name="broker",
        )
        output = stdout.decode() if isinstance(stdout, bytes) else stdout
        return _parse_topic_offset(output, topic)

    # ------------------------------------------------------------------
    # Test
    # ------------------------------------------------------------------

    def test_producer_triggers_consumer_and_kafka_offsets(self, compose_instance):
        """Trigger the producer once and verify 9 consumer runs and Kafka offsets.

        Steps:
        1. Unpause the consumer DAG so the triggerer starts the AssetWatcher.
        2. Wait for the Kafka MessageQueueTrigger to begin polling.
        3. Trigger the producer DAG and wait for it to succeed.
        4. Wait for 9 consumer DAG runs to reach a terminal state.
        5. Verify that the ``fizz_buzz`` topic has offset 9 (all messages produced).
        6. Verify that the ``dlq`` topic has offset 1 (the malformed message).
        """
        # 1. Unpause consumer so the triggerer registers the AssetWatcher
        self.airflow_client.un_pause_dag(CONSUMER_DAG_ID)

        # 2. Wait for the triggerer to start the MessageQueueTrigger and subscribe.
        #    The trigger uses poll_interval=1 and auto.offset.reset=latest so it
        #    must be actively polling before the producer writes.
        self._wait_for_kafka_consumer_group(compose_instance, "kafka_default_group")

        # 3. Trigger producer and wait for it to complete
        producer_state = self.airflow_client.trigger_dag_and_wait(PRODUCER_DAG_ID)
        assert producer_state == "success", f"Producer DAG did not succeed. Final state: {producer_state}"

        # 4. Wait for all 9 consumer DAG runs
        consumer_runs = self._wait_for_consumer_dag_runs(expected_count=EXPECTED_CONSUMER_RUNS)
        assert len(consumer_runs) == EXPECTED_CONSUMER_RUNS, (
            f"Expected {EXPECTED_CONSUMER_RUNS} consumer runs, got {len(consumer_runs)}"
        )

        # 5. Verify consumer run outcomes:
        #    - 8 runs process valid orders and succeed
        #    - 1 run hits the malformed message, process_message fails after retries,
        #      then handle_dlq sends it to the DLQ; the overall run is still "failed"
        success_runs = [r for r in consumer_runs if r["state"] == "success"]
        failed_runs = [r for r in consumer_runs if r["state"] == "failed"]
        assert len(success_runs) == 8, (
            f"Expected 8 successful consumer runs, got {len(success_runs)}. "
            f"States: {[(r['dag_run_id'], r['state']) for r in consumer_runs]}"
        )
        assert len(failed_runs) == 1, (
            f"Expected 1 failed consumer run (malformed message), got {len(failed_runs)}. "
            f"States: {[(r['dag_run_id'], r['state']) for r in consumer_runs]}"
        )

        # 6. Verify Kafka topic offsets
        fizz_buzz_offset = self._get_topic_offset(compose_instance, "fizz_buzz")
        assert fizz_buzz_offset == EXPECTED_FIZZ_BUZZ_OFFSET, (
            f"Expected fizz_buzz offset {EXPECTED_FIZZ_BUZZ_OFFSET}, got {fizz_buzz_offset}"
        )

        dlq_offset = self._get_topic_offset(compose_instance, "dlq")
        assert dlq_offset == EXPECTED_DLQ_OFFSET, (
            f"Expected dlq offset {EXPECTED_DLQ_OFFSET}, got {dlq_offset}"
        )
