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

    def _wait_for_kafka_consumer_group(self, compose_instance, timeout: int = 60, check_interval: int = 3):
        """Poll until any Kafka consumer group is registered, indicating the trigger is active."""
        start = time.monotonic()
        while time.monotonic() - start < timeout:
            stdout, _, _ = compose_instance.exec_in_container(
                command=[
                    "kafka-consumer-groups",
                    "--bootstrap-server",
                    "broker:29092",
                    "--list",
                ],
                service_name="broker",
            )
            output = stdout.decode() if isinstance(stdout, bytes) else stdout
            # Any non-empty group listing means the trigger's consumer has registered
            groups = [line.strip() for line in output.strip().splitlines() if line.strip()]
            if groups:
                return
            time.sleep(check_interval)
        raise TimeoutError(f"No Kafka consumer group registered within {timeout}s")

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
        stdout, _, _ = compose_instance.exec_in_container(
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

    def _wait_for_topic_offset(
        self, compose_instance, topic: str, expected_offset: int, timeout: int = 30, check_interval: int = 2
    ) -> int:
        """Poll until *topic* reaches *expected_offset*, then return the offset."""
        start = time.monotonic()
        offset = self._get_topic_offset(compose_instance, topic)
        while offset < expected_offset and time.monotonic() - start < timeout:
            time.sleep(check_interval)
            offset = self._get_topic_offset(compose_instance, topic)
        return offset

    def _get_task_states(self, run_id: str) -> dict[str, str]:
        """Return a mapping of task_id -> state for a consumer DAG run."""
        response = self.airflow_client.get_task_instances(CONSUMER_DAG_ID, run_id)
        return {ti["task_id"]: ti["state"] for ti in response["task_instances"]}

    def test_producer_triggers_consumer_and_kafka_offsets(self, compose_instance):
        """Trigger the producer once and verify 9 consumer runs and Kafka offsets.

        Steps:
        1. Unpause the consumer DAG so the triggerer starts the AssetWatcher.
        2. Wait for the Kafka MessageQueueTrigger to begin polling.
        3. Trigger the producer DAG and wait for it to succeed.
        4. Wait for 9 consumer DAG runs to reach a terminal state.
        5. All 9 DAG runs succeed. Verify task-level behavior:
           - 1 run has a failed ``process_message`` task and executes ``handle_dlq``.
           - 8 runs succeed on ``process_message`` and skip ``handle_dlq``.
        6. Verify that the ``fizz_buzz`` topic has offset 9 (all messages produced).
        7. Verify that the ``dlq`` topic has offset 1 (the malformed message).
        """
        # 1. Unpause consumer so the triggerer registers the AssetWatcher
        self.airflow_client.un_pause_dag(CONSUMER_DAG_ID)

        # 2. Wait for the triggerer to start the MessageQueueTrigger and subscribe.
        #    The trigger uses poll_interval=1 and auto.offset.reset=latest so it
        #    must be actively polling before the producer writes.
        self._wait_for_kafka_consumer_group(compose_instance)

        # 3. Trigger producer and wait for it to complete
        producer_state = self.airflow_client.trigger_dag_and_wait(PRODUCER_DAG_ID)
        assert producer_state == "success", f"Producer DAG did not succeed. Final state: {producer_state}"

        # 4. Wait for all 9 consumer DAG runs
        consumer_runs = self._wait_for_consumer_dag_runs(expected_count=EXPECTED_CONSUMER_RUNS)
        assert len(consumer_runs) == EXPECTED_CONSUMER_RUNS, (
            f"Expected {EXPECTED_CONSUMER_RUNS} consumer runs, got {len(consumer_runs)}"
        )

        # 5. All 9 DAG runs should succeed
        for run in consumer_runs:
            assert run["state"] == "success", (
                f"Expected all consumer runs to succeed, but run {run['dag_run_id']} "
                f"has state '{run['state']}'"
            )

        # 6. Verify task-level behavior per run:
        #    - 1 run: process_message fails (malformed message), handle_dlq executes
        #    - 8 runs: process_message succeeds, handle_dlq is skipped
        dlq_runs = []
        normal_runs = []
        for run in consumer_runs:
            ti_states = self._get_task_states(run["dag_run_id"])
            if ti_states.get("process_message") == "failed":
                dlq_runs.append(run["dag_run_id"])
                assert ti_states.get("should_handle_dlq") == "success", (
                    f"Run {run['dag_run_id']}: expected should_handle_dlq=success, "
                    f"got '{ti_states.get('should_handle_dlq')}'"
                )
                assert ti_states.get("handle_dlq") == "success", (
                    f"Run {run['dag_run_id']}: expected handle_dlq=success, "
                    f"got '{ti_states.get('handle_dlq')}'"
                )
            else:
                normal_runs.append(run["dag_run_id"])
                assert ti_states.get("process_message") == "success", (
                    f"Run {run['dag_run_id']}: expected process_message=success, "
                    f"got '{ti_states.get('process_message')}'"
                )
                assert ti_states.get("should_handle_dlq") == "success", (
                    f"Run {run['dag_run_id']}: expected should_handle_dlq=success, "
                    f"got '{ti_states.get('should_handle_dlq')}'"
                )
                assert ti_states.get("handle_dlq") == "skipped", (
                    f"Run {run['dag_run_id']}: expected handle_dlq=skipped, "
                    f"got '{ti_states.get('handle_dlq')}'"
                )

        assert len(dlq_runs) == 1, (
            f"Expected exactly 1 run with failed process_message (DLQ path), got {len(dlq_runs)}: {dlq_runs}"
        )
        assert len(normal_runs) == 8, (
            f"Expected 8 runs with successful process_message, got {len(normal_runs)}: {normal_runs}"
        )

        # 7. Verify Kafka topic offsets
        #    The DLQ message is produced by the last consumer run to complete, so
        #    kafka-get-offsets may briefly report a stale offset; poll with a short timeout.
        fizz_buzz_offset = self._wait_for_topic_offset(
            compose_instance, "fizz_buzz", EXPECTED_FIZZ_BUZZ_OFFSET
        )
        assert fizz_buzz_offset == EXPECTED_FIZZ_BUZZ_OFFSET, (
            f"Expected fizz_buzz offset {EXPECTED_FIZZ_BUZZ_OFFSET}, got {fizz_buzz_offset}"
        )

        dlq_offset = self._wait_for_topic_offset(compose_instance, "dlq", EXPECTED_DLQ_OFFSET)
        assert dlq_offset == EXPECTED_DLQ_OFFSET, (
            f"Expected dlq offset {EXPECTED_DLQ_OFFSET}, got {dlq_offset}"
        )
