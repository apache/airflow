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

import threading
from unittest import mock

import pytest

from airflow.sdk._shared.observability.metrics.aggregating_logger import MAX_METRICS
from airflow.sdk.api.datamodels._generated import ForwardMetric, MetricKind
from airflow.sdk.execution_time.comms import CommsDecoder, ForwardMetrics
from airflow.sdk.observability.metrics import forwarding_logger
from airflow.sdk.observability.metrics.forwarding_logger import ForwardingLogger


@pytest.fixture
def comms():
    return mock.Mock(spec=CommsDecoder)


@pytest.fixture
def backend(comms):
    backend = ForwardingLogger(comms=comms)
    yield backend
    backend.close()


class TestForwardingLogger:
    def test_flush_sends_one_message_with_every_kind(self, backend, comms):
        backend.incr("ti_successes", tags={"dag_id": "dag"})
        backend.incr("ti_successes", tags={"dag_id": "dag"})
        backend.gauge("pool.open_slots", 3, delta=True)
        backend.timing("task.duration", 12)
        backend.timing("task.duration", 30)

        backend.flush()

        comms.send.assert_called_once_with(
            ForwardMetrics(
                metrics=[
                    ForwardMetric(
                        kind=MetricKind.COUNTER, name="ti_successes", tags={"dag_id": "dag"}, value=2
                    ),
                    ForwardMetric(kind=MetricKind.GAUGE, name="pool.open_slots", value=3, delta=True),
                    ForwardMetric(kind=MetricKind.TIMING, name="task.duration", values=[12.0, 30.0]),
                ]
            )
        )

    def test_flush_with_nothing_accumulated_sends_nothing(self, backend, comms):
        backend.flush()

        comms.send.assert_not_called()

    def test_flush_drops_the_batch_when_sending_fails(self, backend, comms):
        comms.send.side_effect = OSError("socket closed")
        backend.incr("ti_successes")

        backend.flush()

        comms.send.side_effect = None
        backend.flush()
        comms.send.assert_called_once()

    def test_close_sends_what_is_left_and_stops_the_thread(self, comms):
        backend = ForwardingLogger(comms=comms)
        backend.incr("ti_successes")

        backend.close()

        comms.send.assert_called_once_with(
            ForwardMetrics(metrics=[ForwardMetric(kind=MetricKind.COUNTER, name="ti_successes", value=1)])
        )
        backend._flush_thread.join(timeout=5)
        assert not backend._flush_thread.is_alive()

    def test_large_batch_is_flushed_without_waiting(self, backend, comms):
        for i in range(MAX_METRICS):
            backend.incr(f"metric_{i}")

        comms.send.assert_called_once()
        assert len(comms.send.call_args.args[0].metrics) == MAX_METRICS

    @mock.patch.object(forwarding_logger, "FLUSH_INTERVAL_SECONDS", 0.01)
    def test_batches_are_flushed_periodically(self, comms):
        sent = threading.Event()
        comms.send.side_effect = lambda _: sent.set()
        backend = ForwardingLogger(comms=comms)
        try:
            backend.incr("ti_successes")

            assert sent.wait(timeout=5)
        finally:
            backend.close()
