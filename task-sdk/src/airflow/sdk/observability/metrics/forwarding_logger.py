#
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
"""Stats backend that forwards aggregated metrics to the supervisor."""

from __future__ import annotations

import logging
import threading
from typing import TYPE_CHECKING

from airflow.sdk._shared.observability.metrics.aggregating_logger import AggregatedMetrics, AggregatingLogger
from airflow.sdk.api.datamodels._generated import ForwardMetric, MetricKind
from airflow.sdk.execution_time.comms import ForwardMetrics

if TYPE_CHECKING:
    from airflow.sdk.execution_time.comms import CommsDecoder

log = logging.getLogger(__name__)

FLUSH_INTERVAL_SECONDS = 30


def _to_forward_metrics(aggregated: AggregatedMetrics) -> list[ForwardMetric]:
    return [
        *(
            ForwardMetric(kind=MetricKind.COUNTER, name=name, tags=dict(tags) or None, value=value)
            for (name, tags), value in aggregated.counters.items()
        ),
        *(
            ForwardMetric(kind=MetricKind.GAUGE, name=name, tags=dict(tags) or None, value=value, delta=delta)
            for (name, tags), (value, delta) in aggregated.gauges.items()
        ),
        *(
            ForwardMetric(kind=MetricKind.TIMING, name=name, tags=dict(tags) or None, values=values)
            for (name, tags), values in aggregated.timings.items()
        ),
    ]


class ForwardingLogger(AggregatingLogger):
    """Aggregate metrics in memory and ship them to the supervisor in batches."""

    def __init__(self, comms: CommsDecoder):
        super().__init__(on_large_batch=self.flush)
        self._comms = comms
        self._stop_flushing = threading.Event()
        self._flush_thread = threading.Thread(
            target=self._flush_periodically, name="stats-forwarder", daemon=True
        )
        self._flush_thread.start()

    def flush(self) -> None:
        """Send everything accumulated so far to the supervisor."""
        # Metrics must never break the task, so nothing in here is allowed to propagate.
        try:
            metrics = _to_forward_metrics(self.drain())
            if metrics:
                self._comms.send(ForwardMetrics(metrics=metrics))
        except Exception:
            log.warning("Could not forward metrics to the supervisor; dropping them", exc_info=True)

    def close(self) -> None:
        """Stop the periodic flush and send whatever is left; called once at process exit."""
        self._stop_flushing.set()
        self.flush()

    def _flush_periodically(self) -> None:
        while not self._stop_flushing.wait(FLUSH_INTERVAL_SECONDS):
            self.flush()
