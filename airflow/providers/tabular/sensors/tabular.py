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

from datetime import datetime
from functools import cached_property
from typing import TYPE_CHECKING

from airflow.providers.tabular.hooks.tabular import TabularHook
from airflow.sensors.base import BaseSensorOperator

if TYPE_CHECKING:
    from airflow.utils.context import Context

PROPERTY_KEY_VTTS = "kafka.connect.vtts"


class TabularVttsSensor(BaseSensorOperator):
    """
    Sensor to block on tables written by the Kafka Connect to Iceberg sink.

    :param identifier: The identifier of the table.
    :param tabular_conn_id: The connection used to connect to Tabular.
    :param num_snapshots: Number of most recent snapshots to check

    https://github.com/tabular-io/iceberg-kafka-connect

    Every Iceberg commit performed by the sink includes some snapshot
    summary properties. As mentioned above, one such property is the
    control topic offsets. Another is the unique UUID assigned to every
    commit. Finally, there is a VTTS (valid-through timestamp) property
    indicating through what timestamp records have been fully processed,
    i.e. all records processed from then on will have a timestamp greater
    than the VTTS. This is calculated by taking the maximum timestamp of
    records processed from each topic partition, and taking the minimum of
    these. If any partitions were not processed as part of the commit then
    the VTTS is not set.
    """

    def __init__(
        self,
        *,
        identifier: str,
        tabular_conn_id: str = "tabular_default",
        num_snapshots: int = 10,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.identifier = identifier
        self.tabular_conn_id = tabular_conn_id
        self.num_snapshots = num_snapshots

    @cached_property
    def hook(self) -> TabularHook:
        """Create and return a TabularHook."""
        return TabularHook(tabular_conn_id=self.tabular_conn_id)

    def poke(self, context: Context) -> bool:
        """
        Pokes until the job has successfully finished.

        :param context: The task context during execution.
        :return: True if it succeeded and False if not.
        """
        catalog = self.hook.load_rest_catalog()
        table = catalog.load_table(identifier=self.identifier)

        snapshot = table.current_snapshot()

        for _ in range(self.num_snapshots):
            if snapshot is not None and (summary := snapshot.summary):
                if vtts := summary.additional_properties.get(PROPERTY_KEY_VTTS):
                    dt = datetime.fromtimestamp(int(vtts) / 1000.0)
                    self.log.info("Found VTTS: %s", dt)
                    diff = int((dt - datetime.now()).total_seconds())

                    if diff < 0:
                        self.log.info("VTTS passed %s seconds ago", abs(diff))
                        return True
                    else:
                        self.log.info("Waiting on VTTS, lagging %s seconds behind", diff)
                        return False
                else:
                    snapshot_id = "" if snapshot.parent_snapshot_id is None else f"({snapshot.snapshot_id})"
                    self.log.warning(
                        f"Key '%s' not found on snapshot %s summary", PROPERTY_KEY_VTTS, snapshot_id)

                # Since there can be another operation in between, check the parent
                if snapshot.parent_snapshot_id is not None:
                    snapshot = table.snapshot_by_id(snapshot.parent_snapshot_id)
                else:
                    self.log.info("Snapshot does not have a parent: %s", snapshot)
                    return False
            else:
                self.log.info("Could not find snapshot summary, retrying later")

        return False
