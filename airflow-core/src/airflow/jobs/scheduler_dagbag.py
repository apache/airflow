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
from __future__ import annotations

from airflow.models.dagbag import DBDagBag, dag_cache_conf


class SchedulerDBDagBag(DBDagBag):
    """
    DagBag for the scheduler, reporting cache activity under ``scheduler.dag_bag``.

    Defaults to no size limit (``[scheduler] dag_cache_size = 0``). The scheduler reaches the
    cache through the Dag version of each active Dag run, so its working set is the versions with
    runs in flight, which no fixed count predicts well. A size limit would evict versions that are
    still being scheduled, whereas each re-check resets an entry's expiry, so the TTL reclaims
    each version only once its runs finish and it stops being requested.

    :meta private:
    """

    @classmethod
    def from_config(cls) -> SchedulerDBDagBag:
        """Build an instance from the ``[scheduler]`` cache options."""
        cache_size, cache_ttl = dag_cache_conf("scheduler", size_fallback=0, ttl_fallback=3600)
        return cls(
            load_op_links=False,
            cache_size=cache_size,
            cache_ttl=cache_ttl,
            stats_prefix="scheduler.dag_bag",
        )
