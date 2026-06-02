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
"""
Example Dag for event-driven scheduling using Assets and AssetWatchers.

Three watchers demonstrate the two trigger patterns in one place:

* The first watcher uses ``FileDeleteTrigger`` for a single specific path —
  one watcher, one independent poll loop in the triggerer.
* The other two use ``DirectoryFileDeleteTrigger`` with a matching
  ``shared_stream_key`` of ``("directory-scan", directory, poke_interval)``;
  the triggerer runs **one** directory listing loop for the pair and
  broadcasts the result to both. Each still fires only for its own filename.

The Dag runs when any of the three watchers' assets is updated. Touch
``/tmp/test``, ``/tmp/region-flags/us.flag``, or ``/tmp/region-flags/eu.flag``
to trigger a run.
"""

from __future__ import annotations

from airflow.providers.standard.triggers.file import (
    DirectoryFileDeleteTrigger,
    FileDeleteTrigger,
)
from airflow.sdk import DAG, Asset, AssetWatcher, chain, task

# Independent single-file watcher — has its own poll loop in the triggerer.
single_file_trigger = FileDeleteTrigger(filepath="/tmp/test")
single_file_asset = Asset(
    "example_asset",
    watchers=[AssetWatcher(name="test_asset_watcher", trigger=single_file_trigger)],
)

# Shared-stream watchers — same directory + poke interval, so the triggerer
# runs one scan for both. Each watcher's ``filter_shared_stream`` matches on
# its own filename and ``unlink``s the flag file as a subscriber-side effect.
us_trigger = DirectoryFileDeleteTrigger(directory="/tmp/region-flags", filename="us.flag", poke_interval=5.0)
eu_trigger = DirectoryFileDeleteTrigger(directory="/tmp/region-flags", filename="eu.flag", poke_interval=5.0)
us_asset = Asset(
    "region_us_flag",
    watchers=[AssetWatcher(name="us_flag_watcher", trigger=us_trigger)],
)
eu_asset = Asset(
    "region_eu_flag",
    watchers=[AssetWatcher(name="eu_flag_watcher", trigger=eu_trigger)],
)


with DAG(
    dag_id="example_asset_with_watchers",
    schedule=[single_file_asset, us_asset, eu_asset],
    catchup=False,
    tags=["example"],
):

    @task
    def test_task():
        print("Hello world")

    chain(test_task())
