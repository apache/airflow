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

from airflow.sdk import DAG, Asset, task

hotload_raw_events = Asset(
    uri="file://cs5150/hotload/raw_events.csv",
    name="cs5150_hotload_raw_events",
)
hotload_curated_events = Asset(
    uri="file://cs5150/hotload/curated_events.csv",
    name="cs5150_hotload_curated_events",
)


with DAG(
    dag_id="cs5150_hotload_event_pipeline",
    schedule=None,
    tags=["cs5150", "hotload", "lineage-demo"],
):
    """Small DAG used to verify whether Breeze hot-loads new DAG files."""

    @task(outlets=[hotload_raw_events])
    def collect_events():
        print("Produced hotload raw events")

    @task(inlets=[hotload_raw_events], outlets=[hotload_curated_events])
    def curate_events():
        print("Produced hotload curated events")

    collect_events() >> curate_events()
