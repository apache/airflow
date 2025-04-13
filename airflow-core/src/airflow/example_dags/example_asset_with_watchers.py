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
Example DAG for demonstrating the usage of event driven scheduling using assets and triggers.
"""

from __future__ import annotations

from airflow.providers.standard.triggers.file import FileDeleteTrigger
from airflow.sdk import DAG, Asset, AssetWatcher, chain, task

file_path = "/tmp/test"

trigger = FileDeleteTrigger(filepath=file_path)
asset = Asset("example_asset", watchers=[AssetWatcher(name="test_asset_watcher", trigger=trigger)])


with DAG(
    dag_id="example_asset_with_watchers",
    schedule=[asset],
    catchup=False,
):

    @task
    def test_task():
        print("Hello world")

    chain(test_task())
