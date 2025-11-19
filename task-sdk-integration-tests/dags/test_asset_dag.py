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
"""
Test DAG for asset operations.

This file contains:
- asset_producer_dag: A DAG that produces an asset
- asset_consumer_dag: A DAG that is triggered by the asset
"""

from __future__ import annotations

from airflow.sdk import DAG, Asset, task

test_asset = Asset(uri="test://asset1", name="test_asset")

with DAG(
    dag_id="asset_producer_dag",
    description="DAG that produces an asset for testing",
    schedule=None,
    catchup=False,
) as producer_dag:

    @task(outlets=[test_asset])
    def produce_asset():
        """Task that produces the test asset."""
        print("Producing test asset")
        return "asset_produced"

    produce_asset()
