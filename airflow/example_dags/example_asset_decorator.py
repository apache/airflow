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

import pendulum

from airflow.assets import Asset
from airflow.decorators import dag, task
from airflow.decorators.assets import asset

asset1 = Asset(uri="s3://bucket/object", name="asset1")


@asset(uri="s3://bucket/asset_decorator", schedule=None)
def asset_decorator(self, context, asset1, asset2):
    pass


@dag(
    schedule=[Asset(name="asset_decorator", uri="s3://bucket/asset_decorator")],
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["consumes", "asset-scheduled"],
)
def consumes_asset_decorator():
    @task(outlets=[Asset("s3://consuming_1_task/asset_other.txt")])
    def process_nothing():
        pass

    process_nothing()


consumes_asset_decorator()
