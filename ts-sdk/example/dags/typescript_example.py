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

from airflow.sdk import Asset, dag, task


@task
def python_start():
    return "hello from Python"


@task.stub(queue="typescript")
def build_message(): ...


@task.stub(queue="typescript")
def read_connection(): ...


@task.stub(queue="typescript")
def write_and_delete_variable(): ...


# The TypeScript task cannot see this inlet. Declaring it keeps the asset active, and the
# Execution API only reads and writes the state of active assets.
typescript_example_orders = Asset(name="typescript_example_orders", uri="x-typescript-example://orders")


@task.stub(queue="typescript", inlets=[typescript_example_orders])
def use_asset_state_store(): ...


@dag(dag_id="typescript_example", schedule=None, catchup=False, tags=["typescript", "example"])
def typescript_example():
    start = python_start()
    message = build_message()
    read_connection()
    write_and_delete_variable()
    use_asset_state_store()

    start >> message


typescript_example()
