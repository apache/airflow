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

import os
import tempfile

from airflow.decorators import task
from airflow.models.baseoperator import chain
from airflow.models.dag import DAG
from airflow.providers.standard.triggers.file import FileTrigger
from airflow.sdk.definitions.asset import Asset

file_path = tempfile.NamedTemporaryFile().name

with DAG(
    dag_id="example_create_file",
    catchup=False,
):

    @task
    def create_file():
        with open(file_path, "w") as file:
            file.write("This is an example file.\n")

    chain(create_file())

trigger = FileTrigger(filepath=file_path, poke_interval=10)
asset = Asset("example_asset", watchers=[trigger])

with DAG(
    dag_id="example_asset_with_watchers",
    schedule=[asset],
    catchup=False,
):

    @task
    def delete_file():
        if os.path.exists(file_path):
            os.remove(file_path)  # Delete the file

    chain(delete_file())
