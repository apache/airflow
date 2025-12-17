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
"""HBase datasets."""

from __future__ import annotations

from urllib.parse import urlunparse

from airflow.datasets import Dataset


def hbase_table_dataset(
    host: str,
    port: int = 9090,
    table_name: str = "",
    extra: dict | None = None,
) -> Dataset:
    """
    Create a Dataset for HBase table.
    
    :param host: HBase Thrift server host
    :param port: HBase Thrift server port
    :param table_name: Name of the HBase table
    :param extra: Extra parameters
    :return: Dataset object
    """
    return Dataset(
        uri=urlunparse((
            "hbase",
            f"{host}:{port}",
            f"/{table_name}",
            None,
            None,
            None,
        ))
    )