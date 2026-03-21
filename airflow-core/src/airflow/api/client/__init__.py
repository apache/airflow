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
"""API Client that allows interacting with Airflow API."""

from __future__ import annotations

import warnings

from airflow.api.client.local_client import Client
from airflow.api.client.local_rest_client import LocalRESTClient


def get_current_api_client() -> Client:
    warnings.warn(
        "get_current_api_client() is deprecated. Use get_local_rest_client() instead, "
        "which uses the REST API rather than direct database access.",
        DeprecationWarning,
        stacklevel=2,
    )
    return Client()


def get_local_rest_client(*, process_type: str = "unknown") -> LocalRESTClient:
    """
    Get a REST API client for use inside trusted Airflow processes.

    This client automatically authenticates using a ``SystemUser`` and
    requires no user credentials. It is intended for plugins and Airflow
    component code running in processes that have metadata database access
    (scheduler, DAG processor, triggerer). It is **not** available from
    workers or task code, which do not have direct database access in
    Airflow 3.

    Example::

        from airflow.api.client import get_local_rest_client

        client = get_local_rest_client()
        client.pools.create(name="my_pool", slots=5)
        pools = client.pools.list()

    :param process_type: Identifier for the calling process (e.g. ``"dag_processor"``).
    """
    return LocalRESTClient(process_type=process_type)
