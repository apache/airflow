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
Fixtures for integration tests against a real Polaris server.

In the Airflow tree the server is the Breeze integration ``polaris``
(``breeze --integration polaris``), reachable as ``http://polaris:8181``. Outside Breeze, run
the ``polaris`` service of ``integration-polaris.yml`` with port 8181 published and set
``INTEGRATION_POLARIS=true`` and ``POLARIS_INTEGRATION_URL=http://localhost:8181``. Credentials
are the server's bootstrap ``root`` / ``s3cr3t`` in realm ``POLARIS``.
"""

from __future__ import annotations

import json
import uuid

import pytest

from airflow.models import Connection
from airflow.providers.apache.polaris.hooks.polaris import PolarisCatalogHook, PolarisManagementHook

from integration.apache.polaris import CLIENT_ID, CLIENT_SECRET, REALM, URL


@pytest.fixture
def conn(create_connection_without_db):
    """Create a ``polaris`` connection to the integration server; returns its conn_id."""

    def _create(conn_id: str = "polaris_it", login=CLIENT_ID, password=CLIENT_SECRET, **extra):
        extra.setdefault("realm", REALM)
        create_connection_without_db(
            Connection(
                conn_id=conn_id,
                conn_type="polaris",
                host=URL,
                login=login,
                password=password,
                extra=json.dumps(extra),
            )
        )
        return conn_id

    return _create


@pytest.fixture
def catalog_spec():
    """Return the definition of a FILE-backed catalog with the given name."""

    def _spec(name: str) -> dict:
        location = f"file:///tmp/polaris/{name}"
        return {
            "type": "INTERNAL",
            "name": name,
            "properties": {"default-base-location": location},
            "storageConfigInfo": {"storageType": "FILE", "allowedLocations": [location]},
        }

    return _spec


@pytest.fixture
def catalog_name(conn, catalog_spec):
    """A fresh catalog, deleted (with its contents) afterwards."""
    name = f"it_{uuid.uuid4().hex[:8]}"
    hook = PolarisManagementHook(conn())
    hook.create_catalog(catalog_spec(name))
    # Managing namespaces, tables and policies needs only root's catalog_admin role; pyiceberg's
    # table access (vended credentials) also needs content privileges.
    hook.add_grant(name, "catalog_admin", {"type": "catalog", "privilege": "CATALOG_MANAGE_CONTENT"})
    yield name
    catalog_hook = PolarisCatalogHook(conn(default_catalog=name))

    def drop_namespace(namespace: list[str]) -> None:
        for child in catalog_hook.list_namespaces(parent=namespace):
            drop_namespace(child)
        for table in catalog_hook.list_tables(namespace):
            catalog_hook.drop_table(namespace, table.name, purge=True, if_missing="skip")
        for ident in catalog_hook.list_policies(namespace):
            catalog_hook.drop_policy(namespace, ident.name, detach_all=True, if_missing="skip")
        catalog_hook.drop_namespace(namespace, if_missing="skip")

    for namespace in catalog_hook.list_namespaces():
        drop_namespace(namespace)
    hook.delete_catalog(name, if_missing="skip")
