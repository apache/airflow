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

from unittest import mock

import pytest


@pytest.fixture
def hook_conn(request):
    """
    Patch ``BaseHook.get_connection()`` by mock value.

    This fixture optionally parametrized, if ``param`` not set or empty it just mock method.
    If param is dictionary or :class:`~airflow.models.Connection` than return it,
    If param is exception than add side effect.
    Otherwise, it raises an error
    """
    from airflow.models import Connection

    try:
        conn = request.param
    except AttributeError:
        conn = None

    with mock.patch("airflow.hooks.base.BaseHook.get_connection") as m:
        if not conn:
            pass  # Don't do anything if param not specified or empty
        elif isinstance(conn, dict):
            m.return_value = Connection(**conn)
        elif not isinstance(conn, Connection):
            m.return_value = conn
        elif isinstance(conn, Exception):
            m.side_effect = conn
        else:
            raise TypeError(
                f"{request.node.name!r}: expected dict, Connection object or Exception, "
                f"but got {type(conn).__name__}"
            )

        yield m
