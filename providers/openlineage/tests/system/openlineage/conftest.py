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

import pytest

from airflow.listeners import get_listener_manager
from airflow.providers.openlineage.plugins.listener import OpenLineageListener

from system.openlineage.transport.variable import VariableTransport


@pytest.fixture(autouse=True)
def set_transport_variable():
    lm = get_listener_manager()
    lm.clear()
    listener = OpenLineageListener()
    listener.adapter._client = listener.adapter.get_or_create_openlineage_client()
    listener.adapter._client.transport = VariableTransport({})
    lm.add_listener(listener)
    yield
    lm.clear()
