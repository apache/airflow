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

import logging
from logging.config import dictConfig

import pytest

from airflow.config_templates.airflow_local_settings import DEFAULT_LOGGING_CONFIG
from airflow.listeners.listener import get_listener_manager


@pytest.fixture(scope="module", autouse=True)
def reset_to_default_logging():
    """
    Initialize ``BaseTaskRunner`` might have side effect to another tests.
    This fixture reset back logging to default after execution of separate module  in this test package.
    """
    yield
    airflow_logger = logging.getLogger("airflow")
    airflow_logger.handlers = []
    dictConfig(DEFAULT_LOGGING_CONFIG)
    get_listener_manager().clear()
