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

import os

import pytest

from airflow.utils import db


@pytest.fixture(autouse=True)
def reset_environment():
    """
    Resets env variables.
    """
    init_env = os.environ
    yield
    changed_env = os.environ
    for key, value in changed_env.items():
        if key not in init_env:
            del os.environ[key]

        init_value = init_env[key]
        if value != init_value:
            os.environ[key] = init_value


@pytest.fixture()
def reset_db():
    """
    Resets Airflow db.
    """
    db.resetdb()
    yield
    pass
