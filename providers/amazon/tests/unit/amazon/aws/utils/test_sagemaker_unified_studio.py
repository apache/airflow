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

import os

from airflow.providers.amazon.aws.utils.sagemaker_unified_studio import is_local_runner, workflows_env_key


def test_is_local_runner_false():
    assert not is_local_runner()


def test_is_local_runner_true():
    os.environ[workflows_env_key] = "Local"
    assert is_local_runner()


def test_is_local_runner_false_with_env_var():
    os.environ[workflows_env_key] = "False"
    assert not is_local_runner()


def test_is_local_runner_false_with_env_var_empty():
    os.environ[workflows_env_key] = ""
    assert not is_local_runner()


def test_is_local_runner_false_with_env_var_invalid():
    os.environ[workflows_env_key] = "random string"
    assert not is_local_runner()


def test_is_local_runner_false_with_string_int():
    os.environ[workflows_env_key] = "1"
    assert not is_local_runner()
