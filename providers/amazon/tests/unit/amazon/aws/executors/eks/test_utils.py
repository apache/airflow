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

from airflow.providers.amazon.aws.executors.eks.utils import (
    CONFIG_DEFAULTS,
    CONFIG_GROUP_NAME,
    AllEksConfigKeys,
)
from airflow.providers.amazon.get_provider_info import get_provider_info


def test_config_keys_and_defaults_match_the_provider_config_section():
    options = get_provider_info()["config"][CONFIG_GROUP_NAME]["options"]

    assert set(AllEksConfigKeys()) == set(options)
    assert {key: options[key]["default"] for key in CONFIG_DEFAULTS} == CONFIG_DEFAULTS
