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
from botocore import UNSIGNED
from botocore.config import Config

from airflow.providers.amazon.aws.utils.boto import (
    build_botocore_config,
    deserialize_botocore_config_params,
    serialize_botocore_config_params,
)


def test_serde_botocore_config():
    params = {"signature_version": UNSIGNED, "retries": {"mode": "standard", "max_attempts": 10}}

    ser_params = serialize_botocore_config_params(**params)
    assert ser_params["signature_version"] == "unsigned"
    assert ser_params["retries"] == {"mode": "standard", "max_attempts": 10}

    de_params = deserialize_botocore_config_params(**ser_params)
    assert de_params["signature_version"] is UNSIGNED
    assert de_params["retries"] == {"mode": "standard", "max_attempts": 10}

    config1 = build_botocore_config(**de_params)
    assert config1.signature_version is UNSIGNED
    assert config1.retries == {"mode": "standard", "max_attempts": 10}

    config2 = Config(**de_params)
    assert config2.signature_version == config1.signature_version
    assert config2.retries == config1.retries


@pytest.mark.parametrize(
    "signature_version", [pytest.param(None, id="not-set"), pytest.param("s3v4", id="SigV4")]
)
def test_serde_other_signature_versions(signature_version):
    ser_params = serialize_botocore_config_params(signature_version=signature_version)
    de_params = deserialize_botocore_config_params(**ser_params)

    assert ser_params["signature_version"] == de_params["signature_version"] == signature_version
