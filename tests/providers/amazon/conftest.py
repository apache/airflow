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
import warnings
from importlib import metadata

import pytest


@pytest.fixture(scope="session")
def botocore_version():
    try:
        version = metadata.version("botocore")
    except ModuleNotFoundError:
        warnings.warn("'botocore' package not found'", UserWarning, stacklevel=2)
        return None

    try:
        return tuple(map(int, version.split(".")[:3]))
    except Exception:
        warnings.warn(f"Unable to parse botocore {version!r}", UserWarning, stacklevel=2)
        return None


@pytest.fixture(autouse=True)
def filter_botocore_warnings(botocore_version):
    """Filter known botocore future warnings."""

    with warnings.catch_warnings():
        if botocore_version and botocore_version < (1, 29):
            # By default, for some clients botocore use deprecated endpoints `{region}.{service}.{dnsSuffix}`
            # In botocore 1.29 it will be replaced by `{service}.{region}.{dnsSuffix}`
            # and the warning should gone
            # See: https://github.com/boto/botocore/issues/2705
            warnings.filterwarnings(
                "ignore",
                category=FutureWarning,
                module="botocore.client",
                message="The .* client is currently using a deprecated endpoint.*",
            )
        yield


@pytest.fixture(scope="package")
def aws_testing_env_vars(tmp_path_factory):
    """Session scoped fixture, return mock AWS specific environment variables for unit tests."""
    tmp_dir = tmp_path_factory.mktemp("aws-configs-")

    def empty_config(name: str) -> str:
        config = tmp_dir / name
        config.touch()
        return str(config)

    return {
        # Mock values for access_key, secret_key and token
        "AWS_ACCESS_KEY_ID": "airflow-testing",
        "AWS_SECRET_ACCESS_KEY": "airflow-testing",
        "AWS_SESSION_TOKEN": "airflow-testing",
        "AWS_SECURITY_TOKEN": "airflow-testing",
        # Set default region as N.Virginia (eu-west-1).
        # Otherwise some unit tests might fail if this sets to other region.
        "AWS_DEFAULT_REGION": "us-east-1",
        "AWS_REGION": "us-east-1",
        # Create empty configuration file
        "AWS_SHARED_CREDENTIALS_FILE": empty_config("aws_shared_credentials_file"),
        "AWS_CONFIG_FILE": empty_config("aws_config_file"),
        "BOTO_CONFIG": empty_config("legacy_boto2_config.cfg"),
    }


@pytest.fixture(autouse=True)
def set_default_aws_settings(aws_testing_env_vars, monkeypatch):
    """
    Change AWS configurations (env vars) before start each test.
    1. Remove all existed variables which prefixed by AWS.
        It might be some credentials, botocore configurations, etc.
    2. Use pre-defined variables for unit testing.
    """
    for env_name in os.environ:
        if env_name.startswith("AWS"):
            monkeypatch.delenv(env_name, raising=False)
    for env_name, value in aws_testing_env_vars.items():
        monkeypatch.setenv(env_name, value)


@pytest.fixture(scope="package", autouse=True)
def setup_default_aws_connections():
    with pytest.MonkeyPatch.context() as mp_ctx:
        mp_ctx.setenv("AIRFLOW_CONN_AWS_DEFAULT", '{"conn_type": "aws"}')
        mp_ctx.setenv("AIRFLOW_CONN_EMR_DEFAULT", '{"conn_type": "emr", "extra": {}}')
        yield
