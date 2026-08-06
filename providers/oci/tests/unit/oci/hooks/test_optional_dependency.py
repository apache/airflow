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
import subprocess
import sys
from textwrap import dedent
from unittest import mock

import pytest

from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException
from airflow.providers.oci.hooks.base import _get_oci_sdk


def test_get_oci_sdk_requires_optional_extra():
    with mock.patch.dict(sys.modules, {"oci": None}):
        with pytest.raises(
            AirflowOptionalProviderFeatureException,
            match=r"pip install 'apache-airflow-providers-oci\[oci\]'",
        ):
            _get_oci_sdk()


def test_hook_modules_import_without_optional_oci_sdk():
    subprocess.run(
        [
            sys.executable,
            "-c",
            dedent(
                """
                import sys

                sys.modules["oci"] = None
                import airflow.providers.oci.hooks.base
                import airflow.providers.oci.hooks.generative_ai
                """
            ),
        ],
        check=True,
    )


def test_hooks_support_selective_oci_service_imports():
    subprocess.run(
        [
            sys.executable,
            "-c",
            dedent(
                """
                import oci

                assert not hasattr(oci, "generative_ai")
                assert not hasattr(oci, "identity")

                from airflow.providers.oci.hooks.base import OciBaseHook
                from airflow.providers.oci.hooks.generative_ai import OciGenerativeAIHook

                assert OciGenerativeAIHook()._get_client_class().__module__.startswith(
                    "oci.generative_ai"
                )

                hook = OciBaseHook()
                hook.get_oci_config = lambda: ({}, None)
                success, message = hook.test_connection()

                assert not success
                assert "AttributeError" not in message
                assert hasattr(oci, "identity")
                """
            ),
        ],
        check=True,
        env={**os.environ, "OCI_PYTHON_SDK_NO_SERVICE_IMPORTS": "True"},
    )
