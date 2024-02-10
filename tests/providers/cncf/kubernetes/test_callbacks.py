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

from unittest.mock import MagicMock

from airflow.providers.cncf.kubernetes.callbacks import KubernetesPodOperatorCallback


class MockWrapper:
    mock_callbacks = MagicMock()

    @classmethod
    def reset(cls):
        cls.mock_callbacks.reset_mock()


class MockKubernetesPodOperatorCallback(KubernetesPodOperatorCallback):
    """`KubernetesPodOperator` callbacks methods."""

    @staticmethod
    def on_sync_client_creation(*args, **kwargs) -> None:
        MockWrapper.mock_callbacks.on_sync_client_creation(*args, **kwargs)

    @staticmethod
    def on_async_client_creation(*args, **kwargs) -> None:
        MockWrapper.mock_callbacks.on_async_client_creation(*args, **kwargs)

    @staticmethod
    def on_pod_creation(*args, **kwargs) -> None:
        MockWrapper.mock_callbacks.on_pod_creation(*args, **kwargs)

    @staticmethod
    def on_pod_starting(*args, **kwargs) -> None:
        MockWrapper.mock_callbacks.on_pod_starting(*args, **kwargs)

    @staticmethod
    def on_pod_completion(*args, **kwargs) -> None:
        MockWrapper.mock_callbacks.on_pod_completion(*args, **kwargs)

    @staticmethod
    def on_pod_cleanup(*args, **kwargs) -> None:
        MockWrapper.mock_callbacks.on_pod_cleanup(*args, **kwargs)

    @staticmethod
    def on_operator_resuming(*args, **kwargs) -> None:
        MockWrapper.mock_callbacks.on_operator_resuming(*args, **kwargs)

    @staticmethod
    def progress_callback(*args, **kwargs) -> None:
        MockWrapper.mock_callbacks.progress_callback(*args, **kwargs)
