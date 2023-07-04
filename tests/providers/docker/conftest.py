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

from contextlib import AbstractContextManager, contextmanager
from unittest import mock

import pytest


@contextmanager
def _mocker_context(o, additional_modules: list | None = None) -> AbstractContextManager[mock.MagicMock]:
    """
    Helper context for mocking multiple reference of same object
        :param o: Object/Class for mocking.
        :param additional_modules: additional modules where ``o`` exists.
    """
    patched = []
    object_name = o.__name__
    mocked_object = mock.MagicMock(name=f"Mocked.{object_name}", spec=o)
    additional_modules = additional_modules or []
    try:
        for mdl in [o.__module__, *additional_modules]:
            mocker = mock.patch(f"{mdl}.{object_name}", mocked_object)
            mocker.start()
            patched.append(mocker)

        yield mocked_object
    finally:
        for mocker in reversed(patched):
            mocker.stop()


@pytest.fixture
def docker_api_client_patcher():
    """Patch ``docker.APIClient`` by mock value."""
    from airflow.providers.docker.hooks.docker import APIClient

    with _mocker_context(APIClient, ["airflow.providers.docker.hooks.docker"]) as m:
        yield m


@pytest.fixture
def docker_hook_patcher():
    """Patch DockerHook by mock value."""
    from airflow.providers.docker.operators.docker import DockerHook

    with _mocker_context(DockerHook, ["airflow.providers.docker.operators.docker"]) as m:
        yield m
