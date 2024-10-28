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
from contextlib import contextmanager
from pathlib import Path

from airflow.configuration import conf

log = logging.getLogger(__name__)

# TODO(potiuk) change the tests use better approach sing Pytest fixtures rather than
#              `unit_test_mode` parameter. It's potentially disruptive so we should not do it **JUST** yet


def remove_all_configurations():
    old_sections, old_proxies = (conf._sections, conf._proxies)
    conf._sections = {}
    conf._proxies = {}
    return old_sections, old_proxies


def restore_all_configurations(sections: dict, proxies: dict):
    conf._sections = sections  # type: ignore
    conf._proxies = proxies  # type: ignore


@contextmanager
def use_config(config: str):
    """
    Temporary load the deprecated test configuration.
    """
    sections, proxies = remove_all_configurations()
    conf.read(str(Path(__file__).parents[1] / "config_templates" / config))
    try:
        yield
    finally:
        restore_all_configurations(sections, proxies)


@contextmanager
def set_deprecated_options(
    deprecated_options: dict[tuple[str, str], tuple[str, str, str]],
):
    """
    Temporary replaces deprecated options with the ones provided.
    """
    old_deprecated_options = conf.deprecated_options
    conf.deprecated_options = deprecated_options
    try:
        yield
    finally:
        conf.deprecated_options = old_deprecated_options


@contextmanager
def set_sensitive_config_values(sensitive_config_values: set[tuple[str, str]]):
    """
    Temporary replaces sensitive values with the ones provided.
    """
    old_sensitive_config_values = conf.sensitive_config_values
    conf.sensitive_config_values = sensitive_config_values
    try:
        yield
    finally:
        conf.sensitive_config_values = old_sensitive_config_values
