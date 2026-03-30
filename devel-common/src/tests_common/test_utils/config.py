#
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

import contextlib
import os
from typing import TYPE_CHECKING, Literal, overload

if TYPE_CHECKING:
    from airflow.configuration import AirflowConfigParser
    from airflow.sdk.configuration import AirflowSDKConfigParser

# Provider config test data for parametrized tests.
# Options listed here must NOT be overridden in unit_tests.cfg, otherwise
# tests that assert default values via conf.get() will see the unit_tests.cfg
# value instead.

# (section, option, expected_value)
# Options defined in provider metadata (provider.yaml) with non-None defaults.
PROVIDER_METADATA_CONFIG_OPTIONS: list[tuple[str, str, str]] = [
    ("celery", "celery_app_name", "airflow.providers.celery.executors.celery_executor"),
    ("celery", "worker_enable_remote_control", "true"),
    ("celery", "task_acks_late", "True"),
    ("kubernetes_executor", "namespace", "default"),
    ("kubernetes_executor", "delete_worker_pods", "True"),
    ("celery_kubernetes_executor", "kubernetes_queue", "kubernetes"),
]

# Options defined in provider_config_fallback_defaults.cfg.
CFG_FALLBACK_CONFIG_OPTIONS: list[tuple[str, str, str]] = [
    ("celery", "flower_host", "0.0.0.0"),
    ("celery", "pool", "prefork"),
    ("celery", "worker_precheck", "False"),
    ("kubernetes_executor", "in_cluster", "True"),
    ("kubernetes_executor", "verify_ssl", "True"),
    ("elasticsearch", "end_of_log_mark", "end_of_log"),
]

# Options where provider metadata and cfg fallback have DIFFERENT default values.
# (section, option, metadata_value, cfg_fallback_value)
PROVIDER_METADATA_OVERRIDES_CFG_FALLBACK: list[tuple[str, str, str, str]] = [
    (
        "celery",
        "celery_app_name",
        "airflow.providers.celery.executors.celery_executor",
        "airflow.executors.celery_executor",
    ),
]


@contextlib.contextmanager
def conf_vars(overrides):
    """Automatically detects which config modules are loaded (Core, SDK, or both) and updates them accordingly temporarily."""
    import sys

    from airflow import settings

    configs = []
    if "airflow.configuration" in sys.modules:
        from airflow.configuration import conf

        configs.append(conf)
    if "airflow.sdk.configuration" in sys.modules:
        from airflow.sdk.configuration import conf

        configs.append(conf)

    originals = [{} for _ in configs]
    original_env_vars = {}
    for (section, key), value in overrides.items():
        env = configs[0]._env_var_name(section, key)
        if env in os.environ:
            original_env_vars[env] = os.environ.pop(env)

        for i, conf in enumerate(configs):
            originals[i][(section, key)] = conf.get(section, key) if conf.has_option(section, key) else None
            if value is not None:
                if not conf.has_section(section):
                    conf.add_section(section)
                conf.set(section, key, value)
            elif conf.has_section(section) and conf.has_option(section, key):
                conf.remove_option(section, key)

    if "airflow.configuration" in sys.modules:
        settings.configure_vars()

    try:
        yield
    finally:
        for i, conf in enumerate(configs):
            for (section, key), value in originals[i].items():
                if value is not None:
                    if not conf.has_section(section):
                        conf.add_section(section)
                    conf.set(section, key, value)
                elif conf.has_section(section) and conf.has_option(section, key):
                    conf.remove_option(section, key)

        for env, value in original_env_vars.items():
            os.environ[env] = value

        if "airflow.configuration" in sys.modules:
            settings.configure_vars()


@overload
def create_fresh_airflow_config(variant: Literal["core"] = ...) -> AirflowConfigParser: ...


@overload
def create_fresh_airflow_config(variant: Literal["task-sdk"]) -> AirflowSDKConfigParser: ...


def create_fresh_airflow_config(
    variant: Literal["core", "task-sdk"] = "core",
) -> AirflowConfigParser | AirflowSDKConfigParser:
    """Create a fresh, fully-initialized config parser independent of the singleton.

    Use this instead of ``from airflow.settings import conf`` when the test mutates
    parser state (e.g. ``make_sure_configuration_loaded(with_providers=False)``).
    A fresh instance avoids interference with other tests that may run in parallel.

    :param variant: Which config parser to create — ``"core"`` (default) for the
        full Airflow config, or ``"task-sdk"`` for the lightweight SDK config.
    """
    if variant == "core":
        from airflow.configuration import initialize_config as initialize_core_config

        return initialize_core_config()
    if variant == "task-sdk":
        from airflow.sdk.configuration import initialize_config as initialize_sdk_config

        return initialize_sdk_config()
    raise ValueError(f"Unknown variant: {variant!r}. Expected 'core' or 'task-sdk'.")


@contextlib.contextmanager
def env_vars(overrides):
    """
    Temporarily patches env vars, restoring env as it was after context exit.

    Example:
        with env_vars({'AIRFLOW_CONN_AWS_DEFAULT': 's3://@'}):
            # now we have an aws default connection available
    """
    orig_vars = {}
    new_vars = []
    for env, value in overrides.items():
        if env in os.environ:
            orig_vars[env] = os.environ.pop(env, "")
        else:
            new_vars.append(env)
        os.environ[env] = value
    try:
        yield
    finally:
        for env, value in orig_vars.items():
            os.environ[env] = value
        for env in new_vars:
            os.environ.pop(env)
