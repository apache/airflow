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
