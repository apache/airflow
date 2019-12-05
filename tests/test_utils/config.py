# -*- coding: utf-8 -*-
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

try:
    from contextdecorator import contextmanager
except ImportError:
    from contextlib import contextmanager
import os

from airflow.configuration import conf
from airflow import settings


@contextmanager
def conf_vars(overrides):
    original = {}
    original_env_vars = {}
    for (section, key), value in overrides.items():

        env = conf._env_var_name(section, key)
        if env in os.environ:
            original_env_vars[env] = os.environ.pop(env)

        if conf.has_option(section, key):
            original[(section, key)] = conf.get(section, key)
        else:
            original[(section, key)] = None
        if value is not None:
            conf.set(section, key, value)
        else:
            conf.remove_option(section, key)
    settings.configure_vars()
    try:
        yield
    finally:
        for (section, key), value in original.items():
            if value is not None:
                conf.set(section, key, value)
            else:
                conf.remove_option(section, key)
        for env, value in original_env_vars.items():
            os.environ[env] = value
        settings.configure_vars()
