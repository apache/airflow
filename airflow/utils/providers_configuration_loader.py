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

from functools import wraps
from typing import Callable, TypeVar

from airflow.typing_compat import ParamSpec

PS = ParamSpec("PS")
RT = TypeVar("RT")


def providers_configuration_loaded(func: Callable[PS, RT]) -> Callable[PS, RT]:
    """
    Make sure that providers configuration is loaded before actually calling the decorated function.

    ProvidersManager initialization of configuration is relatively inexpensive - it walks through
    all providers's entrypoints, retrieve the provider_info and loads config yaml parts of the get_info.
    Unlike initialization of hooks and operators it does not import any of the provider's code, so it can
    be run quickly by all commands that need to access providers configuration. We cannot even import
    ProvidersManager while importing any of the commands, so we need to locally import it here.

    We cannot initialize the configuration in settings/conf because of the way how conf/settings are used
    internally - they are loaded while importing airflow, and we need to access airflow version conf in the
    ProvidesManager initialization, so instead we opt for decorating all the methods that need it with this
    decorator.

    The decorator should be placed below @suppress_logs_and_warning but above @provide_session in order to
    avoid spoiling the output of formatted options with some warnings ar infos, and to be prepared that
    session creation might need some configuration defaults from the providers configuration.

    :param func: function to makes sure that providers configuration is loaded before actually calling
    """

    @wraps(func)
    def wrapped_function(*args, **kwargs) -> RT:
        from airflow.providers_manager import ProvidersManager

        ProvidersManager().initialize_providers_configuration()
        return func(*args, **kwargs)

    return wrapped_function
