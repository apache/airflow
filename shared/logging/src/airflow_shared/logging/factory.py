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
"""
Build a remote logging IO handler from a provider-supplied registry.

This module owns the runtime-independent logic for resolving and instantiating
``RemoteLogIO`` handlers. Both ``airflow.logging_config`` (core) and
``airflow.sdk.log._load_logging_config`` (Task SDK) call
:func:`resolve_remote_task_log` with their own ``ProvidersManager`` instance and
``import_string`` implementation; that function in turn delegates to
:func:`_build_remote_task_log_from_provider` for the provider-dispatch step and to
:func:`airflow._shared.logging.remote.discover_remote_log_handler` for the
user-defined ``logging_config_class`` and legacy fallbacks.
"""

from __future__ import annotations

import logging
import os
from collections.abc import Callable
from typing import TYPE_CHECKING, Any
from urllib.parse import urlsplit

from .remote import discover_remote_log_handler

if TYPE_CHECKING:
    from airflow.configuration import AirflowConfigParser
    from airflow.logging.remote import RemoteLogIO
    from airflow.providers_manager import ProvidersManager, RemoteLoggingInfo as _CoreRemoteLoggingInfo
    from airflow.sdk.configuration import AirflowSDKConfigParser
    from airflow.sdk.providers_manager_runtime import (
        ProvidersManagerTaskRuntime,
        RemoteLoggingInfo as _SDKRemoteLoggingInfo,
    )

log = logging.getLogger(__name__)

DEFAULT_LOGGING_CONFIG_PATH = "airflow.config_templates.airflow_local_settings.DEFAULT_LOGGING_CONFIG"
# Default ``[logging] logging_config_class`` value when the user has not overridden it.


def _instantiate_handler(
    info: _CoreRemoteLoggingInfo | _SDKRemoteLoggingInfo,
    *,
    import_string: Callable[[str], Any],
) -> Any | None:
    try:
        cls = import_string(info.classpath)
    except Exception as err:
        log.info(
            "Remote task logs will not be available; failed to import %s: %s",
            info.classpath,
            err,
        )
        if "PYTEST_CURRENT_TEST" in os.environ:
            raise
        return None

    factory = getattr(cls, "from_config", None)
    if factory is None:
        log.warning(
            "Provider %s registered %s as a remote logging handler, but the class does not "
            "implement classmethod 'from_config'. The handler will be skipped.",
            info.package_name,
            info.classpath,
        )
        return None

    try:
        return factory()
    except Exception as err:
        log.info(
            "Remote task logs will not be available; %s.from_config raised: %s",
            info.classpath,
            err,
        )
        if "PYTEST_CURRENT_TEST" in os.environ:
            raise
        return None


def _build_remote_task_log_from_provider(
    *,
    remote_base_log_folder: str | None,
    providers_manager: ProvidersManager | ProvidersManagerTaskRuntime,
    import_string: Callable[[str], Any],
) -> RemoteLogIO | None:
    """
    Resolve a remote logging IO handler from its URL scheme.

    The shared layer only routes to the right provider class; each provider's
    ``from_config`` classmethod imports ``conf`` itself and reads its own
    backend-specific keys.

    Provider dispatch does not produce a default conn id. Users who need one
    set ``[logging] remote_log_conn_id`` explicitly; providers that want a
    backend-specific default can read from their own hook inside ``from_config``.

    :param remote_base_log_folder: Value of ``[logging] remote_base_log_folder``.
        Its URL scheme is the dispatch key.
    :param providers_manager: The ``ProvidersManager`` (core) or
        ``ProvidersManagerTaskRuntime`` (Task SDK) whose
        ``remote_logging_handler_by_scheme`` method resolves the scheme.
    :param import_string: ``import_string`` callable from the caller's runtime.
    :returns: The instantiated handler, or ``None`` when no provider claims the
        scheme.
    """
    if not remote_base_log_folder:
        return None

    if not (scheme := urlsplit(remote_base_log_folder).scheme):
        return None

    if (info := providers_manager.remote_logging_handler_by_scheme(scheme)) is None:
        return None

    return _instantiate_handler(info, import_string=import_string)


def resolve_remote_task_log(
    *,
    conf: AirflowConfigParser | AirflowSDKConfigParser,
    providers_manager: ProvidersManager | ProvidersManagerTaskRuntime,
    import_string: Callable[[str], Any],
) -> tuple[RemoteLogIO | None, str | None]:
    """
    Resolve the active remote log handler via the three-tier precedence rule.

    Order:

    1. A user-defined ``logging_config_class`` module that exports
       ``REMOTE_TASK_LOG`` (or ``DEFAULT_REMOTE_CONN_ID``) wins — preserves
       existing custom configs unchanged.
    2. ProvidersManager dispatch (provider yaml ``remote-logging:`` blocks).
    3. Legacy attr-path against the default logging module. Transitional while
       ``airflow_local_settings.py`` still carries the per-scheme if/elif chain;
       removed when every provider has migrated.

    :param conf: AirflowConfigParser-like object exposing ``get``.
    :param providers_manager: Caller's ``ProvidersManager`` /
        ``ProvidersManagerTaskRuntime``.
    :param import_string: ``import_string`` callable from the caller's runtime.
    :returns: ``(remote_task_log, default_remote_conn_id)``. Either element may
        be ``None`` when no path produces a result.
    """
    logging_class_path = (
        conf.get("logging", "logging_config_class", fallback=DEFAULT_LOGGING_CONFIG_PATH)
        or DEFAULT_LOGGING_CONFIG_PATH
    )
    user_defined = logging_class_path != DEFAULT_LOGGING_CONFIG_PATH

    if user_defined:
        remote_task_log, default_remote_conn_id = discover_remote_log_handler(
            logging_class_path, DEFAULT_LOGGING_CONFIG_PATH, import_string
        )
        if remote_task_log is not None or default_remote_conn_id is not None:
            return remote_task_log, default_remote_conn_id

    if conf.getboolean("logging", "remote_logging", fallback=False):
        remote_task_log = _build_remote_task_log_from_provider(
            remote_base_log_folder=conf.get("logging", "remote_base_log_folder", fallback=None),
            providers_manager=providers_manager,
            import_string=import_string,
        )
        if remote_task_log is not None:
            return remote_task_log, conf.get("logging", "remote_log_conn_id", fallback=None)

    return discover_remote_log_handler(logging_class_path, DEFAULT_LOGGING_CONFIG_PATH, import_string)
