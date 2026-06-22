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
"""Map the ``[sandbox] provider`` config value to a :class:`SandboxProvider`.

Mirrors ``crabbox --provider <name>``. Custom backends can be registered by
fully-qualified class path (``my_pkg.module:MyProvider``).
"""

from __future__ import annotations

from importlib import import_module

from airflow.providers.sandbox.backends.base import SandboxProvider

_BUILTIN: dict[str, str] = {
    "local": "airflow.providers.sandbox.backends.local:LocalProvider",
    "daytona": "airflow.providers.sandbox.backends.daytona:DaytonaProvider",
    "e2b": "airflow.providers.sandbox.backends.e2b:E2BProvider",
    "modal": "airflow.providers.sandbox.backends.modal:ModalProvider",
    "islo": "airflow.providers.sandbox.backends.islo:IsloProvider",
}


def load_provider(name: str) -> SandboxProvider:
    """Instantiate the provider named by config (built-in alias or ``mod:Class``)."""
    if not name:
        raise ValueError("[sandbox] provider is not set")
    spec = _BUILTIN.get(name.strip().lower(), name.strip())
    if ":" not in spec:
        raise ValueError(
            f"Unknown provider {name!r}. Use one of {sorted(_BUILTIN)} "
            "or a fully-qualified 'module:Class' path."
        )
    module_path, _, class_name = spec.partition(":")
    cls = getattr(import_module(module_path), class_name)
    if not (isinstance(cls, type) and issubclass(cls, SandboxProvider)):
        raise TypeError(f"{spec} is not a SandboxProvider subclass")
    return cls()
