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

from typing import TYPE_CHECKING, Any

from airflow.providers.common.compat.sdk import AirflowNotFoundException, BaseHook

if TYPE_CHECKING:
    from openfeature.client import OpenFeatureClient
    from openfeature.evaluation_context import EvaluationContext

# Connection ids whose provider has already been registered in this process.
_REGISTERED_CONNECTIONS: set[str] = set()


class OpenFeatureHook(BaseHook):
    """
    Evaluate feature flags through the `OpenFeature <https://openfeature.dev>`__ SDK.

    OpenFeature is a vendor-neutral evaluation API: a *provider* adapts it to a concrete backend
    (flagd, GrowthBook, Unleash, an in-house system, ...). This hook registers that provider from the
    connection and evaluates flags, so the same DAG code works against any backend.

    The connection ``extra`` may declare the provider to register once per process::

        {"provider_class": "airflow.providers.openfeature.providers.fractional.FractionalProvider",
         "provider_kwargs": {}}

    If no ``provider_class`` is set (or the connection does not exist), the hook uses whatever
    OpenFeature provider is already registered globally (for example one set in
    ``airflow_local_settings``).

    :param conn_id: connection id whose ``extra`` optionally declares the OpenFeature provider.
    """

    conn_name_attr = "conn_id"
    default_conn_name = "openfeature_default"
    conn_type = "openfeature"
    hook_name = "OpenFeature"

    def __init__(self, conn_id: str = default_conn_name) -> None:
        super().__init__()
        self.conn_id = conn_id

    def get_conn(self) -> OpenFeatureClient:
        """Return the OpenFeature client, registering the connection's provider on first use."""
        return self.get_client()

    def get_client(self) -> OpenFeatureClient:
        from openfeature import api

        self._register_provider()
        return api.get_client()

    def _register_provider(self) -> None:
        if self.conn_id in _REGISTERED_CONNECTIONS:
            return
        try:
            conn = self.get_connection(self.conn_id)
        except AirflowNotFoundException:
            # The connection is optional: fall back to the globally-registered provider.
            _REGISTERED_CONNECTIONS.add(self.conn_id)
            return
        extra = conn.extra_dejson or {}
        provider_class = extra.get("provider_class")
        if provider_class:
            import importlib

            from openfeature import api

            module_path, _, class_name = provider_class.rpartition(".")
            provider = getattr(importlib.import_module(module_path), class_name)(
                **(extra.get("provider_kwargs") or {})
            )
            api.set_provider(provider)
        _REGISTERED_CONNECTIONS.add(self.conn_id)

    @staticmethod
    def _evaluation_context(entity: str | None, attributes: dict[str, Any] | None) -> EvaluationContext:
        from openfeature.evaluation_context import EvaluationContext

        return EvaluationContext(targeting_key=entity, attributes=attributes or {})

    def is_enabled(
        self, flag_key: str, entity: str | None = None, default: bool = False, **attributes: Any
    ) -> bool:
        """Resolve a boolean flag for ``entity`` (the targeting key), returning ``default`` if unknown."""
        return self.get_client().get_boolean_value(
            flag_key, default, self._evaluation_context(entity, attributes)
        )

    def get_variant(
        self, flag_key: str, entity: str | None = None, default: str = "", **attributes: Any
    ) -> str:
        """Resolve a string/variant flag for ``entity``, returning ``default`` if unknown."""
        return self.get_client().get_string_value(
            flag_key, default, self._evaluation_context(entity, attributes)
        )
