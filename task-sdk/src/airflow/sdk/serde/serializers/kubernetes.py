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

import logging
from typing import TYPE_CHECKING, Any, cast

import pydantic

from airflow.sdk.module_loading import qualname
from airflow.sdk.serde.typing import is_pydantic_model

# lazy loading for performance reasons
serializers = [
    "kubernetes.client.models.v1_resource_requirements.V1ResourceRequirements",
    "kubernetes.client.models.v1_pod.V1Pod",
]

if TYPE_CHECKING:
    from airflow.sdk.serde import U

__version__ = 1

# Registering these as deserializers also adds them to the deserialization allowlist,
# so they load without kubernetes.* in [core] allowed_deserialization_classes.
deserializers = serializers
log = logging.getLogger(__name__)


def serialize(o: object) -> tuple[U, str, int, bool]:
    from kubernetes.client import models as k8s

    if not k8s:
        return "", "", 0, False

    if isinstance(o, (k8s.V1Pod, k8s.V1ResourceRequirements)):
        from kubernetes.client import ApiClient

        # We're running this in an except block, so we don't want it to fail
        # under any circumstances, e.g. accessing a non-existing attribute.
        def safe_get_name(pod):
            try:
                return pod.metadata.name
            except Exception:
                return None

        try:
            return ApiClient().sanitize_for_serialization(o), qualname(o), __version__, True
        except Exception:
            log.warning("Serialization failed for pod %s", safe_get_name(o))
            log.debug("traceback for serialization error", exc_info=True)
            return "", "", 0, False

    return "", "", 0, False


def deserialize(cls: type, version: int, data: dict) -> Any:
    if version > __version__:
        raise TypeError(f"serialized version {version} is newer than class version {__version__}")

    if not is_pydantic_model(cls):
        raise TypeError(f"do not know how to deserialize {qualname(cls)}")
    model = cast("type[pydantic.BaseModel]", cls)

    try:
        return model.model_validate(data)
    except pydantic.ValidationError as e:
        try:
            coerced = _coerce_legacy_values(data, e)
        except (KeyError, TypeError, ValueError):
            raise e
        return model.model_validate(coerced)


def _coerce_legacy_values(data: dict, error: pydantic.ValidationError) -> dict:
    # kubernetes<37 stored numeric quantities/labels and string booleans that the
    # pydantic models of kubernetes>=37 reject in strict mode.
    for err in error.errors():
        *path, key = err["loc"]
        parent: Any = data
        for part in path:
            parent = parent[part]
        value = parent[key]
        if err["type"] == "string_type" and isinstance(value, bool):
            parent[key] = str(value).lower()
        elif err["type"] == "string_type" and isinstance(value, (int, float)):
            parent[key] = str(value)
        elif err["type"] == "bool_type" and str(value).lower() in ("true", "false"):
            parent[key] = str(value).lower() == "true"
    return data
