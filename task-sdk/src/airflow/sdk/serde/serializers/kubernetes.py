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
from typing import TYPE_CHECKING

import pydantic

from airflow.sdk.module_loading import qualname

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


def deserialize(cls: type, version: int, data: dict) -> pydantic.BaseModel:
    if version > __version__:
        raise TypeError(f"serialized version {version} is newer than class version {__version__}")

    if not issubclass(cls, pydantic.BaseModel):
        raise TypeError(f"do not know how to deserialize {qualname(cls)}")

    try:
        return cls.model_validate(data)
    except pydantic.ValidationError as e:
        try:
            _coerce_legacy_values(data, e)
        except (KeyError, TypeError, ValueError):
            raise e
        return cls.model_validate(data)


def _coerce_legacy_values(data: dict, error: pydantic.ValidationError) -> None:
    # kubernetes<37 stored numeric quantities/labels, string booleans and fields since
    # removed, all of which the pydantic models of kubernetes>=37 reject.
    for err in error.errors():
        *path, key = err["loc"]
        parent = _find_parent(data, path)
        value = err["input"]
        new: str | bool
        if err["type"] == "extra_forbidden" and isinstance(parent, dict):
            del parent[key]
            continue
        if err["type"] == "string_type" and isinstance(value, bool):
            new = str(value).lower()
        elif err["type"] == "string_type" and isinstance(value, (int, float)):
            new = str(value)
        elif err["type"] == "bool_type" and isinstance(value, str) and value.lower() in ("true", "false"):
            new = value.lower() == "true"
        else:
            continue
        if isinstance(parent, dict):
            parent[key] = new
        elif isinstance(key, int):
            parent[key] = new


def _find_parent(data: dict, path: list[int | str]) -> dict | list:
    # Union fields add type tags to ``loc`` that are not keys in the data.
    parent: dict | list = data
    for part in path:
        if isinstance(parent, dict):
            child = parent[part]
        elif isinstance(part, int):
            child = parent[part]
        else:
            raise TypeError(f"cannot index a list with {part!r}")
        if not isinstance(child, (dict, list)):
            raise TypeError(f"{part!r} does not point to a dict or list")
        parent = child
    return parent
