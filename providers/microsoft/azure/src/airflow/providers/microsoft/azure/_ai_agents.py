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

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pydantic import JsonValue

VERSION_INTERMEDIATE_STATUSES = {"creating", "deleting"}
VERSION_SUCCESS_STATUSES = {"active"}
VERSION_FAILURE_STATUSES = {"failed"}
VERSION_DELETED_STATUS = "deleted"


def _serialize_resource(resource: Any) -> JsonValue:
    """Serialize an SDK model or response object into XCom-safe primitives."""
    if resource is None or isinstance(resource, str | int | float | bool):
        return resource
    if isinstance(resource, list | tuple):
        return [_serialize_resource(item) for item in resource]
    if isinstance(resource, dict):
        if not all(isinstance(key, str) for key in resource):
            raise TypeError("Azure AI Hosted agent resources must use string keys for XCom serialization.")
        return {key: _serialize_resource(value) for key, value in resource.items()}
    if hasattr(resource, "as_dict"):
        return _serialize_resource(resource.as_dict())
    if hasattr(resource, "model_dump"):
        return _serialize_resource(resource.model_dump())
    raise TypeError(
        f"Cannot serialize Azure AI Hosted agent resource of type {type(resource).__name__} for XCom."
    )


def _get_resource_attr(resource: Any, attr: str) -> Any:
    """Get an attribute from an SDK resource or mapping."""
    if isinstance(resource, dict):
        return resource.get(attr)
    return getattr(resource, attr, None)


def _get_version_status(version: Any) -> str:
    """Return a normalized Hosted agent version status string."""
    status = _get_resource_attr(version, "status")
    if hasattr(status, "value"):
        status = status.value
    if status is None:
        raise ValueError("Azure AI Hosted agent version did not include a status.")
    return str(status).lower()


def _get_agent_version(version: Any) -> str:
    """Return the version identifier from a Hosted agent version payload."""
    agent_version = _get_resource_attr(version, "version")
    if agent_version is None:
        raise ValueError("Azure AI Hosted agent response did not include a version.")
    return str(agent_version)
