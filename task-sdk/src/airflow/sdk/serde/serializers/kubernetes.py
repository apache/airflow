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

from airflow.utils.module_loading import qualname

# lazy loading for performance reasons
serializers = [
    "kubernetes.client.models.v1_resource_requirements.V1ResourceRequirements",
    "kubernetes.client.models.v1_pod.V1Pod",
]

if TYPE_CHECKING:
    from airflow.sdk.serde import U

__version__ = 1

deserializers: list[type[object]] = []
log = logging.getLogger(__name__)


def serialize(o: object) -> tuple[U, str, int, bool]:
    from kubernetes.client import models as k8s

    if not k8s:
        return "", "", 0, False

    if isinstance(o, (k8s.V1Pod, k8s.V1ResourceRequirements)):
        from airflow.providers.cncf.kubernetes.pod_generator import PodGenerator

        # We're running this in an except block, so we don't want it to fail
        # under any circumstances, e.g. accessing a non-existing attribute.
        def safe_get_name(pod):
            try:
                return pod.metadata.name
            except Exception:
                return None

        try:
            return PodGenerator.serialize_pod(o), qualname(o), __version__, True
        except Exception:
            log.warning("Serialization failed for pod %s", safe_get_name(o))
            log.debug("traceback for serialization error", exc_info=True)
            return "", "", 0, False

    return "", "", 0, False
