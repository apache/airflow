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

from collections.abc import Callable
from typing import TYPE_CHECKING

from airflow.providers.oracle.hooks.base_oci import OciBaseHook, _get_oci_sdk

if TYPE_CHECKING:
    from oci.generative_ai import GenerativeAiClient


class OciGenerativeAIHook(OciBaseHook["GenerativeAiClient"]):
    """
    Hook for OCI Generative AI Hosted Applications and Hosted Deployments.

    The hook exposes the native OCI Generative AI management client through ``conn`` and
    ``get_conn()``. Client methods return OCI SDK responses so callers retain response data,
    ``ETags``, request identifiers, and work request identifiers.

    :param oci_conn_id: The :ref:`OCI connection id <howto/connection:oci>`.
    :param service_endpoint: Optional Generative AI service endpoint selected by the Dag author.
    """

    hook_name = "OCI Generative AI"

    def _get_client_class(self) -> Callable[..., GenerativeAiClient]:
        return _get_oci_sdk().generative_ai.GenerativeAiClient
