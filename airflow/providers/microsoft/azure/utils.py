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

import warnings

from azure.core.pipeline import PipelineContext, PipelineRequest
from azure.core.pipeline.policies import BearerTokenCredentialPolicy
from azure.core.pipeline.transport import HttpRequest
from azure.identity import DefaultAzureCredential
from msrest.authentication import BasicTokenAuthentication


def get_field(*, conn_id: str, conn_type: str, extras: dict, field_name: str):
    """Get field from extra, first checking short name, then for backcompat we check for prefixed name."""
    backcompat_prefix = f"extra__{conn_type}__"
    backcompat_key = f"{backcompat_prefix}{field_name}"
    ret = None
    if field_name.startswith("extra__"):
        raise ValueError(
            f"Got prefixed name {field_name}; please remove the '{backcompat_prefix}' prefix "
            "when using this method."
        )
    if field_name in extras:
        if backcompat_key in extras:
            warnings.warn(
                f"Conflicting params `{field_name}` and `{backcompat_key}` found in extras for conn "
                f"{conn_id}. Using value for `{field_name}`.  Please ensure this is the correct "
                f"value and remove the backcompat key `{backcompat_key}`."
            )
        ret = extras[field_name]
    elif backcompat_key in extras:
        ret = extras.get(backcompat_key)
    if ret == "":
        return None
    return ret


class AzureIdentityCredentialAdapter(BasicTokenAuthentication):
    """Adapt azure-identity credentials for backward compatibility.

    Adapt credentials from azure-identity to be compatible with SD
    that needs msrestazure or azure.common.credentials

    Check https://stackoverflow.com/questions/63384092/exception-attributeerror-defaultazurecredential-object-has-no-attribute-sig
    """

    def __init__(self, credential=None, resource_id="https://management.azure.com/.default", **kwargs):
        """Adapt azure-identity credentials for backward compatibility.

        :param credential: Any azure-identity credential (DefaultAzureCredential by default)
        :param str resource_id: The scope to use to get the token (default ARM)
        """
        super().__init__(None)
        if credential is None:
            credential = DefaultAzureCredential()
        self._policy = BearerTokenCredentialPolicy(credential, resource_id, **kwargs)

    def _make_request(self):
        return PipelineRequest(
            HttpRequest("AzureIdentityCredentialAdapter", "https://fakeurl"), PipelineContext(None)
        )

    def set_token(self):
        """Ask the azure-core BearerTokenCredentialPolicy policy to get a token.

        Using the policy gives us for free the caching system of azure-core.
        We could make this code simpler by using private method, but by definition
        I can't assure they will be there forever, so mocking a fake call to the policy
        to extract the token, using 100% public API.
        """
        request = self._make_request()
        self._policy.on_request(request)
        # Read Authorization, and get the second part after Bearer
        token = request.http_request.headers["Authorization"].split(" ", 1)[1]
        self.token = {"access_token": token}

    def signed_session(self, azure_session=None):
        self.set_token()
        return super().signed_session(azure_session)
