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
from functools import partial, wraps
from urllib.parse import urlparse, urlunparse

from azure.core.pipeline import PipelineContext, PipelineRequest
from azure.core.pipeline.policies import BearerTokenCredentialPolicy
from azure.core.pipeline.transport import HttpRequest
from azure.identity import DefaultAzureCredential
from azure.identity.aio import DefaultAzureCredential as AsyncDefaultAzureCredential
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
                f"value and remove the backcompat key `{backcompat_key}`.",
                UserWarning,
                stacklevel=2,
            )
        ret = extras[field_name]
    elif backcompat_key in extras:
        ret = extras.get(backcompat_key)
    if ret == "":
        return None
    return ret


def _get_default_azure_credential(
    *,
    managed_identity_client_id: str | None = None,
    workload_identity_tenant_id: str | None = None,
    use_async: bool = False,
) -> DefaultAzureCredential | AsyncDefaultAzureCredential:
    """
    Get DefaultAzureCredential based on provided arguments.

    If managed_identity_client_id and workload_identity_tenant_id are provided, this function returns
    DefaultAzureCredential with managed identity.
    """
    credential_cls: type[AsyncDefaultAzureCredential] | type[DefaultAzureCredential] = (
        AsyncDefaultAzureCredential if use_async else DefaultAzureCredential
    )
    if managed_identity_client_id and workload_identity_tenant_id:
        return credential_cls(
            managed_identity_client_id=managed_identity_client_id,
            workload_identity_tenant_id=workload_identity_tenant_id,
            additionally_allowed_tenants=[workload_identity_tenant_id],
        )
    else:
        return credential_cls()


get_sync_default_azure_credential: partial[DefaultAzureCredential] = partial(
    _get_default_azure_credential,  #  type: ignore[arg-type]
    use_async=False,
)

get_async_default_azure_credential: partial[AsyncDefaultAzureCredential] = partial(
    _get_default_azure_credential,  #  type: ignore[arg-type]
    use_async=True,
)


def add_managed_identity_connection_widgets(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        from flask_appbuilder.fieldwidgets import BS3TextFieldWidget
        from flask_babel import lazy_gettext
        from wtforms import StringField

        widgets = func(*args, **kwargs)
        widgets.update(
            {
                "managed_identity_client_id": StringField(
                    lazy_gettext("Managed Identity Client ID"), widget=BS3TextFieldWidget()
                ),
                "workload_identity_tenant_id": StringField(
                    lazy_gettext("Workload Identity Tenant ID"), widget=BS3TextFieldWidget()
                ),
            }
        )
        return widgets

    return wrapper


class AzureIdentityCredentialAdapter(BasicTokenAuthentication):
    """
    Adapt azure-identity credentials for backward compatibility.

    Adapt credentials from azure-identity to be compatible with SD
    that needs msrestazure or azure.common.credentials

    Check https://stackoverflow.com/questions/63384092/exception-attributeerror-defaultazurecredential-object-has-no-attribute-sig
    """

    def __init__(
        self,
        credential=None,
        resource_id="https://management.azure.com/.default",
        *,
        managed_identity_client_id: str | None = None,
        workload_identity_tenant_id: str | None = None,
        **kwargs,
    ):
        """
        Adapt azure-identity credentials for backward compatibility.

        :param credential: Any azure-identity credential (DefaultAzureCredential by default)
        :param resource_id: The scope to use to get the token (default ARM)
        :param managed_identity_client_id: The client ID of a user-assigned managed identity.
            If provided with `workload_identity_tenant_id`, they'll pass to ``DefaultAzureCredential``.
        :param workload_identity_tenant_id: ID of the application's Microsoft Entra tenant.
            Also called its "directory" ID.
            If provided with `managed_identity_client_id`, they'll pass to ``DefaultAzureCredential``.
        """
        super().__init__(None)  # type: ignore[arg-type]
        if credential is None:
            credential = get_sync_default_azure_credential(
                managed_identity_client_id=managed_identity_client_id,
                workload_identity_tenant_id=workload_identity_tenant_id,
            )
        self._policy = BearerTokenCredentialPolicy(credential, resource_id, **kwargs)

    def _make_request(self):
        return PipelineRequest(
            HttpRequest("AzureIdentityCredentialAdapter", "https://fakeurl"), PipelineContext(None)
        )

    def set_token(self):
        """
        Ask the azure-core BearerTokenCredentialPolicy policy to get a token.

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


def parse_blob_account_url(host: str | None, login: str | None) -> str:
    account_url = host if host else f"https://{login}.blob.core.windows.net/"

    parsed_url = urlparse(account_url)

    # if there's no netloc then user provided the DNS name without the https:// prefix.
    if parsed_url.scheme == "":
        account_url = "https://" + account_url
        parsed_url = urlparse(account_url)

    netloc = parsed_url.netloc
    if "." not in netloc:
        # if there's no netloc and no dots in the path, then user only
        # provided the Active Directory ID, not the full URL or DNS name
        netloc = f"{login}.blob.core.windows.net/"

    # Now enforce 3 to 23 character limit on account name
    # https://learn.microsoft.com/en-us/azure/storage/common/storage-account-overview#storage-account-name
    host_components = netloc.split(".")
    host_components[0] = host_components[0][:24]
    netloc = ".".join(host_components)

    url_components = [
        parsed_url.scheme,
        netloc,
        parsed_url.path,
        parsed_url.params,
        parsed_url.query,
        parsed_url.fragment,
    ]
    return urlunparse(url_components)
