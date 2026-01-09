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

import base64
import re
from collections.abc import Mapping, MutableMapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from requests.exceptions import RequestException

from airflow.configuration import conf
from airflow.providers.http.hooks.http import HttpHook

if TYPE_CHECKING:
    from requests import Response

    from airflow.models.connection import Connection


class InformaticaEDCError(RuntimeError):
    """Raised when the Informatica Enterprise Data Catalog API returns an error."""


@dataclass(frozen=True)
class InformaticaConnectionConfig:
    """Container for Informatica EDC connection settings."""

    base_url: str
    username: str | None
    password: str | None
    security_domain: str | None
    verify_ssl: bool
    request_timeout: int
    provider_id: str
    modified_by: str | None

    @property
    def auth_header(self) -> str | None:
        """Return the authorization header for the configured credentials."""
        if not self.username:
            return None

        domain_prefix = f"{self.security_domain}\\" if self.security_domain else ""
        credential = f"{domain_prefix}{self.username}:{self.password or ''}"
        token = base64.b64encode(bytes(credential, "utf-8")).decode("utf-8")
        return f"Basic {token}"


class InformaticaEDCHook(HttpHook):
    """Hook providing a minimal client for the Informatica EDC REST API."""

    conn_name_attr = "informatica_edc_conn_id"
    default_conn_name = conf.get("informatica", "default_conn_id", fallback="informatica_edc_default")
    conn_type = "informatica_edc"
    hook_name = "Informatica EDC"
    _lineage_association = "core.DataSetDataFlow"

    def __init__(
        self,
        informatica_edc_conn_id: str = default_conn_name,
        *,
        request_timeout: int | None = None,
        **kwargs,
    ) -> None:
        super().__init__(http_conn_id=informatica_edc_conn_id, method="GET", **kwargs)
        self._config: InformaticaConnectionConfig | None = None
        self._request_timeout = request_timeout or conf.getint("informatica", "request_timeout", fallback=30)

    @property
    def config(self) -> InformaticaConnectionConfig:
        """Return cached connection configuration."""
        if self._config is None:
            connection = self.get_connection(self.http_conn_id)
            self._config = self._build_connection_config(connection)
        return self._config

    def _build_connection_config(self, connection: Connection) -> InformaticaConnectionConfig:
        """Build a configuration object from an Airflow connection."""
        host = connection.host or ""
        schema = connection.schema or "https"
        if host.startswith("http://") or host.startswith("https://"):
            base_url = host
        else:
            base_url = f"{schema}://{host}" if host else f"{schema}://"
        if connection.port:
            base_url = f"{base_url}:{connection.port}"

        extras: MutableMapping[str, Any] = connection.extra_dejson or {}
        verify_ssl_raw = extras.get("verify_ssl", extras.get("verify", True))
        verify_ssl = str(verify_ssl_raw).lower() not in {"0", "false", "no"}

        provider_id = str(extras.get("provider_id", "enrichment"))
        modified_by = str(extras.get("modified_by", connection.login or "airflow"))
        security_domain = extras.get("security_domain") or extras.get("domain")

        return InformaticaConnectionConfig(
            base_url=base_url.rstrip("/"),
            username=connection.login,
            password=connection.password,
            security_domain=str(security_domain) if security_domain else None,
            verify_ssl=verify_ssl,
            request_timeout=self._request_timeout,
            provider_id=provider_id,
            modified_by=modified_by,
        )

    def close_session(self) -> None:
        pass

    def get_conn(
        self,
        headers: dict[str, Any] | None = None,
        extra_options: dict[str, Any] | None = None,
    ) -> Any:
        """Return a configured session augmented with Informatica specific headers."""
        session = super().get_conn(headers=headers, extra_options=extra_options)
        session.verify = self.config.verify_ssl
        session.headers.update({"Accept": "application/json", "Content-Type": "application/json"})
        if self.config.auth_header:
            session.headers["Authorization"] = self.config.auth_header
        return session

    def _build_url(self, endpoint: str) -> str:
        endpoint = endpoint if endpoint.startswith("/") else f"/{endpoint}"
        return f"{self.config.base_url}{endpoint}"

    def _request(
        self,
        method: str,
        endpoint: str,
        *,
        params: Mapping[str, Any] | None = None,
        json: Mapping[str, Any] | None = None,
    ) -> Response:
        """Execute an HTTP request and raise :class:`InformaticaEDCError` on failure."""
        url = self._build_url(endpoint)
        session = self.get_conn()
        try:
            response = session.request(
                method=method.upper(),
                url=url,
                params=params,
                json=json,
                timeout=self.config.request_timeout,
            )
        except RequestException as exc:
            raise InformaticaEDCError(f"Failed to call Informatica EDC endpoint {endpoint}: {exc}") from exc

        if response.ok:
            return response

        message = response.text or response.reason
        raise InformaticaEDCError(
            f"Informatica EDC request to {endpoint} returned {response.status_code}: {message}"
        )

    def _encode_id(self, id, tilde=False):
        """
        Encode an ID to be safe. Return String.

        Parameters
        ----------
        id : String
            ID of object
        tilde : Boolean, optional (default=True)
            Whether to encode with a tilde or percent sign.
        """
        # Replace three underscores with two backslashes
        if ":___" in id:
            id = id.replace(":___", "://")

        # Get REGEX set-up
        regex = re.compile("([^a-zA-Z0-9-_])")

        # Initialize a few variables
        id_lst = list(id)
        idx = 0

        # Replace each unsafe char with "~Hex(Byte(unsafe char))~"
        while regex.search(id, idx) is not None:
            idx = regex.search(id, idx).span()[1]
            if tilde:
                id_lst[idx - 1] = "~" + str(bytes(id_lst[idx - 1], "utf-8").hex()) + "~"
            else:
                id_lst[idx - 1] = "%" + str(bytes(id_lst[idx - 1], "utf-8").hex())

        return "".join(id_lst)

    def get_object(self, object_id: str, include_ref_objects: bool = False) -> dict[str, Any]:
        """Retrieve a catalog object by its identifier."""
        encoded_object_id = self._encode_id(object_id, tilde=True)
        include_refs = "true" if include_ref_objects else "false"

        url = f"/access/2/catalog/data/objects/{encoded_object_id}?includeRefObjects={include_refs}"

        response = self._request("GET", url)
        return response.json()

    def create_lineage_link(self, source_object_id: str, target_object_id: str) -> dict[str, Any]:
        """Create a lineage relationship between source and target objects."""
        if source_object_id == target_object_id:
            raise InformaticaEDCError(
                "Source and target object identifiers must differ when creating lineage."
            )

        payload = {
            "providerId": self.config.provider_id,
            "modifiedBy": self.config.modified_by,
            "updates": [
                {
                    "id": target_object_id,
                    "newSourceLinks": [
                        {
                            "objectId": source_object_id,
                            "associationId": self._lineage_association,
                            "properties": [
                                {
                                    "attrUuid": "core.targetAttribute",
                                    "value": self._lineage_association,
                                }
                            ],
                        }
                    ],
                    "deleteSourceLinks": [],
                    "newFacts": [],
                    "deleteFacts": [],
                }
            ],
        }

        response = self._request("PATCH", "/access/1/catalog/data/objects", json=payload)
        return response.json() if response.content else {}
