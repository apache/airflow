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
"""Hook for the Informatica Intelligent Data Management Cloud (IDMC) REST APIs."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Any

import aiohttp
import requests
from requests.exceptions import RequestException

from airflow.providers.common.compat.sdk import AirflowException, BaseHook, conf

if TYPE_CHECKING:
    from requests import Response

    from airflow.providers.common.compat.sdk import Connection


class InformaticaIDMCError(AirflowException):
    """Raised when an IDMC REST API call returns an error."""


class IDMCTimeoutException(AirflowException):
    """Raised when an IDMC run does not finish within the allowed time."""


class IDMCAuthVersion(str, Enum):
    """Selects which IDMC login flow the hook should use."""

    V2 = "v2"
    V3 = "v3"


# Task type aliases recognised by the v2 ``/api/v2/job`` endpoint.
# The IDMC docs describe these short codes; we normalise common synonyms
# from the Mass Ingestion / CDI UI to spare users the lookup.
_TASK_TYPE_ALIASES: Mapping[str, str] = {
    "MAPPING_TASK": "MTT",
    "MAPPING": "MTT",
    "MTT": "MTT",
    "DATA_REPLICATION": "DRS",
    "DRS": "DRS",
    "DATA_SYNCHRONIZATION": "DSS",
    "DSS": "DSS",
    "DATA_MASKING": "DMASK",
    "DMASK": "DMASK",
    "POWERCENTER": "PCS",
    "PCS": "PCS",
    "REPLICATION_TASK": "RTM",
    "RTM": "RTM",
    "WORKFLOW": "WORKFLOW",
    "TASKFLOW": "TASKFLOW",
}

# Run-status values that the v2 activity log uses.  IDMC's v3 ``/jobs`` API
# uses similar (but capitalised) labels — both flow through ``_normalise_status``.
_TERMINAL_SUCCESS_STATUSES = {"SUCCESS", "COMPLETED", "OK"}
_TERMINAL_WARNING_STATUSES = {"WARNING"}
_TERMINAL_FAILURE_STATUSES = {"FAILED", "FAILURE", "ERROR"}
_TERMINAL_CANCELLED_STATUSES = {"STOPPED", "CANCELLED", "CANCELED", "ABORTED"}


@dataclass
class _IDMCSession:
    """Holds login material returned by the IDMC login endpoint."""

    session_id: str
    base_api_url: str
    session_header_name: str
    extra_products: dict[str, str] = field(default_factory=dict)


class IDMCRunStatus(str, Enum):
    """Normalised run status used across IDMC services."""

    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    WARNING = "WARNING"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"

    @classmethod
    def is_terminal(cls, status: str) -> bool:
        """Return True if ``status`` is a terminal run state."""
        return status in {cls.SUCCESS.value, cls.WARNING.value, cls.FAILED.value, cls.CANCELLED.value}

    @classmethod
    def is_successful(cls, status: str) -> bool:
        """Return True if ``status`` represents a successful (or warning) outcome."""
        return status in {cls.SUCCESS.value, cls.WARNING.value}


def _normalise_status(raw: str | None) -> str:
    """Map an IDMC raw status string to the :class:`IDMCRunStatus` vocabulary."""
    if raw is None:
        return IDMCRunStatus.RUNNING.value
    upper = raw.strip().upper()
    if upper in _TERMINAL_SUCCESS_STATUSES:
        return IDMCRunStatus.SUCCESS.value
    if upper in _TERMINAL_WARNING_STATUSES:
        return IDMCRunStatus.WARNING.value
    if upper in _TERMINAL_FAILURE_STATUSES:
        return IDMCRunStatus.FAILED.value
    if upper in _TERMINAL_CANCELLED_STATUSES:
        return IDMCRunStatus.CANCELLED.value
    if upper in {"QUEUED", "STARTING", "INITIALIZED", "PENDING"}:
        return IDMCRunStatus.QUEUED.value
    return IDMCRunStatus.RUNNING.value


class InformaticaIDMCHook(BaseHook):
    """
    Interact with the Informatica Intelligent Data Management Cloud (IDMC).

    The hook supports both the legacy v2 login flow (``icSessionId`` + per-org
    ``serverUrl``) and the modern v3 control plane login (``INFA-SESSION-ID``
    + per-product ``baseApiUrl``).  Pick the flow with the ``auth_version``
    connection extra (``v2`` or ``v3``) or by passing ``auth_version`` to the
    constructor.

    Connection configuration:

    - ``host``: the pod login host, e.g. ``dm-us.informaticacloud.com``
      (no scheme; the hook always uses HTTPS).  May also be a full URL.
    - ``login`` / ``password``: IDMC username and password.
    - ``extra``:
      - ``auth_version``: ``"v2"`` (default) or ``"v3"``
      - ``security_domain``: optional IDMC security domain (v2)
      - ``verify_ssl``: optional, default true
      - ``request_timeout``: optional, seconds
      - ``product``: optional product key from the v3 ``products`` array
        (default ``"Integration Cloud"``)

    :param informatica_idmc_conn_id: the Airflow connection id.
    :param auth_version: override the connection's ``auth_version`` extra.
    :param request_timeout: override the connection's request timeout.
    """

    conn_name_attr = "informatica_idmc_conn_id"
    default_conn_name = conf.get("informatica", "default_idmc_conn_id", fallback="informatica_idmc_default")
    conn_type = "informatica_idmc"
    hook_name = "Informatica IDMC"

    DEFAULT_LOGIN_HOST = "dm-us.informaticacloud.com"
    DEFAULT_PRODUCT_KEY = "Integration Cloud"

    def __init__(
        self,
        informatica_idmc_conn_id: str = default_conn_name,
        *,
        auth_version: str | IDMCAuthVersion | None = None,
        request_timeout: int | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.informatica_idmc_conn_id = informatica_idmc_conn_id
        self._explicit_auth_version: IDMCAuthVersion | None = (
            IDMCAuthVersion(auth_version) if isinstance(auth_version, str) else auth_version
        )
        self._explicit_request_timeout = request_timeout
        self._session: _IDMCSession | None = None

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        """Build custom UI labels for the IDMC connection form."""
        return {
            "hidden_fields": ["schema", "port"],
            "relabeling": {"host": "Login host", "login": "Username", "password": "Password"},
            "placeholders": {
                "host": "dm-us.informaticacloud.com",
                "extra": (
                    '{"auth_version": "v3", "security_domain": "Native",'
                    ' "verify_ssl": true, "request_timeout": 60,'
                    ' "product": "Integration Cloud"}'
                ),
            },
        }

    @property
    def auth_version(self) -> IDMCAuthVersion:
        """Resolve the auth flow to use, preferring the constructor override."""
        if self._explicit_auth_version is not None:
            return self._explicit_auth_version
        connection = self.get_connection(self.informatica_idmc_conn_id)
        extras: Mapping[str, Any] = connection.extra_dejson or {}
        return IDMCAuthVersion(str(extras.get("auth_version", "v2")).lower())

    def _request_timeout(self, connection: Connection) -> int:
        if self._explicit_request_timeout is not None:
            return self._explicit_request_timeout
        extras: Mapping[str, Any] = connection.extra_dejson or {}
        return int(extras.get("request_timeout", 60))

    @staticmethod
    def _login_base_url(connection: Connection) -> str:
        host = (connection.host or InformaticaIDMCHook.DEFAULT_LOGIN_HOST).strip()
        if host.startswith("http://") or host.startswith("https://"):
            base = host.rstrip("/")
        else:
            base = f"https://{host.rstrip('/')}"
        return base

    @staticmethod
    def _verify_ssl(connection: Connection) -> bool:
        extras: Mapping[str, Any] = connection.extra_dejson or {}
        raw = extras.get("verify_ssl", extras.get("verify", True))
        return str(raw).lower() not in {"0", "false", "no"}

    @staticmethod
    def _build_v2_login_payload(connection: Connection) -> dict[str, Any]:
        if not connection.login or not connection.password:
            raise InformaticaIDMCError("IDMC v2 login requires both username and password on the connection.")
        extras: Mapping[str, Any] = connection.extra_dejson or {}
        payload: dict[str, Any] = {
            "@type": "login",
            "username": connection.login,
            "password": connection.password,
        }
        domain = extras.get("security_domain") or extras.get("domain")
        if domain:
            payload["securitydomain"] = domain
        return payload

    @staticmethod
    def _build_v3_login_payload(connection: Connection) -> dict[str, Any]:
        if not connection.login or not connection.password:
            raise InformaticaIDMCError("IDMC v3 login requires both username and password on the connection.")
        return {"username": connection.login, "password": connection.password}

    @staticmethod
    def _select_v3_product(payload: Mapping[str, Any], product_key: str) -> str:
        products = payload.get("products") or []
        for product in products:
            if isinstance(product, Mapping) and product.get("name") == product_key:
                base = product.get("baseApiUrl")
                if base:
                    return str(base).rstrip("/")
        if products and isinstance(products[0], Mapping):
            base = products[0].get("baseApiUrl")
            if base:
                return str(base).rstrip("/")
        raise InformaticaIDMCError(
            f"IDMC v3 login response did not include a product with name {product_key!r}."
        )

    def _login_v2(self, connection: Connection, *, timeout: int, verify: bool) -> _IDMCSession:
        url = f"{self._login_base_url(connection)}/ma/api/v2/user/login"
        try:
            response = self._raw_post(
                url,
                json=self._build_v2_login_payload(connection),
                timeout=timeout,
                verify=verify,
            )
        except RequestException as exc:
            raise InformaticaIDMCError(f"IDMC v2 login failed: {exc}") from exc
        if not response.ok:
            raise InformaticaIDMCError(
                f"IDMC v2 login returned {response.status_code}: {response.text or response.reason}"
            )
        data = response.json()
        session_id = data.get("icSessionId")
        server_url = data.get("serverUrl")
        if not session_id or not server_url:
            raise InformaticaIDMCError("IDMC v2 login response missing icSessionId/serverUrl.")
        return _IDMCSession(
            session_id=str(session_id),
            base_api_url=str(server_url).rstrip("/"),
            session_header_name="icSessionId",
        )

    def _login_v3(self, connection: Connection, *, timeout: int, verify: bool) -> _IDMCSession:
        url = f"{self._login_base_url(connection)}/saas/public/core/v3/login"
        try:
            response = self._raw_post(
                url,
                json=self._build_v3_login_payload(connection),
                timeout=timeout,
                verify=verify,
            )
        except RequestException as exc:
            raise InformaticaIDMCError(f"IDMC v3 login failed: {exc}") from exc
        if not response.ok:
            raise InformaticaIDMCError(
                f"IDMC v3 login returned {response.status_code}: {response.text or response.reason}"
            )
        data = response.json()
        user_info = data.get("userInfo") or {}
        session_id = user_info.get("sessionId") or data.get("sessionId")
        if not session_id:
            raise InformaticaIDMCError("IDMC v3 login response missing sessionId.")
        extras: Mapping[str, Any] = connection.extra_dejson or {}
        product_key = str(extras.get("product", self.DEFAULT_PRODUCT_KEY))
        base_api_url = self._select_v3_product(data, product_key)
        return _IDMCSession(
            session_id=str(session_id),
            base_api_url=base_api_url,
            session_header_name="INFA-SESSION-ID",
        )

    def _raw_post(
        self,
        url: str,
        *,
        json: Mapping[str, Any] | None,
        timeout: int,
        verify: bool,
    ) -> Response:
        """Plain ``requests``-style POST that bypasses the cached session."""
        return requests.post(
            url,
            json=dict(json or {}),
            headers={"Accept": "application/json", "Content-Type": "application/json"},
            timeout=timeout,
            verify=verify,
        )

    def session(self) -> _IDMCSession:
        """Login (if necessary) and return the cached IDMC session."""
        if self._session is not None:
            return self._session
        connection = self.get_connection(self.informatica_idmc_conn_id)
        timeout = self._request_timeout(connection)
        verify = self._verify_ssl(connection)
        if self.auth_version is IDMCAuthVersion.V3:
            self._session = self._login_v3(connection, timeout=timeout, verify=verify)
        else:
            self._session = self._login_v2(connection, timeout=timeout, verify=verify)
        return self._session

    def reset_session(self) -> None:
        """Drop the cached session so the next call will log in again."""
        self._session = None

    def _request(
        self,
        method: str,
        endpoint: str,
        *,
        json_body: Mapping[str, Any] | None = None,
        params: Mapping[str, Any] | None = None,
    ) -> Any:
        session = self.session()
        connection = self.get_connection(self.informatica_idmc_conn_id)
        timeout = self._request_timeout(connection)
        verify = self._verify_ssl(connection)
        url = self._build_url(session, endpoint)
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            session.session_header_name: session.session_id,
        }
        try:
            response = requests.request(
                method=method.upper(),
                url=url,
                headers=headers,
                json=dict(json_body) if json_body is not None else None,
                params=dict(params) if params is not None else None,
                timeout=timeout,
                verify=verify,
            )
        except RequestException as exc:
            raise InformaticaIDMCError(f"IDMC request to {endpoint} failed: {exc}") from exc
        if not response.ok:
            raise InformaticaIDMCError(
                f"IDMC request to {endpoint} returned {response.status_code}: "
                f"{response.text or response.reason}"
            )
        if not response.content:
            return {}
        try:
            return response.json()
        except ValueError:
            return {"raw": response.text}

    @staticmethod
    def _build_url(session: _IDMCSession, endpoint: str) -> str:
        endpoint = endpoint if endpoint.startswith("/") else f"/{endpoint}"
        return f"{session.base_api_url}{endpoint}"

    @staticmethod
    def _normalise_task_type(task_type: str) -> str:
        key = task_type.strip().upper().replace("-", "_").replace(" ", "_")
        try:
            return _TASK_TYPE_ALIASES[key]
        except KeyError as exc:
            valid = sorted(set(_TASK_TYPE_ALIASES.values()))
            raise InformaticaIDMCError(
                f"Unsupported IDMC task type {task_type!r}. Valid task types: {', '.join(valid)}."
            ) from exc

    def start_task(
        self,
        *,
        task_id: str | None = None,
        task_federated_id: str | None = None,
        task_type: str = "MTT",
        callback_url: str | None = None,
    ) -> dict[str, Any]:
        """
        Start a CDI task (mapping task by default) and return the launch response.

        Either ``task_id`` (the IDMC internal id) or ``task_federated_id`` must be
        supplied.  ``task_type`` accepts both short codes (``MTT``, ``DSS``,
        ``DRS``, ``DMASK``, ``PCS``, ``RTM``, ``WORKFLOW``, ``TASKFLOW``) and the
        full UI labels (e.g. ``"Mapping Task"`` / ``"MAPPING_TASK"``).

        The returned dictionary always contains a normalised ``run_id`` key
        alongside the raw IDMC response.
        """
        if not task_id and not task_federated_id:
            raise InformaticaIDMCError("start_task requires either task_id or task_federated_id.")
        normalised_type = self._normalise_task_type(task_type)
        body: dict[str, Any] = {"@type": "job", "taskType": normalised_type}
        if task_id:
            body["taskId"] = task_id
        if task_federated_id:
            body["taskFederatedId"] = task_federated_id
        if callback_url:
            body["callbackURL"] = callback_url
        response = self._request("POST", "/api/v2/job", json_body=body)
        run_id = response.get("runId") or response.get("id")
        if run_id is None:
            raise InformaticaIDMCError(f"IDMC start_task response did not include a runId: {response!r}")
        return {"run_id": str(run_id), "task_type": normalised_type, "raw": response}

    def start_taskflow(
        self,
        taskflow_api_name: str,
        *,
        input_parameters: Mapping[str, Any] | None = None,
        callback_url: str | None = None,
    ) -> dict[str, Any]:
        """
        Start an IDMC taskflow by its REST API name.

        ``taskflow_api_name`` is the value defined under "API Name" on the
        Taskflow's properties page in IDMC.  ``input_parameters`` is sent in the
        request body and surfaces inside the taskflow as input parameters.

        Returns a dictionary with ``run_id`` (the taskflow run identifier
        returned by IDMC) and the raw response.
        """
        if not taskflow_api_name:
            raise InformaticaIDMCError("start_taskflow requires a non-empty taskflow_api_name.")
        endpoint = f"/active-bpel/rt/{taskflow_api_name}"
        body: dict[str, Any] = {}
        if input_parameters:
            body["inputs"] = dict(input_parameters)
        if callback_url:
            body["callbackURL"] = callback_url
        response = self._request("POST", endpoint, json_body=body or None)
        run_id = response.get("RunId") or response.get("runId") or response.get("id")
        if run_id is None:
            raise InformaticaIDMCError(f"IDMC start_taskflow response did not include a RunId: {response!r}")
        return {"run_id": str(run_id), "raw": response}

    def get_task_run_status(self, run_id: str | int) -> dict[str, Any]:
        """Return normalised status info for a CDI task run."""
        response = self._request("GET", "/api/v2/activity/activityLog", params={"runId": str(run_id)})
        entries = response if isinstance(response, list) else response.get("entries", [response])
        if not entries:
            return {"run_id": str(run_id), "status": IDMCRunStatus.RUNNING.value, "raw": response}
        latest = entries[0]
        raw_status = latest.get("runStatus") or latest.get("state") or latest.get("status")
        return {
            "run_id": str(run_id),
            "status": _normalise_status(raw_status if isinstance(raw_status, str) else None),
            "raw_status": raw_status,
            "raw": latest,
        }

    def get_taskflow_run_status(self, run_id: str | int) -> dict[str, Any]:
        """Return normalised status info for a CDI taskflow run."""
        response = self._request(
            "GET",
            f"/active-bpel/services/tf/status/{run_id}",
        )
        raw_status = response.get("status") or response.get("state")
        return {
            "run_id": str(run_id),
            "status": _normalise_status(raw_status if isinstance(raw_status, str) else None),
            "raw_status": raw_status,
            "raw": response,
        }

    # --- async path used by the trigger ----------------------------------

    async def _async_login(self) -> _IDMCSession:
        connection = self.get_connection(self.informatica_idmc_conn_id)
        timeout = self._request_timeout(connection)
        verify = self._verify_ssl(connection)
        login_base = self._login_base_url(connection)
        async with aiohttp.ClientSession() as client:
            if self.auth_version is IDMCAuthVersion.V3:
                payload = self._build_v3_login_payload(connection)
                url = f"{login_base}/saas/public/core/v3/login"
                async with client.post(
                    url, json=payload, ssl=verify, timeout=aiohttp.ClientTimeout(total=timeout)
                ) as response:
                    if response.status >= 400:
                        text = await response.text()
                        raise InformaticaIDMCError(f"IDMC v3 login returned {response.status}: {text}")
                    data = await response.json()
                user_info = data.get("userInfo") or {}
                session_id = user_info.get("sessionId") or data.get("sessionId")
                if not session_id:
                    raise InformaticaIDMCError("IDMC v3 login response missing sessionId.")
                extras: Mapping[str, Any] = connection.extra_dejson or {}
                product_key = str(extras.get("product", self.DEFAULT_PRODUCT_KEY))
                base_api_url = self._select_v3_product(data, product_key)
                self._session = _IDMCSession(
                    session_id=str(session_id),
                    base_api_url=base_api_url,
                    session_header_name="INFA-SESSION-ID",
                )
            else:
                payload = self._build_v2_login_payload(connection)
                url = f"{login_base}/ma/api/v2/user/login"
                async with client.post(
                    url, json=payload, ssl=verify, timeout=aiohttp.ClientTimeout(total=timeout)
                ) as response:
                    if response.status >= 400:
                        text = await response.text()
                        raise InformaticaIDMCError(f"IDMC v2 login returned {response.status}: {text}")
                    data = await response.json()
                if not data.get("icSessionId") or not data.get("serverUrl"):
                    raise InformaticaIDMCError("IDMC v2 login response missing icSessionId/serverUrl.")
                self._session = _IDMCSession(
                    session_id=str(data["icSessionId"]),
                    base_api_url=str(data["serverUrl"]).rstrip("/"),
                    session_header_name="icSessionId",
                )
        return self._session

    async def _async_request(
        self,
        method: str,
        endpoint: str,
        *,
        params: Mapping[str, Any] | None = None,
    ) -> Any:
        session = self._session or await self._async_login()
        connection = self.get_connection(self.informatica_idmc_conn_id)
        timeout = self._request_timeout(connection)
        verify = self._verify_ssl(connection)
        url = self._build_url(session, endpoint)
        headers = {
            "Accept": "application/json",
            session.session_header_name: session.session_id,
        }
        async with aiohttp.ClientSession() as client:
            async with client.request(
                method=method.upper(),
                url=url,
                headers=headers,
                params=dict(params) if params else None,
                ssl=verify,
                timeout=aiohttp.ClientTimeout(total=timeout),
            ) as response:
                if response.status >= 400:
                    text = await response.text()
                    raise InformaticaIDMCError(
                        f"IDMC request to {endpoint} returned {response.status}: {text}"
                    )
                if response.content_length == 0:
                    return {}
                return await response.json(content_type=None)

    async def aget_task_run_status(self, run_id: str | int) -> dict[str, Any]:
        """Async variant of :meth:`get_task_run_status` for use from triggers."""
        response = await self._async_request(
            "GET", "/api/v2/activity/activityLog", params={"runId": str(run_id)}
        )
        entries = response if isinstance(response, list) else response.get("entries", [response])
        if not entries:
            return {"run_id": str(run_id), "status": IDMCRunStatus.RUNNING.value, "raw": response}
        latest = entries[0]
        raw_status = latest.get("runStatus") or latest.get("state") or latest.get("status")
        return {
            "run_id": str(run_id),
            "status": _normalise_status(raw_status if isinstance(raw_status, str) else None),
            "raw_status": raw_status,
            "raw": latest,
        }

    async def aget_taskflow_run_status(self, run_id: str | int) -> dict[str, Any]:
        """Async variant of :meth:`get_taskflow_run_status` for use from triggers."""
        response = await self._async_request("GET", f"/active-bpel/services/tf/status/{run_id}")
        raw_status = response.get("status") or response.get("state")
        return {
            "run_id": str(run_id),
            "status": _normalise_status(raw_status if isinstance(raw_status, str) else None),
            "raw_status": raw_status,
            "raw": response,
        }

    def cancel_task(self, run_id: str | int) -> dict[str, Any]:
        """Best-effort cancel of an IDMC CDI task run."""
        return self._request(
            "POST",
            "/api/v2/job/stop",
            json_body={"@type": "stopJob", "runId": str(run_id)},
        )
