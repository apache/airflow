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
"""Curated Google toolset exposing allow-listed Discovery API methods as pydantic-ai tools."""

from __future__ import annotations

import json
import logging
import os
import re
from fnmatch import fnmatchcase
from functools import cache
from importlib import import_module
from typing import TYPE_CHECKING, Any

import requests
from pydantic_ai.exceptions import ModelRetry
from pydantic_ai.tools import ToolDefinition
from pydantic_ai.toolsets.abstract import AbstractToolset, ToolsetTool
from pydantic_core import SchemaValidator, core_schema

from airflow.providers.common.ai.utils.tool_definition import return_schema_kwargs
from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException

try:
    import googleapiclient.discovery_cache
    from googleapiclient.discovery import ResourceMethodParameters, build
except ImportError as e:
    raise AirflowOptionalProviderFeatureException() from e

if TYPE_CHECKING:
    from collections.abc import Sequence

    from pydantic_ai._run_context import RunContext

_PASSTHROUGH_VALIDATOR = SchemaValidator(core_schema.any_schema())

log = logging.getLogger(__name__)

_DISCOVERY_DOC_URL = "https://www.googleapis.com/discovery/v1/apis/{api}/{version}/rest"


def _normalize_method(method: str) -> str:
    """Comparison key for method entries: case- and underscore-insensitive."""
    return method.replace("_", "").casefold()


def _require_string_arg(*, tool_args: dict[str, Any], key: str) -> str:
    value = tool_args.get(key)
    if not isinstance(value, str) or not value:
        raise ValueError(f"The {key!r} argument is required and must be a non-empty string.")
    return value


def _format_project_parameter(*, project: str, param_def: dict[str, Any]) -> str:
    pattern = param_def.get("pattern")
    if not isinstance(pattern, str):
        return project
    try:
        if re.fullmatch(pattern, project):
            return project
        resource_project = f"projects/{project}"
        if re.fullmatch(pattern, resource_project):
            return resource_project
    except re.error:
        return project
    return project


def _get_google_base_hook_class() -> type[Any]:
    try:
        return import_module("airflow.providers.google.common.hooks.base_google").GoogleBaseHook
    except ImportError as e:
        raise AirflowOptionalProviderFeatureException() from e


# Methods that put credentials or decrypted secrets into the agent's context.
# Keys are version-agnostic (``api:resource.method``) so v1/v1beta variants are
# equally protected. A wildcard in ``allowed_methods`` never matches these --
# each must be listed verbatim (with its version) to be callable. Defense in
# depth, not exhaustive: least-privilege IAM on the connection's service
# account remains the real boundary.
_CREDENTIAL_RETURNING_METHODS = frozenset(
    _normalize_method(method)
    for method in (
        "secretmanager:projects.secrets.versions.access",
        "iamcredentials:projects.serviceAccounts.generateAccessToken",
        "iamcredentials:projects.serviceAccounts.generateIdToken",
        "iamcredentials:projects.serviceAccounts.signBlob",
        "iamcredentials:projects.serviceAccounts.signJwt",
        "iam:projects.serviceAccounts.keys.create",
        "cloudkms:projects.locations.keyRings.cryptoKeys.decrypt",
        "cloudkms:projects.locations.keyRings.cryptoKeys.rawDecrypt",
        "cloudkms:projects.locations.keyRings.cryptoKeys.cryptoKeyVersions.asymmetricDecrypt",
        "cloudkms:projects.locations.keyRings.cryptoKeys.cryptoKeyVersions.rawDecrypt",
        "sqladmin:users.insert",
        "sqladmin:users.update",
    )
)

_SCHEMA_DEPTH = 3

# JSON Schemas for the three Google tools.
_LIST_METHODS_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "api": {
            "type": "string",
            "description": "Optional API filter as '<api>/<version>', e.g. 'storage/v1'.",
        },
    },
}

_DESCRIBE_METHOD_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "api": {"type": "string", "description": "API and version, e.g. 'storage/v1'."},
        "method": {
            "type": "string",
            "description": "Method path within the API, e.g. 'objects.list'.",
        },
    },
    "required": ["api", "method"],
}

_CALL_GCP_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "api": {"type": "string", "description": "API and version, e.g. 'storage/v1'."},
        "method": {
            "type": "string",
            "description": "Method path within the API, e.g. 'objects.list'.",
        },
        "parameters": {
            "type": "object",
            "description": (
                "Flat path/query parameters exactly as the API expects them (see describe_gcp_method)."
            ),
        },
        "body": {
            "type": "object",
            "description": "Request body for methods that take one (see describe_gcp_method).",
        },
    },
    "required": ["api", "method"],
}


class GoogleCloudToolset(AbstractToolset[Any]):
    """
    Curated toolset that gives an LLM agent allow-listed access to Google APIs.

    Covers every API published through Google's Discovery Service -- the
    bundled discovery documents in ``google-api-python-client`` (Cloud control
    and admin planes, BigQuery/Firestore/Datastore query methods, Workspace
    APIs such as Drive and Sheets, and more). Exposes three tools:

    - ``list_gcp_methods`` -- the methods this toolset allows, grouped by
      ``api/version`` (the allow-list intersected with the discovery documents).
    - ``describe_gcp_method`` -- a method's parameters and request/response
      schemas, read from the discovery document, so the agent can check what a
      call expects *before* making it.
    - ``call_gcp`` -- execute an allowed method and return the response as
      JSON, following ``nextPageToken`` pagination up to ``max_pages``.

    Credentials, impersonation, and the target project come exclusively from
    the Airflow connection (resolved lazily through the google provider's
    ``GoogleBaseHook``); none of them are tool arguments, so the model cannot
    steer them.

    Not covered: gRPC-only data planes absent from the Discovery surface (for
    example the Bigtable data API and BigQuery Storage streams), streaming
    methods, media upload/download (``alt=media`` is rejected; metadata calls
    on the same resources work), and batch HTTP endpoints. For those, use the
    dedicated Google hook with
    :class:`~airflow.providers.common.ai.toolsets.hook.HookToolset`. A bundled
    discovery document also does not guarantee Google still operates the API --
    a handful of bundled documents describe shut-down services, and calls to
    them fail with Google's own error.

    When a call fails, Google's error message is returned to the agent as a
    retry (:class:`pydantic_ai.ModelRetry`) so the model can correct its
    parameters within the run. pydantic-ai bounds this by the tool's
    ``max_retries``, so an unrecoverable error -- bad credentials, missing IAM
    permissions -- exhausts the retries and fails the task for Airflow to retry.

    :param gcp_conn_id: Airflow connection ID for Google Cloud credentials.
    :param allowed_methods: Methods the agent may call, in
        ``"<api>/<version>:<resource.method>"`` form -- e.g.
        ``["storage/v1:objects.list", "bigquery/v2:jobs.query",
        "compute/v1:instances.*"]``. Required -- access is deny-by-default and
        there is deliberately no auto-discovery. The method part accepts
        ``*``/``?`` wildcards; the API and version must be explicit, so an
        agent can never roam onto ``alpha``/``beta`` surfaces that were not
        deliberately listed. Matching is case- and underscore-insensitive.

        Method names are the method IDs from Google's REST reference docs and
        the `APIs Explorer <https://developers.google.com/apis-explorer>`__ --
        both the path form (``objects.list``) and the doc-style prefixed form
        (``storage.objects.list``) are accepted. Enumerate an API's entries at
        authoring time with :meth:`get_available_methods`.

        Methods that return credentials or decrypted secrets (for example
        ``secretmanager/v1:projects.secrets.versions.access``,
        ``iamcredentials/v1:projects.serviceAccounts.generateAccessToken``)
        are never matched by a wildcard -- each must be listed verbatim to be
        callable.

        .. note::
            This is an application-level guardrail: it bounds what the agent
            can *ask for*, not what the credentials can *do*. Pair it with a
            least-privilege service account on ``gcp_conn_id`` for a hard
            guarantee.

    :param impersonation_chain: Optional service account (or chain) to
        impersonate, passed through to ``GoogleBaseHook``.
    :param enforce_project: Pin API calls to the connection's project. Missing
        ``project``/``projectId`` parameters are filled from the connection; a
        model-supplied value that differs is rejected, including a best-effort
        check on ``projects/<id>/...`` path parameters. Set ``False`` only for
        deliberate cross-project access (the service account's IAM still
        applies). No-op when the connection defines no project. Default
        ``True``.
    :param allow_remote_discovery: Permit ``allowed_methods`` entries for APIs
        missing from the locally bundled discovery documents. Their documents
        are fetched lazily from ``googleapis.com`` at first tool use -- never
        at Dag parse time. Default ``False`` (local metadata only).
    :param max_pages: Upper bound on pages followed via ``nextPageToken`` for
        paginated methods. Default ``5``.
    :param max_output_bytes: Upper bound on the serialized response returned to
        the agent. Larger payloads are clipped and flagged with
        ``"truncated": true``. Default ``65536``.
    """

    def __init__(
        self,
        gcp_conn_id: str = "google_cloud_default",
        *,
        allowed_methods: list[str],
        impersonation_chain: str | Sequence[str] | None = None,
        enforce_project: bool = True,
        allow_remote_discovery: bool = False,
        max_pages: int = 5,
        max_output_bytes: int = 65536,
    ) -> None:
        if not allowed_methods:
            raise ValueError("allowed_methods must be a non-empty list.")
        if max_pages < 1:
            raise ValueError("max_pages must be at least 1.")
        if max_output_bytes < 1:
            raise ValueError("max_output_bytes must be a positive number of bytes.")

        # Validation below reads google-api-python-client's bundled discovery
        # documents only -- local files, no credentials, no network
        patterns: dict[tuple[str, str], list[str]] = {}
        literal_methods: set[str] = set()
        for entry in allowed_methods:
            api_version, sep, method_path = entry.partition(":")
            api, vsep, version = api_version.partition("/")
            if not sep or not vsep or not api or not version or not method_path:
                raise ValueError(
                    f"Invalid method {entry!r}: expected '<api>/<version>:<resource.method>', "
                    "e.g. 'storage/v1:objects.list'."
                )
            if any(ch in api_version for ch in "*?["):
                raise ValueError(f"Invalid method {entry!r}: wildcards are only allowed in the method part.")
            key = (api, version)
            bundled = key in _bundled_apis()
            if not bundled and not allow_remote_discovery:
                raise ValueError(
                    f"API {api!r} version {version!r} is not in the bundled discovery documents. "
                    "Upgrade google-api-python-client, or pass allow_remote_discovery=True to "
                    "fetch the document at call time. gRPC-only services are not available via "
                    "the Discovery surface -- use the dedicated Google hook with HookToolset."
                )
            has_wildcard = any(ch in method_path for ch in "*?[")
            if bundled and not has_wildcard:
                canonical = _get_bundled_method_paths(api, version).get(_normalize_method(method_path))
                if canonical is None:
                    raise ValueError(
                        f"Unknown method {method_path!r} for {api}/{version} in {entry!r}. "
                        "Use the method ID from Google's REST reference, e.g. 'objects.list' "
                        "or 'storage.objects.list'."
                    )
                # Store the canonical path so doc-style IDs ("storage.objects.list")
                # and path-style entries ("objects.list") authorize identically.
                method_path = canonical
            elif bundled:
                pattern = _normalize_method(f"{api}/{version}:{method_path}")
                if not any(
                    fnmatchcase(_normalize_method(f"{api}/{version}:{path}"), pattern)
                    and _normalize_method(f"{api}:{path}") not in _CREDENTIAL_RETURNING_METHODS
                    for path in set(_get_bundled_method_paths(api, version).values())
                ):
                    raise ValueError(
                        f"Pattern {method_path!r} matches no methods of {api}/{version} in {entry!r}. "
                        "Wildcards never match credential-returning methods; list those verbatim."
                    )
            normalized = _normalize_method(f"{api}/{version}:{method_path}")
            patterns.setdefault(key, []).append(normalized)
            if not has_wildcard:
                literal_methods.add(normalized)

        self._gcp_conn_id = gcp_conn_id
        self._impersonation_chain = impersonation_chain
        self._enforce_project = enforce_project
        self._allow_remote_discovery = allow_remote_discovery
        self._max_pages = max_pages
        self._max_output_bytes = max_output_bytes
        self._patterns = patterns
        self._literal_methods = frozenset(literal_methods)
        # Populated lazily at call time, so Dag parsing never touches network or credentials.
        self._hook: Any | None = None
        self._services: dict[tuple[str, str], Any] = {}
        self._remote_docs: dict[tuple[str, str], dict[str, Any]] = {}

    @property
    def id(self) -> str:
        return f"gcp-{self._gcp_conn_id}"

    @staticmethod
    def get_available_methods(api_version: str) -> list[str]:
        """
        Return ready-to-use ``allowed_methods`` entries for every method of a bundled API.

        Authoring-time discovery helper -- reads only the locally bundled
        discovery document (no network, no credentials)::

            GoogleCloudToolset.get_available_methods("storage/v1")
            # ['storage/v1:bucketAccessControls.delete', ..., 'storage/v1:objects.list', ...]

        :param api_version: API and version, e.g. ``"storage/v1"``.
        """
        api, _, version = api_version.partition("/")
        if not version or (api, version) not in _bundled_apis():
            raise ValueError(
                f"API {api_version!r} is not in the bundled discovery documents. "
                "Expected '<api>/<version>', e.g. 'storage/v1'."
            )
        return sorted(
            f"{api}/{version}:{path}" for path in set(_get_bundled_method_paths(api, version).values())
        )

    # ------------------------------------------------------------------
    # AbstractToolset interface
    # ------------------------------------------------------------------

    async def get_tools(self, ctx: RunContext[Any]) -> dict[str, ToolsetTool[Any]]:
        tools: dict[str, ToolsetTool[Any]] = {}

        for name, description, schema in (
            (
                "list_gcp_methods",
                "List the Google API methods this toolset allows, grouped by api/version.",
                _LIST_METHODS_SCHEMA,
            ),
            (
                "describe_gcp_method",
                "Show a method's parameters and request/response schemas before calling it.",
                _DESCRIBE_METHOD_SCHEMA,
            ),
            (
                "call_gcp",
                "Execute an allowed Google API method and return the response as JSON.",
                _CALL_GCP_SCHEMA,
            ),
        ):
            # sequential=True because all tools share per-API service clients
            # with synchronous I/O -- they must not run concurrently.
            # return_schema is "string": every tool returns a JSON-encoded string,
            # so code mode renders `-> str` instead of `-> Any`.
            tool_def = ToolDefinition(
                name=name,
                description=description,
                parameters_json_schema=schema,
                sequential=True,
                **return_schema_kwargs({"type": "string"}),
            )
            tools[name] = ToolsetTool(
                toolset=self,
                tool_def=tool_def,
                max_retries=1,
                args_validator=_PASSTHROUGH_VALIDATOR,
            )
        return tools

    async def call_tool(
        self,
        name: str,
        tool_args: dict[str, Any],
        ctx: RunContext[Any],
        tool: ToolsetTool[Any],
    ) -> Any:
        if name not in ("list_gcp_methods", "describe_gcp_method", "call_gcp"):
            raise ValueError(f"Unknown tool: {name!r}")
        try:
            if name == "list_gcp_methods":
                api_filter = tool_args.get("api")
                if api_filter is not None and not isinstance(api_filter, str):
                    raise ValueError("The 'api' argument must be a string, e.g. 'storage/v1'.")
                return self._list_methods(api_filter=api_filter)
            api = _require_string_arg(tool_args=tool_args, key="api")
            method = _require_string_arg(tool_args=tool_args, key="method")
            if name == "describe_gcp_method":
                return self._describe_method(api_version=api, method=method)
            parameters = tool_args.get("parameters") or {}
            if not isinstance(parameters, dict):
                raise ValueError("The 'parameters' argument must be a JSON object.")
            body = tool_args.get("body")
            if body is not None and not isinstance(body, dict):
                raise ValueError("The 'body' argument must be a JSON object.")
            return self._call_gcp(api_version=api, method=method, parameters=parameters, body=body)
        except Exception as e:
            # Hand Google's own error back to the agent as a retry so it can
            # correct its parameters within the run. pydantic-ai bounds this by
            # the tool's max_retries, so an unrecoverable error (bad
            # credentials, missing IAM permissions) exhausts the budget and
            # fails the task for Airflow to retry.
            raise ModelRetry(
                f"The {name} tool failed: {e}\n"
                "Use list_gcp_methods to see what you may call and "
                "describe_gcp_method to check the expected parameters, then try again."
            ) from e

    # ------------------------------------------------------------------
    # Authorization
    # ------------------------------------------------------------------

    def _is_method_allowed(self, *, api: str, version: str, method_path: str) -> bool:
        key = _normalize_method(f"{api}/{version}:{method_path}")
        if _normalize_method(f"{api}:{method_path}") in _CREDENTIAL_RETURNING_METHODS:
            return key in self._literal_methods
        return any(fnmatchcase(key, pattern) for pattern in self._patterns.get((api, version), ()))

    def _get_allowed_api(self, *, api_version: str) -> tuple[str, str]:
        """Validate a tool-supplied ``api/version`` against the allow-list."""
        api, _, version = api_version.partition("/")
        key = (api, version)
        if not version or key not in self._patterns:
            allowed = ", ".join(sorted(f"{a}/{v}" for a, v in self._patterns))
            raise ValueError(
                f"API {api_version!r} is not in this toolset's allowed methods. Allowed APIs: {allowed}."
            )
        return key

    # ------------------------------------------------------------------
    # Tool implementations
    # ------------------------------------------------------------------

    def _list_methods(self, *, api_filter: str | None) -> str:
        keys = [self._get_allowed_api(api_version=api_filter)] if api_filter else sorted(self._patterns)
        listing = {}
        for api, version in keys:
            method_paths = self._get_method_paths(api=api, version=version)
            listing[f"{api}/{version}"] = sorted(
                canonical
                for canonical in set(method_paths.values())
                if self._is_method_allowed(api=api, version=version, method_path=canonical)
            )
        return json.dumps(listing)

    def _describe_method(self, *, api_version: str, method: str) -> str:
        api, version = self._get_allowed_api(api_version=api_version)
        doc = self._get_doc(api=api, version=version)
        canonical = self._get_method_paths(api=api, version=version).get(_normalize_method(method))
        if canonical is None:
            raise ValueError(f"Unknown method {method!r} for {api}/{version}.")
        if not self._is_method_allowed(api=api, version=version, method_path=canonical):
            raise ValueError(f"Method {api}/{version}:{canonical} is not in this toolset's allowed methods.")
        log.info("Describing Google Cloud API method %s/%s:%s", api, version, canonical)
        method_doc = _get_method_doc(doc, canonical)
        request_ref = method_doc.get("request")
        response_ref = method_doc.get("response")
        return json.dumps(
            {
                "api": f"{api}/{version}",
                "method": canonical,
                "http_method": method_doc.get("httpMethod"),
                "description": (method_doc.get("description") or "")[:600],
                "parameters": _describe_parameters(method_doc),
                "request_body": _describe_schema(doc, request_ref) if request_ref else None,
                "response": _describe_schema(doc, response_ref) if response_ref else None,
                "scopes": method_doc.get("scopes", []),
            }
        )

    def _call_gcp(
        self,
        *,
        api_version: str,
        method: str,
        parameters: dict[str, Any],
        body: dict[str, Any] | None,
    ) -> str:
        api, version = self._get_allowed_api(api_version=api_version)
        doc = self._get_doc(api=api, version=version)
        canonical = self._get_method_paths(api=api, version=version).get(_normalize_method(method))
        if canonical is None:
            raise ValueError(f"Unknown method {method!r} for {api}/{version}.")
        if not self._is_method_allowed(api=api, version=version, method_path=canonical):
            raise ValueError(
                f"Method {api}/{version}:{canonical} is not in this toolset's allowed methods. "
                "Use list_gcp_methods to see what you may call."
            )
        log.info("Calling Google Cloud API method %s/%s:%s", api, version, canonical)
        method_doc = _get_method_doc(doc, canonical)

        params = dict(parameters)
        if str(params.get("alt", "")).casefold() == "media":
            raise ValueError(
                "Media download (alt=media) is not supported by this toolset; "
                "metadata calls on the same resource work."
            )
        # media_body/media_mime_type are googleapiclient upload kwargs, not API
        # parameters -- a string media_body would make the client read a local
        # file from the worker. Media transfer is unsupported here.
        for media_param in ("media_body", "media_mime_type"):
            if media_param in params:
                raise ValueError(
                    f"Media upload ({media_param}) is not supported by this toolset; "
                    "metadata calls on the same resource work."
                )
        self._apply_project_guard(method_doc=method_doc, params=params)
        params = _normalize_parameters_for_google_client(method_doc=method_doc, parameters=params)

        service = self._get_service(api=api, version=version)
        *resource_path, method_name = canonical.split(".")
        node: Any = service
        for resource in resource_path:
            node = getattr(node, resource)()
        bound = getattr(node, method_name)
        request = bound(**params, body=body) if body is not None else bound(**params)
        response = request.execute()

        pages = [response]
        next_method = getattr(node, f"{method_name}_next", None)
        while next_method is not None and isinstance(pages[-1], dict) and len(pages) < self._max_pages:
            request = next_method(previous_request=request, previous_response=pages[-1])
            if request is None:
                break
            pages.append(request.execute())

        if len(pages) == 1:
            return self._serialize_response(response)
        payload: dict[str, Any] = {"pages": pages, "page_count": len(pages)}
        if isinstance(pages[-1], dict) and pages[-1].get("nextPageToken"):
            payload["more_pages_available"] = True
        return self._serialize_response(payload)

    # ------------------------------------------------------------------
    # Project pinning
    # ------------------------------------------------------------------

    def _apply_project_guard(self, *, method_doc: dict[str, Any], params: dict[str, Any]) -> None:
        if not (project := self._get_hook().project_id):
            return
        param_defs = method_doc.get("parameters", {})
        for param_name in ("project", "projectId"):
            if param_name in param_defs:
                expected = _format_project_parameter(project=project, param_def=param_defs[param_name])
                supplied = params.get(param_name)
                if supplied is None:
                    params[param_name] = expected
                elif supplied == project and expected != project:
                    params[param_name] = expected
                elif self._enforce_project and supplied != expected:
                    raise ValueError(f"{param_name} must be {expected!r} (the connection's project).")
        if not self._enforce_project:
            return
        # Best-effort guard on path-style parameters ("projects/<id>/...").
        for param_name, param_def in param_defs.items():
            value = params.get(param_name)
            if (
                param_def.get("location") == "path"
                and isinstance(value, str)
                and value.startswith("projects/")
            ):
                segments = value.split("/")
                if len(segments) >= 2 and segments[1] != project:
                    raise ValueError(
                        f"{param_name} references project {segments[1]!r}; this toolset is "
                        f"pinned to {project!r}."
                    )

    # ------------------------------------------------------------------
    # Lazy hook/service/document resolution and serialization
    # ------------------------------------------------------------------

    def _get_hook(self) -> Any:
        if self._hook is None:
            self._hook = _get_google_base_hook_class()(
                gcp_conn_id=self._gcp_conn_id, impersonation_chain=self._impersonation_chain
            )
        return self._hook

    def _get_service(self, *, api: str, version: str) -> Any:
        key = (api, version)
        if key not in self._services:
            self._services[key] = build(
                api,
                version,
                credentials=self._get_hook().get_credentials(),
                cache_discovery=False,
                static_discovery=key in _bundled_apis(),
            )
        return self._services[key]

    def _get_doc(self, *, api: str, version: str) -> dict[str, Any]:
        key = (api, version)
        if key in _bundled_apis():
            return _load_bundled_doc(api, version)
        # Normally only reachable when allow_remote_discovery=True let the entry
        # through construction; re-checked here in case the bundle differs between
        # the parsing and execution environments. Fetched lazily at call time,
        # never at Dag parse time.
        if not self._allow_remote_discovery:
            raise ValueError(
                f"API {api}/{version} has no bundled discovery document and "
                "allow_remote_discovery is disabled."
            )
        if key not in self._remote_docs:
            response = requests.get(_DISCOVERY_DOC_URL.format(api=api, version=version), timeout=30)
            response.raise_for_status()
            self._remote_docs[key] = response.json()
        return self._remote_docs[key]

    def _get_method_paths(self, *, api: str, version: str) -> dict[str, str]:
        key = (api, version)
        if key in _bundled_apis():
            return _get_bundled_method_paths(api, version)
        return _collect_method_paths(self._get_doc(api=api, version=version))

    def _serialize_response(self, response: Any) -> str:
        payload = json.dumps(response, default=str)
        if len(payload) > self._max_output_bytes:
            return json.dumps(
                {
                    "truncated": True,
                    "max_output_bytes": self._max_output_bytes,
                    "data": payload[: self._max_output_bytes],
                }
            )
        return payload


@cache
def _bundled_docs_dir() -> str:
    return os.path.join(os.path.dirname(googleapiclient.discovery_cache.__file__), "documents")


@cache
def _bundled_apis() -> frozenset[tuple[str, str]]:
    pairs = set()
    for filename in os.listdir(_bundled_docs_dir()):
        if filename.endswith(".json"):
            api, _, version = filename[:-5].partition(".")
            if version:
                pairs.add((api, version))
    return frozenset(pairs)


def _load_bundled_doc(api: str, version: str) -> dict[str, Any]:
    path = os.path.join(_bundled_docs_dir(), f"{api}.{version}.json")
    with open(path) as f:
        return json.load(f)


@cache
def _get_bundled_method_paths(api: str, version: str) -> dict[str, str]:
    return _collect_method_paths(_load_bundled_doc(api, version))


def _collect_method_paths(doc: dict[str, Any]) -> dict[str, str]:
    """
    Map normalized method paths to their canonical form for one discovery document.

    Google's reference docs display method IDs with the API name prefixed
    (``storage.buckets.list``), so each path is also registered under that
    alias -- users can paste either form.
    """
    paths: dict[str, str] = {}
    for method_name in doc.get("methods", {}):
        paths[_normalize_method(method_name)] = method_name

    def walk(resources: dict[str, Any], prefix: str) -> None:
        for resource_name, resource in resources.items():
            for method_name in resource.get("methods", {}):
                canonical = f"{prefix}{resource_name}.{method_name}"
                paths[_normalize_method(canonical)] = canonical
            if "resources" in resource:
                walk(resource["resources"], f"{prefix}{resource_name}.")

    walk(doc.get("resources", {}), "")

    if api_name := doc.get("name"):
        for canonical in list(paths.values()):
            paths.setdefault(_normalize_method(f"{api_name}.{canonical}"), canonical)
    return paths


def _get_method_doc(doc: dict[str, Any], method_path: str) -> dict[str, Any]:
    """Return the discovery entry for a canonical method path."""
    *resource_path, method_name = method_path.split(".")
    node = doc
    for resource in resource_path:
        node = node["resources"][resource]
    return node["methods"][method_name]


def _normalize_parameters_for_google_client(
    *,
    method_doc: dict[str, Any],
    parameters: dict[str, Any],
) -> dict[str, Any]:
    """
    Convert documented Google REST parameter names to googleapiclient keyword names.

    Discovery documents describe parameters using the public REST names shown in
    Google docs and APIs Explorer, for example ``interval.startTime`` or
    ``options.requestedPolicyVersion``. The generated ``googleapiclient`` Python
    methods cannot accept those names directly as keyword arguments, so the
    client converts them to safe Python names such as ``interval_startTime`` and
    ``options_requestedPolicyVersion``.

    Keep the tool contract model-facing and doc-facing by accepting the REST
    names from ``describe_gcp_method``, then translate only at the final
    ``googleapiclient`` call boundary using the same per-method mapping the
    client uses internally.
    """
    method_params = ResourceMethodParameters(method_doc)
    rest_to_client = {rest_name: client_name for client_name, rest_name in method_params.argmap.items()}

    normalized: dict[str, Any] = {}
    for name, value in parameters.items():
        client_name = rest_to_client.get(name, name)
        if client_name in normalized and normalized[client_name] != value:
            raise ValueError(
                f"Parameter {name!r} conflicts with another parameter mapped to {client_name!r}."
            )
        normalized[client_name] = value
    return normalized


def _describe_parameters(method_doc: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for name, param in method_doc.get("parameters", {}).items():
        entry: dict[str, Any] = {
            "type": param.get("type", "string"),
            "location": param.get("location", "query"),
        }
        if param.get("required"):
            entry["required"] = True
        if param.get("description"):
            entry["description"] = param["description"][:200]
        out[name] = entry
    return out


def _describe_schema(
    doc: dict[str, Any],
    node: dict[str, Any],
    depth: int = _SCHEMA_DEPTH,
    seen: frozenset[str] = frozenset(),
) -> Any:
    """
    Render a discovery schema as a compact JSON-friendly summary.

    Discovery schemas are ``$ref``-based and recursive, so resolution carries a
    visited set: a schema already on the current path is cut to its name.
    """
    if "$ref" in node:
        ref = node["$ref"]
        if ref in seen or depth <= 0:
            return ref
        return _describe_schema(doc, doc.get("schemas", {}).get(ref, {}), depth - 1, seen | {ref})
    type_name = node.get("type", "any")
    if type_name == "object":
        properties = node.get("properties")
        if properties and depth > 0:
            out = {}
            for name, member in properties.items():
                summary = _describe_schema(doc, member, depth - 1, seen)
                if member.get("required"):
                    out[name] = {"type": summary, "required": True}
                else:
                    out[name] = {"type": summary}
            return out
        if "additionalProperties" in node and depth > 0:
            return {"<key>": _describe_schema(doc, node["additionalProperties"], depth - 1, seen)}
        return type_name
    if type_name == "array":
        if depth > 0:
            return [_describe_schema(doc, node.get("items", {}), depth - 1, seen)]
        return type_name
    if enum := node.get("enum"):
        return f"{type_name} (one of: {', '.join(enum[:20])})"
    return type_name
