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
"""Curated AWS toolset exposing allow-listed boto3 operations as pydantic-ai tools."""

from __future__ import annotations

import json
import re
from fnmatch import fnmatchcase
from functools import cache
from typing import TYPE_CHECKING, Any

from pydantic_ai.exceptions import ModelRetry
from pydantic_ai.tools import ToolDefinition
from pydantic_ai.toolsets.abstract import AbstractToolset, ToolsetTool
from pydantic_core import SchemaValidator, core_schema

from airflow.providers.common.ai.utils.tool_definition import return_schema_kwargs

try:
    import botocore.session
    from botocore import xform_name
    from botocore.response import StreamingBody

    from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
except ImportError as e:
    from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException

    raise AirflowOptionalProviderFeatureException() from e

if TYPE_CHECKING:
    from botocore.client import BaseClient
    from botocore.model import ServiceModel, Shape
    from pydantic_ai._run_context import RunContext

_PASSTHROUGH_VALIDATOR = SchemaValidator(core_schema.any_schema())


def _normalize_action(action: str) -> str:
    """Comparison key for actions: case- and underscore-insensitive."""
    return action.replace("_", "").casefold()


# High-confidence operations that can put credentials, tokens, passwords,
# decrypted secret material, or plaintext key material into the agent's context.
# A wildcard in ``allowed_actions`` never matches these -- each must be listed
# verbatim to be callable. This is a defense-in-depth guard, not an exhaustive
# AWS security boundary; least-privilege IAM on the connection's role remains
# the real boundary.
_CREDENTIAL_RETURNING_ACTIONS = frozenset(
    _normalize_action(action)
    for action in (
        "sts:AssumeRole",
        "sts:AssumeRoleWithSAML",
        "sts:AssumeRoleWithWebIdentity",
        "sts:GetFederationToken",
        "sts:GetSessionToken",
        "secretsmanager:GetSecretValue",
        "secretsmanager:BatchGetSecretValue",
        "ssm:GetParameter",
        "ssm:GetParameters",
        "ssm:GetParametersByPath",
        "kms:Decrypt",
        "kms:GenerateDataKey",
        "kms:GenerateDataKeyPair",
        "ecr:GetAuthorizationToken",
        "ecr-public:GetAuthorizationToken",
        "iam:CreateAccessKey",
        "iam:CreateLoginProfile",
        "iam:UpdateLoginProfile",
        "iam:CreateServiceSpecificCredential",
        "iam:ResetServiceSpecificCredential",
        "cognito-identity:GetCredentialsForIdentity",
        "cognito-identity:GetOpenIdToken",
        "cognito-identity:GetOpenIdTokenForDeveloperIdentity",
        "redshift:GetClusterCredentials",
        "redshift:GetClusterCredentialsWithIAM",
        "redshift-serverless:GetCredentials",
        "lightsail:GetInstanceAccessDetails",
        "lightsail:GetRelationalDatabaseMasterUserPassword",
    )
)

_SHAPE_DEPTH = 3

# JSON Schemas for the three AWS tools.
_LIST_OPERATIONS_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "service": {
            "type": "string",
            "description": "Optional service name filter, e.g. 's3'.",
        },
    },
}

_DESCRIBE_OPERATION_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "service": {"type": "string", "description": "AWS service name, e.g. 's3'."},
        "operation": {
            "type": "string",
            "description": "API operation name, e.g. 'ListObjectsV2'.",
        },
    },
    "required": ["service", "operation"],
}

_CALL_AWS_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "service": {"type": "string", "description": "AWS service name, e.g. 's3'."},
        "operation": {
            "type": "string",
            "description": "API operation name, e.g. 'ListObjectsV2'.",
        },
        "parameters": {
            "type": "object",
            "description": (
                "Operation parameters exactly as the API expects them (see describe_aws_operation)."
            ),
        },
    },
    "required": ["service", "operation"],
}


class AWSToolset(AbstractToolset[Any]):
    """
    Curated toolset that gives an LLM agent allow-listed access to AWS APIs.

    Exposes three tools:

    - ``list_aws_operations`` -- the operations this toolset allows, grouped by
      service (the allow-list intersected with the botocore service model).
    - ``describe_aws_operation`` -- an operation's parameter and response shapes,
      derived from the botocore service model, so the agent can check what a
      call expects *before* making it.
    - ``call_aws`` -- execute an allowed operation and return the response as
      JSON (paginated operations are aggregated up to ``max_items``).

    Credentials, region, and session configuration come exclusively from the
    Airflow connection (resolved lazily through the amazon provider's
    ``AwsBaseHook``); none of them are tool arguments, so the model cannot
    steer them.

    When a call fails, AWS's own error message is returned to the agent as a
    retry (:class:`pydantic_ai.ModelRetry`) so the model can correct its
    parameters within the run. pydantic-ai bounds this by the tool's
    ``max_retries``, so an unrecoverable error -- bad credentials, missing IAM
    permissions -- exhausts the retries and fails the task for Airflow to retry.

    :param aws_conn_id: Airflow connection ID for AWS credentials.
    :param allowed_actions: Operations the agent may call, in
        ``"<service>:<Operation>"`` form using boto3 service names and API
        operation names -- e.g. ``["s3:ListBuckets", "s3:GetObject",
        "athena:*"]``. Required -- access is deny-by-default and there is
        deliberately no auto-discovery. The operation part accepts ``*``/``?``
        wildcards; the service part must be explicit. Matching is case- and
        underscore-insensitive, so ``"s3:list_buckets"`` and
        ``"s3:ListBuckets"`` are equivalent.

        Operations that return credentials or decrypted secrets (for example
        ``sts:AssumeRole``, ``secretsmanager:GetSecretValue``, ``kms:Decrypt``)
        are never matched by a wildcard -- each must be listed verbatim to be
        callable.

        .. note::
            This is an application-level guardrail: it bounds what the agent
            can *ask for*, not what the credentials can *do*. Pair it with a
            least-privilege IAM role on ``aws_conn_id`` for a hard guarantee.

    :param region_name: AWS region for API calls. ``None`` (default) uses the
        region configured on the connection. Deliberately not exposed as a tool
        argument.
    :param max_items: Upper bound on items aggregated from paginated operations
        (applied via the paginator's ``MaxItems``). Default ``1000``.
    :param max_output_bytes: Upper bound on the serialized response returned to
        the agent. Larger payloads are clipped and flagged with
        ``"truncated": true``. Default ``65536``.
    """

    def __init__(
        self,
        aws_conn_id: str = "aws_default",
        *,
        allowed_actions: list[str],
        region_name: str | None = None,
        max_items: int = 1000,
        max_output_bytes: int = 65536,
    ) -> None:
        if not allowed_actions:
            raise ValueError("allowed_actions must be a non-empty list.")

        # Validation below uses botocore's bundled service definitions only --
        # local file reads, no credentials, no network -- so it is safe to run
        # at Dag parse time.
        patterns: dict[str, list[str]] = {}
        literal_actions: set[str] = set()
        for action in allowed_actions:
            service, sep, operation = action.partition(":")
            if not sep or not service or not operation:
                raise ValueError(
                    f"Invalid action {action!r}: expected '<service>:<Operation>', e.g. 's3:ListBuckets'."
                )
            if any(ch in service for ch in "*?["):
                raise ValueError(f"Invalid action {action!r}: wildcards are not allowed in the service part.")
            if service not in _get_available_services():
                raise ValueError(
                    f"Unknown AWS service {service!r} in action {action!r}. "
                    "Use the boto3 service name, e.g. 's3' or 'secretsmanager'."
                )
            key = _normalize_action(action)
            if any(ch in operation for ch in "*?["):
                model = _load_service_model(service)
                matched_operations = [
                    name
                    for name in model.operation_names
                    if self._is_pattern_match_allowed(key, service, name)
                ]
                if not matched_operations:
                    raise ValueError(
                        f"Action pattern {action!r} does not match any non-sensitive operation "
                        f"for service {service!r}."
                    )
                patterns.setdefault(service, []).append(key)
            else:
                if _resolve_operation_name(_load_service_model(service), operation) is None:
                    raise ValueError(
                        f"Unknown operation {operation!r} for service {service!r} in action "
                        f"{action!r}. Use the API operation name, e.g. 'ListBuckets'."
                    )
                patterns.setdefault(service, []).append(key)
                literal_actions.add(key)

        self._aws_conn_id = aws_conn_id
        self._region_name = region_name
        self._max_items = max_items
        self._max_output_bytes = max_output_bytes
        self._patterns = patterns
        self._literal_actions = frozenset(literal_actions)
        # Populated lazily at call time, so Dag parsing never creates AWS clients.
        self._clients: dict[str, BaseClient] = {}

    @property
    def id(self) -> str:
        return f"aws-{self._aws_conn_id}"

    # ------------------------------------------------------------------
    # AbstractToolset interface
    # ------------------------------------------------------------------

    async def get_tools(self, ctx: RunContext[Any]) -> dict[str, ToolsetTool[Any]]:
        tools: dict[str, ToolsetTool[Any]] = {}

        for name, description, schema in (
            (
                "list_aws_operations",
                "List the AWS operations this toolset allows, grouped by service.",
                _LIST_OPERATIONS_SCHEMA,
            ),
            (
                "describe_aws_operation",
                "Show an operation's parameters and response shape before calling it.",
                _DESCRIBE_OPERATION_SCHEMA,
            ),
            (
                "call_aws",
                "Execute an allowed AWS operation and return the response as JSON.",
                _CALL_AWS_SCHEMA,
            ),
        ):
            # sequential=True because all tools share per-service boto3 clients
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
        if name not in ("list_aws_operations", "describe_aws_operation", "call_aws"):
            raise ValueError(f"Unknown tool: {name!r}")
        try:
            if name == "list_aws_operations":
                return self._list_operations(tool_args.get("service"))
            if name == "describe_aws_operation":
                return self._describe_operation(tool_args["service"], tool_args["operation"])
            return self._call_aws(
                tool_args["service"], tool_args["operation"], tool_args.get("parameters") or {}
            )
        except Exception as e:
            # Hand AWS's own error back to the agent as a retry so it can correct
            # its parameters within the run. pydantic-ai bounds this by the tool's
            # max_retries, so an unrecoverable error (bad credentials, missing IAM
            # permissions) exhausts the budget and fails the task for Airflow to
            # retry, rather than being silently worked around.
            raise ModelRetry(
                f"The {name} tool failed: {e}\n"
                "Use list_aws_operations to see what you may call and "
                "describe_aws_operation to check the expected parameters, then try again."
            ) from e

    # ------------------------------------------------------------------
    # Authorization
    # ------------------------------------------------------------------

    def _is_action_allowed(self, *, service: str, operation: str) -> bool:
        key = _normalize_action(f"{service}:{operation}")
        if key in _CREDENTIAL_RETURNING_ACTIONS:
            return key in self._literal_actions
        return any(fnmatchcase(key, pattern) for pattern in self._patterns.get(service, ()))

    def _is_pattern_match_allowed(self, pattern: str, service: str, operation: str) -> bool:
        key = _normalize_action(f"{service}:{operation}")
        return key not in _CREDENTIAL_RETURNING_ACTIONS and fnmatchcase(key, pattern)

    def _resolve_allowed_service(self, service: str) -> str:
        """Validate a tool-supplied service name against the allow-list."""
        if service not in self._patterns:
            raise ValueError(
                f"Service {service!r} is not in this toolset's allowed actions. "
                f"Allowed services: {', '.join(sorted(self._patterns))}."
            )
        return service

    def _resolve_allowed_operation(self, service: str, operation: str) -> tuple[str, ServiceModel]:
        model = _load_service_model(self._resolve_allowed_service(service))
        canonical = _resolve_operation_name(model, operation)
        if canonical is None:
            raise ValueError(f"Unknown operation {operation!r} for service {service!r}.")
        if not self._is_action_allowed(service=service, operation=canonical):
            raise ValueError(f"Operation {service}:{canonical} is not in this toolset's allowed actions.")
        return canonical, model

    # ------------------------------------------------------------------
    # Tool implementations
    # ------------------------------------------------------------------

    def _list_operations(self, service: str | None) -> str:
        services = [self._resolve_allowed_service(service)] if service else sorted(self._patterns)
        listing = {
            svc: sorted(
                name
                for name in _load_service_model(svc).operation_names
                if self._is_action_allowed(service=svc, operation=name)
            )
            for svc in services
        }
        return json.dumps(listing)

    def _describe_operation(self, service: str, operation: str) -> str:
        canonical, model = self._resolve_allowed_operation(service, operation)
        op = model.operation_model(canonical)
        return json.dumps(
            {
                "service": service,
                "operation": canonical,
                "documentation": _strip_html(op.documentation)[:600],
                "input": _describe_shape(op.input_shape),
                "output": _describe_shape(op.output_shape),
            }
        )

    def _call_aws(self, service: str, operation: str, parameters: dict[str, Any]) -> str:
        canonical, _ = self._resolve_allowed_operation(service, operation)
        client = self._get_client(service)
        method_name = xform_name(canonical)
        if client.can_paginate(method_name):
            pages = client.get_paginator(method_name).paginate(
                **parameters, PaginationConfig={"MaxItems": self._max_items}
            )
            response = pages.build_full_result()
        else:
            response = getattr(client, method_name)(**parameters)
        return self._serialize_response(response)

    # ------------------------------------------------------------------
    # Lazy client resolution and serialization
    # ------------------------------------------------------------------

    def _get_client(self, service: str) -> BaseClient:
        if service not in self._clients:
            hook = AwsBaseHook(
                aws_conn_id=self._aws_conn_id, client_type=service, region_name=self._region_name
            )
            self._clients[service] = hook.get_conn()
        return self._clients[service]

    def _serialize_response(self, response: Any) -> str:
        if isinstance(response, dict):
            response.pop("ResponseMetadata", None)
        payload = json.dumps(_to_jsonable(response, max_bytes=self._max_output_bytes), default=str)
        if len(payload) > self._max_output_bytes:
            return json.dumps(
                {
                    "truncated": True,
                    "max_output_bytes": self._max_output_bytes,
                    "data": payload[: self._max_output_bytes],
                }
            )
        return payload


# ---------------------------------------------------------------------------
# Private botocore helpers
# ---------------------------------------------------------------------------


@cache
def _get_available_services() -> frozenset[str]:
    return frozenset(botocore.session.get_session().get_available_services())


@cache
def _load_service_model(service: str) -> ServiceModel:
    return botocore.session.get_session().get_service_model(service)


def _resolve_operation_name(model: ServiceModel, operation: str) -> str | None:
    """Resolve a case/underscore-insensitive operation name to its canonical API name."""
    wanted = _normalize_action(operation)
    for name in model.operation_names:
        if _normalize_action(name) == wanted:
            return name
    return None


def _describe_shape(shape: Shape | None, depth: int = _SHAPE_DEPTH) -> Any:
    """Render a botocore shape as a compact JSON-friendly summary."""
    if shape is None:
        return None
    type_name = shape.type_name
    if type_name == "structure":
        if depth <= 0:
            return type_name
        required = set(shape.required_members)
        return {
            name: (
                {"type": _describe_shape(member, depth - 1), "required": True}
                if name in required
                else {"type": _describe_shape(member, depth - 1)}
            )
            for name, member in shape.members.items()
        }
    if type_name == "list":
        return [_describe_shape(shape.member, depth - 1)] if depth > 0 else type_name
    if type_name == "map":
        return {"<key>": _describe_shape(shape.value, depth - 1)} if depth > 0 else type_name
    enum = getattr(shape, "enum", None)
    if enum:
        return f"{type_name} (one of: {', '.join(enum)})"
    return type_name


_HTML_TAG_RE = re.compile(r"<[^>]+>")


def _strip_html(text: str | None) -> str:
    return " ".join(_HTML_TAG_RE.sub(" ", text or "").split())


def _to_jsonable(value: Any, *, max_bytes: int) -> Any:
    """Make a boto3 response JSON-serializable; datetimes are handled by ``default=str``."""
    if isinstance(value, StreamingBody):
        # Read one byte past the cap so truncation is detectable.
        raw = value.read(max_bytes + 1)
        text = raw[:max_bytes].decode("utf-8", errors="replace")
        if len(raw) > max_bytes:
            return {"truncated": True, "data": text}
        return text
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    if isinstance(value, dict):
        return {k: _to_jsonable(v, max_bytes=max_bytes) for k, v in value.items()}
    if isinstance(value, list):
        return [_to_jsonable(v, max_bytes=max_bytes) for v in value]
    return value
