#!/usr/bin/env python3
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
"""Shared contracts for registry generator payloads and API schemas.

The generator scripts use these Pydantic models to validate payloads before
writing JSON files. The same models are also used to export JSON Schema
artifacts and to build the OpenAPI document for the registry API.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, model_validator


class CategoryContract(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str
    name: str
    module_count: int = 0


class DownloadStatsContract(BaseModel):
    model_config = ConfigDict(extra="forbid")

    weekly: int = 0
    monthly: int = 0
    total: int = 0


class ConnectionTypeContract(BaseModel):
    model_config = ConfigDict(extra="forbid")

    conn_type: str
    hook_class: str = ""
    docs_url: str | None = None


class ProviderContract(BaseModel):
    """Top-level provider entry in providers.json."""

    model_config = ConfigDict(extra="forbid")

    id: str
    name: str
    package_name: str
    description: str
    lifecycle: str = "production"
    logo: str | None = None
    version: str
    versions: list[str]
    airflow_versions: list[str] = Field(default_factory=list)
    pypi_downloads: DownloadStatsContract = Field(default_factory=DownloadStatsContract)
    module_counts: dict[str, int] = Field(default_factory=dict)
    categories: list[CategoryContract] = Field(default_factory=list)
    connection_types: list[ConnectionTypeContract] = Field(default_factory=list)
    requires_python: str = ""
    dependencies: list[str] = Field(default_factory=list)
    optional_extras: dict[str, list[str]] = Field(default_factory=dict)
    dependents: list[str] = Field(default_factory=list)
    related_providers: list[str] = Field(default_factory=list)
    docs_url: str
    source_url: str
    pypi_url: str
    first_released: str = ""
    last_updated: str = ""


class ProvidersCatalogContract(BaseModel):
    model_config = ConfigDict(extra="forbid")

    providers: list[ProviderContract]


class ModuleContract(BaseModel):
    """A registry module entry.

    ``module_path`` is optional for older versioned metadata generated from git
    tags where only import paths are available.
    """

    model_config = ConfigDict(extra="forbid")

    id: str | None = None
    name: str
    type: str
    import_path: str
    module_path: str | None = None
    short_description: str
    docs_url: str
    source_url: str
    category: str
    provider_id: str | None = None
    provider_name: str | None = None


class ModulesCatalogContract(BaseModel):
    model_config = ConfigDict(extra="forbid")

    modules: list[ModuleContract]


class ProviderModulesContract(BaseModel):
    model_config = ConfigDict(extra="forbid")

    provider_id: str
    provider_name: str
    version: str
    modules: list[ModuleContract]


class ParameterContract(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    type: str | None = None
    default: Any = None
    required: bool
    origin: str
    description: str | None = None


class ClassParametersEntryContract(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    name: str
    type: str
    mro_chain: list[str] = Field(alias="mro", serialization_alias="mro")
    parameters: list[ParameterContract]


class ProviderParametersContract(BaseModel):
    model_config = ConfigDict(extra="forbid")

    provider_id: str
    provider_name: str
    version: str
    generated_at: str | None = None
    classes: dict[str, ClassParametersEntryContract]


class StandardConnectionFieldContract(BaseModel):
    model_config = ConfigDict(extra="forbid")

    visible: bool
    label: str
    placeholder: str | None = None


class CustomConnectionFieldContract(BaseModel):
    # Keep this extensible for provider-specific form metadata.
    model_config = ConfigDict(extra="allow")

    label: str
    type: Any
    default: Any = None
    format: str | None = None
    description: str | None = None
    is_sensitive: bool = False
    enum: list[Any] | None = None
    minimum: int | float | None = None
    maximum: int | float | None = None


class ProviderConnectionTypeContract(BaseModel):
    # Keep this extensible for provider-specific hook metadata.
    model_config = ConfigDict(extra="allow")

    connection_type: str
    hook_class: str | None = None
    standard_fields: dict[str, StandardConnectionFieldContract]
    custom_fields: dict[str, CustomConnectionFieldContract] = Field(default_factory=dict)


class ProviderConnectionsContract(BaseModel):
    model_config = ConfigDict(extra="forbid")

    provider_id: str
    provider_name: str
    version: str
    generated_at: str | None = None
    connection_types: list[ProviderConnectionTypeContract]


class ProviderVersionMetadataContract(BaseModel):
    model_config = ConfigDict(extra="forbid")

    provider_id: str
    version: str
    generated_at: str
    requires_python: str
    dependencies: list[str]
    optional_extras: dict[str, list[str]]
    connection_types: list[ConnectionTypeContract]
    module_counts: dict[str, int]
    modules: list[ModuleContract]


class ProviderVersionsContract(BaseModel):
    model_config = ConfigDict(extra="forbid")

    latest: str
    versions: list[str]

    @model_validator(mode="after")
    def ensure_latest_is_listed(self) -> ProviderVersionsContract:
        if self.latest not in self.versions:
            raise ValueError("latest version must be included in versions list")
        return self


def _validate(model_type: type[BaseModel], payload: dict[str, Any]) -> dict[str, Any]:
    model_type.model_validate(payload)
    return payload


def validate_providers_catalog(payload: dict[str, Any]) -> dict[str, Any]:
    return _validate(ProvidersCatalogContract, payload)


def validate_modules_catalog(payload: dict[str, Any]) -> dict[str, Any]:
    return _validate(ModulesCatalogContract, payload)


def validate_provider_modules(payload: dict[str, Any]) -> dict[str, Any]:
    return _validate(ProviderModulesContract, payload)


def validate_provider_parameters(payload: dict[str, Any]) -> dict[str, Any]:
    return _validate(ProviderParametersContract, payload)


def validate_provider_connections(payload: dict[str, Any]) -> dict[str, Any]:
    return _validate(ProviderConnectionsContract, payload)


def validate_provider_version_metadata(payload: dict[str, Any]) -> dict[str, Any]:
    return _validate(ProviderVersionMetadataContract, payload)


def validate_provider_versions(payload: dict[str, Any]) -> dict[str, Any]:
    return _validate(ProviderVersionsContract, payload)


@dataclass(frozen=True)
class OpenApiEndpoint:
    path: str
    tag: str
    operation_id: str
    summary: str
    response_description: str
    response_component: str
    parameters: tuple[str, ...] = ()
    include_not_found: bool = False


OPENAPI_ENDPOINTS: tuple[OpenApiEndpoint, ...] = (
    OpenApiEndpoint(
        path="/api/providers.json",
        tag="Catalog",
        operation_id="listProviders",
        summary="List providers",
        response_description="Provider catalog.",
        response_component="ProvidersCatalogPayload",
    ),
    OpenApiEndpoint(
        path="/api/modules.json",
        tag="Catalog",
        operation_id="listModules",
        summary="List modules",
        response_description="Module catalog.",
        response_component="ModulesCatalogPayload",
    ),
    OpenApiEndpoint(
        path="/api/providers/{providerId}/modules.json",
        tag="Providers",
        operation_id="getProviderModulesLatest",
        summary="Get provider modules (latest)",
        response_description="Provider modules for latest version.",
        response_component="ProviderModulesPayload",
        parameters=("ProviderId",),
        include_not_found=True,
    ),
    OpenApiEndpoint(
        path="/api/providers/{providerId}/parameters.json",
        tag="Providers",
        operation_id="getProviderParametersLatest",
        summary="Get provider parameters (latest)",
        response_description="Provider parameters for latest version.",
        response_component="ProviderParametersPayload",
        parameters=("ProviderId",),
        include_not_found=True,
    ),
    OpenApiEndpoint(
        path="/api/providers/{providerId}/connections.json",
        tag="Providers",
        operation_id="getProviderConnectionsLatest",
        summary="Get provider connections (latest)",
        response_description="Provider connections for latest version.",
        response_component="ProviderConnectionsPayload",
        parameters=("ProviderId",),
        include_not_found=True,
    ),
    OpenApiEndpoint(
        path="/api/providers/{providerId}/{version}/modules.json",
        tag="Provider Versions",
        operation_id="getProviderModulesByVersion",
        summary="Get provider modules (versioned)",
        response_description="Versioned provider modules.",
        response_component="ProviderModulesPayload",
        parameters=("ProviderId", "Version"),
        include_not_found=True,
    ),
    OpenApiEndpoint(
        path="/api/providers/{providerId}/{version}/parameters.json",
        tag="Provider Versions",
        operation_id="getProviderParametersByVersion",
        summary="Get provider parameters (versioned)",
        response_description="Versioned provider parameters.",
        response_component="ProviderParametersPayload",
        parameters=("ProviderId", "Version"),
        include_not_found=True,
    ),
    OpenApiEndpoint(
        path="/api/providers/{providerId}/{version}/connections.json",
        tag="Provider Versions",
        operation_id="getProviderConnectionsByVersion",
        summary="Get provider connections (versioned)",
        response_description="Versioned provider connections.",
        response_component="ProviderConnectionsPayload",
        parameters=("ProviderId", "Version"),
        include_not_found=True,
    ),
    OpenApiEndpoint(
        path="/api/providers/{providerId}/versions.json",
        tag="Provider Versions",
        operation_id="getProviderVersions",
        summary="Get provider versions",
        response_description="Published provider versions.",
        response_component="ProviderVersionsPayload",
        parameters=("ProviderId",),
        include_not_found=True,
    ),
)


def _strip_schema_meta(schema: dict[str, Any]) -> dict[str, Any]:
    sanitized = dict(schema)
    sanitized.pop("$schema", None)
    sanitized.pop("$id", None)
    return sanitized


_OPENAPI_COMPONENT_MODELS: dict[str, type[BaseModel]] = {
    "ProvidersCatalogPayload": ProvidersCatalogContract,
    "ModulesCatalogPayload": ModulesCatalogContract,
    "ProviderModulesPayload": ProviderModulesContract,
    "ProviderParametersPayload": ProviderParametersContract,
    "ProviderConnectionsPayload": ProviderConnectionsContract,
    "ProviderVersionMetadataPayload": ProviderVersionMetadataContract,
    "ProviderVersionsPayload": ProviderVersionsContract,
}


def _collect_openapi_component_schemas(
    root_models: dict[str, type[BaseModel]],
) -> dict[str, dict[str, Any]]:
    components: dict[str, dict[str, Any]] = {}
    for schema_name, model_type in root_models.items():
        schema = model_type.model_json_schema(ref_template="#/components/schemas/{model}")
        defs = schema.pop("$defs", {})
        schema = _strip_schema_meta(schema)
        if schema_name in components and components[schema_name] != schema:
            raise ValueError(f"Conflicting OpenAPI schema definition for {schema_name}")
        components[schema_name] = schema
        for def_name, def_schema in defs.items():
            cleaned = _strip_schema_meta(def_schema)
            if def_name in components and components[def_name] != cleaned:
                raise ValueError(f"Conflicting OpenAPI schema definition for {def_name}")
            components.setdefault(def_name, cleaned)
    return components


def _build_openapi_get_operation(endpoint: OpenApiEndpoint) -> dict[str, Any]:
    operation: dict[str, Any] = {
        "tags": [endpoint.tag],
        "operationId": endpoint.operation_id,
        "summary": endpoint.summary,
        "responses": {
            "200": {
                "description": endpoint.response_description,
                "content": {
                    "application/json": {
                        "schema": {"$ref": f"#/components/schemas/{endpoint.response_component}"}
                    }
                },
            }
        },
    }
    if endpoint.parameters:
        operation["parameters"] = [
            {"$ref": f"#/components/parameters/{parameter_name}"} for parameter_name in endpoint.parameters
        ]
    if endpoint.include_not_found:
        operation["responses"]["404"] = {"$ref": "#/components/responses/NotFound"}
    return operation


def _build_openapi_paths(endpoints: tuple[OpenApiEndpoint, ...]) -> dict[str, dict[str, Any]]:
    return {endpoint.path: {"get": _build_openapi_get_operation(endpoint)} for endpoint in endpoints}


def build_openapi_document() -> dict[str, Any]:
    """Build OpenAPI 3.1 schema from shared registry contracts."""

    component_schemas = _collect_openapi_component_schemas(_OPENAPI_COMPONENT_MODELS)

    return {
        "openapi": "3.1.0",
        "jsonSchemaDialect": "https://spec.openapis.org/oas/3.1/dialect/base",
        "info": {
            "title": "Airflow Registry API",
            "version": "1.0.0",
            "description": "JSON endpoints for Apache Airflow provider and module discovery.",
        },
        "tags": [
            {"name": "Catalog", "description": "Global registry datasets."},
            {"name": "Providers", "description": "Provider-scoped latest metadata."},
            {"name": "Provider Versions", "description": "Version-specific provider metadata."},
        ],
        "paths": _build_openapi_paths(OPENAPI_ENDPOINTS),
        "components": {
            "parameters": {
                "ProviderId": {
                    "name": "providerId",
                    "in": "path",
                    "required": True,
                    "schema": {"type": "string"},
                    "description": "Provider identifier (for example: amazon).",
                },
                "Version": {
                    "name": "version",
                    "in": "path",
                    "required": True,
                    "schema": {"type": "string"},
                    "description": "Provider version (for example: 9.22.0).",
                },
            },
            "responses": {
                "NotFound": {"description": "Static endpoint file not found."},
            },
            "schemas": component_schemas,
        },
    }
