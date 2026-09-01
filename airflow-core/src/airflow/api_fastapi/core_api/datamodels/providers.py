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

from pydantic import Field

from airflow.api_fastapi.core_api.base import BaseModel


class ProviderResponse(BaseModel):
    """Provider serializer for responses."""

    package_name: str
    description: str
    version: str
    documentation_url: str | None


class ProviderCollectionResponse(BaseModel):
    """Provider Collection serializer for responses."""

    providers: list[ProviderResponse]
    total_entries: int


# The models below mirror the provider metadata structure defined in
# airflow-core/src/airflow/provider_info.schema.json (deprecated keys excluded).


class ProviderIntegrationInfo(BaseModel):
    """Integration provided by a provider."""

    integration_name: str = Field(alias="integration-name")
    external_doc_url: str | None = Field(None, alias="external-doc-url")
    how_to_guide: list[str] | None = Field(None, alias="how-to-guide")
    logo: str | None = None
    tags: list[str] | None = None


class ProviderIntegrationModulesInfo(BaseModel):
    """Python modules an integration contributes for one kind of functionality."""

    integration_name: str = Field(alias="integration-name")
    python_modules: list[str] = Field(alias="python-modules")


class ProviderAssetUriInfo(BaseModel):
    """Asset URI scheme handling provided by a provider."""

    schemes: list[str]
    handler: str | None = None
    factory: str | None = None
    to_openlineage_converter: str | None = None


class ProviderDialectInfo(BaseModel):
    """SQL dialect provided by a provider."""

    dialect_type: str | None = Field(None, alias="dialect-type")
    dialect_class_name: str | None = Field(None, alias="dialect-class-name")


class ProviderTransferInfo(BaseModel):
    """Transfer operator provided by a provider."""

    source_integration_name: str = Field(alias="source-integration-name")
    target_integration_name: str = Field(alias="target-integration-name")
    python_module: str = Field(alias="python-module")
    how_to_guide: str | None = Field(None, alias="how-to-guide")


class ProviderConnectionTypeInfo(BaseModel):
    """Connection type provided by a provider."""

    connection_type: str = Field(alias="connection-type")
    hook_class_name: str = Field(alias="hook-class-name")
    hook_name: str | None = Field(None, alias="hook-name")


class ProviderRemoteLoggingInfo(BaseModel):
    """Remote logging IO handler provided by a provider."""

    classpath: str
    scheme: str


class ProviderConfigOptionInfo(BaseModel):
    """Configuration option contributed by a provider."""

    description: str | None = None
    version_added: str | None = None
    type: str | None = None
    example: str | int | float | None = None
    default: str | int | float | None = None
    sensitive: bool | None = None


class ProviderConfigSectionInfo(BaseModel):
    """Configuration section contributed by a provider."""

    description: str | None = None
    options: dict[str, ProviderConfigOptionInfo] = Field(default_factory=dict)


class ProviderTaskDecoratorInfo(BaseModel):
    """TaskFlow decorator provided by a provider."""

    name: str | None = None
    class_name: str | None = Field(None, alias="class-name")


class ProviderPluginInfo(BaseModel):
    """Plugin provided by a provider."""

    name: str | None = None
    plugin_class: str | None = Field(None, alias="plugin-class")


class ProviderInfoResponse(BaseModel):
    """Typed provider metadata (from ``provider.yaml``) exposed by the API."""

    name: str | None = None
    filesystems: list[str] | None = None
    integrations: list[ProviderIntegrationInfo] | None = None
    operators: list[ProviderIntegrationModulesInfo] | None = None
    sensors: list[ProviderIntegrationModulesInfo] | None = None
    hooks: list[ProviderIntegrationModulesInfo] | None = None
    triggers: list[ProviderIntegrationModulesInfo] | None = None
    bundles: list[ProviderIntegrationModulesInfo] | None = None
    asset_uris: list[ProviderAssetUriInfo] | None = Field(None, alias="asset-uris")
    dialects: list[ProviderDialectInfo] | None = None
    transfers: list[ProviderTransferInfo] | None = None
    connection_types: list[ProviderConnectionTypeInfo] | None = Field(None, alias="connection-types")
    extra_links: list[str] | None = Field(None, alias="extra-links")
    secrets_backends: list[str] | None = Field(None, alias="secrets-backends")
    logging: list[str] | None = None
    remote_logging: list[ProviderRemoteLoggingInfo] | None = Field(None, alias="remote-logging")
    auth_backends: list[str] | None = Field(None, alias="auth-backends")
    auth_managers: list[str] | None = Field(None, alias="auth-managers")
    notifications: list[str] | None = None
    executors: list[str] | None = None
    db_managers: list[str] | None = Field(None, alias="db-managers")
    cli: list[str] | None = None
    config: dict[str, ProviderConfigSectionInfo] | None = None
    task_decorators: list[ProviderTaskDecoratorInfo] | None = Field(None, alias="task-decorators")
    plugins: list[ProviderPluginInfo] | None = None
    queues: list[str] | None = None


class ProviderDetailsResponse(ProviderResponse):
    """Detailed provider serializer for responses."""

    provider_info: ProviderInfoResponse
