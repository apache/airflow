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

from functools import cached_property
from typing import TYPE_CHECKING, Any, Literal
from urllib.parse import urlsplit

from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException

try:
    import duckdb

    from airflow.providers.duckdb.hooks.duckdb import DuckDBHook
except ImportError:
    raise AirflowOptionalProviderFeatureException(
        "This feature requires the 'duckdb' provider to be installed. Install it with: "
        "pip install 'apache-airflow-providers-amazon[duckdb]'"
    )

if TYPE_CHECKING:
    from duckdb import DuckDBPyConnection

CredentialStrategy = Literal["credential_chain", "config", "none"]


class AwsDuckDBHook(DuckDBHook):
    """
    Interact with DuckDB using AWS credentials brokered by Airflow.

    Extends :class:`~airflow.providers.duckdb.hooks.duckdb.DuckDBHook` with an S3 secret built from
    an Airflow AWS connection, so ``read_parquet('s3://...')`` and ``COPY ... TO 's3://...'`` work
    with no credential wiring in the Dag.

    Two credential strategies are supported:

    ``credential_chain`` (the default)
        Issues ``CREATE SECRET (TYPE s3, PROVIDER credential_chain)``, which resolves credentials
        through the AWS SDK inside DuckDB's ``aws`` extension. Nothing secret is written into SQL
        text, and because DuckDB holds the resolution itself the SDK refreshes expiring credentials,
        so a query that outlives a set of temporary credentials does not fail partway through.

    ``config``
        Resolves credentials through :class:`~airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`
        and writes them into the secret explicitly. Also works with a container-assigned role, since
        boto3 resolves that on the Airflow side, but the credentials are frozen at connect time and
        become part of the SQL statement. Needed when the connection carries static keys the AWS SDK
        running inside DuckDB cannot see, or when talking to an S3-compatible endpoint with its own
        credentials. Prefer ``credential_chain`` where it works.

    Either strategy is better than none: DuckDB's ``httpfs`` extension has its own HTTP client which
    reads credentials from the standard ``AWS_*`` environment variables but does **not** implement the
    ECS container credential provider, so an ambient task role is invisible to it and S3 access fails
    with an opaque ``HTTP 403``. A session with no secret at all also ignores the Airflow connection
    entirely, including its region and endpoint.

    This hook needs the ``httpfs`` and ``aws`` extensions and installs them if they are missing, which
    is where it diverges from :class:`~airflow.providers.duckdb.hooks.duckdb.DuckDBHook`. Using this
    hook says the task runs against AWS, so those two extensions should just work. Set
    ``autoinstall_extensions=False`` to forbid the download, in which case the extensions have to be
    present already, either in an ``extension_directory`` or baked into the image.

    :param aws_conn_id: the :ref:`AWS connection <howto/connection:aws>` used to reach S3. Set to
        ``None`` to fall back to the ambient AWS environment. Explicit only — because ``None`` is a
        meaningful value here, this one is not read from the DuckDB connection's ``extra``.
    :param region_name: region for the S3 secret. Defaults to the region from ``aws_conn_id``.
    :param credential_strategy: ``credential_chain``, ``config``, or ``none`` to create no secret at
        all (for a DuckDB database that never touches S3).
    :param credential_chain: providers DuckDB's credential chain consults, for example
        ``"env;config;sts;instance"``. Only used with the ``credential_chain`` strategy; DuckDB's own
        default order applies when it is not set.
    :param s3_endpoint_url: override the S3 endpoint, for an S3-compatible service or a VPC endpoint.
        Defaults to ``endpoint_url`` from the AWS connection extra.
    :param secret_name: name of the DuckDB secret to create.

    As with :class:`~airflow.providers.duckdb.hooks.duckdb.DuckDBHook`, every parameter except
    ``aws_conn_id`` may also be set in the DuckDB connection ``extra``, and an explicit argument wins.
    """

    #: ``httpfs`` provides the S3 filesystem; ``aws`` provides the credential chain secret provider.
    required_extensions = ("httpfs", "aws")

    def __init__(
        self,
        *args,
        aws_conn_id: str | None = "aws_default",
        region_name: str | None = None,
        credential_strategy: CredentialStrategy | None = None,
        credential_chain: str | None = None,
        s3_endpoint_url: str | None = None,
        secret_name: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.aws_conn_id = aws_conn_id
        self._region_name = region_name
        self._credential_strategy = credential_strategy
        self._credential_chain = credential_chain
        self._s3_endpoint_url = s3_endpoint_url
        self._secret_name = secret_name

    @cached_property
    def aws_hook(self) -> AwsBaseHook:
        """Return the AWS hook credentials and region are resolved through."""
        return AwsBaseHook(aws_conn_id=self.aws_conn_id, region_name=self._region_name)

    @property
    def autoinstall_extensions(self) -> bool:
        """
        Default to installing missing extensions, unlike the generic hook.

        Reaching for the AWS hook is a statement that the task runs against AWS, so ``httpfs`` and
        ``aws`` should just work. The generic hook cannot assume that and leaves downloads off.
        """
        return bool(self.resolve_parameter("autoinstall_extensions", self._autoinstall_extensions, True))

    @property
    def region_name(self) -> str | None:
        return self.resolve_parameter("region_name", self._region_name)

    @property
    def credential_strategy(self) -> CredentialStrategy:
        return self.resolve_parameter("credential_strategy", self._credential_strategy, "credential_chain")

    @property
    def credential_chain(self) -> str | None:
        return self.resolve_parameter("credential_chain", self._credential_chain)

    @property
    def secret_name(self) -> str:
        return self.resolve_parameter("secret_name", self._secret_name, "airflow_aws")

    def get_region_name(self) -> str | None:
        """Return the region for the S3 secret."""
        return self.region_name or self.aws_hook.region_name

    def get_s3_endpoint_url(self) -> str | None:
        """Return the S3 endpoint override, if any."""
        explicit = self.resolve_parameter("s3_endpoint_url", self._s3_endpoint_url)
        if explicit:
            return explicit
        return self.connection_extra.get("endpoint_url") or self.aws_hook.conn_config.endpoint_url

    def configure_secrets(self, conn: DuckDBPyConnection) -> None:
        """Create the DuckDB S3 secret for this connection."""
        strategy = self.credential_strategy
        if strategy == "none":
            self.log.debug("credential_strategy is 'none'; not creating a DuckDB S3 secret.")
            return

        self._validate_identifier(self.secret_name, "secret")
        parts = ["TYPE s3", *self._provider_clauses(strategy)]

        region = self.get_region_name()
        if region:
            parts.append(f"REGION '{self._quote(region)}'")

        endpoint_url = self.get_s3_endpoint_url()
        if endpoint_url:
            parts.extend(self._endpoint_clauses(endpoint_url))

        self.log.info("Creating DuckDB S3 secret %r using the %r strategy.", self.secret_name, strategy)
        try:
            conn.execute(f"CREATE OR REPLACE SECRET {self.secret_name} ({', '.join(parts)});")
        except duckdb.Error as error:
            if strategy != "credential_chain":
                raise
            # DuckDB 1.4 onwards resolves the credential chain when the secret is created rather than
            # when S3 is first read, so an environment with no credentials fails here. A chain that
            # finds nothing is not a misconfiguration, and a query that never touches S3 is still
            # valid, so warn rather than refusing to open the connection.
            self.log.warning(
                "Could not create DuckDB S3 secret %r from the credential chain: %s. S3 access will "
                "fail. Set credential_strategy='config' to supply credentials explicitly, or 'none' "
                "if this database does not use S3.",
                self.secret_name,
                error,
            )

    def _provider_clauses(self, strategy: CredentialStrategy) -> list[str]:
        if strategy == "credential_chain":
            clauses = ["PROVIDER credential_chain"]
            if self.credential_chain:
                clauses.append(f"CHAIN '{self._quote(self.credential_chain)}'")
            return clauses
        if strategy == "config":
            credentials = self.aws_hook.get_credentials(region_name=self.get_region_name())
            clauses = [
                "PROVIDER config",
                f"KEY_ID '{self._quote(credentials.access_key)}'",
                f"SECRET '{self._quote(credentials.secret_key)}'",
            ]
            if credentials.token:
                clauses.append(f"SESSION_TOKEN '{self._quote(credentials.token)}'")
            return clauses
        raise ValueError(
            f"Unknown credential_strategy {strategy!r}. Expected 'credential_chain', 'config' or 'none'."
        )

    def _endpoint_clauses(self, endpoint_url: str) -> list[str]:
        """
        Translate an endpoint URL into DuckDB secret clauses.

        DuckDB wants a bare ``host[:port]`` and a separate TLS flag rather than a URL, and an
        S3-compatible endpoint generally needs path-style addressing because virtual-host-style
        buckets do not resolve against it.
        """
        split = urlsplit(endpoint_url if "//" in endpoint_url else f"//{endpoint_url}")
        host = split.netloc or split.path
        clauses = [f"ENDPOINT '{self._quote(host)}'", "URL_STYLE 'path'"]
        if split.scheme == "http":
            clauses.append("USE_SSL false")
        return clauses

    @staticmethod
    def _quote(value: str) -> str:
        """Escape single quotes so a value cannot break out of its SQL literal."""
        return value.replace("'", "''")

    def get_openlineage_database_info(self, connection: Any) -> Any:
        """Return no lineage metadata; a DuckDB database is task-local and has no stable namespace."""
        return None
