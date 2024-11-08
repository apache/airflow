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
"""Abstract operator that child classes implement ``COPY INTO <TABLE> SQL in Snowflake``."""

from __future__ import annotations

from typing import Any, Sequence

from airflow.models import BaseOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.snowflake.utils.common import enclose_param


class CopyFromExternalStageToSnowflakeOperator(BaseOperator):
    """
    Executes a COPY INTO command to load files from an external stage from clouds to Snowflake.

    This operator requires the snowflake_conn_id connection. The snowflake host, login,
    and, password field must be setup in the connection. Other inputs can be defined
    in the connection or hook instantiation.

    :param namespace: snowflake namespace
    :param table: snowflake table
    :param file_format: file format name i.e. CSV, AVRO, etc
    :param stage: reference to a specific snowflake stage. If the stage's schema is not the same as the
        table one, it must be specified
    :param prefix: cloud storage location specified to limit the set of files to load
    :param files: files to load into table
    :param pattern: pattern to load files from external location to table
    :param snowflake_conn_id:  Reference to :ref:`Snowflake connection id<howto/connection:snowflake>`
    :param account: snowflake account name
    :param warehouse: name of snowflake warehouse
    :param database: name of snowflake database
    :param region: name of snowflake region
    :param role: name of snowflake role
    :param schema: name of snowflake schema
    :param authenticator: authenticator for Snowflake.
        'snowflake' (default) to use the internal Snowflake authenticator
        'externalbrowser' to authenticate using your web browser and
        Okta, ADFS or any other SAML 2.0-compliant identify provider
        (IdP) that has been defined for your account
        ``https://<your_okta_account_name>.okta.com`` to authenticate
        through native Okta.
    :param session_parameters: You can set session-level parameters at
        the time you connect to Snowflake
    :param copy_options: snowflake COPY INTO syntax copy options
    :param validation_mode: snowflake COPY INTO syntax validation mode

    """

    template_fields: Sequence[str] = ("files",)
    template_fields_renderers = {"files": "json"}

    def __init__(
        self,
        *,
        files: list | None = None,
        table: str,
        stage: str,
        prefix: str | None = None,
        file_format: str,
        schema: str | None = None,
        columns_array: list | None = None,
        pattern: str | None = None,
        warehouse: str | None = None,
        database: str | None = None,
        autocommit: bool = True,
        snowflake_conn_id: str = "snowflake_default",
        role: str | None = None,
        authenticator: str | None = None,
        session_parameters: dict | None = None,
        copy_options: str | None = None,
        validation_mode: str | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.files = files
        self.table = table
        self.stage = stage
        self.prefix = prefix
        self.file_format = file_format
        self.schema = schema
        self.columns_array = columns_array
        self.pattern = pattern
        self.warehouse = warehouse
        self.database = database
        self.autocommit = autocommit
        self.snowflake_conn_id = snowflake_conn_id
        self.role = role
        self.authenticator = authenticator
        self.session_parameters = session_parameters
        self.copy_options = copy_options
        self.validation_mode = validation_mode

        self.hook: SnowflakeHook | None = None
        self._sql: str | None = None
        self._result: list[dict[str, Any]] = []

    def execute(self, context: Any) -> None:
        self.hook = SnowflakeHook(
            snowflake_conn_id=self.snowflake_conn_id,
            warehouse=self.warehouse,
            database=self.database,
            role=self.role,
            schema=self.schema,
            authenticator=self.authenticator,
            session_parameters=self.session_parameters,
        )

        if self.schema:
            into = f"{self.schema}.{self.table}"
        else:
            into = self.table

        if self.columns_array:
            into = f"{into}({', '.join(self.columns_array)})"

        self._sql = f"""
        COPY INTO {into}
             FROM  @{self.stage}/{self.prefix or ""}
        {"FILES=(" + ",".join(map(enclose_param, self.files)) + ")" if self.files else ""}
        {"PATTERN=" + enclose_param(self.pattern) if self.pattern else ""}
        FILE_FORMAT={self.file_format}
        {self.copy_options or ""}
        {self.validation_mode or ""}
        """
        self.log.info("Executing COPY command...")
        self._result = self.hook.run(  # type: ignore # mypy does not work well with return_dictionaries=True
            sql=self._sql,
            autocommit=self.autocommit,
            handler=lambda x: x.fetchall(),
            return_dictionaries=True,
        )
        self.log.info("COPY command completed")

    @staticmethod
    def _extract_openlineage_unique_dataset_paths(
        query_result: list[dict[str, Any]],
    ) -> tuple[list[tuple[str, str]], list[str]]:
        """
        Extract and return unique OpenLineage dataset paths and file paths that failed to be parsed.

        Each row in the results is expected to have a 'file' field, which is a URI.
        The function parses these URIs and constructs a set of unique OpenLineage (namespace, name) tuples.
        Additionally, it captures any URIs that cannot be parsed or processed
        and returns them in a separate error list.

        For Azure, Snowflake has a unique way of representing URI:
            azure://<account_name>.blob.core.windows.net/<container_name>/path/to/file.csv
        that is transformed by this function to a Dataset with more universal naming convention:
            Dataset(namespace="wasbs://container_name@account_name", name="path/to"), as described at
        https://github.com/OpenLineage/OpenLineage/blob/main/spec/Naming.md#wasbs-azure-blob-storage

        :param query_result: A list of dictionaries, each containing a 'file' key with a URI value.
        :return: Two lists - the first is a sorted list of tuples, each representing a unique dataset path,
         and the second contains any URIs that cannot be parsed or processed correctly.

        >>> method = CopyFromExternalStageToSnowflakeOperator._extract_openlineage_unique_dataset_paths

        >>> results = [{"file": "azure://my_account.blob.core.windows.net/azure_container/dir3/file.csv"}]
        >>> method(results)
        ([('wasbs://azure_container@my_account', 'dir3')], [])

        >>> results = [{"file": "azure://my_account.blob.core.windows.net/azure_container"}]
        >>> method(results)
        ([('wasbs://azure_container@my_account', '/')], [])

        >>> results = [{"file": "s3://bucket"}, {"file": "gcs://bucket/"}, {"file": "s3://bucket/a.csv"}]
        >>> method(results)
        ([('gcs://bucket', '/'), ('s3://bucket', '/')], [])

        >>> results = [{"file": "s3://bucket/dir/file.csv"}, {"file": "gcs://bucket/dir/dir2/a.txt"}]
        >>> method(results)
        ([('gcs://bucket', 'dir/dir2'), ('s3://bucket', 'dir')], [])

        >>> results = [
        ...     {"file": "s3://bucket/dir/file.csv"},
        ...     {"file": "azure://my_account.something_new.windows.net/azure_container"},
        ... ]
        >>> method(results)
        ([('s3://bucket', 'dir')], ['azure://my_account.something_new.windows.net/azure_container'])
        """
        import re
        from pathlib import Path
        from urllib.parse import urlparse

        azure_regex = r"azure:\/\/(\w+)?\.blob.core.windows.net\/(\w+)\/?(.*)?"
        extraction_error_files = []
        unique_dataset_paths = set()

        for row in query_result:
            uri = urlparse(row["file"])
            if uri.scheme == "azure":
                match = re.fullmatch(azure_regex, row["file"])
                if not match:
                    extraction_error_files.append(row["file"])
                    continue
                account_name, container_name, name = match.groups()
                namespace = f"wasbs://{container_name}@{account_name}"
            else:
                namespace = f"{uri.scheme}://{uri.netloc}"
                name = uri.path.lstrip("/")

            name = Path(name).parent.as_posix()
            if name in ("", "."):
                name = "/"

            unique_dataset_paths.add((namespace, name))

        return sorted(unique_dataset_paths), sorted(extraction_error_files)

    def get_openlineage_facets_on_complete(self, task_instance):
        """Implement _on_complete because we rely on return value of a query."""
        import re

        from airflow.providers.common.compat.openlineage.facet import (
            Dataset,
            Error,
            ExternalQueryRunFacet,
            ExtractionErrorRunFacet,
            SQLJobFacet,
        )
        from airflow.providers.openlineage.extractors import OperatorLineage
        from airflow.providers.openlineage.sqlparser import SQLParser

        if not self._sql:
            return OperatorLineage()

        query_results = self._result or []
        # If no files were uploaded we get [{"status": "0 files were uploaded..."}]
        if len(query_results) == 1 and query_results[0].get("status"):
            query_results = []
        unique_dataset_paths, extraction_error_files = self._extract_openlineage_unique_dataset_paths(
            query_results
        )
        input_datasets = [Dataset(namespace=namespace, name=name) for namespace, name in unique_dataset_paths]

        run_facets = {}
        if extraction_error_files:
            self.log.debug(
                "Unable to extract Dataset namespace and name for the following files: `%s`.",
                extraction_error_files,
            )
            run_facets["extractionError"] = ExtractionErrorRunFacet(
                totalTasks=len(query_results),
                failedTasks=len(extraction_error_files),
                errors=[
                    Error(
                        errorMessage="Unable to extract Dataset namespace and name.",
                        stackTrace=None,
                        task=file_uri,
                        taskNumber=None,
                    )
                    for file_uri in extraction_error_files
                ],
            )

        connection = self.hook.get_connection(getattr(self.hook, str(self.hook.conn_name_attr)))
        database_info = self.hook.get_openlineage_database_info(connection)

        dest_name = self.table
        schema = self.hook.get_openlineage_default_schema()
        database = database_info.database
        if schema:
            dest_name = f"{schema}.{dest_name}"
            if database:
                dest_name = f"{database}.{dest_name}"

        snowflake_namespace = SQLParser.create_namespace(database_info)
        query = SQLParser.normalize_sql(self._sql)
        query = re.sub(r"\n+", "\n", re.sub(r" +", " ", query))

        run_facets["externalQuery"] = ExternalQueryRunFacet(
            externalQueryId=self.hook.query_ids[0], source=snowflake_namespace
        )

        return OperatorLineage(
            inputs=input_datasets,
            outputs=[Dataset(namespace=snowflake_namespace, name=dest_name)],
            job_facets={"sql": SQLJobFacet(query=query)},
            run_facets=run_facets,
        )
