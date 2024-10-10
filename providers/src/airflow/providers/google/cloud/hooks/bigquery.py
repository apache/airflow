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

"""BigQuery Hook and a very basic PEP 249 implementation for BigQuery."""

from __future__ import annotations

import asyncio
import json
import logging
import re
import time
import uuid
from copy import deepcopy
from datetime import datetime, timedelta
from functools import cached_property
from typing import TYPE_CHECKING, Any, Iterable, Mapping, NoReturn, Sequence, Union, cast

from aiohttp import ClientSession as ClientSession
from gcloud.aio.bigquery import Job, Table as Table_async
from google.cloud.bigquery import (
    DEFAULT_RETRY,
    Client,
    CopyJob,
    ExternalConfig,
    ExtractJob,
    LoadJob,
    QueryJob,
    SchemaField,
    UnknownJob,
)
from google.cloud.bigquery.dataset import AccessEntry, Dataset, DatasetListItem, DatasetReference
from google.cloud.bigquery.retry import DEFAULT_JOB_RETRY
from google.cloud.bigquery.table import (
    EncryptionConfiguration,
    Row,
    RowIterator,
    Table,
    TableReference,
)
from google.cloud.exceptions import NotFound
from googleapiclient.discovery import Resource, build
from pandas_gbq import read_gbq
from pandas_gbq.gbq import GbqConnector  # noqa: F401 Used in ``airflow.contrib.hooks.bigquery``
from requests import Session
from sqlalchemy import create_engine

from airflow.exceptions import AirflowException, AirflowProviderDeprecationWarning
from airflow.providers.common.sql.hooks.sql import DbApiHook
from airflow.providers.google.cloud.utils.bigquery import bq_cast
from airflow.providers.google.cloud.utils.credentials_provider import _get_scopes
from airflow.providers.google.common.consts import CLIENT_INFO
from airflow.providers.google.common.deprecated import deprecated
from airflow.providers.google.common.hooks.base_google import (
    PROVIDE_PROJECT_ID,
    GoogleBaseAsyncHook,
    GoogleBaseHook,
    get_field,
)
from airflow.utils.hashlib_wrapper import md5
from airflow.utils.helpers import convert_camel_to_snake
from airflow.utils.log.logging_mixin import LoggingMixin

if TYPE_CHECKING:
    import pandas as pd
    from google.api_core.page_iterator import HTTPIterator
    from google.api_core.retry import Retry

log = logging.getLogger(__name__)

BigQueryJob = Union[CopyJob, QueryJob, LoadJob, ExtractJob]


class BigQueryHook(GoogleBaseHook, DbApiHook):
    """
    Interact with BigQuery.

    This hook uses the Google Cloud connection.

    :param gcp_conn_id: The Airflow connection used for GCP credentials.
    :param use_legacy_sql: This specifies whether to use legacy SQL dialect.
    :param location: The location of the BigQuery resource.
    :param priority: Specifies a priority for the query.
        Possible values include INTERACTIVE and BATCH.
        The default value is INTERACTIVE.
    :param api_resource_configs: This contains params configuration applied for
        Google BigQuery jobs.
    :param impersonation_chain: This is the optional service account to
        impersonate using short term credentials.
    :param impersonation_scopes: Optional list of scopes for impersonated account.
        Will override scopes from connection.
    :param labels: The BigQuery resource label.
    """

    conn_name_attr = "gcp_conn_id"
    default_conn_name = "google_cloud_bigquery_default"
    conn_type = "gcpbigquery"
    hook_name = "Google Bigquery"

    @classmethod
    def get_connection_form_widgets(cls) -> dict[str, Any]:
        """Return connection widgets to add to connection form."""
        from flask_appbuilder.fieldwidgets import BS3TextFieldWidget
        from flask_babel import lazy_gettext
        from wtforms import validators
        from wtforms.fields.simple import BooleanField, StringField

        from airflow.www.validators import ValidJson

        connection_form_widgets = super().get_connection_form_widgets()
        connection_form_widgets["use_legacy_sql"] = BooleanField(lazy_gettext("Use Legacy SQL"), default=True)
        connection_form_widgets["location"] = StringField(
            lazy_gettext("Location"), widget=BS3TextFieldWidget()
        )
        connection_form_widgets["priority"] = StringField(
            lazy_gettext("Priority"),
            default="INTERACTIVE",
            widget=BS3TextFieldWidget(),
            validators=[validators.AnyOf(["INTERACTIVE", "BATCH"])],
        )
        connection_form_widgets["api_resource_configs"] = StringField(
            lazy_gettext("API Resource Configs"), widget=BS3TextFieldWidget(), validators=[ValidJson()]
        )
        connection_form_widgets["labels"] = StringField(
            lazy_gettext("Labels"), widget=BS3TextFieldWidget(), validators=[ValidJson()]
        )
        connection_form_widgets["labels"] = StringField(
            lazy_gettext("Labels"), widget=BS3TextFieldWidget(), validators=[ValidJson()]
        )
        return connection_form_widgets

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        """Return custom field behaviour."""
        return super().get_ui_field_behaviour()

    def __init__(
        self,
        use_legacy_sql: bool = True,
        location: str | None = None,
        priority: str = "INTERACTIVE",
        api_resource_configs: dict | None = None,
        impersonation_scopes: str | Sequence[str] | None = None,
        labels: dict | None = None,
        **kwargs,
    ) -> None:
        if kwargs.get("delegate_to") is not None:
            raise RuntimeError(
                "The `delegate_to` parameter has been deprecated before and finally removed in this version"
                " of Google Provider. You MUST convert it to `impersonate_chain`"
            )
        super().__init__(**kwargs)
        self.use_legacy_sql: bool = self._get_field("use_legacy_sql", use_legacy_sql)
        self.location: str | None = self._get_field("location", location)
        self.priority: str = self._get_field("priority", priority)
        self.running_job_id: str | None = None
        self.api_resource_configs: dict = self._get_field("api_resource_configs", api_resource_configs or {})
        self.labels = self._get_field("labels", labels or {})
        self.impersonation_scopes: str | Sequence[str] | None = impersonation_scopes

    @cached_property
    @deprecated(
        planned_removal_date="November 01, 2024",
        reason="This property is no longer in actual use. ",
        category=AirflowProviderDeprecationWarning,
    )
    def credentials_path(self) -> str:
        return "bigquery_hook_credentials.json"

    def get_conn(self) -> BigQueryConnection:
        """Get a BigQuery PEP 249 connection object."""
        http_authorized = self._authorize()
        service = build("bigquery", "v2", http=http_authorized, cache_discovery=False)
        return BigQueryConnection(
            service=service,
            project_id=self.project_id,
            use_legacy_sql=self.use_legacy_sql,
            location=self.location,
            num_retries=self.num_retries,
            hook=self,
        )

    @deprecated(
        planned_removal_date="November 01, 2024",
        use_instead="airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_client.",
        category=AirflowProviderDeprecationWarning,
    )
    def get_service(self) -> Resource:
        """Get a BigQuery service object. Deprecated."""
        http_authorized = self._authorize()
        return build("bigquery", "v2", http=http_authorized, cache_discovery=False)

    def get_client(self, project_id: str = PROVIDE_PROJECT_ID, location: str | None = None) -> Client:
        """
        Get an authenticated BigQuery Client.

        :param project_id: Project ID for the project which the client acts on behalf of.
        :param location: Default location for jobs / datasets / tables.
        """
        return Client(
            client_info=CLIENT_INFO,
            project=project_id,
            location=location,
            credentials=self.get_credentials(),
        )

    def get_uri(self) -> str:
        """Override from ``DbApiHook`` for ``get_sqlalchemy_engine()``."""
        return f"bigquery://{self.project_id}"

    def get_sqlalchemy_engine(self, engine_kwargs: dict | None = None):
        """
        Create an SQLAlchemy engine object.

        :param engine_kwargs: Kwargs used in :func:`~sqlalchemy.create_engine`.
        """
        if engine_kwargs is None:
            engine_kwargs = {}
        credentials_path = get_field(self.extras, "key_path")
        if credentials_path:
            return create_engine(self.get_uri(), credentials_path=credentials_path, **engine_kwargs)
        keyfile_dict = get_field(self.extras, "keyfile_dict")
        if keyfile_dict:
            keyfile_content = keyfile_dict if isinstance(keyfile_dict, dict) else json.loads(keyfile_dict)
            return create_engine(self.get_uri(), credentials_info=keyfile_content, **engine_kwargs)
        try:
            # 1. If the environment variable GOOGLE_APPLICATION_CREDENTIALS is set
            # ADC uses the service account key or configuration file that the variable points to.
            # 2. If the environment variable GOOGLE_APPLICATION_CREDENTIALS isn't set
            # ADC uses the service account that is attached to the resource that is running your code.
            return create_engine(self.get_uri(), **engine_kwargs)
        except Exception as e:
            self.log.error(e)
            raise AirflowException(
                "For now, we only support instantiating SQLAlchemy engine by"
                " using ADC or extra fields `key_path` and `keyfile_dict`."
            )

    def get_records(self, sql, parameters=None):
        if self.location is None:
            raise AirflowException("Need to specify 'location' to use BigQueryHook.get_records()")
        return super().get_records(sql, parameters=parameters)

    @staticmethod
    def _resolve_table_reference(
        table_resource: dict[str, Any],
        project_id: str = PROVIDE_PROJECT_ID,
        dataset_id: str | None = None,
        table_id: str | None = None,
    ) -> dict[str, Any]:
        try:
            # Check if tableReference is present and is valid
            TableReference.from_api_repr(table_resource["tableReference"])
        except KeyError:
            # Something is wrong so we try to build the reference
            table_resource["tableReference"] = table_resource.get("tableReference", {})
            values = [("projectId", project_id), ("tableId", table_id), ("datasetId", dataset_id)]
            for key, value in values:
                # Check if value is already present if no use the provided one
                resolved_value = table_resource["tableReference"].get(key, value)
                if not resolved_value:
                    # If there's no value in tableReference and provided one is None raise error
                    raise AirflowException(
                        f"Table resource is missing proper `tableReference` and `{key}` is None"
                    )
                table_resource["tableReference"][key] = resolved_value
        return table_resource

    def insert_rows(
        self,
        table: Any,
        rows: Any,
        target_fields: Any = None,
        commit_every: Any = 1000,
        replace: Any = False,
        **kwargs,
    ) -> None:
        """
        Insert rows.

        Insertion is currently unsupported. Theoretically, you could use
        BigQuery's streaming API to insert rows into a table, but this hasn't
        been implemented.
        """
        raise NotImplementedError()

    def get_pandas_df(
        self,
        sql: str,
        parameters: Iterable | Mapping[str, Any] | None = None,
        dialect: str | None = None,
        **kwargs,
    ) -> pd.DataFrame:
        """
        Get a Pandas DataFrame for the BigQuery results.

        The DbApiHook method must be overridden because Pandas doesn't support
        PEP 249 connections, except for SQLite.

        .. seealso::
            https://github.com/pandas-dev/pandas/blob/055d008615272a1ceca9720dc365a2abd316f353/pandas/io/sql.py#L415
            https://github.com/pandas-dev/pandas/issues/6900

        :param sql: The BigQuery SQL to execute.
        :param parameters: The parameters to render the SQL query with (not
            used, leave to override superclass method)
        :param dialect: Dialect of BigQuery SQL – legacy SQL or standard SQL
            defaults to use `self.use_legacy_sql` if not specified
        :param kwargs: (optional) passed into pandas_gbq.read_gbq method
        """
        if dialect is None:
            dialect = "legacy" if self.use_legacy_sql else "standard"

        credentials, project_id = self.get_credentials_and_project_id()

        return read_gbq(sql, project_id=project_id, dialect=dialect, credentials=credentials, **kwargs)

    @GoogleBaseHook.fallback_to_default_project_id
    def table_exists(self, dataset_id: str, table_id: str, project_id: str) -> bool:
        """
        Check if a table exists in Google BigQuery.

        :param project_id: The Google cloud project in which to look for the
            table. The connection supplied to the hook must provide access to
            the specified project.
        :param dataset_id: The name of the dataset in which to look for the
            table.
        :param table_id: The name of the table to check the existence of.
        """
        table_reference = TableReference(DatasetReference(project_id, dataset_id), table_id)
        try:
            self.get_client(project_id=project_id).get_table(table_reference)
            return True
        except NotFound:
            return False

    @GoogleBaseHook.fallback_to_default_project_id
    def table_partition_exists(
        self, dataset_id: str, table_id: str, partition_id: str, project_id: str
    ) -> bool:
        """
        Check if a partition exists in Google BigQuery.

        :param project_id: The Google cloud project in which to look for the
            table. The connection supplied to the hook must provide access to
            the specified project.
        :param dataset_id: The name of the dataset in which to look for the
            table.
        :param table_id: The name of the table to check the existence of.
        :param partition_id: The name of the partition to check the existence of.
        """
        table_reference = TableReference(DatasetReference(project_id, dataset_id), table_id)
        try:
            return partition_id in self.get_client(project_id=project_id).list_partitions(table_reference)
        except NotFound:
            return False

    @GoogleBaseHook.fallback_to_default_project_id
    def create_empty_table(
        self,
        project_id: str = PROVIDE_PROJECT_ID,
        dataset_id: str | None = None,
        table_id: str | None = None,
        table_resource: dict[str, Any] | None = None,
        schema_fields: list | None = None,
        time_partitioning: dict | None = None,
        cluster_fields: list[str] | None = None,
        labels: dict | None = None,
        view: dict | None = None,
        materialized_view: dict | None = None,
        encryption_configuration: dict | None = None,
        retry: Retry = DEFAULT_RETRY,
        location: str | None = None,
        exists_ok: bool = True,
    ) -> Table:
        """
        Create a new, empty table in the dataset.

        To create a view, which is defined by a SQL query, parse a dictionary to
        the *view* argument.

        :param project_id: The project to create the table into.
        :param dataset_id: The dataset to create the table into.
        :param table_id: The Name of the table to be created.
        :param table_resource: Table resource as described in documentation:
            https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#Table
            If provided all other parameters are ignored.
        :param schema_fields: If set, the schema field list as defined here:
            https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs#configuration.load.schema

            .. code-block:: python

                schema_fields = [
                    {"name": "emp_name", "type": "STRING", "mode": "REQUIRED"},
                    {"name": "salary", "type": "INTEGER", "mode": "NULLABLE"},
                ]

        :param labels: a dictionary containing labels for the table, passed to BigQuery
        :param retry: Optional. How to retry the RPC.
        :param time_partitioning: configure optional time partitioning fields i.e.
            partition by field, type and expiration as per API specifications.

            .. seealso::
                https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#timePartitioning
        :param cluster_fields: [Optional] The fields used for clustering.
            BigQuery supports clustering for both partitioned and
            non-partitioned tables.
            https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#clustering.fields
        :param view: [Optional] A dictionary containing definition for the view.
            If set, it will create a view instead of a table:
            https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#ViewDefinition

            .. code-block:: python

                view = {
                    "query": "SELECT * FROM `test-project-id.test_dataset_id.test_table_prefix*` LIMIT 1000",
                    "useLegacySql": False,
                }

        :param materialized_view: [Optional] The materialized view definition.
        :param encryption_configuration: [Optional] Custom encryption configuration (e.g., Cloud KMS keys).

            .. code-block:: python

                encryption_configuration = {
                    "kmsKeyName": "projects/testp/locations/us/keyRings/test-kr/cryptoKeys/test-key",
                }

        :param num_retries: Maximum number of retries in case of connection problems.
        :param location: (Optional) The geographic location where the table should reside.
        :param exists_ok: If ``True``, ignore "already exists" errors when creating the table.
        :return: Created table
        """
        _table_resource: dict[str, Any] = {}

        if self.location:
            _table_resource["location"] = self.location

        if schema_fields:
            _table_resource["schema"] = {"fields": schema_fields}

        if time_partitioning:
            _table_resource["timePartitioning"] = time_partitioning

        if cluster_fields:
            _table_resource["clustering"] = {"fields": cluster_fields}

        if labels:
            _table_resource["labels"] = labels

        if view:
            _table_resource["view"] = view

        if materialized_view:
            _table_resource["materializedView"] = materialized_view

        if encryption_configuration:
            _table_resource["encryptionConfiguration"] = encryption_configuration

        table_resource = table_resource or _table_resource
        table_resource = self._resolve_table_reference(
            table_resource=table_resource,
            project_id=project_id,
            dataset_id=dataset_id,
            table_id=table_id,
        )
        table = Table.from_api_repr(table_resource)
        return self.get_client(project_id=project_id, location=location).create_table(
            table=table, exists_ok=exists_ok, retry=retry
        )

    @GoogleBaseHook.fallback_to_default_project_id
    def create_empty_dataset(
        self,
        dataset_id: str | None = None,
        project_id: str = PROVIDE_PROJECT_ID,
        location: str | None = None,
        dataset_reference: dict[str, Any] | None = None,
        exists_ok: bool = True,
    ) -> dict[str, Any]:
        """
        Create a new empty dataset.

        .. seealso:: https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets/insert

        :param project_id: The name of the project where we want to create
            an empty a dataset. Don't need to provide, if projectId in dataset_reference.
        :param dataset_id: The id of dataset. Don't need to provide, if datasetId in dataset_reference.
        :param location: (Optional) The geographic location where the dataset should reside.
            There is no default value but the dataset will be created in US if nothing is provided.
        :param dataset_reference: Dataset reference that could be provided with request body. More info:
            https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets#resource
        :param exists_ok: If ``True``, ignore "already exists" errors when creating the dataset.
        """
        dataset_reference = dataset_reference or {}

        if "datasetReference" not in dataset_reference:
            dataset_reference["datasetReference"] = {}

        for param, value in zip(["datasetId", "projectId"], [dataset_id, project_id]):
            specified_param = dataset_reference["datasetReference"].get(param)
            if specified_param:
                if value:
                    self.log.info(
                        "`%s` was provided in both `dataset_reference` and as `%s`. "
                        "Using value from `dataset_reference`",
                        param,
                        convert_camel_to_snake(param),
                    )
                continue  # use specified value
            if not value:
                raise ValueError(
                    f"Please specify `{param}` either in `dataset_reference` "
                    f"or by providing `{convert_camel_to_snake(param)}`",
                )
            # dataset_reference has no param but we can fallback to default value
            self.log.info(
                "%s was not specified in `dataset_reference`. Will use default value %s.", param, value
            )
            dataset_reference["datasetReference"][param] = value

        location = location or self.location
        project_id = project_id or self.project_id
        if location:
            dataset_reference["location"] = dataset_reference.get("location", location)

        dataset: Dataset = Dataset.from_api_repr(dataset_reference)
        self.log.info("Creating dataset: %s in project: %s ", dataset.dataset_id, dataset.project)
        dataset_object = self.get_client(project_id=project_id, location=location).create_dataset(
            dataset=dataset, exists_ok=exists_ok
        )
        self.log.info("Dataset created successfully.")
        return dataset_object.to_api_repr()

    @GoogleBaseHook.fallback_to_default_project_id
    def get_dataset_tables(
        self,
        dataset_id: str,
        project_id: str = PROVIDE_PROJECT_ID,
        max_results: int | None = None,
        retry: Retry = DEFAULT_RETRY,
    ) -> list[dict[str, Any]]:
        """
        Get the list of tables for a given dataset.

        For more information, see:
        https://cloud.google.com/bigquery/docs/reference/rest/v2/tables/list

        :param dataset_id: the dataset ID of the requested dataset.
        :param project_id: (Optional) the project of the requested dataset. If None,
            self.project_id will be used.
        :param max_results: (Optional) the maximum number of tables to return.
        :param retry: How to retry the RPC.
        :return: List of tables associated with the dataset.
        """
        self.log.info("Start getting tables list from dataset: %s.%s", project_id, dataset_id)
        tables = self.get_client().list_tables(
            dataset=DatasetReference(project=project_id, dataset_id=dataset_id),
            max_results=max_results,
            retry=retry,
        )
        # Convert to a list (consumes all values)
        return [t.reference.to_api_repr() for t in tables]

    @GoogleBaseHook.fallback_to_default_project_id
    def delete_dataset(
        self,
        dataset_id: str,
        project_id: str = PROVIDE_PROJECT_ID,
        delete_contents: bool = False,
        retry: Retry = DEFAULT_RETRY,
    ) -> None:
        """
        Delete a dataset of Big query in your project.

        :param project_id: The name of the project where we have the dataset.
        :param dataset_id: The dataset to be delete.
        :param delete_contents: If True, delete all the tables in the dataset.
            If False and the dataset contains tables, the request will fail.
        :param retry: How to retry the RPC.
        """
        self.log.info("Deleting from project: %s  Dataset:%s", project_id, dataset_id)
        self.get_client(project_id=project_id).delete_dataset(
            dataset=DatasetReference(project=project_id, dataset_id=dataset_id),
            delete_contents=delete_contents,
            retry=retry,
            not_found_ok=True,
        )

    @deprecated(
        planned_removal_date="November 01, 2024",
        use_instead="airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.create_empty_table",
        instructions="Use the replacement method with passing the `table_resource` object. "
        "This gives more flexibility.",
        category=AirflowProviderDeprecationWarning,
    )
    @GoogleBaseHook.fallback_to_default_project_id
    def create_external_table(
        self,
        external_project_dataset_table: str,
        schema_fields: list,
        source_uris: list,
        source_format: str = "CSV",
        autodetect: bool = False,
        compression: str = "NONE",
        ignore_unknown_values: bool = False,
        max_bad_records: int = 0,
        skip_leading_rows: int = 0,
        field_delimiter: str = ",",
        quote_character: str | None = None,
        allow_quoted_newlines: bool = False,
        allow_jagged_rows: bool = False,
        encoding: str = "UTF-8",
        src_fmt_configs: dict | None = None,
        labels: dict | None = None,
        description: str | None = None,
        encryption_configuration: dict | None = None,
        location: str | None = None,
        project_id: str = PROVIDE_PROJECT_ID,
    ) -> Table:
        """
        Create an external table in the dataset with data from Google Cloud Storage.

        .. seealso:: https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#resource

        This method is deprecated. Please use :func:`.create_empty_table` with
        the ``table_resource`` object. See function documentation for more
        details about these parameters.

        :param external_project_dataset_table:
            The dotted ``(<project>.|<project>:)<dataset>.<table>($<partition>)`` BigQuery
            table name to create external table.
            If ``<project>`` is not included, project will be the
            project defined in the connection json.
        :param schema_fields: The schema field list as defined here:
            https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#resource
        :param source_uris: The source Google Cloud
            Storage URI (e.g. gs://some-bucket/some-file.txt). A single wild
            per-object name can be used.
        :param source_format: File format to export.
        :param autodetect: Try to detect schema and format options automatically.
            Any option specified explicitly will be honored.
        :param compression: [Optional] The compression type of the data source.
            Possible values include GZIP and NONE.
            The default value is NONE.
            This setting is ignored for Google Cloud Bigtable,
            Google Cloud Datastore backups and Avro formats.
        :param ignore_unknown_values: [Optional] Indicates if BigQuery should allow
            extra values that are not represented in the table schema.
            If true, the extra values are ignored. If false, records with extra columns
            are treated as bad records, and if there are too many bad records, an
            invalid error is returned in the job result.
        :param max_bad_records: The maximum number of bad records that BigQuery can
            ignore when running the job.
        :param skip_leading_rows: Number of rows to skip when loading from a CSV.
        :param field_delimiter: The delimiter to use when loading from a CSV.
        :param quote_character: The value that is used to quote data sections in a CSV
            file.
        :param allow_quoted_newlines: Whether to allow quoted newlines (true) or not
            (false).
        :param allow_jagged_rows: Accept rows that are missing trailing optional columns.
            The missing values are treated as nulls. If false, records with missing
            trailing columns are treated as bad records, and if there are too many bad
            records, an invalid error is returned in the job result. Only applicable when
            source_format is CSV.
        :param encoding: The character encoding of the data. See:

            .. seealso::
                https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#externalDataConfiguration.csvOptions.encoding
        :param src_fmt_configs: configure optional fields specific to the source format
        :param labels: A dictionary containing labels for the BiqQuery table.
        :param description: A string containing the description for the BigQuery table.
        :param encryption_configuration: [Optional] Custom encryption configuration (e.g., Cloud KMS keys).

            .. code-block:: python

                encryption_configuration = {
                    "kmsKeyName": "projects/testp/locations/us/keyRings/test-kr/cryptoKeys/test-key",
                }
        """
        location = location or self.location
        src_fmt_configs = src_fmt_configs or {}
        source_format = source_format.upper()
        compression = compression.upper()

        external_config_api_repr = {
            "autodetect": autodetect,
            "sourceFormat": source_format,
            "sourceUris": source_uris,
            "compression": compression,
            "ignoreUnknownValues": ignore_unknown_values,
        }

        # if following fields are not specified in src_fmt_configs,
        # honor the top-level params for backward-compatibility
        backward_compatibility_configs = {
            "skipLeadingRows": skip_leading_rows,
            "fieldDelimiter": field_delimiter,
            "quote": quote_character,
            "allowQuotedNewlines": allow_quoted_newlines,
            "allowJaggedRows": allow_jagged_rows,
            "encoding": encoding,
        }
        src_fmt_to_param_mapping = {"CSV": "csvOptions", "GOOGLE_SHEETS": "googleSheetsOptions"}
        src_fmt_to_configs_mapping = {
            "csvOptions": [
                "allowJaggedRows",
                "allowQuotedNewlines",
                "fieldDelimiter",
                "skipLeadingRows",
                "quote",
                "encoding",
            ],
            "googleSheetsOptions": ["skipLeadingRows"],
        }
        if source_format in src_fmt_to_param_mapping:
            valid_configs = src_fmt_to_configs_mapping[src_fmt_to_param_mapping[source_format]]
            src_fmt_configs = _validate_src_fmt_configs(
                source_format, src_fmt_configs, valid_configs, backward_compatibility_configs
            )
            external_config_api_repr[src_fmt_to_param_mapping[source_format]] = src_fmt_configs

        # build external config
        external_config = ExternalConfig.from_api_repr(external_config_api_repr)
        if schema_fields:
            external_config.schema = [SchemaField.from_api_repr(f) for f in schema_fields]
        if max_bad_records:
            external_config.max_bad_records = max_bad_records

        # build table definition
        table = Table(table_ref=TableReference.from_string(external_project_dataset_table, project_id))
        table.external_data_configuration = external_config
        if labels:
            table.labels = labels

        if description:
            table.description = description

        if encryption_configuration:
            table.encryption_configuration = EncryptionConfiguration.from_api_repr(encryption_configuration)

        self.log.info("Creating external table: %s", external_project_dataset_table)
        table_object = self.create_empty_table(
            table_resource=table.to_api_repr(), project_id=project_id, location=location, exists_ok=True
        )
        self.log.info("External table created successfully: %s", external_project_dataset_table)
        return table_object

    @GoogleBaseHook.fallback_to_default_project_id
    def update_table(
        self,
        table_resource: dict[str, Any],
        fields: list[str] | None = None,
        dataset_id: str | None = None,
        table_id: str | None = None,
        project_id: str = PROVIDE_PROJECT_ID,
    ) -> dict[str, Any]:
        """
        Change some fields of a table.

        Use ``fields`` to specify which fields to update. At least one field
        must be provided. If a field is listed in ``fields`` and is ``None``
        in ``table``, the field value will be deleted.

        If ``table.etag`` is not ``None``, the update will only succeed if
        the table on the server has the same ETag. Thus reading a table with
        ``get_table``, changing its fields, and then passing it to
        ``update_table`` will ensure that the changes will only be saved if
        no modifications to the table occurred since the read.

        :param project_id: The project to create the table into.
        :param dataset_id: The dataset to create the table into.
        :param table_id: The Name of the table to be created.
        :param table_resource: Table resource as described in documentation:
            https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#Table
            The table has to contain ``tableReference`` or ``project_id``, ``dataset_id`` and ``table_id``
            have to be provided.
        :param fields: The fields of ``table`` to change, spelled as the Table
            properties (e.g. "friendly_name").
        """
        fields = fields or list(table_resource.keys())
        table_resource = self._resolve_table_reference(
            table_resource=table_resource, project_id=project_id, dataset_id=dataset_id, table_id=table_id
        )

        table = Table.from_api_repr(table_resource)
        self.log.info("Updating table: %s", table_resource["tableReference"])
        table_object = self.get_client(project_id=project_id).update_table(table=table, fields=fields)
        self.log.info("Table %s.%s.%s updated successfully", project_id, dataset_id, table_id)
        return table_object.to_api_repr()

    @deprecated(
        planned_removal_date="November 01, 2024",
        use_instead="airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.update_table",
        category=AirflowProviderDeprecationWarning,
    )
    @GoogleBaseHook.fallback_to_default_project_id
    def patch_table(
        self,
        dataset_id: str,
        table_id: str,
        project_id: str = PROVIDE_PROJECT_ID,
        description: str | None = None,
        expiration_time: int | None = None,
        external_data_configuration: dict | None = None,
        friendly_name: str | None = None,
        labels: dict | None = None,
        schema: list | None = None,
        time_partitioning: dict | None = None,
        view: dict | None = None,
        require_partition_filter: bool | None = None,
        encryption_configuration: dict | None = None,
    ) -> None:
        """
        Patch information in an existing table.

        It only updates fields that are provided in the request object. This
        method is deprecated. Please use :func:`.update_table` instead.

        Reference: https://cloud.google.com/bigquery/docs/reference/rest/v2/tables/patch

        :param dataset_id: The dataset containing the table to be patched.
        :param table_id: The Name of the table to be patched.
        :param project_id: The project containing the table to be patched.
        :param description: [Optional] A user-friendly description of this table.
        :param expiration_time: [Optional] The time when this table expires,
            in milliseconds since the epoch.
        :param external_data_configuration: [Optional] A dictionary containing
            properties of a table stored outside of BigQuery.
        :param friendly_name: [Optional] A descriptive name for this table.
        :param labels: [Optional] A dictionary containing labels associated with this table.
        :param schema: [Optional] If set, the schema field list as defined here:
            https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs#configuration.load.schema
            The supported schema modifications and unsupported schema modification are listed here:
            https://cloud.google.com/bigquery/docs/managing-table-schemas

            .. code-block:: python

                schema = [
                    {"name": "emp_name", "type": "STRING", "mode": "REQUIRED"},
                    {"name": "salary", "type": "INTEGER", "mode": "NULLABLE"},
                ]

        :param time_partitioning: [Optional] A dictionary containing time-based partitioning
             definition for the table.
        :param view: [Optional] A dictionary containing definition for the view.
            If set, it will patch a view instead of a table:
            https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#ViewDefinition

            .. code-block:: python

                view = {
                    "query": "SELECT * FROM `test-project-id.test_dataset_id.test_table_prefix*` LIMIT 500",
                    "useLegacySql": False,
                }

        :param require_partition_filter: [Optional] If true, queries over the this table require a
            partition filter. If false, queries over the table
        :param encryption_configuration: [Optional] Custom encryption configuration (e.g., Cloud KMS keys).

            .. code-block:: python

                encryption_configuration = {
                    "kmsKeyName": "projects/testp/locations/us/keyRings/test-kr/cryptoKeys/test-key",
                }

        """
        table_resource: dict[str, Any] = {}

        if description is not None:
            table_resource["description"] = description
        if expiration_time is not None:
            table_resource["expirationTime"] = expiration_time
        if external_data_configuration:
            table_resource["externalDataConfiguration"] = external_data_configuration
        if friendly_name is not None:
            table_resource["friendlyName"] = friendly_name
        if labels:
            table_resource["labels"] = labels
        if schema:
            table_resource["schema"] = {"fields": schema}
        if time_partitioning:
            table_resource["timePartitioning"] = time_partitioning
        if view:
            table_resource["view"] = view
        if require_partition_filter is not None:
            table_resource["requirePartitionFilter"] = require_partition_filter
        if encryption_configuration:
            table_resource["encryptionConfiguration"] = encryption_configuration

        self.update_table(
            table_resource=table_resource,
            fields=list(table_resource.keys()),
            project_id=project_id,
            dataset_id=dataset_id,
            table_id=table_id,
        )

    @GoogleBaseHook.fallback_to_default_project_id
    def insert_all(
        self,
        project_id: str,
        dataset_id: str,
        table_id: str,
        rows: list,
        ignore_unknown_values: bool = False,
        skip_invalid_rows: bool = False,
        fail_on_error: bool = False,
    ) -> None:
        """
        Stream data into BigQuery one record at a time without a load job.

        .. seealso::
            For more information, see:
            https://cloud.google.com/bigquery/docs/reference/rest/v2/tabledata/insertAll

        :param project_id: The name of the project where we have the table
        :param dataset_id: The name of the dataset where we have the table
        :param table_id: The name of the table
        :param rows: the rows to insert

            .. code-block:: python

                rows = [{"json": {"a_key": "a_value_0"}}, {"json": {"a_key": "a_value_1"}}]

        :param ignore_unknown_values: [Optional] Accept rows that contain values
            that do not match the schema. The unknown values are ignored.
            The default value  is false, which treats unknown values as errors.
        :param skip_invalid_rows: [Optional] Insert all valid rows of a request,
            even if invalid rows exist. The default value is false, which causes
            the entire request to fail if any invalid rows exist.
        :param fail_on_error: [Optional] Force the task to fail if any errors occur.
            The default value is false, which indicates the task should not fail
            even if any insertion errors occur.
        """
        self.log.info("Inserting %s row(s) into table %s:%s.%s", len(rows), project_id, dataset_id, table_id)

        table_ref = TableReference(dataset_ref=DatasetReference(project_id, dataset_id), table_id=table_id)
        bq_client = self.get_client(project_id=project_id)
        table = bq_client.get_table(table_ref)
        errors = bq_client.insert_rows(
            table=table,
            rows=rows,
            ignore_unknown_values=ignore_unknown_values,
            skip_invalid_rows=skip_invalid_rows,
        )
        if errors:
            error_msg = f"{len(errors)} insert error(s) occurred. Details: {errors}"
            self.log.error(error_msg)
            if fail_on_error:
                raise AirflowException(f"BigQuery job failed. Error was: {error_msg}")
        else:
            self.log.info("All row(s) inserted successfully: %s:%s.%s", project_id, dataset_id, table_id)

    @GoogleBaseHook.fallback_to_default_project_id
    def update_dataset(
        self,
        fields: Sequence[str],
        dataset_resource: dict[str, Any],
        dataset_id: str | None = None,
        project_id: str = PROVIDE_PROJECT_ID,
        retry: Retry = DEFAULT_RETRY,
    ) -> Dataset:
        """
        Change some fields of a dataset.

        Use ``fields`` to specify which fields to update. At least one field
        must be provided. If a field is listed in ``fields`` and is ``None`` in
        ``dataset``, it will be deleted.

        If ``dataset.etag`` is not ``None``, the update will only
        succeed if the dataset on the server has the same ETag. Thus
        reading a dataset with ``get_dataset``, changing its fields,
        and then passing it to ``update_dataset`` will ensure that the changes
        will only be saved if no modifications to the dataset occurred
        since the read.

        :param dataset_resource: Dataset resource that will be provided
            in request body.
            https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets#resource
        :param dataset_id: The id of the dataset.
        :param fields: The properties of ``dataset`` to change (e.g. "friendly_name").
        :param project_id: The Google Cloud Project ID
        :param retry: How to retry the RPC.
        """
        dataset_resource["datasetReference"] = dataset_resource.get("datasetReference", {})

        for key, value in zip(["datasetId", "projectId"], [dataset_id, project_id]):
            spec_value = dataset_resource["datasetReference"].get(key)
            if value and not spec_value:
                dataset_resource["datasetReference"][key] = value

        self.log.info("Start updating dataset")
        dataset = self.get_client(project_id=project_id).update_dataset(
            dataset=Dataset.from_api_repr(dataset_resource),
            fields=fields,
            retry=retry,
        )
        self.log.info("Dataset successfully updated: %s", dataset)
        return dataset

    @deprecated(
        planned_removal_date="November 01, 2024",
        use_instead="airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.update_dataset",
        category=AirflowProviderDeprecationWarning,
    )
    def patch_dataset(
        self, dataset_id: str, dataset_resource: dict, project_id: str = PROVIDE_PROJECT_ID
    ) -> dict:
        """
        Patches information in an existing dataset.

        It only replaces fields that are provided in the submitted dataset resource.

        This method is deprecated. Please use :func:`.update_dataset` instead.

        More info:
        https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets/patch

        :param dataset_id: The BigQuery Dataset ID
        :param dataset_resource: Dataset resource that will be provided
            in request body.
            https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets#resource
        :param project_id: The Google Cloud Project ID
        """
        project_id = project_id or self.project_id
        if not dataset_id or not isinstance(dataset_id, str):
            raise ValueError(
                f"dataset_id argument must be provided and has a type 'str'. You provided: {dataset_id}"
            )

        service = self.get_service()
        dataset_project_id = project_id or self.project_id

        self.log.info("Start patching dataset: %s:%s", dataset_project_id, dataset_id)
        dataset = (
            service.datasets()
            .patch(
                datasetId=dataset_id,
                projectId=dataset_project_id,
                body=dataset_resource,
            )
            .execute(num_retries=self.num_retries)
        )
        self.log.info("Dataset successfully patched: %s", dataset)

        return dataset

    @deprecated(
        planned_removal_date="November 01, 2024",
        use_instead="airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_dataset_tables",
        category=AirflowProviderDeprecationWarning,
    )
    def get_dataset_tables_list(
        self,
        dataset_id: str,
        project_id: str = PROVIDE_PROJECT_ID,
        table_prefix: str | None = None,
        max_results: int | None = None,
    ) -> list[dict[str, Any]]:
        """
        List tables of a BigQuery dataset.

        If a table prefix is specified, only tables beginning by it are
        returned. This method is deprecated. Please use
        :func:`.get_dataset_tables` instead.

        For more information, see:
        https://cloud.google.com/bigquery/docs/reference/rest/v2/tables/list

        :param dataset_id: The BigQuery Dataset ID
        :param project_id: The Google Cloud Project ID
        :param table_prefix: Tables must begin by this prefix to be returned (case sensitive)
        :param max_results: The maximum number of results to return in a single response page.
            Leverage the page tokens to iterate through the entire collection.
        :return: List of tables associated with the dataset
        """
        project_id = project_id or self.project_id
        tables = self.get_client().list_tables(
            dataset=DatasetReference(project=project_id, dataset_id=dataset_id),
            max_results=max_results,
        )

        if table_prefix:
            result = [t.reference.to_api_repr() for t in tables if t.table_id.startswith(table_prefix)]
        else:
            result = [t.reference.to_api_repr() for t in tables]

        self.log.info("%s tables found", len(result))
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def get_datasets_list(
        self,
        project_id: str = PROVIDE_PROJECT_ID,
        include_all: bool = False,
        filter_: str | None = None,
        max_results: int | None = None,
        page_token: str | None = None,
        retry: Retry = DEFAULT_RETRY,
        return_iterator: bool = False,
    ) -> list[DatasetListItem] | HTTPIterator:
        """
        Get all BigQuery datasets in the current project.

        For more information, see:
        https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets/list

        :param project_id: Google Cloud Project for which you try to get all datasets
        :param include_all: True if results include hidden datasets. Defaults to False.
        :param filter_: An expression for filtering the results by label. For syntax, see
            https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets/list#filter.
        :param filter_: str
        :param max_results: Maximum number of datasets to return.
        :param max_results: int
        :param page_token: Token representing a cursor into the datasets. If not passed,
            the API will return the first page of datasets. The token marks the beginning of the
            iterator to be returned and the value of the ``page_token`` can be accessed at
            ``next_page_token`` of the :class:`~google.api_core.page_iterator.HTTPIterator`.
        :param page_token: str
        :param retry: How to retry the RPC.
        :param return_iterator: Instead of returning a list[Row], returns a HTTPIterator
            which can be used to obtain the next_page_token property.
        """
        iterator = self.get_client(project_id=project_id).list_datasets(
            project=project_id,
            include_all=include_all,
            filter=filter_,
            max_results=max_results,
            page_token=page_token,
            retry=retry,
        )

        # If iterator is requested, we cannot perform a list() on it to log the number
        # of datasets because we will have started iteration
        if return_iterator:
            # The iterator returned by list_datasets() is a HTTPIterator but annotated
            # as Iterator
            return iterator  #  type: ignore

        datasets_list = list(iterator)
        self.log.info("Datasets List: %s", len(datasets_list))
        return datasets_list

    @GoogleBaseHook.fallback_to_default_project_id
    def get_dataset(self, dataset_id: str, project_id: str = PROVIDE_PROJECT_ID) -> Dataset:
        """
        Fetch the dataset referenced by *dataset_id*.

        :param dataset_id: The BigQuery Dataset ID
        :param project_id: The Google Cloud Project ID
        :return: dataset_resource

        .. seealso::
            For more information, see Dataset Resource content:
            https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets#resource
        """
        dataset = self.get_client(project_id=project_id).get_dataset(
            dataset_ref=DatasetReference(project_id, dataset_id)
        )
        self.log.info("Dataset Resource: %s", dataset)
        return dataset

    @GoogleBaseHook.fallback_to_default_project_id
    def run_grant_dataset_view_access(
        self,
        source_dataset: str,
        view_dataset: str,
        view_table: str,
        view_project: str | None = None,
        project_id: str = PROVIDE_PROJECT_ID,
    ) -> dict[str, Any]:
        """
        Grant authorized view access of a dataset to a view table.

        If this view has already been granted access to the dataset, do nothing.
        This method is not atomic. Running it may clobber a simultaneous update.

        :param source_dataset: the source dataset
        :param view_dataset: the dataset that the view is in
        :param view_table: the table of the view
        :param project_id: the project of the source dataset. If None,
            self.project_id will be used.
        :param view_project: the project that the view is in. If None,
            self.project_id will be used.
        :return: the datasets resource of the source dataset.
        """
        view_project = view_project or project_id
        view_access = AccessEntry(
            role=None,
            entity_type="view",
            entity_id={"projectId": view_project, "datasetId": view_dataset, "tableId": view_table},
        )

        dataset = self.get_dataset(project_id=project_id, dataset_id=source_dataset)

        # Check to see if the view we want to add already exists.
        if view_access not in dataset.access_entries:
            self.log.info(
                "Granting table %s:%s.%s authorized view access to %s:%s dataset.",
                view_project,
                view_dataset,
                view_table,
                project_id,
                source_dataset,
            )
            dataset.access_entries += [view_access]
            dataset = self.update_dataset(
                fields=["access"], dataset_resource=dataset.to_api_repr(), project_id=project_id
            )
        else:
            self.log.info(
                "Table %s:%s.%s already has authorized view access to %s:%s dataset.",
                view_project,
                view_dataset,
                view_table,
                project_id,
                source_dataset,
            )
        return dataset.to_api_repr()

    @GoogleBaseHook.fallback_to_default_project_id
    def run_table_upsert(
        self, dataset_id: str, table_resource: dict[str, Any], project_id: str = PROVIDE_PROJECT_ID
    ) -> dict[str, Any]:
        """
        Update a table if it exists, otherwise create a new one.

        Since BigQuery does not natively allow table upserts, this is not an
        atomic operation.

        :param dataset_id: the dataset to upsert the table into.
        :param table_resource: a table resource. see
            https://cloud.google.com/bigquery/docs/reference/v2/tables#resource
        :param project_id: the project to upsert the table into.  If None,
            project will be self.project_id.
        """
        table_id = table_resource["tableReference"]["tableId"]
        table_resource = self._resolve_table_reference(
            table_resource=table_resource, project_id=project_id, dataset_id=dataset_id, table_id=table_id
        )

        tables_list_resp = self.get_dataset_tables(dataset_id=dataset_id, project_id=project_id)
        if any(table["tableId"] == table_id for table in tables_list_resp):
            self.log.info("Table %s:%s.%s exists, updating.", project_id, dataset_id, table_id)
            table = self.update_table(table_resource=table_resource)
        else:
            self.log.info("Table %s:%s.%s does not exist. creating.", project_id, dataset_id, table_id)
            table = self.create_empty_table(
                table_resource=table_resource, project_id=project_id
            ).to_api_repr()
        return table

    @deprecated(
        planned_removal_date="November 01, 2024",
        use_instead="airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.delete_table",
        category=AirflowProviderDeprecationWarning,
    )
    def run_table_delete(self, deletion_dataset_table: str, ignore_if_missing: bool = False) -> None:
        """
        Delete an existing table from the dataset.

        If the table does not exist, return an error unless *ignore_if_missing*
        is set to True.

        This method is deprecated. Please use :func:`.delete_table` instead.

        :param deletion_dataset_table: A dotted
            ``(<project>.|<project>:)<dataset>.<table>`` that indicates which table
            will be deleted.
        :param ignore_if_missing: if True, then return success even if the
            requested table does not exist.
        :return:
        """
        return self.delete_table(table_id=deletion_dataset_table, not_found_ok=ignore_if_missing)

    @GoogleBaseHook.fallback_to_default_project_id
    def delete_table(
        self,
        table_id: str,
        not_found_ok: bool = True,
        project_id: str = PROVIDE_PROJECT_ID,
    ) -> None:
        """
        Delete an existing table from the dataset.

        If the table does not exist, return an error unless *not_found_ok* is
        set to True.

        :param table_id: A dotted ``(<project>.|<project>:)<dataset>.<table>``
            that indicates which table will be deleted.
        :param not_found_ok: if True, then return success even if the
            requested table does not exist.
        :param project_id: the project used to perform the request
        """
        self.get_client(project_id=project_id).delete_table(
            table=table_id,
            not_found_ok=not_found_ok,
        )
        self.log.info("Deleted table %s", table_id)

    @deprecated(
        planned_removal_date="November 01, 2024",
        use_instead="airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.list_rows",
        category=AirflowProviderDeprecationWarning,
    )
    def get_tabledata(
        self,
        dataset_id: str,
        table_id: str,
        max_results: int | None = None,
        selected_fields: str | None = None,
        page_token: str | None = None,
        start_index: int | None = None,
    ) -> list[dict]:
        """
        Get data from given table.

        This method is deprecated. Please use :func:`.list_rows` instead.

        .. seealso:: https://cloud.google.com/bigquery/docs/reference/v2/tabledata/list

        :param dataset_id: the dataset ID of the requested table.
        :param table_id: the table ID of the requested table.
        :param max_results: the maximum results to return.
        :param selected_fields: List of fields to return (comma-separated). If
            unspecified, all fields are returned.
        :param page_token: page token, returned from a previous call,
            identifying the result set.
        :param start_index: zero based index of the starting row to read.
        :return: list of rows
        """
        rows = self.list_rows(
            dataset_id=dataset_id,
            table_id=table_id,
            max_results=max_results,
            selected_fields=selected_fields,
            page_token=page_token,
            start_index=start_index,
        )
        return [dict(r) for r in rows]

    @GoogleBaseHook.fallback_to_default_project_id
    def list_rows(
        self,
        dataset_id: str,
        table_id: str,
        max_results: int | None = None,
        selected_fields: list[str] | str | None = None,
        page_token: str | None = None,
        start_index: int | None = None,
        project_id: str = PROVIDE_PROJECT_ID,
        location: str | None = None,
        retry: Retry = DEFAULT_RETRY,
        return_iterator: bool = False,
    ) -> list[Row] | RowIterator:
        """
        List rows in a table.

        See https://cloud.google.com/bigquery/docs/reference/rest/v2/tabledata/list

        :param dataset_id: the dataset ID of the requested table.
        :param table_id: the table ID of the requested table.
        :param max_results: the maximum results to return.
        :param selected_fields: List of fields to return (comma-separated). If
            unspecified, all fields are returned.
        :param page_token: page token, returned from a previous call,
            identifying the result set.
        :param start_index: zero based index of the starting row to read.
        :param project_id: Project ID for the project which the client acts on behalf of.
        :param location: Default location for job.
        :param retry: How to retry the RPC.
        :param return_iterator: Instead of returning a list[Row], returns a RowIterator
            which can be used to obtain the next_page_token property.
        :return: list of rows
        """
        location = location or self.location
        if isinstance(selected_fields, str):
            selected_fields = selected_fields.split(",")

        if selected_fields:
            selected_fields_sequence = [SchemaField(n, "") for n in selected_fields]
        else:
            selected_fields_sequence = None

        table = self._resolve_table_reference(
            table_resource={},
            project_id=project_id,
            dataset_id=dataset_id,
            table_id=table_id,
        )

        iterator = self.get_client(project_id=project_id, location=location).list_rows(
            table=Table.from_api_repr(table),
            selected_fields=selected_fields_sequence,
            max_results=max_results,
            page_token=page_token,
            start_index=start_index,
            retry=retry,
        )
        if return_iterator:
            return iterator
        return list(iterator)

    @GoogleBaseHook.fallback_to_default_project_id
    def get_schema(self, dataset_id: str, table_id: str, project_id: str = PROVIDE_PROJECT_ID) -> dict:
        """
        Get the schema for a given dataset and table.

        .. seealso:: https://cloud.google.com/bigquery/docs/reference/v2/tables#resource

        :param dataset_id: the dataset ID of the requested table
        :param table_id: the table ID of the requested table
        :param project_id: the optional project ID of the requested table.
                If not provided, the connector's configured project will be used.
        :return: a table schema
        """
        table_ref = TableReference(dataset_ref=DatasetReference(project_id, dataset_id), table_id=table_id)
        table = self.get_client(project_id=project_id).get_table(table_ref)
        return {"fields": [s.to_api_repr() for s in table.schema]}

    @GoogleBaseHook.fallback_to_default_project_id
    def update_table_schema(
        self,
        schema_fields_updates: list[dict[str, Any]],
        include_policy_tags: bool,
        dataset_id: str,
        table_id: str,
        project_id: str = PROVIDE_PROJECT_ID,
    ) -> dict[str, Any]:
        """
        Update fields within a schema for a given dataset and table.

        Note that some fields in schemas are immutable; trying to change them
        will cause an exception.

        If a new field is included, it will be inserted, which requires all
        required fields to be set.

        .. seealso:: https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#TableSchema

        :param include_policy_tags: If set to True policy tags will be included in
            the update request which requires special permissions even if unchanged
            see https://cloud.google.com/bigquery/docs/column-level-security#roles
        :param dataset_id: the dataset ID of the requested table to be updated
        :param table_id: the table ID of the table to be updated
        :param schema_fields_updates: a partial schema resource. See
            https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#TableSchema

            .. code-block:: python

                schema_fields_updates = [
                    {"name": "emp_name", "description": "Some New Description"},
                    {"name": "salary", "description": "Some New Description"},
                    {
                        "name": "departments",
                        "fields": [
                            {"name": "name", "description": "Some New Description"},
                            {"name": "type", "description": "Some New Description"},
                        ],
                    },
                ]

        :param project_id: The name of the project where we want to update the table.
        """

        def _build_new_schema(
            current_schema: list[dict[str, Any]], schema_fields_updates: list[dict[str, Any]]
        ) -> list[dict[str, Any]]:
            # Turn schema_field_updates into a dict keyed on field names
            schema_fields_updates_dict = {field["name"]: field for field in deepcopy(schema_fields_updates)}

            # Create a new dict for storing the new schema, initiated based on the current_schema
            # as of Python 3.6, dicts retain order.
            new_schema = {field["name"]: field for field in deepcopy(current_schema)}

            # Each item in schema_fields_updates contains a potential patch
            # to a schema field, iterate over them
            for field_name, patched_value in schema_fields_updates_dict.items():
                # If this field already exists, update it
                if field_name in new_schema:
                    # If this field is of type RECORD and has a fields key we need to patch it recursively
                    if "fields" in patched_value:
                        patched_value["fields"] = _build_new_schema(
                            new_schema[field_name]["fields"], patched_value["fields"]
                        )
                    # Update the new_schema with the patched value
                    new_schema[field_name].update(patched_value)
                # This is a new field, just include the whole configuration for it
                else:
                    new_schema[field_name] = patched_value

            return list(new_schema.values())

        def _remove_policy_tags(schema: list[dict[str, Any]]):
            for field in schema:
                if "policyTags" in field:
                    del field["policyTags"]
                if "fields" in field:
                    _remove_policy_tags(field["fields"])

        current_table_schema = self.get_schema(
            dataset_id=dataset_id, table_id=table_id, project_id=project_id
        )["fields"]
        new_schema = _build_new_schema(current_table_schema, schema_fields_updates)

        if not include_policy_tags:
            _remove_policy_tags(new_schema)

        table = self.update_table(
            table_resource={"schema": {"fields": new_schema}},
            fields=["schema"],
            project_id=project_id,
            dataset_id=dataset_id,
            table_id=table_id,
        )
        return table

    @GoogleBaseHook.fallback_to_default_project_id
    def poll_job_complete(
        self,
        job_id: str,
        project_id: str = PROVIDE_PROJECT_ID,
        location: str | None = None,
        retry: Retry = DEFAULT_RETRY,
    ) -> bool:
        """
        Check if jobs have completed.

        :param job_id: id of the job.
        :param project_id: Google Cloud Project where the job is running
        :param location: location the job is running
        :param retry: How to retry the RPC.
        """
        location = location or self.location
        job = self.get_client(project_id=project_id, location=location).get_job(job_id=job_id)
        return job.done(retry=retry)

    @deprecated(
        planned_removal_date="November 01, 2024",
        use_instead="airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.cancel_job",
        category=AirflowProviderDeprecationWarning,
    )
    def cancel_query(self) -> None:
        """Cancel all started queries that have not yet completed."""
        if self.running_job_id:
            self.cancel_job(job_id=self.running_job_id)
        else:
            self.log.info("No running BigQuery jobs to cancel.")

    @GoogleBaseHook.fallback_to_default_project_id
    def cancel_job(
        self,
        job_id: str,
        project_id: str = PROVIDE_PROJECT_ID,
        location: str | None = None,
    ) -> None:
        """
        Cancel a job and wait for cancellation to complete.

        :param job_id: id of the job.
        :param project_id: Google Cloud Project where the job is running
        :param location: location the job is running
        """
        project_id = project_id or self.project_id
        location = location or self.location

        if self.poll_job_complete(job_id=job_id, project_id=project_id, location=location):
            self.log.info("No running BigQuery jobs to cancel.")
            return

        self.log.info("Attempting to cancel job : %s, %s", project_id, job_id)
        self.get_client(location=location, project_id=project_id).cancel_job(job_id=job_id)

        # Wait for all the calls to cancel to finish
        max_polling_attempts = 12
        polling_attempts = 0

        job_complete = False
        while polling_attempts < max_polling_attempts and not job_complete:
            polling_attempts += 1
            job_complete = self.poll_job_complete(job_id=job_id, project_id=project_id, location=location)
            if job_complete:
                self.log.info("Job successfully canceled: %s, %s", project_id, job_id)
            elif polling_attempts == max_polling_attempts:
                self.log.info(
                    "Stopping polling due to timeout. Job %s, %s "
                    "has not completed cancel and may or may not finish.",
                    project_id,
                    job_id,
                )
            else:
                self.log.info("Waiting for canceled job %s, %s to finish.", project_id, job_id)
                time.sleep(5)

    @GoogleBaseHook.fallback_to_default_project_id
    @GoogleBaseHook.refresh_credentials_retry()
    def get_job(
        self,
        job_id: str,
        project_id: str = PROVIDE_PROJECT_ID,
        location: str | None = None,
    ) -> BigQueryJob | UnknownJob:
        """
        Retrieve a BigQuery job.

        .. seealso:: https://cloud.google.com/bigquery/docs/reference/v2/jobs

        :param job_id: The ID of the job. The ID must contain only letters (a-z, A-Z),
            numbers (0-9), underscores (_), or dashes (-). The maximum length is 1,024
            characters.
        :param project_id: Google Cloud Project where the job is running.
        :param location: Location where the job is running.
        """
        client = self.get_client(project_id=project_id, location=location)
        job = client.get_job(job_id=job_id, project=project_id, location=location)
        return job

    @staticmethod
    def _custom_job_id(configuration: dict[str, Any]) -> str:
        hash_base = json.dumps(configuration, sort_keys=True)
        uniqueness_suffix = md5(hash_base.encode()).hexdigest()
        microseconds_from_epoch = int(
            (datetime.now() - datetime.fromtimestamp(0)) / timedelta(microseconds=1)
        )
        return f"airflow_{microseconds_from_epoch}_{uniqueness_suffix}"

    @GoogleBaseHook.fallback_to_default_project_id
    def insert_job(
        self,
        configuration: dict,
        job_id: str | None = None,
        project_id: str = PROVIDE_PROJECT_ID,
        location: str | None = None,
        nowait: bool = False,
        retry: Retry = DEFAULT_RETRY,
        timeout: float | None = None,
    ) -> BigQueryJob:
        """
        Execute a BigQuery job and wait for it to complete.

        .. seealso:: https://cloud.google.com/bigquery/docs/reference/v2/jobs

        :param configuration: The configuration parameter maps directly to
            BigQuery's configuration field in the job object. See
            https://cloud.google.com/bigquery/docs/reference/v2/jobs for
            details.
        :param job_id: The ID of the job. The ID must contain only letters (a-z, A-Z),
            numbers (0-9), underscores (_), or dashes (-). The maximum length is 1,024
            characters. If not provided then uuid will be generated.
        :param project_id: Google Cloud Project where the job is running.
        :param location: Location the job is running.
        :param nowait: Whether to insert job without waiting for the result.
        :param retry: How to retry the RPC.
        :param timeout: The number of seconds to wait for the underlying HTTP transport
            before using ``retry``.
        :return: The job ID.
        """
        location = location or self.location
        job_id = job_id or self._custom_job_id(configuration)

        client = self.get_client(project_id=project_id, location=location)
        job_data = {
            "configuration": configuration,
            "jobReference": {"jobId": job_id, "projectId": project_id, "location": location},
        }

        supported_jobs: dict[str, type[CopyJob] | type[QueryJob] | type[LoadJob] | type[ExtractJob]] = {
            LoadJob._JOB_TYPE: LoadJob,
            CopyJob._JOB_TYPE: CopyJob,
            ExtractJob._JOB_TYPE: ExtractJob,
            QueryJob._JOB_TYPE: QueryJob,
        }

        job: type[CopyJob] | type[QueryJob] | type[LoadJob] | type[ExtractJob] | None = None
        for job_type, job_object in supported_jobs.items():
            if job_type in configuration:
                job = job_object
                break

        if not job:
            raise AirflowException(f"Unknown job type. Supported types: {supported_jobs.keys()}")
        job_api_repr = job.from_api_repr(job_data, client)
        self.log.info("Inserting job %s", job_api_repr.job_id)
        if nowait:
            # Initiate the job and don't wait for it to complete.
            job_api_repr._begin()
        else:
            # Start the job and wait for it to complete and get the result.
            job_api_repr.result(timeout=timeout, retry=retry)
        return job_api_repr

    @deprecated(
        planned_removal_date="November 01, 2024",
        use_instead="airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.insert_job",
        category=AirflowProviderDeprecationWarning,
    )
    def run_with_configuration(self, configuration: dict) -> str:
        """
        Execute a BigQuery SQL query.

        .. seealso:: https://cloud.google.com/bigquery/docs/reference/v2/jobs

        This method is deprecated. Please use :func:`.insert_job` instead.

        :param configuration: The configuration parameter maps directly to
            BigQuery's configuration field in the job object. See
            https://cloud.google.com/bigquery/docs/reference/v2/jobs for
            details.
        """
        job = self.insert_job(configuration=configuration, project_id=self.project_id)
        self.running_job_id = job.job_id
        return job.job_id

    @deprecated(
        planned_removal_date="November 01, 2024",
        use_instead="airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.insert_job",
        category=AirflowProviderDeprecationWarning,
    )
    def run_load(
        self,
        destination_project_dataset_table: str,
        source_uris: list,
        schema_fields: list | None = None,
        source_format: str = "CSV",
        create_disposition: str = "CREATE_IF_NEEDED",
        skip_leading_rows: int = 0,
        write_disposition: str = "WRITE_EMPTY",
        field_delimiter: str = ",",
        max_bad_records: int = 0,
        quote_character: str | None = None,
        ignore_unknown_values: bool = False,
        allow_quoted_newlines: bool = False,
        allow_jagged_rows: bool = False,
        encoding: str = "UTF-8",
        schema_update_options: Iterable | None = None,
        src_fmt_configs: dict | None = None,
        time_partitioning: dict | None = None,
        cluster_fields: list | None = None,
        autodetect: bool = False,
        encryption_configuration: dict | None = None,
        labels: dict | None = None,
        description: str | None = None,
    ) -> str:
        """
        Load data from Google Cloud Storage to BigQuery.

        .. seealso:: https://cloud.google.com/bigquery/docs/reference/v2/jobs

        This method is deprecated. Please use :func:`.insert_job` instead.

        :param destination_project_dataset_table:
            The dotted ``(<project>.|<project>:)<dataset>.<table>($<partition>)`` BigQuery
            table to load data into. If ``<project>`` is not included, project will be the
            project defined in the connection json. If a partition is specified the
            operator will automatically append the data, create a new partition or create
            a new DAY partitioned table.
        :param schema_fields: The schema field list as defined here:
            https://cloud.google.com/bigquery/docs/reference/v2/jobs#configuration.load
            Required if autodetect=False; optional if autodetect=True.
        :param autodetect: Attempt to autodetect the schema for CSV and JSON
            source files.
        :param source_uris: The source Google Cloud
            Storage URI (e.g. gs://some-bucket/some-file.txt). A single wild
            per-object name can be used.
        :param source_format: File format to export.
        :param create_disposition: The create disposition if the table doesn't exist.
        :param skip_leading_rows: Number of rows to skip when loading from a CSV.
        :param write_disposition: The write disposition if the table already exists.
        :param field_delimiter: The delimiter to use when loading from a CSV.
        :param max_bad_records: The maximum number of bad records that BigQuery can
            ignore when running the job.
        :param quote_character: The value that is used to quote data sections in a CSV
            file.
        :param ignore_unknown_values: [Optional] Indicates if BigQuery should allow
            extra values that are not represented in the table schema.
            If true, the extra values are ignored. If false, records with extra columns
            are treated as bad records, and if there are too many bad records, an
            invalid error is returned in the job result.
        :param allow_quoted_newlines: Whether to allow quoted newlines (true) or not
            (false).
        :param allow_jagged_rows: Accept rows that are missing trailing optional columns.
            The missing values are treated as nulls. If false, records with missing
            trailing columns are treated as bad records, and if there are too many bad
            records, an invalid error is returned in the job result. Only applicable when
            source_format is CSV.
        :param encoding: The character encoding of the data.

            .. seealso::
                https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#externalDataConfiguration.csvOptions.encoding
        :param schema_update_options: Allows the schema of the destination
            table to be updated as a side effect of the load job.
        :param src_fmt_configs: configure optional fields specific to the source format
        :param time_partitioning: configure optional time partitioning fields i.e.
            partition by field, type and  expiration as per API specifications.
        :param cluster_fields: Request that the result of this load be stored sorted
            by one or more columns. BigQuery supports clustering for both partitioned and
            non-partitioned tables. The order of columns given determines the sort order.
        :param encryption_configuration: [Optional] Custom encryption configuration (e.g., Cloud KMS keys).

            .. code-block:: python

                encryption_configuration = {
                    "kmsKeyName": "projects/testp/locations/us/keyRings/test-kr/cryptoKeys/test-key",
                }

        :param labels: A dictionary containing labels for the BiqQuery table.
        :param description: A string containing the description for the BigQuery table.
        """
        if not self.project_id:
            raise ValueError("The project_id should be set")

        # To provide backward compatibility
        schema_update_options = list(schema_update_options or [])

        # bigquery only allows certain source formats
        # we check to make sure the passed source format is valid
        # if it's not, we raise a ValueError
        # Refer to this link for more details:
        #   https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs#configuration.query.tableDefinitions.(key).sourceFormat

        if schema_fields is None and not autodetect:
            raise ValueError("You must either pass a schema or autodetect=True.")

        if src_fmt_configs is None:
            src_fmt_configs = {}

        source_format = source_format.upper()
        allowed_formats = [
            "CSV",
            "NEWLINE_DELIMITED_JSON",
            "AVRO",
            "GOOGLE_SHEETS",
            "DATASTORE_BACKUP",
            "PARQUET",
        ]
        if source_format not in allowed_formats:
            raise ValueError(
                f"{source_format} is not a valid source format. "
                f"Please use one of the following types: {allowed_formats}."
            )

        # bigquery also allows you to define how you want a table's schema to change
        # as a side effect of a load
        # for more details:
        # https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs#configuration.load.schemaUpdateOptions
        allowed_schema_update_options = ["ALLOW_FIELD_ADDITION", "ALLOW_FIELD_RELAXATION"]
        if not set(allowed_schema_update_options).issuperset(set(schema_update_options)):
            raise ValueError(
                f"{schema_update_options} contains invalid schema update options. "
                f"Please only use one or more of the following options: {allowed_schema_update_options}"
            )

        destination_project, destination_dataset, destination_table = self.split_tablename(
            table_input=destination_project_dataset_table,
            default_project_id=self.project_id,
            var_name="destination_project_dataset_table",
        )

        configuration: dict[str, Any] = {
            "load": {
                "autodetect": autodetect,
                "createDisposition": create_disposition,
                "destinationTable": {
                    "projectId": destination_project,
                    "datasetId": destination_dataset,
                    "tableId": destination_table,
                },
                "sourceFormat": source_format,
                "sourceUris": source_uris,
                "writeDisposition": write_disposition,
                "ignoreUnknownValues": ignore_unknown_values,
            }
        }

        time_partitioning = _cleanse_time_partitioning(destination_project_dataset_table, time_partitioning)
        if time_partitioning:
            configuration["load"].update({"timePartitioning": time_partitioning})

        if cluster_fields:
            configuration["load"].update({"clustering": {"fields": cluster_fields}})

        if schema_fields:
            configuration["load"]["schema"] = {"fields": schema_fields}

        if schema_update_options:
            if write_disposition not in ["WRITE_APPEND", "WRITE_TRUNCATE"]:
                raise ValueError(
                    "schema_update_options is only "
                    "allowed if write_disposition is "
                    "'WRITE_APPEND' or 'WRITE_TRUNCATE'."
                )
            else:
                self.log.info("Adding experimental 'schemaUpdateOptions': %s", schema_update_options)
                configuration["load"]["schemaUpdateOptions"] = schema_update_options

        if max_bad_records:
            configuration["load"]["maxBadRecords"] = max_bad_records

        if encryption_configuration:
            configuration["load"]["destinationEncryptionConfiguration"] = encryption_configuration

        if labels or description:
            configuration["load"].update({"destinationTableProperties": {}})

            if labels:
                configuration["load"]["destinationTableProperties"]["labels"] = labels

            if description:
                configuration["load"]["destinationTableProperties"]["description"] = description

        src_fmt_to_configs_mapping = {
            "CSV": [
                "allowJaggedRows",
                "allowQuotedNewlines",
                "autodetect",
                "fieldDelimiter",
                "skipLeadingRows",
                "ignoreUnknownValues",
                "nullMarker",
                "quote",
                "encoding",
                "preserveAsciiControlCharacters",
            ],
            "DATASTORE_BACKUP": ["projectionFields"],
            "NEWLINE_DELIMITED_JSON": ["autodetect", "ignoreUnknownValues"],
            "PARQUET": ["autodetect", "ignoreUnknownValues"],
            "AVRO": ["useAvroLogicalTypes"],
        }

        valid_configs = src_fmt_to_configs_mapping[source_format]

        # if following fields are not specified in src_fmt_configs,
        # honor the top-level params for backward-compatibility
        backward_compatibility_configs = {
            "skipLeadingRows": skip_leading_rows,
            "fieldDelimiter": field_delimiter,
            "ignoreUnknownValues": ignore_unknown_values,
            "quote": quote_character,
            "allowQuotedNewlines": allow_quoted_newlines,
            "encoding": encoding,
        }

        src_fmt_configs = _validate_src_fmt_configs(
            source_format, src_fmt_configs, valid_configs, backward_compatibility_configs
        )

        configuration["load"].update(src_fmt_configs)

        if allow_jagged_rows:
            configuration["load"]["allowJaggedRows"] = allow_jagged_rows

        job = self.insert_job(configuration=configuration, project_id=self.project_id)
        self.running_job_id = job.job_id
        return job.job_id

    @deprecated(
        planned_removal_date="November 01, 2024",
        use_instead="airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.insert_job",
        category=AirflowProviderDeprecationWarning,
    )
    def run_copy(
        self,
        source_project_dataset_tables: list | str,
        destination_project_dataset_table: str,
        write_disposition: str = "WRITE_EMPTY",
        create_disposition: str = "CREATE_IF_NEEDED",
        labels: dict | None = None,
        encryption_configuration: dict | None = None,
    ) -> str:
        """
        Copy data from one BigQuery table to another.

        .. seealso:: https://cloud.google.com/bigquery/docs/reference/v2/jobs#configuration.copy

        This method is deprecated. Please use :func:`.insert_job` instead.

        :param source_project_dataset_tables: One or more dotted
            ``(project:|project.)<dataset>.<table>``
            BigQuery tables to use as the source data. Use a list if there are
            multiple source tables.
            If ``<project>`` is not included, project will be the project defined
            in the connection json.
        :param destination_project_dataset_table: The destination BigQuery
            table. Format is: ``(project:|project.)<dataset>.<table>``
        :param write_disposition: The write disposition if the table already exists.
        :param create_disposition: The create disposition if the table doesn't exist.
        :param labels: a dictionary containing labels for the job/query,
            passed to BigQuery
        :param encryption_configuration: [Optional] Custom encryption configuration (e.g., Cloud KMS keys).

            .. code-block:: python

                    encryption_configuration = {
                        "kmsKeyName": "projects/testp/locations/us/keyRings/test-kr/cryptoKeys/test-key",
                    }
        """
        if not self.project_id:
            raise ValueError("The project_id should be set")

        source_project_dataset_tables = (
            [source_project_dataset_tables]
            if not isinstance(source_project_dataset_tables, list)
            else source_project_dataset_tables
        )

        source_project_dataset_tables_fixup = []
        for source_project_dataset_table in source_project_dataset_tables:
            source_project, source_dataset, source_table = self.split_tablename(
                table_input=source_project_dataset_table,
                default_project_id=self.project_id,
                var_name="source_project_dataset_table",
            )
            source_project_dataset_tables_fixup.append(
                {"projectId": source_project, "datasetId": source_dataset, "tableId": source_table}
            )

        destination_project, destination_dataset, destination_table = self.split_tablename(
            table_input=destination_project_dataset_table, default_project_id=self.project_id
        )
        configuration = {
            "copy": {
                "createDisposition": create_disposition,
                "writeDisposition": write_disposition,
                "sourceTables": source_project_dataset_tables_fixup,
                "destinationTable": {
                    "projectId": destination_project,
                    "datasetId": destination_dataset,
                    "tableId": destination_table,
                },
            }
        }

        if labels:
            configuration["labels"] = labels

        if encryption_configuration:
            configuration["copy"]["destinationEncryptionConfiguration"] = encryption_configuration

        job = self.insert_job(configuration=configuration, project_id=self.project_id)
        self.running_job_id = job.job_id
        return job.job_id

    @deprecated(
        planned_removal_date="November 01, 2024",
        use_instead="airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.insert_job",
        category=AirflowProviderDeprecationWarning,
    )
    def run_extract(
        self,
        source_project_dataset_table: str,
        destination_cloud_storage_uris: list[str],
        compression: str = "NONE",
        export_format: str = "CSV",
        field_delimiter: str = ",",
        print_header: bool = True,
        labels: dict | None = None,
        return_full_job: bool = False,
    ) -> str | BigQueryJob:
        """
        Copy data from BigQuery to Google Cloud Storage.

        .. seealso:: https://cloud.google.com/bigquery/docs/reference/v2/jobs

        This method is deprecated. Please use :func:`.insert_job` instead.

        :param source_project_dataset_table: The dotted ``<dataset>.<table>``
            BigQuery table to use as the source data.
        :param destination_cloud_storage_uris: The destination Google Cloud
            Storage URI (e.g. gs://some-bucket/some-file.txt). Follows
            convention defined here:
            https://cloud.google.com/bigquery/exporting-data-from-bigquery#exportingmultiple
        :param compression: Type of compression to use.
        :param export_format: File format to export.
        :param field_delimiter: The delimiter to use when extracting to a CSV.
        :param print_header: Whether to print a header for a CSV file extract.
        :param labels: a dictionary containing labels for the job/query,
            passed to BigQuery
        :param return_full_job: return full job instead of job id only
        """
        if not self.project_id:
            raise ValueError("The project_id should be set")

        source_project, source_dataset, source_table = self.split_tablename(
            table_input=source_project_dataset_table,
            default_project_id=self.project_id,
            var_name="source_project_dataset_table",
        )

        configuration: dict[str, Any] = {
            "extract": {
                "sourceTable": {
                    "projectId": source_project,
                    "datasetId": source_dataset,
                    "tableId": source_table,
                },
                "compression": compression,
                "destinationUris": destination_cloud_storage_uris,
                "destinationFormat": export_format,
            }
        }

        if labels:
            configuration["labels"] = labels

        if export_format == "CSV":
            # Only set fieldDelimiter and printHeader fields if using CSV.
            # Google does not like it if you set these fields for other export
            # formats.
            configuration["extract"]["fieldDelimiter"] = field_delimiter
            configuration["extract"]["printHeader"] = print_header

        job = self.insert_job(configuration=configuration, project_id=self.project_id)
        self.running_job_id = job.job_id
        if return_full_job:
            return job
        return job.job_id

    @deprecated(
        planned_removal_date="November 01, 2024",
        use_instead="airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.insert_job",
        category=AirflowProviderDeprecationWarning,
    )
    def run_query(
        self,
        sql: str,
        destination_dataset_table: str | None = None,
        write_disposition: str = "WRITE_EMPTY",
        allow_large_results: bool = False,
        flatten_results: bool | None = None,
        udf_config: list | None = None,
        use_legacy_sql: bool | None = None,
        maximum_billing_tier: int | None = None,
        maximum_bytes_billed: float | None = None,
        create_disposition: str = "CREATE_IF_NEEDED",
        query_params: list | None = None,
        labels: dict | None = None,
        schema_update_options: Iterable | None = None,
        priority: str | None = None,
        time_partitioning: dict | None = None,
        api_resource_configs: dict | None = None,
        cluster_fields: list[str] | None = None,
        location: str | None = None,
        encryption_configuration: dict | None = None,
    ) -> str:
        """
        Execute a BigQuery SQL query.

        Optionally persists results in a BigQuery table.

        .. seealso:: https://cloud.google.com/bigquery/docs/reference/v2/jobs

        This method is deprecated. Please use :func:`.insert_job` instead.

        For more details about these parameters.

        :param sql: The BigQuery SQL to execute.
        :param destination_dataset_table: The dotted ``<dataset>.<table>``
            BigQuery table to save the query results.
        :param write_disposition: What to do if the table already exists in
            BigQuery.
        :param allow_large_results: Whether to allow large results.
        :param flatten_results: If true and query uses legacy SQL dialect, flattens
            all nested and repeated fields in the query results. ``allowLargeResults``
            must be true if this is set to false. For standard SQL queries, this
            flag is ignored and results are never flattened.
        :param udf_config: The User Defined Function configuration for the query.
            See https://cloud.google.com/bigquery/user-defined-functions for details.
        :param use_legacy_sql: Whether to use legacy SQL (true) or standard SQL (false).
            If `None`, defaults to `self.use_legacy_sql`.
        :param api_resource_configs: a dictionary that contain params
            'configuration' applied for Google BigQuery Jobs API:
            https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs
            for example, {'query': {'useQueryCache': False}}. You could use it
            if you need to provide some params that are not supported by the
            BigQueryHook like args.
        :param maximum_billing_tier: Positive integer that serves as a
            multiplier of the basic price.
        :param maximum_bytes_billed: Limits the bytes billed for this job.
            Queries that will have bytes billed beyond this limit will fail
            (without incurring a charge). If unspecified, this will be
            set to your project default.
        :param create_disposition: Specifies whether the job is allowed to
            create new tables.
        :param query_params: a list of dictionary containing query parameter types and
            values, passed to BigQuery
        :param labels: a dictionary containing labels for the job/query,
            passed to BigQuery
        :param schema_update_options: Allows the schema of the destination
            table to be updated as a side effect of the query job.
        :param priority: Specifies a priority for the query.
            Possible values include INTERACTIVE and BATCH.
            If `None`, defaults to `self.priority`.
        :param time_partitioning: configure optional time partitioning fields i.e.
            partition by field, type and expiration as per API specifications.
        :param cluster_fields: Request that the result of this query be stored sorted
            by one or more columns. BigQuery supports clustering for both partitioned and
            non-partitioned tables. The order of columns given determines the sort order.
        :param location: The geographic location of the job. Required except for
            US and EU. See details at
            https://cloud.google.com/bigquery/docs/locations#specifying_your_location
        :param encryption_configuration: [Optional] Custom encryption configuration (e.g., Cloud KMS keys).

            .. code-block:: python

                    encryption_configuration = {
                        "kmsKeyName": "projects/testp/locations/us/keyRings/test-kr/cryptoKeys/test-key",
                    }
        """
        if not self.project_id:
            raise ValueError("The project_id should be set")

        labels = labels or self.labels
        schema_update_options = list(schema_update_options or [])

        priority = priority or self.priority

        if time_partitioning is None:
            time_partitioning = {}

        if not api_resource_configs:
            api_resource_configs = self.api_resource_configs
        else:
            _validate_value("api_resource_configs", api_resource_configs, dict)
        configuration = deepcopy(api_resource_configs)
        if "query" not in configuration:
            configuration["query"] = {}

        else:
            _validate_value("api_resource_configs['query']", configuration["query"], dict)

        if sql is None and not configuration["query"].get("query", None):
            raise TypeError("`BigQueryBaseCursor.run_query` missing 1 required positional argument: `sql`")

        # BigQuery also allows you to define how you want a table's schema to change
        # as a side effect of a query job
        # for more details:
        #   https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs#configuration.query.schemaUpdateOptions

        allowed_schema_update_options = ["ALLOW_FIELD_ADDITION", "ALLOW_FIELD_RELAXATION"]

        if not set(allowed_schema_update_options).issuperset(set(schema_update_options)):
            raise ValueError(
                f"{schema_update_options} contains invalid schema update options."
                f" Please only use one or more of the following options: {allowed_schema_update_options}"
            )

        if schema_update_options:
            if write_disposition not in ["WRITE_APPEND", "WRITE_TRUNCATE"]:
                raise ValueError(
                    "schema_update_options is only "
                    "allowed if write_disposition is "
                    "'WRITE_APPEND' or 'WRITE_TRUNCATE'."
                )

        if destination_dataset_table:
            destination_project, destination_dataset, destination_table = self.split_tablename(
                table_input=destination_dataset_table, default_project_id=self.project_id
            )

            destination_dataset_table = {  # type: ignore
                "projectId": destination_project,
                "datasetId": destination_dataset,
                "tableId": destination_table,
            }

        if cluster_fields:
            cluster_fields = {"fields": cluster_fields}  # type: ignore

        query_param_list: list[tuple[Any, str, str | bool | None | dict, type | tuple[type]]] = [
            (sql, "query", None, (str,)),
            (priority, "priority", priority, (str,)),
            (use_legacy_sql, "useLegacySql", self.use_legacy_sql, bool),
            (query_params, "queryParameters", None, list),
            (udf_config, "userDefinedFunctionResources", None, list),
            (maximum_billing_tier, "maximumBillingTier", None, int),
            (maximum_bytes_billed, "maximumBytesBilled", None, float),
            (time_partitioning, "timePartitioning", {}, dict),
            (schema_update_options, "schemaUpdateOptions", None, list),
            (destination_dataset_table, "destinationTable", None, dict),
            (cluster_fields, "clustering", None, dict),
        ]

        for param, param_name, param_default, param_type in query_param_list:
            if param_name not in configuration["query"] and param in [None, {}, ()]:
                if param_name == "timePartitioning":
                    param_default = _cleanse_time_partitioning(destination_dataset_table, time_partitioning)
                param = param_default

            if param in [None, {}, ()]:
                continue

            _api_resource_configs_duplication_check(param_name, param, configuration["query"])

            configuration["query"][param_name] = param

            # check valid type of provided param,
            # it last step because we can get param from 2 sources,
            # and first of all need to find it

            _validate_value(param_name, configuration["query"][param_name], param_type)

            if param_name == "schemaUpdateOptions" and param:
                self.log.info("Adding experimental 'schemaUpdateOptions': %s", schema_update_options)

            if param_name == "destinationTable":
                for key in ["projectId", "datasetId", "tableId"]:
                    if key not in configuration["query"]["destinationTable"]:
                        raise ValueError(
                            "Not correct 'destinationTable' in "
                            "api_resource_configs. 'destinationTable' "
                            "must be a dict with {'projectId':'', "
                            "'datasetId':'', 'tableId':''}"
                        )
                else:
                    configuration["query"].update(
                        {
                            "allowLargeResults": allow_large_results,
                            "flattenResults": flatten_results,
                            "writeDisposition": write_disposition,
                            "createDisposition": create_disposition,
                        }
                    )

        if (
            "useLegacySql" in configuration["query"]
            and configuration["query"]["useLegacySql"]
            and "queryParameters" in configuration["query"]
        ):
            raise ValueError("Query parameters are not allowed when using legacy SQL")

        if labels:
            _api_resource_configs_duplication_check("labels", labels, configuration)
            configuration["labels"] = labels

        if encryption_configuration:
            configuration["query"]["destinationEncryptionConfiguration"] = encryption_configuration

        job = self.insert_job(configuration=configuration, project_id=self.project_id, location=location)
        self.running_job_id = job.job_id
        return job.job_id

    def generate_job_id(self, job_id, dag_id, task_id, logical_date, configuration, force_rerun=False) -> str:
        if force_rerun:
            hash_base = str(uuid.uuid4())
        else:
            hash_base = json.dumps(configuration, sort_keys=True)

        uniqueness_suffix = md5(hash_base.encode()).hexdigest()

        if job_id:
            return f"{job_id}_{uniqueness_suffix}"

        exec_date = logical_date.isoformat()
        job_id = f"airflow_{dag_id}_{task_id}_{exec_date}_{uniqueness_suffix}"
        return re.sub(r"[:\-+.]", "_", job_id)

    def split_tablename(
        self, table_input: str, default_project_id: str, var_name: str | None = None
    ) -> tuple[str, str, str]:
        if "." not in table_input:
            raise ValueError(f"Expected table name in the format of <dataset>.<table>. Got: {table_input}")

        if not default_project_id:
            raise ValueError("INTERNAL: No default project is specified")

        def var_print(var_name):
            if var_name is None:
                return ""
            else:
                return f"Format exception for {var_name}: "

        if table_input.count(".") + table_input.count(":") > 3:
            raise ValueError(f"{var_print(var_name)}Use either : or . to specify project got {table_input}")
        cmpt = table_input.rsplit(":", 1)
        project_id = None
        rest = table_input
        if len(cmpt) == 1:
            project_id = None
            rest = cmpt[0]
        elif len(cmpt) == 2 and cmpt[0].count(":") <= 1:
            if cmpt[-1].count(".") != 2:
                project_id = cmpt[0]
                rest = cmpt[1]
        else:
            raise ValueError(
                f"{var_print(var_name)}Expect format of (<project:)<dataset>.<table>, got {table_input}"
            )

        cmpt = rest.split(".")
        if len(cmpt) == 3:
            if project_id:
                raise ValueError(f"{var_print(var_name)}Use either : or . to specify project")
            project_id = cmpt[0]
            dataset_id = cmpt[1]
            table_id = cmpt[2]

        elif len(cmpt) == 2:
            dataset_id = cmpt[0]
            table_id = cmpt[1]
        else:
            raise ValueError(
                f"{var_print(var_name)}Expect format of (<project.|<project:)<dataset>.<table>, "
                f"got {table_input}"
            )
        if project_id is None:
            if var_name is not None:
                self.log.info(
                    'Project is not included in %s: %s; using project "%s"',
                    var_name,
                    table_input,
                    default_project_id,
                )
            project_id = default_project_id

        return project_id, dataset_id, table_id

    @GoogleBaseHook.fallback_to_default_project_id
    def get_query_results(
        self,
        job_id: str,
        location: str,
        max_results: int | None = None,
        selected_fields: list[str] | str | None = None,
        project_id: str = PROVIDE_PROJECT_ID,
        retry: Retry = DEFAULT_RETRY,
        job_retry: Retry = DEFAULT_JOB_RETRY,
    ) -> list[dict[str, Any]]:
        """
        Get query results given a job_id.

        :param job_id: The ID of the job.
            The ID must contain only letters (a-z, A-Z), numbers (0-9), underscores (_), or
            dashes (-). The maximum length is 1,024 characters.
        :param location: The location used for the operation.
        :param selected_fields: List of fields to return (comma-separated). If
            unspecified, all fields are returned.
        :param max_results: The maximum number of records (rows) to be fetched
            from the table.
        :param project_id: Google Cloud Project where the job ran.
        :param retry: How to retry the RPC.
        :param job_retry: How to retry failed jobs.

        :return: List of rows where columns are filtered by selected fields, when given

        :raises: AirflowException
        """
        if isinstance(selected_fields, str):
            selected_fields = selected_fields.split(",")
        job = self.get_job(job_id=job_id, project_id=project_id, location=location)
        if not isinstance(job, QueryJob):
            raise AirflowException(f"Job '{job_id}' is not a query job")

        if job.state != "DONE":
            raise AirflowException(f"Job '{job_id}' is not in DONE state")

        rows = [dict(row) for row in job.result(max_results=max_results, retry=retry, job_retry=job_retry)]
        return [{k: row[k] for k in row if k in selected_fields} for row in rows] if selected_fields else rows

    @property
    def scopes(self) -> Sequence[str]:
        """
        Return OAuth 2.0 scopes.

        :return: Returns the scope defined in impersonation_scopes, the connection configuration, or the default scope
        """
        scope_value: str | None
        if self.impersonation_chain and self.impersonation_scopes:
            scope_value = ",".join(self.impersonation_scopes)
        else:
            scope_value = self._get_field("scope", None)
        return _get_scopes(scope_value)


class BigQueryConnection:
    """
    BigQuery connection.

    BigQuery does not have a notion of a persistent connection. Thus, these
    objects are small stateless factories for cursors, which do all the real
    work.
    """

    def __init__(self, *args, **kwargs) -> None:
        self._args = args
        self._kwargs = kwargs

    def close(self) -> None:
        """Do nothing. Not needed for BigQueryConnection."""

    def commit(self) -> None:
        """Do nothing. BigQueryConnection does not support transactions."""

    def cursor(self) -> BigQueryCursor:
        """Return a new :py:class:`Cursor` object using the connection."""
        return BigQueryCursor(*self._args, **self._kwargs)

    def rollback(self) -> NoReturn:
        """Do nothing. BigQueryConnection does not support transactions."""
        raise NotImplementedError("BigQueryConnection does not have transactions")


class BigQueryBaseCursor(LoggingMixin):
    """
    BigQuery cursor.

    The BigQuery base cursor contains helper methods to execute queries against
    BigQuery. The methods can be used directly by operators, in cases where a
    PEP 249 cursor isn't needed.
    """

    def __init__(
        self,
        service: Any,
        project_id: str,
        hook: BigQueryHook,
        use_legacy_sql: bool = True,
        api_resource_configs: dict | None = None,
        location: str | None = None,
        num_retries: int = 5,
        labels: dict | None = None,
    ) -> None:
        super().__init__()
        self.service = service
        self.project_id = project_id
        self.use_legacy_sql = use_legacy_sql
        if api_resource_configs:
            _validate_value("api_resource_configs", api_resource_configs, dict)
        self.api_resource_configs: dict = api_resource_configs or {}
        self.running_job_id: str | None = None
        self.location = location
        self.num_retries = num_retries
        self.labels = labels
        self.hook = hook

    @deprecated(
        planned_removal_date="November 01, 2024",
        use_instead="airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.create_empty_table",
        category=AirflowProviderDeprecationWarning,
    )
    def create_empty_table(self, *args, **kwargs):
        """
        Create empty table. DEPRECATED.

        Please use :func:`~airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.create_empty_table`
        instead.
        """
        return self.hook.create_empty_table(*args, **kwargs)

    @deprecated(
        planned_removal_date="November 01, 2024",
        use_instead="airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.create_empty_dataset",
        category=AirflowProviderDeprecationWarning,
    )
    def create_empty_dataset(self, *args, **kwargs) -> dict[str, Any]:
        """
        Create empty dataset. DEPRECATED.

        Please use :func:`~airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.create_empty_dataset`
        instead.
        """
        return self.hook.create_empty_dataset(*args, **kwargs)

    @deprecated(
        planned_removal_date="November 01, 2024",
        use_instead="airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_dataset_tables",
        category=AirflowProviderDeprecationWarning,
    )
    def get_dataset_tables(self, *args, **kwargs) -> list[dict[str, Any]]:
        """
        Get dataset tables. DEPRECATED.

        Please use :func:`~airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_dataset_tables`
        instead.
        """
        return self.hook.get_dataset_tables(*args, **kwargs)

    @deprecated(
        planned_removal_date="November 01, 2024",
        use_instead="airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.delete_dataset",
        category=AirflowProviderDeprecationWarning,
    )
    def delete_dataset(self, *args, **kwargs) -> None:
        """
        Delete dataset. DEPRECATED.

        Please use :func:`~airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.delete_dataset`
        instead.
        """
        return self.hook.delete_dataset(*args, **kwargs)

    @deprecated(
        planned_removal_date="November 01, 2024",
        use_instead="airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.create_external_table",
        category=AirflowProviderDeprecationWarning,
    )
    def create_external_table(self, *args, **kwargs):
        """
        Create external table. DEPRECATED.

        Please use :func:`~airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.create_external_table`
        instead.
        """
        return self.hook.create_external_table(*args, **kwargs)

    @deprecated(
        planned_removal_date="November 01, 2024",
        use_instead="airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.patch_table",
        category=AirflowProviderDeprecationWarning,
    )
    def patch_table(self, *args, **kwargs) -> None:
        """
        Patch table. DEPRECATED.

        Please use :func:`~airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.patch_table`
        instead.
        """
        return self.hook.patch_table(*args, **kwargs)

    @deprecated(
        planned_removal_date="November 01, 2024",
        use_instead="airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.insert_all",
        category=AirflowProviderDeprecationWarning,
    )
    def insert_all(self, *args, **kwargs) -> None:
        """
        Insert all. DEPRECATED.

        Please use :func:`~airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.insert_all`
        instead.
        """
        return self.hook.insert_all(*args, **kwargs)

    @deprecated(
        planned_removal_date="November 01, 2024",
        use_instead="airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.update_dataset",
        category=AirflowProviderDeprecationWarning,
    )
    def update_dataset(self, *args, **kwargs) -> dict:
        """
        Update dataset. DEPRECATED.

        Please use :func:`~airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.update_dataset`
        instead.
        """
        return Dataset.to_api_repr(self.hook.update_dataset(*args, **kwargs))

    @deprecated(
        planned_removal_date="November 01, 2024",
        use_instead="airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.patch_dataset",
        category=AirflowProviderDeprecationWarning,
    )
    def patch_dataset(self, *args, **kwargs) -> dict:
        """
        Patch dataset. DEPRECATED.

        Please use :func:`~airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.patch_dataset`
        instead.
        """
        return self.hook.patch_dataset(*args, **kwargs)

    @deprecated(
        planned_removal_date="November 01, 2024",
        use_instead="airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_dataset_tables_list",
        category=AirflowProviderDeprecationWarning,
    )
    def get_dataset_tables_list(self, *args, **kwargs) -> list[dict[str, Any]]:
        """
        Get dataset tables list. DEPRECATED.

        Please use :func:`~airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_dataset_tables_list`
        instead.
        """
        return self.hook.get_dataset_tables_list(*args, **kwargs)

    @deprecated(
        planned_removal_date="November 01, 2024",
        use_instead="airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_datasets_list",
        category=AirflowProviderDeprecationWarning,
    )
    def get_datasets_list(self, *args, **kwargs) -> list | HTTPIterator:
        """
        Get datasets list. DEPRECATED.

        Please use :func:`~airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_datasets_list`
        instead.
        """
        return self.hook.get_datasets_list(*args, **kwargs)

    @deprecated(
        planned_removal_date="November 01, 2024",
        use_instead="airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_dataset",
        category=AirflowProviderDeprecationWarning,
    )
    def get_dataset(self, *args, **kwargs) -> Dataset:
        """
        Get dataset. DEPRECATED.

        Please use :func:`~airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_dataset`
        instead.
        """
        return self.hook.get_dataset(*args, **kwargs)

    @deprecated(
        planned_removal_date="November 01, 2024",
        use_instead="airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.run_grant_dataset_view_access",
        category=AirflowProviderDeprecationWarning,
    )
    def run_grant_dataset_view_access(self, *args, **kwargs) -> dict:
        """
        Grant view access to dataset. DEPRECATED.

        Please use
        :func:`~airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.run_grant_dataset_view_access`
        instead.
        """
        return self.hook.run_grant_dataset_view_access(*args, **kwargs)

    @deprecated(
        planned_removal_date="November 01, 2024",
        use_instead="airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.run_table_upsert",
        category=AirflowProviderDeprecationWarning,
    )
    def run_table_upsert(self, *args, **kwargs) -> dict:
        """
        Upsert table. DEPRECATED.

        Please use :func:`~airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.run_table_upsert`
        instead.
        """
        return self.hook.run_table_upsert(*args, **kwargs)

    @deprecated(
        planned_removal_date="November 01, 2024",
        use_instead="airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.run_table_delete",
        category=AirflowProviderDeprecationWarning,
    )
    def run_table_delete(self, *args, **kwargs) -> None:
        """
        Delete table. DEPRECATED.

        Please use :func:`~airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.run_table_delete`
        instead.
        """
        return self.hook.run_table_delete(*args, **kwargs)

    @deprecated(
        planned_removal_date="November 01, 2024",
        use_instead="airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_tabledata",
        category=AirflowProviderDeprecationWarning,
    )
    def get_tabledata(self, *args, **kwargs) -> list[dict]:
        """
        Get table data. DEPRECATED.

        Please use :func:`~airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_tabledata`
        instead.
        """
        return self.hook.get_tabledata(*args, **kwargs)

    @deprecated(
        planned_removal_date="November 01, 2024",
        use_instead="airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_schema",
        category=AirflowProviderDeprecationWarning,
    )
    def get_schema(self, *args, **kwargs) -> dict:
        """
        Get Schema. DEPRECATED.

        Please use :func:`~airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_schema`
        instead.
        """
        return self.hook.get_schema(*args, **kwargs)

    @deprecated(
        planned_removal_date="November 01, 2024",
        use_instead="airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.poll_job_complete",
        category=AirflowProviderDeprecationWarning,
    )
    def poll_job_complete(self, *args, **kwargs) -> bool:
        """
        Poll for job completion.DEPRECATED.

        Please use :func:`~airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.poll_job_complete`
        instead.
        """
        return self.hook.poll_job_complete(*args, **kwargs)

    @deprecated(
        planned_removal_date="November 01, 2024",
        use_instead="airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.cancel_query",
        category=AirflowProviderDeprecationWarning,
    )
    def cancel_query(self, *args, **kwargs) -> None:
        """
        Cancel query. DEPRECATED.

        Please use :func:`~airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.cancel_query`
        instead.
        """
        return self.hook.cancel_query(*args, **kwargs)  # type: ignore

    @deprecated(
        planned_removal_date="November 01, 2024",
        use_instead="airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.run_with_configuration",
        category=AirflowProviderDeprecationWarning,
    )
    def run_with_configuration(self, *args, **kwargs) -> str:
        """
        Run with configuration. DEPRECATED.

        Please use :func:`~airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.run_with_configuration`
        instead.
        """
        return self.hook.run_with_configuration(*args, **kwargs)

    @deprecated(
        planned_removal_date="November 01, 2024",
        use_instead="airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.run_load",
        category=AirflowProviderDeprecationWarning,
    )
    def run_load(self, *args, **kwargs) -> str:
        """
        Run load. DEPRECATED.

        Please use :func:`~airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.run_load`
        instead.
        """
        return self.hook.run_load(*args, **kwargs)

    @deprecated(
        planned_removal_date="November 01, 2024",
        use_instead="airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.run_copy",
        category=AirflowProviderDeprecationWarning,
    )
    def run_copy(self, *args, **kwargs) -> str:
        """
        Run copy. DEPRECATED.

        Please use :func:`~airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.run_copy`
        instead.
        """
        return self.hook.run_copy(*args, **kwargs)

    @deprecated(
        planned_removal_date="November 01, 2024",
        use_instead="airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.run_extract",
        category=AirflowProviderDeprecationWarning,
    )
    def run_extract(self, *args, **kwargs) -> str | BigQueryJob:
        """
        Run extraction. DEPRECATED.

        Please use :func:`~airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.run_extract`
        instead.
        """
        return self.hook.run_extract(*args, **kwargs)

    @deprecated(
        planned_removal_date="November 01, 2024",
        use_instead="airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.run_query",
        category=AirflowProviderDeprecationWarning,
    )
    def run_query(self, *args, **kwargs) -> str:
        """
        Run query.  DEPRECATED.

        Please use :func:`~airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.run_query`
        instead.
        """
        return self.hook.run_query(*args, **kwargs)


class BigQueryCursor(BigQueryBaseCursor):
    """
    A very basic BigQuery PEP 249 cursor implementation.

    The PyHive PEP 249 implementation was used as a reference:

    https://github.com/dropbox/PyHive/blob/master/pyhive/presto.py
    https://github.com/dropbox/PyHive/blob/master/pyhive/common.py
    """

    def __init__(
        self,
        service: Any,
        project_id: str,
        hook: BigQueryHook,
        use_legacy_sql: bool = True,
        location: str | None = None,
        num_retries: int = 5,
    ) -> None:
        super().__init__(
            service=service,
            project_id=project_id,
            hook=hook,
            use_legacy_sql=use_legacy_sql,
            location=location,
            num_retries=num_retries,
        )
        self.buffersize: int | None = None
        self.page_token: str | None = None
        self.job_id: str | None = None
        self.buffer: list = []
        self.all_pages_loaded: bool = False
        self._description: list = []

    @property
    def description(self) -> list:
        """Return the cursor description."""
        return self._description

    @description.setter
    def description(self, value):
        self._description = value

    def close(self) -> None:
        """By default, do nothing."""

    @property
    def rowcount(self) -> int:
        """By default, return -1 to indicate that this is not supported."""
        return -1

    def execute(self, operation: str, parameters: dict | None = None) -> None:
        """
        Execute a BigQuery query, and update the BigQueryCursor description.

        :param operation: The query to execute.
        :param parameters: Parameters to substitute into the query.
        """
        sql = _bind_parameters(operation, parameters) if parameters else operation
        self.flush_results()
        job = self._run_query(sql)
        self.job_id = job.job_id
        self.location = self.location or job.location
        query_results = self._get_query_result()
        if "schema" in query_results:
            self.description = _format_schema_for_description(query_results["schema"])
        else:
            self.description = []

    def executemany(self, operation: str, seq_of_parameters: list) -> None:
        """
        Execute a BigQuery query multiple times with different parameters.

        :param operation: The query to execute.
        :param seq_of_parameters: List of dictionary parameters to substitute into the
            query.
        """
        for parameters in seq_of_parameters:
            self.execute(operation, parameters)

    def flush_results(self) -> None:
        """Flush results related cursor attributes."""
        self.page_token = None
        self.job_id = None
        self.all_pages_loaded = False
        self.buffer = []

    def fetchone(self) -> list | None:
        """Fetch the next row of a query result set."""
        return self.next()

    def next(self) -> list | None:
        """
        Return the next row from a buffer.

        Helper method for ``fetchone``.

        If the buffer is empty, attempts to paginate through the result set for
        the next page, and load it into the buffer.
        """
        if not self.job_id:
            return None

        if not self.buffer:
            if self.all_pages_loaded:
                return None

            query_results = self._get_query_result()
            if rows := query_results.get("rows"):
                self.page_token = query_results.get("pageToken")
                fields = query_results["schema"]["fields"]
                col_types = [field["type"] for field in fields]

                for dict_row in rows:
                    typed_row = [bq_cast(vs["v"], col_types[idx]) for idx, vs in enumerate(dict_row["f"])]
                    self.buffer.append(typed_row)

                if not self.page_token:
                    self.all_pages_loaded = True

            else:
                # Reset all state since we've exhausted the results.
                self.flush_results()
                return None

        return self.buffer.pop(0)

    def fetchmany(self, size: int | None = None) -> list:
        """
        Fetch the next set of rows of a query result.

        This returns a sequence of sequences (e.g. a list of tuples). An empty
        sequence is returned when no more rows are available.

        The number of rows to fetch per call is specified by the parameter. If
        it is not given, the cursor's arraysize determines the number of rows to
        be fetched.

        This method tries to fetch as many rows as indicated by the size
        parameter. If this is not possible due to the specified number of rows
        not being available, fewer rows may be returned.

        An :py:class:`~pyhive.exc.Error` (or subclass) exception is raised if
        the previous call to :py:meth:`execute` did not produce any result set,
        or no call was issued yet.
        """
        if size is None:
            size = self.arraysize
        result = []
        for _ in range(size):
            one = self.fetchone()
            if one is None:
                break
            result.append(one)
        return result

    def fetchall(self) -> list[list]:
        """
        Fetch all (remaining) rows of a query result.

        A sequence of sequences (e.g. a list of tuples) is returned.
        """
        result = list(iter(self.fetchone, None))
        return result

    def get_arraysize(self) -> int:
        """
        Get number of rows to fetch at a time.

        .. seealso:: :func:`.fetchmany()`
        """
        return self.buffersize or 1

    def set_arraysize(self, arraysize: int) -> None:
        """
        Set the number of rows to fetch at a time.

        .. seealso:: :func:`.fetchmany()`
        """
        self.buffersize = arraysize

    arraysize = property(get_arraysize, set_arraysize)

    def setinputsizes(self, sizes: Any) -> None:
        """Do nothing by default."""

    def setoutputsize(self, size: Any, column: Any = None) -> None:
        """Do nothing by default."""

    def _get_query_result(self) -> dict:
        """Get job query results; data, schema, job type, etc."""
        query_results = (
            self.service.jobs()
            .getQueryResults(
                projectId=self.project_id,
                jobId=self.job_id,
                location=self.location,
                pageToken=self.page_token,
            )
            .execute(num_retries=self.num_retries)
        )

        return query_results

    def _run_query(
        self,
        sql,
        location: str | None = None,
    ) -> BigQueryJob:
        """Run a job query and return the job instance."""
        if not self.project_id:
            raise ValueError("The project_id should be set")

        configuration = self._prepare_query_configuration(sql)
        job = self.hook.insert_job(configuration=configuration, project_id=self.project_id, location=location)

        return job

    def _prepare_query_configuration(
        self,
        sql,
        destination_dataset_table: str | None = None,
        write_disposition: str = "WRITE_EMPTY",
        allow_large_results: bool = False,
        flatten_results: bool | None = None,
        udf_config: list | None = None,
        use_legacy_sql: bool | None = None,
        maximum_billing_tier: int | None = None,
        maximum_bytes_billed: float | None = None,
        create_disposition: str = "CREATE_IF_NEEDED",
        query_params: list | None = None,
        labels: dict | None = None,
        schema_update_options: Iterable | None = None,
        priority: str | None = None,
        time_partitioning: dict | None = None,
        api_resource_configs: dict | None = None,
        cluster_fields: list[str] | None = None,
        encryption_configuration: dict | None = None,
    ):
        """Prepare configuration for query."""
        labels = labels or self.hook.labels
        schema_update_options = list(schema_update_options or [])

        priority = priority or self.hook.priority

        if time_partitioning is None:
            time_partitioning = {}

        if not api_resource_configs:
            api_resource_configs = self.hook.api_resource_configs
        else:
            _validate_value("api_resource_configs", api_resource_configs, dict)

        configuration = deepcopy(api_resource_configs)

        if "query" not in configuration:
            configuration["query"] = {}
        else:
            _validate_value("api_resource_configs['query']", configuration["query"], dict)

        if sql is None and not configuration["query"].get("query", None):
            raise TypeError("`BigQueryBaseCursor.run_query` missing 1 required positional argument: `sql`")

        # BigQuery also allows you to define how you want a table's schema to change
        # as a side effect of a query job
        # for more details:
        #   https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs#configuration.query.schemaUpdateOptions

        allowed_schema_update_options = ["ALLOW_FIELD_ADDITION", "ALLOW_FIELD_RELAXATION"]

        if not set(allowed_schema_update_options).issuperset(set(schema_update_options)):
            raise ValueError(
                f"{schema_update_options} contains invalid schema update options."
                f" Please only use one or more of the following options: {allowed_schema_update_options}"
            )

        if schema_update_options:
            if write_disposition not in ["WRITE_APPEND", "WRITE_TRUNCATE"]:
                raise ValueError(
                    "schema_update_options is only "
                    "allowed if write_disposition is "
                    "'WRITE_APPEND' or 'WRITE_TRUNCATE'."
                )

        if destination_dataset_table:
            destination_project, destination_dataset, destination_table = self.hook.split_tablename(
                table_input=destination_dataset_table, default_project_id=self.project_id
            )

            destination_dataset_table = {  # type: ignore
                "projectId": destination_project,
                "datasetId": destination_dataset,
                "tableId": destination_table,
            }

        if cluster_fields:
            cluster_fields = {"fields": cluster_fields}  # type: ignore

        query_param_list: list[tuple[Any, str, str | bool | None | dict, type | tuple[type]]] = [
            (sql, "query", None, (str,)),
            (priority, "priority", priority, (str,)),
            (use_legacy_sql, "useLegacySql", self.use_legacy_sql, bool),
            (query_params, "queryParameters", None, list),
            (udf_config, "userDefinedFunctionResources", None, list),
            (maximum_billing_tier, "maximumBillingTier", None, int),
            (maximum_bytes_billed, "maximumBytesBilled", None, float),
            (time_partitioning, "timePartitioning", {}, dict),
            (schema_update_options, "schemaUpdateOptions", None, list),
            (destination_dataset_table, "destinationTable", None, dict),
            (cluster_fields, "clustering", None, dict),
        ]

        for param, param_name, param_default, param_type in query_param_list:
            if param_name not in configuration["query"] and param in [None, {}, ()]:
                if param_name == "timePartitioning":
                    param_default = _cleanse_time_partitioning(destination_dataset_table, time_partitioning)
                param = param_default

            if param in [None, {}, ()]:
                continue

            _api_resource_configs_duplication_check(param_name, param, configuration["query"])

            configuration["query"][param_name] = param

            # check valid type of provided param,
            # it last step because we can get param from 2 sources,
            # and first of all need to find it

            _validate_value(param_name, configuration["query"][param_name], param_type)

            if param_name == "schemaUpdateOptions" and param:
                self.log.info("Adding experimental 'schemaUpdateOptions': %s", schema_update_options)

            if param_name == "destinationTable":
                for key in ["projectId", "datasetId", "tableId"]:
                    if key not in configuration["query"]["destinationTable"]:
                        raise ValueError(
                            "Not correct 'destinationTable' in "
                            "api_resource_configs. 'destinationTable' "
                            "must be a dict with {'projectId':'', "
                            "'datasetId':'', 'tableId':''}"
                        )
                else:
                    configuration["query"].update(
                        {
                            "allowLargeResults": allow_large_results,
                            "flattenResults": flatten_results,
                            "writeDisposition": write_disposition,
                            "createDisposition": create_disposition,
                        }
                    )

        if (
            "useLegacySql" in configuration["query"]
            and configuration["query"]["useLegacySql"]
            and "queryParameters" in configuration["query"]
        ):
            raise ValueError("Query parameters are not allowed when using legacy SQL")

        if labels:
            _api_resource_configs_duplication_check("labels", labels, configuration)
            configuration["labels"] = labels

        if encryption_configuration:
            configuration["query"]["destinationEncryptionConfiguration"] = encryption_configuration

        return configuration


def _bind_parameters(operation: str, parameters: dict) -> str:
    """Bind parameters to a SQL query."""
    # inspired by MySQL Python Connector (conversion.py)
    string_parameters = {}  # type dict[str, str]
    for name, value in parameters.items():
        if value is None:
            string_parameters[name] = "NULL"
        elif isinstance(value, str):
            string_parameters[name] = "'" + _escape(value) + "'"
        else:
            string_parameters[name] = str(value)
    return operation % string_parameters


def _escape(s: str) -> str:
    """Escape special characters in a SQL query string."""
    e = s
    e = e.replace("\\", "\\\\")
    e = e.replace("\n", "\\n")
    e = e.replace("\r", "\\r")
    e = e.replace("'", "\\'")
    e = e.replace('"', '\\"')
    return e


@deprecated(
    planned_removal_date="April 01, 2025",
    use_instead="airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.split_tablename",
    category=AirflowProviderDeprecationWarning,
)
def split_tablename(
    table_input: str, default_project_id: str, var_name: str | None = None
) -> tuple[str, str, str]:
    if "." not in table_input:
        raise ValueError(f"Expected table name in the format of <dataset>.<table>. Got: {table_input}")

    if not default_project_id:
        raise ValueError("INTERNAL: No default project is specified")

    def var_print(var_name):
        if var_name is None:
            return ""
        else:
            return f"Format exception for {var_name}: "

    if table_input.count(".") + table_input.count(":") > 3:
        raise ValueError(f"{var_print(var_name)}Use either : or . to specify project got {table_input}")
    cmpt = table_input.rsplit(":", 1)
    project_id = None
    rest = table_input
    if len(cmpt) == 1:
        project_id = None
        rest = cmpt[0]
    elif len(cmpt) == 2 and cmpt[0].count(":") <= 1:
        if cmpt[-1].count(".") != 2:
            project_id = cmpt[0]
            rest = cmpt[1]
    else:
        raise ValueError(
            f"{var_print(var_name)}Expect format of (<project:)<dataset>.<table>, got {table_input}"
        )

    cmpt = rest.split(".")
    if len(cmpt) == 3:
        if project_id:
            raise ValueError(f"{var_print(var_name)}Use either : or . to specify project")
        project_id = cmpt[0]
        dataset_id = cmpt[1]
        table_id = cmpt[2]

    elif len(cmpt) == 2:
        dataset_id = cmpt[0]
        table_id = cmpt[1]
    else:
        raise ValueError(
            f"{var_print(var_name)}Expect format of (<project.|<project:)<dataset>.<table>, got {table_input}"
        )

    # Exclude partition from the table name
    table_id = table_id.split("$")[0]

    if project_id is None:
        if var_name is not None:
            log.info(
                'Project is not included in %s: %s; using project "%s"',
                var_name,
                table_input,
                default_project_id,
            )
        project_id = default_project_id

    return project_id, dataset_id, table_id


def _cleanse_time_partitioning(
    destination_dataset_table: str | None, time_partitioning_in: dict | None
) -> dict:  # if it is a partitioned table ($ is in the table name) add partition load option
    if time_partitioning_in is None:
        time_partitioning_in = {}
    time_partitioning_out = {}
    if destination_dataset_table and "$" in destination_dataset_table:
        time_partitioning_out["type"] = "DAY"
    time_partitioning_out.update(time_partitioning_in)
    return time_partitioning_out


def _validate_value(key: Any, value: Any, expected_type: type | tuple[type]) -> None:
    """Check expected type and raise error if type is not correct."""
    if not isinstance(value, expected_type):
        raise TypeError(f"{key} argument must have a type {expected_type} not {type(value)}")


def _api_resource_configs_duplication_check(
    key: Any, value: Any, config_dict: dict, config_dict_name="api_resource_configs"
) -> None:
    if key in config_dict and value != config_dict[key]:
        raise ValueError(
            f"Values of {key} param are duplicated. "
            f"{config_dict_name} contained {key} param "
            f"in `query` config and {key} was also provided "
            "with arg to run_query() method. Please remove duplicates."
        )


def _validate_src_fmt_configs(
    source_format: str,
    src_fmt_configs: dict,
    valid_configs: list[str],
    backward_compatibility_configs: dict | None = None,
) -> dict:
    """
    Validate ``src_fmt_configs`` against a valid config for the source format.

    Adds the backward compatibility config to ``src_fmt_configs``.

    :param source_format: File format to export.
    :param src_fmt_configs: Configure optional fields specific to the source format.
    :param valid_configs: Valid configuration specific to the source format
    :param backward_compatibility_configs: The top-level params for backward-compatibility
    """
    if backward_compatibility_configs is None:
        backward_compatibility_configs = {}

    for k, v in backward_compatibility_configs.items():
        if k not in src_fmt_configs and k in valid_configs:
            src_fmt_configs[k] = v

    for k in src_fmt_configs:
        if k not in valid_configs:
            raise ValueError(f"{k} is not a valid src_fmt_configs for type {source_format}.")

    return src_fmt_configs


def _format_schema_for_description(schema: dict) -> list:
    """
    Reformat the schema to match cursor description standard.

    The description should be a tuple of 7 elemenbts: name, type, display_size,
    internal_size, precision, scale, null_ok.
    """
    description = []
    for field in schema["fields"]:
        mode = field.get("mode", "NULLABLE")
        field_description = (
            field["name"],
            field["type"],
            None,
            None,
            None,
            None,
            mode == "NULLABLE",
        )
        description.append(field_description)
    return description


class BigQueryAsyncHook(GoogleBaseAsyncHook):
    """Uses gcloud-aio library to retrieve Job details."""

    sync_hook_class = BigQueryHook

    def __init__(
        self,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ):
        super().__init__(
            gcp_conn_id=gcp_conn_id,
            impersonation_chain=impersonation_chain,
            **kwargs,
        )

    async def get_job_instance(
        self, project_id: str | None, job_id: str | None, session: ClientSession
    ) -> Job:
        """Get the specified job resource by job ID and project ID."""
        token = await self.get_token(session=session)
        return Job(
            job_id=job_id,
            project=project_id,
            token=token,
            session=cast(Session, session),
        )

    async def _get_job(
        self, job_id: str | None, project_id: str = PROVIDE_PROJECT_ID, location: str | None = None
    ) -> BigQueryJob | UnknownJob:
        """
        Get BigQuery job by its ID, project ID and location.

        WARNING.
        This is a temporary workaround  for issues below, and it's not intended to be used elsewhere!
            https://github.com/apache/airflow/issues/35833
            https://github.com/talkiq/gcloud-aio/issues/584

        This method was developed, because neither the `google-cloud-bigquery` nor the `gcloud-aio-bigquery`
        provides asynchronous access to a BigQuery jobs with location parameter. That's why this method wraps
        synchronous client call with the event loop's run_in_executor() method.

        This workaround must be deleted along with the method _get_job_sync() and replaced by more robust and
        cleaner solution in one of two cases:
            1. The `google-cloud-bigquery` library provides async client with get_job method, that supports
            optional parameter `location`
            2. The `gcloud-aio-bigquery` library supports the `location` parameter in get_job() method.
        """
        loop = asyncio.get_event_loop()
        job = await loop.run_in_executor(None, self._get_job_sync, job_id, project_id, location)
        return job

    def _get_job_sync(self, job_id, project_id, location):
        """
        Get BigQuery job by its ID, project ID and location synchronously.

        WARNING
        This is a temporary workaround  for issues below, and it's not intended to be used elsewhere!
            https://github.com/apache/airflow/issues/35833
            https://github.com/talkiq/gcloud-aio/issues/584

        This workaround must be deleted along with the method _get_job() and replaced by more robust and
        cleaner solution in one of two cases:
            1. The `google-cloud-bigquery` library provides async client with get_job method, that supports
            optional parameter `location`
            2. The `gcloud-aio-bigquery` library supports the `location` parameter in get_job() method.
        """
        hook = BigQueryHook(**self._hook_kwargs)
        return hook.get_job(job_id=job_id, project_id=project_id, location=location)

    async def get_job_status(
        self, job_id: str | None, project_id: str = PROVIDE_PROJECT_ID, location: str | None = None
    ) -> dict[str, str]:
        job = await self._get_job(job_id=job_id, project_id=project_id, location=location)
        if job.state == "DONE":
            if job.error_result:
                return {"status": "error", "message": job.error_result["message"]}
            return {"status": "success", "message": "Job completed"}
        return {"status": str(job.state).lower(), "message": "Job running"}

    async def get_job_output(
        self,
        job_id: str | None,
        project_id: str = PROVIDE_PROJECT_ID,
    ) -> dict[str, Any]:
        """Get the BigQuery job output for a given job ID asynchronously."""
        async with ClientSession() as session:
            self.log.info("Executing get_job_output..")
            job_client = await self.get_job_instance(project_id, job_id, session)
            job_query_response = await job_client.get_query_results(cast(Session, session))
            return job_query_response

    async def create_job_for_partition_get(
        self,
        dataset_id: str | None,
        table_id: str | None = None,
        project_id: str = PROVIDE_PROJECT_ID,
    ):
        """Create a new job and get the job_id using gcloud-aio."""
        async with ClientSession() as session:
            self.log.info("Executing create_job..")
            job_client = await self.get_job_instance(project_id, "", session)

            query_request = {
                "query": "SELECT partition_id "
                f"FROM `{project_id}.{dataset_id}.INFORMATION_SCHEMA.PARTITIONS`"
                + (f" WHERE table_id={table_id}" if table_id else ""),
                "useLegacySql": False,
            }
            job_query_resp = await job_client.query(query_request, cast(Session, session))
            return job_query_resp["jobReference"]["jobId"]

    async def cancel_job(self, job_id: str, project_id: str | None, location: str | None) -> None:
        """
        Cancel a BigQuery job.

        :param job_id: ID of the job to cancel.
        :param project_id: Google Cloud Project where the job was running.
        :param location: Location where the job was running.
        """
        async with ClientSession() as session:
            token = await self.get_token(session=session)
            job = Job(job_id=job_id, project=project_id, location=location, token=token, session=session)  # type: ignore[arg-type]

            self.log.info(
                "Attempting to cancel BigQuery job: %s in project: %s, location: %s",
                job_id,
                project_id,
                location,
            )
            try:
                await job.cancel()
                self.log.info("Job %s cancellation requested.", job_id)
            except Exception as e:
                self.log.error("Failed to cancel BigQuery job %s: %s", job_id, str(e))
                raise

    # TODO: Convert get_records into an async method
    def get_records(
        self,
        query_results: dict[str, Any],
        as_dict: bool = False,
        selected_fields: str | list[str] | None = None,
    ) -> list[Any]:
        """
        Convert a response from BigQuery to records.

        :param query_results: the results from a SQL query
        :param as_dict: if True returns the result as a list of dictionaries, otherwise as list of lists.
        :param selected_fields:
        """
        if isinstance(selected_fields, str):
            selected_fields = selected_fields.split(",")
        buffer: list[Any] = []
        if rows := query_results.get("rows"):
            fields = query_results["schema"]["fields"]
            fields = [field for field in fields if not selected_fields or field["name"] in selected_fields]
            fields_names = [field["name"] for field in fields]
            col_types = [field["type"] for field in fields]
            for dict_row in rows:
                typed_row = [bq_cast(vs["v"], col_type) for vs, col_type in zip(dict_row["f"], col_types)]
                if as_dict:
                    typed_row_dict = dict(zip(fields_names, typed_row))
                    buffer.append(typed_row_dict)
                else:
                    buffer.append(typed_row)
        return buffer

    def value_check(
        self,
        sql: str,
        pass_value: Any,
        records: list[Any],
        tolerance: float | None = None,
    ) -> None:
        """
        Match a single query resulting row and tolerance with pass_value.

        :raise AirflowException: if matching fails
        """
        if not records:
            raise AirflowException("The query returned None")
        pass_value_conv = self._convert_to_float_if_possible(pass_value)
        is_numeric_value_check = isinstance(pass_value_conv, float)

        error_msg = (
            f"Test failed.\n"
            f"Pass value:{pass_value_conv}\n"
            f"Tolerance:{f'{tolerance:.1%}' if tolerance else None}\n"
            f"Query:\n{sql}\n"
            f"Results:\n{records!s}"
        )

        if not is_numeric_value_check:
            tests = [str(record) == pass_value_conv for record in records]
        else:
            try:
                numeric_records = [float(record) for record in records]
            except (ValueError, TypeError):
                raise AirflowException(f"Converting a result to float failed.\n{error_msg}")
            tests = self._get_numeric_matches(numeric_records, pass_value_conv, tolerance)

        if not all(tests):
            raise AirflowException(error_msg)

    @staticmethod
    def _get_numeric_matches(
        records: list[float], pass_value: Any, tolerance: float | None = None
    ) -> list[bool]:
        """
        Match numeric pass_value, tolerance with records value.

        :param records: List of value to match against
        :param pass_value: Expected value
        :param tolerance: Allowed tolerance for match to succeed
        """
        if tolerance:
            return [
                pass_value * (1 - tolerance) <= record <= pass_value * (1 + tolerance) for record in records
            ]

        return [record == pass_value for record in records]

    @staticmethod
    def _convert_to_float_if_possible(s: Any) -> Any:
        """
        Convert a string to a numeric value if appropriate.

        :param s: the string to be converted
        """
        try:
            return float(s)
        except (ValueError, TypeError):
            return s

    def interval_check(
        self,
        row1: str | None,
        row2: str | None,
        metrics_thresholds: dict[str, Any],
        ignore_zero: bool,
        ratio_formula: str,
    ) -> None:
        """
        Check values of metrics (SQL expressions) are within a certain tolerance.

        :param row1: first resulting row of a query execution job for first SQL query
        :param row2: first resulting row of a query execution job for second SQL query
        :param metrics_thresholds: a dictionary of ratios indexed by metrics, for
            example 'COUNT(*)': 1.5 would require a 50 percent or less difference
            between the current day, and the prior days_back.
        :param ignore_zero: whether we should ignore zero metrics
        :param ratio_formula: which formula to use to compute the ratio between
            the two metrics. Assuming cur is the metric of today and ref is
            the metric to today - days_back.
            max_over_min: computes max(cur, ref) / min(cur, ref)
            relative_diff: computes abs(cur-ref) / ref
        """
        if not row2:
            raise AirflowException("The second SQL query returned None")
        if not row1:
            raise AirflowException("The first SQL query returned None")

        ratio_formulas = {
            "max_over_min": lambda cur, ref: max(cur, ref) / min(cur, ref),
            "relative_diff": lambda cur, ref: abs(cur - ref) / ref,
        }

        metrics_sorted = sorted(metrics_thresholds.keys())

        current = dict(zip(metrics_sorted, row1))
        reference = dict(zip(metrics_sorted, row2))
        ratios: dict[str, Any] = {}
        test_results: dict[str, Any] = {}

        for metric in metrics_sorted:
            cur = float(current[metric])
            ref = float(reference[metric])
            threshold = float(metrics_thresholds[metric])
            if cur == 0 or ref == 0:
                ratios[metric] = None
                test_results[metric] = ignore_zero
            else:
                ratios[metric] = ratio_formulas[ratio_formula](
                    float(current[metric]), float(reference[metric])
                )
                test_results[metric] = float(ratios[metric]) < threshold

            self.log.info(
                (
                    "Current metric for %s: %s\n"
                    "Past metric for %s: %s\n"
                    "Ratio for %s: %s\n"
                    "Threshold: %s\n"
                ),
                metric,
                cur,
                metric,
                ref,
                metric,
                ratios[metric],
                threshold,
            )

        if not all(test_results.values()):
            failed_tests = [metric for metric, value in test_results.items() if not value]
            self.log.warning(
                "The following %s tests out of %s failed:",
                len(failed_tests),
                len(metrics_sorted),
            )
            for k in failed_tests:
                self.log.warning(
                    "'%s' check failed. %s is above %s",
                    k,
                    ratios[k],
                    metrics_thresholds[k],
                )
            raise AirflowException(f"The following tests have failed:\n {', '.join(sorted(failed_tests))}")

        self.log.info("All tests have passed")


class BigQueryTableAsyncHook(GoogleBaseAsyncHook):
    """Async hook for BigQuery Table."""

    sync_hook_class = BigQueryHook

    def __init__(
        self,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ):
        super().__init__(
            gcp_conn_id=gcp_conn_id,
            impersonation_chain=impersonation_chain,
            **kwargs,
        )

    async def get_table_client(
        self, dataset: str, table_id: str, project_id: str, session: ClientSession
    ) -> Table_async:
        """
        Get a Google Big Query Table object.

        :param dataset:  The name of the dataset in which to look for the table storage bucket.
        :param table_id: The name of the table to check the existence of.
        :param project_id: The Google cloud project in which to look for the table.
            The connection supplied to the hook must provide
            access to the specified project.
        :param session: aiohttp ClientSession
        """
        token = await self.get_token(session=session)
        return Table_async(
            dataset_name=dataset,
            table_name=table_id,
            project=project_id,
            token=token,
            session=cast(Session, session),
        )
