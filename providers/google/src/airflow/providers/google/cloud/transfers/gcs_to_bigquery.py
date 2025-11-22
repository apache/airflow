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
"""This module contains a Google Cloud Storage to BigQuery operator."""

from __future__ import annotations

import json
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

from google.api_core.exceptions import BadRequest, Conflict
from google.cloud.bigquery import (
    DEFAULT_RETRY,
    CopyJob,
    ExternalConfig,
    ExtractJob,
    LoadJob,
    QueryJob,
    SchemaField,
    UnknownJob,
)
from google.cloud.bigquery.table import EncryptionConfiguration, Table, TableReference

from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook, BigQueryJob
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.links.bigquery import BigQueryTableLink
from airflow.providers.google.cloud.triggers.bigquery import BigQueryInsertJobTrigger
from airflow.providers.google.common.hooks.base_google import PROVIDE_PROJECT_ID
from airflow.providers.google.version_compat import BaseOperator
from airflow.utils.helpers import merge_dicts

if TYPE_CHECKING:
    from google.api_core.retry import Retry

    from airflow.providers.common.compat.sdk import Context

ALLOWED_FORMATS = [
    "CSV",
    "NEWLINE_DELIMITED_JSON",
    "AVRO",
    "GOOGLE_SHEETS",
    "DATASTORE_BACKUP",
    "PARQUET",
    "ORC",
]


class GCSToBigQueryOperator(BaseOperator):
    """
    Loads files from Google Cloud Storage into BigQuery.

    The schema to be used for the BigQuery table may be specified in one of
    two ways. You may either directly pass the schema fields in, or you may
    point the operator to a Google Cloud Storage object name. The object in
    Google Cloud Storage must be a JSON file with the schema fields in it.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GCSToBigQueryOperator`

    :param bucket: The bucket to load from. (templated)
    :param source_objects: String or List of Google Cloud Storage URIs to load from. (templated)
        If source_format is 'DATASTORE_BACKUP', the list must only contain a single URI.
    :param destination_project_dataset_table: The dotted
        ``(<project>.|<project>:)<dataset>.<table>`` BigQuery table to load data into.
        If ``<project>`` is not included, project will be the project defined in
        the connection json. (templated)
    :param schema_fields: If set, the schema field list as defined here:
        https://cloud.google.com/bigquery/docs/reference/v2/jobs#configuration.load
        Should not be set when source_format is 'DATASTORE_BACKUP'.
        Parameter must be defined if 'schema_object' is null and autodetect is False.
    :param schema_object: If set, a GCS object path pointing to a .json file that
        contains the schema for the table. (templated)
        Parameter must be defined if 'schema_fields' is null and autodetect is False.
    :param schema_object_bucket: [Optional] If set, the GCS bucket where the schema object
        template is stored. (templated) (Default: the value of ``bucket``)
    :param source_format: File format to export.
    :param compression: [Optional] The compression type of the data source.
        Possible values include GZIP and NONE.
        The default value is NONE.
        This setting is ignored for Google Cloud Bigtable,
        Google Cloud Datastore backups and Avro formats.
    :param create_disposition: The create disposition if the table doesn't exist.
    :param skip_leading_rows: The number of rows at the top of a CSV file that BigQuery
        will skip when loading the data.
        When autodetect is on, the behavior is the following:
        skip_leading_rows unspecified - Autodetect tries to detect headers in the first row.
        If they are not detected, the row is read as data. Otherwise, data is read starting
        from the second row.
        skip_leading_rows is 0 - Instructs autodetect that there are no headers and data
        should be read starting from the first row.
        skip_leading_rows = N > 0 - Autodetect skips N-1 rows and tries to detect headers
        in row N. If headers are not detected, row N is just skipped. Otherwise, row N is
        used to extract column names for the detected schema.
        Default value set to None so that autodetect option can detect schema fields.
    :param write_disposition: The write disposition if the table already exists.
    :param field_delimiter: The delimiter to use when loading from a CSV.
    :param max_bad_records: The maximum number of bad records that BigQuery can
        ignore when running the job.
    :param quote_character: The value that is used to quote data sections in a CSV file.
    :param ignore_unknown_values: [Optional] Indicates if BigQuery should allow
        extra values that are not represented in the table schema.
        If true, the extra values are ignored. If false, records with extra columns
        are treated as bad records, and if there are too many bad records, an
        invalid error is returned in the job result.
    :param allow_quoted_newlines: Whether to allow quoted newlines (true) or not (false).
    :param allow_jagged_rows: Accept rows that are missing trailing optional columns.
        The missing values are treated as nulls. If false, records with missing trailing
        columns are treated as bad records, and if there are too many bad records, an
        invalid error is returned in the job result. Only applicable to CSV, ignored
        for other formats.
    :param encoding: The character encoding of the data. See:
        https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs#configuration.query.tableDefinitions.(key).csvOptions.encoding
        https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#externalDataConfiguration.csvOptions.encoding
    :param max_id_key: If set, the name of a column in the BigQuery table
        that's to be loaded. This will be used to select the MAX value from
        BigQuery after the load occurs. The results will be returned by the
        execute() command, which in turn gets stored in XCom for future
        operators to use. This can be helpful with incremental loads--during
        future executions, you can pick up from the max ID.
    :param schema_update_options: Allows the schema of the destination
        table to be updated as a side effect of the load job.
    :param src_fmt_configs: configure optional fields specific to the source format
    :param external_table: Flag to specify if the destination table should be
        a BigQuery external table. Default Value is False.
    :param time_partitioning: configure optional time partitioning fields i.e.
        partition by field, type and  expiration as per API specifications.
        Note that 'field' is not available in concurrency with
        dataset.table$partition.
        Ignored if 'range_partitioning' is set.
    :param range_partitioning: configure optional range partitioning fields i.e.
        partition by field and integer interval as per API specifications.
    :param cluster_fields: Request that the result of this load be stored sorted
        by one or more columns. BigQuery supports clustering for both partitioned and
        non-partitioned tables. The order of columns given determines the sort order.
        Not applicable for external tables.
    :param autodetect: [Optional] Indicates if we should automatically infer the
        options and schema for CSV and JSON sources. (Default: ``True``).
        Parameter must be set to True if 'schema_fields' and 'schema_object' are undefined.
        It is suggested to set to True if table are create outside of Airflow.
        If autodetect is None and no schema is provided (neither via schema_fields
        nor a schema_object), assume the table already exists.
    :param encryption_configuration: [Optional] Custom encryption configuration (e.g., Cloud KMS keys).

        .. code-block:: python

            encryption_configuration = {
                "kmsKeyName": "projects/testp/locations/us/keyRings/test-kr/cryptoKeys/test-key",
            }
    :param location: [Optional] The geographic location of the job. Required except for US and EU.
        See details at https://cloud.google.com/bigquery/docs/locations#specifying_your_location
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :param labels: [Optional] Labels for the BiqQuery table.
    :param description: [Optional] Description for the BigQuery table. This will only be used if the
        destination table is newly created. If the table already exists and a value different than the
        current description is provided, the job will fail.
    :param deferrable: Run operator in the deferrable mode
    :param force_delete: Force the destination table to be deleted if it already exists.
    """

    template_fields: Sequence[str] = (
        "bucket",
        "source_objects",
        "schema_object",
        "schema_object_bucket",
        "destination_project_dataset_table",
        "impersonation_chain",
        "src_fmt_configs",
    )
    template_ext: Sequence[str] = (".sql",)
    ui_color = "#f0eee4"
    operator_extra_links = (BigQueryTableLink(),)

    def __init__(
        self,
        *,
        bucket,
        source_objects,
        destination_project_dataset_table,
        schema_fields=None,
        schema_object=None,
        schema_object_bucket=None,
        source_format="CSV",
        compression="NONE",
        create_disposition="CREATE_IF_NEEDED",
        skip_leading_rows=None,
        write_disposition="WRITE_EMPTY",
        field_delimiter=",",
        max_bad_records=0,
        quote_character=None,
        ignore_unknown_values=False,
        allow_quoted_newlines=False,
        allow_jagged_rows=False,
        encoding="UTF-8",
        max_id_key=None,
        gcp_conn_id="google_cloud_default",
        schema_update_options=(),
        src_fmt_configs=None,
        external_table=False,
        time_partitioning=None,
        range_partitioning=None,
        cluster_fields=None,
        autodetect=True,
        encryption_configuration=None,
        location=None,
        impersonation_chain: str | Sequence[str] | None = None,
        labels=None,
        description=None,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        result_retry: Retry = DEFAULT_RETRY,
        result_timeout: float | None = None,
        cancel_on_kill: bool = True,
        job_id: str | None = None,
        force_rerun: bool = True,
        reattach_states: set[str] | None = None,
        project_id: str = PROVIDE_PROJECT_ID,
        force_delete: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.hook: BigQueryHook | None = None
        self.configuration: dict[str, Any] = {}

        # GCS config
        if src_fmt_configs is None:
            src_fmt_configs = {}
        if time_partitioning is None:
            time_partitioning = {}
        if range_partitioning is None:
            range_partitioning = {}
        if range_partitioning and time_partitioning:
            raise ValueError("Only one of time_partitioning or range_partitioning can be set.")
        self.bucket = bucket
        self.source_objects = source_objects
        self.schema_object = schema_object

        if schema_object_bucket is None:
            schema_object_bucket = bucket
        self.schema_object_bucket = schema_object_bucket

        # BQ config
        self.destination_project_dataset_table = destination_project_dataset_table
        self.project_id = project_id
        self.schema_fields = schema_fields
        if source_format.upper() not in ALLOWED_FORMATS:
            raise ValueError(
                f"{source_format} is not a valid source format. "
                f"Please use one of the following types: {ALLOWED_FORMATS}."
            )
        self.source_format = source_format.upper()
        self.compression = compression
        self.create_disposition = create_disposition
        self.skip_leading_rows = skip_leading_rows
        self.write_disposition = write_disposition
        self.field_delimiter = field_delimiter
        self.max_bad_records = max_bad_records
        self.quote_character = quote_character
        self.ignore_unknown_values = ignore_unknown_values
        self.allow_quoted_newlines = allow_quoted_newlines
        self.allow_jagged_rows = allow_jagged_rows
        self.external_table = external_table
        self.encoding = encoding

        self.max_id_key = max_id_key
        self.gcp_conn_id = gcp_conn_id

        self.schema_update_options = schema_update_options
        self.src_fmt_configs = src_fmt_configs
        self.time_partitioning = time_partitioning
        self.range_partitioning = range_partitioning
        self.cluster_fields = cluster_fields
        self.autodetect = autodetect
        self.encryption_configuration = encryption_configuration
        self.location = location
        self.impersonation_chain = impersonation_chain

        self.labels = labels
        self.description = description

        self.job_id = job_id
        self.deferrable = deferrable
        self.result_retry = result_retry
        self.result_timeout = result_timeout
        self.force_rerun = force_rerun
        self.reattach_states: set[str] = reattach_states or set()
        self.cancel_on_kill = cancel_on_kill
        self.force_delete = force_delete

        self.source_uris: list[str] = []

    def _submit_job(
        self,
        hook: BigQueryHook,
        job_id: str,
    ) -> BigQueryJob:
        # Submit a new job without waiting for it to complete.
        return hook.insert_job(
            configuration=self.configuration,
            project_id=self.project_id or hook.project_id,
            location=self.location,
            job_id=job_id,
            timeout=self.result_timeout,
            retry=self.result_retry,
            nowait=True,
        )

    @staticmethod
    def _handle_job_error(job: BigQueryJob | UnknownJob) -> None:
        if job.error_result:
            raise AirflowException(f"BigQuery job {job.job_id} failed: {job.error_result}")

    def execute(self, context: Context):
        hook = BigQueryHook(
            gcp_conn_id=self.gcp_conn_id,
            location=self.location,
            impersonation_chain=self.impersonation_chain,
        )
        self.hook = hook
        self.source_format = self.source_format.upper()

        job_id = self.hook.generate_job_id(
            job_id=self.job_id,
            dag_id=self.dag_id,
            task_id=self.task_id,
            logical_date=None,
            configuration=self.configuration,
            run_after=hook.get_run_after_or_logical_date(context),
            force_rerun=self.force_rerun,
        )

        self.source_objects = (
            self.source_objects if isinstance(self.source_objects, list) else [self.source_objects]
        )
        self.source_uris = [f"gs://{self.bucket}/{source_object}" for source_object in self.source_objects]

        if not self.schema_fields:
            # Check for self.autodetect explicitly False. self.autodetect equal to None
            # entails we do not want to detect schema from files. Instead, it means we
            # rely on an already existing table's schema
            if not self.schema_object and self.autodetect is False:
                raise AirflowException(
                    "Table schema was not found. Neither schema object nor schema fields were specified"
                )
            if self.schema_object and self.source_format != "DATASTORE_BACKUP":
                gcs_hook = GCSHook(
                    gcp_conn_id=self.gcp_conn_id,
                    impersonation_chain=self.impersonation_chain,
                )
                self.schema_fields = json.loads(
                    gcs_hook.download(self.schema_object_bucket, self.schema_object).decode("utf-8")
                )
                self.log.info("Loaded fields from schema object: %s", self.schema_fields)
            else:
                self.schema_fields = None

        if self.external_table:
            self.log.info("Creating a new BigQuery table for storing data...")
            table_obj_api_repr = self._create_external_table()

            BigQueryTableLink.persist(
                context=context,
                dataset_id=table_obj_api_repr["tableReference"]["datasetId"],
                project_id=table_obj_api_repr["tableReference"]["projectId"],
                table_id=table_obj_api_repr["tableReference"]["tableId"],
            )
            if self.max_id_key:
                max_id = self._find_max_value_in_column()
                return max_id
        else:
            if self.force_delete:
                self.log.info("Deleting table %s", self.destination_project_dataset_table)
                hook.delete_table(table_id=self.destination_project_dataset_table)
            else:
                self.log.info("Using existing BigQuery table for storing data...")
            self.configuration = self._use_existing_table()

            try:
                self.log.info("Executing: %s", self.configuration)
                job: BigQueryJob | UnknownJob = self._submit_job(self.hook, job_id)
            except Conflict:
                # If the job already exists retrieve it
                job = self.hook.get_job(
                    project_id=self.project_id or self.hook.project_id,
                    location=self.location,
                    job_id=job_id,
                )
                if job.state not in self.reattach_states:
                    # Same job configuration, so we need force_rerun
                    raise AirflowException(
                        f"Job with id: {job_id} already exists and is in {job.state} state. If you "
                        f"want to force rerun it consider setting `force_rerun=True`."
                        f"Or, if you want to reattach in this scenario add {job.state} to `reattach_states`"
                    )
                # Job already reached state DONE
                if job.state == "DONE":
                    raise AirflowException("Job is already in state DONE. Can not reattach to this job.")

                # We are reattaching to a job
                self.log.info("Reattaching to existing Job in state %s", job.state)
                self._handle_job_error(job)

            job_types = {
                LoadJob._JOB_TYPE: ["sourceTable", "destinationTable"],
                CopyJob._JOB_TYPE: ["sourceTable", "destinationTable"],
                ExtractJob._JOB_TYPE: ["sourceTable"],
                QueryJob._JOB_TYPE: ["destinationTable"],
            }

            if self.hook.project_id:
                for job_type, tables_prop in job_types.items():
                    job_configuration = job.to_api_repr()["configuration"]
                    if job_type in job_configuration:
                        for table_prop in tables_prop:
                            if table_prop in job_configuration[job_type]:
                                table = job_configuration[job_type][table_prop]
                                persist_kwargs = {
                                    "context": context,
                                    "table_id": table,
                                }
                                if not isinstance(table, str):
                                    persist_kwargs["table_id"] = table["tableId"]
                                    persist_kwargs["dataset_id"] = table["datasetId"]
                                    persist_kwargs["project_id"] = table["projectId"]
                                BigQueryTableLink.persist(**persist_kwargs)

            self.job_id = job.job_id
            context["ti"].xcom_push(key="job_id", value=self.job_id)
            if self.deferrable:
                self.defer(
                    timeout=self.execution_timeout,
                    trigger=BigQueryInsertJobTrigger(
                        conn_id=self.gcp_conn_id,
                        job_id=self.job_id,
                        project_id=self.project_id or self.hook.project_id,
                        location=self.location or self.hook.location,
                        impersonation_chain=self.impersonation_chain,
                    ),
                    method_name="execute_complete",
                )
            else:
                job.result(timeout=self.result_timeout, retry=self.result_retry)
                self._handle_job_error(job)
                if self.max_id_key:
                    return self._find_max_value_in_column()

    def execute_complete(self, context: Context, event: dict[str, Any]):
        """
        Return immediately and relies on trigger to throw a success event. Callback for the trigger.

        Relies on trigger to throw an exception, otherwise it assumes execution was successful.
        """
        if event["status"] == "error":
            raise AirflowException(event["message"])
        self.log.info(
            "%s completed with response %s ",
            self.task_id,
            event["message"],
        )
        # Save job_id as an attribute to be later used by listeners
        self.job_id = event.get("job_id")
        return self._find_max_value_in_column()

    def _find_max_value_in_column(self):
        hook = BigQueryHook(
            gcp_conn_id=self.gcp_conn_id,
            location=self.location,
            impersonation_chain=self.impersonation_chain,
        )
        if self.max_id_key:
            self.log.info("Selecting the MAX value from BigQuery column %r...", self.max_id_key)
            select_command = (
                f"SELECT MAX({self.max_id_key}) AS max_value FROM {self.destination_project_dataset_table}"
            )
            self.configuration = {
                "query": {
                    "query": select_command,
                    "useLegacySql": False,
                    "schemaUpdateOptions": [],
                }
            }
            try:
                job_id = hook.insert_job(
                    configuration=self.configuration, project_id=self.project_id or hook.project_id
                )
                rows = list(hook.get_job(job_id=job_id, location=self.location).result())
            except BadRequest as e:
                if "Unrecognized name:" in e.message:
                    raise AirflowException(
                        f"Could not determine MAX value in column {self.max_id_key} "
                        f"since the default value of 'string_field_n' was set by BQ"
                    )
                raise AirflowException(e.message)
            if rows:
                for row in rows:
                    max_id = row[0] if row[0] else 0
                    self.log.info(
                        "Loaded BQ data with MAX value of column %s.%s: %s",
                        self.destination_project_dataset_table,
                        self.max_id_key,
                        max_id,
                    )
                    return str(max_id)
            else:
                raise RuntimeError(f"The {select_command} returned no rows!")

    def _create_external_table(self):
        external_config_api_repr = {
            "autodetect": self.autodetect,
            "sourceFormat": self.source_format,
            "sourceUris": self.source_uris,
            "compression": self.compression.upper(),
            "ignoreUnknownValues": self.ignore_unknown_values,
        }
        # if following fields are not specified in src_fmt_configs,
        # honor the top-level params for backward-compatibility
        backward_compatibility_configs = {
            "skipLeadingRows": self.skip_leading_rows,
            "fieldDelimiter": self.field_delimiter,
            "quote": self.quote_character,
            "allowQuotedNewlines": self.allow_quoted_newlines,
            "allowJaggedRows": self.allow_jagged_rows,
            "encoding": self.encoding,
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
                "preserveAsciiControlCharacters",
                "columnNameCharacterMap",
            ],
            "googleSheetsOptions": ["skipLeadingRows"],
        }
        if self.source_format in src_fmt_to_param_mapping:
            valid_configs = src_fmt_to_configs_mapping[src_fmt_to_param_mapping[self.source_format]]
            self.src_fmt_configs = self._validate_src_fmt_configs(
                self.source_format, self.src_fmt_configs, valid_configs, backward_compatibility_configs
            )
            external_config_api_repr[src_fmt_to_param_mapping[self.source_format]] = self.src_fmt_configs

        external_config = ExternalConfig.from_api_repr(external_config_api_repr)
        if self.schema_fields:
            external_config.schema = [SchemaField.from_api_repr(f) for f in self.schema_fields]
        if self.max_bad_records:
            external_config.max_bad_records = self.max_bad_records

        # build table definition
        table = Table(
            table_ref=TableReference.from_string(self.destination_project_dataset_table, self.hook.project_id)
        )
        table.external_data_configuration = external_config
        if self.labels:
            table.labels = self.labels

        if self.description:
            table.description = self.description

        if self.encryption_configuration:
            table.encryption_configuration = EncryptionConfiguration.from_api_repr(
                self.encryption_configuration
            )
        table_obj_api_repr = table.to_api_repr()

        self.log.info("Creating external table: %s", self.destination_project_dataset_table)
        self.hook.create_table(
            table_resource=table_obj_api_repr,
            project_id=self.project_id or self.hook.project_id,
            dataset_id=table.dataset_id,
            table_id=table.table_id,
            location=self.location,
            exists_ok=True,
        )
        self.log.info("External table created successfully: %s", self.destination_project_dataset_table)
        return table_obj_api_repr

    def _use_existing_table(self):
        destination_project_id, destination_dataset, destination_table = self.hook.split_tablename(
            table_input=self.destination_project_dataset_table,
            default_project_id=self.hook.project_id,
            var_name="destination_project_dataset_table",
        )

        # bigquery also allows you to define how you want a table's schema to change
        # as a side effect of a load
        # for more details:
        # https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs#configuration.load.schemaUpdateOptions
        allowed_schema_update_options = ["ALLOW_FIELD_ADDITION", "ALLOW_FIELD_RELAXATION"]
        if not set(allowed_schema_update_options).issuperset(set(self.schema_update_options)):
            raise ValueError(
                f"{self.schema_update_options} contains invalid schema update options. "
                f"Please only use one or more of the following options: {allowed_schema_update_options}"
            )

        self.configuration = {
            "load": {
                "autodetect": self.autodetect,
                "createDisposition": self.create_disposition,
                "destinationTable": {
                    "projectId": destination_project_id,
                    "datasetId": destination_dataset,
                    "tableId": destination_table,
                },
                "sourceFormat": self.source_format,
                "sourceUris": self.source_uris,
                "writeDisposition": self.write_disposition,
                "ignoreUnknownValues": self.ignore_unknown_values,
            },
        }
        self.time_partitioning = self._cleanse_time_partitioning(
            self.destination_project_dataset_table, self.time_partitioning
        )
        if self.time_partitioning:
            self.configuration["load"].update({"timePartitioning": self.time_partitioning})
        if self.range_partitioning:
            self.configuration["load"].update({"rangePartitioning": self.range_partitioning})

        if self.cluster_fields:
            self.configuration["load"].update({"clustering": {"fields": self.cluster_fields}})

        if self.schema_fields:
            self.configuration["load"]["schema"] = {"fields": self.schema_fields}

        if self.schema_update_options:
            # To provide backward compatibility
            self.schema_update_options = list(self.schema_update_options or [])
            self.log.info("Adding experimental 'schemaUpdateOptions': %s", self.schema_update_options)
            self.configuration["load"]["schemaUpdateOptions"] = self.schema_update_options

        if self.max_bad_records:
            self.configuration["load"]["maxBadRecords"] = self.max_bad_records

        if self.encryption_configuration:
            self.configuration["load"]["destinationEncryptionConfiguration"] = self.encryption_configuration

        if self.labels or self.description:
            self.configuration["load"].update({"destinationTableProperties": {}})
            if self.labels:
                self.configuration["load"]["destinationTableProperties"]["labels"] = self.labels
            if self.description:
                self.configuration["load"]["destinationTableProperties"]["description"] = self.description

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
                "columnNameCharacterMap",
            ],
            "DATASTORE_BACKUP": ["projectionFields"],
            "NEWLINE_DELIMITED_JSON": ["autodetect", "ignoreUnknownValues"],
            "PARQUET": ["autodetect", "ignoreUnknownValues"],
            "AVRO": ["useAvroLogicalTypes"],
            "ORC": ["autodetect"],
        }

        valid_configs = src_fmt_to_configs_mapping[self.source_format]

        # if following fields are not specified in src_fmt_configs,
        # honor the top-level params for backward-compatibility
        backward_compatibility_configs = {
            "skipLeadingRows": self.skip_leading_rows,
            "fieldDelimiter": self.field_delimiter,
            "ignoreUnknownValues": self.ignore_unknown_values,
            "quote": self.quote_character,
            "allowQuotedNewlines": self.allow_quoted_newlines,
            "encoding": self.encoding,
        }

        self.src_fmt_configs = self._validate_src_fmt_configs(
            self.source_format, self.src_fmt_configs, valid_configs, backward_compatibility_configs
        )

        self.configuration["load"].update(self.src_fmt_configs)

        if self.allow_jagged_rows:
            self.configuration["load"]["allowJaggedRows"] = self.allow_jagged_rows
        return self.configuration

    def _validate_src_fmt_configs(
        self,
        source_format: str,
        src_fmt_configs: dict,
        valid_configs: list[str],
        backward_compatibility_configs: dict | None = None,
    ) -> dict:
        """
        Validate the given src_fmt_configs against a valid configuration for the source format.

        Adds the backward compatibility config to the src_fmt_configs.

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

    def _cleanse_time_partitioning(
        self, destination_dataset_table: str | None, time_partitioning_in: dict | None
    ) -> dict:  # if it is a partitioned table ($ is in the table name) add partition load option
        if time_partitioning_in is None:
            time_partitioning_in = {}

        time_partitioning_out = {}
        if destination_dataset_table and "$" in destination_dataset_table:
            time_partitioning_out["type"] = "DAY"
        time_partitioning_out.update(time_partitioning_in)
        return time_partitioning_out

    def on_kill(self) -> None:
        if self.job_id and self.cancel_on_kill:
            self.hook.cancel_job(job_id=self.job_id, location=self.location)  # type: ignore[union-attr]
        else:
            self.log.info("Skipping to cancel job: %s.%s", self.location, self.job_id)

    def get_openlineage_facets_on_complete(self, task_instance):
        """Implement on_complete as we will include final BQ job id."""
        from airflow.providers.common.compat.openlineage.facet import (
            Dataset,
            ExternalQueryRunFacet,
            Identifier,
            SymlinksDatasetFacet,
        )
        from airflow.providers.google.cloud.openlineage.utils import (
            WILDCARD,
            extract_ds_name_from_gcs_path,
            get_facets_from_bq_table,
            get_identity_column_lineage_facet,
        )
        from airflow.providers.openlineage.extractors import OperatorLineage

        if not self.hook:
            self.hook = BigQueryHook(
                gcp_conn_id=self.gcp_conn_id,
                location=self.location,
                impersonation_chain=self.impersonation_chain,
            )

        project_id = self.project_id or self.hook.project_id
        table_object = self.hook.get_client(project_id).get_table(self.destination_project_dataset_table)

        output_dataset_facets = get_facets_from_bq_table(table_object)

        source_objects = (
            self.source_objects if isinstance(self.source_objects, list) else [self.source_objects]
        )
        input_dataset_facets = {}
        if "schema" in output_dataset_facets:
            input_dataset_facets["schema"] = output_dataset_facets["schema"]

        input_datasets = []
        for blob in sorted(source_objects):
            additional_facets = {}

            if WILDCARD in blob:
                # For path with wildcard we attach a symlink with unmodified path.
                additional_facets = {
                    "symlink": SymlinksDatasetFacet(
                        identifiers=[Identifier(namespace=f"gs://{self.bucket}", name=blob, type="file")]
                    ),
                }

            dataset = Dataset(
                namespace=f"gs://{self.bucket}",
                name=extract_ds_name_from_gcs_path(blob),
                facets=merge_dicts(input_dataset_facets, additional_facets),
            )
            input_datasets.append(dataset)

        output_dataset = Dataset(
            namespace="bigquery",
            name=str(table_object.reference),
            facets={
                **output_dataset_facets,
                **get_identity_column_lineage_facet(
                    dest_field_names=[field.name for field in table_object.schema],
                    input_datasets=input_datasets,
                ),
            },
        )

        run_facets = {}
        if self.job_id:
            run_facets = {
                "externalQuery": ExternalQueryRunFacet(externalQueryId=self.job_id, source="bigquery"),
            }

        return OperatorLineage(inputs=input_datasets, outputs=[output_dataset], run_facets=run_facets)
