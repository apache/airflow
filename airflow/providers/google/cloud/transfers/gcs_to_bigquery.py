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
from typing import TYPE_CHECKING, Any, Sequence

from google.api_core.exceptions import Conflict
from google.api_core.retry import Retry
from google.cloud.bigquery import DEFAULT_RETRY, CopyJob, ExtractJob, LoadJob, QueryJob

from airflow import AirflowException
from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.bigquery import (
    BigQueryHook,
    BigQueryJob,
    _cleanse_time_partitioning,
)
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.links.bigquery import BigQueryTableLink
from airflow.providers.google.cloud.triggers.bigquery import BigQueryInsertJobTrigger

if TYPE_CHECKING:
    from airflow.utils.context import Context


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
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :param schema_update_options: Allows the schema of the destination
        table to be updated as a side effect of the load job.
    :param src_fmt_configs: configure optional fields specific to the source format
    :param external_table: Flag to specify if the destination table should be
        a BigQuery external table. Default Value is False.
    :param time_partitioning: configure optional time partitioning fields i.e.
        partition by field, type and  expiration as per API specifications.
        Note that 'field' is not available in concurrency with
        dataset.table$partition.
    :param cluster_fields: Request that the result of this load be stored sorted
        by one or more columns. BigQuery supports clustering for both partitioned and
        non-partitioned tables. The order of columns given determines the sort order.
        Not applicable for external tables.
    :param autodetect: [Optional] Indicates if we should automatically infer the
        options and schema for CSV and JSON sources. (Default: ``True``).
        Parameter must be set to True if 'schema_fields' and 'schema_object' are undefined.
        It is suggested to set to True if table are create outside of Airflow.
    :param encryption_configuration: [Optional] Custom encryption configuration (e.g., Cloud KMS keys).
        **Example**: ::

            encryption_configuration = {
                "kmsKeyName": "projects/testp/locations/us/keyRings/test-kr/cryptoKeys/test-key"
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
    """

    template_fields: Sequence[str] = (
        "bucket",
        "source_objects",
        "schema_object",
        "schema_object_bucket",
        "destination_project_dataset_table",
        "impersonation_chain",
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
        delegate_to=None,
        schema_update_options=(),
        src_fmt_configs=None,
        external_table=False,
        time_partitioning=None,
        cluster_fields=None,
        autodetect=True,
        encryption_configuration=None,
        location=None,
        impersonation_chain: str | Sequence[str] | None = None,
        labels=None,
        description=None,
        deferrable: bool = False,
        result_retry: Retry = DEFAULT_RETRY,
        result_timeout: float | None = None,
        cancel_on_kill: bool = True,
        job_id: str | None = None,
        force_rerun: bool = True,
        reattach_states: set[str] | None = None,
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
        self.bucket = bucket
        self.source_objects = source_objects
        self.schema_object = schema_object

        if schema_object_bucket is None:
            schema_object_bucket = bucket
        self.schema_object_bucket = schema_object_bucket

        # BQ config
        self.destination_project_dataset_table = destination_project_dataset_table
        self.schema_fields = schema_fields
        self.source_format = source_format
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
        self.delegate_to = delegate_to

        self.schema_update_options = schema_update_options
        self.src_fmt_configs = src_fmt_configs
        self.time_partitioning = time_partitioning
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

    def _submit_job(
        self,
        hook: BigQueryHook,
        job_id: str,
    ) -> BigQueryJob:
        # Submit a new job without waiting for it to complete.
        return hook.insert_job(
            configuration=self.configuration,
            project_id=hook.project_id,
            location=self.location,
            job_id=job_id,
            timeout=self.result_timeout,
            retry=self.result_retry,
            nowait=True,
        )

    @staticmethod
    def _handle_job_error(job: BigQueryJob) -> None:
        if job.error_result:
            raise AirflowException(f"BigQuery job {job.job_id} failed: {job.error_result}")

    def execute(self, context: Context):
        hook = BigQueryHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            location=self.location,
            impersonation_chain=self.impersonation_chain,
        )
        self.hook = hook
        job_id = self.hook.generate_job_id(
            job_id=self.job_id,
            dag_id=self.dag_id,
            task_id=self.task_id,
            logical_date=context["logical_date"],
            configuration=self.configuration,
            force_rerun=self.force_rerun,
        )

        self.source_objects = (
            self.source_objects if isinstance(self.source_objects, list) else [self.source_objects]
        )
        source_uris = [f"gs://{self.bucket}/{source_object}" for source_object in self.source_objects]
        if not self.schema_fields:
            gcs_hook = GCSHook(
                gcp_conn_id=self.gcp_conn_id,
                delegate_to=self.delegate_to,
                impersonation_chain=self.impersonation_chain,
            )
            if self.schema_object and self.source_format != "DATASTORE_BACKUP":
                schema_fields = json.loads(gcs_hook.download(self.bucket, self.schema_object).decode("utf-8"))
                self.log.info("Autodetected fields from schema object: %s", schema_fields)

        if self.external_table:
            self.log.info("Creating a new BigQuery table for storing data...")
            project_id, dataset_id, table_id = self.hook.split_tablename(
                table_input=self.destination_project_dataset_table,
                default_project_id=self.hook.project_id or "",
            )
            table_resource = {
                "tableReference": {
                    "projectId": project_id,
                    "datasetId": dataset_id,
                    "tableId": table_id,
                },
                "labels": self.labels,
                "description": self.description,
                "externalDataConfiguration": {
                    "source_uris": source_uris,
                    "source_format": self.source_format,
                    "maxBadRecords": self.max_bad_records,
                    "autodetect": self.autodetect,
                    "compression": self.compression,
                    "csvOptions": {
                        "fieldDelimeter": self.field_delimiter,
                        "skipLeadingRows": self.skip_leading_rows,
                        "quote": self.quote_character,
                        "allowQuotedNewlines": self.allow_quoted_newlines,
                        "allowJaggedRows": self.allow_jagged_rows,
                    },
                },
                "location": self.location,
                "encryptionConfiguration": self.encryption_configuration,
            }
            table_resource_checked_schema = self._check_schema_fields(table_resource)
            table = self.hook.create_empty_table(
                table_resource=table_resource_checked_schema,
            )
            max_id = self._find_max_value_in_column()
            BigQueryTableLink.persist(
                context=context,
                task_instance=self,
                dataset_id=table.to_api_repr()["tableReference"]["datasetId"],
                project_id=table.to_api_repr()["tableReference"]["projectId"],
                table_id=table.to_api_repr()["tableReference"]["tableId"],
            )
            return max_id
        else:
            self.log.info("Using existing BigQuery table for storing data...")
            destination_project, destination_dataset, destination_table = self.hook.split_tablename(
                table_input=self.destination_project_dataset_table,
                default_project_id=self.hook.project_id or "",
                var_name="destination_project_dataset_table",
            )
            self.configuration = {
                "load": {
                    "autodetect": self.autodetect,
                    "createDisposition": self.create_disposition,
                    "destinationTable": {
                        "projectId": destination_project,
                        "datasetId": destination_dataset,
                        "tableId": destination_table,
                    },
                    "destinationTableProperties": {
                        "description": self.description,
                        "labels": self.labels,
                    },
                    "sourceFormat": self.source_format,
                    "skipLeadingRows": self.skip_leading_rows,
                    "sourceUris": source_uris,
                    "writeDisposition": self.write_disposition,
                    "ignoreUnknownValues": self.ignore_unknown_values,
                    "allowQuotedNewlines": self.allow_quoted_newlines,
                    "encoding": self.encoding,
                    "allowJaggedRows": self.allow_jagged_rows,
                    "fieldDelimiter": self.field_delimiter,
                    "maxBadRecords": self.max_bad_records,
                    "quote": self.quote_character,
                    "schemaUpdateOptions": self.schema_update_options,
                },
            }
            if self.cluster_fields:
                self.configuration["load"].update({"clustering": {"fields": self.cluster_fields}})
            time_partitioning = _cleanse_time_partitioning(
                self.destination_project_dataset_table, self.time_partitioning
            )
            if time_partitioning:
                self.configuration["load"].update({"timePartitioning": time_partitioning})
            # fields that should only be set if defined
            set_if_def = {
                "quote": self.quote_character,
                "destinationEncryptionConfiguration": self.encryption_configuration,
            }
            for k, v in set_if_def.items():
                if v:
                    self.configuration["load"][k] = v
            self.configuration = self._check_schema_fields(self.configuration)
            try:
                self.log.info("Executing: %s", self.configuration)
                job = self._submit_job(self.hook, job_id)
            except Conflict:
                # If the job already exists retrieve it
                job = self.hook.get_job(
                    project_id=self.hook.project_id,
                    location=self.location,
                    job_id=job_id,
                )
                if job.state in self.reattach_states:
                    # We are reattaching to a job
                    job._begin()
                    self._handle_job_error(job)
                else:
                    # Same job configuration so we need force_rerun
                    raise AirflowException(
                        f"Job with id: {job_id} already exists and is in {job.state} state. If you "
                        f"want to force rerun it consider setting `force_rerun=True`."
                        f"Or, if you want to reattach in this scenario add {job.state} to `reattach_states`"
                    )

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
                                    "task_instance": self,
                                    "project_id": self.hook.project_id,
                                    "table_id": table,
                                }
                                if not isinstance(table, str):
                                    persist_kwargs["table_id"] = table["tableId"]
                                    persist_kwargs["dataset_id"] = table["datasetId"]
                                BigQueryTableLink.persist(**persist_kwargs)

            self.job_id = job.job_id
            context["ti"].xcom_push(key="job_id", value=self.job_id)
            if self.deferrable:
                self.defer(
                    timeout=self.execution_timeout,
                    trigger=BigQueryInsertJobTrigger(
                        conn_id=self.gcp_conn_id,
                        job_id=self.job_id,
                        project_id=self.hook.project_id,
                    ),
                    method_name="execute_complete",
                )
            else:
                job.result(timeout=self.result_timeout, retry=self.result_retry)
                max_id = self._find_max_value_in_column()
                self._handle_job_error(job)
                return max_id

    def execute_complete(self, context: Context, event: dict[str, Any]):
        """
        Callback for when the trigger fires - returns immediately.
        Relies on trigger to throw an exception, otherwise it assumes execution was
        successful.
        """
        if event["status"] == "error":
            raise AirflowException(event["message"])
        self.log.info(
            "%s completed with response %s ",
            self.task_id,
            event["message"],
        )
        return self._find_max_value_in_column()

    def _find_max_value_in_column(self):
        hook = BigQueryHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            location=self.location,
            impersonation_chain=self.impersonation_chain,
        )
        if self.max_id_key:
            self.log.info(f"Selecting the MAX value from BigQuery column '{self.max_id_key}'...")
            select_command = (
                f"SELECT MAX({self.max_id_key}) AS max_value "
                f"FROM {self.destination_project_dataset_table}"
            )

            self.configuration = {
                "query": {
                    "query": select_command,
                    "useLegacySql": False,
                    "schemaUpdateOptions": [],
                }
            }
            job_id = hook.insert_job(configuration=self.configuration, project_id=hook.project_id)
            rows = list(hook.get_job(job_id=job_id, location=self.location).result())
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

    def _check_schema_fields(self, table_resource):
        """
        Helper method to detect schema fields if they were not specified by user and autodetect=True.
        If source_objects were passed, method reads the second row in CSV file. If there is at least one digit
        table_resurce is returned without changes so that BigQuery can determine schema_fields in the
        next step.
        If there are only characters, the first row with fields is used to construct schema_fields argument
        with type 'STRING'. Table_resource is updated with new schema_fileds key and returned back to operator
        :param table_resource: Configuration or table_resource dictionary
        :return: table_resource: Updated table_resource dict with schema_fields
        """
        if not self.autodetect and not self.schema_fields:
            raise RuntimeError(
                "Table schema was not found. Set autodetect=True to "
                "automatically set schema fields from source objects or pass "
                "schema_fields explicitly"
            )
        elif not self.schema_fields:
            for source_object in self.source_objects:
                gcs_hook = GCSHook(
                    gcp_conn_id=self.gcp_conn_id,
                    delegate_to=self.delegate_to,
                    impersonation_chain=self.impersonation_chain,
                )
                blob = gcs_hook.download(
                    bucket_name=self.schema_object_bucket,
                    object_name=source_object,
                )
                fields, values = [item.split(",") for item in blob.decode("utf-8").splitlines()][:2]
                import re

                if any(re.match(r"[\d\-\\.]+$", value) for value in values):
                    return table_resource
                else:
                    schema_fields = []
                    for field in fields:
                        schema_fields.append({"name": field, "type": "STRING", "mode": "NULLABLE"})
                    self.schema_fields = schema_fields
                    if self.external_table:
                        table_resource["externalDataConfiguration"]["csvOptions"]["skipLeadingRows"] = 1
                    elif not self.external_table:
                        table_resource["load"]["skipLeadingRows"] = 1
        if self.external_table:
            table_resource["schema"] = {"fields": self.schema_fields}
        elif not self.external_table:
            table_resource["load"]["schema"] = {"fields": self.schema_fields}
        return table_resource

    def on_kill(self) -> None:
        if self.job_id and self.cancel_on_kill:
            self.hook.cancel_job(job_id=self.job_id, location=self.location)  # type: ignore[union-attr]
        else:
            self.log.info("Skipping to cancel job: %s.%s", self.location, self.job_id)
