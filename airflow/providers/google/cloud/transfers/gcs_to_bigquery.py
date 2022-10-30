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
import warnings
from typing import TYPE_CHECKING, Sequence

from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook

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
    :param skip_leading_rows: Number of rows to skip when loading from a CSV.
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
    :param description: [Optional] Description for the BigQuery table.
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
        skip_leading_rows=0,
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
        **kwargs,
    ):

        super().__init__(**kwargs)

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

    def execute(self, context: Context):
        bq_hook = BigQueryHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            location=self.location,
            impersonation_chain=self.impersonation_chain,
        )

        if not self.schema_fields:
            if self.schema_object and self.source_format != "DATASTORE_BACKUP":
                gcs_hook = GCSHook(
                    gcp_conn_id=self.gcp_conn_id,
                    delegate_to=self.delegate_to,
                    impersonation_chain=self.impersonation_chain,
                )
                blob = gcs_hook.download(
                    bucket_name=self.schema_object_bucket,
                    object_name=self.schema_object,
                )
                schema_fields = json.loads(blob.decode("utf-8"))
            else:
                schema_fields = None
        else:
            schema_fields = self.schema_fields

        self.source_objects = (
            self.source_objects if isinstance(self.source_objects, list) else [self.source_objects]
        )
        source_uris = [f"gs://{self.bucket}/{source_object}" for source_object in self.source_objects]

        if self.external_table:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", DeprecationWarning)
                bq_hook.create_external_table(
                    external_project_dataset_table=self.destination_project_dataset_table,
                    schema_fields=schema_fields,
                    source_uris=source_uris,
                    source_format=self.source_format,
                    autodetect=self.autodetect,
                    compression=self.compression,
                    skip_leading_rows=self.skip_leading_rows,
                    field_delimiter=self.field_delimiter,
                    max_bad_records=self.max_bad_records,
                    quote_character=self.quote_character,
                    ignore_unknown_values=self.ignore_unknown_values,
                    allow_quoted_newlines=self.allow_quoted_newlines,
                    allow_jagged_rows=self.allow_jagged_rows,
                    encoding=self.encoding,
                    src_fmt_configs=self.src_fmt_configs,
                    encryption_configuration=self.encryption_configuration,
                    labels=self.labels,
                    description=self.description,
                )
        else:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", DeprecationWarning)
                bq_hook.run_load(
                    destination_project_dataset_table=self.destination_project_dataset_table,
                    schema_fields=schema_fields,
                    source_uris=source_uris,
                    source_format=self.source_format,
                    autodetect=self.autodetect,
                    create_disposition=self.create_disposition,
                    skip_leading_rows=self.skip_leading_rows,
                    write_disposition=self.write_disposition,
                    field_delimiter=self.field_delimiter,
                    max_bad_records=self.max_bad_records,
                    quote_character=self.quote_character,
                    ignore_unknown_values=self.ignore_unknown_values,
                    allow_quoted_newlines=self.allow_quoted_newlines,
                    allow_jagged_rows=self.allow_jagged_rows,
                    encoding=self.encoding,
                    schema_update_options=self.schema_update_options,
                    src_fmt_configs=self.src_fmt_configs,
                    time_partitioning=self.time_partitioning,
                    cluster_fields=self.cluster_fields,
                    encryption_configuration=self.encryption_configuration,
                    labels=self.labels,
                    description=self.description,
                )

        if self.max_id_key:
            select_command = f"SELECT MAX({self.max_id_key}) FROM `{self.destination_project_dataset_table}`"
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", DeprecationWarning)
                job_id = bq_hook.run_query(
                    sql=select_command,
                    location=self.location,
                    use_legacy_sql=False,
                )
            result = bq_hook.get_job(job_id=job_id, location=self.location).result()
            row = next(iter(result), None)
            if row is None:
                raise RuntimeError(f"The {select_command} returned no rows!")
            max_id = row[0]
            self.log.info(
                "Loaded BQ data with max %s.%s=%s",
                self.destination_project_dataset_table,
                self.max_id_key,
                max_id,
            )
            return max_id
