# -*- coding: utf-8 -*-
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
"""
This module contains Google BigQuery operators.
"""

import json
import warnings
from typing import Iterable, List, Optional, Union

from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook, _parse_gcs_url
from airflow.exceptions import AirflowException
from airflow.models.baseoperator import BaseOperator, BaseOperatorLink
from airflow.models.taskinstance import TaskInstance
from airflow.utils.decorators import apply_defaults


class BigQueryConsoleLink(BaseOperatorLink):
    """
    Helper class for constructing BigQuery link.
    """
    name = 'BigQuery Console'

    def get_link(self, operator, dttm):
        ti = TaskInstance(task=operator, execution_date=dttm)
        job_id = ti.xcom_pull(task_ids=operator.task_id, key='job_id')
        return 'https://console.cloud.google.com/bigquery?j={job_id}'.format(
            job_id=job_id) if job_id else ''


# pylint: disable=too-many-instance-attributes
class BigQueryOperator(BaseOperator):
    """
    Executes BigQuery SQL queries in a specific BigQuery database

    :param sql: the sql code to be executed (templated)
    :type sql: Can receive a str representing a sql statement,
        a list of str (sql statements), or reference to a template file.
        Template reference are recognized by str ending in '.sql'.
    :param destination_dataset_table: A dotted
        ``(<project>.|<project>:)<dataset>.<table>`` that, if set, will store the results
        of the query. (templated)
    :type destination_dataset_table: str
    :param write_disposition: Specifies the action that occurs if the destination table
        already exists. (default: 'WRITE_EMPTY')
    :type write_disposition: str
    :param create_disposition: Specifies whether the job is allowed to create new tables.
        (default: 'CREATE_IF_NEEDED')
    :type create_disposition: str
    :param allow_large_results: Whether to allow large results.
    :type allow_large_results: bool
    :param flatten_results: If true and query uses legacy SQL dialect, flattens
        all nested and repeated fields in the query results. ``allow_large_results``
        must be ``true`` if this is set to ``false``. For standard SQL queries, this
        flag is ignored and results are never flattened.
    :type flatten_results: bool
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud Platform.
    :type gcp_conn_id: str
    :param bigquery_conn_id: (Deprecated) The connection ID used to connect to Google Cloud Platform.
        This parameter has been deprecated. You should pass the gcp_conn_id parameter instead.
    :type bigquery_conn_id: str
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must have domain-wide
        delegation enabled.
    :type delegate_to: str
    :param udf_config: The User Defined Function configuration for the query.
        See https://cloud.google.com/bigquery/user-defined-functions for details.
    :type udf_config: list
    :param use_legacy_sql: Whether to use legacy SQL (true) or standard SQL (false).
    :type use_legacy_sql: bool
    :param maximum_billing_tier: Positive integer that serves as a multiplier
        of the basic price.
        Defaults to None, in which case it uses the value set in the project.
    :type maximum_billing_tier: int
    :param maximum_bytes_billed: Limits the bytes billed for this job.
        Queries that will have bytes billed beyond this limit will fail
        (without incurring a charge). If unspecified, this will be
        set to your project default.
    :type maximum_bytes_billed: float
    :param api_resource_configs: a dictionary that contain params
        'configuration' applied for Google BigQuery Jobs API:
        https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs
        for example, {'query': {'useQueryCache': False}}. You could use it
        if you need to provide some params that are not supported by BigQueryOperator
        like args.
    :type api_resource_configs: dict
    :param schema_update_options: Allows the schema of the destination
        table to be updated as a side effect of the load job.
    :type schema_update_options: Optional[Union[list, tuple, set]]
    :param query_params: a list of dictionary containing query parameter types and
        values, passed to BigQuery. The structure of dictionary should look like
        'queryParameters' in Google BigQuery Jobs API:
        https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs.
        For example, [{ 'name': 'corpus', 'parameterType': { 'type': 'STRING' },
        'parameterValue': { 'value': 'romeoandjuliet' } }].
    :type query_params: list
    :param labels: a dictionary containing labels for the job/query,
        passed to BigQuery
    :type labels: dict
    :param priority: Specifies a priority for the query.
        Possible values include INTERACTIVE and BATCH.
        The default value is INTERACTIVE.
    :type priority: str
    :param time_partitioning: configure optional time partitioning fields i.e.
        partition by field, type and expiration as per API specifications.
    :type time_partitioning: dict
    :param cluster_fields: Request that the result of this query be stored sorted
        by one or more columns. This is only available in conjunction with
        time_partitioning. The order of columns given determines the sort order.
    :type cluster_fields: list[str]
    :param location: The geographic location of the job. Required except for
        US and EU. See details at
        https://cloud.google.com/bigquery/docs/locations#specifying_your_location
    :type location: str
    :param encryption_configuration: [Optional] Custom encryption configuration (e.g., Cloud KMS keys).
        **Example**: ::

            encryption_configuration = {
                "kmsKeyName": "projects/testp/locations/us/keyRings/test-kr/cryptoKeys/test-key"
            }
    :type encryption_configuration: dict
    """

    template_fields = ('sql', 'destination_dataset_table', 'labels')
    template_ext = ('.sql', )
    ui_color = '#e4f0e8'

    operator_extra_links = (
        BigQueryConsoleLink(),
    )

    # pylint: disable=too-many-arguments, too-many-locals
    @apply_defaults
    def __init__(self,
                 sql: Union[str, Iterable],
                 destination_dataset_table: Optional[str] = None,
                 write_disposition: Optional[str] = 'WRITE_EMPTY',
                 allow_large_results: Optional[bool] = False,
                 flatten_results: Optional[bool] = None,
                 gcp_conn_id: Optional[str] = 'google_cloud_default',
                 bigquery_conn_id: Optional[str] = None,
                 delegate_to: Optional[str] = None,
                 udf_config: Optional[list] = None,
                 use_legacy_sql: Optional[bool] = True,
                 maximum_billing_tier: Optional[int] = None,
                 maximum_bytes_billed: Optional[float] = None,
                 create_disposition: Optional[str] = 'CREATE_IF_NEEDED',
                 schema_update_options: Optional[Union[list, tuple, set]] = None,
                 query_params: Optional[list] = None,
                 labels: Optional[dict] = None,
                 priority: Optional[str] = 'INTERACTIVE',
                 time_partitioning: Optional[dict] = None,
                 api_resource_configs: Optional[dict] = None,
                 cluster_fields: Optional[List[str]] = None,
                 location: Optional[str] = None,
                 encryption_configuration=None,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)

        if bigquery_conn_id:
            warnings.warn(
                "The bigquery_conn_id parameter has been deprecated. You should pass "
                "the gcp_conn_id parameter.", DeprecationWarning, stacklevel=3)
            gcp_conn_id = bigquery_conn_id

        self.sql = sql
        self.destination_dataset_table = destination_dataset_table
        self.write_disposition = write_disposition
        self.create_disposition = create_disposition
        self.allow_large_results = allow_large_results
        self.flatten_results = flatten_results
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.udf_config = udf_config
        self.use_legacy_sql = use_legacy_sql
        self.maximum_billing_tier = maximum_billing_tier
        self.maximum_bytes_billed = maximum_bytes_billed
        self.schema_update_options = schema_update_options
        self.query_params = query_params
        self.labels = labels
        self.bq_cursor = None
        self.priority = priority
        self.time_partitioning = time_partitioning
        self.api_resource_configs = api_resource_configs
        self.cluster_fields = cluster_fields
        self.location = location
        self.encryption_configuration = encryption_configuration

    def execute(self, context):
        if self.bq_cursor is None:
            self.log.info('Executing: %s', self.sql)
            hook = BigQueryHook(
                bigquery_conn_id=self.gcp_conn_id,
                use_legacy_sql=self.use_legacy_sql,
                delegate_to=self.delegate_to,
                location=self.location,
            )
            conn = hook.get_conn()
            self.bq_cursor = conn.cursor()
        if isinstance(self.sql, str):
            job_id = self.bq_cursor.run_query(
                sql=self.sql,
                destination_dataset_table=self.destination_dataset_table,
                write_disposition=self.write_disposition,
                allow_large_results=self.allow_large_results,
                flatten_results=self.flatten_results,
                udf_config=self.udf_config,
                maximum_billing_tier=self.maximum_billing_tier,
                maximum_bytes_billed=self.maximum_bytes_billed,
                create_disposition=self.create_disposition,
                query_params=self.query_params,
                labels=self.labels,
                schema_update_options=self.schema_update_options,
                priority=self.priority,
                time_partitioning=self.time_partitioning,
                api_resource_configs=self.api_resource_configs,
                cluster_fields=self.cluster_fields,
                encryption_configuration=self.encryption_configuration
            )
        elif isinstance(self.sql, Iterable):
            job_id = [
                self.bq_cursor.run_query(
                    sql=s,
                    destination_dataset_table=self.destination_dataset_table,
                    write_disposition=self.write_disposition,
                    allow_large_results=self.allow_large_results,
                    flatten_results=self.flatten_results,
                    udf_config=self.udf_config,
                    maximum_billing_tier=self.maximum_billing_tier,
                    maximum_bytes_billed=self.maximum_bytes_billed,
                    create_disposition=self.create_disposition,
                    query_params=self.query_params,
                    labels=self.labels,
                    schema_update_options=self.schema_update_options,
                    priority=self.priority,
                    time_partitioning=self.time_partitioning,
                    api_resource_configs=self.api_resource_configs,
                    cluster_fields=self.cluster_fields,
                    encryption_configuration=self.encryption_configuration
                )
                for s in self.sql]
        else:
            raise AirflowException(
                "argument 'sql' of type {} is neither a string nor an iterable".format(type(str)))
        context['task_instance'].xcom_push(key='job_id', value=job_id)

    def on_kill(self):
        super().on_kill()
        if self.bq_cursor is not None:
            self.log.info('Cancelling running query')
            self.bq_cursor.cancel_query()


class BigQueryCreateEmptyTableOperator(BaseOperator):
    """
    Creates a new, empty table in the specified BigQuery dataset,
    optionally with schema.

    The schema to be used for the BigQuery table may be specified in one of
    two ways. You may either directly pass the schema fields in, or you may
    point the operator to a Google cloud storage object name. The object in
    Google cloud storage must be a JSON file with the schema fields in it.
    You can also create a table without schema.

    :param project_id: The project to create the table into. (templated)
    :type project_id: str
    :param dataset_id: The dataset to create the table into. (templated)
    :type dataset_id: str
    :param table_id: The Name of the table to be created. (templated)
    :type table_id: str
    :param schema_fields: If set, the schema field list as defined here:
        https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs#configuration.load.schema

        **Example**: ::

            schema_fields=[{"name": "emp_name", "type": "STRING", "mode": "REQUIRED"},
                           {"name": "salary", "type": "INTEGER", "mode": "NULLABLE"}]

    :type schema_fields: list
    :param gcs_schema_object: Full path to the JSON file containing
        schema (templated). For
        example: ``gs://test-bucket/dir1/dir2/employee_schema.json``
    :type gcs_schema_object: str
    :param time_partitioning: configure optional time partitioning fields i.e.
        partition by field, type and  expiration as per API specifications.

        .. seealso::
            https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#timePartitioning
    :type time_partitioning: dict
    :param bigquery_conn_id: (Optional) The connection ID used to connect to Google Cloud Platform and
        interact with the Bigquery service.
    :type bigquery_conn_id: str
    :param google_cloud_storage_conn_id: (Optional) The connection ID used to connect to Google Cloud
        Platform and interact with the Google Cloud Storage service.
    :type google_cloud_storage_conn_id: str
    :param delegate_to: The account to impersonate, if any. For this to
        work, the service account making the request must have domain-wide
        delegation enabled.
    :type delegate_to: str
    :param labels: a dictionary containing labels for the table, passed to BigQuery

        **Example (with schema JSON in GCS)**: ::

            CreateTable = BigQueryCreateEmptyTableOperator(
                task_id='BigQueryCreateEmptyTableOperator_task',
                dataset_id='ODS',
                table_id='Employees',
                project_id='internal-gcp-project',
                gcs_schema_object='gs://schema-bucket/employee_schema.json',
                bigquery_conn_id='airflow-conn-id',
                google_cloud_storage_conn_id='airflow-conn-id'
            )

        **Corresponding Schema file** (``employee_schema.json``): ::

            [
              {
                "mode": "NULLABLE",
                "name": "emp_name",
                "type": "STRING"
              },
              {
                "mode": "REQUIRED",
                "name": "salary",
                "type": "INTEGER"
              }
            ]

        **Example (with schema in the DAG)**: ::

            CreateTable = BigQueryCreateEmptyTableOperator(
                task_id='BigQueryCreateEmptyTableOperator_task',
                dataset_id='ODS',
                table_id='Employees',
                project_id='internal-gcp-project',
                schema_fields=[{"name": "emp_name", "type": "STRING", "mode": "REQUIRED"},
                               {"name": "salary", "type": "INTEGER", "mode": "NULLABLE"}],
                bigquery_conn_id='airflow-conn-id-account',
                google_cloud_storage_conn_id='airflow-conn-id'
            )
    :type labels: dict
    :param encryption_configuration: [Optional] Custom encryption configuration (e.g., Cloud KMS keys).
        **Example**: ::

            encryption_configuration = {
                "kmsKeyName": "projects/testp/locations/us/keyRings/test-kr/cryptoKeys/test-key"
            }
    :type encryption_configuration: dict
    """
    template_fields = ('dataset_id', 'table_id', 'project_id',
                       'gcs_schema_object', 'labels')
    ui_color = '#f0eee4'

    # pylint: disable=too-many-arguments
    @apply_defaults
    def __init__(self,
                 dataset_id,
                 table_id,
                 project_id=None,
                 schema_fields=None,
                 gcs_schema_object=None,
                 time_partitioning=None,
                 bigquery_conn_id='google_cloud_default',
                 google_cloud_storage_conn_id='google_cloud_default',
                 delegate_to=None,
                 labels=None,
                 encryption_configuration=None,
                 *args, **kwargs):

        super().__init__(*args, **kwargs)

        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.schema_fields = schema_fields
        self.gcs_schema_object = gcs_schema_object
        self.bigquery_conn_id = bigquery_conn_id
        self.google_cloud_storage_conn_id = google_cloud_storage_conn_id
        self.delegate_to = delegate_to
        self.time_partitioning = {} if time_partitioning is None else time_partitioning
        self.labels = labels
        self.encryption_configuration = encryption_configuration

    def execute(self, context):
        bq_hook = BigQueryHook(bigquery_conn_id=self.bigquery_conn_id,
                               delegate_to=self.delegate_to)

        if not self.schema_fields and self.gcs_schema_object:

            gcs_bucket, gcs_object = _parse_gcs_url(self.gcs_schema_object)

            gcs_hook = GoogleCloudStorageHook(
                google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
                delegate_to=self.delegate_to)
            schema_fields = json.loads(gcs_hook.download(
                gcs_bucket,
                gcs_object).decode("utf-8"))
        else:
            schema_fields = self.schema_fields

        conn = bq_hook.get_conn()
        cursor = conn.cursor()

        cursor.create_empty_table(
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            table_id=self.table_id,
            schema_fields=schema_fields,
            time_partitioning=self.time_partitioning,
            labels=self.labels,
            encryption_configuration=self.encryption_configuration
        )


# pylint: disable=too-many-instance-attributes
class BigQueryCreateExternalTableOperator(BaseOperator):
    """
    Creates a new external table in the dataset with the data in Google Cloud
    Storage.

    The schema to be used for the BigQuery table may be specified in one of
    two ways. You may either directly pass the schema fields in, or you may
    point the operator to a Google cloud storage object name. The object in
    Google cloud storage must be a JSON file with the schema fields in it.

    :param bucket: The bucket to point the external table to. (templated)
    :type bucket: str
    :param source_objects: List of Google cloud storage URIs to point
        table to. (templated)
        If source_format is 'DATASTORE_BACKUP', the list must only contain a single URI.
    :type source_objects: list
    :param destination_project_dataset_table: The dotted ``(<project>.)<dataset>.<table>``
        BigQuery table to load data into (templated). If ``<project>`` is not included,
        project will be the project defined in the connection json.
    :type destination_project_dataset_table: str
    :param schema_fields: If set, the schema field list as defined here:
        https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs#configuration.load.schema

        **Example**: ::

            schema_fields=[{"name": "emp_name", "type": "STRING", "mode": "REQUIRED"},
                           {"name": "salary", "type": "INTEGER", "mode": "NULLABLE"}]

        Should not be set when source_format is 'DATASTORE_BACKUP'.
    :type schema_fields: list
    :param schema_object: If set, a GCS object path pointing to a .json file that
        contains the schema for the table. (templated)
    :type schema_object: str
    :param source_format: File format of the data.
    :type source_format: str
    :param compression: [Optional] The compression type of the data source.
        Possible values include GZIP and NONE.
        The default value is NONE.
        This setting is ignored for Google Cloud Bigtable,
        Google Cloud Datastore backups and Avro formats.
    :type compression: str
    :param skip_leading_rows: Number of rows to skip when loading from a CSV.
    :type skip_leading_rows: int
    :param field_delimiter: The delimiter to use for the CSV.
    :type field_delimiter: str
    :param max_bad_records: The maximum number of bad records that BigQuery can
        ignore when running the job.
    :type max_bad_records: int
    :param quote_character: The value that is used to quote data sections in a CSV file.
    :type quote_character: str
    :param allow_quoted_newlines: Whether to allow quoted newlines (true) or not (false).
    :type allow_quoted_newlines: bool
    :param allow_jagged_rows: Accept rows that are missing trailing optional columns.
        The missing values are treated as nulls. If false, records with missing trailing
        columns are treated as bad records, and if there are too many bad records, an
        invalid error is returned in the job result. Only applicable to CSV, ignored
        for other formats.
    :type allow_jagged_rows: bool
    :param bigquery_conn_id: (Optional) The connection ID used to connect to Google Cloud Platform and
        interact with the Bigquery service.
    :type bigquery_conn_id: str
    :param google_cloud_storage_conn_id: (Optional) The connection ID used to connect to Google Cloud
        Platform and interact with the Google Cloud Storage service.
        cloud storage hook.
    :type google_cloud_storage_conn_id: str
    :param delegate_to: The account to impersonate, if any. For this to
        work, the service account making the request must have domain-wide
        delegation enabled.
    :type delegate_to: str
    :param src_fmt_configs: configure optional fields specific to the source format
    :type src_fmt_configs: dict
    :param labels: a dictionary containing labels for the table, passed to BigQuery
    :type labels: dict
    :param encryption_configuration: [Optional] Custom encryption configuration (e.g., Cloud KMS keys).
        **Example**: ::

            encryption_configuration = {
                "kmsKeyName": "projects/testp/locations/us/keyRings/test-kr/cryptoKeys/test-key"
            }
    :type encryption_configuration: dict
    """
    template_fields = ('bucket', 'source_objects',
                       'schema_object', 'destination_project_dataset_table', 'labels')
    ui_color = '#f0eee4'

    # pylint: disable=too-many-arguments
    @apply_defaults
    def __init__(self,
                 bucket,
                 source_objects,
                 destination_project_dataset_table,
                 schema_fields=None,
                 schema_object=None,
                 source_format='CSV',
                 compression='NONE',
                 skip_leading_rows=0,
                 field_delimiter=',',
                 max_bad_records=0,
                 quote_character=None,
                 allow_quoted_newlines=False,
                 allow_jagged_rows=False,
                 bigquery_conn_id='google_cloud_default',
                 google_cloud_storage_conn_id='google_cloud_default',
                 delegate_to=None,
                 src_fmt_configs=None,
                 labels=None,
                 encryption_configuration=None,
                 *args, **kwargs):

        super().__init__(*args, **kwargs)

        # GCS config
        self.bucket = bucket
        self.source_objects = source_objects
        self.schema_object = schema_object

        # BQ config
        self.destination_project_dataset_table = destination_project_dataset_table
        self.schema_fields = schema_fields
        self.source_format = source_format
        self.compression = compression
        self.skip_leading_rows = skip_leading_rows
        self.field_delimiter = field_delimiter
        self.max_bad_records = max_bad_records
        self.quote_character = quote_character
        self.allow_quoted_newlines = allow_quoted_newlines
        self.allow_jagged_rows = allow_jagged_rows

        self.bigquery_conn_id = bigquery_conn_id
        self.google_cloud_storage_conn_id = google_cloud_storage_conn_id
        self.delegate_to = delegate_to

        self.src_fmt_configs = src_fmt_configs if src_fmt_configs is not None else dict()
        self.labels = labels
        self.encryption_configuration = encryption_configuration

    def execute(self, context):
        bq_hook = BigQueryHook(bigquery_conn_id=self.bigquery_conn_id,
                               delegate_to=self.delegate_to)

        if not self.schema_fields and self.schema_object \
                and self.source_format != 'DATASTORE_BACKUP':
            gcs_hook = GoogleCloudStorageHook(
                google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
                delegate_to=self.delegate_to)
            schema_fields = json.loads(gcs_hook.download(
                self.bucket,
                self.schema_object).decode("utf-8"))
        else:
            schema_fields = self.schema_fields

        source_uris = ['gs://{}/{}'.format(self.bucket, source_object)
                       for source_object in self.source_objects]
        conn = bq_hook.get_conn()
        cursor = conn.cursor()

        cursor.create_external_table(
            external_project_dataset_table=self.destination_project_dataset_table,
            schema_fields=schema_fields,
            source_uris=source_uris,
            source_format=self.source_format,
            compression=self.compression,
            skip_leading_rows=self.skip_leading_rows,
            field_delimiter=self.field_delimiter,
            max_bad_records=self.max_bad_records,
            quote_character=self.quote_character,
            allow_quoted_newlines=self.allow_quoted_newlines,
            allow_jagged_rows=self.allow_jagged_rows,
            src_fmt_configs=self.src_fmt_configs,
            labels=self.labels,
            encryption_configuration=self.encryption_configuration
        )


class BigQueryDeleteDatasetOperator(BaseOperator):
    """
    This operator deletes an existing dataset from your Project in Big query.
    https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets/delete

    :param project_id: The project id of the dataset.
    :type project_id: str
    :param dataset_id: The dataset to be deleted.
    :type dataset_id: str
    :param delete_contents: (Optional) Whether to force the deletion even if the dataset is not empty.
        Will delete all tables (if any) in the dataset if set to True.
        Will raise HttpError 400: "{dataset_id} is still in use" if set to False and dataset is not empty.
        The default value is False.
    :type delete_contents: bool
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud Platform.
    :type gcp_conn_id: str
    :param bigquery_conn_id: (Deprecated) The connection ID used to connect to Google Cloud Platform.
        This parameter has been deprecated. You should pass the gcp_conn_id parameter instead.
    :type bigquery_conn_id: str

    **Example**: ::

        delete_temp_data = BigQueryDeleteDatasetOperator(
            dataset_id='temp-dataset',
            project_id='temp-project',
            delete_contents=True, # Force the deletion of the dataset as well as its tables (if any).
            gcp_conn_id='_my_gcp_conn_',
            task_id='Deletetemp',
            dag=dag)
    """

    template_fields = ('dataset_id', 'project_id')
    ui_color = '#f00004'

    @apply_defaults
    def __init__(self,
                 dataset_id,
                 project_id=None,
                 delete_contents=False,
                 gcp_conn_id='google_cloud_default',
                 bigquery_conn_id=None,
                 delegate_to=None,
                 *args, **kwargs):

        if bigquery_conn_id:
            warnings.warn(
                "The bigquery_conn_id parameter has been deprecated. You should pass "
                "the gcp_conn_id parameter.", DeprecationWarning, stacklevel=3)
            gcp_conn_id = bigquery_conn_id

        self.dataset_id = dataset_id
        self.project_id = project_id
        self.delete_contents = delete_contents
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to

        self.log.info('Dataset id: %s', self.dataset_id)
        self.log.info('Project id: %s', self.project_id)

        super().__init__(*args, **kwargs)

    def execute(self, context):
        bq_hook = BigQueryHook(bigquery_conn_id=self.gcp_conn_id,
                               delegate_to=self.delegate_to)

        conn = bq_hook.get_conn()
        cursor = conn.cursor()

        cursor.delete_dataset(
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            delete_contents=self.delete_contents
        )


class BigQueryCreateEmptyDatasetOperator(BaseOperator):
    """
    This operator is used to create new dataset for your Project in Big query.
    https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets#resource

    :param project_id: The name of the project where we want to create the dataset.
        Don't need to provide, if projectId in dataset_reference.
    :type project_id: str
    :param dataset_id: The id of dataset. Don't need to provide,
        if datasetId in dataset_reference.
    :type dataset_id: str
    :param dataset_reference: Dataset reference that could be provided with request body.
        More info:
        https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets#resource
    :type dataset_reference: dict
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud Platform.
    :type gcp_conn_id: str
    :param bigquery_conn_id: (Deprecated) The connection ID used to connect to Google Cloud Platform.
        This parameter has been deprecated. You should pass the gcp_conn_id parameter instead.
    :type bigquery_conn_id: str
        **Example**: ::

            create_new_dataset = BigQueryCreateEmptyDatasetOperator(
                dataset_id='new-dataset',
                project_id='my-project',
                dataset_reference={"friendlyName": "New Dataset"}
                gcp_conn_id='_my_gcp_conn_',
                task_id='newDatasetCreator',
                dag=dag)

    """

    template_fields = ('dataset_id', 'project_id')
    ui_color = '#f0eee4'

    @apply_defaults
    def __init__(self,
                 dataset_id,
                 project_id=None,
                 dataset_reference=None,
                 gcp_conn_id='google_cloud_default',
                 bigquery_conn_id=None,
                 delegate_to=None,
                 *args, **kwargs):

        if bigquery_conn_id:
            warnings.warn(
                "The bigquery_conn_id parameter has been deprecated. You should pass "
                "the gcp_conn_id parameter.", DeprecationWarning, stacklevel=3)
            gcp_conn_id = bigquery_conn_id

        self.dataset_id = dataset_id
        self.project_id = project_id
        self.gcp_conn_id = gcp_conn_id
        self.dataset_reference = dataset_reference if dataset_reference else {}
        self.delegate_to = delegate_to

        self.log.info('Dataset id: %s', self.dataset_id)
        self.log.info('Project id: %s', self.project_id)

        super().__init__(*args, **kwargs)

    def execute(self, context):
        bq_hook = BigQueryHook(bigquery_conn_id=self.gcp_conn_id,
                               delegate_to=self.delegate_to)

        conn = bq_hook.get_conn()
        cursor = conn.cursor()

        cursor.create_empty_dataset(
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            dataset_reference=self.dataset_reference)


class BigQueryGetDatasetOperator(BaseOperator):
    """
    This operator is used to return the dataset specified by dataset_id.

    :param dataset_id: The id of dataset. Don't need to provide,
        if datasetId in dataset_reference.
    :type dataset_id: str
    :param project_id: The name of the project where we want to create the dataset.
        Don't need to provide, if projectId in dataset_reference.
    :type project_id: str
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud Platform.
    :type gcp_conn_id: str
    :rtype: dataset
        https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets#resource
    """

    template_fields = ('dataset_id', 'project_id')
    ui_color = '#f0eee4'

    @apply_defaults
    def __init__(self,
                 dataset_id,
                 project_id=None,
                 gcp_conn_id='google_cloud_default',
                 delegate_to=None,
                 *args, **kwargs):
        self.dataset_id = dataset_id
        self.project_id = project_id
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        super().__init__(*args, **kwargs)

    def execute(self, context):
        bq_hook = BigQueryHook(bigquery_conn_id=self.gcp_conn_id,
                               delegate_to=self.delegate_to)
        conn = bq_hook.get_conn()
        cursor = conn.cursor()

        self.log.info('Start getting dataset: %s:%s', self.project_id, self.dataset_id)

        return cursor.get_dataset(
            dataset_id=self.dataset_id,
            project_id=self.project_id)


class BigQueryPatchDatasetOperator(BaseOperator):
    """
    This operator is used to patch dataset for your Project in BigQuery.
    It only replaces fields that are provided in the submitted dataset resource.

    :param dataset_id: The id of dataset. Don't need to provide,
        if datasetId in dataset_reference.
    :type dataset_id: str
    :param dataset_resource: Dataset resource that will be provided with request body.
        https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets#resource
    :type dataset_resource: dict
    :param project_id: The name of the project where we want to create the dataset.
        Don't need to provide, if projectId in dataset_reference.
    :type project_id: str
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud Platform.
    :type gcp_conn_id: str
    :rtype: dataset
        https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets#resource
    """

    template_fields = ('dataset_id', 'project_id')
    ui_color = '#f0eee4'

    @apply_defaults
    def __init__(self,
                 dataset_id,
                 dataset_resource,
                 project_id=None,
                 gcp_conn_id='google_cloud_default',
                 delegate_to=None,
                 *args, **kwargs):
        self.dataset_id = dataset_id
        self.project_id = project_id
        self.gcp_conn_id = gcp_conn_id
        self.dataset_resource = dataset_resource
        self.delegate_to = delegate_to
        super().__init__(*args, **kwargs)

    def execute(self, context):
        bq_hook = BigQueryHook(bigquery_conn_id=self.gcp_conn_id,
                               delegate_to=self.delegate_to)

        conn = bq_hook.get_conn()
        cursor = conn.cursor()

        self.log.info('Start patching dataset: %s:%s', self.project_id, self.dataset_id)

        return cursor.patch_dataset(
            dataset_id=self.dataset_id,
            dataset_resource=self.dataset_resource,
            project_id=self.project_id)


class BigQueryUpdateDatasetOperator(BaseOperator):
    """
    This operator is used to update dataset for your Project in BigQuery.
    The update method replaces the entire dataset resource, whereas the patch
    method only replaces fields that are provided in the submitted dataset resource.

    :param dataset_id: The id of dataset. Don't need to provide,
        if datasetId in dataset_reference.
    :type dataset_id: str
    :param dataset_resource: Dataset resource that will be provided with request body.
        https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets#resource
    :type dataset_resource: dict
    :param project_id: The name of the project where we want to create the dataset.
        Don't need to provide, if projectId in dataset_reference.
    :type project_id: str
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud Platform.
    :type gcp_conn_id: str
    :rtype: dataset
        https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets#resource
    """

    template_fields = ('dataset_id', 'project_id')
    ui_color = '#f0eee4'

    @apply_defaults
    def __init__(self,
                 dataset_id,
                 dataset_resource,
                 project_id=None,
                 gcp_conn_id='google_cloud_default',
                 delegate_to=None,
                 *args, **kwargs):
        self.dataset_id = dataset_id
        self.project_id = project_id
        self.gcp_conn_id = gcp_conn_id
        self.dataset_resource = dataset_resource
        self.delegate_to = delegate_to
        super().__init__(*args, **kwargs)

    def execute(self, context):
        bq_hook = BigQueryHook(bigquery_conn_id=self.gcp_conn_id,
                               delegate_to=self.delegate_to)

        conn = bq_hook.get_conn()
        cursor = conn.cursor()

        self.log.info('Start updating dataset: %s:%s', self.project_id, self.dataset_id)

        return cursor.update_dataset(
            dataset_id=self.dataset_id,
            dataset_resource=self.dataset_resource,
            project_id=self.project_id)
