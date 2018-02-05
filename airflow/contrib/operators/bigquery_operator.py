# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json

from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook, _parse_gcs_url
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class BigQueryOperator(BaseOperator):
    """
    Executes BigQuery SQL queries in a specific BigQuery database

    :param bql: the sql code to be executed
    :type bql: Can receive a str representing a sql statement,
        a list of str (sql statements), or reference to a template file.
        Template reference are recognized by str ending in '.sql'
    :param destination_dataset_table: A dotted
        (<project>.|<project>:)<dataset>.<table> that, if set, will store the results
        of the query.
    :type destination_dataset_table: string
    :param write_disposition: Specifies the action that occurs if the destination table
        already exists. (default: 'WRITE_EMPTY')
    :type write_disposition: string
    :param create_disposition: Specifies whether the job is allowed to create new tables.
        (default: 'CREATE_IF_NEEDED')
    :type create_disposition: string
    :param bigquery_conn_id: reference to a specific BigQuery hook.
    :type bigquery_conn_id: string
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must have domain-wide
        delegation enabled.
    :type delegate_to: string
    :param udf_config: The User Defined Function configuration for the query.
        See https://cloud.google.com/bigquery/user-defined-functions for details.
    :type udf_config: list
    :param use_legacy_sql: Whether to use legacy SQL (true) or standard SQL (false).
    :type use_legacy_sql: boolean
    :param maximum_billing_tier: Positive integer that serves as a multiplier
        of the basic price.
        Defaults to None, in which case it uses the value set in the project.
    :type maximum_billing_tier: integer
    :param schema_update_options: Allows the schema of the desitination
        table to be updated as a side effect of the load job.
    :type schema_update_options: tuple
    :param query_params: a dictionary containing query parameter types and
        values, passed to BigQuery.
    :type query_params: dict

    """
    template_fields = ('bql', 'destination_dataset_table')
    template_ext = ('.sql', )
    ui_color = '#e4f0e8'

    @apply_defaults
    def __init__(self,
                 bql,
                 destination_dataset_table=False,
                 write_disposition='WRITE_EMPTY',
                 allow_large_results=False,
                 bigquery_conn_id='bigquery_default',
                 delegate_to=None,
                 udf_config=False,
                 use_legacy_sql=True,
                 maximum_billing_tier=None,
                 create_disposition='CREATE_IF_NEEDED',
                 schema_update_options=(),
                 query_params=None,
                 priority='INTERACTIVE',
                 *args,
                 **kwargs):
        super(BigQueryOperator, self).__init__(*args, **kwargs)
        self.bql = bql
        self.destination_dataset_table = destination_dataset_table
        self.write_disposition = write_disposition
        self.create_disposition = create_disposition
        self.allow_large_results = allow_large_results
        self.bigquery_conn_id = bigquery_conn_id
        self.delegate_to = delegate_to
        self.udf_config = udf_config
        self.use_legacy_sql = use_legacy_sql
        self.maximum_billing_tier = maximum_billing_tier
        self.schema_update_options = schema_update_options
        self.query_params = query_params
        self.bq_cursor = None
        self.priority = priority

    def execute(self, context):
        if self.bq_cursor is None:
            self.log.info('Executing: %s', self.bql)
            hook = BigQueryHook(
                bigquery_conn_id=self.bigquery_conn_id,
                use_legacy_sql=self.use_legacy_sql,
                delegate_to=self.delegate_to)
            conn = hook.get_conn()
            self.bq_cursor = conn.cursor()
        self.bq_cursor.run_query(
            self.bql,
            destination_dataset_table=self.destination_dataset_table,
            write_disposition=self.write_disposition,
            allow_large_results=self.allow_large_results,
            udf_config=self.udf_config,
            maximum_billing_tier=self.maximum_billing_tier,
            create_disposition=self.create_disposition,
            query_params=self.query_params,
            schema_update_options=self.schema_update_options,
            priority=self.priority)

    def on_kill(self):
        super(BigQueryOperator, self).on_kill()
        if self.bq_cursor is not None:
            self.log.info('Canceling running query due to execution timeout')
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

    :param project_id: The project to create the table into.
    :type project_id: string
    :param dataset_id: The dataset to create the table into.
    :type dataset_id: string
    :param table_id: The Name of the table to be created.
    :type table_id: string
    :param schema_fields: If set, the schema field list as defined here:
        https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs#configuration.load.schema

        **Example**: ::

            schema_fields=[{"name": "emp_name", "type": "STRING", "mode": "REQUIRED"},
                           {"name": "salary", "type": "INTEGER", "mode": "NULLABLE"}]

    :type schema_fields: list
    :param gcs_schema_object: Full path to the JSON file containing schema. For
        example: ``gs://test-bucket/dir1/dir2/employee_schema.json``
    :type gcs_schema_object: string
    :param time_partitioning: configure optional time partitioning fields i.e.
        partition by field, type and  expiration as per API specifications.

        .. seealso::
            https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#timePartitioning
    :type time_partitioning: dict
    :param bigquery_conn_id: Reference to a specific BigQuery hook.
    :type bigquery_conn_id: string
    :param google_cloud_storage_conn_id: Reference to a specific Google
        cloud storage hook.
    :type google_cloud_storage_conn_id: string
    :param delegate_to: The account to impersonate, if any. For this to
        work, the service account making the request must have domain-wide
        delegation enabled.
    :type delegate_to: string

    **Example (with schema JSON in GCS)**: ::

        CreateTable = BigQueryCreateEmptyTableOperator(
            task_id='BigQueryCreateEmptyTableOperator_task',
            dataset_id='ODS',
            table_id='Employees',
            project_id='internal-gcp-project',
            gcs_schema_object='gs://schema-bucket/employee_schema.json',
            bigquery_conn_id='airflow-service-account',
            google_cloud_storage_conn_id='airflow-service-account'
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
            bigquery_conn_id='airflow-service-account',
            google_cloud_storage_conn_id='airflow-service-account'
        )

    """
    template_fields = ('dataset_id', 'table_id', 'project_id', 'gcs_schema_object')
    ui_color = '#f0eee4'

    @apply_defaults
    def __init__(self,
                 dataset_id,
                 table_id,
                 project_id=None,
                 schema_fields=None,
                 gcs_schema_object=None,
                 time_partitioning={},
                 bigquery_conn_id='bigquery_default',
                 google_cloud_storage_conn_id='google_cloud_storage_default',
                 delegate_to=None,
                 *args, **kwargs):

        super(BigQueryCreateEmptyTableOperator, self).__init__(*args, **kwargs)

        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.schema_fields = schema_fields
        self.gcs_schema_object = gcs_schema_object
        self.bigquery_conn_id = bigquery_conn_id
        self.google_cloud_storage_conn_id = google_cloud_storage_conn_id
        self.delegate_to = delegate_to
        self.time_partitioning = time_partitioning

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
            time_partitioning=self.time_partitioning
        )
