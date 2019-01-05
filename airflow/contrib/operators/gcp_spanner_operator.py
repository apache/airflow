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
import six

from airflow import AirflowException
from airflow.contrib.hooks.gcp_spanner_hook import CloudSpannerHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class CloudSpannerInstanceDeployOperator(BaseOperator):
    """
    Creates a new Cloud Spanner instance, or if an instance with the same instance_id
    exists in the specified project, updates the Cloud Spanner instance.

    :param project_id: The ID of the project which owns the Cloud Spanner Database.
    :type project_id: str
    :param instance_id: Cloud Spanner instance ID.
    :type instance_id: str
    :param configuration_name:  The name of the Cloud Spanner instance configuration
      defining how the instance will be created. Required for
      instances that do not yet exist.
    :type configuration_name: str
    :param node_count: (Optional) The number of nodes allocated to the Cloud Spanner
      instance.
    :type node_count: int
    :param display_name: (Optional) The display name for the Cloud Spanner  instance in
      the GCP Console. (Must be between 4 and 30 characters.) If this value is not set
      in the constructor, the name is the same as the instance ID.
    :type display_name: str
    :param gcp_conn_id: The connection ID used to connect to Google Cloud Platform.
    :type gcp_conn_id: str
    """
    # [START gcp_spanner_deploy_template_fields]
    template_fields = ('project_id', 'instance_id', 'configuration_name', 'display_name',
                       'gcp_conn_id')
    # [END gcp_spanner_deploy_template_fields]

    @apply_defaults
    def __init__(self,
                 project_id,
                 instance_id,
                 configuration_name,
                 node_count,
                 display_name,
                 gcp_conn_id='google_cloud_default',
                 *args, **kwargs):
        self.instance_id = instance_id
        self.project_id = project_id
        self.configuration_name = configuration_name
        self.node_count = node_count
        self.display_name = display_name
        self.gcp_conn_id = gcp_conn_id
        self._validate_inputs()
        self._hook = CloudSpannerHook(gcp_conn_id=gcp_conn_id)
        super(CloudSpannerInstanceDeployOperator, self).__init__(*args, **kwargs)

    def _validate_inputs(self):
        if not self.project_id:
            raise AirflowException("The required parameter 'project_id' is empty")
        if not self.instance_id:
            raise AirflowException("The required parameter 'instance_id' is empty")

    def execute(self, context):
        if not self._hook.get_instance(self.project_id, self.instance_id):
            self.log.info("Creating Cloud Spanner instance '%s'", self.instance_id)
            func = self._hook.create_instance
        else:
            self.log.info("Updating Cloud Spanner instance '%s'", self.instance_id)
            func = self._hook.update_instance
        return func(self.project_id,
                    self.instance_id,
                    self.configuration_name,
                    self.node_count,
                    self.display_name)


class CloudSpannerInstanceDeleteOperator(BaseOperator):
    """
    Deletes a Cloud Spanner instance. If an instance does not exist,
    no action is taken and the operator succeeds.

    :param project_id: The ID of the project that owns the Cloud Spanner Database.
    :type project_id: str
    :param instance_id: The Cloud Spanner instance ID.
    :type instance_id: str
    :param gcp_conn_id: The connection ID used to connect to Google Cloud Platform.
    :type gcp_conn_id: str
    """
    # [START gcp_spanner_delete_template_fields]
    template_fields = ('project_id', 'instance_id', 'gcp_conn_id')
    # [END gcp_spanner_delete_template_fields]

    @apply_defaults
    def __init__(self,
                 project_id,
                 instance_id,
                 gcp_conn_id='google_cloud_default',
                 *args, **kwargs):
        self.instance_id = instance_id
        self.project_id = project_id
        self.gcp_conn_id = gcp_conn_id
        self._validate_inputs()
        self._hook = CloudSpannerHook(gcp_conn_id=gcp_conn_id)
        super(CloudSpannerInstanceDeleteOperator, self).__init__(*args, **kwargs)

    def _validate_inputs(self):
        if not self.project_id:
            raise AirflowException("The required parameter 'project_id' is empty")
        if not self.instance_id:
            raise AirflowException("The required parameter 'instance_id' is empty")

    def execute(self, context):
        if self._hook.get_instance(self.project_id, self.instance_id):
            return self._hook.delete_instance(self.project_id,
                                              self.instance_id)
        else:
            self.log.info("Instance '%s' does not exist in project '%s'. "
                          "Aborting delete.", self.instance_id, self.project_id)
            return True


class CloudSpannerInstanceDatabaseQueryOperator(BaseOperator):
    """
    Executes an arbitrary DML query (INSERT, UPDATE, DELETE).

    :param project_id: The ID of the project that owns the Cloud Spanner Database.
    :type project_id: str
    :param instance_id: The Cloud Spanner instance ID.
    :type instance_id: str
    :param database_id: The Cloud Spanner database ID.
    :type database_id: str
    :param query: The query or list of queries to be executed. Can be a path to a SQL
       file.
    :type query: str or list
    :param gcp_conn_id: The connection ID used to connect to Google Cloud Platform.
    :type gcp_conn_id: str
    """
    # [START gcp_spanner_query_template_fields]
    template_fields = ('project_id', 'instance_id', 'database_id', 'query', 'gcp_conn_id')
    template_ext = ('.sql',)
    # [END gcp_spanner_query_template_fields]

    @apply_defaults
    def __init__(self,
                 project_id,
                 instance_id,
                 database_id,
                 query,
                 gcp_conn_id='google_cloud_default',
                 *args, **kwargs):
        self.instance_id = instance_id
        self.project_id = project_id
        self.database_id = database_id
        self.query = query
        self.gcp_conn_id = gcp_conn_id
        self._validate_inputs()
        self._hook = CloudSpannerHook(gcp_conn_id=gcp_conn_id)
        super(CloudSpannerInstanceDatabaseQueryOperator, self).__init__(*args, **kwargs)

    def _validate_inputs(self):
        if not self.project_id:
            raise AirflowException("The required parameter 'project_id' is empty")
        if not self.instance_id:
            raise AirflowException("The required parameter 'instance_id' is empty")
        if not self.database_id:
            raise AirflowException("The required parameter 'database_id' is empty")
        if not self.query:
            raise AirflowException("The required parameter 'query' is empty")

    def execute(self, context):
        queries = self.query
        if isinstance(self.query, six.string_types):
            queries = [x.strip() for x in self.query.split(';')]
            self.sanitize_queries(queries)
        self.log.info("Executing DML query(-ies) on "
                      "projects/%s/instances/%s/databases/%s",
                      self.project_id, self.instance_id, self.database_id)
        self.log.info(queries)
        self._hook.execute_dml(self.project_id, self.instance_id,
                               self.database_id, queries)

    @staticmethod
    def sanitize_queries(queries):
        if len(queries) and queries[-1] == '':
            del queries[-1]


class CloudSpannerInstanceDatabaseDeployOperator(BaseOperator):
    """
    Creates a new Cloud Spanner database, or if database exists,
    the operator does nothing.

    :param project_id: The ID of the project that owns the Cloud Spanner Database.
    :type project_id: str
    :param instance_id: The Cloud Spanner instance ID.
    :type instance_id: str
    :param database_id: The Cloud Spanner database ID.
    :type database_id: str
    :param ddl_statements: The string list containing DDL for the new database.
    :type ddl_statements: list of str
    :param gcp_conn_id: The connection ID used to connect to Google Cloud Platform.
    :type gcp_conn_id: str
    """
    # [START gcp_spanner_database_deploy_template_fields]
    template_fields = ('project_id', 'instance_id', 'database_id', 'ddl_statements',
                       'gcp_conn_id')
    template_ext = ('.sql', )
    # [END gcp_spanner_database_deploy_template_fields]

    @apply_defaults
    def __init__(self,
                 project_id,
                 instance_id,
                 database_id,
                 ddl_statements,
                 gcp_conn_id='google_cloud_default',
                 *args, **kwargs):
        self.instance_id = instance_id
        self.project_id = project_id
        self.database_id = database_id
        self.ddl_statements = ddl_statements
        self.gcp_conn_id = gcp_conn_id
        self._validate_inputs()
        self._hook = CloudSpannerHook(gcp_conn_id=gcp_conn_id)
        super(CloudSpannerInstanceDatabaseDeployOperator, self).__init__(*args, **kwargs)

    def _validate_inputs(self):
        if not self.project_id:
            raise AirflowException("The required parameter 'project_id' is empty")
        if not self.instance_id:
            raise AirflowException("The required parameter 'instance_id' is empty")
        if not self.database_id:
            raise AirflowException("The required parameter 'database_id' is empty")
        if not self.ddl_statements:
            raise AirflowException("The required parameter 'ddl_statements' is empty")

    def execute(self, context):
        if not self._hook.get_database(self.project_id,
                                       self.instance_id,
                                       self.database_id):
            self.log.info("Creating Cloud Spanner database "
                          "'%s' in project '%s' and instance '%s'",
                          self.database_id, self.project_id, self.instance_id)
            return self._hook.create_database(project_id=self.project_id,
                                              instance_id=self.instance_id,
                                              database_id=self.database_id,
                                              ddl_statements=self.ddl_statements)
        else:
            self.log.info("The database '%s' in project '%s' and instance '%s'"
                          " already exists. Nothing to do. Exiting.",
                          self.database_id, self.project_id, self.instance_id)
        return True


class CloudSpannerInstanceDatabaseUpdateOperator(BaseOperator):
    """
    Updates a Cloud Spanner database with the specified DDL statement.

    :param project_id: The ID of the project that owns the the Cloud Spanner Database.
    :type project_id: str
    :param instance_id: The Cloud Spanner instance ID.
    :type instance_id: str
    :param database_id: The Cloud Spanner database ID.
    :type database_id: str
    :param ddl_statements: The string list containing DDL to apply to the database.
    :type ddl_statements: list[str]
    :param operation_id: (Optional) Unique per database operation id that can
           be specified to implement idempotency check.
    :type operation_id: str
    :param gcp_conn_id: The connection ID used to connect to Google Cloud Platform.
    :type gcp_conn_id: str
    """
    # [START gcp_spanner_database_update_template_fields]
    template_fields = ('project_id', 'instance_id', 'database_id', 'ddl_statements',
                       'gcp_conn_id')
    template_ext = ('.sql', )
    # [END gcp_spanner_database_update_template_fields]

    @apply_defaults
    def __init__(self,
                 project_id,
                 instance_id,
                 database_id,
                 ddl_statements,
                 operation_id=None,
                 gcp_conn_id='google_cloud_default',
                 *args, **kwargs):
        self.instance_id = instance_id
        self.project_id = project_id
        self.database_id = database_id
        self.ddl_statements = ddl_statements
        self.operation_id = operation_id
        self.gcp_conn_id = gcp_conn_id
        self._validate_inputs()
        self._hook = CloudSpannerHook(gcp_conn_id=gcp_conn_id)
        super(CloudSpannerInstanceDatabaseUpdateOperator, self).__init__(*args, **kwargs)

    def _validate_inputs(self):
        if not self.project_id:
            raise AirflowException("The required parameter 'project_id' is empty")
        if not self.instance_id:
            raise AirflowException("The required parameter 'instance_id' is empty")
        if not self.database_id:
            raise AirflowException("The required parameter 'database_id' is empty")
        if not self.ddl_statements:
            raise AirflowException("The required parameter 'ddl_statements' is empty")

    def execute(self, context):
        if not self._hook.get_database(self.project_id,
                                       self.instance_id,
                                       self.database_id):
            raise AirflowException("The Cloud Spanner database "
                                   "'%s' in project '%s' and instance '%s' is missing."
                                   " Create the database first before you can update it.",
                                   self.database_id, self.project_id, self.instance_id)
        else:
            return self._hook.update_database(project_id=self.project_id,
                                              instance_id=self.instance_id,
                                              database_id=self.database_id,
                                              ddl_statements=self.ddl_statements,
                                              operation_id=self.operation_id)


class CloudSpannerInstanceDatabaseDeleteOperator(BaseOperator):
    """
    Deletes a Cloud Spanner database.

    :param project_id: The ID of the project that owns the Cloud Spanner Database.
    :type project_id: str
    :param instance_id: Cloud Spanner instance ID.
    :type instance_id: str
    :param database_id: Cloud Spanner database ID.
    :type database_id: str
    :param gcp_conn_id: The connection ID used to connect to Google Cloud Platform.
    :type gcp_conn_id: str
    """
    # [START gcp_spanner_database_delete_template_fields]
    template_fields = ('project_id', 'instance_id', 'database_id',
                       'gcp_conn_id')
    # [END gcp_spanner_database_delete_template_fields]

    @apply_defaults
    def __init__(self,
                 project_id,
                 instance_id,
                 database_id,
                 gcp_conn_id='google_cloud_default',
                 *args, **kwargs):
        self.instance_id = instance_id
        self.project_id = project_id
        self.database_id = database_id
        self.gcp_conn_id = gcp_conn_id
        self._validate_inputs()
        self._hook = CloudSpannerHook(gcp_conn_id=gcp_conn_id)
        super(CloudSpannerInstanceDatabaseDeleteOperator, self).__init__(*args, **kwargs)

    def _validate_inputs(self):
        if not self.project_id:
            raise AirflowException("The required parameter 'project_id' is empty")
        if not self.instance_id:
            raise AirflowException("The required parameter 'instance_id' is empty")
        if not self.database_id:
            raise AirflowException("The required parameter 'database_id' is empty")

    def execute(self, context):
        db = self._hook.get_database(self.project_id,
                                     self.instance_id,
                                     self.database_id)
        if not db:
            self.log.info("The Cloud Spanner database was missing: "
                          "'%s' in project '%s' and instance '%s'. Assuming success.",
                          self.database_id, self.project_id, self.instance_id)
            return True
        else:
            return self._hook.delete_database(project_id=self.project_id,
                                              instance_id=self.instance_id,
                                              database_id=self.database_id)
