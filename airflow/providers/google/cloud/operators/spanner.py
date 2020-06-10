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
This module contains Google Spanner operators.
"""
from typing import List, Optional, Union

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.spanner import SpannerHook
from airflow.utils.decorators import apply_defaults


class SpannerDeployInstanceOperator(BaseOperator):
    """
    Creates a new Cloud Spanner instance, or if an instance with the same instance_id
    exists in the specified project, updates the Cloud Spanner instance.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:SpannerDeployInstanceOperator`

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
    :param project_id: Optional, the ID of the project which owns the Cloud Spanner
        Database.  If set to None or missing, the default project_id from the GCP connection is used.
    :type project_id: str
    :param gcp_conn_id: The connection ID used to connect to Google Cloud Platform.
    :type gcp_conn_id: str
    """
    # [START gcp_spanner_deploy_template_fields]
    template_fields = ('project_id', 'instance_id', 'configuration_name', 'display_name',
                       'gcp_conn_id')
    # [END gcp_spanner_deploy_template_fields]

    @apply_defaults
    def __init__(self,
                 instance_id: int,
                 configuration_name: str,
                 node_count: str,
                 display_name: str,
                 project_id: Optional[str] = None,
                 gcp_conn_id: str = 'google_cloud_default',
                 *args, **kwargs) -> None:
        self.instance_id = instance_id
        self.project_id = project_id
        self.configuration_name = configuration_name
        self.node_count = node_count
        self.display_name = display_name
        self.gcp_conn_id = gcp_conn_id
        self._validate_inputs()
        super().__init__(*args, **kwargs)

    def _validate_inputs(self):
        if self.project_id == '':
            raise AirflowException("The required parameter 'project_id' is empty")
        if not self.instance_id:
            raise AirflowException("The required parameter 'instance_id' "
                                   "is empty or None")

    def execute(self, context):
        hook = SpannerHook(gcp_conn_id=self.gcp_conn_id)
        if not hook.get_instance(project_id=self.project_id, instance_id=self.instance_id):
            self.log.info("Creating Cloud Spanner instance '%s'", self.instance_id)
            func = hook.create_instance
        else:
            self.log.info("Updating Cloud Spanner instance '%s'", self.instance_id)
            func = hook.update_instance
        func(project_id=self.project_id,
             instance_id=self.instance_id,
             configuration_name=self.configuration_name,
             node_count=self.node_count,
             display_name=self.display_name)


class SpannerDeleteInstanceOperator(BaseOperator):
    """
    Deletes a Cloud Spanner instance. If an instance does not exist,
    no action is taken and the operator succeeds.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:SpannerDeleteInstanceOperator`

    :param instance_id: The Cloud Spanner instance ID.
    :type instance_id: str
    :param project_id: Optional, the ID of the project that owns the Cloud Spanner
        Database.  If set to None or missing, the default project_id from the GCP connection is used.
    :type project_id: str
    :param gcp_conn_id: The connection ID used to connect to Google Cloud Platform.
    :type gcp_conn_id: str
    """
    # [START gcp_spanner_delete_template_fields]
    template_fields = ('project_id', 'instance_id', 'gcp_conn_id')
    # [END gcp_spanner_delete_template_fields]

    @apply_defaults
    def __init__(self,
                 instance_id: int,
                 project_id: Optional[str] = None,
                 gcp_conn_id: str = 'google_cloud_default',
                 *args, **kwargs) -> None:
        self.instance_id = instance_id
        self.project_id = project_id
        self.gcp_conn_id = gcp_conn_id
        self._validate_inputs()
        super().__init__(*args, **kwargs)

    def _validate_inputs(self):
        if self.project_id == '':
            raise AirflowException("The required parameter 'project_id' is empty")
        if not self.instance_id:
            raise AirflowException("The required parameter 'instance_id' "
                                   "is empty or None")

    def execute(self, context):
        hook = SpannerHook(gcp_conn_id=self.gcp_conn_id)
        if hook.get_instance(project_id=self.project_id, instance_id=self.instance_id):
            return hook.delete_instance(project_id=self.project_id,
                                        instance_id=self.instance_id)
        else:
            self.log.info("Instance '%s' does not exist in project '%s'. "
                          "Aborting delete.", self.instance_id, self.project_id)
            return True


class SpannerQueryDatabaseInstanceOperator(BaseOperator):
    """
    Executes an arbitrary DML query (INSERT, UPDATE, DELETE).

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:SpannerQueryDatabaseInstanceOperator`

    :param instance_id: The Cloud Spanner instance ID.
    :type instance_id: str
    :param database_id: The Cloud Spanner database ID.
    :type database_id: str
    :param query: The query or list of queries to be executed. Can be a path to a SQL
       file.
    :type query: str or list
    :param project_id: Optional, the ID of the project that owns the Cloud Spanner
        Database.  If set to None or missing, the default project_id from the GCP connection is used.
    :type project_id: str
    :param gcp_conn_id: The connection ID used to connect to Google Cloud Platform.
    :type gcp_conn_id: str
    """
    # [START gcp_spanner_query_template_fields]
    template_fields = ('project_id', 'instance_id', 'database_id', 'query', 'gcp_conn_id')
    template_ext = ('.sql',)
    # [END gcp_spanner_query_template_fields]

    @apply_defaults
    def __init__(self,
                 instance_id: int,
                 database_id: str,
                 query: Union[str, List[str]],
                 project_id: Optional[str] = None,
                 gcp_conn_id: str = 'google_cloud_default',
                 *args, **kwargs) -> None:
        self.instance_id = instance_id
        self.project_id = project_id
        self.database_id = database_id
        self.query = query
        self.gcp_conn_id = gcp_conn_id
        self._validate_inputs()
        super().__init__(*args, **kwargs)

    def _validate_inputs(self):
        if self.project_id == '':
            raise AirflowException("The required parameter 'project_id' is empty")
        if not self.instance_id:
            raise AirflowException("The required parameter 'instance_id' "
                                   "is empty or None")
        if not self.database_id:
            raise AirflowException("The required parameter 'database_id' "
                                   "is empty or None")
        if not self.query:
            raise AirflowException("The required parameter 'query' is empty")

    def execute(self, context):
        hook = SpannerHook(gcp_conn_id=self.gcp_conn_id)
        queries = self.query
        if isinstance(self.query, str):
            queries = [x.strip() for x in self.query.split(';')]
            self.sanitize_queries(queries)
        self.log.info("Executing DML query(-ies) on "
                      "projects/%s/instances/%s/databases/%s",
                      self.project_id, self.instance_id, self.database_id)
        self.log.info(queries)
        hook.execute_dml(project_id=self.project_id,
                         instance_id=self.instance_id,
                         database_id=self.database_id,
                         queries=queries)

    @staticmethod
    def sanitize_queries(queries):
        """
        Drops empty query in queries.

        :param queries: queries
        :type queries: List[str]
        :rtype: None
        """
        if queries and queries[-1] == '':
            del queries[-1]


class SpannerDeployDatabaseInstanceOperator(BaseOperator):
    """
    Creates a new Cloud Spanner database, or if database exists,
    the operator does nothing.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:SpannerDeployDatabaseInstanceOperator`

    :param instance_id: The Cloud Spanner instance ID.
    :type instance_id: str
    :param database_id: The Cloud Spanner database ID.
    :type database_id: str
    :param ddl_statements: The string list containing DDL for the new database.
    :type ddl_statements: list[str]
    :param project_id: Optional, the ID of the project that owns the Cloud Spanner
        Database.  If set to None or missing, the default project_id from the GCP connection is used.
    :type project_id: str
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
                 instance_id: int,
                 database_id: str,
                 ddl_statements: List[str],
                 project_id: Optional[str] = None,
                 gcp_conn_id: str = 'google_cloud_default',
                 *args, **kwargs) -> None:
        self.instance_id = instance_id
        self.project_id = project_id
        self.database_id = database_id
        self.ddl_statements = ddl_statements
        self.gcp_conn_id = gcp_conn_id
        self._validate_inputs()
        super().__init__(*args, **kwargs)

    def _validate_inputs(self):
        if self.project_id == '':
            raise AirflowException("The required parameter 'project_id' is empty")
        if not self.instance_id:
            raise AirflowException("The required parameter 'instance_id' is empty "
                                   "or None")
        if not self.database_id:
            raise AirflowException("The required parameter 'database_id' is empty"
                                   " or None")

    def execute(self, context):
        hook = SpannerHook(gcp_conn_id=self.gcp_conn_id)
        if not hook.get_database(project_id=self.project_id,
                                 instance_id=self.instance_id,
                                 database_id=self.database_id):
            self.log.info("Creating Cloud Spanner database "
                          "'%s' in project '%s' and instance '%s'",
                          self.database_id, self.project_id, self.instance_id)
            return hook.create_database(project_id=self.project_id,
                                        instance_id=self.instance_id,
                                        database_id=self.database_id,
                                        ddl_statements=self.ddl_statements)
        else:
            self.log.info("The database '%s' in project '%s' and instance '%s'"
                          " already exists. Nothing to do. Exiting.",
                          self.database_id, self.project_id, self.instance_id)
        return True


class SpannerUpdateDatabaseInstanceOperator(BaseOperator):
    """
    Updates a Cloud Spanner database with the specified DDL statement.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:SpannerUpdateDatabaseInstanceOperator`

    :param instance_id: The Cloud Spanner instance ID.
    :type instance_id: str
    :param database_id: The Cloud Spanner database ID.
    :type database_id: str
    :param ddl_statements: The string list containing DDL to apply to the database.
    :type ddl_statements: list[str]
    :param project_id: Optional, the ID of the project that owns the Cloud Spanner
        Database.  If set to None or missing, the default project_id from the GCP connection is used.
    :type project_id: str
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
                 instance_id: int,
                 database_id: str,
                 ddl_statements: List[str],
                 project_id: Optional[str] = None,
                 operation_id: Optional[str] = None,
                 gcp_conn_id: str = 'google_cloud_default',
                 *args, **kwargs) -> None:
        self.instance_id = instance_id
        self.project_id = project_id
        self.database_id = database_id
        self.ddl_statements = ddl_statements
        self.operation_id = operation_id
        self.gcp_conn_id = gcp_conn_id
        self._validate_inputs()
        super().__init__(*args, **kwargs)

    def _validate_inputs(self):
        if self.project_id == '':
            raise AirflowException("The required parameter 'project_id' is empty")
        if not self.instance_id:
            raise AirflowException("The required parameter 'instance_id' is empty"
                                   " or None")
        if not self.database_id:
            raise AirflowException("The required parameter 'database_id' is empty"
                                   " or None")
        if not self.ddl_statements:
            raise AirflowException("The required parameter 'ddl_statements' is empty"
                                   " or None")

    def execute(self, context):
        hook = SpannerHook(gcp_conn_id=self.gcp_conn_id)
        if not hook.get_database(project_id=self.project_id,
                                 instance_id=self.instance_id,
                                 database_id=self.database_id):
            raise AirflowException("The Cloud Spanner database '{}' in project '{}' and "
                                   "instance '{}' is missing. Create the database first "
                                   "before you can update it.".format(self.database_id,
                                                                      self.project_id,
                                                                      self.instance_id))
        else:
            return hook.update_database(project_id=self.project_id,
                                        instance_id=self.instance_id,
                                        database_id=self.database_id,
                                        ddl_statements=self.ddl_statements,
                                        operation_id=self.operation_id)


class SpannerDeleteDatabaseInstanceOperator(BaseOperator):
    """
    Deletes a Cloud Spanner database.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:SpannerDeleteDatabaseInstanceOperator`

    :param instance_id: Cloud Spanner instance ID.
    :type instance_id: str
    :param database_id: Cloud Spanner database ID.
    :type database_id: str
    :param project_id: Optional, the ID of the project that owns the Cloud Spanner
        Database.  If set to None or missing, the default project_id from the GCP connection is used.
    :type project_id: str
    :param gcp_conn_id: The connection ID used to connect to Google Cloud Platform.
    :type gcp_conn_id: str
    """
    # [START gcp_spanner_database_delete_template_fields]
    template_fields = ('project_id', 'instance_id', 'database_id',
                       'gcp_conn_id')
    # [END gcp_spanner_database_delete_template_fields]

    @apply_defaults
    def __init__(self,
                 instance_id: int,
                 database_id: str,
                 project_id: Optional[str] = None,
                 gcp_conn_id: str = 'google_cloud_default',
                 *args, **kwargs) -> None:
        self.instance_id = instance_id
        self.project_id = project_id
        self.database_id = database_id
        self.gcp_conn_id = gcp_conn_id
        self._validate_inputs()
        super().__init__(*args, **kwargs)

    def _validate_inputs(self):
        if self.project_id == '':
            raise AirflowException("The required parameter 'project_id' is empty")
        if not self.instance_id:
            raise AirflowException("The required parameter 'instance_id' is empty"
                                   " or None")
        if not self.database_id:
            raise AirflowException("The required parameter 'database_id' is empty"
                                   " or None")

    def execute(self, context):
        hook = SpannerHook(gcp_conn_id=self.gcp_conn_id)
        database = hook.get_database(project_id=self.project_id,
                                     instance_id=self.instance_id,
                                     database_id=self.database_id)
        if not database:
            self.log.info("The Cloud Spanner database was missing: "
                          "'%s' in project '%s' and instance '%s'. Assuming success.",
                          self.database_id, self.project_id, self.instance_id)
            return True
        else:
            return hook.delete_database(project_id=self.project_id,
                                        instance_id=self.instance_id,
                                        database_id=self.database_id)
