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
#

from airflow import AirflowException
from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook
from apiclient.discovery import HttpError, build


class SpannerHook(GoogleCloudBaseHook):
    """
    Interact with Google Cloud Spanner. This hook uses the Google Cloud Platform
    connection.

    This object is not threads safe. If you want to make multiple requests
    simultaneously, you will need to create a hook per thread.
    """

    def __init__(
        self, gcp_conn_id="google_cloud_default", delegate_to=None
    ):
        super(SpannerHook, self).__init__(
            gcp_conn_id=gcp_conn_id, delegate_to=delegate_to
        )
        self.connection = self.get_conn("v1")

    def get_conn(self, version="v1"):
        """
        Returns a Google Cloud Spanner service object.
        """
        http_authorized = self._authorize()
        return build("spanner", version, http=http_authorized, cache_discovery=False)

    def list_instance_configs(self, project_id):
        """
        Lists the supported instance configurations for a given project.

        .. seealso::
            For more information, see:
            https://cloud.google.com/spanner/docs/reference/rest/v1/projects.instanceConfigs/list

        :param project_id: The name of the project for which a list of supported instance
            configurations is requested
        :type project_id: str
        """

        project_id = project_id if project_id is not None else self.project_id

        self.log.info("Retrieving Spanner instance configs")

        try:
            resp = (
                self.connection.projects()
                .instanceConfigs()
                .list(parent="projects/{}".format(project_id))
                .execute()
            )

            self.log.info("Spanner instance configs retrieved successfully")

            return resp
        except HttpError as err:
            raise AirflowException(
                "BigQuery job failed. Error was: {}".format(err.content)
            )

    def create_instance(self, project_id, body):
        """
        Method to create a Cloud Spanner instance and begins preparing it to begin serving.
        If the named instance already exists, it will return 409 Instance Already Exists.

        .. seealso::
            For more information, see:
            https://cloud.google.com/spanner/docs/reference/rest/v1/projects.instances/create

        :param project_id: The name of the project in which to create the instance
        :type project_id: str
        :param body: the request body containing instance creation data
        :type rows: dict

        **Example or body**:
            body = {
                "instanceId": "spanner-instance-1",
                "instance": {
                    "nodeCount": 1,
                    "config": "projects/spanner-project/instanceConfigs/eur3",
                    "displayName": "Spanner Instance 1",
                },
            }
        """

        project_id = project_id if project_id is not None else self.project_id

        if "instanceId" not in body:
            raise ValueError("Spanner instanceId is undefined")

        self.log.info("Creating Spanner instance")

        try:
            resp = (
                self.connection.projects()
                .instances()
                .create(parent="projects/{}".format(project_id), body=body)
                .execute()
            )

            self.log.info("Spanner instance created successfully")
            return resp
        except HttpError as err:
            raise AirflowException(
                "BigQuery job failed. Error was: {}".format(err.content)
            )

    def get_instance(self, project_id, instance_id):
        """
        Method to get information about a Cloud Spanner instance.

        .. seealso::
            For more information, see:
            https://cloud.google.com/spanner/docs/reference/rest/v1/projects.instances/get

        :param project_id: The name of the project in which to get the instance
        :type project_id: str
        :param instance_id: The name of the instance
        :type instance_id: str
        """

        project_id = project_id if project_id is not None else self.project_id

        self.log.info("Retrieving Spanner instance")

        try:
            resp = (
                self.connection.projects()
                .instances()
                .get(name="projects/{}/instances/{}".format(project_id, instance_id))
                .execute()
            )

            self.log.info("Spanner instance retrieved successfully")

            return resp
        except HttpError as err:
            raise AirflowException(
                "BigQuery job failed. Error was: {}".format(err.content)
            )

    def delete_instance(self, project_id, instance_id):
        """
        Method to delete a Cloud Spanner instance.

        .. seealso::
            For more information, see:
            https://cloud.google.com/spanner/docs/reference/rest/v1/projects.instances/delete

        :param project_id: The name of the project in which to delete the instance
        :type project_id: str
        :param instance_id: The name of the instance
        :type instance_id: str
        """

        project_id = project_id if project_id is not None else self.project_id

        self.log.info("Deleting Spanner instance")

        try:
            self.connection.projects().instances().delete(
                name="projects/{}/instances/{}".format(project_id, instance_id)
            ).execute()

            self.log.info("Spanner instance deleted successfully")
        except HttpError as err:
            raise AirflowException(
                "BigQuery job failed. Error was: {}".format(err.content)
            )

    def list_instances(self, project_id):
        """
        Method to list all Cloud Spanner instances in the given project.

        .. seealso::
            For more information, see:
            https://cloud.google.com/spanner/docs/reference/rest/v1/projects.instances/list

        :param project_id: The name of the project in which to delete the instance
        :type project_id: str
        """

        project_id = project_id if project_id is not None else self.project_id

        self.log.info("Retrieving all Spanner instances")

        try:
            resp = (
                self.connection.projects()
                .instances()
                .list(parent="projects/{}".format(project_id))
                .execute()
            )

            self.log.info("All Spanner instances retrieved successfully")

            return resp
        except HttpError as err:
            raise AirflowException(
                "BigQuery job failed. Error was: {}".format(err.content)
            )

    def create_database(self, project_id, instance_id, body):
        """
        Method to creates a new Cloud Spanner database and starts to prepare it for serving.
        If the named database already exists, it will return 409 Database Already Exists.

        .. seealso::
            For more information, see:
            https://cloud.google.com/spanner/docs/reference/rest/v1/projects.instances.databases/create

        :param project_id: The name of the project
        :type project_id: str
        :param instance_id: The name of the instance in which to create the database
        :type instance_id: str
        :param body: the request body containing database creation data
        :type rows: dict

        **Example or body**:
            body = {"createStatement": "CREATE DATABASE spannerdb"}
        """

        project_id = project_id if project_id is not None else self.project_id

        self.log.info("Creating Spanner database")

        try:
            resp = (
                self.connection.projects()
                .instances()
                .databases()
                .create(
                    parent="projects/{}/instances/{}".format(project_id, instance_id),
                    body=body,
                )
                .execute()
            )

            self.log.info("Spanner database created successfully")
            return resp
        except HttpError as err:
            raise AirflowException(
                "BigQuery job failed. Error was: {}".format(err.content)
            )

    def get_database(self, project_id, instance_id, database_id):
        """
        Method to get the state of a Cloud Spanner database.

        .. seealso::
            For more information, see:
            https://cloud.google.com/spanner/docs/reference/rest/v1/projects.instances.databases/get

        :param project_id: The name of the project
        :type project_id: str
        :param instance_id: The name of the instance in which to get the database
        :type instance_id: str
        :param database_id: The name of the database
        :type database_id: str
        """

        project_id = project_id if project_id is not None else self.project_id

        self.log.info("Retrieving Spanner database")

        try:
            resp = (
                self.connection.projects()
                .instances()
                .databases()
                .get(
                    name="projects/{}/instances/{}/databases/{}".format(
                        project_id, instance_id, database_id
                    )
                )
                .execute()
            )

            self.log.info("Spanner database retrieved successfully")
            return resp
        except HttpError as err:
            raise AirflowException(
                "BigQuery job failed. Error was: {}".format(err.content)
            )

    def drop_database(self, project_id, instance_id, database_id):
        """
        Method to drop a Cloud Spanner database.

        .. seealso::
            For more information, see:
            https://cloud.google.com/spanner/docs/reference/rest/v1/projects.instances.databases/dropDatabase

        :param project_id: The name of the project
        :type project_id: str
        :param instance_id: The name of the instance in which to drop the database
        :type instance_id: str
        :param database_id: The name of the database
        :type database_id: str
        """

        project_id = project_id if project_id is not None else self.project_id

        self.log.info("Dropping Spanner database")

        try:
            self.connection.projects().instances().databases().dropDatabase(
                database="projects/{}/instances/{}/databases/{}".format(
                    project_id, instance_id, database_id
                )
            ).execute()

            self.log.info("Spanner database dropped successfully")
        except HttpError as err:
            raise AirflowException(
                "BigQuery job failed. Error was: {}".format(err.content)
            )

    def list_databases(self, project_id, instance_id):
        """
        Method to lists all databases in a Cloud Spanner isntance.

        .. seealso::
            For more information, see:
            https://cloud.google.com/spanner/docs/reference/rest/v1/projects.instances.databases/list

        :param project_id: The name of the project
        :type project_id: str
        :param instance_id: The name of the instance in which to list all the databases
        :type instance_id: str
        """

        project_id = project_id if project_id is not None else self.project_id

        self.log.info("Retrieving all Spanner databases")

        try:
            resp = (
                self.connection.projects()
                .instances()
                .databases()
                .list(parent="projects/{}/instances/{}".format(project_id, instance_id))
                .execute()
            )

            self.log.info("All Spanner databases retrieved successfully")
            return resp
        except HttpError as err:
            raise AirflowException(
                "BigQuery job failed. Error was: {}".format(err.content)
            )

    def create_session(self, project_id, instance_id, database_id):
        """
        Method to creates a new session that can be used to perform Cloud Spanner
        database transactions.

        .. seealso::
            For more information, see:
            https://cloud.google.com/spanner/docs/reference/rest/v1/projects.instances.databases.sessions/create

        :param project_id: The name of the project
        :type project_id: str
        :param instance_id: The name of the instance
        :type instance_id: str
        :param database_id: The database in which the new session is to be created
        :type database_id: str
        """

        project_id = project_id if project_id is not None else self.project_id

        self.log.info("Creating Spanner database session")

        try:
            resp = (
                self.connection.projects()
                .instances()
                .databases()
                .sessions()
                .create(
                    database="projects/{}/instances/{}/databases/{}".format(
                        project_id, instance_id, database_id
                    ),
                    body={"session": {}},
                )
                .execute()
            )

            self.log.info("Spanner database session created successfully")
            return resp
        except HttpError as err:
            raise AirflowException(
                "BigQuery job failed. Error was: {}".format(err.content)
            )

    def delete_session(self, session_id):
        """
        Method to end a session, releasing server resources associated with it.

        .. seealso::
            For more information, see:
            https://cloud.google.com/spanner/docs/reference/rest/v1/projects.instances.databases.sessions/delete

        :param session_id: The name of the session
        :type session_id: str

        **Example or session_id**:
            session_id = ('projects/{project_id}/instances/{instance_id}'
                          '/databases/{database_id}/sessions/{session_id}')
        """

        self.log.info("Deleting Spanner database session")

        try:
            self.connection.projects().instances().databases().sessions().delete(
                name=session_id
            ).execute()

            self.log.info("Spanner database session deleted successfully")
        except HttpError as err:
            raise AirflowException(
                "BigQuery job failed. Error was: {}".format(err.content)
            )

    def execute_sql(self, session_id, body):
        """
        Method to execute an SQL statement and return all results in a single reply.
        Notice: This method cannot be used to return a result set larger than 10 MiB.

        .. seealso::
            For more information, see:
            https://cloud.google.com/spanner/docs/reference/rest/v1/projects.instances.databases.sessions/executeSql

        :param session_id: The name of the session
        :type session_id: str

        **Example or session_id**:
            session_id = ('projects/{project_id}/instances/{instance_id}'
                          '/databases/{database_id}/sessions/{session_id}')

        :param body: The request body that contains data, such as SQL statement, etc.
        :type body: dict

        **Example or body**:
            body = {"sql": "SELECT * FROM Product"}
        """

        self.log.info("Executing SQL on Spanner")

        try:
            resp = (
                self.connection.projects()
                .instances()
                .databases()
                .sessions()
                .executeSql(session=session_id, body=body)
                .execute()
            )

            self.log.info("SQL executed successfully")
            return resp
        except HttpError as err:
            raise AirflowException(
                "BigQuery job failed. Error was: {}".format(err.content)
            )
