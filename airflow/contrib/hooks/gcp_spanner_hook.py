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
        self, spanner_conn_id="google_cloud_spanner_default", delegate_to=None
    ):
        super(SpannerHook, self).__init__(
            gcp_conn_id=spanner_conn_id, delegate_to=delegate_to
        )
        self.connection = self.get_conn("v1")

    def get_conn(self, version="v1"):
        """
        Returns a Google Cloud Spanner service object.
        """
        http_authorized = self._authorize()
        return build("spanner", version, http=http_authorized, cache_discovery=False)

    def list_instance_configs(self, project_id):
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

    def create_session(self, project_id, instance_id, database_id, body):
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
                    body=body,
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
