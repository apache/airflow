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

import time

from airflow import AirflowException
from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook
from apiclient.discovery import HttpError, build

NUM_RETRIES = 5
SLEEP_TIME_IN_SECONDS = 60


class MemorystoreHook(GoogleCloudBaseHook):
    """
    Interact with Google Cloud Memorystore. This hook uses the Google Cloud Platform
    connection.

    This object is not threads safe. If you want to make multiple requests
    simultaneously, you will need to create a hook per thread.
    """

    def __init__(
        self,
        gcp_conn_id="google_cloud_default",
        delegate_to=None,
        version="v1beta1",
    ):
        super(MemorystoreHook, self).__init__(gcp_conn_id, delegate_to)
        self.version = version

    def get_conn(self):
        """
        Returns a Google Cloud Memorystore service object.
        """
        credentials = self._get_credentials()
        return build("redis", self.version, credentials=credentials, cache_discovery=False)

    def list_instances(self, project_id, location_id):
        """
        Lists all Redis instances owned by the specified project in
        either the specified location (region) or all locations.
        location_id can be set to - (wildcard) to query all the regions available
        to the project.

        .. seealso::
            For more information, see:
            https://cloud.google.com/memorystore/docs/redis/reference/rest/v1beta1/projects.locations.instances/list

        :param project_id: The ID of the project in which to query the instances
        :type project_id: str
        :param location_id: The name of the location
        :type location_id: str
        """

        project_id = project_id if project_id is not None else self.project_id

        self.log.info("Start requesting list of Redis instances")
        try:
            resp = (
                self.get_conn().projects()
                .locations()
                .instances()
                .list(parent="projects/{}/locations/{}".format(project_id, location_id))
                .execute()
            )

            self.log.info("Redis instances retrieved")
            return resp["instances"]
        except HttpError as err:
            raise AirflowException(
                "BigQuery job failed. Error was: {}".format(err.content)
            )

    def create_instance(
        self,
        project_id,
        location_id,
        instance_id,
        tier="BASIC",
        memory_size=1,
        wait_until_finish=False,
    ):
        """
        Method to create a Cloud Memorystore Redis instance based on the specified
        tier and memory size.

        .. seealso::
            For more information, see:
            https://cloud.google.com/memorystore/docs/redis/reference/rest/v1/projects.locations.instances/create

        :param project_id: The ID of the project in which to create the instance
        :type project_id: str
        :param location_id: The name of the location
        :type location_id: str
        :param instance_id: The ID of the instance
        :type instance_id: str
        :param tier: The service tier of the instance. Defaults to use "BASIC"
        :type tier: str

        ** Available tiers **:
        TIER_UNSPECIFIED, BASIC, STANDARD_HA

        :param memory_size: Redis memory size in GiB. Defaults to use 1
        :type memory_size: int
        :param wait_until_finish: Waits for the operation to finish. Defaults to False
        :type wait_until_finish: bool
        """

        project_id = project_id if project_id is not None else self.project_id

        body = {
            "name": "projects/{}/locations/{}/instances/{}".format(
                project_id, location_id, instance_id
            ),
            "tier": tier,
            "memorySizeGb": memory_size,
        }

        self.log.info("Start creating Redis instance")
        try:
            resp = (
                self.get_conn().projects()
                .locations()
                .instances()
                .create(
                    parent="projects/{}/locations/{}".format(project_id, location_id),
                    instanceId=instance_id,
                    body=body,
                )
                .execute()
            )

            self.log.info("Redis instance is being created")
            if wait_until_finish:
                return self._wait_until_finish(project_id, resp["name"])
            else:
                return resp
        except HttpError as err:
            raise AirflowException(
                "BigQuery job failed. Error was: {}".format(err.content)
            )

    def delete_instance(
        self, project_id, location_id, instance_id, wait_until_finish=False
    ):
        """
        Method to delete a specified Redis instance. Instance stops serving and data is deleted.

        .. seealso::
            For more information, see:
            https://cloud.google.com/memorystore/docs/redis/reference/rest/v1beta1/projects.locations.instances/delete

        :param project_id: The ID of the project in which to delete the instance
        :type project_id: str
        :param location_id: The name of the location
        :type location_id: str
        :param instance_id: The ID of the instance
        :type instance_id: str
        :param wait_until_finish: Waits for the operation to finish. Defaults to False
        :type wait_until_finish: bool
        """

        project_id = project_id if project_id is not None else self.project_id

        self.log.info("Start deleting Redis instance")
        try:
            resp = (
                self.get_conn().projects()
                .locations()
                .instances()
                .delete(
                    name="projects/{}/locations/{}/instances/{}".format(
                        project_id, location_id, instance_id
                    )
                )
                .execute()
            )

            self.log.info("Redis instance is being deleted")
            if wait_until_finish:
                return self._wait_until_finish(project_id, resp["name"])
            else:
                return resp
        except HttpError as err:
            raise AirflowException(
                "BigQuery job failed. Error was: {}".format(err.content)
            )

    def get_instance(
        self, project_id, location_id, instance_id, wait_until_finish=False
    ):
        """
        Method to get the details of a specified Redis instance.

        .. seealso::
            For more information, see:
            https://cloud.google.com/memorystore/docs/redis/reference/rest/v1beta1/projects.locations.instances/get

        :param project_id: The ID of the project in which to query the details
                           of the specified instance
        :type project_id: str
        :param location_id: The name of the location
        :type location_id: str
        :param instance_id: The ID of the instance
        :type instance_id: str
        :param wait_until_finish: Waits for the operation to finish. Defaults to False
        :type wait_until_finish: bool
        """

        project_id = project_id if project_id is not None else self.project_id

        self.log.info("Start requesting Redis instance")
        try:
            resp = (
                self.get_conn().projects()
                .locations()
                .instances()
                .get(
                    name="projects/{}/locations/{}/instances/{}".format(
                        project_id, location_id, instance_id
                    )
                )
                .execute()
            )
            self.log.info("Redis instance requested")
            return resp
        except HttpError as err:
            raise AirflowException(
                "BigQuery job failed. Error was: {}".format(err.content)
            )

    def _wait_until_finish(self, project_id, operation):
        """
        Check the status of the operation and return once the operation is done.

        :param project_id: The ID of the project that holds the Redis instance
        :type project_id: str
        :param operation: Name of the operation
        :type operation: str
        """
        while True:
            resp = (
                self.get_conn()
                .projects()
                .locations()
                .operations()
                .get(name=operation)
                .execute(num_retries=NUM_RETRIES)
            )

            done = resp.get("done")
            self.log.info(
                "Updating status for operation name: {}. Done: {}".format(
                    operation, done
                )
            )

            if done:
                error = resp.get("error")
                if error:
                    raise AirflowException(error.get("message"))
                self.log.info("Operation completed.")
                return resp

            time.sleep(SLEEP_TIME_IN_SECONDS)
