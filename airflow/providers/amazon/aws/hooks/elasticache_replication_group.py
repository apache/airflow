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
from __future__ import annotations

import time

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook


class ElastiCacheReplicationGroupHook(AwsBaseHook):
    """
    Interact with Amazon ElastiCache.

    Provide thick wrapper around :external+boto3:py:class:`boto3.client("elasticache") <ElastiCache.Client>`.

    :param max_retries: Max retries for checking availability of and deleting replication group
            If this is not supplied then this is defaulted to 10
    :param exponential_back_off_factor: Multiplication factor for deciding next sleep time
            If this is not supplied then this is defaulted to 1
    :param initial_poke_interval: Initial sleep time in seconds
            If this is not supplied then this is defaulted to 60 seconds

    Additional arguments (such as ``aws_conn_id``) may be specified and
    are passed down to the underlying AwsBaseHook.

    .. seealso::
        - :class:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`
    """

    TERMINAL_STATES = frozenset({"available", "create-failed", "deleting"})

    def __init__(
        self,
        max_retries: int = 10,
        exponential_back_off_factor: float = 1,
        initial_poke_interval: float = 60,
        *args,
        **kwargs,
    ):
        self.max_retries = max_retries
        self.exponential_back_off_factor = exponential_back_off_factor
        self.initial_poke_interval = initial_poke_interval

        kwargs["client_type"] = "elasticache"
        super().__init__(*args, **kwargs)

    def create_replication_group(self, config: dict) -> dict:
        """
        Create a Redis (cluster mode disabled) or a Redis (cluster mode enabled) replication group.

        .. seealso::
            - :external+boto3:py:meth:`ElastiCache.Client.create_replication_group`

        :param config: Configuration for creating the replication group
        :return: Response from ElastiCache create replication group API
        """
        return self.conn.create_replication_group(**config)

    def delete_replication_group(self, replication_group_id: str) -> dict:
        """
        Delete an existing replication group.

        .. seealso::
            - :external+boto3:py:meth:`ElastiCache.Client.delete_replication_group`

        :param replication_group_id: ID of replication group to delete
        :return: Response from ElastiCache delete replication group API
        """
        return self.conn.delete_replication_group(ReplicationGroupId=replication_group_id)

    def describe_replication_group(self, replication_group_id: str) -> dict:
        """
        Get information about a particular replication group.

        .. seealso::
            - :external+boto3:py:meth:`ElastiCache.Client.describe_replication_groups`

        :param replication_group_id: ID of replication group to describe
        :return: Response from ElastiCache describe replication group API
        """
        return self.conn.describe_replication_groups(ReplicationGroupId=replication_group_id)

    def get_replication_group_status(self, replication_group_id: str) -> str:
        """
        Get current status of replication group.

        .. seealso::
            - :external+boto3:py:meth:`ElastiCache.Client.describe_replication_groups`

        :param replication_group_id: ID of replication group to check for status
        :return: Current status of replication group
        """
        return self.describe_replication_group(replication_group_id)["ReplicationGroups"][0]["Status"]

    def is_replication_group_available(self, replication_group_id: str) -> bool:
        """
        Check if replication group is available or not.

        :param replication_group_id: ID of replication group to check for availability
        :return: True if available else False
        """
        return self.get_replication_group_status(replication_group_id) == "available"

    def wait_for_availability(
        self,
        replication_group_id: str,
        initial_sleep_time: float | None = None,
        exponential_back_off_factor: float | None = None,
        max_retries: int | None = None,
    ) -> bool:
        """
        Check if replication group is available or not by performing a describe over it.

        :param replication_group_id: ID of replication group to check for availability
        :param initial_sleep_time: Initial sleep time in seconds
            If this is not supplied then this is defaulted to class level value
        :param exponential_back_off_factor: Multiplication factor for deciding next sleep time
            If this is not supplied then this is defaulted to class level value
        :param max_retries: Max retries for checking availability of replication group
            If this is not supplied then this is defaulted to class level value
        :return: True if replication is available else False
        """
        sleep_time = initial_sleep_time or self.initial_poke_interval
        exponential_back_off_factor = exponential_back_off_factor or self.exponential_back_off_factor
        max_retries = max_retries or self.max_retries
        num_tries = 0
        status = "not-found"
        stop_poking = False

        while not stop_poking and num_tries <= max_retries:
            status = self.get_replication_group_status(replication_group_id=replication_group_id)
            stop_poking = status in self.TERMINAL_STATES

            self.log.info(
                "Current status of replication group with ID %s is %s", replication_group_id, status
            )

            if not stop_poking:
                num_tries += 1

                # No point in sleeping if all tries have exhausted
                if num_tries > max_retries:
                    break

                self.log.info("Poke retry %s. Sleep time %s seconds. Sleeping...", num_tries, sleep_time)

                time.sleep(sleep_time)

                sleep_time *= exponential_back_off_factor

        if status != "available":
            self.log.warning('Replication group is not available. Current status is "%s"', status)

            return False

        return True

    def wait_for_deletion(
        self,
        replication_group_id: str,
        initial_sleep_time: float | None = None,
        exponential_back_off_factor: float | None = None,
        max_retries: int | None = None,
    ):
        """
        Delete a replication group ensuring it is either deleted or can't be deleted.

        :param replication_group_id: ID of replication to delete
        :param initial_sleep_time: Initial sleep time in second
            If this is not supplied then this is defaulted to class level value
        :param exponential_back_off_factor: Multiplication factor for deciding next sleep time
            If this is not supplied then this is defaulted to class level value
        :param max_retries: Max retries for checking availability of replication group
            If this is not supplied then this is defaulted to class level value
        :return: Response from ElastiCache delete replication group API and flag to identify if deleted or not
        """
        deleted = False
        sleep_time = initial_sleep_time or self.initial_poke_interval
        exponential_back_off_factor = exponential_back_off_factor or self.exponential_back_off_factor
        max_retries = max_retries or self.max_retries
        num_tries = 0
        response = None

        while not deleted and num_tries <= max_retries:
            try:
                status = self.get_replication_group_status(replication_group_id=replication_group_id)

                self.log.info(
                    "Current status of replication group with ID %s is %s", replication_group_id, status
                )

                # Can only delete if status is `available`
                # Status becomes `deleting` after this call so this will only run once
                if status == "available":
                    self.log.info("Initiating delete and then wait for it to finish")

                    response = self.delete_replication_group(replication_group_id=replication_group_id)

            except self.conn.exceptions.ReplicationGroupNotFoundFault:
                self.log.info("Replication group with ID '%s' does not exist", replication_group_id)

                deleted = True

            # This should never occur as we only issue a delete request when status is `available`
            # which is a valid status for deletion. Still handling for safety.
            except self.conn.exceptions.InvalidReplicationGroupStateFault as exp:
                # status      Error Response
                # creating  - Cache cluster <cluster_id> is not in a valid state to be deleted.
                # deleting  - Replication group <replication_group_id> has status deleting which is not valid
                #             for deletion.
                # modifying - Replication group <replication_group_id> has status deleting which is not valid
                #             for deletion.

                message = exp.response["Error"]["Message"]

                self.log.warning("Received error message from AWS ElastiCache API : %s", message)

            if not deleted:
                num_tries += 1

                # No point in sleeping if all tries have exhausted
                if num_tries > max_retries:
                    break

                self.log.info("Poke retry %s. Sleep time %s seconds. Sleeping...", num_tries, sleep_time)

                time.sleep(sleep_time)

                sleep_time *= exponential_back_off_factor

        return response, deleted

    def ensure_delete_replication_group(
        self,
        replication_group_id: str,
        initial_sleep_time: float | None = None,
        exponential_back_off_factor: float | None = None,
        max_retries: int | None = None,
    ) -> dict:
        """
        Delete a replication group ensuring it is either deleted or can't be deleted.

        :param replication_group_id: ID of replication to delete
        :param initial_sleep_time: Initial sleep time in second
            If this is not supplied then this is defaulted to class level value
        :param exponential_back_off_factor: Multiplication factor for deciding next sleep time
            If this is not supplied then this is defaulted to class level value
        :param max_retries: Max retries for checking availability of replication group
            If this is not supplied then this is defaulted to class level value
        :return: Response from ElastiCache delete replication group API
        :raises AirflowException: If replication group is not deleted
        """
        self.log.info("Deleting replication group with ID %s", replication_group_id)

        response, deleted = self.wait_for_deletion(
            replication_group_id=replication_group_id,
            initial_sleep_time=initial_sleep_time,
            exponential_back_off_factor=exponential_back_off_factor,
            max_retries=max_retries,
        )

        if not deleted:
            raise AirflowException(f'Replication group could not be deleted. Response "{response}"')

        return response
