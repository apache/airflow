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

from time import sleep

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook


class ElastiCacheHook(AwsBaseHook):
    """
    Interact with AWS ElastiCache
    """

    # Constants for ElastiCache describe API response keys
    REPLICATION_GROUPS = 'ReplicationGroups'
    REPLICATION_GROUP = 'ReplicationGroup'
    STATUS = 'Status'

    # Constants for ElastiCache replication group status
    STATUS_CREATING = 'creating'
    STATUS_AVAILABLE = 'available'
    STATUS_DELETING = 'deleting'
    STATUS_CREATE_FAILED = 'create-failed'
    STATUS_MODIFYING = 'modifying'
    STATUS_SNAPSHOTTING = 'snapshotting'

    def __init__(
        self, max_retries=10, exponential_back_off_factor=1, initial_poke_interval=60, *args, **kwargs
    ):
        """
        :param max_retries: Max tries for checking availability of and deleting replication group
        :param exponential_back_off_factor: Factor for deciding next sleep time
        :param initial_poke_interval: Initial sleep time in seconds
        """
        self.max_retries = max_retries
        self.exponential_back_off_factor = exponential_back_off_factor
        self.initial_poke_interval = initial_poke_interval

        super().__init__(client_type='elasticache', *args, **kwargs)

    def create_replication_group(self, config):
        """
        Call ElastiCache API for creating a replication group

        :param config: Python dictionary to use as config for creating replication group
        :return: Response from ElastiCache create replication group API
        """
        return self.conn.create_replication_group(**config)

    def delete_replication_group(self, replication_group_id):
        """
        Call ElastiCache API for deleting a replication group

        :param replication_group_id: ID of replication group to delete
        :return: Response from ElastiCache delete replication group API
        """
        return self.conn.delete_replication_group(ReplicationGroupId=replication_group_id)

    def describe_replication_group(self, replication_group_id):
        """
        Call ElastiCache API for describing a replication group

        :param replication_group_id: ID of replication group to describe
        :return: Response from ElastiCache describe replication group API
        """
        return self.conn.describe_replication_groups(ReplicationGroupId=replication_group_id)

    def get_replication_group_status(self, replication_group_id):
        """
        Get current status of replication group

        :param replication_group_id: ID of replication group to check for status
        :return: Current status of replication group
        """
        return self.describe_replication_group(replication_group_id)[self.REPLICATION_GROUPS][0][self.STATUS]

    def is_replication_group_available(self, replication_group_id):
        """
        Helper for checking if replication is available or not

        :param replication_group_id: ID of replication group to check for availability
        :return: True if available else False
        replication group
        """
        return self.get_replication_group_status(replication_group_id) == self.STATUS_AVAILABLE

    def _has_reached_terminal_state(self, replication_group_id):
        """
        Helper for checking if we should stop poking replication group for availability or not

        :param replication_group_id: ID of replication group to check
        :return: Flag to check if availability check should be stopped or not and current status of
        replication group
        """
        status = self.get_replication_group_status(replication_group_id=replication_group_id)

        return status in (
            self.STATUS_AVAILABLE,
            self.STATUS_CREATE_FAILED,
            self.STATUS_DELETING
        ), status

    def wait_for_availability(
        self,
        replication_group_id,
        initial_sleep_time=None,
        exponential_back_off_factor=None,
        max_retries=None
    ):
        """
        Check if replication is available or not by performing a describe over it

        :param max_retries: Max tries for checking availability of replication group
        :param exponential_back_off_factor: Factor for deciding next sleep time
        :param initial_sleep_time: Initial sleep time in seconds
        :param replication_group_id: ID of replication group to check for availability
        :return: True if replication is available else False
        """
        sleep_time = initial_sleep_time or self.initial_poke_interval
        exponential_back_off_factor = exponential_back_off_factor or self.exponential_back_off_factor
        max_retries = max_retries or self.max_retries
        num_retries = 0
        status = self.STATUS_CREATE_FAILED

        while num_retries < max_retries:
            stop_poking, status = self._has_reached_terminal_state(replication_group_id=replication_group_id)

            self.log.info(
                'Current status of replication group with ID : %s is %s',
                replication_group_id,
                status
            )

            if stop_poking:
                break

            num_retries += 1

            self.log.info('Poke retry: %s. Sleep time: %s. Sleeping...', num_retries, sleep_time)

            sleep(sleep_time)

            sleep_time = sleep_time * exponential_back_off_factor

        if status != self.STATUS_AVAILABLE:
            self.log.warning('Replication group is not available. Current status : "%s"', status)

            return False

        return True

    def wait_for_deletion(
        self,
        replication_group_id,
        initial_sleep_time=None,
        exponential_back_off_factor=None,
        max_retries=None
    ):
        """
        Helper for deleting a replication group ensuring it is either deleted or can't be deleted

        :param replication_group_id: ID of replication to delete
        :param max_retries: Max tries for checking availability of replication group
        :param exponential_back_off_factor: Factor for deciding next sleep time
        :param initial_sleep_time: Initial sleep time in second
        :return: Response from ElastiCache delete replication group API and flag to identify if deleted or not
        """
        deleted = False
        sleep_time = initial_sleep_time or self.initial_poke_interval
        exponential_back_off_factor = exponential_back_off_factor or self.exponential_back_off_factor
        max_retries = max_retries or self.max_retries
        num_retries = 0
        response = None

        while not deleted and num_retries < max_retries:
            try:
                status = self.get_replication_group_status(replication_group_id=replication_group_id)

                self.log.info(
                    'Current status of replication group with ID : %s is %s',
                    replication_group_id,
                    status
                )

                # Can only delete if status is `available`
                # Status becomes `deleting` after this call so this will only run once
                if status == self.STATUS_AVAILABLE:
                    response = self.delete_replication_group(replication_group_id=replication_group_id)

            except self.conn.exceptions.ReplicationGroupNotFoundFault:
                self.log.info("Replication group with ID : '%s' does not exist", replication_group_id)

                deleted = True

            # This should never occur as we only issue a delete request when status is `creating`
            # which is a valid status for deletion. Still handling for safety.
            except self.conn.exceptions.InvalidReplicationGroupStateFault as exp:
                """
                status      Error Response
                creating  - Cache cluster <cluster_id> is not in a valid state to be deleted.
                deleting  - Replication group <replication_group_id> has status deleting which is not valid
                            for deletion.
                modifying - Replication group <replication_group_id> has status deleting which is not valid
                            for deletion.
                """
                message = exp.response['Error']['Message']

                self.log.info('Received message : %s', message)

            if not deleted:
                num_retries += 1

                self.log.info('Poke retry: %s. Sleep time: %s. Sleeping...', num_retries, sleep_time)

                sleep(sleep_time)

                sleep_time = sleep_time * exponential_back_off_factor

        return response, deleted

    def ensure_delete_replication_group(
        self,
        replication_group_id,
        initial_sleep_time=None,
        exponential_back_off_factor=None,
        max_retries=None
    ):
        """
        Delete a replication group ensuring it is either deleted or can't be deleted

        :param replication_group_id: ID of replication to delete
        :param replication_group_id: ID of replication to delete
        :param max_retries: Max tries for checking availability of replication group
        :param exponential_back_off_factor: Factor for deciding next sleep time
        :param initial_sleep_time: Initial sleep time in second
        :return: Response from ElastiCache delete replication group API
        """
        self.log.info('Deleting replication group with ID : %s', replication_group_id)

        response, deleted = self.wait_for_deletion(
            replication_group_id=replication_group_id,
            initial_sleep_time=initial_sleep_time,
            exponential_back_off_factor=exponential_back_off_factor,
            max_retries=max_retries
        )

        if not deleted:
            raise AirflowException(
                'Replication group could not be deleted. Response : "{0}"'.format(response)
            )

        return response
