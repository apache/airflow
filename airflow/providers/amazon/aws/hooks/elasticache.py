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

import re
from time import sleep

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook


class ElastiCacheDescribeResponseKeys:
    """
    Constants for ElastiCache describe API response keys
    """
    REPLICATION_GROUPS = 'ReplicationGroups'
    REPLICATION_GROUP = 'ReplicationGroup'
    STATUS = 'Status'


class ElastiCacheDeleteResponses:
    """
    Constants for ElastiCache delete API response messages
    """
    RESPONSE_NOT_VALID_STATE = 'not in a valid state to be deleted'
    RESPONSE_DELETING_STATE = 'has status deleting which is not valid for deletion'
    RESPONSE_MODIFYING_STATE = 'has status modifying which is not valid for deletion'


class ElastiCacheStatus:
    """
    Constants for ElastiCache replication group status
    """
    STATUS_CREATING = 'creating'
    STATUS_AVAILABLE = 'available'
    STATUS_DELETING = 'deleting'
    STATUS_CREATE_FAILED = 'create-failed'


class ElastiCacheHook(AwsBaseHook):
    """
    Interact with AWS ElastiCache
    """

    def __init__(
        self, max_retries=10, exponential_back_off_factor=1, initial_poke_interval=60, *args, **kwargs
    ):
        """
        :param max_retries: Max tries for checking availability of and deleting replication group
        :param exponential_back_off_factor: Factor for deciding next sleep time
        :param initial_poke_interval: Initial sleep time
        """
        self.max_retries = max_retries
        self.exponential_back_off_factor = exponential_back_off_factor
        self.initial_poke_interval = initial_poke_interval

        super().__init__(client_type='elasticache', *args, **kwargs)

    def __create_replication_group(self, config):
        """
        Call ElastiCache API for creating a replication group
        :param config: Python dictionary to use as config for creating replication group
        :return: Response from ElastiCache create replication group API
        """
        return self.get_conn().create_replication_group(**config)

    def __delete_replication_group(self, replication_group_id):
        """
        Call ElastiCache API for deleting a replication group
        :param replication_group_id: ID of replication group to delete
        :return: Response from ElastiCache delete replication group API
        """
        return self.get_conn().delete_replication_group(ReplicationGroupId=replication_group_id)

    def describe_replication_group(self, replication_group_id):
        """
        Call ElastiCache API for describing a replication group
        :param replication_group_id: ID of replication group to describe
        :return: Response from ElastiCache describe replication group API
        """
        return self.get_conn().describe_replication_groups(ReplicationGroupId=replication_group_id)

    def create_replication_group(self, config):
        """
        Create replication group with given config and check if it being created or not
        :param config: Python dictionary to use as config for creating replication group
        :return: Response from ElastiCache create replication group API
        """
        self.log.info('Creating replication group with config : %s', config)

        response = self.__create_replication_group(config=config)

        status = response[ElastiCacheDescribeResponseKeys.REPLICATION_GROUP][
            ElastiCacheDescribeResponseKeys.STATUS]

        if status != ElastiCacheStatus.STATUS_CREATING:
            raise AirflowException('Replication group could not be created. Status: "{0}"'.format(status))

        return response

    def _is_replication_group_available(self, replication_group_id):
        """
        Helper for checking if replication is available or not
        :param replication_group_id: ID of replication group to check for availability
        :return: Flag to check if availability check should be stopped or not and current status of
        replication group
        """
        desc = self.describe_replication_group(replication_group_id)

        status = desc[ElastiCacheDescribeResponseKeys.REPLICATION_GROUPS][0][
            ElastiCacheDescribeResponseKeys.STATUS]

        return status in (
            ElastiCacheStatus.STATUS_AVAILABLE,
            ElastiCacheStatus.STATUS_CREATE_FAILED,
            ElastiCacheStatus.STATUS_DELETING
        ), status

    def is_replication_group_available(self, replication_group_id):
        """
        Check if replication is available or not by performing a describe over it
        :param replication_group_id: ID of replication group to check for availability
        :return: True if replication is available else False
        """
        sleep_time = self.initial_poke_interval
        num_retries = 0
        status = ElastiCacheStatus.STATUS_CREATE_FAILED

        while num_retries < self.max_retries:
            stop_poking, status = self._is_replication_group_available(
                replication_group_id=replication_group_id
            )

            self.log.info('Status : %s', status)

            if stop_poking:
                self.log.info('Received signal to stop poking. Current status : "%s"', status)
                break

            num_retries += 1

            self.log.info('Poke retry: %s. Sleep time: %s. Sleeping...', num_retries, sleep_time)

            sleep(sleep_time)

            sleep_time = sleep_time * self.exponential_back_off_factor

        if status != ElastiCacheStatus.STATUS_AVAILABLE:
            self.log.warning('Replication group is not available. Current status : "%s"', status)

            return False

        return True

    def _delete_replication_group(self, replication_group_id):
        """
        Helper for deleting a replication group ensuring it is either deleted or can't be deleted
        :param replication_group_id: ID of replication to delete
        :return: Response from ElastiCache delete replication group API and flag to identify if deleted or not
        """
        response = None
        deleted = False
        sleep_time = self.initial_poke_interval
        num_retries = 0

        while not deleted and num_retries < self.max_retries:
            try:
                response = self.__delete_replication_group(replication_group_id=replication_group_id)

                self.log.info('Replication group with ID : %s : is being deleted...', replication_group_id)

            except self.get_conn().exceptions.ReplicationGroupNotFoundFault:
                self.log.info("Replication group with ID : '%s' does not exist", replication_group_id)

                deleted = True

            except self.get_conn().exceptions.InvalidReplicationGroupStateFault as exp:
                message = exp.response['Error']['Message']

                self.log.info('Received message : %s', message)

                if re.search(ElastiCacheDeleteResponses.RESPONSE_NOT_VALID_STATE, message) \
                        or re.search(ElastiCacheDeleteResponses.RESPONSE_MODIFYING_STATE, message):

                    self.log.info(
                        'Replication group with ID : %s : is not in valid state to be deleted',
                        replication_group_id
                    )

                elif re.search(ElastiCacheDeleteResponses.RESPONSE_DELETING_STATE, message):
                    self.log.info(
                        'Replication group with ID : %s : is being deleted...', replication_group_id
                    )

                else:
                    raise AirflowException(str(exp))

                if not deleted:
                    num_retries += 1

                    self.log.info('Poke retry: %s. Sleep time: %s. Sleeping...', num_retries, sleep_time)

                    sleep(sleep_time)

                    sleep_time = sleep_time * self.exponential_back_off_factor

        return response, deleted

    def delete_replication_group(self, replication_group_id):
        """
        Delete a replication group ensuring it is either deleted or can't be deleted
        :param replication_group_id: ID of replication to delete
        :return: Response from ElastiCache delete replication group API
        """
        self.log.info('Deleting replication group with ID : %s', replication_group_id)

        response, deleted = self._delete_replication_group(replication_group_id=replication_group_id)

        if not deleted:
            raise AirflowException(
                'Replication group could not be deleted. Response : "{0}"'.format(response)
            )

        return response
