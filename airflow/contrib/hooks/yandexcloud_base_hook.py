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

import yandexcloud
from yandex.cloud.iam.v1.service_account_service_pb2 import ListServiceAccountsRequest
from yandex.cloud.iam.v1.service_account_service_pb2_grpc import ServiceAccountServiceStub
from yandex.cloud.vpc.v1.network_service_pb2 import ListNetworksRequest
from yandex.cloud.vpc.v1.network_service_pb2_grpc import NetworkServiceStub
from yandex.cloud.vpc.v1.subnet_service_pb2 import ListSubnetsRequest
from yandex.cloud.vpc.v1.subnet_service_pb2_grpc import SubnetServiceStub

from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook


class YandexCloudBaseHook(BaseHook):
    """
    A base hook for Yandex.Cloud related tasks.
    :param connection_id: The connection ID to use when fetching connection info.
    :type connection_id: str
    """

    def __init__(self,
                 connection_id=None,
                 default_folder_id=None,
                 default_public_ssh_key=None
                 ):
        super(YandexCloudBaseHook, self).__init__()
        self.connection_id = connection_id or 'yandexcloud_default'
        self.connection = self.get_connection(self.connection_id)
        self.extras = self.connection.extra_dejson
        self.sdk = yandexcloud.SDK(token=self._get_credentials())
        self.default_folder_id = default_folder_id or self._get_field('folder_id', False)
        self.default_public_ssh_key = default_public_ssh_key or self._get_field('public_ssh_key', False)
        self.client = self.sdk.client

    def _get_credentials(self):
        oauth_token = self._get_field('oauth', False)
        if not oauth_token:
            raise AirflowException('No credentials are found in connection.')
        return oauth_token

    def _get_field(self, f, default=None):
        """
        Fetches a field from extras, and returns it.
        """
        long_f = 'extra__yandexcloud__{}'.format(f)
        if hasattr(self, 'extras') and long_f in self.extras:
            return self.extras[long_f]
        else:
            return default

    def wait_for_operation(self, operation):
        """
        Wait until operation is finished
        :param operation: Operation to wait
        :type operation: Operation
        :return operation waiter object
        :rtype OperationWaiter
        """
        waiter = self.sdk.waiter(operation.id)
        for _ in waiter:
            time.sleep(1)
        return waiter.operation

    def find_service_account_id(self, folder_id):
        """
        Get service account id in case the folder has the only one service account
        :param folder_id: ID of the folder
        :type operation: str
        :return ID of the service account
        :rtype str
        """
        service = self.client(ServiceAccountServiceStub)
        service_accounts = service.List(ListServiceAccountsRequest(folder_id=folder_id)).service_accounts
        if len(service_accounts) == 1:
            return service_accounts[0].id
        if len(service_accounts) == 0:
            message = 'There are no service accounts in folder {}, please create it.'
            raise RuntimeError(message.format(folder_id))
        message = 'There are more than one service account in folder {}, please specify it'
        raise RuntimeError(message.format(folder_id))

    def find_network(self, folder_id):
        """
        Get ID of the first network in folder
        :param folder_id: ID of the folder
        :type operation: str
        :return ID of the network
        :rtype str
        """
        networks = self.client(NetworkServiceStub).List(ListNetworksRequest(folder_id=folder_id)).networks
        networks = [n for n in networks if n.folder_id == folder_id]

        if not networks:
            raise RuntimeError('No networks in folder: {}'.format(folder_id))
        if len(networks) > 1:
            raise RuntimeError(
                'There are more than one service account in folder {}, please specify it'.format(folder_id)
            )
        return networks[0].id

    def find_subnet(self, folder_id, availability_zone_id, network_id):
        """
        Get ID of the subnetwork of specified network in specified availability zone
        :param folder_id: ID of the folder
        :type operation: str
        :param availability_zone_id: ID of the availability zone
        :type operation: str
        :param network_id: ID of the network
        :type operation: str
        :return ID of the subnetwork
        :rtype str
        """
        subnet_service = self.client(SubnetServiceStub)
        subnets = subnet_service.List(ListSubnetsRequest(
            folder_id=folder_id)).subnets
        applicable = [s for s in subnets if s.zone_id == availability_zone_id and s.network_id == network_id]
        if len(applicable) == 1:
            return applicable[0].id
        if len(applicable) == 0:
            message = 'There are no subnets in {} zone, please create it.'
            raise RuntimeError(message.format(availability_zone_id))
        message = 'There are more than one subnet in {} zone, please specify it'
        raise RuntimeError(message.format(availability_zone_id))
