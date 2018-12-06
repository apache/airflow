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

from airflow.contrib.hooks.azure_kubernetes_hook import AzureKubernetesServiceHook
from airflow.models import BaseOperator

from azure.mgmt.containerservice.models import ContainerServiceLinuxProfile
from azure.mgmt.containerservice.models import ContainerServiceServicePrincipalProfile
from azure.mgmt.containerservice.models import ContainerServiceSshConfiguration
from azure.mgmt.containerservice.models import ContainerServiceSshPublicKey
from azure.mgmt.containerservice.models import ContainerServiceStorageProfileTypes
from azure.mgmt.containerservice.models import ManagedCluster
from azure.mgmt.containerservice.models import ManagedClusterAgentPoolProfile

from airflow.contrib.utils.aks_utils import \
    is_valid_ssh_rsa_public_key, get_poller_result, get_public_key, get_default_dns_prefix
from knack.log import get_logger
from msrestazure.azure_exceptions import CloudError

logger = get_logger(__name__)


class AzureKubernetesOperator(BaseOperator):
    """
    Start a Azure Kubernetes Service

    :param ci_conn_id: connection id of a
        service principal which will be used to start the azure kubernetes service
    :type ci_conn_id: str
    :param resource_group: Required name of the resource group
        wherein this container instance should be started
    :type resource_group: str
    :param name: Required name of this container. Please note this name
        has to be unique in order to run containers in parallel.
    :type name: str
    :param ssh_key_value: the ssh value used to connect to machine to be used
    :type ssh_key_value: str
    :param dns_name_prefix: DNS prefix specified when creating the managed cluster.
    :type region: dns_name_prefix
    :param admin_username: The administrator username to use for Linux VMs.
    :type admin_username: str
    :param kubernetes_version: Version of Kubernetes specified when creating the managed cluster.
    :type kubernetes_version: str
    :param node_vm_size: Vm to be spin up.
    :type node_vm_size: str or ContainerServiceVMSizeTypes Enum
    :param node_osdisk_size: Size in GB to be used to specify the disk size for
        every machine in this master/agent pool. If you specify 0, it will apply the default
        osDisk size according to the vmSize specified.
    :type node_osdisk_size: int
    :param node_count: Number of agents (VMs) to host docker containers.
        Allowed values must be in the range of 1 to 100 (inclusive). The default value is 1.
    :type node_count: int
    :param no_ssh_key: Specified if it is linuxprofile.
    :type no_ssh_key: boolean
    :param vnet_subnet_id: VNet SubnetID specifies the vnet's subnet identifier.
    :type vnet_subnet_id: str
    :param max_pods: Maximum number of pods that can run on a node.
    :type max_pods: int
    :param os_type: OsType to be used to specify os type. Choose from Linux and Windows.
        Default to Linux.
    :type os_type: str or OSType Enum
    :param tags: Resource tags.
    :type tags: dict[str, str]
    :param location: Required resource location
    :type location: str

    :Example:

    >>> a = AzureKubernetesOperator(task_id="task",ci_conn_id='azure_kubernetes_default',
            resource_group="my_resource_group",
            name="my_aks_container",
            ssh_key_value=None,
            dns_name_prefix=None,
            location="my_region",
            tags=None
        )
    """

    def __init__(self, ci_conn_id, resource_group, name, ssh_key_value,
                 dns_name_prefix=None,
                 location=None,
                 admin_username="azureuser",
                 kubernetes_version='',
                 node_vm_size="Standard_DS2_v2",
                 node_osdisk_size=0,
                 node_count=3,
                 no_ssh_key=False,
                 vnet_subnet_id=None,
                 max_pods=None,
                 os_type="Linux",
                 tags=None,
                 *args, **kwargs):

        self.ci_conn_id = ci_conn_id
        self.resource_group = resource_group
        self.name = name
        self.no_ssh_key = no_ssh_key
        self.dns_name_prefix = dns_name_prefix
        self.location = location
        self.admin_username = admin_username
        self.node_vm_size = node_vm_size
        self.node_count = node_count
        self.ssh_key_value = ssh_key_value
        self.vnet_subnet_id = vnet_subnet_id
        self.max_pods = max_pods
        self.os_type = os_type
        self.tags = tags
        self.node_osdisk_size = node_osdisk_size
        self.kubernetes_version = kubernetes_version

        super(AzureKubernetesOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        ci_hook = AzureKubernetesServiceHook(self.ci_conn_id)

        containerservice = ci_hook.get_conn()

        if not self.no_ssh_key:
            try:
                if not self.ssh_key_value or not is_valid_ssh_rsa_public_key(self.ssh_key_value):
                    raise ValueError()
            except (TypeError, ValueError):
                self.ssh_key_value = get_public_key(self)

        # dns_prefix
        if not self.dns_name_prefix:
            self.dns_name_prefix = get_default_dns_prefix(
                self, self.name, self.resource_group, ci_hook.subscription_id)

        # Check if the resource group exists
        if ci_hook.check_resource(ci_hook.credentials, ci_hook.subscription_id, self.resource_group):
            logger.info("Resource group already existing:" + self.resource_group)
        else:
            logger.info("Creating resource {0}".format(self.resource_group))
            created_resource_group = ci_hook.create_resource(
                ci_hook.credentials, ci_hook.subscription_id, self.resource_group, {
                    'location': self.location})
            print('Got resource group:', created_resource_group.name)

        # Add agent_pool_profile
        agent_pool_profile = ManagedClusterAgentPoolProfile(
            name='nodepool1',  # Must be 12 chars or less before ACS RP adds to it
            count=int(self.node_count),
            vm_size=self.node_vm_size,
            os_type=self.os_type,
            storage_profile=ContainerServiceStorageProfileTypes.managed_disks,
            vnet_subnet_id=self.vnet_subnet_id,
            max_pods=int(self.max_pods) if self.max_pods else None
        )

        if self.node_osdisk_size:
            agent_pool_profile.os_disk_size_gb = int(self.node_osdisk_size)

        linux_profile = None

        # LinuxProfile is just used for SSH access to VMs, so omit it if --no-ssh-key was specified.
        if not self.no_ssh_key:
            ssh_config = ContainerServiceSshConfiguration(
                public_keys=[ContainerServiceSshPublicKey(key_data=self.ssh_key_value)])
            linux_profile = ContainerServiceLinuxProfile(admin_username=self.admin_username, ssh=ssh_config)

        service_profile = ContainerServiceServicePrincipalProfile(
            client_id=ci_hook.clientId, secret=ci_hook.clientSecret, key_vault_secret_ref=None)

        mc = ManagedCluster(
            location=self.location, tags=self.tags,
            dns_prefix=self.dns_name_prefix,
            kubernetes_version=self.kubernetes_version,
            agent_pool_profiles=[agent_pool_profile],
            linux_profile=linux_profile,
            service_principal_profile=service_profile)

        try:
            logger.info("Checking if the AKS instance {0} is present".format(self.name))
            response = containerservice.managed_clusters.get(self.resource_group, self.name)
            logger.info("Response : {0}".format(response))
            logger.info("AKS instance : {0} found".format(response.name))
            return response
        except CloudError:
            poller = containerservice.managed_clusters.create_or_update(
                resource_group_name=self.resource_group, resource_name=self.name, parameters=mc)
            response = get_poller_result(self, poller)
            logger.info("AKS instance created {0}".format(self.name))
            return response
