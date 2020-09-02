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
from datetime import timedelta
from typing import Optional

from azure.batch import BatchServiceClient, batch_auth, models as batch_models

from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook
from airflow.utils import timezone


class AzureBatchHook(BaseHook):
    """
    Hook for Azure Batch APIs
    """

    def __init__(self, azure_batch_conn_id='azure_batch_default'):
        super().__init__()
        self.conn_id = azure_batch_conn_id
        self.connection = self.get_conn()
        self.extra = self._connection().extra_dejson

    def _connection(self):
        """
        Get connected to azure batch service
        """
        conn = self.get_connection(self.conn_id)
        return conn

    def get_conn(self):
        """
        Get the batch client connection

        :return: Azure batch client
        """
        conn = self._connection()

        def _get_required_param(name):
            """Extract required parameter from extra JSON, raise exception if not found"""
            value = conn.extra_dejson.get(name)
            if not value:
                raise AirflowException(
                    'Extra connection option is missing required parameter: `{}`'.format(name)
                )
            return value

        batch_account_name = _get_required_param('account_name')
        batch_account_key = _get_required_param('account_key')
        batch_account_url = _get_required_param('account_url')
        credentials = batch_auth.SharedKeyCredentials(batch_account_name, batch_account_key)
        batch_client = BatchServiceClient(credentials, batch_url=batch_account_url)
        return batch_client

    def configure_pool(
        self,
        pool_id: str,
        vm_size: str,
        display_name: Optional[str] = None,
        target_dedicated_nodes: Optional[int] = None,
        use_latest_image_and_sku: bool = False,
        vm_publisher: Optional[str] = None,
        vm_offer: Optional[str] = None,
        sku_starts_with: Optional[str] = None,
        **kwargs,
    ):
        """
        Configures a pool

        :param pool_id: A string that uniquely identifies the Pool within the Account
        :type pool_id: str

        :param vm_size: The size of virtual machines in the Pool.
        :type vm_size: str

        :param display_name: The display name for the Pool
        :type display_name: str

        :param target_dedicated_nodes: The desired number of dedicated Compute Nodes in the Pool.
        :type target_dedicated_nodes: Optional[int]

        :param use_latest_image_and_sku: Whether to use the latest verified vm image and sku
        :type use_latest_image_and_sku: bool

        :param vm_publisher: The publisher of the Azure Virtual Machines Marketplace Image.
            For example, Canonical or MicrosoftWindowsServer.
        :type vm_publisher: Optional[str]

        :param vm_offer: The offer type of the Azure Virtual Machines Marketplace Image.
            For example, UbuntuServer or WindowsServer.
        :type vm_offer: Optional[str]

        :param sku_starts_with: The start name of the sku to search
        :type sku_starts_with: Optional[str]
        """
        if use_latest_image_and_sku:
            self.log.info('Using latest verified virtual machine image with node agent sku')
            sku_to_use, image_ref_to_use = self._get_latest_verified_image_vm_and_sku(
                publisher=vm_publisher, offer=vm_offer, sku_starts_with=sku_starts_with
            )
            pool = batch_models.PoolAddParameter(
                id=pool_id,
                vm_size=vm_size,
                display_name=display_name,
                virtual_machine_configuration=batch_models.VirtualMachineConfiguration(
                    image_reference=image_ref_to_use, node_agent_sku_id=sku_to_use
                ),
                target_dedicated_nodes=target_dedicated_nodes,
                **kwargs,
            )

        elif self.extra.get('os_family'):
            self.log.info(
                'Using cloud service configuration to create pool, ' 'virtual machine configuration ignored'
            )
            pool = batch_models.PoolAddParameter(
                id=pool_id,
                vm_size=vm_size,
                display_name=display_name,
                cloud_service_configuration=batch_models.CloudServiceConfiguration(
                    os_family=self.extra.get('os_family'), os_version=self.extra.get('os_version')
                ),
                target_dedicated_nodes=target_dedicated_nodes,
                **kwargs,
            )

        else:
            self.log.info('Using virtual machine configuration to create a pool')
            pool = batch_models.PoolAddParameter(
                id=pool_id,
                vm_size=vm_size,
                display_name=display_name,
                virtual_machine_configuration=batch_models.VirtualMachineConfiguration(
                    image_reference=batch_models.ImageReference(
                        publisher=self.extra.get('vm_publisher'),
                        offer=self.extra.get('vm_offer'),
                        sku=self.extra.get('vm_sku'),
                        version=self.extra.get("vm_version"),
                    ),
                    node_agent_sku_id=self.extra.get('node_agent_sku_id'),
                ),
                target_dedicated_nodes=target_dedicated_nodes,
                **kwargs,
            )
        return pool

    def create_pool(self, pool):
        """
        Creates a pool if not already existing

        :param pool: the pool object to create
        :type pool: batch_models.PoolAddParameter

        """
        try:
            self.log.info("Attempting to create a pool: %s", pool.id)
            self.connection.pool.add(pool)
            self.log.info("Created pool: %s", pool.id)
        except batch_models.BatchErrorException as e:
            if e.error.code != "PoolExists":
                raise
            else:
                self.log.info("Pool %s already exists", pool.id)

    def _get_latest_verified_image_vm_and_sku(self, publisher, offer, sku_starts_with):
        """
        Get latest verified image vm and sku

        :param publisher: The publisher of the Azure Virtual Machines Marketplace Image.
            For example, Canonical or MicrosoftWindowsServer.
        :type publisher: str
        :param offer: The offer type of the Azure Virtual Machines Marketplace Image.
            For example, UbuntuServer or WindowsServer.
        :type offer: str
        :param sku_starts_with: The start name of the sku to search
        :type sku_starts_with: str
        """

        options = batch_models.AccountListSupportedImagesOptions(filter="verificationType eq 'verified'")
        images = self.connection.account.list_supported_images(account_list_supported_images_options=options)
        # pick the latest supported sku
        skus_to_use = [
            (image.node_agent_sku_id, image.image_reference)
            for image in images
            if image.image_reference.publisher.lower() == publisher.lower()
            and image.image_reference.offer.lower() == offer.lower()
            and image.image_reference.sku.startswith(sku_starts_with)
        ]

        # pick first
        agent_sku_id, image_ref_to_use = skus_to_use[0]
        return agent_sku_id, image_ref_to_use

    def wait_for_all_node_state(self, pool_id, node_state):
        """
        Wait for all nodes in a pool to reach given states

        :param pool_id: A string that identifies the pool
        :type pool_id: str
        :param node_state: A set of batch_models.ComputeNodeState
        :type node_state: set
        """
        self.log.info('waiting for all nodes in pool %s to reach one of: %s', pool_id, node_state)
        while True:
            # refresh pool to ensure that there is no resize error
            pool = self.connection.pool.get(pool_id)
            if pool.resize_errors is not None:
                resize_errors = "\n".join([repr(e) for e in pool.resize_errors])
                raise RuntimeError('resize error encountered for pool {}:\n{}'.format(pool.id, resize_errors))
            nodes = list(self.connection.compute_node.list(pool.id))
            if len(nodes) >= pool.target_dedicated_nodes and all(node.state in node_state for node in nodes):
                return nodes
            # Allow the timeout to be controlled by the AzureBatchOperator
            # specified timeout. This way we don't interrupt a startTask inside
            # the pool
            time.sleep(10)

    def configure_job(self, job_id: str, pool_id: str, display_name: Optional[str] = None, **kwargs):
        """
        Configures a job for use in the pool

        :param job_id: A string that uniquely identifies the job within the account
        :type job_id: str
        :param pool_id: A string that identifies the pool
        :type pool_id: str
        :param display_name: The display name for the job
        :type display_name: str
        """

        job = batch_models.JobAddParameter(
            id=job_id,
            pool_info=batch_models.PoolInformation(pool_id=pool_id),
            display_name=display_name,
            **kwargs,
        )
        return job

    def create_job(self, job):
        """
        Creates a job in the pool

        :param job: The job object to create
        :type job: batch_models.JobAddParameter
        """
        try:
            self.connection.job.add(job)
            self.log.info("Job %s created", job.id)
        except batch_models.BatchErrorException as err:
            if err.error.code != "JobExists":
                raise
            else:
                self.log.info("Job %s already exists", job.id)

    def configure_task(
        self,
        task_id: str,
        command_line: str,
        display_name: Optional[str] = None,
        container_settings=None,
        **kwargs,
    ):
        """
        Creates a task

        :param task_id: A string that identifies the task to create
        :type task_id: str
        :param command_line: The command line of the Task.
        :type command_line: str
        :param display_name: A display name for the Task
        :type display_name: str
        :param container_settings: The settings for the container under which the Task runs.
            If the Pool that will run this Task has containerConfiguration set,
            this must be set as well. If the Pool that will run this Task doesn't have
            containerConfiguration set, this must not be set.
        :type container_settings: batch_models.TaskContainerSettings
        """
        task = batch_models.TaskAddParameter(
            id=task_id,
            command_line=command_line,
            display_name=display_name,
            container_settings=container_settings,
            **kwargs,
        )
        self.log.info("Task created: %s", task_id)
        return task

    def add_single_task_to_job(self, job_id, task):
        """
        Add a single task to given job if it doesn't exist

        :param job_id: A string that identifies the given job
        :type job_id: str
        :param task: The task to add
        :type task: batch_models.TaskAddParameter
        """
        try:

            self.connection.task.add(job_id=job_id, task=task)
        except batch_models.BatchErrorException as err:
            if err.error.code != "TaskExists":
                raise
            else:
                self.log.info("Task %s already exists", task.id)

    def wait_for_job_tasks_to_complete(self, job_id, timeout):
        """
        Wait for tasks in a particular job to complete

        :param job_id: A string that identifies the job
        :type job_id: str
        :param timeout: The amount of time to wait before timing out in minutes
        :type timeout: int
        """
        timeout_time = timezone.utcnow() + timedelta(minutes=timeout)
        while timezone.utcnow() < timeout_time:
            tasks = self.connection.task.list(job_id)

            incomplete_tasks = [task for task in tasks if task.state != batch_models.TaskState.completed]
            if not incomplete_tasks:
                return
            for task in incomplete_tasks:
                self.log.info("Waiting for %s to complete, currently on %s state", task.id, task.state)
            time.sleep(15)
        raise TimeoutError("Timed out waiting for tasks to complete")
