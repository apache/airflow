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
from datetime import timedelta
from typing import Any

from azure.batch import BatchServiceClient, batch_auth, models as batch_models
from azure.batch.models import JobAddParameter, PoolAddParameter, TaskAddParameter

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.models import Connection
from airflow.providers.microsoft.azure.utils import get_field
from airflow.utils import timezone


class AzureBatchHook(BaseHook):
    """
    Hook for Azure Batch APIs

    :param azure_batch_conn_id: :ref:`Azure Batch connection id<howto/connection:azure_batch>`
        of a service principal which will be used to start the container instance.
    """

    conn_name_attr = "azure_batch_conn_id"
    default_conn_name = "azure_batch_default"
    conn_type = "azure_batch"
    hook_name = "Azure Batch Service"

    def _get_field(self, extras, name):
        return get_field(
            conn_id=self.conn_id,
            conn_type=self.conn_type,
            extras=extras,
            field_name=name,
        )

    @staticmethod
    def get_connection_form_widgets() -> dict[str, Any]:
        """Returns connection widgets to add to connection form"""
        from flask_appbuilder.fieldwidgets import BS3TextFieldWidget
        from flask_babel import lazy_gettext
        from wtforms import StringField

        return {
            "account_url": StringField(lazy_gettext("Batch Account URL"), widget=BS3TextFieldWidget()),
        }

    @staticmethod
    def get_ui_field_behaviour() -> dict[str, Any]:
        """Returns custom field behaviour"""
        return {
            "hidden_fields": ["schema", "port", "host", "extra"],
            "relabeling": {
                "login": "Batch Account Name",
                "password": "Batch Account Access Key",
            },
        }

    def __init__(self, azure_batch_conn_id: str = default_conn_name) -> None:
        super().__init__()
        self.conn_id = azure_batch_conn_id
        self.connection = self.get_conn()

    def _connection(self) -> Connection:
        """Get connected to Azure Batch service"""
        conn = self.get_connection(self.conn_id)
        return conn

    def get_conn(self):
        """
        Get the Batch client connection

        :return: Azure Batch client
        """
        conn = self._connection()

        batch_account_url = self._get_field(conn.extra_dejson, "account_url")
        if not batch_account_url:
            raise AirflowException("Batch Account URL parameter is missing.")

        credentials = batch_auth.SharedKeyCredentials(conn.login, conn.password)
        batch_client = BatchServiceClient(credentials, batch_url=batch_account_url)
        return batch_client

    def configure_pool(
        self,
        pool_id: str,
        vm_size: str,
        vm_node_agent_sku_id: str,
        vm_publisher: str | None = None,
        vm_offer: str | None = None,
        sku_starts_with: str | None = None,
        vm_sku: str | None = None,
        vm_version: str | None = None,
        os_family: str | None = None,
        os_version: str | None = None,
        display_name: str | None = None,
        target_dedicated_nodes: int | None = None,
        use_latest_image_and_sku: bool = False,
        **kwargs,
    ) -> PoolAddParameter:
        """
        Configures a pool

        :param pool_id: A string that uniquely identifies the Pool within the Account

        :param vm_size: The size of virtual machines in the Pool.

        :param display_name: The display name for the Pool

        :param target_dedicated_nodes: The desired number of dedicated Compute Nodes in the Pool.

        :param use_latest_image_and_sku: Whether to use the latest verified vm image and sku

        :param vm_publisher: The publisher of the Azure Virtual Machines Marketplace Image.
            For example, Canonical or MicrosoftWindowsServer.

        :param vm_offer: The offer type of the Azure Virtual Machines Marketplace Image.
            For example, UbuntuServer or WindowsServer.

        :param sku_starts_with: The start name of the sku to search

        :param vm_sku: The name of the virtual machine sku to use

        :param vm_version: The version of the virtual machine
        :param vm_version: str

        :param vm_node_agent_sku_id: The node agent sku id of the virtual machine

        :param os_family: The Azure Guest OS family to be installed on the virtual machines in the Pool.

        :param os_version: The OS family version

        """
        if use_latest_image_and_sku:
            self.log.info("Using latest verified virtual machine image with node agent sku")
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

        elif os_family:
            self.log.info(
                "Using cloud service configuration to create pool, virtual machine configuration ignored"
            )
            pool = batch_models.PoolAddParameter(
                id=pool_id,
                vm_size=vm_size,
                display_name=display_name,
                cloud_service_configuration=batch_models.CloudServiceConfiguration(
                    os_family=os_family, os_version=os_version
                ),
                target_dedicated_nodes=target_dedicated_nodes,
                **kwargs,
            )

        else:
            self.log.info("Using virtual machine configuration to create a pool")
            pool = batch_models.PoolAddParameter(
                id=pool_id,
                vm_size=vm_size,
                display_name=display_name,
                virtual_machine_configuration=batch_models.VirtualMachineConfiguration(
                    image_reference=batch_models.ImageReference(
                        publisher=vm_publisher,
                        offer=vm_offer,
                        sku=vm_sku,
                        version=vm_version,
                    ),
                    node_agent_sku_id=vm_node_agent_sku_id,
                ),
                target_dedicated_nodes=target_dedicated_nodes,
                **kwargs,
            )
        return pool

    def create_pool(self, pool: PoolAddParameter) -> None:
        """
        Creates a pool if not already existing

        :param pool: the pool object to create

        """
        try:
            self.log.info("Attempting to create a pool: %s", pool.id)
            self.connection.pool.add(pool)
            self.log.info("Created pool: %s", pool.id)
        except batch_models.BatchErrorException as err:
            if not err.error or err.error.code != "PoolExists":
                raise
            else:
                self.log.info("Pool %s already exists", pool.id)

    def _get_latest_verified_image_vm_and_sku(
        self,
        publisher: str | None = None,
        offer: str | None = None,
        sku_starts_with: str | None = None,
    ) -> tuple:
        """
        Get latest verified image vm and sku

        :param publisher: The publisher of the Azure Virtual Machines Marketplace Image.
            For example, Canonical or MicrosoftWindowsServer.
        :param offer: The offer type of the Azure Virtual Machines Marketplace Image.
            For example, UbuntuServer or WindowsServer.
        :param sku_starts_with: The start name of the sku to search
        """
        options = batch_models.AccountListSupportedImagesOptions(filter="verificationType eq 'verified'")
        images = self.connection.account.list_supported_images(account_list_supported_images_options=options)
        # pick the latest supported sku
        skus_to_use = [
            (image.node_agent_sku_id, image.image_reference)
            for image in images
            if image.image_reference.publisher.lower() == publisher
            and image.image_reference.offer.lower() == offer
            and image.image_reference.sku.startswith(sku_starts_with)
        ]

        # pick first
        agent_sku_id, image_ref_to_use = skus_to_use[0]
        return agent_sku_id, image_ref_to_use

    def wait_for_all_node_state(self, pool_id: str, node_state: set) -> list:
        """
        Wait for all nodes in a pool to reach given states

        :param pool_id: A string that identifies the pool
        :param node_state: A set of batch_models.ComputeNodeState
        """
        self.log.info("waiting for all nodes in pool %s to reach one of: %s", pool_id, node_state)
        while True:
            # refresh pool to ensure that there is no resize error
            pool = self.connection.pool.get(pool_id)
            if pool.resize_errors is not None:
                resize_errors = "\n".join(repr(e) for e in pool.resize_errors)
                raise RuntimeError(f"resize error encountered for pool {pool.id}:\n{resize_errors}")
            nodes = list(self.connection.compute_node.list(pool.id))
            if len(nodes) >= pool.target_dedicated_nodes and all(node.state in node_state for node in nodes):
                return nodes
            # Allow the timeout to be controlled by the AzureBatchOperator
            # specified timeout. This way we don't interrupt a startTask inside
            # the pool
            time.sleep(10)

    def configure_job(
        self,
        job_id: str,
        pool_id: str,
        display_name: str | None = None,
        **kwargs,
    ) -> JobAddParameter:
        """
        Configures a job for use in the pool

        :param job_id: A string that uniquely identifies the job within the account
        :param pool_id: A string that identifies the pool
        :param display_name: The display name for the job
        """
        job = batch_models.JobAddParameter(
            id=job_id,
            pool_info=batch_models.PoolInformation(pool_id=pool_id),
            display_name=display_name,
            **kwargs,
        )
        return job

    def create_job(self, job: JobAddParameter) -> None:
        """
        Creates a job in the pool

        :param job: The job object to create
        """
        try:
            self.connection.job.add(job)
            self.log.info("Job %s created", job.id)
        except batch_models.BatchErrorException as err:
            if not err.error or err.error.code != "JobExists":
                raise
            else:
                self.log.info("Job %s already exists", job.id)

    def configure_task(
        self,
        task_id: str,
        command_line: str,
        display_name: str | None = None,
        container_settings=None,
        **kwargs,
    ) -> TaskAddParameter:
        """
        Creates a task

        :param task_id: A string that identifies the task to create
        :param command_line: The command line of the Task.
        :param display_name: A display name for the Task
        :param container_settings: The settings for the container under which the Task runs.
            If the Pool that will run this Task has containerConfiguration set,
            this must be set as well. If the Pool that will run this Task doesn't have
            containerConfiguration set, this must not be set.
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

    def add_single_task_to_job(self, job_id: str, task: TaskAddParameter) -> None:
        """
        Add a single task to given job if it doesn't exist

        :param job_id: A string that identifies the given job
        :param task: The task to add
        """
        try:

            self.connection.task.add(job_id=job_id, task=task)
        except batch_models.BatchErrorException as err:
            if not err.error or err.error.code != "TaskExists":
                raise
            else:
                self.log.info("Task %s already exists", task.id)

    def wait_for_job_tasks_to_complete(self, job_id: str, timeout: int) -> list[batch_models.CloudTask]:
        """
        Wait for tasks in a particular job to complete

        :param job_id: A string that identifies the job
        :param timeout: The amount of time to wait before timing out in minutes
        """
        timeout_time = timezone.utcnow() + timedelta(minutes=timeout)
        while timezone.utcnow() < timeout_time:
            tasks = self.connection.task.list(job_id)

            incomplete_tasks = [task for task in tasks if task.state != batch_models.TaskState.completed]
            if not incomplete_tasks:
                # detect if any task in job has failed
                fail_tasks = [
                    task
                    for task in tasks
                    if task.executionInfo.result == batch_models.TaskExecutionResult.failure
                ]
                return fail_tasks
            for task in incomplete_tasks:
                self.log.info("Waiting for %s to complete, currently on %s state", task.id, task.state)
            time.sleep(15)
        raise TimeoutError("Timed out waiting for tasks to complete")

    def test_connection(self):
        """Test a configured Azure Batch connection."""
        try:
            # Attempt to list existing  jobs under the configured Batch account and retrieve
            # the first in the returned iterator. The Azure Batch API does allow for creation of a
            # BatchServiceClient with incorrect values but then will fail properly once items are
            # retrieved using the client. We need to _actually_ try to retrieve an object to properly
            # test the connection.
            next(self.get_conn().job.list(), None)
        except Exception as e:
            return False, str(e)
        return True, "Successfully connected to Azure Batch."
