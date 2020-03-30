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
from airflow.models import BaseOperator
from airflow.providers.microsoft.azure.hooks.azure_batch import AzureBatchHook
from airflow.utils.decorators import apply_defaults
from azure.batch import models as batch_models


class AzureBatchOperator(BaseOperator):
    """

    :param batch_pool_id:
    :param batch_pool_vm_size:
    :param batch_job_id:
    :param batch_task_id:
    :param batch_task_command_line:
    :param batch_pool_display_name:
    :param batch_job_display_name:
    :param batch_task_display_name:
    :param batch_task_container_settings:
    :param target_dedicated_nodes:
    :param azure_batch_conn_id:
    :param use_latest_verified_vm_image_and_sku:
    :param vm_publisher:
    :param vm_offer:
    :param sku_starts_with:
    :param timeout:
    :param should_delete_job:
    :param should_delete_pool:
    :param args:
    :param kwargs:
            """
    template_fields = ('batch_pool_id', 'batch_pool_vm_size', 'batch_job_id',
                       'batch_task_id', 'batch_task_command_line')
    ui_color = '#f0f0e4'

    @apply_defaults
    def __init__(self,
                 batch_pool_id: str,
                 batch_pool_vm_size: str,
                 batch_job_id: str,
                 batch_task_id: str,
                 batch_task_command_line: str,
                 batch_pool_display_name: str = None,
                 batch_job_display_name: str = None,
                 batch_task_display_name: str = None,
                 batch_task_container_settings: batch_models.TaskContainerSettings = None,
                 batch_max_retries: int = 3,
                 target_dedicated_nodes: int = None,
                 azure_batch_conn_id: str = None,
                 use_latest_verified_vm_image_and_sku: bool = False,
                 vm_publisher: str = None,
                 vm_offer: str = None,
                 sku_starts_with: str = None,
                 timeout: int = 25,

                 should_delete_job: bool = False,
                 should_delete_pool: bool = False,
                 *args,
                 **kwargs):  # pylint: disable=too-many-arguments

        super().__init__(*args, **kwargs)
        self.batch_pool_id = batch_pool_id
        self.batch_pool_vm_size = batch_pool_vm_size
        self.batch_job_id = batch_job_id
        self.batch_task_id = batch_task_id
        self.batch_task_command_line = batch_task_command_line
        self.batch_pool_display_name = batch_pool_display_name
        self.batch_job_display_name = batch_job_display_name
        self.batch_task_display_name = batch_task_display_name
        self.batch_task_container_settings = batch_task_container_settings
        self.batch_max_retries = batch_max_retries
        self.target_dedicated_nodes = target_dedicated_nodes
        self.azure_batch_conn_id = azure_batch_conn_id
        self.use_latest_image = use_latest_verified_vm_image_and_sku
        self.vm_publisher = vm_publisher
        self.vm_offer = vm_offer
        self.sku_starts_with = sku_starts_with
        self.timeout = timeout
        self.hook = self.get_hook()

    def execute(self, context):

        self.hook.connection.config.retry_policy = self.batch_max_retries

        pool = self.hook.configure_pool(
                    pool_id=self.batch_pool_id,
                    vm_size=self.batch_pool_vm_size,
                    display_name=self.batch_pool_display_name,
                    target_dedicated_nodes=self.target_dedicated_nodes,
                    use_latest_verified_vm_image_and_sku=self.use_latest_image,
                    vm_publisher=self.vm_publisher,
                    vm_offer=self.vm_offer,
                    sku_starts_with=self.sku_starts_with,
            )
        # Create pool if not already exist
        self.hook.create_pool(pool)
        self.hook.wait_for_all_node_state(self.batch_pool_id,
                                          {batch_models.ComputeNodeState.start_task_failed,
                                           batch_models.ComputeNodeState.unusable,
                                           batch_models.ComputeNodeState.idle}
                                          )
        # Create job if not already exist
        job = self.hook.configure_job(job_id=self.batch_job_id,
                                      pool_id=self.batch_pool_id,
                                      display_name=self.batch_job_display_name,)
        self.hook.create_job(job)
        # Create task
        task = self.hook.configure_task(
            task_id=self.batch_task_id,
            command_line=self.batch_task_command_line,
            display_name=self.batch_task_display_name,
            container_settings=self.batch_task_container_settings
        )
        # Add task to job
        self.hook.add_single_task_to_job(
            job_id=self.batch_job_id,
            task=task
        )
        # Wait for tasks to complete
        self.hook.wait_for_job_tasks_to_complete(
            job_id=self.batch_job_id,
            timeout=self.timeout)

    def on_kill(self) -> None:
        response = self.hook.connection.job.terminate(
            job_id=self.batch_job_id,
            terminate_reason='Job killed by user'
        )
        self.log.info("Azure Batch job (%s) terminated: %s", self.batch_job_id, response)

    def get_hook(self):
        """Create and return an AzureBatchHook."""
        return AzureBatchHook(
            azure_batch_conn_id=self.azure_batch_conn_id
        )
