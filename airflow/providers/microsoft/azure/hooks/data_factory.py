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
"""
.. spelling::

    CreateRunResponse
    DatasetResource
    LinkedServiceResource
    LROPoller
    PipelineResource
    PipelineRun
    TriggerResource
    datafactory
    mgmt
"""
import inspect
import time
from functools import wraps
from typing import Any, Callable, Dict, Optional, Set, Union

from azure.core.polling import LROPoller
from azure.identity import ClientSecretCredential, DefaultAzureCredential
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.datafactory.models import (
    CreateRunResponse,
    DatasetResource,
    Factory,
    LinkedServiceResource,
    PipelineResource,
    PipelineRun,
    TriggerResource,
)

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.typing_compat import TypedDict

Credentials = Union[ClientSecretCredential, DefaultAzureCredential]


def provide_targeted_factory(func: Callable) -> Callable:
    """
    Provide the targeted factory to the decorated function in case it isn't specified.

    If ``resource_group_name`` or ``factory_name`` is not provided it defaults to the value specified in
    the connection extras.
    """
    signature = inspect.signature(func)

    @wraps(func)
    def wrapper(*args, **kwargs) -> Callable:
        bound_args = signature.bind(*args, **kwargs)

        def bind_argument(arg, default_key):
            # Check if arg was not included in the function signature or, if it is, the value is not provided.
            if arg not in bound_args.arguments or bound_args.arguments[arg] is None:
                self = args[0]
                conn = self.get_connection(self.conn_id)
                default_value = conn.extra_dejson.get(default_key)
                if not default_value:
                    raise AirflowException("Could not determine the targeted data factory.")

                bound_args.arguments[arg] = conn.extra_dejson[default_key]

        bind_argument("resource_group_name", "extra__azure_data_factory__resource_group_name")
        bind_argument("factory_name", "extra__azure_data_factory__factory_name")

        return func(*bound_args.args, **bound_args.kwargs)

    return wrapper


class PipelineRunInfo(TypedDict):
    """Type class for the pipeline run info dictionary."""

    run_id: str
    factory_name: Optional[str]
    resource_group_name: Optional[str]


class AzureDataFactoryPipelineRunStatus:
    """Azure Data Factory pipeline operation statuses."""

    QUEUED = "Queued"
    IN_PROGRESS = "InProgress"
    SUCCEEDED = "Succeeded"
    FAILED = "Failed"
    CANCELING = "Canceling"
    CANCELLED = "Cancelled"

    TERMINAL_STATUSES = {CANCELLED, FAILED, SUCCEEDED}


class AzureDataFactoryPipelineRunException(AirflowException):
    """An exception that indicates a pipeline run failed to complete."""


class AzureDataFactoryHook(BaseHook):
    """
    A hook to interact with Azure Data Factory.

    :param azure_data_factory_conn_id: The :ref:`Azure Data Factory connection id<howto/connection:adf>`.
    """

    conn_type: str = 'azure_data_factory'
    conn_name_attr: str = 'azure_data_factory_conn_id'
    default_conn_name: str = 'azure_data_factory_default'
    hook_name: str = 'Azure Data Factory'

    @staticmethod
    def get_connection_form_widgets() -> Dict[str, Any]:
        """Returns connection widgets to add to connection form"""
        from flask_appbuilder.fieldwidgets import BS3TextFieldWidget
        from flask_babel import lazy_gettext
        from wtforms import StringField

        return {
            "extra__azure_data_factory__tenantId": StringField(
                lazy_gettext('Tenant ID'), widget=BS3TextFieldWidget()
            ),
            "extra__azure_data_factory__subscriptionId": StringField(
                lazy_gettext('Subscription ID'), widget=BS3TextFieldWidget()
            ),
            "extra__azure_data_factory__resource_group_name": StringField(
                lazy_gettext('Resource Group Name'), widget=BS3TextFieldWidget()
            ),
            "extra__azure_data_factory__factory_name": StringField(
                lazy_gettext('Factory Name'), widget=BS3TextFieldWidget()
            ),
        }

    @staticmethod
    def get_ui_field_behaviour() -> Dict[str, Any]:
        """Returns custom field behaviour"""
        return {
            "hidden_fields": ['schema', 'port', 'host', 'extra'],
            "relabeling": {
                'login': 'Client ID',
                'password': 'Secret',
            },
        }

    def __init__(self, azure_data_factory_conn_id: str = default_conn_name):
        self._conn: DataFactoryManagementClient = None
        self.conn_id = azure_data_factory_conn_id
        super().__init__()

    def get_conn(self) -> DataFactoryManagementClient:
        if self._conn is not None:
            return self._conn

        conn = self.get_connection(self.conn_id)
        tenant = conn.extra_dejson.get('extra__azure_data_factory__tenantId')

        try:
            subscription_id = conn.extra_dejson['extra__azure_data_factory__subscriptionId']
        except KeyError:
            raise ValueError("A Subscription ID is required to connect to Azure Data Factory.")

        credential: Credentials
        if conn.login is not None and conn.password is not None:
            if not tenant:
                raise ValueError("A Tenant ID is required when authenticating with Client ID and Secret.")

            credential = ClientSecretCredential(
                client_id=conn.login, client_secret=conn.password, tenant_id=tenant
            )
        else:
            credential = DefaultAzureCredential()
        self._conn = self._create_client(credential, subscription_id)

        return self._conn

    @provide_targeted_factory
    def get_factory(
        self, resource_group_name: Optional[str] = None, factory_name: Optional[str] = None, **config: Any
    ) -> Factory:
        """
        Get the factory.

        :param resource_group_name: The resource group name.
        :param factory_name: The factory name.
        :param config: Extra parameters for the ADF client.
        :return: The factory.
        """
        return self.get_conn().factories.get(resource_group_name, factory_name, **config)

    def _factory_exists(self, resource_group_name, factory_name) -> bool:
        """Return whether or not the factory already exists."""
        factories = {
            factory.name for factory in self.get_conn().factories.list_by_resource_group(resource_group_name)
        }

        return factory_name in factories

    @staticmethod
    def _create_client(credential: Credentials, subscription_id: str):
        return DataFactoryManagementClient(
            credential=credential,
            subscription_id=subscription_id,
        )

    @provide_targeted_factory
    def update_factory(
        self,
        factory: Factory,
        resource_group_name: Optional[str] = None,
        factory_name: Optional[str] = None,
        **config: Any,
    ) -> Factory:
        """
        Update the factory.

        :param factory: The factory resource definition.
        :param resource_group_name: The resource group name.
        :param factory_name: The factory name.
        :param config: Extra parameters for the ADF client.
        :raise AirflowException: If the factory does not exist.
        :return: The factory.
        """
        if not self._factory_exists(resource_group_name, factory_name):
            raise AirflowException(f"Factory {factory!r} does not exist.")

        return self.get_conn().factories.create_or_update(
            resource_group_name, factory_name, factory, **config
        )

    @provide_targeted_factory
    def create_factory(
        self,
        factory: Factory,
        resource_group_name: Optional[str] = None,
        factory_name: Optional[str] = None,
        **config: Any,
    ) -> Factory:
        """
        Create the factory.

        :param factory: The factory resource definition.
        :param resource_group_name: The resource group name.
        :param factory_name: The factory name.
        :param config: Extra parameters for the ADF client.
        :raise AirflowException: If the factory already exists.
        :return: The factory.
        """
        if self._factory_exists(resource_group_name, factory_name):
            raise AirflowException(f"Factory {factory!r} already exists.")

        return self.get_conn().factories.create_or_update(
            resource_group_name, factory_name, factory, **config
        )

    @provide_targeted_factory
    def delete_factory(
        self, resource_group_name: Optional[str] = None, factory_name: Optional[str] = None, **config: Any
    ) -> None:
        """
        Delete the factory.

        :param resource_group_name: The resource group name.
        :param factory_name: The factory name.
        :param config: Extra parameters for the ADF client.
        """
        self.get_conn().factories.delete(resource_group_name, factory_name, **config)

    @provide_targeted_factory
    def get_linked_service(
        self,
        linked_service_name: str,
        resource_group_name: Optional[str] = None,
        factory_name: Optional[str] = None,
        **config: Any,
    ) -> LinkedServiceResource:
        """
        Get the linked service.

        :param linked_service_name: The linked service name.
        :param resource_group_name: The resource group name.
        :param factory_name: The factory name.
        :param config: Extra parameters for the ADF client.
        :return: The linked service.
        """
        return self.get_conn().linked_services.get(
            resource_group_name, factory_name, linked_service_name, **config
        )

    def _linked_service_exists(self, resource_group_name, factory_name, linked_service_name) -> bool:
        """Return whether or not the linked service already exists."""
        linked_services = {
            linked_service.name
            for linked_service in self.get_conn().linked_services.list_by_factory(
                resource_group_name, factory_name
            )
        }

        return linked_service_name in linked_services

    @provide_targeted_factory
    def update_linked_service(
        self,
        linked_service_name: str,
        linked_service: LinkedServiceResource,
        resource_group_name: Optional[str] = None,
        factory_name: Optional[str] = None,
        **config: Any,
    ) -> LinkedServiceResource:
        """
        Update the linked service.

        :param linked_service_name: The linked service name.
        :param linked_service: The linked service resource definition.
        :param resource_group_name: The resource group name.
        :param factory_name: The factory name.
        :param config: Extra parameters for the ADF client.
        :raise AirflowException: If the linked service does not exist.
        :return: The linked service.
        """
        if not self._linked_service_exists(resource_group_name, factory_name, linked_service_name):
            raise AirflowException(f"Linked service {linked_service_name!r} does not exist.")

        return self.get_conn().linked_services.create_or_update(
            resource_group_name, factory_name, linked_service_name, linked_service, **config
        )

    @provide_targeted_factory
    def create_linked_service(
        self,
        linked_service_name: str,
        linked_service: LinkedServiceResource,
        resource_group_name: Optional[str] = None,
        factory_name: Optional[str] = None,
        **config: Any,
    ) -> LinkedServiceResource:
        """
        Create the linked service.

        :param linked_service_name: The linked service name.
        :param linked_service: The linked service resource definition.
        :param resource_group_name: The resource group name.
        :param factory_name: The factory name.
        :param config: Extra parameters for the ADF client.
        :raise AirflowException: If the linked service already exists.
        :return: The linked service.
        """
        if self._linked_service_exists(resource_group_name, factory_name, linked_service_name):
            raise AirflowException(f"Linked service {linked_service_name!r} already exists.")

        return self.get_conn().linked_services.create_or_update(
            resource_group_name, factory_name, linked_service_name, linked_service, **config
        )

    @provide_targeted_factory
    def delete_linked_service(
        self,
        linked_service_name: str,
        resource_group_name: Optional[str] = None,
        factory_name: Optional[str] = None,
        **config: Any,
    ) -> None:
        """
        Delete the linked service.

        :param linked_service_name: The linked service name.
        :param resource_group_name: The linked service name.
        :param factory_name: The factory name.
        :param config: Extra parameters for the ADF client.
        """
        self.get_conn().linked_services.delete(
            resource_group_name, factory_name, linked_service_name, **config
        )

    @provide_targeted_factory
    def get_dataset(
        self,
        dataset_name: str,
        resource_group_name: Optional[str] = None,
        factory_name: Optional[str] = None,
        **config: Any,
    ) -> DatasetResource:
        """
        Get the dataset.

        :param dataset_name: The dataset name.
        :param resource_group_name: The resource group name.
        :param factory_name: The factory name.
        :param config: Extra parameters for the ADF client.
        :return: The dataset.
        """
        return self.get_conn().datasets.get(resource_group_name, factory_name, dataset_name, **config)

    def _dataset_exists(self, resource_group_name, factory_name, dataset_name) -> bool:
        """Return whether or not the dataset already exists."""
        datasets = {
            dataset.name
            for dataset in self.get_conn().datasets.list_by_factory(resource_group_name, factory_name)
        }

        return dataset_name in datasets

    @provide_targeted_factory
    def update_dataset(
        self,
        dataset_name: str,
        dataset: DatasetResource,
        resource_group_name: Optional[str] = None,
        factory_name: Optional[str] = None,
        **config: Any,
    ) -> DatasetResource:
        """
        Update the dataset.

        :param dataset_name: The dataset name.
        :param dataset: The dataset resource definition.
        :param resource_group_name: The resource group name.
        :param factory_name: The factory name.
        :param config: Extra parameters for the ADF client.
        :raise AirflowException: If the dataset does not exist.
        :return: The dataset.
        """
        if not self._dataset_exists(resource_group_name, factory_name, dataset_name):
            raise AirflowException(f"Dataset {dataset_name!r} does not exist.")

        return self.get_conn().datasets.create_or_update(
            resource_group_name, factory_name, dataset_name, dataset, **config
        )

    @provide_targeted_factory
    def create_dataset(
        self,
        dataset_name: str,
        dataset: DatasetResource,
        resource_group_name: Optional[str] = None,
        factory_name: Optional[str] = None,
        **config: Any,
    ) -> DatasetResource:
        """
        Create the dataset.

        :param dataset_name: The dataset name.
        :param dataset: The dataset resource definition.
        :param resource_group_name: The resource group name.
        :param factory_name: The factory name.
        :param config: Extra parameters for the ADF client.
        :raise AirflowException: If the dataset already exists.
        :return: The dataset.
        """
        if self._dataset_exists(resource_group_name, factory_name, dataset_name):
            raise AirflowException(f"Dataset {dataset_name!r} already exists.")

        return self.get_conn().datasets.create_or_update(
            resource_group_name, factory_name, dataset_name, dataset, **config
        )

    @provide_targeted_factory
    def delete_dataset(
        self,
        dataset_name: str,
        resource_group_name: Optional[str] = None,
        factory_name: Optional[str] = None,
        **config: Any,
    ) -> None:
        """
        Delete the dataset.

        :param dataset_name: The dataset name.
        :param resource_group_name: The dataset name.
        :param factory_name: The factory name.
        :param config: Extra parameters for the ADF client.
        """
        self.get_conn().datasets.delete(resource_group_name, factory_name, dataset_name, **config)

    @provide_targeted_factory
    def get_pipeline(
        self,
        pipeline_name: str,
        resource_group_name: Optional[str] = None,
        factory_name: Optional[str] = None,
        **config: Any,
    ) -> PipelineResource:
        """
        Get the pipeline.

        :param pipeline_name: The pipeline name.
        :param resource_group_name: The resource group name.
        :param factory_name: The factory name.
        :param config: Extra parameters for the ADF client.
        :return: The pipeline.
        """
        return self.get_conn().pipelines.get(resource_group_name, factory_name, pipeline_name, **config)

    def _pipeline_exists(self, resource_group_name, factory_name, pipeline_name) -> bool:
        """Return whether or not the pipeline already exists."""
        pipelines = {
            pipeline.name
            for pipeline in self.get_conn().pipelines.list_by_factory(resource_group_name, factory_name)
        }

        return pipeline_name in pipelines

    @provide_targeted_factory
    def update_pipeline(
        self,
        pipeline_name: str,
        pipeline: PipelineResource,
        resource_group_name: Optional[str] = None,
        factory_name: Optional[str] = None,
        **config: Any,
    ) -> PipelineResource:
        """
        Update the pipeline.

        :param pipeline_name: The pipeline name.
        :param pipeline: The pipeline resource definition.
        :param resource_group_name: The resource group name.
        :param factory_name: The factory name.
        :param config: Extra parameters for the ADF client.
        :raise AirflowException: If the pipeline does not exist.
        :return: The pipeline.
        """
        if not self._pipeline_exists(resource_group_name, factory_name, pipeline_name):
            raise AirflowException(f"Pipeline {pipeline_name!r} does not exist.")

        return self.get_conn().pipelines.create_or_update(
            resource_group_name, factory_name, pipeline_name, pipeline, **config
        )

    @provide_targeted_factory
    def create_pipeline(
        self,
        pipeline_name: str,
        pipeline: PipelineResource,
        resource_group_name: Optional[str] = None,
        factory_name: Optional[str] = None,
        **config: Any,
    ) -> PipelineResource:
        """
        Create the pipeline.

        :param pipeline_name: The pipeline name.
        :param pipeline: The pipeline resource definition.
        :param resource_group_name: The resource group name.
        :param factory_name: The factory name.
        :param config: Extra parameters for the ADF client.
        :raise AirflowException: If the pipeline already exists.
        :return: The pipeline.
        """
        if self._pipeline_exists(resource_group_name, factory_name, pipeline_name):
            raise AirflowException(f"Pipeline {pipeline_name!r} already exists.")

        return self.get_conn().pipelines.create_or_update(
            resource_group_name, factory_name, pipeline_name, pipeline, **config
        )

    @provide_targeted_factory
    def delete_pipeline(
        self,
        pipeline_name: str,
        resource_group_name: Optional[str] = None,
        factory_name: Optional[str] = None,
        **config: Any,
    ) -> None:
        """
        Delete the pipeline.

        :param pipeline_name: The pipeline name.
        :param resource_group_name: The pipeline name.
        :param factory_name: The factory name.
        :param config: Extra parameters for the ADF client.
        """
        self.get_conn().pipelines.delete(resource_group_name, factory_name, pipeline_name, **config)

    @provide_targeted_factory
    def run_pipeline(
        self,
        pipeline_name: str,
        resource_group_name: Optional[str] = None,
        factory_name: Optional[str] = None,
        **config: Any,
    ) -> CreateRunResponse:
        """
        Run a pipeline.

        :param pipeline_name: The pipeline name.
        :param resource_group_name: The resource group name.
        :param factory_name: The factory name.
        :param config: Extra parameters for the ADF client.
        :return: The pipeline run.
        """
        return self.get_conn().pipelines.create_run(
            resource_group_name, factory_name, pipeline_name, **config
        )

    @provide_targeted_factory
    def get_pipeline_run(
        self,
        run_id: str,
        resource_group_name: Optional[str] = None,
        factory_name: Optional[str] = None,
        **config: Any,
    ) -> PipelineRun:
        """
        Get the pipeline run.

        :param run_id: The pipeline run identifier.
        :param resource_group_name: The resource group name.
        :param factory_name: The factory name.
        :param config: Extra parameters for the ADF client.
        :return: The pipeline run.
        """
        return self.get_conn().pipeline_runs.get(resource_group_name, factory_name, run_id, **config)

    def get_pipeline_run_status(
        self,
        run_id: str,
        resource_group_name: Optional[str] = None,
        factory_name: Optional[str] = None,
    ) -> str:
        """
        Get a pipeline run's current status.

        :param run_id: The pipeline run identifier.
        :param resource_group_name: The resource group name.
        :param factory_name: The factory name.
        :return: The status of the pipeline run.
        """
        self.log.info(f"Getting the status of run ID {run_id}.")
        pipeline_run_status = self.get_pipeline_run(
            run_id=run_id,
            factory_name=factory_name,
            resource_group_name=resource_group_name,
        ).status
        self.log.info(f"Current status of pipeline run {run_id}: {pipeline_run_status}")

        return pipeline_run_status

    def wait_for_pipeline_run_status(
        self,
        run_id: str,
        expected_statuses: Union[str, Set[str]],
        resource_group_name: Optional[str] = None,
        factory_name: Optional[str] = None,
        check_interval: int = 60,
        timeout: int = 60 * 60 * 24 * 7,
    ) -> bool:
        """
        Waits for a pipeline run to match an expected status.

        :param run_id: The pipeline run identifier.
        :param expected_statuses: The desired status(es) to check against a pipeline run's current status.
        :param resource_group_name: The resource group name.
        :param factory_name: The factory name.
        :param check_interval: Time in seconds to check on a pipeline run's status.
        :param timeout: Time in seconds to wait for a pipeline to reach a terminal status or the expected
            status.
        :return: Boolean indicating if the pipeline run has reached the ``expected_status``.
        """
        pipeline_run_info = PipelineRunInfo(
            run_id=run_id,
            factory_name=factory_name,
            resource_group_name=resource_group_name,
        )
        pipeline_run_status = self.get_pipeline_run_status(**pipeline_run_info)

        start_time = time.monotonic()

        while (
            pipeline_run_status not in AzureDataFactoryPipelineRunStatus.TERMINAL_STATUSES
            and pipeline_run_status not in expected_statuses
        ):
            # Check if the pipeline-run duration has exceeded the ``timeout`` configured.
            if start_time + timeout < time.monotonic():
                raise AzureDataFactoryPipelineRunException(
                    f"Pipeline run {run_id} has not reached a terminal status after {timeout} seconds."
                )

            # Wait to check the status of the pipeline run based on the ``check_interval`` configured.
            time.sleep(check_interval)

            pipeline_run_status = self.get_pipeline_run_status(**pipeline_run_info)

        return pipeline_run_status in expected_statuses

    @provide_targeted_factory
    def cancel_pipeline_run(
        self,
        run_id: str,
        resource_group_name: Optional[str] = None,
        factory_name: Optional[str] = None,
        **config: Any,
    ) -> None:
        """
        Cancel the pipeline run.

        :param run_id: The pipeline run identifier.
        :param resource_group_name: The resource group name.
        :param factory_name: The factory name.
        :param config: Extra parameters for the ADF client.
        """
        self.get_conn().pipeline_runs.cancel(resource_group_name, factory_name, run_id, **config)

    @provide_targeted_factory
    def get_trigger(
        self,
        trigger_name: str,
        resource_group_name: Optional[str] = None,
        factory_name: Optional[str] = None,
        **config: Any,
    ) -> TriggerResource:
        """
        Get the trigger.

        :param trigger_name: The trigger name.
        :param resource_group_name: The resource group name.
        :param factory_name: The factory name.
        :param config: Extra parameters for the ADF client.
        :return: The trigger.
        """
        return self.get_conn().triggers.get(resource_group_name, factory_name, trigger_name, **config)

    def _trigger_exists(self, resource_group_name, factory_name, trigger_name) -> bool:
        """Return whether or not the trigger already exists."""
        triggers = {
            trigger.name
            for trigger in self.get_conn().triggers.list_by_factory(resource_group_name, factory_name)
        }

        return trigger_name in triggers

    @provide_targeted_factory
    def update_trigger(
        self,
        trigger_name: str,
        trigger: TriggerResource,
        resource_group_name: Optional[str] = None,
        factory_name: Optional[str] = None,
        **config: Any,
    ) -> TriggerResource:
        """
        Update the trigger.

        :param trigger_name: The trigger name.
        :param trigger: The trigger resource definition.
        :param resource_group_name: The resource group name.
        :param factory_name: The factory name.
        :param config: Extra parameters for the ADF client.
        :raise AirflowException: If the trigger does not exist.
        :return: The trigger.
        """
        if not self._trigger_exists(resource_group_name, factory_name, trigger_name):
            raise AirflowException(f"Trigger {trigger_name!r} does not exist.")

        return self.get_conn().triggers.create_or_update(
            resource_group_name, factory_name, trigger_name, trigger, **config
        )

    @provide_targeted_factory
    def create_trigger(
        self,
        trigger_name: str,
        trigger: TriggerResource,
        resource_group_name: Optional[str] = None,
        factory_name: Optional[str] = None,
        **config: Any,
    ) -> TriggerResource:
        """
        Create the trigger.

        :param trigger_name: The trigger name.
        :param trigger: The trigger resource definition.
        :param resource_group_name: The resource group name.
        :param factory_name: The factory name.
        :param config: Extra parameters for the ADF client.
        :raise AirflowException: If the trigger already exists.
        :return: The trigger.
        """
        if self._trigger_exists(resource_group_name, factory_name, trigger_name):
            raise AirflowException(f"Trigger {trigger_name!r} already exists.")

        return self.get_conn().triggers.create_or_update(
            resource_group_name, factory_name, trigger_name, trigger, **config
        )

    @provide_targeted_factory
    def delete_trigger(
        self,
        trigger_name: str,
        resource_group_name: Optional[str] = None,
        factory_name: Optional[str] = None,
        **config: Any,
    ) -> None:
        """
        Delete the trigger.

        :param trigger_name: The trigger name.
        :param resource_group_name: The resource group name.
        :param factory_name: The factory name.
        :param config: Extra parameters for the ADF client.
        """
        self.get_conn().triggers.delete(resource_group_name, factory_name, trigger_name, **config)

    @provide_targeted_factory
    def start_trigger(
        self,
        trigger_name: str,
        resource_group_name: Optional[str] = None,
        factory_name: Optional[str] = None,
        **config: Any,
    ) -> LROPoller:
        """
        Start the trigger.

        :param trigger_name: The trigger name.
        :param resource_group_name: The resource group name.
        :param factory_name: The factory name.
        :param config: Extra parameters for the ADF client.
        :return: An Azure operation poller.
        """
        return self.get_conn().triggers.begin_start(resource_group_name, factory_name, trigger_name, **config)

    @provide_targeted_factory
    def stop_trigger(
        self,
        trigger_name: str,
        resource_group_name: Optional[str] = None,
        factory_name: Optional[str] = None,
        **config: Any,
    ) -> LROPoller:
        """
        Stop the trigger.

        :param trigger_name: The trigger name.
        :param resource_group_name: The resource group name.
        :param factory_name: The factory name.
        :param config: Extra parameters for the ADF client.
        :return: An Azure operation poller.
        """
        return self.get_conn().triggers.begin_stop(resource_group_name, factory_name, trigger_name, **config)

    @provide_targeted_factory
    def rerun_trigger(
        self,
        trigger_name: str,
        run_id: str,
        resource_group_name: Optional[str] = None,
        factory_name: Optional[str] = None,
        **config: Any,
    ) -> None:
        """
        Rerun the trigger.

        :param trigger_name: The trigger name.
        :param run_id: The trigger run identifier.
        :param resource_group_name: The resource group name.
        :param factory_name: The factory name.
        :param config: Extra parameters for the ADF client.
        """
        return self.get_conn().trigger_runs.rerun(
            resource_group_name, factory_name, trigger_name, run_id, **config
        )

    @provide_targeted_factory
    def cancel_trigger(
        self,
        trigger_name: str,
        run_id: str,
        resource_group_name: Optional[str] = None,
        factory_name: Optional[str] = None,
        **config: Any,
    ) -> None:
        """
        Cancel the trigger.

        :param trigger_name: The trigger name.
        :param run_id: The trigger run identifier.
        :param resource_group_name: The resource group name.
        :param factory_name: The factory name.
        :param config: Extra parameters for the ADF client.
        """
        self.get_conn().trigger_runs.cancel(resource_group_name, factory_name, trigger_name, run_id, **config)
