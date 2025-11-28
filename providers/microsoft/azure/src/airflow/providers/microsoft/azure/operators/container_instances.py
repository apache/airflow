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

import re
import time
from collections import namedtuple
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, cast

from azure.mgmt.containerinstance.models import (
    Container,
    ContainerGroup,
    ContainerGroupDiagnostics,
    ContainerGroupIdentity,
    ContainerGroupSubnetId,
    ContainerPort,
    DnsConfiguration,
    EnvironmentVariable,
    IpAddress,
    ResourceIdentityType,
    ResourceRequests,
    ResourceRequirements,
    UserAssignedIdentities,
    Volume as _AzureVolume,
    VolumeMount,
)
from msrestazure.azure_exceptions import CloudError

from airflow.exceptions import AirflowException
from airflow.providers.common.compat.sdk import AirflowTaskTimeout, BaseOperator
from airflow.providers.microsoft.azure.hooks.container_instance import AzureContainerInstanceHook
from airflow.providers.microsoft.azure.hooks.container_registry import AzureContainerRegistryHook
from airflow.providers.microsoft.azure.hooks.container_volume import AzureContainerVolumeHook

if TYPE_CHECKING:
    from airflow.utils.context import Context

Volume = namedtuple(
    "Volume",
    ["conn_id", "account_name", "share_name", "mount_path", "read_only"],
)

DEFAULT_ENVIRONMENT_VARIABLES: dict[str, str] = {}
DEFAULT_SECURED_VARIABLES: Sequence[str] = []
DEFAULT_VOLUMES: Sequence[Volume] = []
DEFAULT_MEMORY_IN_GB = 2.0
DEFAULT_CPU = 1.0


class AzureContainerInstancesOperator(BaseOperator):
    """
    Start a container on Azure Container Instances.

    :param ci_conn_id: connection id of a service principal which will be used
        to start the container instance
    :param registry_conn_id: connection id of a user which can login to a
        private docker registry. For Azure use :ref:`Azure connection id<howto/connection:azure>`
    :param resource_group: name of the resource group wherein this container
        instance should be started
    :param name: name of this container instance. Please note this name has
        to be unique in order to run containers in parallel.
    :param image: the docker image to be used
    :param region: the region wherein this container instance should be started
    :param environment_variables: key,value pairs containing environment
        variables which will be passed to the running container
    :param secured_variables: names of environmental variables that should not
        be exposed outside the container (typically passwords).
    :param volumes: list of ``Volume`` tuples to be mounted to the container.
        Currently only Azure Fileshares are supported.
    :param memory_in_gb: the amount of memory to allocate to this container
    :param cpu: the number of cpus to allocate to this container
    :param gpu: GPU Resource for the container.
    :param command: the command to run inside the container
    :param container_timeout: max time allowed for the execution of
        the container instance.
    :param tags: azure tags as dict of str:str
    :param xcom_all: Control if logs are pushed to XCOM similarly to how DockerOperator does.
        Possible values include: 'None', 'True', 'False'. Defaults to 'None', meaning no logs
        are pushed to XCOM which is the historical behaviour. 'True' means push all logs to XCOM
        which may run the risk of hitting XCOM size limits. 'False' means push only the last line
        of the logs to XCOM. However, the logs are pushed into XCOM under "logs", not return_value
        to avoid breaking the existing behaviour.
    :param os_type: The operating system type required by the containers
        in the container group. Possible values include: 'Windows', 'Linux'
    :param restart_policy: Restart policy for all containers within the container group.
        Possible values include: 'Always', 'OnFailure', 'Never'
    :param ip_address: The IP address type of the container group.
    :param subnet_ids: The subnet resource IDs for a container group
    :param dns_config: The DNS configuration for a container group.
    :param diagnostics: Container group diagnostic information (Log Analytics).
    :param priority: Container group priority, Possible values include: 'Regular', 'Spot'
    :param identity: List of User/System assigned identities for the container group.

    **Example**::

        AzureContainerInstancesOperator(
            ci_conn_id="azure_service_principal",
            registry_conn_id="azure_registry_user",
            resource_group="my-resource-group",
            name="my-container-name-{{ ds }}",
            image="myprivateregistry.azurecr.io/my_container:latest",
            region="westeurope",
            environment_variables={
                "MODEL_PATH": "my_value",
                "POSTGRES_LOGIN": "{{ macros.connection('postgres_default').login }}",
                "POSTGRES_PASSWORD": "{{ macros.connection('postgres_default').password }}",
                "JOB_GUID": "{{ ti.xcom_pull(task_ids='task1', key='guid') }}",
            },
            secured_variables=["POSTGRES_PASSWORD"],
            volumes=[
                (
                    "azure_container_instance_conn_id",
                    "my_storage_container",
                    "my_fileshare",
                    "/input-data",
                    True,
                ),
            ],
            memory_in_gb=14.0,
            cpu=4.0,
            gpu=GpuResource(count=1, sku="K80"),
            subnet_ids=[
                {
                    "id": "/subscriptions/00000000-0000-0000-0000-00000000000/resourceGroups/my_rg/providers/Microsoft.Network/virtualNetworks/my_vnet/subnets/my_subnet"
                }
            ],
            dns_config={"name_servers": ["10.0.0.10", "10.0.0.11"]},
            diagnostics={
                "log_analytics": {
                    "workspaceId": "workspaceid",
                    "workspaceKey": "workspaceKey",
                }
            },
            priority="Regular",
            identity = {
                "type": "UserAssigned" | "SystemAssigned" | "SystemAssigned,UserAssigned",
                "resource_ids": [
                  "/subscriptions/<sub>/resourceGroups/<rg>/providers/Microsoft.ManagedIdentity/userAssignedIdentities/<id>"
                ]
                "user_assigned_identities": {
                  "/subscriptions/.../userAssignedIdentities/<id>": {}
                }
            }
            command=["/bin/echo", "world"],
            task_id="start_container",
        )
    """

    template_fields: Sequence[str] = ("name", "image", "command", "environment_variables", "volumes")
    template_fields_renderers = {"command": "bash", "environment_variables": "json"}

    def __init__(
        self,
        *,
        ci_conn_id: str,
        resource_group: str,
        name: str,
        image: str,
        region: str,
        registry_conn_id: str | None = None,
        environment_variables: dict | None = None,
        secured_variables: str | None = None,
        volumes: list | None = None,
        memory_in_gb: Any | None = None,
        cpu: Any | None = None,
        gpu: Any | None = None,
        command: list[str] | None = None,
        remove_on_error: bool = True,
        fail_if_exists: bool = True,
        tags: dict[str, str] | None = None,
        xcom_all: bool | None = None,
        os_type: str = "Linux",
        restart_policy: str = "Never",
        ip_address: IpAddress | None = None,
        ports: list[ContainerPort] | None = None,
        subnet_ids: list[ContainerGroupSubnetId] | None = None,
        dns_config: DnsConfiguration | None = None,
        diagnostics: ContainerGroupDiagnostics | None = None,
        priority: str | None = "Regular",
        identity: ContainerGroupIdentity | dict | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.ci_conn_id = ci_conn_id
        self.resource_group = resource_group
        self.name = name
        self.image = image
        self.region = region
        self.registry_conn_id = registry_conn_id
        self.environment_variables = environment_variables or DEFAULT_ENVIRONMENT_VARIABLES
        self.secured_variables = secured_variables or DEFAULT_SECURED_VARIABLES
        self.volumes = volumes or DEFAULT_VOLUMES
        self.memory_in_gb = memory_in_gb or DEFAULT_MEMORY_IN_GB
        self.cpu = cpu or DEFAULT_CPU
        self.gpu = gpu
        self.command = command
        self.remove_on_error = remove_on_error
        self.fail_if_exists = fail_if_exists
        self._ci_hook: Any = None
        self.tags = tags
        self.xcom_all = xcom_all
        self.os_type = os_type
        if self.os_type not in ["Linux", "Windows"]:
            raise AirflowException(
                "Invalid value for the os_type argument. "
                "Please set 'Linux' or 'Windows' as the os_type. "
                f"Found `{self.os_type}`."
            )
        self.restart_policy = restart_policy
        if self.restart_policy not in ["Always", "OnFailure", "Never"]:
            raise AirflowException(
                "Invalid value for the restart_policy argument. "
                "Please set one of 'Always', 'OnFailure','Never' as the restart_policy. "
                f"Found `{self.restart_policy}`"
            )
        self.ip_address = ip_address
        self.ports = ports
        self.subnet_ids = subnet_ids
        self.dns_config = dns_config
        self.diagnostics = diagnostics
        self.priority = priority
        self.identity = self._ensure_identity(identity)
        if self.priority not in ["Regular", "Spot"]:
            raise AirflowException(
                "Invalid value for the priority argument. "
                "Please set 'Regular' or 'Spot' as the priority. "
                f"Found `{self.priority}`."
            )

    # helper to accept dict (user-friendly) or ContainerGroupIdentity (SDK object)
    @staticmethod
    def _ensure_identity(identity: ContainerGroupIdentity | dict | None) -> ContainerGroupIdentity | None:
        """
        Normalize identity input into a ContainerGroupIdentity instance.

        Accepts:
         - None -> returns None
         - ContainerGroupIdentity -> returned as-is
         - dict -> converted to ContainerGroupIdentity
         - any other object -> returned as-is (pass-through) to preserve backwards compatibility

        Expected dict shapes:
          {"type": "UserAssigned", "resource_ids": ["/.../userAssignedIdentities/id1", ...]}
        or
          {"type": "SystemAssigned"}
        or
          {"type": "SystemAssigned,UserAssigned", "resource_ids": [...]}
        """
        if identity is None:
            return None

        if isinstance(identity, ContainerGroupIdentity):
            return identity

        if isinstance(identity, dict):
            # require type
            id_type = identity.get("type")
            if not id_type:
                raise AirflowException(
                    "identity dict must include 'type' key with value 'UserAssigned' or 'SystemAssigned'"
                )

            # map common string type names to ResourceIdentityType enum values if available
            type_map = {
                "SystemAssigned": ResourceIdentityType.system_assigned,
                "UserAssigned": ResourceIdentityType.user_assigned,
                "SystemAssigned,UserAssigned": ResourceIdentityType.system_assigned_user_assigned,
                "SystemAssigned, UserAssigned": ResourceIdentityType.system_assigned_user_assigned,
            }
            cg_type = type_map.get(id_type, id_type)

            # build user_assigned_identities mapping if resource_ids provided
            resource_ids = identity.get("resource_ids")
            if resource_ids:
                if not isinstance(resource_ids, (list, tuple)):
                    raise AirflowException("identity['resource_ids'] must be a list of resource id strings")
                user_assigned_identities: dict[str, Any] = {rid: {} for rid in resource_ids}
            else:
                # accept a pre-built mapping if given
                user_assigned_identities = identity.get("user_assigned_identities") or {}

            return ContainerGroupIdentity(
                type=cg_type,
                user_assigned_identities=cast(
                    "dict[str, UserAssignedIdentities] | None", user_assigned_identities
                ),
            )
        return identity

    def execute(self, context: Context) -> int:
        # Check name again in case it was templated.
        self._check_name(self.name)

        self._ci_hook = AzureContainerInstanceHook(azure_conn_id=self.ci_conn_id)

        if self.fail_if_exists:
            self.log.info("Testing if container group already exists")
            if self._ci_hook.exists(self.resource_group, self.name):
                raise AirflowException("Container group exists")

        if self.registry_conn_id:
            registry_hook = AzureContainerRegistryHook(self.registry_conn_id)
            image_registry_credentials: list | None = [
                registry_hook.connection,
            ]
        else:
            image_registry_credentials = None

        environment_variables = []
        for key, value in self.environment_variables.items():
            if key in self.secured_variables:
                e = EnvironmentVariable(name=key, secure_value=value)
            else:
                e = EnvironmentVariable(name=key, value=value)
            environment_variables.append(e)

        volumes: list[_AzureVolume] = []
        volume_mounts: list[VolumeMount | VolumeMount] = []
        for conn_id, account_name, share_name, mount_path, read_only in self.volumes:
            hook = AzureContainerVolumeHook(conn_id)

            mount_name = f"mount-{len(volumes)}"
            volumes.append(hook.get_file_volume(mount_name, share_name, account_name, read_only))
            volume_mounts.append(VolumeMount(name=mount_name, mount_path=mount_path, read_only=read_only))

        exit_code = 1
        try:
            self.log.info("Starting container group with %.1f cpu %.1f mem", self.cpu, self.memory_in_gb)
            if self.gpu:
                self.log.info("GPU count: %.1f, GPU SKU: %s", self.gpu.count, self.gpu.sku)

            resources = ResourceRequirements(
                requests=ResourceRequests(memory_in_gb=self.memory_in_gb, cpu=self.cpu, gpu=self.gpu)
            )

            if self.ip_address and not self.ports:
                self.ports = [ContainerPort(port=80)]
                self.log.info("Default port set. Container will listen on port 80")

            container = Container(
                name=self.name,
                image=self.image,
                resources=resources,
                command=self.command,
                environment_variables=environment_variables,
                volume_mounts=volume_mounts,
                ports=self.ports,
            )

            container_group = ContainerGroup(
                location=self.region,
                containers=[
                    container,
                ],
                image_registry_credentials=image_registry_credentials,
                volumes=volumes,
                restart_policy=self.restart_policy,
                os_type=self.os_type,
                tags=self.tags,
                ip_address=self.ip_address,
                subnet_ids=self.subnet_ids,
                dns_config=self.dns_config,
                diagnostics=self.diagnostics,
                priority=self.priority,
                identity=self.identity,
            )

            self._ci_hook.create_or_update(self.resource_group, self.name, container_group)

            self.log.info("Container group started %s/%s", self.resource_group, self.name)

            exit_code = self._monitor_logging(self.resource_group, self.name)
            if self.xcom_all is not None:
                logs = self._ci_hook.get_logs(self.resource_group, self.name)
                if logs is None:
                    context["ti"].xcom_push(key="logs", value=[])
                else:
                    if self.xcom_all:
                        context["ti"].xcom_push(key="logs", value=logs)
                    else:
                        # slice off the last entry in the list logs and return it as a list
                        context["ti"].xcom_push(key="logs", value=logs[-1:])

            self.log.info("Container had exit code: %s", exit_code)
            if exit_code != 0:
                raise AirflowException(f"Container had a non-zero exit code, {exit_code}")
            return exit_code

        except CloudError:
            self.log.exception("Could not start container group")
            raise AirflowException("Could not start container group")

        finally:
            if exit_code == 0 or self.remove_on_error:
                self.on_kill()

    def on_kill(self) -> None:
        self.log.info("Deleting container group")
        try:
            self._ci_hook.delete(self.resource_group, self.name)
        except Exception:
            self.log.exception("Could not delete container group")

    def _monitor_logging(self, resource_group: str, name: str) -> int:
        last_state = None
        last_message_logged = None
        last_line_logged = None

        while True:
            try:
                cg_state = self._ci_hook.get_state(resource_group, name)
                instance_view = cg_state.containers[0].instance_view
                # If there is no instance view, we show the provisioning state
                if instance_view is not None:
                    c_state = instance_view.current_state
                    state, exit_code, detail_status = (
                        c_state.state,
                        c_state.exit_code,
                        c_state.detail_status,
                    )
                else:
                    state = cg_state.provisioning_state
                    exit_code = 0
                    detail_status = "Provisioning"

                if instance_view is not None and instance_view.events is not None:
                    messages = [event.message for event in instance_view.events]
                    last_message_logged = self._log_last(messages, last_message_logged)

                if state != last_state:
                    self.log.info("Container group state changed to %s", state)
                    last_state = state

                if state in ["Running", "Terminated", "Succeeded"]:
                    try:
                        logs = self._ci_hook.get_logs(resource_group, name)
                        if logs and logs[0] is None:
                            self.log.error("Container log is broken, marking as failed.")
                            return 1
                        last_line_logged = self._log_last(logs, last_line_logged)
                    except CloudError:
                        self.log.exception(
                            "Exception while getting logs from container instance, retrying..."
                        )

                if state == "Terminated":
                    self.log.info("Container exited with detail_status %s", detail_status)
                    return exit_code

                if state == "Unhealthy":
                    self.log.error("Azure provision unhealthy")
                    return 1

                if state == "Failed":
                    self.log.error("Azure provision failure")
                    return 1

            except AirflowTaskTimeout:
                raise
            except CloudError as err:
                if "ResourceNotFound" in str(err):
                    self.log.warning(
                        "ResourceNotFound, container is probably removed "
                        "by another process "
                        "(make sure that the name is unique)."
                    )
                    return 1
                self.log.exception("Exception while getting container groups")
            except Exception:
                self.log.exception("Exception while getting container groups")

            time.sleep(1)

    def _log_last(self, logs: list | None, last_line_logged: Any) -> Any | None:
        if logs:
            # determine the last line which was logged before
            last_line_index = 0
            for i in range(len(logs) - 1, -1, -1):
                if logs[i] == last_line_logged:
                    # this line is the same, hence print from i+1
                    last_line_index = i + 1
                    break

            # log all new ones
            for line in logs[last_line_index:]:
                self.log.info(line.rstrip())

            return logs[-1]
        return None

    @staticmethod
    def _check_name(name: str) -> str:
        regex_check = re.match("[a-z0-9]([-a-z0-9]*[a-z0-9])?", name)
        if regex_check is None or regex_check.group() != name:
            raise AirflowException('ACI name must match regex [a-z0-9]([-a-z0-9]*[a-z0-9])? (like "my-name")')
        if len(name) > 63:
            raise AirflowException("ACI name cannot be longer than 63 characters")
        return name
