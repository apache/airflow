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

from time import sleep

from airflow.contrib.hooks.azure_batchai_hook import (AzureBatchAIHook)

from airflow.exceptions import AirflowException, AirflowTaskTimeout
from airflow.models import BaseOperator

from azure.mgmt.batchai.models import (ClusterCreateParameters,
                                       UserAccountSettings)

from msrestazure.azure_exceptions import CloudError


class AzureBatchAIOperator(BaseOperator):
    """
    Start a cluster on Azure Batch AI
    :param bai_conn_id: connection id of a service principal which will be used
        to start the batch ai cluster
    :type bai_conn_id: str
    :param resource_group: name of the resource group wherein this cluster
        should be started
    :type resource_group: str
    :param workspace_name: name of the workspace wherein this cluster
        should be started
    :type workspace_name: str
    :param cluster_name: name of the batch ai cluster
    :type cluster_name: str
    :param location: the location wherein this cluster should be started
    :type location: str
    :param scale_type: either "manual" or "auto" based on desired scale settings
    :type scale_type: str
    :param: environment_variables: key,value pairs containing environment variables
        which will be passed to the running cluster, must include username and password
    :type: environment_variables: dict
    :param: volumes: list of volumes to be mounted to the cluster.
        Currently only Azure Fileshares are supported.
    :type: volumes: list[<conn_id, account_name, share_name, mount_path, read_only>]
    :param: publisher: publisher of the image to be used in the cluster
    :type: publisher: str
    :param: offer: offer of the image to be used in the cluster
    :type: offer: str
    :param: sku: sku of the image to be used in the cluster
    :type: sku: str
    :param: version: publisher of the image to be used in the cluster
    :type: version: str
     :Example:
     >>>  a = AzureBatchAIOperator(
                bai_conn_id='azure_service_principal',
                resource_group='my-resource-group',
                workspace_name='my-workspace-name-{{ ds }}',
                cluster_name='my-cluster-name',
                location='westeurope',
                scale_type'auto_scale',
                environment_variables={'USERNAME': '{{ ds }}',
                 'PASSWORD': '{{ ds }}},
                volumes=[],
            )
    """

    template_fields = ('name', 'environment_variables')
    template_ext = tuple()

    def __init__(self, bai_conn_id, resource_group, workspace_name, cluster_name, location, scale_type,
                 environment_variables={}, volumes=[], publisher='Canonical', offer='UbuntuServer',
                 sku='16.04-LTS', version='latest', *args, **kwargs):
        self.bai_conn_id = bai_conn_id
        self.resource_group = resource_group
        self.workspace_name = workspace_name
        self.cluster_name = cluster_name
        self.location = location
        self.scale_type = scale_type
        self.environment_variables = environment_variables
        self.volumes = volumes
        self.publisher = publisher
        self.offer = offer
        self.sku = sku
        self.version = version
        super(AzureBatchAIOperator, self).__init__(*args, **kwargs)

    def execute(self):
        batch_ai_hook = AzureBatchAIHook(self.bai_conn_id)

        try:
            self.log.info("Starting Batch AI cluster with offer %s and sku %s",
                          self.offer, self.sku)

            username = self.environment_variables['USERNAME']
            password = self.environment_variables['PASSWORD']

            user_account_settings = UserAccountSettings(
                admin_user_name=username,
                admin_user_password=password)

            parameters = ClusterCreateParameters(
                vm_size='STANDARD_NC6',
                user_account_settings=user_account_settings,
                location=self.location,
                vm_priority='dedicated',
                scale_settings=None,
                virtual_machine_configuration=None,
                node_setup=None,
                subnet=None)

            batch_ai_hook.create(self.resource_group,
                                 self.workspace_name,
                                 self.cluster_name,
                                 self.location, parameters)

            self.log.info("Cluster started")

            exit_code = self._monitor_logging(batch_ai_hook, self.resource_group, self.workspace_name)
            self.log.info("Container had exit code: %s", exit_code)

            if exit_code and exit_code != 0:
                raise AirflowException("Container had a non-zero exit code, %s"
                                       % exit_code)
        except CloudError as e:
            self.log.exception("Could not start batch ai cluster, %s", str(e))
            raise AirflowException("Could not start batch ai cluster")

        finally:
            self.log.info("Deleting Batch AI cluster")
            try:
                batch_ai_hook.delete(self.resource_group, self.workspace_name, self.cluster_name)
            except Exception:
                self.log.exception("Could not delete batch ai cluster")

    def _monitor_logging(self, batch_ai_hook, resource_group, name):
        last_state = None
        last_message_logged = None
        last_line_logged = None
        for _ in range(43200):
            # roughly 12 hours
            try:
                state, exit_code = batch_ai_hook.get_state_exitcode(self.resource_group,
                                                                    self.workspace_name,
                                                                    self.cluster_name)

                if state != last_state:
                    self.log.info("Cluster state changed to %s", state)
                    last_state = state

                if state == "Terminated":
                    return exit_code
                messages = batch_ai_hook.get_messages(self.resource_group,
                                                      self.workspace_name,
                                                      self.cluster_name)
                last_message_logged = self._log_last(messages, last_message_logged)

                if state == "Running":
                    try:
                        logs = batch_ai_hook.get_messages(self.resource_group,
                                                          self.workspace_name,
                                                          self.cluster_name)
                        last_line_logged = self._log_last(logs, last_line_logged)
                    except CloudError as err:
                        self.log.exception("Exception (%s) while getting logs from cluster, "
                                           "retrying...", str(err))

            except CloudError as err:
                if 'ResourceNotFound' in str(err):
                    self.log.warning("ResourceNotFound, cluster is probably removed by another process "
                                     "(make sure that the name is unique). Error: %s", str(err))
                    return 1
                else:
                    self.log.exception("Exception while getting cluster")

            except Exception as e:
                self.log.exception("Exception while getting cluster: %s", str(e))
            sleep(1)
        raise AirflowTaskTimeout("Did not complete on time")
