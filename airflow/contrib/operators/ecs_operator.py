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
import sys

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from airflow.contrib.hooks.aws_hook import AwsHook


class ECSOperator(BaseOperator):
    """
    Execute a task on AWS EC2 Container Service

    :param task_definition: the task definition name on EC2 Container Service
    :type task_definition: str
    :param cluster: the cluster name on EC2 Container Service
    :type cluster: str
    :param overrides: the same parameter that boto3 will receive (templated):
        http://boto3.readthedocs.org/en/latest/reference/services/ecs.html#ECS.Client.run_task
    :type overrides: dict
    :param aws_conn_id: connection id of AWS credentials / region name. If None,
        credential boto3 strategy will be used
        (http://boto3.readthedocs.io/en/latest/guide/configuration.html).
    :type aws_conn_id: str
    :param region_name: region name to use in AWS Hook.
        Override the region_name in connection (if provided)
    :type region_name: str
    :param launch_type: the launch type on which to run your task ('EC2' or 'FARGATE')
    :type launch_type: str
    :param group: the name of the task group associated with the task
    :type group: str
    :param placement_constraints: an array of placement constraint objects to use for
        the task
    :type placement_constraints: list
    :param platform_version: the platform version on which your task is running
    :type platform_version: str
    :param network_configuration: the network configuration for the task
    :type network_configuration: dict
    """

    ui_color = '#f0ede4'
    client = None
    arn = None
    template_fields = ('overrides',)

    @apply_defaults
    def __init__(self, task_definition, cluster, overrides,
                 aws_conn_id=None, region_name=None, launch_type='EC2',
                 group=None, placement_constraints=None, platform_version='LATEST',
                 network_configuration=None, **kwargs):
        super(ECSOperator, self).__init__(**kwargs)

        self.aws_conn_id = aws_conn_id
        self.region_name = region_name
        self.task_definition = task_definition
        self.cluster = cluster
        self.overrides = overrides
        self.launch_type = launch_type
        self.group = group
        self.placement_constraints = placement_constraints
        self.platform_version = platform_version
        self.network_configuration = network_configuration

        self.hook = self.get_hook()

    def execute(self, context):
        self.log.info(
            'Running ECS Task - Task definition: %s - on cluster %s',
            self.task_definition, self.cluster
        )
        self.log.info('ECSOperator overrides: %s', self.overrides)

        self.client = self.hook.get_client_type(
            'ecs',
            region_name=self.region_name
        )

        run_opts = {
            'cluster': self.cluster,
            'taskDefinition': self.task_definition,
            'overrides': self.overrides,
            'startedBy': self.owner,
            'launchType': self.launch_type,
            'platformVersion': self.platform_version,
        }
        if self.group is not None:
            run_opts['group'] = self.group
        if self.placement_constraints is not None:
            run_opts['placementConstraints'] = self.placement_constraints
        if self.network_configuration is not None:
            run_opts['networkConfiguration'] = self.network_configuration
        response = self.client.run_task(**run_opts)

        failures = response['failures']
        if len(failures) > 0:
            raise AirflowException(response)
        self.log.info('ECS Task started: %s', response)

        self.arn = response['tasks'][0]['taskArn']
        self._wait_for_task_ended()

        self._check_success_task()
        self.log.info('ECS Task has been successfully executed: %s', response)

    def _wait_for_task_ended(self):
        waiter = self.client.get_waiter('tasks_stopped')
        waiter.config.max_attempts = sys.maxsize  # timeout is managed by airflow
        waiter.wait(
            cluster=self.cluster,
            tasks=[self.arn]
        )

    def _check_success_task(self):
        response = self.client.describe_tasks(
            cluster=self.cluster,
            tasks=[self.arn]
        )
        self.log.info('ECS Task stopped, check status: %s', response)

        if len(response.get('failures', [])) > 0:
            raise AirflowException(response)

        for task in response['tasks']:
            containers = task['containers']
            for container in containers:
                if container.get('lastStatus') == 'STOPPED' and \
                        container['exitCode'] != 0:
                    raise AirflowException(
                        'This task is not in success state {}'.format(task))
                elif container.get('lastStatus') == 'PENDING':
                    raise AirflowException('This task is still pending {}'.format(task))
                elif 'error' in container.get('reason', '').lower():
                    raise AirflowException(
                        'This containers encounter an error during launching : {}'.
                        format(container.get('reason', '').lower()))

    def get_hook(self):
        return AwsHook(
            aws_conn_id=self.aws_conn_id
        )

    def on_kill(self):
        response = self.client.stop_task(
            cluster=self.cluster,
            task=self.arn,
            reason='Task killed by the user')
        self.log.info(response)
