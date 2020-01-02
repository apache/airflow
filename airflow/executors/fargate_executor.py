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
"""AWS Fargate Executor."""

import time
from typing import Dict, List, Optional, Callable

from .base_executor import BaseExecutor, TaskInstanceKeyType, CommandType
from airflow.utils.module_loading import import_string
from airflow.utils.state import State

from airflow import configuration
import boto3  # Additional Requirement from base-airflow
from boto3.exceptions import Boto3Error


ExecutorConfigFunctionType = Callable[[CommandType], dict]


class FargateExecutor(BaseExecutor):
    """
    The Airflow Scheduler create a shell command, and passes it to the executor. This Fargate Executor simply runs
    said airflow command on a remote AWS Fargate Cluster with an task-definition configured with the same containers as
    the Scheduler. It then periodically checks in with the launched tasks (via task-arns) to determine the status.

    Prerequisite: proper configuration of boto3 library
    See https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html for credential management
    """

    # Number of retries in the scenario where the API cannot find a task key. We do this because sometimes AWS falsely
    # returns an error on a newly instantiated task that exists.
    MAX_FAILURE_CHECKS = 3

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # This is the function used to generate boto3's execute-task api calls.
        if configuration.conf.has_option('fargate', 'execution_config_function'):
            self.executor_config_function: ExecutorConfigFunctionType = import_string(
                configuration.conf.get('fargate', 'execution_config_function')
            )()
        else:
            self.executor_config_function: ExecutorConfigFunctionType = default_task_id_to_fargate_options_function()
        self.region = configuration.conf.get('fargate', 'region')
        self.cluster = configuration.conf.get('fargate', 'cluster')
        self.active_workers: Optional[FargateTaskCollection] = None
        self.ecs = None

    def start(self):
        self.active_workers = FargateTaskCollection()
        # NOTE: See https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html for authentication
        # and access-key management. You can store an environmental variable, setup aws config from console, or use IAM
        # roles.
        self.ecs = boto3.client('ecs', region_name=self.region)

    def sync(self):
        # Check on all active workers
        all_task_arns = self.active_workers.get_all_arns()
        if not all_task_arns:
            self.log.debug("No active tasks, skipping sync")
            return
        self.log.debug("Active Workers: {}".format(all_task_arns))
        describe_dict = self.ecs.describe_tasks(tasks=all_task_arns, cluster=self.cluster)
        self.log.debug(f'ECS Response: {describe_dict}')
        # check & handle the failures first in the JSON response.
        if describe_dict['failures']:
            self._sync_api_failures(describe_dict['failures'])
        # check & handle airflow sucesses & failures next
        updated_tasks = describe_dict['tasks']
        for task_json in updated_tasks:
            task = FargateTask(task_json)
            # Update the active workers with new statuses. Consequtive task failure counter resets back to 0.
            self.active_workers.update_task(task)
            # get state of current task
            task_state = task.get_task_state()
            task_key = self.active_workers.arn_to_key[task.task_arn]
            # mark finished tasks as either a success/failure
            if task_state == State.FAILED:
                self.fail(task_key)
            elif task_state == State.SUCCESS:
                self.success(task_key)
            # if task is no longer running then remove from active workers and place in finished
            if task_state != State.RUNNING:
                self.log.debug(f'Task {task_key} marked as {task_state} after running on {task.task_arn}')
                self.active_workers.pop_by_key(task_key)

    def _sync_api_failures(self, arn_failures):
        for failure in arn_failures:
            arn = failure["arn"]
            task = self.active_workers.task_by_arn(arn)
            task.api_failure_count += 1
            # Sometimes ECS doesn't update the ARN key right away, and will show up as missing. Check later.
            if task.api_failure_count >= self.__class__.MAX_FAILURE_CHECKS:
                task_key = self.active_workers.arn_to_key[arn]
                self.active_workers.pop_by_key(task_key)
                self.log.error(f'ECS Executor could not find task {arn} because {failure["reason"]}; '
                               f'marking key {task_key} as failed')
                self.fail(task_key)
            else:
                self.log.debug(f'ECS Executor could not find task {arn} because {failure["reason"]}; '
                               f'skipping for now on strike #{task.api_failure_count}')

    def execute_async(self, key: TaskInstanceKeyType, command: CommandType,
                      queue=None, executor_config=None):
        """
        This method will execute the command asynchronously.
        """
        # run a task
        task_execution_api = self.executor_config_function(command)
        try:
            task_response = self.ecs.run_task(**task_execution_api)
            self.log.debug(f'Executor responded with "{task_response}"')
            if task_response['failures'] or not task_response['tasks']:
                raise FargateError(', '.join([str(f) for f in task_response['failures']]))
            task = FargateTask(task_response['tasks'][0])
            self.log.info(f'Executor running task "{key}" on "{task.task_arn}"')
            self.active_workers.add_task(task, key)
        except (FargateError, Boto3Error) as e:
            self.log.error(f'Executor failed to run task {key} with the following exception: {str(e)}')
            self.fail(key)

    def end(self, heartbeat_interval=10):
        """
        This method is called when the caller is done submitting job and is
        wants to wait synchronously for the job submitted previously to be
        all done.
        """
        while True:
            self.sync()
            if not self.active_workers:
                break
            time.sleep(heartbeat_interval)

    def terminate(self):
        """
        This method is called when the daemon receives a SIGTERM
        """
        self.sync()
        for worker in self.active_workers:
            self.ecs.stop_task(
                cluster=self.cluster,
                task=worker.task_arn,
                reason='Airflow Executor received a sig-term'
            )
        self.end()


def default_task_id_to_fargate_options_function():
    """
    This is a function which returns a function. The outer function takes no arguments, and returns the inner function.
    The inner function takes in an airflow CLI command an outputs a json compatible with the boto3 run_task API
    linked above. In other words, if you don't like the way I call the boto3 Fargate API then call it yourself by
    overriding the airflow config file.

    i.e: execution_config_function = pipeline_plugin.default_task_id_to_fargate_options_function
    """
    # Absolutely mandatory configurations
    region = configuration.conf.get('fargate', 'region')
    cluster = configuration.conf.get('fargate', 'cluster')

    # grab a few variables
    task_definition = configuration.conf.get('fargate', 'task_definition')
    container_name = configuration.conf.get('fargate', 'container_name')
    security_groups = configuration.conf.get('fargate', 'security_groups').split(',')

    launch_type = 'FARGATE'
    if configuration.conf.has_option('fargate', 'launch_type'):
        launch_type = configuration.conf.get('fargate', 'launch_type')

    platform_version = 'LATEST'
    if configuration.conf.has_option('fargate', 'platform_version'):
        platform_version = configuration.conf.get('fargate', 'platform_version')

    assign_public_ip = 'ENABLED'
    if configuration.conf.has_option('fargate', 'assign_public_ip'):
        assign_public_ip = configuration.conf.get('fargate', 'assign_public_ip')

    subnets = None
    if configuration.conf.has_option('fargate', 'subnets'):
        subnets = configuration.conf.get('fargate', 'subnets').split(',')

    # build the function based on the provided configurations
    return get_default_execute_config_function(region, cluster, task_definition, container_name, platform_version,
                                               launch_type, assign_public_ip, security_groups, subnets)


def get_default_execute_config_function(region: str, cluster: str, task_definition: str, container_name: str,
                                        platform_version: str, launch_type: str, assign_public_ip: str,
                                        security_groups: List[str], subnets: List[str]):
    # If no subnet list provided, get all subnets from default region
    if not subnets:
        ec2 = boto3.client('ec2', region_name=region)
        subnets = [x['SubnetId'] for x in ec2.describe_subnets()['Subnets']]

    network_configuration = {
        'awsvpcConfiguration': {
            'subnets': subnets,
            'securityGroups': security_groups,
            'assignPublicIp': assign_public_ip
        }
    }

    def with_default(airflow_task_command: CommandType) -> dict:
        if isinstance(airflow_task_command, str):
            # This is done for backwards compatibility with older versions of airflow (Airflow < 1.10.2)
            # split airflow bash command into an array to feed into the entrypoint of the docker container.
            airflow_task_command = airflow_task_command.split(',')
        execution_config = dict(
            cluster=cluster,
            taskDefinition=task_definition,
            overrides={
                'containerOverrides': [
                    {
                        'name': container_name,
                        # The docker file will receive an array of commands & run them with exec "$@"
                        'command': airflow_task_command
                    }
                ]
            },
            count=1,
            launchType=launch_type,
            platformVersion=platform_version,
            networkConfiguration=network_configuration
        )
        return execution_config

    return with_default


class FargateTask:
    """This class wraps around the boto3 JSON response of the start_task v1 API call. Mildly useful"""
    def __init__(self, json_response):
        self.json = json_response
        self.api_failure_count = 0

    @property
    def containers(self):
        return self.json['containers']

    @property
    def task_arn(self):
        return self.json['taskArn']

    def get_task_state(self) -> State:
        """Checks all containers in a given task to check task status. Each container has a last-known status,
        a health-status, and an exit-code. This method only checks exit codes and statuses.
        """
        # lastStatus can be RUNNING'|'PENDING'|'STOPPED'
        # healthStatus can be 'HEALTHY'|'UNHEALTHY'|'UNKNOWN'
        containers = self.containers
        is_finished = all([x['lastStatus'] == "STOPPED" for x in containers])
        has_exit_codes = all(['exitCode' in x for x in containers])
        if not is_finished or not has_exit_codes:
            return State.RUNNING
        is_error = any([x['exitCode'] != 0 for x in containers])
        return State.FAILED if is_error else State.SUCCESS

    def __repr__(self):
        return f'({self.task_arn}, {self.get_task_state()})'


class FargateTaskCollection:
    """
    A three-way dictionary between Airflow task ids, Fargate ARNs, and Fargate task objects
    """
    def __init__(self):
        self.key_to_arn: Dict[TaskInstanceKeyType, str] = {}
        self.arn_to_key: Dict[str, TaskInstanceKeyType] = {}
        self.tasks: Dict[str, FargateTask] = {}

    def add_task(self, task: FargateTask, airflow_task_key: TaskInstanceKeyType):
        arn = task.task_arn
        self.tasks[arn] = task
        self.key_to_arn[airflow_task_key] = arn
        self.arn_to_key[arn] = airflow_task_key

    def update_task(self, task: FargateTask):
        self.tasks[task.task_arn] = task

    def task_by_key(self, task_key: TaskInstanceKeyType) -> FargateTask:
        arn = self.key_to_arn[task_key]
        return self.task_by_arn(arn)

    def task_by_arn(self, arn) -> FargateTask:
        return self.tasks[arn]

    def pop_by_key(self, task_key: TaskInstanceKeyType) -> FargateTask:
        arn = self.key_to_arn[task_key]
        task = self.tasks[arn]
        del self.key_to_arn[task_key]
        del self.arn_to_key[arn]
        del self.tasks[arn]
        return task

    def pop_by_arn(self, arn: str) -> FargateTask:
        task = self.tasks[arn]
        task_key = self.arn_to_key[arn]
        del self.key_to_arn[task_key]
        del self.arn_to_key[arn]
        del self.tasks[arn]
        return task

    def get_all_arns(self) -> List[str]:
        return list(self.key_to_arn.values())

    def get_all_task_keys(self) -> List[TaskInstanceKeyType]:
        return list(self.key_to_arn.keys())

    def __getitem__(self, value):
        return self.task_by_arn(value)

    def __len__(self):
        return len(self.tasks)


class FargateError(Exception):
    pass
