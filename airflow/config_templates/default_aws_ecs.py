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
"""
Default AWS ECS configuration

This is the default configuration for calling the ECS `run_task` function.
The AWS ECS Executor calls Boto3's run_task(**kwargs) function with the kwargs templated by this
dictionary. See the URL below for documentation on the parameters accepted by the Boto3 run_task function.

In other words, if you don't like the way Airflow calls the Boto3 RunTask API, then call it yourself by
overriding the airflow config file.

.. seealso::
https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ecs.html#ECS.Client.run_task
:return: Dictionary kwargs to be used by ECS run_task() function.
"""
from airflow.configuration import conf

DEFAULT_AWS_ECS_CONFIG = {
    'cluster': conf.get('aws_ecs', 'cluster'),
    'taskDefinition': conf.get('aws_ecs', 'task_definition'),
    'platformVersion': 'LATEST',

    'overrides': {
        'containerOverrides': [{
            'name': conf.get('aws_ecs', 'container_name'),
            # The executor will overwrite the 'command' property during execution.
            # Must always be the first container!
            'command': []
        }]
    },
    'count': 1
}

if conf.has_option('aws_ecs', 'launch_type'):
    DEFAULT_AWS_ECS_CONFIG['launchType'] = conf.get('aws_ecs', 'launch_type')

# Only build this section if 'subnets', 'security_groups', and 'assign_public_ip' are populated
if (conf.has_option('aws_ecs', 'subnets') and conf.has_option('aws_ecs', 'security_groups') and
        conf.has_option('aws_ecs', 'assign_public_ip')):
    DEFAULT_AWS_ECS_CONFIG['networkConfiguration'] = {
        'subnets': conf.get('aws_ecs', 'subnets').split(','),
        'securityGroups': conf.get('aws_ecs', 'security_groups').split(','),
        'assignPublicIp': conf.get('aws_ecs', 'assign_public_ip')
    }
