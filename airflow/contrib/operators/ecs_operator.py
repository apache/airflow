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
"""This module is deprecated. Please use `airflow.providers.amazon.aws.operators.ecs`."""

import warnings

# pylint: disable=unused-import
from airflow.providers.amazon.aws.operators.ecs import ECSOperator, ECSProtocol as NewECSProtocol  # noqa
from airflow.typing_compat import Protocol, runtime_checkable

<<<<<<< HEAD
_log = logging.getLogger(__name__)

=======
warnings.warn(
    "This module is deprecated. Please use `airflow.providers.amazon.aws.operators.ecs`.",
    DeprecationWarning, stacklevel=2
)
>>>>>>> 0d5ecde61bc080d2c53c9021af252973b497fb7d


@runtime_checkable
class ECSProtocol(NewECSProtocol, Protocol):
    """
    This class is deprecated. Please use `airflow.providers.amazon.aws.operators.ecs.ECSProtocol`.
    """

<<<<<<< HEAD
    ui_color = '#f0ede4'
    client = None
    arn = None
    template_fields = ('overrides',)

    @apply_defaults
    def __init__(self, task_definition, cluster, overrides,
                 aws_conn_id=None, region_name=None, **kwargs):
        super(ECSOperator, self).__init__(**kwargs)

        self.aws_conn_id = aws_conn_id
        self.region_name = region_name
        self.task_definition = task_definition
        self.cluster = cluster
        self.overrides = overrides

        self.hook = self.get_hook()

    def execute(self, context):

        _log.info('Running ECS Task - Task definition: {} - on cluster {}'.format(
            self.task_definition,
            self.cluster
        ))
        _log.info('ECSOperator overrides: {}'.format(self.overrides))

        self.client = self.hook.get_client_type(
            'ecs',
            region_name=self.region_name
        )

        response = self.client.run_task(
            cluster=self.cluster,
            taskDefinition=self.task_definition,
            overrides=self.overrides,
            startedBy=self.owner
        )

        failures = response['failures']
        if (len(failures) > 0):
            raise AirflowException(response)
        _log.info('ECS Task started: {}'.format(response))

        self.arn = response['tasks'][0]['taskArn']
        self._wait_for_task_ended()

        self._check_success_task()
        _log.info('ECS Task has been successfully executed: {}'.format(response))

    def _wait_for_task_ended(self):
        waiter = self.client.get_waiter('tasks_stopped')
        waiter.config.max_attempts = sys.maxint  # timeout is managed by airflow
        waiter.wait(
            cluster=self.cluster,
            tasks=[self.arn]
        )

    def _check_success_task(self):
        response = self.client.describe_tasks(
            cluster=self.cluster,
            tasks=[self.arn]
        )
        _log.info('ECS Task stopped, check status: {}'.format(response))

        if (len(response.get('failures', [])) > 0):
            raise AirflowException(response)
=======
    # A Protocol cannot be instantiated
>>>>>>> 0d5ecde61bc080d2c53c9021af252973b497fb7d

    def __new__(cls, *args, **kwargs):
        warnings.warn(
            """This class is deprecated.
            Please use `airflow.providers.amazon.aws.operators.ecs.ECSProtocol`.""",
            DeprecationWarning,
            stacklevel=2,
        )
<<<<<<< HEAD

    def on_kill(self):
        response = self.client.stop_task(
            cluster=self.cluster,
            task=self.arn,
            reason='Task killed by the user')
        _log.info(response)
=======
>>>>>>> 0d5ecde61bc080d2c53c9021af252973b497fb7d
