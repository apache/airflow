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

import unittest

import mock
from parameterized import parameterized
from spython.instance import Instance

from airflow.exceptions import AirflowException
from airflow.providers.singularity.operators.singularity import SingularityOperator


class SingularityOperatorTestCase(unittest.TestCase):
    @mock.patch('airflow.providers.singularity.operators.singularity.Client')
    def test_execute(self, client_mock):
        instance = mock.Mock(
            autospec=Instance,
            **{
                'start.return_value': 0,
                'stop.return_value': 0,
            },
        )

        client_mock.instance.return_value = instance
        client_mock.execute.return_value = {'return_code': 0, 'message': 'message'}

        task = SingularityOperator(task_id='task-id', image="docker://busybox", command="echo hello")
        task.execute({})

        client_mock.instance.assert_called_once_with("docker://busybox", options=[], args=None, start=False)

        client_mock.execute.assert_called_once_with(mock.ANY, "echo hello", return_result=True)

        execute_args, _ = client_mock.execute.call_args
        self.assertIs(execute_args[0], instance)

        instance.start.assert_called_once_with()
        instance.stop.assert_called_once_with()

    @parameterized.expand(
        [
            ("",),
            (None,),
        ]
    )
    def test_command_is_required(self, command):
        task = SingularityOperator(task_id='task-id', image="docker://busybox", command=command)
        with self.assertRaisesRegex(AirflowException, "You must define a command."):
            task.execute({})

    @mock.patch('airflow.providers.singularity.operators.singularity.Client')
    def test_image_should_be_pulled_when_not_exists(self, client_mock):
        instance = mock.Mock(
            autospec=Instance,
            **{
                'start.return_value': 0,
                'stop.return_value': 0,
            },
        )

        client_mock.pull.return_value = '/tmp/busybox_latest.sif'
        client_mock.instance.return_value = instance
        client_mock.execute.return_value = {'return_code': 0, 'message': 'message'}

        task = SingularityOperator(
            task_id='task-id',
            image="docker://busybox",
            command="echo hello",
            pull_folder="/tmp",
            force_pull=True,
        )
        task.execute({})

        client_mock.instance.assert_called_once_with(
            "/tmp/busybox_latest.sif", options=[], args=None, start=False
        )
        client_mock.pull.assert_called_once_with("docker://busybox", stream=True, pull_folder="/tmp")
        client_mock.execute.assert_called_once_with(mock.ANY, "echo hello", return_result=True)

    @parameterized.expand(
        [
            (
                None,
                [],
            ),
            (
                [],
                [],
            ),
            (
                ["AAA"],
                ['--bind', 'AAA'],
            ),
            (
                ["AAA", "BBB"],
                ['--bind', 'AAA', '--bind', 'BBB'],
            ),
            (
                ["AAA", "BBB", "CCC"],
                ['--bind', 'AAA', '--bind', 'BBB', '--bind', 'CCC'],
            ),
        ]
    )
    @mock.patch('airflow.providers.singularity.operators.singularity.Client')
    def test_bind_options(self, volumes, expected_options, client_mock):
        instance = mock.Mock(
            autospec=Instance,
            **{
                'start.return_value': 0,
                'stop.return_value': 0,
            },
        )
        client_mock.pull.return_value = 'docker://busybox'
        client_mock.instance.return_value = instance
        client_mock.execute.return_value = {'return_code': 0, 'message': 'message'}

        task = SingularityOperator(
            task_id='task-id',
            image="docker://busybox",
            command="echo hello",
            force_pull=True,
            volumes=volumes,
        )
        task.execute({})

        client_mock.instance.assert_called_once_with(
            "docker://busybox", options=expected_options, args=None, start=False
        )

    @parameterized.expand(
        [
            (
                None,
                [],
            ),
            (
                "",
                ['--workdir', ''],
            ),
            (
                "/work-dir/",
                ['--workdir', '/work-dir/'],
            ),
        ]
    )
    @mock.patch('airflow.providers.singularity.operators.singularity.Client')
    def test_working_dir(self, working_dir, expected_working_dir, client_mock):
        instance = mock.Mock(
            autospec=Instance,
            **{
                'start.return_value': 0,
                'stop.return_value': 0,
            },
        )
        client_mock.pull.return_value = 'docker://busybox'
        client_mock.instance.return_value = instance
        client_mock.execute.return_value = {'return_code': 0, 'message': 'message'}

        task = SingularityOperator(
            task_id='task-id',
            image="docker://busybox",
            command="echo hello",
            force_pull=True,
            working_dir=working_dir,
        )
        task.execute({})

        client_mock.instance.assert_called_once_with(
            "docker://busybox", options=expected_working_dir, args=None, start=False
        )
