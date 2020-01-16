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

import unittest

import mock
import six
from parameterized import parameterized
from spython.instance import Instance

from airflow import AirflowException
from airflow.contrib.operators.singularity_operator import SingularityOperator


class SingularityOperatorTestCase(unittest.TestCase):
    @mock.patch('airflow.contrib.operators.singularity_operator.Client')
    def test_execute(self, client_mock):
        instance = mock.Mock(autospec=Instance, **{
            'start.return_value': 0,
            'stop.return_value': 0,
        })

        client_mock.instance.return_value = instance
        client_mock.execute.return_value = {'return_code': 0,
                                            'message': 'message'}

        task = SingularityOperator(
            task_id='task-id',
            image="awesome-image",
            command="awesome-command"
        )
        task.execute({})

        client_mock.instance.assert_called_once_with("awesome-image",
                                                     options=[],
                                                     args=None,
                                                     start=False)

        client_mock.execute.assert_called_once_with(mock.ANY,
                                                    "awesome-command",
                                                    return_result=True)

        execute_args, _ = client_mock.execute.call_args
        self.assertIs(execute_args[0], instance)

        instance.start.assert_called_once_with()
        instance.stop.assert_called_once_with()

    @parameterized.expand([
        ("",),
        (None,),
    ])
    def test_command_is_required(self, command):
        task = SingularityOperator(
            task_id='task-id',
            image="awesome-image",
            command=command
        )
        with six.assertRaisesRegex(self, AirflowException, "You must define a command."):
            task.execute({})

    @mock.patch('airflow.contrib.operators.singularity_operator.shutil')
    @mock.patch('airflow.contrib.operators.singularity_operator.os.path.exists')
    @mock.patch('airflow.contrib.operators.singularity_operator.Client')
    def test_image_should_be_pulled_when_not_exists(self, client_mock, exists_mock, shutil_mock):
        instance = mock.Mock(autospec=Instance, **{
            'start.return_value': 0,
            'stop.return_value': 0,
        })

        client_mock.pull.return_value = 'pull-file', ''
        client_mock.instance.return_value = instance
        client_mock.execute.return_value = {'return_code': 0,
                                            'message': 'message'}
        exists_mock.return_value = False

        task = SingularityOperator(
            task_id='task-id',
            image="awesome-image",
            command="awesome-command",
            pull_folder="/tmp/path",
            force_pull=True
        )
        task.execute({})

        client_mock.instance.assert_called_once_with(
            "/tmp/path/pull-file", options=[], args=None, start=False
        )
        client_mock.pull.assert_called_once_with("awesome-image", stream=True)
        client_mock.execute.assert_called_once_with(mock.ANY,
                                                    "awesome-command",
                                                    return_result=True)

        shutil_mock.move.assert_called_once_with("pull-file", "/tmp/path/pull-file")

    @parameterized.expand([
        (None, [], ),
        ([], [], ),
        (["AAA"], ['--bind', 'AAA'], ),
        (["AAA", "BBB"], ['--bind', 'AAA', '--bind', 'BBB'], ),
        (["AAA", "BBB", "CCC"], ['--bind', 'AAA', '--bind', 'BBB', '--bind', 'CCC'], ),

    ])
    @mock.patch('airflow.contrib.operators.singularity_operator.Client')
    def test_bind_options(self, volumes, expected_options, client_mock):
        instance = mock.Mock(autospec=Instance, **{
            'start.return_value': 0,
            'stop.return_value': 0,
        })
        client_mock.pull.return_value = 'pull-file', ''
        client_mock.instance.return_value = instance
        client_mock.execute.return_value = {'return_code': 0,
                                            'message': 'message'}

        task = SingularityOperator(
            task_id='task-id',
            image="awesome-image",
            command="awesome-command",
            force_pull=True,
            volumes=volumes
        )
        task.execute({})

        client_mock.instance.assert_called_once_with(
            "awesome-image", options=expected_options, args=None, start=False
        )

    @parameterized.expand([
        (None, [], ),
        ("", ['--workdir', ''], ),
        ("/work-dir/", ['--workdir', '/work-dir/'], ),
    ])
    @mock.patch('airflow.contrib.operators.singularity_operator.Client')
    def test_working_dir(self, working_dir, expected_working_dir, client_mock):
        instance = mock.Mock(autospec=Instance, **{
            'start.return_value': 0,
            'stop.return_value': 0,
        })
        client_mock.pull.return_value = 'pull-file', ''
        client_mock.instance.return_value = instance
        client_mock.execute.return_value = {'return_code': 0,
                                            'message': 'message'}

        task = SingularityOperator(
            task_id='task-id',
            image="awesome-image",
            command="awesome-command",
            force_pull=True,
            working_dir=working_dir
        )
        task.execute({})

        client_mock.instance.assert_called_once_with(
            "awesome-image", options=expected_working_dir, args=None, start=False
        )


if __name__ == "__main__":
    unittest.main()
