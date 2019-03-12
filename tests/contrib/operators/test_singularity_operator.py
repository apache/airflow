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

try:
    from airflow.contrib.operators.singularity_operator import SingularityOperator
except ImportError:
    pass

try:
    from unittest import mock
except ImportError:
    try:
        import mock
    except ImportError:
        mock = None


class SingularityOperatorTestCase(unittest.TestCase):
    @mock.patch('airflow.contrib.operators.singularity_operator.Client')
    def test_execute(self, client_mock):

        client_mock.pull_image.return_value = '', ''
        client_mock.instance.start.return_value = 0
        client_mock.instance.stop.return_value = 0
        client_mock.execute.return_value = {'return_code': 0,
                                            'message': 'message'}

        task = SingularityOperator(
            task_id='task-id',
            image="awesome-image",
            command="awesome-command"
        )
        task.execute({})
        client_mock.execute.assert_called_once_with(mock.ANY,
                                                    "awesome-command",
                                                    return_result=True)


if __name__ == "__main__":
    unittest.main()
