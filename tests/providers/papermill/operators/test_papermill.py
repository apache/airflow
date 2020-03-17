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
from unittest.mock import patch

from airflow.providers.papermill.operators.papermill import PapermillOperator


class TestPapermillOperator(unittest.TestCase):
    @patch('airflow.providers.papermill.operators.papermill.pm')
    def test_execute(self, mock_papermill):
        in_nb = "/tmp/does_not_exist"
        out_nb = "/tmp/will_not_exist"
        parameters = {"msg": "hello_world",
                      "train": 1}

        op = PapermillOperator(
            input_nb=in_nb, output_nb=out_nb, parameters=parameters,
            task_id="papermill_operator_test",
            dag=None
        )

        op.pre_execute(context={})  # make sure to have the inlets
        op.execute(context={})

        mock_papermill.execute_notebook.assert_called_once_with(
            in_nb,
            out_nb,
            parameters=parameters,
            progress_bar=False,
            report_mode=True
        )
