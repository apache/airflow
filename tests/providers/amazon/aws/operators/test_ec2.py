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

import unittest

from moto import mock_ec2

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.ec2 import EC2Hook
from airflow.providers.amazon.aws.operators.ec2 import EC2Operator


class TestEC2Operator(unittest.TestCase):

    def test_init(self):
        ec2_operator = EC2Operator(
            task_id="task_test",
            operation="start",
            instance_id="i-123abc",
            aws_conn_id="aws_conn_test",
            region_name="region-test",
            check_interval=3,
        )
        self.assertEqual(ec2_operator.task_id, "task_test")
        self.assertEqual(ec2_operator.operation, "start")
        self.assertEqual(ec2_operator.instance_id, "i-123abc")
        self.assertEqual(ec2_operator.aws_conn_id, "aws_conn_test")
        self.assertEqual(ec2_operator.region_name, "region-test")
        self.assertEqual(ec2_operator.check_interval, 3)
        self.assertEqual(ec2_operator.target_state, "running")

    def test_init_invalid_operation(self):
        invalid_operation = "operation_test"
        with self.assertRaises(AirflowException) as cm:
            EC2Operator(
                task_id="task_test",
                operation=invalid_operation,
                instance_id="i-123abc",
            )
        msg = f"Invalid operation: {invalid_operation}"
        self.assertEqual(str(cm.exception), msg)

    @mock_ec2
    def test_start_instance(self):
        # create instance
        ec2_hook = EC2Hook()
        instances = ec2_hook.get_conn().create_instances(
            MaxCount=1,
            MinCount=1,
        )
        instance_id = instances[0].instance_id

        # start instance
        start_test = EC2Operator(
            task_id="start_test",
            operation="start",
            instance_id=instance_id,
        )
        start_test.execute(None)
        # assert instance state is running
        self.assertEqual(
            ec2_hook.get_conn().Instance(id=instance_id).state["Name"],
            "running"
        )

    @mock_ec2
    def test_stop_instance(self):
        # create instance
        ec2_hook = EC2Hook()
        instances = ec2_hook.get_conn().create_instances(
            MaxCount=1,
            MinCount=1,
        )
        instance_id = instances[0].instance_id

        # stop instance
        stop_test = EC2Operator(
            task_id="stop_test",
            operation="stop",
            instance_id=instance_id,
        )
        stop_test.execute(None)
        # assert instance state is stopped
        self.assertEqual(
            ec2_hook.get_conn().Instance(id=instance_id).state["Name"],
            "stopped"
        )


if __name__ == '__main__':
    unittest.main()
