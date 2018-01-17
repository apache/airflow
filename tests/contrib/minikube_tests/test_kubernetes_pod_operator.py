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
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow import AirflowException
from subprocess import check_call


try:
    check_call(["kubectl", "get", "pods"])
except Exception as e:
    raise unittest.SkipTest(
        "Kubernetes integration tests require a minikube cluster;"
        "Skipping tests {}".format(e)
    )


class KubernetesPodOperatorTest(unittest.TestCase):

    def test_working_pod(self):
        k = KubernetesPodOperator(namespace='default',
                                  image="ubuntu:16.04",
                                  cmds=["bash", "-cx"],
                                  arguments=["echo", "10"],
                                  labels={"foo": "bar"},
                                  name="test",
                                  task_id="task"
                                  )

        k.execute(None)

    def test_faulty_image(self):
        bad_image_name = "foobar"
        k = KubernetesPodOperator(namespace='default',
                                  image=bad_image_name,
                                  cmds=["bash", "-cx"],
                                  arguments=["echo", "10"],
                                  labels={"foo": "bar"},
                                  name="test",
                                  task_id="task",
                                  startup_timeout_seconds=5
                                  )
        with self.assertRaises(AirflowException) as cm:
            k.execute(None),

        print("exception: {}".format(cm))

    def test_pod_failure(self):
        """
            Tests that the task fails when a pod reports a failure
        """

        bad_internal_command = "foobar"
        k = KubernetesPodOperator(namespace='default',
                                  image="ubuntu:16.04",
                                  cmds=["bash", "-cx"],
                                  arguments=[bad_internal_command, "10"],
                                  labels={"foo": "bar"},
                                  name="test",
                                  task_id="task"
                                  )

        with self.assertRaises(AirflowException):
            k.execute(None)
