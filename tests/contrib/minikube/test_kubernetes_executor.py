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
from subprocess import check_call, check_output

import requests
import time
import six

try:
    check_call(["kubectl", "get", "pods"])
except Exception as e:
    raise unittest.SkipTest(
        "Kubernetes integration tests require a minikube cluster;"
        "Skipping tests {}".format(e)
    )


class KubernetesExecutorTest(unittest.TestCase):

    def test_integration_run_dag(self):
        host_ip = check_output(['minikube', 'ip'])
        if six.PY3:
            host_ip = host_ip.decode('UTF-8')
        host = '{}:30809'.format(host_ip.strip())

        #  Enable the dag
        result = requests.get(
            'http://{}/api/experimental/'
            'dags/example_python_operator/paused/false'.format(host)
        )
        self.assertEqual(result.status_code, 200, "Could not enable DAG")

        # Trigger a new dagrun
        result = requests.post(
            'http://{}/api/experimental/'
            'dags/example_python_operator/dag_runs'.format(host),
            json={}
        )
        self.assertEqual(result.status_code, 200, "Could not trigger a DAG-run")

        time.sleep(1)

        result = requests.get(
            'http://{}/api/experimental/latest_runs'.format(host)
        )
        self.assertEqual(result.status_code, 200, "Could not get the latest DAG-run")
        result_json = result.json()

        self.assertGreater(len(result_json['items']), 0)

        execution_date = result_json['items'][0]['execution_date']
        print("Found the job with execution date {}".format(execution_date))

        tries = 0
        state = ''
        # Wait 100 seconds for the operator to complete
        while tries < 20:
            time.sleep(5)

            # Trigger a new dagrun
            result = requests.get(
                'http://{}/api/experimental/dags/example_python_operator/'
                'dag_runs/{}/tasks/print_the_context'.format(host, execution_date)
            )
            self.assertEqual(result.status_code, 200, "Could not get the status")
            result_json = result.json()
            state = result_json['state']
            print("Attempt {}: Current state of operator is {}".format(tries, state))

            if state == 'success':
                break
            tries += 1

        self.assertEqual(state, 'success')

        # Maybe check if we can retrieve the logs, but then we need to extend the API


if __name__ == '__main__':
    unittest.main()
