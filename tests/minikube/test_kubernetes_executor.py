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


import os
import unittest
from subprocess import check_call, check_output
import requests.exceptions
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import time

import re

try:
    check_call(["/usr/local/bin/kubectl", "get", "pods"])
except Exception as e:
    if os.environ.get('KUBERNETES_VERSION'):
        raise e
    else:
        raise unittest.SkipTest(
            "Kubernetes integration tests require a minikube cluster;"
            "Skipping tests {}".format(e)
        )


def get_minikube_host():
    if "MINIKUBE_IP" in os.environ:
        host_ip = os.environ['MINIKUBE_IP']
    else:
        host_ip = check_output(['/usr/local/bin/minikube', 'ip']).decode('UTF-8')

    host = '{}:30809'.format(host_ip.strip())
    return host


class TestKubernetesExecutor(unittest.TestCase):
    @staticmethod
    def _delete_airflow_pod():
        air_pod = check_output(['kubectl', 'get', 'pods']).decode()
        air_pod = air_pod.split('\n')
        names = [re.compile(r'\s+').split(x)[0] for x in air_pod if 'airflow' in x]
        if names:
            check_call(['kubectl', 'delete', 'pod', names[0]])

    def _get_session_with_retries(self):
        session = requests.Session()
        retries = Retry(total=3, backoff_factor=1)
        session.mount('http://', HTTPAdapter(max_retries=retries))
        session.mount('https://', HTTPAdapter(max_retries=retries))
        return session

    def _ensure_airflow_webserver_is_healthy(self):
        response = self.session.get(
            "http://{host}/health".format(host=get_minikube_host()),
            timeout=1,
        )

        self.assertEqual(response.status_code, 200)

    def setUp(self):
        self.session = self._get_session_with_retries()
        self._ensure_airflow_webserver_is_healthy()

    def tearDown(self):
        self.session.close()

    def monitor_task(self, host, execution_date, dag_id, task_id, expected_final_state,
                     timeout):
        tries = 0
        state = ''
        max_tries = max(int(timeout / 5), 1)
        # Wait 100 seconds for the operator to complete
        while tries < max_tries:
            time.sleep(5)

            # Trigger a new dagrun
            try:
                result = self.session.get(
                    'http://{host}/api/experimental/dags/{dag_id}/'
                    'dag_runs/{execution_date}/tasks/{task_id}'
                    .format(host=host,
                            dag_id=dag_id,
                            execution_date=execution_date,
                            task_id=task_id)
                )
                self.assertEqual(result.status_code, 200, "Could not get the status")
                result_json = result.json()
                state = result_json['state']
                print("Attempt {}: Current state of operator is {}".format(tries, state))

                if state == expected_final_state:
                    break
                tries += 1
            except requests.exceptions.ConnectionError as e:
                check_call(["echo", "api call failed. trying again. error {}".format(e)])

        self.assertEqual(state, expected_final_state)

        # Maybe check if we can retrieve the logs, but then we need to extend the API

    def ensure_dag_expected_state(self, host, execution_date, dag_id,
                                  expected_final_state,
                                  timeout):
        tries = 0
        state = ''
        max_tries = max(int(timeout / 5), 1)
        # Wait 100 seconds for the operator to complete
        while tries < max_tries:
            time.sleep(5)

            # Trigger a new dagrun
            result = self.session.get(
                'http://{host}/api/experimental/dags/{dag_id}/'
                'dag_runs/{execution_date}'
                .format(host=host,
                        dag_id=dag_id,
                        execution_date=execution_date)
            )
            print(result)
            self.assertEqual(result.status_code, 200, "Could not get the status")
            result_json = result.json()
            print(result_json)
            state = result_json['state']
            check_call(
                ["echo", "Attempt {}: Current state of dag is {}".format(tries, state)])
            print("Attempt {}: Current state of dag is {}".format(tries, state))

            if state == expected_final_state:
                break
            tries += 1

        self.assertEqual(state, expected_final_state)

        # Maybe check if we can retrieve the logs, but then we need to extend the API

    def start_dag(self, dag_id, host):
        result = self.session.get(
            'http://{host}/api/experimental/'
            'dags/{dag_id}/paused/false'.format(host=host, dag_id=dag_id)
        )
        try:
            result_json = result.json()
        except ValueError:
            result_json = str(result)

        self.assertEqual(result.status_code, 200, "Could not enable DAG: {result}"
                         .format(result=result_json))

        # Trigger a new dagrun
        result = self.session.post(
            'http://{host}/api/experimental/'
            'dags/{dag_id}/dag_runs'.format(host=host, dag_id=dag_id),
            json={}
        )
        try:
            result_json = result.json()
        except ValueError:
            result_json = str(result)

        self.assertEqual(result.status_code, 200, "Could not trigger a DAG-run: {result}"
                         .format(result=result_json))

        time.sleep(1)

        result = self.session.get(
            'http://{}/api/experimental/latest_runs'.format(host)
        )
        self.assertEqual(result.status_code, 200, "Could not get the latest DAG-run:"
                                                  " {result}"
                         .format(result=result.json()))
        result_json = result.json()
        return result_json

    def test_integration_run_dag(self):
        host = get_minikube_host()
        dag_id = 'example_kubernetes_executor_config'

        result_json = self.start_dag(dag_id=dag_id, host=host)

        self.assertGreater(len(result_json['items']), 0)

        execution_date = result_json['items'][0]['execution_date']
        print("Found the job with execution date {}".format(execution_date))

        # Wait 100 seconds for the operator to complete
        self.monitor_task(host=host,
                          execution_date=execution_date,
                          dag_id=dag_id,
                          task_id='start_task',
                          expected_final_state='success', timeout=100)

        self.ensure_dag_expected_state(host=host,
                                       execution_date=execution_date,
                                       dag_id=dag_id,
                                       expected_final_state='success', timeout=100)

    def test_integration_run_dag_with_scheduler_failure(self):
        host = get_minikube_host()
        dag_id = 'example_kubernetes_executor_config'

        result_json = self.start_dag(dag_id=dag_id, host=host)

        self.assertGreater(len(result_json['items']), 0)

        execution_date = result_json['items'][0]['execution_date']
        print("Found the job with execution date {}".format(execution_date))

        self._delete_airflow_pod()

        time.sleep(10)  # give time for pod to restart

        # Wait 100 seconds for the operator to complete
        self.monitor_task(host=host,
                          execution_date=execution_date,
                          dag_id=dag_id,
                          task_id='start_task',
                          expected_final_state='success', timeout=120)

        self.ensure_dag_expected_state(host=host,
                                       execution_date=execution_date,
                                       dag_id=dag_id,
                                       expected_final_state='success', timeout=100)


if __name__ == '__main__':
    unittest.main()
