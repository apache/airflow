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
from __future__ import annotations

import os
import re
import subprocess
import tempfile
import time
import unittest
from datetime import datetime
from pathlib import Path
from subprocess import check_call, check_output

import requests
import requests.exceptions
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

CLUSTER_FORWARDED_PORT = os.environ.get("CLUSTER_FORWARDED_PORT") or "8080"
KUBERNETES_HOST_PORT = (os.environ.get("CLUSTER_HOST") or "localhost") + ":" + CLUSTER_FORWARDED_PORT
EXECUTOR = os.environ.get("EXECUTOR")

print()
print(f"Cluster host/port used: ${KUBERNETES_HOST_PORT}")
print(f"Executor: {EXECUTOR}")
print()


class TestBase(unittest.TestCase):
    def _describe_resources(self, namespace: str):
        kubeconfig_basename = os.path.basename(os.environ.get("KUBECONFIG", "default"))
        output_file_path = (
            Path(tempfile.gettempdir())
            / f"k8s_test_resources_{namespace}_{kubeconfig_basename}_{self.id()}.txt"
        )
        print(f"Dumping resources to {output_file_path}")
        ci = os.environ.get("CI")
        if ci and ci.lower() == "true":
            print("The resource dump will be uploaded as artifact of the CI job")
        with open(output_file_path, "w") as output_file:
            print("=" * 80, file=output_file)
            print(f"Describe resources for namespace {namespace}", file=output_file)
            print(f"Datetime: {datetime.utcnow()}", file=output_file)
            print("=" * 80, file=output_file)
            print("Describing pods", file=output_file)
            print("-" * 80, file=output_file)
            subprocess.call(
                ["kubectl", "describe", "pod", "--namespace", namespace],
                stdout=output_file,
                stderr=subprocess.STDOUT,
            )
            print("=" * 80, file=output_file)
            print("Describing persistent volumes", file=output_file)
            print("-" * 80, file=output_file)
            subprocess.call(
                ["kubectl", "describe", "pv", "--namespace", namespace],
                stdout=output_file,
                stderr=subprocess.STDOUT,
            )
            print("=" * 80, file=output_file)
            print("Describing persistent volume claims", file=output_file)
            print("-" * 80, file=output_file)
            subprocess.call(
                ["kubectl", "describe", "pvc", "--namespace", namespace],
                stdout=output_file,
                stderr=subprocess.STDOUT,
            )
            print("=" * 80, file=output_file)

    @staticmethod
    def _num_pods_in_namespace(namespace):
        air_pod = check_output(["kubectl", "get", "pods", "-n", namespace]).decode()
        air_pod = air_pod.split("\n")
        names = [re.compile(r"\s+").split(x)[0] for x in air_pod if "airflow" in x]
        return len(names)

    @staticmethod
    def _delete_airflow_pod(name=""):
        suffix = "-" + name if name else ""
        air_pod = check_output(["kubectl", "get", "pods"]).decode()
        air_pod = air_pod.split("\n")
        names = [re.compile(r"\s+").split(x)[0] for x in air_pod if "airflow" + suffix in x]
        if names:
            check_call(["kubectl", "delete", "pod", names[0]])

    def _get_session_with_retries(self):
        session = requests.Session()
        session.auth = ("admin", "admin")
        retries = Retry(total=3, backoff_factor=1)
        session.mount("http://", HTTPAdapter(max_retries=retries))
        session.mount("https://", HTTPAdapter(max_retries=retries))
        return session

    def _ensure_airflow_webserver_is_healthy(self):
        max_tries = 10
        timeout_seconds = 5
        for i in range(max_tries):
            try:
                response = self.session.get(
                    f"http://{KUBERNETES_HOST_PORT}/health",
                    timeout=1,
                )
                if response.status_code == 200:
                    print("Airflow webserver is healthy!")
                    return
            except Exception as e:
                print(f"Exception when checking if webserver is healthy {e}")
                if i < max_tries - 1:
                    print(f"Waiting {timeout_seconds} s and retrying.")
                    time.sleep(timeout_seconds)
        raise Exception(
            f"Giving up. The webserver of Airflow was not healthy after {max_tries} tries "
            f"with {timeout_seconds} s delays"
        )

    def setUp(self):
        self.host = KUBERNETES_HOST_PORT
        self.session = self._get_session_with_retries()
        self._ensure_airflow_webserver_is_healthy()

    def tearDown(self):
        self.session.close()

    def monitor_task(self, host, dag_run_id, dag_id, task_id, expected_final_state, timeout):
        tries = 0
        state = ""
        max_tries = max(int(timeout / 5), 1)
        # Wait some time for the operator to complete
        while tries < max_tries:
            time.sleep(5)
            # Check task state
            try:
                get_string = (
                    f"http://{host}/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}"
                )
                print(f"Calling [monitor_task]#1 {get_string}")
                result = self.session.get(get_string)
                if result.status_code == 404:
                    check_call(["echo", "api returned 404."])
                    tries += 1
                    continue
                assert result.status_code == 200, "Could not get the status"
                result_json = result.json()
                print(f"Received [monitor_task]#2: {result_json}")
                state = result_json["state"]
                print(f"Attempt {tries}: Current state of operator is {state}")

                if state == expected_final_state:
                    break
                self._describe_resources(namespace="airflow")
                self._describe_resources(namespace="default")
                tries += 1
            except requests.exceptions.ConnectionError as e:
                check_call(["echo", f"api call failed. trying again. error {e}"])
        if state != expected_final_state:
            print(f"The expected state is wrong {state} != {expected_final_state} (expected)!")
        assert state == expected_final_state

    def ensure_dag_expected_state(self, host, execution_date, dag_id, expected_final_state, timeout):
        tries = 0
        state = ""
        max_tries = max(int(timeout / 5), 1)
        # Wait some time for the operator to complete
        while tries < max_tries:
            time.sleep(5)
            get_string = f"http://{host}/api/v1/dags/{dag_id}/dagRuns"
            print(f"Calling {get_string}")
            # Get all dagruns
            result = self.session.get(get_string)
            assert result.status_code == 200, "Could not get the status"
            result_json = result.json()
            print(f"Received: {result}")
            state = None
            for dag_run in result_json["dag_runs"]:
                if dag_run["execution_date"] == execution_date:
                    state = dag_run["state"]
            check_call(["echo", f"Attempt {tries}: Current state of dag is {state}"])
            print(f"Attempt {tries}: Current state of dag is {state}")

            if state == expected_final_state:
                break
            self._describe_resources("airflow")
            self._describe_resources("default")
            tries += 1
        assert state == expected_final_state

        # Maybe check if we can retrieve the logs, but then we need to extend the API

    def start_dag(self, dag_id, host):
        patch_string = f"http://{host}/api/v1/dags/{dag_id}"
        print(f"Calling [start_dag]#1 {patch_string}")
        result = self.session.patch(patch_string, json={"is_paused": False})
        try:
            result_json = result.json()
        except ValueError:
            result_json = str(result)
        print(f"Received [start_dag]#1 {result_json}")
        assert result.status_code == 200, f"Could not enable DAG: {result_json}"
        post_string = f"http://{host}/api/v1/dags/{dag_id}/dagRuns"
        print(f"Calling [start_dag]#2 {post_string}")
        # Trigger a new dagrun
        result = self.session.post(post_string, json={})
        try:
            result_json = result.json()
        except ValueError:
            result_json = str(result)
        print(f"Received [start_dag]#2 {result_json}")
        assert result.status_code == 200, f"Could not trigger a DAG-run: {result_json}"

        time.sleep(1)

        get_string = f"http://{host}/api/v1/dags/{dag_id}/dagRuns"
        print(f"Calling [start_dag]#3 {get_string}")
        result = self.session.get(get_string)
        assert result.status_code == 200, f"Could not get DAGRuns: {result.json()}"
        result_json = result.json()
        print(f"Received: [start_dag]#3 {result_json}")
        return result_json

    def start_job_in_kubernetes(self, dag_id, host):
        result_json = self.start_dag(dag_id=dag_id, host=host)
        dag_runs = result_json["dag_runs"]
        assert len(dag_runs) > 0
        execution_date = None
        dag_run_id = None
        for dag_run in dag_runs:
            if dag_run["dag_id"] == dag_id:
                execution_date = dag_run["execution_date"]
                dag_run_id = dag_run["dag_run_id"]
                break
        assert execution_date is not None, f"No execution_date can be found for the dag with {dag_id}"
        return dag_run_id, execution_date
