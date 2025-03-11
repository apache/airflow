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

import json
import os
import re
import subprocess
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from subprocess import check_call, check_output
from urllib.parse import parse_qs, urlparse

import pytest
import requests
import requests.exceptions
from requests.adapters import HTTPAdapter
from requests.exceptions import RetryError
from urllib3.exceptions import MaxRetryError
from urllib3.util.retry import Retry

CLUSTER_FORWARDED_PORT = os.environ.get("CLUSTER_FORWARDED_PORT") or "8080"
KUBERNETES_HOST_PORT = (os.environ.get("CLUSTER_HOST") or "localhost") + ":" + CLUSTER_FORWARDED_PORT
EXECUTOR = os.environ.get("EXECUTOR")

print()
print(f"Cluster host/port used: ${KUBERNETES_HOST_PORT}")
print(f"Executor: {EXECUTOR}")
print()


class StringContainingId(str):
    def __eq__(self, other):
        return self in other


class BaseK8STest:
    """Base class for K8S Tests."""

    host: str = KUBERNETES_HOST_PORT
    temp_dir = Path(tempfile.gettempdir())  # Refers to global temp directory, in linux it usual "/tmp"
    session: requests.Session
    test_id: str

    @pytest.fixture(autouse=True)
    def base_tests_setup(self, request):
        if self.set_api_server_base_url_config():
            # only restart the deployment if the configmap was updated
            # speed up the test and make the airflow-api-server deployment more stable
            self.rollout_restart_deployment("airflow-api-server")
            self.ensure_deployment_health("airflow-api-server")

        # Replacement for unittests.TestCase.id()
        self.test_id = f"{request.node.cls.__name__}_{request.node.name}"
        self.session = self._get_session_with_retries()
        try:
            self._ensure_airflow_api_server_is_healthy()
            yield
        finally:
            self.session.close()

    def _describe_resources(self, namespace: str):
        kubeconfig_basename = os.path.basename(os.environ.get("KUBECONFIG", "default"))
        output_file_path = (
            self.temp_dir / f"k8s_test_resources_{namespace}_{kubeconfig_basename}_{self.test_id}.txt"
        )
        print(f"Dumping resources to {output_file_path}")
        ci = os.environ.get("CI")
        if ci and ci.lower() == "true":
            print("The resource dump will be uploaded as artifact of the CI job")
        with open(output_file_path, "w") as output_file:
            print("=" * 80, file=output_file)
            print(f"Describe resources for namespace {namespace}", file=output_file)
            print(f"Datetime: {datetime.now(tz=timezone.utc)}", file=output_file)
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
        air_pod = air_pod.splitlines()
        names = [re.compile(r"\s+").split(x)[0] for x in air_pod if "airflow" in x]
        return len(names)

    @staticmethod
    def _delete_airflow_pod(name=""):
        suffix = f"-{name}" if name else ""
        air_pod = check_output(["kubectl", "get", "pods"]).decode()
        air_pod = air_pod.splitlines()
        names = [re.compile(r"\s+").split(x)[0] for x in air_pod if "airflow" + suffix in x]
        if names:
            check_call(["kubectl", "delete", "pod", names[0]])

    @staticmethod
    def _get_jwt_token(username: str, password: str) -> str:
        """Get the JWT token for the given username and password.

        Note: API server is still using FAB Auth Manager.

        Steps:
        1. Get the login page to get the csrf token
            - The csrf token is in the hidden input field with id "csrf_token"
        2. Login with the username and password
            - Must use the same session to keep the csrf token session
        3. Extract the JWT token from the redirect url
            - Expected to have a connection error
            - The redirect url should have the JWT token as a query parameter

        :param session: The session to use for the request
        :param username: The username to use for the login
        :param password: The password to use for the login
        :return: The JWT token
        """
        # get csrf token from login page
        retry = Retry(total=5, backoff_factor=10)
        session = requests.Session()
        session.mount("http://", HTTPAdapter(max_retries=retry))
        session.mount("https://", HTTPAdapter(max_retries=retry))
        get_login_form_response = session.get(f"http://{KUBERNETES_HOST_PORT}/auth/login")
        csrf_token = re.search(
            r'<input id="csrf_token" name="csrf_token" type="hidden" value="(.+?)">',
            get_login_form_response.text,
        )
        assert csrf_token, "Failed to get csrf token from login page"
        csrf_token_str = csrf_token.group(1)
        assert csrf_token_str, "Failed to get csrf token from login page"
        # login with form data
        login_response = session.post(
            f"http://{KUBERNETES_HOST_PORT}/auth/login",
            data={"username": username, "password": password, "csrf_token": csrf_token_str},
        )
        redirect_url = login_response.url
        # ensure redirect_url is a string
        redirect_url_str = str(redirect_url) if redirect_url is not None else ""
        assert "/?token" in redirect_url_str, f"Login failed with redirect url {redirect_url_str}"
        parsed_url = urlparse(redirect_url_str)
        query_params = parse_qs(str(parsed_url.query))
        jwt_token_list = query_params.get("token")
        jwt_token = jwt_token_list[0] if jwt_token_list else None
        assert jwt_token, f"Failed to get JWT token from redirect url {redirect_url_str}"
        return jwt_token

    def _get_session_with_retries(self):
        class JWTRefreshAdapter(HTTPAdapter):
            def __init__(self, base_instance, **kwargs):
                self.base_instance = base_instance
                super().__init__(**kwargs)

            def send(self, request, **kwargs):
                response = super().send(request, **kwargs)
                if response.status_code in (401, 403):
                    # Refresh token and update the Authorization header with retry logic.
                    attempts = 0
                    jwt_token = None
                    while attempts < 5:
                        try:
                            jwt_token = self.base_instance._get_jwt_token("admin", "admin")
                            break
                        except Exception:
                            attempts += 1
                            time.sleep(1)
                    if jwt_token is None:
                        raise Exception("Failed to refresh JWT token after 5 attempts")
                    request.headers["Authorization"] = f"Bearer {jwt_token}"
                    response = super().send(request, **kwargs)
                return response

        jwt_token = self._get_jwt_token("admin", "admin")
        session = requests.Session()
        session.headers.update({"Authorization": f"Bearer {jwt_token}"})
        retries = Retry(
            total=5,
            backoff_factor=10,
            status_forcelist=[404],
            allowed_methods=Retry.DEFAULT_ALLOWED_METHODS | frozenset(["PATCH", "POST"]),
        )
        adapter = JWTRefreshAdapter(self, max_retries=retries)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session

    def _ensure_airflow_api_server_is_healthy(self):
        max_tries = 10
        timeout_seconds = 5
        for i in range(max_tries):
            try:
                response = self.session.get(
                    f"http://{KUBERNETES_HOST_PORT}/public/monitor/health",
                    timeout=1,
                )
                if response.status_code == 200:
                    print("Airflow api server is healthy!")
                    return
            except Exception as e:
                print(f"Exception when checking if api server is healthy {e}")
                if i < max_tries - 1:
                    print(f"Waiting {timeout_seconds} s and retrying.")
                    time.sleep(timeout_seconds)
        raise Exception(
            f"Giving up. The api server of Airflow was not healthy after {max_tries} tries "
            f"with {timeout_seconds} s delays"
        )

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
                    f"http://{host}/public/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}"
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
                if state in {"failed", "upstream_failed", "removed"}:
                    # If the TI is in failed state (and that's not the state we want) there's no point
                    # continuing to poll, it won't change
                    break
                self._describe_resources(namespace="airflow")
                self._describe_resources(namespace="default")
                tries += 1
            except requests.exceptions.ConnectionError as e:
                check_call(["echo", f"api call failed. trying again. error {e}"])
        if state != expected_final_state:
            print(f"The expected state is wrong {state} != {expected_final_state} (expected)!")
        assert state == expected_final_state

    @staticmethod
    def ensure_deployment_health(deployment_name: str, namespace: str = "airflow"):
        """Watch the deployment until it is healthy."""
        deployment_rollout_status = check_output(
            ["kubectl", "rollout", "status", "deployment", deployment_name, "-n", namespace, "--watch"]
        ).decode()
        assert "successfully rolled out" in deployment_rollout_status

    @staticmethod
    def rollout_restart_deployment(deployment_name: str, namespace: str = "airflow"):
        """Rollout restart the deployment."""
        check_call(["kubectl", "rollout", "restart", "deployment", deployment_name, "-n", namespace])

    def _parse_airflow_cfg_as_dict(self, airflow_cfg: str) -> dict[str, dict[str, str]]:
        """Parse the airflow.cfg file as a dictionary."""
        parsed_airflow_cfg: dict[str, dict[str, str]] = {}
        for line in airflow_cfg.splitlines():
            if line.startswith("["):
                section = line[1:-1]
                parsed_airflow_cfg[section] = {}
            elif "=" in line:
                key, value = line.split("=", 1)
                parsed_airflow_cfg[section][key.strip()] = value.strip()
        return parsed_airflow_cfg

    def _parse_airflow_cfg_dict_as_escaped_toml(self, airflow_cfg_dict: dict) -> str:
        """Parse the airflow.cfg dictionary as a toml string."""
        airflow_cfg_str = ""
        for section, section_dict in airflow_cfg_dict.items():
            airflow_cfg_str += f"[{section}]\n"
            for key, value in section_dict.items():
                airflow_cfg_str += f"{key} = {value}\n"
            airflow_cfg_str += "\n"
        # escape newlines and double quotes
        return airflow_cfg_str.replace("\n", "\\n").replace('"', '\\"')

    def set_api_server_base_url_config(self) -> bool:
        """Set [api/base_url] with `f"http://{KUBERNETES_HOST_PORT}"` as env in k8s configmap.

        :return: True if the configmap was updated successfully, False otherwise
        """
        configmap_name = "airflow-config"
        configmap_key = "airflow.cfg"
        original_configmap_json_str = check_output(
            ["kubectl", "get", "configmap", configmap_name, "-n", "airflow", "-o", "json"]
        ).decode()
        original_config_map = json.loads(original_configmap_json_str)
        original_airflow_cfg = original_config_map["data"][configmap_key]
        # set [api/base_url] with `f"http://{KUBERNETES_HOST_PORT}"` in airflow.cfg
        # The airflow.cfg is toml format, so we need to convert it to json
        airflow_cfg_dict = self._parse_airflow_cfg_as_dict(original_airflow_cfg)
        airflow_cfg_dict["api"]["base_url"] = f"http://{KUBERNETES_HOST_PORT}"
        # update the configmap with the new airflow.cfg
        patch_configmap_result = check_output(
            [
                "kubectl",
                "patch",
                "configmap",
                configmap_name,
                "-n",
                "airflow",
                "--type",
                "merge",
                "-p",
                f'{{"data": {{"{configmap_key}": "{self._parse_airflow_cfg_dict_as_escaped_toml(airflow_cfg_dict)}"}}}}',
            ]
        ).decode()
        if "(no change)" in patch_configmap_result:
            return False
        return True

    def ensure_dag_expected_state(self, host, logical_date, dag_id, expected_final_state, timeout):
        tries = 0
        state = ""
        max_tries = max(int(timeout / 5), 1)
        # Wait some time for the operator to complete
        while tries < max_tries:
            time.sleep(5)
            get_string = f"http://{host}/public/dags/{dag_id}/dagRuns"
            print(f"Calling {get_string}")
            # Get all dagruns
            result = self.session.get(get_string)
            assert result.status_code == 200, "Could not get the status"
            result_json = result.json()
            print(f"Received: {result}")
            state = None
            for dag_run in result_json["dag_runs"]:
                if dag_run["logical_date"] == logical_date:
                    state = dag_run["state"]
            check_call(["echo", f"Attempt {tries}: Current state of dag is {state}"])
            print(f"Attempt {tries}: Current state of dag is {state}")

            if state == expected_final_state:
                break
            if state == "failed":
                # If the DR is in failed state there's no point continuing to poll!
                break
            self._describe_resources("airflow")
            self._describe_resources("default")
            tries += 1
        assert state == expected_final_state

        # Maybe check if we can retrieve the logs, but then we need to extend the API

    def start_dag(self, dag_id, host):
        patch_string = f"http://{host}/public/dags/{dag_id}"
        print(f"Calling [start_dag]#1 {patch_string}")
        max_attempts = 10
        result = {}
        # This loop retries until the DAG parser finishes with max_attempts and the DAG is available for execution.
        # Keep the try/catch block, as the session object has a default retry configuration.
        # If a MaxRetryError, RetryError is raised, it can be safely ignored, indicating that the DAG is not yet parsed.
        while max_attempts:
            try:
                result = self.session.patch(patch_string, json={"is_paused": False})
                if result.status_code == 200:
                    break
            except (MaxRetryError, RetryError):
                pass

            time.sleep(30)
            max_attempts -= 1

        try:
            result_json = result.json()
        except ValueError:
            result_json = str(result)
        print(f"Received [start_dag]#1 {result_json}")
        assert result.status_code == 200, f"Could not enable DAG: {result_json}"
        post_string = f"http://{host}/public/dags/{dag_id}/dagRuns"
        print(f"Calling [start_dag]#2 {post_string}")

        logical_date = datetime.now(timezone.utc).isoformat()
        # Trigger a new dagrun
        result = self.session.post(post_string, json={"logical_date": logical_date})
        try:
            result_json = result.json()
        except ValueError:
            result_json = str(result)
        print(f"Received [start_dag]#2 {result_json}")
        assert result.status_code == 200, f"Could not trigger a DAG-run: {result_json}"

        time.sleep(1)

        get_string = f"http://{host}/public/dags/{dag_id}/dagRuns"
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
        logical_date = None
        dag_run_id = None
        for dag_run in dag_runs:
            if dag_run["dag_id"] == dag_id:
                logical_date = dag_run["logical_date"]
                run_after = dag_run["run_after"]
                dag_run_id = dag_run["dag_run_id"]
                break
        assert run_after is not None, f"No run_after can be found for the dag with {dag_id}"
        return dag_run_id, logical_date
