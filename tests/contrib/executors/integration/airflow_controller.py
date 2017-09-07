# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import subprocess
import time


class RunCommandError(Exception):
    pass


class TimeoutError(Exception):
    pass


class DagRunState:
    SUCCESS = "success"
    FAILED = "failed"
    RUNNING = "running"


def run_command(command):
    process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = process.communicate()
    if process.returncode != 0:
        raise RunCommandError("Error while running command: {}; Stdout: {}; Stderr: {}".format(
            command, stdout, stderr
        ))
    return stdout, stderr


def run_command_in_pod(pod_name, container_name, command):
    return run_command("kubectl exec {pod_name} -c {container_name} -- {command}".format(
        pod_name=pod_name, container_name=container_name, command=command
    ))

def _unpause_dag(dag_id, airflow_pod=None):
    airflow_pod = airflow_pod or _get_airflow_pod()
    return run_command_in_pod(airflow_pod, "scheduler", "airflow unpause {dag_id}".format(dag_id=dag_id))

def run_dag(dag_id, run_id, airflow_pod=None):
    airflow_pod = airflow_pod or _get_airflow_pod()
    _unpause_dag(dag_id, airflow_pod)
    return run_command_in_pod(airflow_pod, "scheduler", "airflow trigger_dag {dag_id} -r {run_id}".format(
        dag_id=dag_id, run_id=run_id
    ))


def _get_pod_by_grep(grep_phrase):
    stdout, stderr = run_command("kubectl get pods | grep {grep_phrase} | awk '{{print $1}}'".format(
        grep_phrase=grep_phrase
    ))
    pod_name = stdout.strip()
    return pod_name


def _get_airflow_pod():
    return _get_pod_by_grep("^airflow")


def _get_postgres_pod():
    return _get_pod_by_grep("^postgres")


def _parse_state(stdout):
    end_line = "(1 row)"
    prev_line = None
    for line in stdout.split("\n"):
        if end_line in line:
            return prev_line.strip()
        prev_line = line

    raise Exception("Unknown psql output: {}".format(stdout))

def get_dag_run_state(dag_id, run_id, postgres_pod=None):
    postgres_pod = postgres_pod or _get_postgres_pod()
    stdout, stderr = run_command_in_pod(
        postgres_pod, "postgres",
        """psql airflow -c "select state from dag_run where dag_id='{dag_id}' and run_id='{run_id}'" """.format(
            dag_id=dag_id, run_id=run_id
        )
    )
    return _parse_state(stdout)


def dag_final_state(dag_id, run_id, postgres_pod=None, poll_interval=1, timeout=120):
    postgres_pod = postgres_pod or _get_postgres_pod()
    for _ in range(0, timeout / poll_interval):
        dag_state = get_dag_run_state(dag_id, run_id, postgres_pod)
        if dag_state != DagRunState.RUNNING:
            return dag_state
        time.sleep(poll_interval)

    raise TimeoutError("Timed out while waiting for DagRun with dag_id: {} run_id: {}".format(dag_id, run_id))


def _kill_pod(pod_name):
    return run_command("kubectl delete pod {pod_name}".format(pod_name=pod_name))


def kill_scheduler():
    airflow_pod = _get_pod_by_grep("^airflow")
    return _kill_pod(airflow_pod)
