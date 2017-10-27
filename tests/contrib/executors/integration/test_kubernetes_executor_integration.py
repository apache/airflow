# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import unittest
import time
from uuid import uuid4
from tests.contrib.executors.integration.airflow_controller import (
    run_command, RunCommandError,
    run_dag, get_dag_run_state, dag_final_state, DagRunState,
    kill_scheduler
)


try:
    run_command("kubectl get pods")
except RunCommandError:
    SKIP_KUBE = True
else:
    SKIP_KUBE = False


class KubernetesExecutorTest(unittest.TestCase):

    @unittest.skipIf(SKIP_KUBE, 'Kubernetes integration tests are unsupported by this configuration')
    def test_kubernetes_executor_dag_runs_successfully(self):
        dag_id, run_id = "example_python_operator", uuid4().hex
        run_dag(dag_id, run_id)
        state = dag_final_state(dag_id, run_id, timeout=120)
        self.assertEquals(state, DagRunState.SUCCESS)

    @unittest.skipIf(SKIP_KUBE, 'Kubernetes integration tests are unsupported by this configuration')
    def test_start_dag_then_kill_scheduler_then_ensure_dag_succeeds(self):
        dag_id, run_id = "example_python_operator", uuid4().hex
        run_dag(dag_id, run_id)

        self.assertEquals(get_dag_run_state(dag_id, run_id), DagRunState.RUNNING)

        time.sleep(10)

        kill_scheduler()

        self.assertEquals(dag_final_state(dag_id, run_id, timeout=180), DagRunState.SUCCESS)

    @unittest.skipIf(SKIP_KUBE, 'Kubernetes integration tests are unsupported by this configuration')
    def test_kubernetes_executor_config_works(self):
        dag_id, run_id = "example_kubernetes_executor", uuid4().hex
        run_dag(dag_id, run_id)

        self.assertEquals(get_dag_run_state(dag_id, run_id), DagRunState.RUNNING)
        self.assertEquals(dag_final_state(dag_id, run_id, timeout=180), DagRunState.SUCCESS)


if __name__ == "__main__":
    unittest.main()
