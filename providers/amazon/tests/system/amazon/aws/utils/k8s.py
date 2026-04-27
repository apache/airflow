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

from airflow.utils.helpers import exactly_one

try:
    from airflow.providers.standard.operators.bash import BashOperator
except ImportError:
    # Fallback for older Airflow versions
    from airflow.operators.bash import BashOperator  # type: ignore[no-redef]


def get_describe_pod_operator(
    cluster_name: str,
    *,
    pod_name: str | None = None,
    namespace: str | None = None,
) -> BashOperator:
    """Return an operator that prints ``kubectl describe pod(s)`` output in the Airflow logs.

    Exactly one of *pod_name* or *namespace* must be provided.

    :param cluster_name: Name of the EKS cluster
    :param pod_name: Describe a single pod by name
    :param namespace: List and describe *all* pods in the given namespace
    """
    if not exactly_one(pod_name, namespace):
        raise ValueError("Exactly one of 'pod_name' or 'namespace' must be provided.")

    if pod_name:
        kubectl_commands = f"""
                echo "***** pod description *****";
                kubectl describe pod {pod_name};"""
    else:
        kubectl_commands = f"""
                echo "***** pods in namespace {namespace} *****";
                kubectl get pods -n {namespace} -o wide;
                echo "***** pod descriptions *****";
                kubectl describe pods -n {namespace};"""

    return BashOperator(
        task_id="describe_pod",
        bash_command=f"""
                install_aws.sh;
                install_kubectl.sh;
                # configure kubectl to hit the right cluster
                aws eks update-kubeconfig --name {cluster_name};
                {kubectl_commands}
                """,
    )
