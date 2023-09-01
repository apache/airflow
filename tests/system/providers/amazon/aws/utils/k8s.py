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

from typing import TYPE_CHECKING

from airflow.operators.bash import BashOperator

if TYPE_CHECKING:
    from airflow.models.operator import Operator


def get_describe_pod_operator(cluster_name: str, pod_name: str) -> Operator:
    """Returns an operator that'll print the output of a `k describe pod` in the airflow logs."""
    return BashOperator(
        task_id="describe_pod",
        bash_command=f"""
                install_aws.sh;
                install_kubectl.sh;
                # configure kubectl to hit the right cluster
                aws eks update-kubeconfig --name {cluster_name};
                # once all this setup is done, actually describe the pod
                echo "vvv pod description below vvv";
                kubectl describe pod {pod_name};
                echo "^^^ pod description above ^^^" """,
    )
