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

from __future__ import absolute_import

from airflow.upgrade.rules.base_rule import BaseRule
from airflow.configuration import conf


class PodTemplateFileRule(BaseRule):

    title = "Users must set a kubernetes.pod_template_file value"

    description = """\
In Airflow 2.0, KubernetesExecutor Users need to set a pod_template_file as a base
value for all pods launched by the KubernetesExecutor
"""

    def check(self):
        pod_template_file = conf.get("kubernetes", "pod_template_file", fallback=None)
        if not pod_template_file:
            return (
                "Please create a pod_template_file by running `airflow generate_pod_template`.\n"
                "This will generate a pod using your aiflow.cfg settings"
            )
