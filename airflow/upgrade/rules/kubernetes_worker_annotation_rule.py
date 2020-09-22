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

from airflow import conf
from airflow.upgrade.rules.base_rule import BaseRule


class KubernetesWorkerAnnotationsRule(BaseRule):
    title = "kubernetes_annotations configuration section has been removed"

    description = ("A new key pod_template_file has been added in the kubernetes section. "
                   "It should point to the YAML pod configuration file. This YAML pod file is "
                   "where you add the kubernetes annotations key value pairs"
                   )

    def check(self):
        kub_annotations = conf.getsection('kubernetes_annotations')
        if kub_annotations:
            pairs = "\n".join(["%s = %s" % kv for kv in kub_annotations.items()])
            msg = ("These settings:\n"
                   "\n[kubernetes_annotaions]\n"
                   "{pairs}\n\n"
                   "Should be in your YAML pod configuration file").format(pairs=pairs)
            return [msg]
