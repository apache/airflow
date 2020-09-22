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

import json

from airflow import conf
from airflow.upgrade.rules.base_rule import BaseRule


class KubernetesWorkerAnnotationsRule(BaseRule):

    title = "kubernetes_annotations configuration section has been removed"

    description = ("A new key worker_annotations has been added to existing kubernetes section instead. "
                   "That is to remove restriction on the character set for k8s annotation keys. "
                   "All key/value pairs from kubernetes_annotations should now go to worker_annotations "
                   "as a json."
                   )

    def check(self):
        kub_annotations = conf.getsection('kubernetes_annotations')
        if kub_annotations:
            annotations_json = json.dumps(kub_annotations)
            pairs = "\n".join(["%s = %s" % kv for kv in kub_annotations.items()])

            return [
                "For example:\n"
                "\n"
                "[kubernetes_annotations]\n"
                '{pairs}'
                "\n"
                "Should be written as:\n"
                "\n"
                "[kubernetes]\n"
                'worker_annotations = {annotations_json}'.format(pairs=pairs,
                                                                 annotations_json=annotations_json)
            ]
