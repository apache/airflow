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

import yaml
from kubernetes import client, config


class KubernetesHelper(object):
    def __init__(self):
        config.load_incluster_config()
        self.job_api = client.BatchV1Api()
        self.pod_api = client.CoreV1Api()

    def launch_job(self, pod_info, namespace):
        dep = yaml.load(pod_info)
        resp = self.job_api.create_namespaced_job(body=dep, namespace=namespace)
        return resp

    def get_status(self, pod_id, namespace):
        return self.job_api.read_namespaced_job(pod_id, namespace).status

    def delete_job(self, job_id, namespace):
        body = client.V1DeleteOptions()
        self.job_api.delete_namespaced_job(name=job_id, namespace=namespace, body=body)
