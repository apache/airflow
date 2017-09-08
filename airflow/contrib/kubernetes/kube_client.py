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


def get_kube_client(in_cluster=True):
    # TODO: This should also allow people to point to a cluster.

    from kubernetes import config, client

    if in_cluster:
        config.load_incluster_config()
        return client.CoreV1Api()
    else:
        NotImplementedError("Running kubernetes jobs from not within the cluster is not supported at this time")
