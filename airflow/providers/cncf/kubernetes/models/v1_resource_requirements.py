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

from typing import Sequence

from kubernetes.client import models as k8s


class V1ResourceRequirements(k8s.V1ResourceRequirements):
    """

    Creates V1ResourceRequirements for templating:

    .. seealso::
      Templates limits and requests to calibrate cpu and memory usage

    :param limits: Key value pairs of config. Refer parent class.
    :param requests: Key value pairs of config. Refer parent class.
    :param local_vars_configuration: Configuration of type
    Kubernetes.Client.Configuration class

    """

    template_fields: Sequence[str] = (
        'limits',
        'requests',
    )
