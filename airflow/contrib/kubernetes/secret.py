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


class Secret:
    """Defines Kubernetes Secret Volume"""

    def __init__(self, deploy_type, deploy_target, secret, key):
        """Initialize a Kubernetes Secret Object. Used to track requested secrets from
        the user.

        :param deploy_type: The type of secret deploy in Kubernetes, either `env` or
            `volume`
        :type deploy_type: ``str``
        :param deploy_target: The environment variable to be created in the worker.
        :type deploy_target: ``str``
        :param secret: Name of the secrets object in Kubernetes
        :type secret: ``str``
        :param key: Key of the secret within the Kubernetes Secret
        :type key: ``str``
        """
        self.deploy_type = deploy_type
        self.deploy_target = deploy_target.upper()
        self.secret = secret
        self.key = key
