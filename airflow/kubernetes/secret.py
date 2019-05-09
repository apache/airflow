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
from airflow.exceptions import AirflowConfigException


class Secret:
    """Defines Kubernetes Secret Volume"""

    def __init__(self, deploy_type, deploy_target, secret, key=None):
        """Initialize a Kubernetes Secret Object. Used to track requested secrets from
        the user.
        :param deploy_type: The type of secret deploy in Kubernetes, either `env` or
            `volume`
        :type deploy_type: str
        :param deploy_target: (Optional) The environment variable when
            `deploy_type` `env` or file path when `deploy_type` `volume` where
            expose secret. If `key` is not provided deploy target should be None.
        :type deploy_target: str or None
        :param secret: Name of the secrets object in Kubernetes
        :type secret: str
        :param key: (Optional) Key of the secret within the Kubernetes Secret
            if not provided in `deploy_type` `env` it will mount all secrets in object
        :type key: str or None
        """
        self.deploy_type = deploy_type
        self.deploy_target = deploy_target

        if deploy_target is not None and deploy_type == 'env':
            # if deploying to env, capitalize the deploy target
            self.deploy_target = deploy_target.upper()

        if key is not None and deploy_target is None:
            raise AirflowConfigException(
                'If `key` is set, `deploy_target` should not be None'
            )

        self.secret = secret
        self.key = key

    def __eq__(self, other):
        return (
            self.deploy_type == other.deploy_type and
            self.deploy_target == other.deploy_target and
            self.secret == other.secret and
            self.key == other.key
        )

    def __repr__(self):
        return 'Secret({}, {}, {}, {})'.format(
            self.deploy_type,
            self.deploy_target,
            self.secret,
            self.key
        )
