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


class InitContainer:
    """
    Defines Kubernetes InitContainer
    """

    def __init__(
            self,
            name,
            image,
            security_context=None,
            init_environment=None,
            volume_mounts=None,
            cmds=None,
            args=None):
        """ Adds Kubernetes InitContainer to pod.
        :param name: the name of the init-container
        :type name: str
        :param image: The docker image for the init-container
        :type image: str
        :param init_environment: A dict containing the environment variables
        :type init_environment: dict
        :param cmds: The command to be run on the init-container
        :type cmds: list[str]
        :param args: The arguments for the command to be run on the init-container
        :type args: list[str]
        """
        self.name = name
        self.image = image
        self.security_context = security_context
        self.init_environment = init_environment or []
        self.volume_mounts = volume_mounts or []
        self.cmds = cmds
        self.args = args or []
