#
# Licensed to the Apache Software Foundation (ASF) under one or more
# TODO: This license is not consistent with license used in the project.
#       Delete the inconsistent license and above line and rerun pre-commit to insert a good license.
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


class ClientConfig:
    """
    Client side configuration of Zeppelin SDK
    """

    def __init__(self, zeppelin_rest_url, query_interval=1, knox_sso_url=None):
        self.zeppelin_rest_url = zeppelin_rest_url
        self.query_interval = query_interval
        self.knox_sso_url = knox_sso_url

    def get_zeppelin_rest_url(self):
        return self.zeppelin_rest_url

    def get_query_interval(self):
        return self.query_interval
