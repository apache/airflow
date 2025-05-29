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
from __future__ import annotations

import rich
from airflow.utils.platform import get_airflow_git_version
from airflow.version import version
from airflowctl.api.client import NEW_API_CLIENT, ClientKind, provide_api_client
from airflowctl.api.operations import VersionOperations
#from airflow.api_fastapi.core_api.routes.public.version import get_version
#from airflow.api_fastapi.core_api.routes.public.version import VersionInfo

@provide_api_client(kind=ClientKind.CLI)
def version_info(arg, api_client=NEW_API_CLIENT):
    """Get version information."""
    #version_response = api_client.version.get()
    version_response = VersionOperations(api_client).get()
    version_dict = version_response.model_dump()
    version_info = {}
    version_info["airflow_version"] = version_dict["version"]
    version_info["git_version"] = version_dict["git_version"]
    version_info["airflowctl_version"] = version_dict["version"]

    rich.print(version_info)

    