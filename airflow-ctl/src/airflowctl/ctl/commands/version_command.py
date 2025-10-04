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

from airflowctl import __version__ as airflowctl_version
from airflowctl.api.client import NEW_API_CLIENT, ClientKind, provide_api_client


@provide_api_client(kind=ClientKind.CLI)
def version_info(arg, api_client=NEW_API_CLIENT):
    """Get version information."""
    version_dict = {"airflowctl_version": airflowctl_version}
    if arg.remote:
        version_response = api_client.version.get()
        version_dict.update(version_response.model_dump())
        rich.print(version_dict)
    else:
        rich.print(version_dict)
