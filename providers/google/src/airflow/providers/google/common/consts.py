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

from google.api_core.gapic_v1.client_info import ClientInfo

from airflow import version

GOOGLE_DEFAULT_DEFERRABLE_METHOD_NAME = "execute_complete"

CLIENT_INFO = ClientInfo(client_library_version="airflow_v" + version.version)

PUBSUB_RETURN_IMMEDIATELY_DEPRECATION_MESSAGE = (
    "`return_immediately` defaults to True, which relies on the deprecated Pub/Sub "
    "`returnImmediately` Pull option and can return zero messages while a backlog exists. "
    "The default will change to False in the first Google provider major release after "
    "March 31, 2027. Pass `return_immediately=False` to adopt the new behaviour now, "
    "or `return_immediately=True` to keep the current one."
)
