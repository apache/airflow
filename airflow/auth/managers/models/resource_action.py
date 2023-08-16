#
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

from enum import Enum


class ResourceAction(Enum):
    """
    Define the type of action/operation the user is doing.

    This is used when doing authorization check to define the type of action/operation
    the user is doing.
    """

    # Create a resource
    POST = "POST"
    # Read a resource
    GET = "GET"
    # Update a resource
    PUT = "PUT"
    # Delete a resource
    DELETE = "DELETE"
