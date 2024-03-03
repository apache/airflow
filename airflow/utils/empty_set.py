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


def _get_empty_set_for_configuration() -> set[tuple[str, str]]:
    """
    Retrieve an empty_set_for_configuration.

    This method is only needed because configuration module has a deprecated method called set, and it
    confuses mypy. This method will be removed when we remove the deprecated method.

    :meta private:
    :return: empty set
    """
    return set()
