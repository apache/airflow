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

from uuid import NAMESPACE_OID, UUID, uuid5

NIL_UUID = UUID(int=0)


def generate_uuid(*values: str | None, namespace: UUID = NAMESPACE_OID) -> str:
    """
    Convert input values to deterministic UUID string representation.

    This function is only intended to generate a hash which used as an identifier, not for any security use.

    Generates a UUID v5 (SHA-1 + Namespace) for each value provided,
    and this UUID is used as the Namespace for the next element.

    If only one non-None value is provided to the function, then the result of the function
    would be the same as result of ``uuid.uuid5``.

    All ``None`` values are replaced by NIL UUID.  If it only one value is provided then return NIL UUID.

    :param namespace: Initial namespace value to pass into the ``uuid.uuid5`` function.
    """
    if not values:
        raise ValueError("Expected at least 1 argument")

    if len(values) == 1 and values[0] is None:
        return str(NIL_UUID)

    result = namespace
    for item in values:
        result = uuid5(result, item if item is not None else str(NIL_UUID))

    return str(result)
