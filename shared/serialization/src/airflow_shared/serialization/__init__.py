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

# Current serialization format keys
CLASSNAME = "__classname__"
VERSION = "__version__"
DATA = "__data__"
SCHEMA_ID = "__id__"
CACHE = "__cache__"

# Legacy serialization format keys (processed by serde for backwards compatibility)
OLD_TYPE = "__type"
OLD_SOURCE = "__source"
OLD_DATA = "__var"
OLD_DICT = "dict"

FORBIDDEN_XCOM_KEYS = frozenset(
    {
        CLASSNAME,
        VERSION,
        DATA,
        SCHEMA_ID,
        CACHE,
        OLD_TYPE,
        OLD_SOURCE,
        OLD_DATA,
    }
)
