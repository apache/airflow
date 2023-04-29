#!/usr/bin/env python
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

import cassandra.type_codes as cassandra_type_codes

if __name__ == "__main__":
    print()
    path_to_patch = cassandra_type_codes.__file__
    with open(path_to_patch, "r+") as f:
        content = f.read()
        if "PYTEST_DONT_REWRITE" in content:
            print(f"The {path_to_patch} is already patched with PYTEST_DONT_REWRITE")
            print()
            exit(0)
        f.seek(0)
        content = content.replace('"""', '"""\nPYTEST_DONT_REWRITE', 1)
        f.write(content)
        f.truncate()
        print(f"Patched {path_to_patch} with PYTEST_DONT_REWRITE")
        print()
        exit(0)
