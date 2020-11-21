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

import collections
import json
import sys

import jsonschema
import yaml


def main() -> int:
    argv = sys.argv[1:]

    schemas = collections.defaultdict(list)
    schema = None

    for arg in argv:
        if ".schema" in arg:
            schema = arg
        else:
            if schema is None:
                raise ValueError(f"No schema specified for {arg}")
            schemas[schema].append(arg)

    for schema, files in schemas.items():
        for file in files:
            with open(schema) as s:
                schema_content = json.load(s)
            with open(file) as f:
                try:
                    file_contents = json.load(f)
                except json.decoder.JSONDecodeError:
                    f.seek(0)
                    file_contents = yaml.safe_load(f)
                jsonschema.validate(file_contents, schema_content)

    return 0


if __name__ == "__main__":
    sys.exit(main())
