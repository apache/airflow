<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
-->

# Airflow OpenAPI clients

This directory contains definition of Airflow OpenAPI client packages.

Supported languages:

* [Python](https://github.com/apache/airflow-client-python) generated through `./gen/python.sh`.

## Generating client code

To generate the client code using dockerized breeze environment, run (at the Airflow source root directory):

```bash
breeze release-management prepare-python-client --distribution-format both
```

The client source code generation uses OpenAPI generator image, generation of packages is done using Hatch.
By default, packages are generated in a dockerized Hatch environment, but you can also use a local one by
setting `--use-local-hatch` flag.

```bash
breeze release-management prepare-python-client --distribution-format both --use-local-hatch
```

## Browsing the generated source code

The generated source code is not committed to Airflow repository, but when releasing the package, Airflow
team also stores generated client code in the
[Airflow Client Python repository](https://github.com/apache/airflow-client-python).
