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

<!-- SPDX-License-Identifier: Apache-2.0 -->

# Shared search logging code

This private distribution contains the response wrappers and timestamp formatter
used by the Elasticsearch and OpenSearch providers. It is not published to PyPI.
Each provider bundles the source under its own `_shared/search` namespace, so
the providers can be installed and released independently.

The response code handles the Elasticsearch/OpenSearch hit format. Client setup,
queries, and Airflow's JSON formatter remain in the providers. The timestamp
formatter uses the standard logging interface and has no Airflow imports.

Consumers declare `apache-airflow-shared-search` in
`tool.airflow.shared_distributions`, symlink the source into their `_shared`
directory, and include it in their source archives using Hatch's `force-include`.
See [the shared library guide](../README.md) for the packaging mechanism.
