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

# Contributing to the UI

## Quick Start

With Breeze:
`breeze start-airflow --dev-mode`

Manually:

- Have the `DEV_MODE` environment variable set to `true` when starting airflow api-server
- Run `pnpm install && pnpm dev`
- Note: Make sure to access the UI via the Airflow localhost port (8080 or 28080) and not the vite port (5173)

## More

See [node environment setup docs](/contributing-docs/15_node_environment_setup.rst)
