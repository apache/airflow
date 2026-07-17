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

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Airflow plugin integration](#airflow-plugin-integration)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Airflow plugin integration

- Preserve the library build and the default component export from `src/main.tsx`; Airflow loads the
  generated bundle dynamically.
- Keep React, React DOM, React Router, and the JSX runtime external in `vite.config.ts`. They are
  shared with the Airflow host application and bundling another copy can break hooks or routing.
- Keep the `AirflowPlugin` UMD global name unless the host integration changes with it.
- Use `globalThis.ChakraUISystem` in production and retain the local theme fallback for development.
- Build UI with Chakra components and semantic theme tokens. Avoid raw color values and styling that
  bypasses the inherited Airflow theme.
- Integrate through Airflow's documented public plugin and REST API surfaces. Do not import private
  modules from the Airflow Core UI into the plugin bundle.
- Treat the React plugin interface as experimental and verify compatibility when upgrading external
  dependencies shared with Airflow.
