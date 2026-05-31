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

## System Requirements

Building the UI requires at least 8GB of system RAM (6GB available).

If you encounter out-of-memory errors during the build, you can increase Node.js heap size:

```bash
export NODE_OPTIONS="--max-old-space-size=8192"
```

## Quick Start

With Breeze:
`breeze start-airflow --dev-mode`

Manually:

- Have the `DEV_MODE` environment variable set to `true` when starting airflow api-server
- Run `pnpm install && pnpm dev`
- Note: Make sure to access the UI via the Airflow localhost port (8080 or 28080) and not the vite port (5173)

## Dependency upgrade caveats

### `@chakra-ui/react` — held at `~3.34.0`

Do not relax this pin or bump `@chakra-ui/react` above `3.34.x` without
re-checking every dialog in this codebase that is mounted conditionally
(`{open ? <Dialog ... /> : undefined}`), e.g. `ClearRunButton`,
`MarkRunAsButton`, `ClearTaskInstanceButton`, `MarkTaskInstanceAsButton`.

`@chakra-ui/react@3.35.0` pulls in `@ark-ui/react@>=5.36.0`, where dialog
`pointer-events` cleanup moved from an inline style on the dialog DOM
to the dismissable layer's close-transition completion. Conditionally
mounted dialogs unmount before that transition runs, leaving the
`pointer-events: none` lock stuck on `document` — the page then refuses
every click (scroll still works) until a full refresh. See PR #67646
for the original revert and the timeline.

To bump safely, first rewrite the conditional-mount sites to always
render the dialog (and gate any expensive dry-run queries with
`enabled: open`) so the dialog can drive its own close transition.

## More

See [node environment setup docs](/contributing-docs/15_node_environment_setup.rst)
