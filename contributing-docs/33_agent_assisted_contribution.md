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

# Agent-assisted contribution (apache-magpie)

This repo uses the [`apache/magpie`](https://github.com/apache/magpie)
framework, installed from its plugin marketplace. The framework provides
maintainer-facing PR-management skills (`pr-management-triage`,
`pr-management-code-review`, `pr-management-stats`, `pr-management-mentor`)
that are exposed as agent skills in agent harnesses such as Claude Code.

Nothing framework-related is committed to this repo, and a fresh clone
needs no setup step — the plugin is installed per-user in your own agent
harness. In Claude Code:

```text
/plugin marketplace add apache/magpie
/plugin install magpie-pr-management@apache-magpie
```

Install `magpie@apache-magpie` instead to get every skill family at once,
or add further families (`magpie-security`, `magpie-release-management`,
…) one at a time. Pin a release rather than tracking `main` by adding the
marketplace from a tag: `/plugin marketplace add apache/magpie@0.2.0`.

The project's recommended set is recorded in `.apache-magpie.lock` and
includes the release-management family, which lets an agent verify a
release candidate — including an optional check that runs your own
changes against a providers wave installed in Breeze:

```text
/plugin install magpie-release-management@apache-magpie
```

Other harnesses — Codex CLI, Gemini CLI, Copilot, Cursor — are covered in
the framework's [marketplace
guide](https://github.com/apache/magpie/blob/main/docs/setup/marketplaces.md).

Airflow-specific modifications to framework workflows live in
[`.apache-magpie-overrides/`](../.apache-magpie-overrides/) (committed) —
the installed plugin reads them at run time, so they apply to every
contributor without anyone editing the framework. Framework changes go via
PR to [`apache/magpie`](https://github.com/apache/magpie).
