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

# Per-user configuration for apache-steward

This file is committed in the adopter repo and holds preferences
that vary per developer (GitHub handle, local clone paths, optional
tool backends). It is **not** project-specific — those facts live in
`<project-config>/project.md`. Fill in the fields that apply to your
setup; the skills skip any block that is missing or marked `TODO`.

## `role_flags`

- `pmc_member: TODO` — set to `true` if you are a PMC member of the
  adopting project. Used by `security-cve-allocate` to decide whether
  you can submit the CVE allocation form directly or need to relay
  the request to a PMC member.

## `environment`

- `upstream_clone: TODO` — absolute path to your local clone of the
  public `<upstream>` repo. Used by `security-issue-fix` when it
  writes changes and opens PRs. The skill validates that the clone
  has a remote pointing at your fork before proceeding.
- `upstream_fork_remote: TODO` — name of the git remote that points
  at your personal fork (e.g. `fork`, `your-github-handle`). If
  omitted, the skill uses the first non-`origin` remote that looks
  like a fork. Explicitly setting this avoids ambiguity when you
  have multiple remotes.

## `tools`

### `ponymail`

- `enabled: false` — set to `true` if you have registered the
  PonyMail MCP in your Claude Code `mcpServers` block. When enabled
  and authenticated, the security skills use PonyMail as the primary
  read backend for mailing-list archive queries; Gmail remains the
  fallback for just-arrived inbound mail and the only backend for
  draft composition.
- `private_lists: []` — list of private mailing-list addresses that
  PonyMail should query (e.g. `["security@<project>.apache.org"]`).
  Only used when `enabled: true`.
