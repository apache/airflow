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

# 3. The airflowctl CLI surface and its output are a stable contract

Date: 2026-07-19

## Status

Accepted

## Context

`airflowctl` is driven by humans at a terminal and, just as often, by scripts and
CI pipelines that shell out to it and parse its output. That makes the *shape* of
the CLI — the command/subcommand tree, flag names, required-vs-optional arity, and
`--output` formats — a contract, like an API's request/response shapes. The tree
is registered declaratively in `ctl/cli_parser.py` + `ctl/cli_config.py`, handlers
live in `ctl/commands/…`, help text is data in `ctl/help_texts.yaml`, and
`ctl/console_formatting.py` (`AirflowConsole`) renders `json`/`yaml`/`table`/`plain`.

A change that reads as a harmless tidy-up — renaming a command, flipping an option
to positional, renaming a flag, altering an output shape — silently breaks every
script on the old surface, invisibly, because the consumers are not in the repo.
The `json`/`yaml` modes are a machine contract in particular: anything that leaks a
log line or warning into the stdout payload corrupts a downstream parser. Unlike
airflow-core, this distribution is **released from `main`** and does **not** consume
newsfragments; surface changes are recorded in `airflow-ctl/RELEASE_NOTES.rst`.

## Decision

The CLI surface and its machine-readable output are treated as a stable,
backward-compatibility-sensitive contract:

- **Command names, flags, and argument arity are stable.** Renaming a command,
  changing a flag, or flipping an argument between positional and `--flag` is a
  breaking change that must be justified and recorded in
  `airflow-ctl/RELEASE_NOTES.rst`. The established convention is required
  parameters positional, optional parameters as `--flag`.
- **`--output` shapes are honoured and machine-parseable for every mode**
  (`json` / `yaml` / `table` / `plain`). Data goes to stdout; logs, warnings,
  and diagnostics go to stderr, so the `json`/`yaml` payload stays parseable.
- **Flag names and behaviour stay consistent across related commands**, and help
  text (`ctl/help_texts.yaml`) matches what the command actually does.
- **User-facing surface changes are documented in `airflow-ctl/RELEASE_NOTES.rst`**,
  not in a newsfragment — this distribution does not use newsfragments.

## Consequences

Operators and automation can pin to `airflowctl`'s commands and output shapes
across releases, and the `json`/`yaml` modes stay safe to parse. Surface changes
carry real process cost — justification plus a RELEASE_NOTES entry — intentional
friction that protects the scripts wrapped around the tool: fixing an awkward name
or flag is a deliberate, documented, compatibility-aware change, not a free cleanup.

A change **violates** this decision when it:

- renames a command/subcommand, renames a flag, or flips an argument between
  positional and `--flag` as an incidental cleanup, without treating it as a
  backward-compatibility-sensitive change and noting it in
  `airflow-ctl/RELEASE_NOTES.rst`;
- changes or ignores a `--output` format, or leaks logs/warnings into the
  `json`/`yaml` payload on stdout so a downstream parser breaks;
- makes flag names or behaviour inconsistent across related commands, or lets
  help text claim a capability the command does not implement;
- records a user-facing surface change in a `newsfragments/` entry instead of
  `airflow-ctl/RELEASE_NOTES.rst`.

## Evidence

- #66768 — makes required CLI params positional, optional as `--flag`: establishes
  the arity convention the surface then holds stable.
- #62665 — `pools export` ignoring `--output table/yaml/plain` broke the output
  contract and was fixed.
- #68522 — `assets list-by-alias` → `list-aliases`: a deliberate, documented rename
  handled as breaking, not slipped in.
- #65608 — an optional flag (`--state`) must keep the command working when unset.
- #65052 — exit codes are part of the machine contract scripts depend on.
