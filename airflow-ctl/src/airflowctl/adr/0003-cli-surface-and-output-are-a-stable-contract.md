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

`airflowctl` is driven by humans at a terminal and, just as often, by scripts,
CI pipelines, and automation that shell out to it and parse its output. That
makes the *shape* of the CLI — the command/subcommand tree, the flag names, the
required-vs-optional argument arity, and the `--output` formats — a contract, in
the same way an API's request/response shapes are. The command tree is
registered declaratively in `ctl/cli_parser.py` + `ctl/cli_config.py`, the
handlers live in `ctl/commands/…`, help text is data in `ctl/help_texts.yaml`,
and `ctl/console_formatting.py` (`AirflowConsole`) renders results as `json`,
`yaml`, `table`, or `plain`.

A change that reads as a harmless tidy-up in a diff — renaming a command,
flipping an option to a positional argument, renaming a flag, or altering the
`json`/`yaml`/`table` output shape — silently breaks every script built on the
old surface. The breakage is invisible in review because the consumers are not
in the repository. The `json`/`yaml` modes in particular are a machine
contract: anything that leaks a log line or warning into the payload on stdout
corrupts a downstream parser.

Unlike airflow-core, this distribution is **released from `main`** and does
**not** consume newsfragments; its release manager regenerates the changelog and
user-facing notes are recorded in `airflow-ctl/RELEASE_NOTES.rst`. So the
place a surface change is announced is the RELEASE_NOTES, not a
`newsfragments/` entry.

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
across releases without rework, and the `json`/`yaml` modes stay safe to parse.
Surface changes carry real process cost — justification plus a RELEASE_NOTES
entry — which is intentional friction that protects the scripts wrapped around
the tool. The trade-off is that fixing an awkward command name or flag is not a
free cleanup; it is a deliberate, documented, compatibility-aware change.

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

- #66768 — "airflowctl: make required CLI params positional, keep optional as
  --flag": establishes the argument-arity convention that the surface then holds
  stable.
- #62665 — "Fix `airflowctl pools export` ignoring --output table/yaml/plain": a
  command that dropped an `--output` mode broke the output contract and was
  fixed.
- #68522 — "Rename assets list-by-alias to list-aliases in airflowctl": a
  deliberate, documented command rename — the kind of surface change that must
  be handled as breaking, not slipped in.
- #65608 — "Fix airflowctl dagrun list crash when --state is omitted": an
  optional flag must keep the command working when unset, not crash.
- #65052 — "Fix CLI error handling and exit codes for failed commands": exit
  codes are part of the machine contract scripts depend on.
