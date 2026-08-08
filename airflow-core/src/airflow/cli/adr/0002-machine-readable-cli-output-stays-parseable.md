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

# 2. Machine-readable CLI output stays parseable

Date: 2026-07-18

## Status

Accepted

## Context

Airflow CLI commands support `-o json` and `-o yaml` for structured output.
That output is routinely consumed by scripts, CI pipelines, and other
tooling, which pipe stdout straight into a JSON/YAML parser. Anything other
than the serialized payload written to stdout — a log line, a deprecation
warning, an interactive prompt, a progress message — corrupts the stream and
breaks the parser, often intermittently and hard to diagnose.

Input validation has the same discipline requirement: the set of legal values
for a flag (such as the output format itself) must be rejected at the
argparse layer with `choices=`, so an invalid invocation fails fast with a
clear usage error instead of failing deep inside a handler after partial work
and partial output.

## Decision

- For any command run with `-o` structured output, only the JSON/YAML payload
  is written to stdout. Nothing else goes to stdout in that mode.
- Logs, warnings, prompts, and diagnostics are written to stderr, keeping
  stdout a clean, parseable channel.
- Validate CLI input at the argparse layer using `choices=` (and equivalent
  argparse constraints) rather than validating deep inside the command
  handler.

## Consequences

Consumers can pipe `-o json`/`-o yaml` output directly into a parser without
stripping stray lines, and errors surface as usage failures before any
partial payload is emitted. Human-readable diagnostics remain visible on
stderr, so nothing is lost — it is just kept off the data channel.

A VIOLATING change looks like: a command that writes a log line, warning, or
interactive prompt to stdout while an `-o json`/`-o yaml` format is active; a
handler that emits progress or informational text to stdout instead of
stderr; or input validation that lives deep in a command handler (raising
after work has begun) instead of being expressed as an argparse `choices=`
constraint at parse time.

## Evidence

- #68598 — route Airflow CLI logs to stderr for `-o` commands so structured output stays parseable.
- #63365 — structured JSON logging for all API server output, keeping log lines off the data stream.
- #67214 — mTLS / private-CA support added with format and connection options validated at the argparse layer.
