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

# 4. A command reads from one explicit source, never a silent fallback

Date: 2026-07-20

## Status

Accepted

## Context

In Airflow 3 a Dag exists in two places at once: as author source in a bundle, and
as the serialized form in the metadata database that the scheduler and workers
execute. They can disagree — that is the point of Dag versioning — so a CLI command
inspecting a Dag must answer a question the user did not ask: *which am I looking
at?*

The tempting answer is "whichever is available" — try the database, fall back to
parsing local files, or the reverse. Reviewers refuse it because the fallback makes
output a function of ambient state the user cannot see. The scenario raised in
review: a contributor edits Dags locally and runs a command for import errors;
whether a SQLite database from a past `airflow standalone` exists in that directory
changes the answer, with nothing in the output saying which source produced it, and
when the two disagree the fallback reports the reachable answer rather than the true
one. The accepted shape: the plain command reads the database (the serialized form
that will actually run), and an argument naming local sources — a file or directory
— switches to local mode, so the user states the source rather than the tool
guessing. The same principle covers resolving *one* thing from a source that can
yield several: if a Dag has several serialized versions, the selection is made
explicit, not picked quietly.

## Decision

**Which source a command reads from is determined by its arguments, never by what
happens to be reachable at runtime.**

- **No source fallback.** A command that cannot reach its configured source fails
  with a clear error naming the source. It does not silently substitute another one.
- **Local-source arguments make local mode explicit.** An argument naming files or
  directories is what selects parsing from disk; without it, a Dag command reads the
  serialized form from the database.
- **Say which source produced the output** when a command can legitimately read
  from more than one — in the output itself for human formats, and as a field for
  machine-readable ones.
- **Resolve ambiguity explicitly.** When a lookup can match several rows — several
  serialized versions of one Dag, several runs for one identifier — the command takes
  an argument that picks one or reports all of them; it does not pick silently.
- **Adding a fallback is not a fix for a failing command.** A command failing
  against its proper source is a defect in that source path; fix it there.

## Consequences

- The same invocation against the same deployment gives the same answer, whatever
  else is in the working directory.
- Errors get louder: a command that used to "work" by falling back now fails and
  says why — the intended trade.
- Some workflows need an extra argument; the argument surface grows slightly rather
  than the space of states a user must reason about.

A change **violates** this decision when it:

- adds a `try the database, else parse local files` (or the reverse) path to a
  command;
- makes a command's result depend on whether a local database file happens to exist;
- silently selects one row when the lookup matched several, instead of taking an
  argument or reporting all matches;
- adds a fallback source to stop a command from failing, rather than fixing the
  path that failed;
- in a command that can read from **more than one** source, produces output that
  does not say which source it came from. (A command with a single source needs no
  such marker — its output is unambiguous by construction, and demanding
  provenance labelling everywhere would fire on most of the CLI.)

## Evidence

- #57321 — closed on this ground; converged on an explicit `--dag-path <file|dir>` implying local mode, bare command reads the database.
- #59038 — `connections test` failure addressed by a database fallback; closed unmerged.
- #60637 — `dags list` showing several entries for a Dag with several serialized versions: the one-name-to-many-rows ambiguity.
