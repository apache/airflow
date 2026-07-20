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

In Airflow 3 a Dag exists in two places at once: as author source inside a bundle,
and as the serialized form in the metadata database that the scheduler and workers
actually execute. Those two can disagree — that is not a bug, it is the whole point
of Dag versioning. A CLI command that inspects a Dag must therefore answer a
question the user did not ask: *which of the two am I looking at?*

The tempting answer is "whichever is available". A command tries the database,
finds nothing useful, and falls back to parsing local files — or the reverse. It
works on the maintainer's machine, it works in the reporter's reproduction, and it
is the design that reviewers refuse.

The reason is that the fallback makes the command's output a function of ambient
state the user cannot see. The concrete scenario raised in review: a contributor
edits Dags in a local checkout and runs a Dag command to look for import errors.
Sometimes they have run `airflow standalone` in that directory and a SQLite
database exists with serialized Dags in it; sometimes they have not. The same
command, the same files, two different answers, and no signal in the output saying
which source produced it. When the two sources disagree — stale serialized rows,
an edit not yet parsed — the fallback reports the answer that happens to be
reachable rather than the answer that is true.

The accepted shape came out of that same discussion and is explicit: the plain
command reads the database (the serialized form the system will actually run), and
an argument naming local sources — a file or directory — switches it to reading
those, implying local mode rather than requiring the user to remember a second
flag. The user states the source; the tool does not guess it.

The same principle covers a command that resolves *one* logical thing from a
source that can yield several. If a Dag has several serialized versions, "show the
Dag" is under-specified, and the fix is to make the selection explicit rather than
to quietly pick one.

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

- The same invocation against the same deployment produces the same answer, whatever
  else is lying around in the working directory.
- Errors get louder: a command that used to "work" by falling back now fails, and
  says why. That is the intended trade.
- Some workflows need an extra argument they did not need before, and the argument
  surface grows slightly — accepted, because the alternative grows the space of
  states a user has to reason about instead.

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

- #57321 — "Allow running Dag-related CLI commands without DB": closed precisely on
  this ground. Reviewers noted the command would return different results depending
  on the state of the local SQLite database, and converged instead on an explicit
  `--dag-path <file|dir>` that implies local mode, leaving the bare command reading
  the serialized form from the database.
- #59038 — a `connections test` failure addressed by adding a database fallback;
  closed unmerged rather than landing the fallback.
- #60637 — `dags list` showing several entries for a Dag with several serialized
  versions: an ambiguity that surfaces when a command resolves one name to many rows.
