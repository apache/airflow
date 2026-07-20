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

# 3. Generated artifacts are regenerated, never hand-edited

Date: 2026-07-20

## Status

Accepted

## Context

A large amount of what is committed under `dev/` is machine-generated and
checked in only so that it can be reviewed and diffed. `dev/breeze/doc/images/`
alone holds hundreds of files: an `output_*.svg` rendering of each breeze
command's help, plus a matching `output_*.txt` hash file used to detect
staleness. Provider dependency metadata and generated provider documentation are
in the same category, produced from `pyproject.toml` files rather than authored.

These artifacts exist to make generator changes visible in review. That only
works if the committed content is *exactly* what the generator produces. A
hand-edited file passes local review — it looks like a plausible help text — and
then fails the `update-breeze-cmd-output` prek hook for everyone else, or worse,
survives and hides the fact that the generator and the committed output have
diverged. At that point the artifact is no longer evidence of anything.

Regeneration also has to be *deterministic*, and this is where the subtler
failures live. If a generator's output depends on the machine, on a cache, or on
some varying external input, then two contributors regenerating the same
artifact produce different files, the hook flaps, and people start committing
whatever their machine produced to make it stop. Airflow has hit exactly this
with variability in generated command documentation, and has had to clean up
generated metadata that was mistakenly committed by hand.

The related trap is regenerating from the *wrong sources*. Breeze runs from the
current git worktree's sources (see
`dev/breeze/doc/adr/0017-use-uvx-to-run-breeze-from-local-sources.md`), but a
cached or globally installed breeze will happily regenerate images from stale
code — producing a diff that does not match what CI computes from the branch.

## Decision

- Committed generated artifacts are produced only by running their generator.
  They are never edited by hand, and never partially edited to "fix" a diff.
- The generator is the source of truth. When committed output is wrong,
  the fix goes into the generator, and the artifact is regenerated.
- Generators produce deterministic output: the same sources yield byte-identical
  artifacts on any contributor's machine. Non-determinism is treated as a defect
  in the generator, not as noise to be committed around.
- Regeneration runs against the pull request's own sources — the breeze in the
  current worktree — not a cached or globally installed copy.
- The prek hook that checks an artifact (for breeze command output,
  `update-breeze-cmd-output`) is the arbiter of staleness; a pull request that
  changes a command's options, groups or help text regenerates the artifact in
  the same change.

## Consequences

Generated files stay trustworthy as review evidence: a diff in an `output_*.svg`
means a command's interface actually changed, and its absence means it did not.
Contributors do not have to reconcile spurious regeneration diffs, and the
staleness hooks stay meaningful rather than becoming noise people learn to
override. The cost is that changing a breeze option requires running the
regeneration step — and running it correctly, from local sources — which is an
extra loop for what can look like a one-word help-text edit.

A change *violates* this decision when it:

- edits a committed generated artifact directly — an `output_*.svg`, its
  `output_*.txt` hash file, generated provider dependency metadata, or generated
  provider documentation — instead of regenerating it;
- changes a breeze command's options, option groups or help text without
  regenerating the corresponding command output in the same pull request;
- commits generated output that differs between machines, or adjusts a committed
  artifact to silence a flapping staleness hook instead of fixing the
  non-determinism in the generator;
- regenerates from a cached or globally installed breeze rather than the current
  worktree's sources, so the committed artifact does not match what CI produces;
- introduces a generated artifact with no generator command and no staleness
  check, leaving it to drift silently.

## Evidence

- #63641 — fixed LLM model list variability in generated command documentation;
  the generator was made deterministic rather than the varying output committed.
- #68801 — removed `generated/provider_dependencies.json*` files that had been
  mistakenly added by hand.
- #68991 — fixed an inconsistency between generated provider docs and
  `pyproject.toml`, correcting the generation path rather than the output.
- #50986 — fixed the airflowctl pre-commit hook for command images after
  generated images and their check drifted apart.
- #65873 — established that breeze runs from the current worktree's sources,
  which is what makes "regenerate from the PR's own sources" well-defined.
