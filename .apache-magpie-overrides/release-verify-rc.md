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

# Override: release-verify-rc — optional contributor check of your own changes

## What this overrides

Adds an **optional** step to the `release-verify-rc` skill for Airflow provider
waves: after (or instead of) the artefact checks, verify that every change the
person running the skill authored is in the release candidate, and let them test
those changes against the RC distributions running in Breeze. Run it when the user
asks to "check my changes in the RC", "test my changes in the providers release",
"tick my boxes", or similar. It is not part of the mechanical PASS / FAIL verdict
and never replaces it.

Everything below is read-only on GitHub except the two steps marked
**confirm first**, which follow the draft-then-confirm rule in
[`airflow-pr-draft-summary`](../.agents/skills/airflow-pr-draft-summary/SKILL.md).

## Step A — Find the wave and the user's entries

1. Find the testing issue: `gh issue list -R apache/airflow --search "Status of testing
   Providers in:title" --state all --limit 3`. The newest open one is the wave; the
   previous one is needed in step 3.
2. From the issue body, take the checkbox lines that **end** in `: @<handle>` — those
   are the user's PRs. Lines under `Linked issues:` that mention the user are issues
   they reported on someone else's PR, not their boxes.
3. List the user's merged PRs touching `providers/` since the previous wave
   (`gh pr list --author <handle> --state merged --search "merged:<prev>..<this>"`),
   ignoring `[v3-*-test]` backports — providers are released from `main`. Any PR not in
   the current issue must be either in the previous issue (already released) or in a
   provider of this wave that has no section in the issue — report the latter, since
   it has no box to tick.

## Step B — Verify each change is in the RC

The wave's RC tags are `providers-<name>/<version>rcN` created on the preparation
date. For every (PR, provider) pair confirm all three, and report any gap:

- the PR's squash commit on `upstream/main` is an ancestor of the RC tag
  (`git merge-base --is-ancestor`);
- `providers/<path>/docs/changelog.rst` at the tag mentions `#<PR>`;
- the RC **wheel from PyPI** contains the changed code — download it with
  `pip download --no-deps` and look for a distinctive added line inside the wheel
  (`zipfile`). Docs-only and test-only PRs have nothing in the wheel; say so.

Git refuses shell loops that call `git` in some agent sandboxes; a small Python
script calling `git` via `subprocess` works.

## Step C — Install the whole wave from files

1. Make sure `dist/` of the **checkout Breeze will run from** is empty, then download
   every RC distribution of the wave plus the latest released `apache-airflow` and
   `apache-airflow-core` into it (`pip download --no-deps --only-binary=:all: --dest
   dist -r <requirements>`).
2. Write one verification Dag to `files/dags/` (gitignored; Breeze's Dags folder),
   with one task per changed behaviour. Each task exercises the changed code path
   through the RC distribution, replaces external services with `unittest.mock`
   (no real credentials), prints what it checked, and raises on the old behaviour.
   Skip — do not fail — a task whose optional extra is not installed. Run `ruff format`
   and `ruff check` on it.
3. Hand the user the command to run **from that same checkout** — `breeze start-airflow`
   needs Docker and an interactive terminal, so the agent does not run it:

   ```bash
   breeze start-airflow --mount-sources remove --use-distributions-from-dist \
     --use-airflow-version wheel --python 3.10 --backend postgres --load-default-connections
   ```

4. Show the user a table of the tasks and what each log should contain, plus any
   change they can additionally check by hand in the UI.

Known pitfalls: `No airflow package found` means `dist/` of the checkout Breeze runs
from is empty (e.g. it was started from the main checkout instead of a worktree) or
`DISTRIBUTION_FORMAT` is set to `sdist`. A Docker build failing with
`error getting credentials … "pass": executable file not found` is the user's
`credsStore` helper, not Airflow — point it out and let the user fix it.

## Step D — Report back (confirm first)

Once the user says the Dag passed:

1. **Tick the boxes (confirm first).** Re-fetch the issue body immediately before
   editing, change only `[ ]` → `[x]` on lines ending in `: @<handle>` for PRs verified
   in step B, and abort if the number of matched lines differs from what was reported.
2. **Draft the testing-issue comment (confirm first)** listing what was tested per
   provider, including changes that are in the wave but missing from the issue. End
   it with the `Drafted-by:` footer.
3. When the user drafts their `[VOTE]` reply, mention that they used this check to
   test all their changes in the candidate.
4. Offer cleanup: delete the downloaded wheels and the verification Dag, and suggest
   `breeze down`.

## Why

Contributors are asked to test their changes in every providers wave, but finding the
right distributions, installing them all together, and building a quick reproduction
for each change is tedious enough that it is often skipped. The agent can do the
mechanical part — inclusion checks, download, test Dag — so the human spends their time
on actually looking at the behaviour.
