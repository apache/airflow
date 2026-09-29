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

# Override: release-vote-tally — Airflow providers waves

## What this overrides

Adapts the `release-vote-tally` skill to Airflow provider waves, which are voted on as a set
and can drop single providers during the vote. Everything else in the skill, including its
golden rules and hard rules, still applies.

## Identifying the wave

- A wave is identified by its preparation date (`YYYY-MM-DD`), not by `<version>-rcN`. Find the
  thread by the `vote_subject_template` in
  [`release-management-config.md`](release-management-config.md).
- Providers waves have no planning issue and no `vote-open`, `vote-passed` or `rc-rolled` labels.
  Skip the planning-issue checks and the label proposal. The "Status of testing Providers" issue
  is not a planning issue.

## Classifying votes

- A vote line that names providers, for example `+1 (non-binding) for amazon and google` or
  `-1 for edge3`, counts for those providers only. Record the providers with the vote. It is a
  scoped vote, not an `AMBIGUOUS` one.
- A plain `+1` counts for the whole set, even when the voter adds that they only tested their own
  changes.
- A single reply can hold several scoped votes, such as `+1` for some providers and `-1` for
  another. Record each one.
- The pass rule applies to the whole set. A scoped `-1` does not count against the set. It counts
  against its provider, and the release manager decides on the thread whether to exclude that
  provider.

## Drafting the `[RESULT][VOTE]` email

Use the email template and listing rules in
[`dev/README_RELEASE_PROVIDERS.md` § Summarize the voting](../dev/README_RELEASE_PROVIDERS.md#summarize-the-voting-for-the-apache-airflow-release)
instead of the skill's default body. In short:

- List binding `+1` voters by name only, without `(binding)` after each name.
- Split non-binding `+1` voters into a whole-set list and a specific-providers list, with the
  providers in brackets after each name.
- For every excluded provider, give its binding and non-binding `-1` counts with the voters'
  names, and leave out a part whose count is zero.
- Keep the exclusion reason and the plan for the next RC, and let the release manager choose
  between the next wave and an ad-hoc release.
- Link the vote thread as `https://lists.apache.org/thread/<id of the [VOTE] email>`.
