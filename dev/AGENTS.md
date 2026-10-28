<!-- SPDX-License-Identifier: Apache-2.0
      https://www.apache.org/licenses/LICENSE-2.0 -->

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [dev/ directory guidelines](#dev-directory-guidelines)
  - [Scripts](#scripts)
  - [Fork CI job statuses and GitHub's rate limits](#fork-ci-job-statuses-and-githubs-rate-limits)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# dev/ directory guidelines

## Scripts

New scripts in `dev/` must be standalone Python scripts (not bash). Each script must include
[inline script metadata](https://packaging.python.org/en/latest/specifications/inline-script-metadata/)
placed **after** the Apache License header, so that `uv run` can execute it without any prior
installation:

```python
#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) ...
#   http://www.apache.org/licenses/LICENSE-2.0
# ...
# /// script
# requires-python = ">=3.9"
# dependencies = [
#     "some-package",
# ]
# ///
```

If the script only uses the standard library, omit the `dependencies` key but keep the
`requires-python` line.

Run scripts with:

```shell
uv run dev/my_script.py [args...]
```

Document `uv run` (not `python`) as the invocation method in READMEs and instructions.

## Fork CI job statuses and GitHub's rate limits

The fork CI monitor (`breeze ci fork-ci-reconcile`, run by `.github/workflows/fork-ci-monitor.yml`)
reports every job of a gated pull request's fork run as its own commit status. Those writes are the
largest share of what the monitor does with the repository's `GITHUB_TOKEN`, whose rate limits every
workflow of `apache/airflow` shares (see "Write budget" in
`dev/breeze/doc/ci/08_fork_ci_for_external_contributors.md`).

If the budget is exceeded, turn the job statuses off: set `job_statuses = false` in
`.github/fork-ci-gate.toml`. Signs that it is exceeded are the monitor's Slack note that job
statuses "did not fit in the budget" persisting across runs, rate-limit retries ("GitHub rate limit,
retrying") or "still rate limited" errors in the monitor's run log or its Slack messages, or other
workflows of the repository failing on secondary rate limits. The aggregate `Fork CI / Tests (AMD)`
status and `CI passed` are posted either way, so turning job statuses off loses detail, not gating.
Lowering `MAX_JOB_STATUSES_PER_RUN` in `fork_ci_reconcile.py` is the softer alternative.
