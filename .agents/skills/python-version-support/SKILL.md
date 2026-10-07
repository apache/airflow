---
name: python-version-support
description: Add or retire Python versions across Apache Airflow, or exclude and enable individual providers on supported interpreters. Guide dependency-readiness preflights, packaging, CI, images, and reviewable PRs or stacks.
---

<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# Python version support

Deliver complete repository-wide support changes as reviewable GitHub PR stacks; provider-only compatibility amendments normally fit in one PR. Fork contributors can submit a single PR or manage the stack manually, as described in the shared workflow. Include core, packaging, image, maintenance-tooling, and documentation work in the initial effort. Providers that genuinely need later upstream compatibility work may remain explicitly excluded.

Choose the workflow matching the scope:

- Repository-wide addition: [adding support](references/adding-support.md), beginning with the dependency-readiness preflight.
- Repository-wide retirement: [removing support](references/removing-support.md).
- Provider-only exclusion or enablement on an already supported interpreter: [provider compatibility](references/provider-compatibility.md).

The repository-wide guides use [the shared stack workflow](references/pr-stack.md). Provider-only work reuses relevant readiness and generation steps without the full interpreter-rollout matrix.

Strongly recommend separate stacks when asked to retire one version and introduce another: land the retirement and establish passing CI first, then add the new version in a follow-up effort. Combining them unnecessarily complicates implementation, CI, and review. If the user explicitly requires a combined effort, reconcile both procedures into one support matrix and coherent merge sequence.

Confirm the target branch and release against the current [Python support policy](../../../README.md#support-for-python-and-kubernetes-versions). Distinguish core/Task SDK support, individual provider support, supported image architectures, and the regular production image's default interpreter.

Follow repository `AGENTS.md` for tests and generated-file ownership; run Python tests through Breeze. Each guide contains the implementation procedure and acceptance gates. External PRs and documentation at their ends are optional sources, not prerequisite reading.
