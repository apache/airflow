<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# Build and review the support stack

Use this procedure with either direction guide. Prepare the complete stack before declaring the support change ready; later layers are planned implementation, not post-merge catch-up work.

## 1. Define the target and assign ownership

Record the intended release, trunk branch, current and resulting Python ranges, image architectures, bundled-provider boundary, and migration baselines. Check for existing work addressing the same support change. Base publication on `upstream/main`, using the confirmed upstream release branch only when the support policy requires it; neither `origin/main` nor local `main` substitutes for the upstream baseline.

When the inventory reveals many equivalent version edits, apply [the shared version-source guidance](version-sources.md) before assigning implementation layers. Prefer one policy definition and derived consumers over repeated literal replacements.

Create a stack plan with one row per layer:

| Layer / base | Purpose and owned inputs | Generated outputs / lock | Runtime declared at this head | Validation and release gate |
| --- | --- | --- | --- | --- |

For interpreter additions, give CI install filtering its own prerequisite layer and independent PR on `upstream/main`; the activation PR stacks on it. Keep the helper, installer, generated Dockerfile content, and regression tests together.

Assign every required surface in the direction guide to a layer. A docs table, image platform, constraints mode, or maintenance script without an owner is unfinished planning. Provider compatibility deferred to upstream is an explicit exception; initial exclusion metadata, install filtering, and documented limitations still belong in this stack.

## 2. Choose native or fork-contribution publication

A native stack has one trunk and a linear chain: the bottom PR targets the trunk; each higher PR targets the branch immediately below it. GitHub must record the PRs as members of the stack.

Native stacks require all head branches and the trunk in the same GitHub repository. Fork contributions remain possible; Airflow's normal fork-to-upstream path uses either:

- **A single PR:** combine the planned layers into one complete, reviewable contribution while retaining their implementation order and validation gates.
- **A manually managed stack:** chain branches locally, include a consistent dependency map in the PR descriptions, and add a "Stacked on #<preceding-PR>" comment to each dependent PR. Identify inherited changes, keep dependent PRs from merging before their prerequisites, and rebase/update their diffs and dependency links as lower PRs change or merge.

Preserve the repository's fork-only publishing rule. Manual dependency links do not create native GitHub stack membership or eligibility for Magpie's native-stack review.

Record the selected publication shape in the plan and continue local implementation. For authorized publication, use [Airflow's publishing workflow](../../airflow-publish-changes/SKILL.md) and [contribution-text guidance](../../airflow-pr-draft-summary/SKILL.md). This skill does not authorize remote writes. Use plain commit subjects: this workflow's `check-no-conventional-commit-message` hook rejects prefixes such as `CI:`. Never add an agent `Co-Authored-By` line.

In an eligible setup, the CLI lifecycle is `gh stack init <bottom-branch>`, `gh stack add <next-branch>`, then `gh stack submit` after local verification and publication authorization. Inspect the installed extension's help for trunk/remote selection. The website also supports creating a native stack from correctly chained PR bases.

### Manual-stack publication and PR template

Create manual-stack PRs bottom-up, marking every PR except the last (top) one as draft. Put the reviewer notice shown below at the very top of each description, before the summary.

Publish descriptions in two passes:

1. When creating each PR, include a `PR stack` section listing only previously created PRs in bottom-to-top order. The first PR's list is initially empty.
2. Once all PRs exist, update every description to list the complete stack in the same order, including the PR itself. Bold the current PR's entry and label it `current PR`.

Use bullets or a numbered list with bare PR URLs, rather than custom Markdown link labels, so GitHub renders each PR's title and status ([GitHub autolink guidance](https://docs.github.com/en/get-started/writing-on-github/working-with-advanced-formatting/autolinked-references-and-urls)). Replace the placeholders below with actual PR numbers. This template shows the second PR's description after the final pass:

```md
> [!IMPORTANT]
> **Note to reviewers**: this is a manual stacked PR. Please review only the latest commit.

[Summary of this layer's changes.]

### PR stack

- https://github.com/apache/airflow/pull/<bottom-PR-number>
- **https://github.com/apache/airflow/pull/<current-PR-number> (current PR)**
- https://github.com/apache/airflow/pull/<top-PR-number>

[Validation for this layer.]
```

Preserve the contribution-text guidance's required disclosure and attribution when filling in the template.

**Gate:** the plan names the actual trunk, publishing repository, and publication shape. Verify native membership for the native workflow; for a manual stack, verify dependency links, merge order, complete stack lists with the current PR bolded, the opening reviewer notice, and draft status on every PR except the top one. A single PR includes the full change. Omit native-stack/Magpie-stack-review claims for both fork alternatives.

## 3. Make each layer coherent at its own head

- Put prerequisites below their consumers. Code, tests, dependency requirements, and interpreter declarations must agree at each head. Prefer moving the runtime floor before using newer APIs; fold inseparable changes into one layer.
- If a transition must land as several layers together, name the contiguous merge unit in every affected PR. Verify the whole unit before its bottom merges and prevent a release from an incomplete unit. A later layer is not evidence that an earlier head works.
- Keep behavior changes and their regression tests together. Separate mechanical Ruff/codemod layers from hand-written compatibility changes; describe the tool, selected rules, and intentional exceptions.
- Assign each generated file and lock to the layer changing its inputs. Co-locate changes sharing a lock workspace so generation stays with its manifests. Inspect workspace membership: a Breeze manifest can feed both its own and the root lock; separate lock files do not imply independent inputs. Group those manifest changes and all affected locks in one layer. Consolidate repeated generation without leaving any intermediate head's lock stale.
- Review generator inputs and independently verify outputs. A stack reviewer may count generated files without reading them; its verdict does not establish output correctness.
- Inspect each adjacent-base diff and the cumulative trunk-to-top diff. Explain necessary file overlap in commit bodies and PR descriptions; keep scope, declared support, release notes, and stack order consistent.

**Gate:** each prefix installs/runs on the versions it advertises, has no dependency on an upper layer, and has reproducible generated outputs. Any indivisible release unit is explicit.

## 4. Validate layers and the complete stack

For each layer, run relevant static checks and behavior tests against its cumulative head. Use `breeze verify` and `breeze ci selective-check --commit-ref <commit>` to inspect selected coverage. Confirm actual workflow jobs, including Python versions and platforms, rather than treating a success rollup or skipped job as executed coverage.

Run full support-change integration coverage at the completed activation/top head. Current labels include `full tests needed` and `all versions`; verify their current semantics and any limiting labels. Native stacks trigger trunk-targeted workflows for every layer, so budget expensive integration coverage deliberately using existing repository mechanisms. Keep required checks for every layer; top-layer success cannot excuse a broken lower head.

Support changes often regenerate provider metadata across every provider, `common.compat` included. On PRs targeting `main`, the selective check fails when files under `providers/common/compat/` change alongside other providers whose `apache-airflow-providers-common-compat` line lacks `# use next version`. Do not add that comment to every provider: release tooling would raise each one's `common-compat` floor at the next release, although none of them uses new `common.compat` code. Ask a maintainer or collaborator to apply the `skip common compat check` label, and state in the PR description that no provider depends on a `common.compat` change in the PR. Add the comment only to providers that do use new `common.compat` code from the same PR.

Keep a validation ledger: head SHA, interpreter/platform/backend, command or CI job, outcome, and any justified exclusion. Separate real failures from canceled, superseded, or unexecuted jobs. Explain unrelated failures with concrete evidence.

**Gate:** every required cell in the direction guide has current evidence; none is inferred from a different interpreter, architecture, or layer.

## 5. Address feedback in its owning layer

Fix lower-layer defects there, cascade-rebase descendants, and regenerate owned outputs. For native stacks, use `gh stack rebase` / `gh stack push` only within authorized publication. For manual stacks, rebase dependent branches and maintain their PR dependency links yourself. Verify that each upper branch contains the latest lower head and that the stack remains linear.

Recheck affected lower heads and descendants after restacking. Compare removed/renamed definitions with remaining callers at their own head, higher heads, and new references on the trunk. Revisit overlapping files and new trunk changes before final integration validation.

**Gate:** review and CI evidence refer to the current heads; no upper branch still contains a stale lower-layer implementation.

## 6. Hand off for whole-stack and per-layer review

Prepare for Magpie's whole-stack review by supplying the layer/base/head table, per-head runtime declarations, generator ownership, exclusions, and actual validation. Its structural checks cover chain currency, overlapping files, removed-definition seams, runtime-floor ordering, duplicate generation/release notes, and narrative consistency. Mechanical and generated changes may receive sampled or no code reading, so keep semantic outliers visible.

After authorized publication of a native stack, when available, run `pr-management-stack-review pr:<member-PR> dry-run` through the installed Magpie plugin for author-side whole-stack feedback. Follow that skill's own fetch and posting boundaries. For a manual stack, a single PR, or an unavailable plugin, apply the relevant structural checks above yourself and state that Magpie's stack review was not run.

A coherent stack-level COMMENT is not approval of any layer. Arrange the separate `pr-management-code-review` passes and required CI/review gates for each PR. Merge bottom-up, individually or as the explicit contiguous merge unit, only after those gates hold. Recheck remaining layers when bases/heads change.

**Gate:** no unresolved structural or support-completeness issue remains; review coverage is stated accurately and per-layer approvals are separate from the stack verdict.

## Optional references

- [1] — GitHub stack topology and same-repository constraint.
- [2] — Creating native stacks.
- [3] — Cascading fixes and rebases.
- [4] — Stack CI behavior and cost.
- [5] — Bottom-up and grouped merging.
- [6] — Magpie whole-stack review contract.

[1]: https://docs.github.com/en/pull-requests/get-started/about-stacked-prs
[2]: https://docs.github.com/en/pull-requests/how-tos/create-pull-requests/creating-stacked-pull-requests
[3]: https://docs.github.com/en/pull-requests/how-tos/create-pull-requests/managing-stacked-pull-requests
[4]: https://docs.github.com/en/pull-requests/how-tos/merge-and-close-pull-requests/optimizing-ci-for-stacked-pull-requests
[5]: https://docs.github.com/en/pull-requests/how-tos/merge-and-close-pull-requests/merging-stacked-pull-requests
[6]: https://github.com/apache/magpie/pull/1543
