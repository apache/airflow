<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# Remove a Python version

Complete the shared [stack workflow](pr-stack.md) and the steps below. Confirm policy/EOL timing, the intended release, the retiring interpreter, and the new minimum.

## 1. Inventory current and historical obligations

Search all spellings of the retiring interpreter in sources, manifests, inline PEP 723 headers, classifiers/markers, image tags, workflow choices, generated docs, and executable release instructions. Separate interpreter references from unrelated package/release numbers.

Classify each hit as active current support, maintained release-branch support, historical documentation, or a separate runtime inside a test/container. Identify release branches still receiving cherry-picks and the images their CI restores. Preserve historical version lists, compatibility mappings, old constraints, and restore entries that still serve those branches.

Inspect `dev/breeze/src/airflow_breeze/global_constants.py`, `.github/actions/prepare_all_ci_images/action.yml`, all distribution manifests, both lock workspaces, scripts' interpreter declarations, and migration mappings. The shared current/all-version list may retain a version while a supported release branch still needs cherry-picks; follow its present policy rather than deleting every occurrence.

**Gate:** every retained old-version reference has a concrete purpose, and every active reference to retire has a stack owner.

## 2. Plan a floor-first retirement stack

Use this order to avoid releasing code that requires a newer interpreter while package metadata still admits the old one:

| Order | Layer purpose | Exit gate |
| --- | --- | --- |
| Bottom | Tooling/runner prerequisites and inline script interpreter declarations | Each tool runs under its declared interpreter; package runtime compatibility remains valid |
| Next | Support-boundary change: distribution floors/classifiers/markers, locks, active CI/image defaults, migration baselines, support docs and generated derivatives | Packages reject the retired interpreter and work on the new minimum; all current support claims match |
| Above | Remove compatibility shims, fallbacks, version-gated skips and callers in focused subsystem layers | Each removal is valid at its own head and preserves behavior on supported versions |
| Top | Mechanical Ruff/codemod modernization, split by reviewable subsystem as needed | Declared floor permits every rewrite; behavior-sensitive edits are separately reviewed |

A floor-first boundary may make an earlier metadata layer larger, but removes the need for an undocumented no-release window. Fold inseparable transitions into a layer or declare a contiguous merge unit through the shared procedure. Include tests with the behavior they protect, not in a detached final test PR.

**Gate:** no shim deletion or new standard-library API depends on a later floor bump.

## 3. Implement tooling prerequisites

Update standalone PEP 723 interpreter requirements before those scripts use newer APIs. Verify the actual runner interpreter for hooks, scripts/shared-library tests, and image/release jobs; runner system Python can differ from Breeze's default.

Keep Breeze's requirement unchanged if its independent floor remains compatible. If it must change, check whether its manifest is also a root-workspace member; group it and every affected lock with the support-boundary manifest changes. Keep lower runner/script changes compatible with unchanged workspace manifests, or fold them into that boundary layer when inseparable.

**Gate:** tooling and generators run with the new minimum, and lower layers still satisfy the package versions they advertise.

## 4. Implement the complete support boundary

1. Raise `requires-python` and remove obsolete classifiers/markers across affected distributions. Inspect core, Task SDK, airflowctl, providers, shared packages, dev/scripts and integration-test projects; preserve intentional independent requirements. If Breeze's manifest changes, refresh its runner lock before using the Breeze generators below because the shim uses `--locked`; keep that lock in this layer and finalize the root lock after generation.
2. Follow [the shared provider metadata-generation steps](provider-compatibility.md#4-regenerate-provider-support-metadata) for provider templates/README support, dependency JSON, and root aggregate markers.
3. Finalize every affected root/Breeze lock with its manifest inputs after generation. Inspect resolver changes, especially `exclude-newer` downgrades; the frozen lock hook does not regenerate.
4. Remove the version from active matrix/build choices and move defaults to supported interpreters. Keep historical/backport image restore paths. Inspect actual cache-refresh and release image loops, not just their comments.
5. Remove download/signature or compatibility paths only when no retained build workflow needs them. Edit `scripts/docker/` sources and run `prek run update-inlined-dockerfile-scripts --all-files`. Validate cold image builds against job time/resource budgets under the new default.
6. Re-key migration baselines to an interpreter that the chosen source Airflow release supports. Preserve required major-version upgrade coverage; retiring one cell must not silently leave only newer-release baselines. Update mapping/selection tests and verify the relevant PostgreSQL/MySQL migration paths.
7. Update all current support claims and examples with the floor: README development tables, package/PyPI/provider READMEs, installation and quick-start docs, contributor setup, Docker/Breeze help, constraints examples, and executable release instructions. Preserve stable-release columns and historical examples unless deliberately upgrading their pinned Airflow release too.
8. Run `prek run update-tested-versions --all-files`, `prek run update-supported-versions --all-files`, and `prek run generate-pypi-readme --all-files`. Regenerate affected Breeze help with `breeze setup regenerate-command-images`. Own each generated output and the single user-visible retirement newsfragment/changelog entry here.

A static example pinned to an older Airflow release may have no constraints for the new minimum. Verify the exact release/constraints pair instead of replacing its Python string mechanically. Keep provider publication coherent: every published package must declare the minimum its code requires.

**Gate:** minimum-version packages resolve/install, active jobs/defaults and documentation agree, generated outputs reproduce, release-branch obligations remain intact, and migration coverage survives the retirement.

## 5. Remove shims and modernize in upper layers

Search fallback imports, typing/async compatibility helpers, interpreter guards, and conditional skips. For each deletion, update all callers in that same layer; inspect its own head, upper heads, and newly introduced trunk references.

Validate changed behavior on the new minimum and other supported interpreters. Removing a fallback should preserve the supported-version API, return types, serialization, and process behavior.

Set Ruff's target version only after the runtime floor permits its output. Keep mechanical rewrites separate from semantic changes. Review enum string formatting/serialization, public timeout parameters, and similar API-sensitive rewrites individually; they are not automatically justified by dropping an interpreter.

**Gate:** no removed helper is still called, no upper layer reintroduces an unresolved reference, and every supposedly mechanical rewrite preserves behavior.

## 6. Close the retirement stack

At the completed head:

- Run required package/provider, core/SDK/airflowctl/shared, constraints, image, and documentation checks across the remaining Python range, including the new minimum.
- Verify current CI/production image installation on AMD64 and ARM64. Preserve the production default rule for all bundled providers.
- Check migration paths/backends against the preserved baseline matrix.
- Repeat the version-reference audit across untouched as well as changed files. Explain retained historical, branch-only, and nested-runtime hits; inspect generated/manifest values too.
- Review prompts, transcripts, image tables, constraints URLs, and release commands semantically. Build/render success does not prove their version claims.
- Confirm each layer's code is compatible with the runtime floor declared at that head, locks/generated files have the correct input owner, and release notes describe the actual boundary layer.

Apply the shared whole-stack/per-layer review and merge gates. Required metadata, docs, image, and maintenance updates belong to this effort; shim cleanup and mechanical modernization are planned upper layers, not unspecified follow-ups.

## Optional references

- [1] — Retirement scope and release-branch image obligations.
- [2] — Standalone script metadata.
- [3] — Compatibility shim removal.
- [4] — Package floors, locks, and behavior-sensitive modernization review.
- [5] — Documentation and pinned-constraints example review.
- [6] — Original retirement umbrella, superseded by the layered implementation.

[1]: https://github.com/apache/airflow/pull/74151
[2]: https://github.com/apache/airflow/pull/74152
[3]: https://github.com/apache/airflow/pull/74153
[4]: https://github.com/apache/airflow/pull/74157
[5]: https://github.com/apache/airflow/pull/74159
[6]: https://github.com/apache/airflow/pull/74144
