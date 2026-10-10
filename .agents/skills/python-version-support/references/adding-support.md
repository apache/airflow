<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# Add a Python version

Complete the shared [stack workflow](pr-stack.md) and the steps below. Support belongs in the policy-approved branch/release; adding it to a patch release is not the normal path. Let `<new>` denote the new major.minor interpreter.

## 0. Establish dependency readiness

Run [the dependency-readiness preflight](readiness-preflight.md), starting with scratch target-version locking and per-component attribution. Present its core/SDK/provider ✅/❌ table, dependency chains, and upstream issue tracker before selecting upgrades or exclusions. Carry unresolved required dependencies into prerequisite layers and justified provider limitations into complete activation metadata.

## 1. Inventory the entire support surface

Search for the preceding and new versions in dotted form, interpreter/image tags, classifiers, identifiers, and filenames. Trace each active hit to its source and consumers. Classify historical/release-specific examples and other runtimes before editing.

Assign all these surfaces to the stack plan:

| Surface | Inputs and consumers to inspect |
| --- | --- |
| Versions and maintenance | `dev/breeze/src/airflow_breeze/global_constants.py`: allowed/current/all lists, patch-level map, historical compatibility lists, production-image default; `scripts/ci/prek/upgrade_important_versions.py` and its fetching/replacement loops |
| Images and caches | `scripts/docker/`, especially `install_os_dependencies.sh` and `common.sh`; both Dockerfiles; CI/prod build workflows; `.github/actions/prepare_all_ci_images/action.yml`; registry refresh scripts and executable release image-verification loops |
| Package resolution | Root and distribution `pyproject.toml` files, provider templates/metadata, Python upper exclusions and classifiers, environment markers, `[tool.uv.sources]`, root and Breeze locks, install/test-collection filtering |
| CI and migration | Breeze matrix/selective-check sources; AMD/ARM workflows; `scripts/ci/testing/get_min_airflow_version_for_python.py`; migration workflow conditions |
| Constraints | First-file download/bootstrap paths, constraints generators and workflows, source-provider / no-provider / regular constraints modes |
| Support claims | README development versus released-version columns, `generated/PYPI_README.md`, installation/quick-start docs, airflowctl README, contributor setup, Docker image tables/defaults, Breeze help, release instructions |

**Gate:** every current support list and consumer has an owner, including the periodic patch updater, both architectures, docs derivatives, and release-maintenance commands.

## 2. Plan the addition stack

Use this dependency order; split compatibility work by subsystem when it improves review, and combine changes that cannot pass independently.

| Order | Layer purpose | Exit gate |
| --- | --- | --- |
| Bottom | Build, resolver, test-harness, and maintenance prerequisites that work with the existing Python range | Existing supported versions pass; new infrastructure behavior has focused regression coverage |
| Middle | Core / Task SDK / airflowctl / shared runtime compatibility, with tests beside each behavior change | Every head preserves its advertised range; dependency-bound changes stay with the manifest layer they require |
| Top | Complete interpreter activation across package metadata, image/version sources, provider exclusions, constraints, CI, migration conditions, and support docs | New-version integration and all completion gates below pass; support claims describe this head |

Build the new interpreter early on a development cumulative head to discover failures. Move each fix into its owning layer and cascade descendants before review. Keep compatibility prerequisites below the support declaration. If activation must be split for size, define a contiguous activation merge unit with all its required docs and generated outputs; complete it before marking any support layer ready.

**Gate:** the plan contains the full support change, not a matrix PR followed by unspecified core, architecture, or documentation repairs.

## 3. Implement and prove prerequisite layers

Inspect existing behavior before changing it:

1. Ensure the patch updater obtains interpreters from the canonical supported list. When changing it, test that a newly enumerated interpreter is both fetched and its patch pin replaced without updating a second hard-coded list.
2. Verify first-version constraints bootstrapping in both image installation and constraints generation. Generation must succeed when that interpreter has no prior constraints file; a successful image build is insufficient. Distinguish an expected first publication from failures fetching an established baseline, and preserve normal regeneration behavior.
3. Check provider-install and test-collection filtering against incompatible `Requires-Python`. Fix filtering/checker gaps with regression tests rather than relying on every provider becoming compatible immediately.
4. Make test partition/resource changes only from measured failures. Co-locate selective-check rule changes with `dev/breeze/doc/ci/04_selective_checks.md` and `dev/breeze/tests/test_selective_checks.py`.

**Gate:** prerequisites work on the current range, and the first-file, updater, and excluded-provider paths have actual evidence.

## 4. Fix runtime compatibility in focused layers

Run affected suites on the cumulative development head with the new interpreter. Classify failures into Airflow behavior, upstream dependency support, runtime-semantic changes, test assumptions, and resource pressure.

Inspect changes to multiprocessing defaults, child-process inheritance/mocking, startup latency, async behavior, typing/introspection, CLI argument handling, and wheel/ABI availability. Fix behavior and its regression test together. Preserve tests' intended contract; broad skips or arbitrary timeout increases do not establish compatibility.

Keep hand-written fixes separate from mechanical rewrites. Put code needing a new dependency floor in the layer that declares and locks that dependency. Validate existing supported interpreters as well. If the new interpreter cannot install at a lower head, record its target-specific evidence at the activation head rather than claiming that lower CI ran it.

**Gate:** the cumulative target suites pass and every lower head remains valid for the range it declares.

## 5. Implement the complete activation layer or merge unit

### Version, package, and image inputs

- Update allowed/current Python lists, patch-level mappings, build choices, active cache/refresh loops, and any minor-version signature identity/issuer maps. Preserve historical compatibility and release-branch restore entries.
- Update actual distribution support bounds/classifiers and generated dependency markers. Inspect core, SDK, airflowctl, shared distributions, dev/scripts, and integration-test projects; do not invent declarations for packages that intentionally lack them.
- Audit every nightly, Git, or wheel override. Check interpreter, OS, CPU architecture, and ABI selectors. An x86_64-only wheel selected for all Linux interpreters will fail ARM64. Prefer a supported release or an explicit unsupported-package limitation to an unverified source override.
- Regenerate Dockerfile inlined scripts from their `scripts/docker/` sources with `prek run update-inlined-dockerfile-scripts --all-files`.
- Keep the regular production-image default at the highest interpreter supported by all bundled providers; core support and image default are separate decisions.

### Initial provider exclusions and resolution

Follow [the provider exclusion workflow](provider-compatibility.md#2-exclude-an-incompatible-provider) for each genuinely incompatible provider, including mandatory reverse dependencies and optional-extra boundaries. Use its [shared metadata-generation order](provider-compatibility.md#4-regenerate-provider-support-metadata) for provider templates, dependency JSON, aggregate markers, and locks. Initial exclusions must cover install/collection filtering and bundled images; future provider enablement follows the same provider guide.

### CI, constraints, and migrations

Wire the new interpreter into actual selected jobs and both architecture paths. Check that labels/selection do not silently omit it.

Build its CI image with `breeze ci-image build --python <new> --platform <platform>` for `linux/amd64` and `linux/arm64`. Use the dependency-upgrade build path where constraints generation requires it. Verify the production build/install path on both architectures with `breeze prod-image build` / `verify`; inspect their help for source-install and locally generated constraints arguments.

Generate constraints using `breeze release-management generate-constraints --python <new> --airflow-constraints-mode <mode> --answer yes` for all promised modes: `constraints-source-providers`, `constraints-no-providers`, and `constraints`. Regular constraints require the workflow's core/SDK/provider wheel inputs. Verify fresh resolution/install from those outputs; source-provider success does not prove regular-wheel resolution.

Set the Python-to-minimum-Airflow mapping to an actually compatible release. Until such a released baseline exists, explicitly gate migration jobs for the new interpreter while retaining migration coverage on existing interpreters. Update mapping/selection regression tests when changing those behaviors.

### Documentation and generated derivatives

In the activation layer, run `prek run update-tested-versions --all-files`, `prek run update-supported-versions --all-files`, then `prek run generate-pypi-readme --all-files`. Preserve README columns describing already released versions.

Update quick start, contributor virtualenv setup, airflowctl README, Docker image/default tables and build instructions, `dev/README_RELEASE_AIRFLOW.md` image-verification loops, and Breeze image documentation. Check commands and examples against the actual target Airflow release, constraints, and image default.

Regenerate Breeze help with `breeze setup regenerate-command-images` or `prek run update-breeze-cmd-output --all-files` when its inputs change. Include a single user-facing support note using the repository's release-note rules, owned by this activation layer.

**Gate:** the advertised interpreter installs, builds, and executes on both supported architectures; constraints and exclusions resolve; migration coverage is deliberate; every active support claim and generated derivative matches the final inputs.

## 6. Close the stack without core follow-ups

At the current completed head, require evidence for:

- Core, Task SDK, airflowctl, shared behavior, and applicable provider collection/tests on the new interpreter; existing-version regression coverage and required database/executor cells.
- CI and production installation/build verification on both AMD64 and ARM64, including architecture-sensitive source/lock paths.
- First constraints publication and regeneration, all promised constraints modes, and package metadata/marker resolution.
- Periodic maintenance consuming the new interpreter, actual CI/cache/release loops including it, and generated outputs reproducing cleanly.
- A semantic review of every active version claim and executable example. Docs build success alone cannot establish this.

Classify every remaining search hit and every skipped CI cell. Provider support may follow later only when initial exclusions and image-default limitations are complete. Stale docs, architecture failures, constraints bootstrap defects, and missed maintenance consumers are unfinished work in this stack. Apply the shared whole-stack/per-layer review and merge gates.

## Optional references

- [1] — Python 3.14 implementation.
- [2] — Documentation and release-instruction omissions.
- [3] — Workspace source overrides and ARM64 compatibility.
- [4] — Periodic Python patch-updater omission.
- [5] — Constraints bootstrap work.
- [6] — Migration baseline mapping.
- [7] — Policy on adding support in patch releases.

[1]: https://github.com/apache/airflow/pull/63520
[2]: https://github.com/apache/airflow/pull/63950
[3]: https://github.com/apache/airflow/pull/64028
[4]: https://github.com/apache/airflow/pull/73039
[5]: https://github.com/apache/airflow/pull/63592
[6]: https://github.com/apache/airflow/pull/63596
[7]: https://github.com/apache/airflow/discussions/51184
