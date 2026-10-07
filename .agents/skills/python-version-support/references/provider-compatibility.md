<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# Change provider Python compatibility

Use this workflow to exclude or enable selected providers on an interpreter already supported by Airflow. Repository-wide addition/retirement guides also reuse the metadata-generation steps below. A provider-only amendment normally belongs in one PR; use the shared [stack workflow](pr-stack.md) when dependency or tooling prerequisites justify a stack.

## 1. Establish scope and readiness

Record provider IDs, target Python minor, architectures, affected extras, and the intended provider release. Dotted IDs map to source directories, e.g. `apache.cassandra` to `providers/apache/cassandra/`. Keep the provider's independent minimum distinct from core's minimum.

Run the relevant profiles of [the dependency-readiness preflight](readiness-preflight.md). Inspect actual provider manifests for mandatory dependencies, extras, markers, and source overrides. `generated/provider_dependencies.json` helps trace mandatory and cross-provider dependencies but omits optional-extra requirements. Separate runtime failures from dev/test tooling failures.

Find existing upstream support issues/PRs, verify released compatible versions, and preserve reproductions and dependency chains. Follow the preflight's upstream-tracking and publication boundaries.

## 2. Exclude an incompatible provider

1. Establish a concrete incompatibility from dependency metadata, target installation/builds, or failing behavior. For new-interpreter additions, follow [the wheel audit's target-specific floor preference](readiness-preflight.md#4-detect-dependencies-without-target-wheels) before excluding a provider: when its dependency's existing lower bound lacks wheels but a newer compatible release provides them, raise the floor only for the new interpreter. Unresolved compilation requirements justify initial exclusion without waiting for a failed build; classify pure-Python sdists and confirmed fallbacks separately. Keep an optional-extra problem scoped to that extra when the base provider and promised CI profile remain usable.
2. Add the target minor to `excluded-python-versions` in `providers/<provider path>/provider.yaml`.
3. Trace every mandatory base `[project.dependencies]` edge to the excluded provider, then cross-provider extras. Exclude each mandatory dependent or repair its path; for example, excluding `postgres` requires excluding or repairing `pgvector`, whose base dependencies include it.
4. Apply the shared generation steps below. Verify whole-minor metadata exclusions and `python_version != "<minor>"` markers in every relevant section, including dependency groups and optional dependencies: the checker covers only some sections. Hand-maintained root extras after `End of automatically generated airflow optional dependencies`, such as `apache-webhdfs`, need manual markers. Mark a direct dev-group entry bringing a blocker, such as `datafusion` bringing `pyarrow`, and remove the marker once compatible wheels/releases are available. Revisit temporary numpy, pandas, and datafusion markers after upgrades.
5. Verify root extras, cross-provider requirements, install filtering, test collection, and affected image/constraints profiles honor the exclusion. The YAML exclusion and generated `Requires-Python`/aggregate markers do not stop CI's `uv sync --all-packages --group ci-image` from installing the excluded member's third-party and dev dependencies; `EXCLUDED_PROVIDERS` controls test selection only. Use [CI image install filtering](ci-install-filtering.md) and prove the recorded blocking dependencies are absent from the target image install set. Metadata and lock checks alone are insufficient.

**Gate:** no advertised target profile installs or collects the incompatible provider through a mandatory path; unaffected Python versions and extras remain usable.

## 3. Enable a previously excluded provider

1. Fix or upgrade direct and transitive dependencies using compatible releases. Put dependency floors, runtime fixes, and their regression tests together.
2. Audit marker-selected requirements before removing the exclusion. A requirement ending at `python_version < "3.14"` can omit a required driver on 3.14 while resolution still succeeds. Update markers and verify the required packages actually install.
3. Remove the YAML exclusion only after the target dependency graph and affected behavior work. For exclusions made during initial interpreter addition because wheels were unavailable, repeat the target-wheel audit and require compatible wheels or verified usable source dispositions for the full selected profile on every promised architecture before enabling support. Check architecture-sensitive wheels and overrides.
4. Apply the shared generation steps below, removing obsolete exclusion markers from all relevant root and cross-provider paths.

**Gate:** the full target profile installs, imports, collects tests, and passes affected tests with its required dependencies present. A successful weakened resolver graph or exclusion removal alone does not enable support.

## 4. Regenerate provider support metadata

Use this order for provider-only amendments and for provider work in repository-wide support changes:

1. Edit owning inputs: `provider.yaml`, provider templates, and dependency declarations. Provider `pyproject.toml` dependency/optional-dependency sections are intentionally preserved across generation and may be edited directly; regenerate other generated fields from their inputs.
2. If Breeze's manifest changed, refresh its runner lock before invoking generators through the `--locked` shim. Keep all affected root/Breeze locks with their manifest inputs.
3. Run `breeze release-management prepare-provider-documentation --reapply-templates-only --skip-git-fetch --only-min-version-update --skip-changelog` with all providers so unsupported classifiers are removed. The generator skips `state: not-ready` providers; maintain their classifiers by hand.
4. Run `breeze run --python 3.13 --answer yes python scripts/ci/prek/update_providers_dependencies.py` to refresh `generated/provider_dependencies.json`.
5. Run `prek run update-pyproject-toml --all-files` twice, then `prek run check-excluded-provider-markers --all-files`. [The generator](../../../../scripts/ci/prek/update_airflow_pyproject_toml.py) applies exclusion markers to providers with or without a minimum version. Separately inspect cross-provider manifests, dependency groups, and manual extras.
6. Explicitly run `uv lock` in each affected lock workspace after generation. The frozen lock hook does not regenerate. Inspect unrelated resolver movement and `exclude-newer` effects.
7. Check provider README support tables and `Requires-Python`/classifiers/markers. `sync-provider-readme` alone only updates dependency tables.

**Gate:** generation reproduces cleanly, metadata agrees with the intended provider support, and no aggregate or transitive requirement bypasses the boundary.

## 5. Validate and describe the amendment

Verify a fresh target install, imports/test collection, affected provider tests, and regressions on other supported interpreters. When raising dependency floors, test the lowest supported dependencies as well, for example:

```bash
breeze testing providers-tests --python <target> --force-lowest-dependencies --test-type "Providers[<provider-id>]"
```

Rebuild affected CI images when their dependency inputs change. Check both supported architectures for binary dependencies. Verify relevant constraints membership and installation paths; regular constraints consume published provider metadata, so checkout compatibility does not establish release availability.

Update the provider changelog directly for a user-visible support change; providers do not consume newsfragments. Keep global interpreter lists and regular-image defaults tied to their own policies. Reassess an image default only after every bundled provider supports it. Use the repository's contribution/publication skills for authorized publication.

**Gate:** the amendment's claims distinguish checkout support, published provider support, and image inclusion; relevant tests and resolution evidence refer to the current change.
