<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# Verify CI image install filtering

Use when provider exclusions affect CI image installation, either during interpreter addition or a provider-only amendment. Keep metadata generation in [the provider workflow](provider-compatibility.md#4-regenerate-provider-support-metadata); this procedure verifies the install boundary.

## Why metadata exclusions are insufficient

`excluded-python-versions` produces provider `Requires-Python` exclusions and marker-gated root dependencies, but CI's `uv sync --all-packages --group ci-image` selects every workspace member, including excluded providers and their dev dependencies. `EXCLUDED_PROVIDERS` affects test selection only. Exclusions must also remove the owning member's requirements from CI installation.

`uv sync --no-install-package <provider>` retains its dependencies. Compiling only the root `ci-image` group misses member dev groups and extras. Use the filtered installer below.

## Follow the image install path

The owning installer is [install_airflow_when_building_images.sh](../../../../scripts/docker/install_airflow_when_building_images.sh), with [filter_ci_install_set.py](../../../../scripts/docker/filter_ci_install_set.py) resolving the kept members' requirements.

1. In the frozen branch, `has_providers_excluded_for_python` detects exclusions for the target minor. It selects `install_workspace_without_excluded_providers`; otherwise the existing sync path runs.
2. The installer exports lock pins with `uv export --frozen --all-packages --group ci-image --no-hashes --no-emit-workspace`.
3. It invokes the helper with the following arguments. `--find-links` is an optional helper argument:

   ```text
   scripts/docker/filter_ci_install_set.py --root <sources> --constraints <pins> \
     --python-minor <minor> --platform {x86_64|aarch64} --workdir <dir> \
     --output <requirements> --editables <file>
   ```

4. The helper turns each kept member into a local `name @ file://` requirement and adds its `dev` group; the root uses `ci-image`. It removes excluded members and runs `uv pip compile` from the workspace root with the lock pins as constraints. Markers apply per dependency edge, and extras requested by kept members are resolved from their own metadata.
5. It verifies that every resolved package is in the lock at a locked version, no excluded member came from an index, local member references are readable, and every kept member is present. It writes third-party requirements and editable member directories.
6. The installer runs `uv pip install --no-deps --no-binary lxml --no-binary xmlsec` on the requirements, installs each member directory with `uv pip install --no-deps --editable`, then runs `uv pip check`.

Current boundaries to carry into planning and validation:

- `UPGRADE_RANDOM_INDICATOR_STRING` selects the unfiltered `uv sync --resolution highest` path. Frozen-build success does not establish upgrade-build support.
- `uv pip check` checks declared `Requires-Python` as well as dependencies. Python upper bounds such as `<3.15` for `cfn-lint` and `<3.14` for `pyvespa`/`gremlinpython` can fail the build even when resolution succeeds. Exclude the owning provider or apply a marker-scoped split to the extra that requests the package; preserve required dependencies of every advertised profile.
- The gate text-matches quoted minors in provider YAML. Verify that intended exclusions activate it and agree with the generated `Requires-Python` exclusions.
- The architecture mapping covers `x86_64` and `aarch64`. Each requires its own build evidence.

## Prove the exclusion at the image boundary

Save exported constraints, helper inputs/results, editables, and build logs under `files/`. For each interpreter/platform, verify:

- Excluded providers are absent from editables, and their recorded incompatible third-party dependencies are absent from requirements and the installed image. Repair any retained member that still requests a blocker.
- Kept members' required runtime/dev/extra dependencies resolve, all resolved versions agree with the lock, and all kept members appear in the output. Source graph resolution establishes membership; exported annotations do not.
- For new-interpreter activation, apply [the wheel audit](readiness-preflight.md#4-detect-dependencies-without-target-wheels), preferring compatible upgrades and target-specific floors before exclusions.
- [Helper regression tests](../../../../scripts/tests/docker/test_filter_ci_install_set.py) cover requirement/group construction, per-edge markers/extras, lock agreement, excluded-member rejection, local references, member completeness, and end-to-end output.
- Frozen builds, `uv pip check`, imports, and relevant test collection pass on both architectures. Diagnose the first error with `breeze ci-image build --python <minor> --platform <platform> --no-upgrade-on-failure --answer yes`; track required upgrade builds separately.
- Existing interpreters that activate the filter have passing builds before merge. The `apache.cassandra` exclusion also activates this path for Python 3.14, so changes to the gate require a 3.14 image build.

**Gate:** current evidence establishes target membership, metadata compatibility, image builds, and affected existing-interpreter regressions. Metadata checks or resolver success alone do not establish a usable image.
