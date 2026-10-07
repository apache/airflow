<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# Centralize repeated version policy

Use this guidance for repository-wide or provider-only support changes when many files need the same policy change.

1. **Group by meaning.** Identify which literals express the same minimum, supported-version list, or default. Core, independently released providers, tooling, and production images can have different policies. Keep historical release pins and fixed feature-introduction thresholds distinct from a moving support minimum.
2. **Reuse or define one source per policy.** Prefer an existing canonical list or constant; derive its minimum/default where that relationship is intentional. For a standalone minimum, a definition such as `MIN_PYTHON_VERSION = "3.11"` can replace repeated literals. Import that definition in consumers that share its policy; use numeric version components for comparisons rather than ordering version strings.
3. **Respect distribution boundaries.** Put runtime constants in a lightweight module available in every consuming installation. Reuse an existing shared distribution only where it is already a dependency. Tooling can use a lightweight reader instead of importing Airflow or Breeze and their dependencies; existing readers in `scripts/ci/prek/common_prek_utils.py` illustrate this approach. Avoid introducing cross-package dependencies solely to share a constant, or imposing core's minimum on independently versioned providers.
4. **Generate declarative consumers.** Package metadata, PEP 723 headers, YAML matrices, Dockerfiles, and documentation may need literal values. Derive them through their owning templates/generators or validate them against the canonical policy. Keep installer-readable metadata available without importing the package. Centralization reduces manual edits even when many generated files still change.
5. **Place the refactor before the policy change.** When independently viable, introduce the shared source/readers with existing values in a prerequisite layer, then change policy and regenerate derivatives in the support-boundary/activation layer. Fold inseparable changes together. Keep this focused on the repeated policy rather than expanding into a broad monorepo refactor.

**Gate:** one policy edit reaches all consumers of that policy. Verify imports in the actual installed distributions and generators in their actual runners; check derived outputs and maintenance loops. When changing a reader/generator, add a regression proving it consumes the canonical value without a second literal edit. Explain remaining literals as independent policy, historical pins, fixed compatibility thresholds, or generated declarations.
