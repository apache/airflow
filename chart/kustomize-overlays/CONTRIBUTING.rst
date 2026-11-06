.. Licensed to the Apache Software Foundation (ASF) under one
   or more contributor license agreements.  See the NOTICE file
   distributed with this work for additional information
   regarding copyright ownership.  The ASF licenses this file
   to you under the Apache License, Version 2.0 (the
   "License"); you may not use this file except in compliance
   with the License.  You may obtain a copy of the License at

..   http://www.apache.org/licenses/LICENSE-2.0

.. Unless required by applicable law or agreed to in writing,
   software distributed under the License is distributed on an
   "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
   KIND, either express or implied.  See the License for the
   specific language governing permissions and limitations
   under the License.

Contributing Kustomize Overlays
===============================

This document is the authoritative reference for adding, evolving, and
retiring overlays under ``chart/kustomize-overlays/``.

Why this directory exists
-------------------------

The Airflow Helm chart has historically carried components that are not
Airflow-native. They make the chart heavier than it needs to be and pull
maintenance toward things that already have external owners. Expressing
these components as Kustomize overlays keeps the chart focused on Airflow
itself while still giving users a working starting point for the rest.

The chart never removes a component without a working overlay already in
place. Users always have a migration path before anything disappears.

Criteria for chart vs Kustomize
-------------------------------

A component **belongs in the chart** when all of the following are true:

* It is required to run Airflow (scheduler, API server, dag-processor,
  triggerer, workers).
* Removing/adding it requires changes to Airflow's own configuration.
* It has no external owner.
* It is used by the larger majority of users (>80%)

A component **belongs in Kustomize** when any of the following are true:

* It can be expressed as a standalone Kubernetes resource without modifying
  chart-rendered resources.
* It is environment-specific (authentication schemes, logging backends,
  autoscaling controllers, etc.).
* It has an external owner (KEDA, Elasticsearch, any PostgreSQL distribution, etc.).
* It requires CRDs that the chart does not install.
* It is used by a minority of users, such that the additional complexity and maintenance burden do not pay off

If a component qualifies for Kustomize but no overlay exists yet, it stays in
the chart until the overlay is in place and verified.

Overlay structure
-----------------

Each overlay directory must contain:

* ``kustomization.yaml`` - the Kustomize entry point.
* The Kubernetes resources the overlay produces.
* ``STATUS.yaml`` - a small YAML document declaring the verification state.
* ``README.rst`` - usage instructions and a migration guide from the
  equivalent chart-side configuration.

STATUS file format
------------------

The ``STATUS.yaml`` file is a small YAML document with the following fields.

For a verified overlay:

.. code-block:: yaml

    status: tested
    chart-version: "1.21.0"
    last-verified: "2026-04-25"

For a starting-point overlay without functional CI coverage:

.. code-block:: yaml

    status: not-tested
    reason: "Pending community validation. Use as a starting point only."

For an overlay scheduled for removal:

.. code-block:: yaml

    status: deprecated
    message: "Replaced by <overlay-name>. Will be removed in chart 3.0.0."

The optional ``verify:`` block is the smoke-test contract and is also
**the discovery key for CI**:

.. code-block:: yaml

    verify:
      timeout_seconds: 300        # optional; default 300, max 3600
      # `name` is the SUFFIX only - the runner auto-prepends
      # `<release-name>-` so the same overlay works under any release.
      # Write `foo`, not `RELEASE-NAME-foo`.
      resources:
        - kind: Deployment
          name: foo               # -> matches <release-name>-foo
          ready: true             # waits for rollout to complete
        - kind: Job
          name: bootstrap
          complete: true          # waits for condition=complete
        - kind: Secret
          name: foo               # neither flag = waits for create

How discovery works:

* ``SelectiveChecks.kustomize_overlay_names`` scans
  ``chart/kustomize-overlays/*/STATUS.yaml`` at CI time and emits the
  list of overlay directory names whose ``STATUS.yaml`` contains a
  ``verify:`` block. An overlay **without** a ``verify:`` block is
  invisible to CI - the smoke-test workflow's matrix never sees it,
  and the workflow is skipped entirely when the list is empty.
* The same workflow is gated by
  ``SelectiveChecks.run_kustomize_overlays_tests``, which only trips
  on changes under ``chart/kustomize-overlays/`` and the narrow set
  of files that drive the runner (the prek hook, the breeze command,
  the workflow file, the chart templates files). Unrelated chart edits do
  not pull in a 30-40 minute kind cluster spin-up.

Practical rule: as soon as an overlay has a ``verify:`` block, CI
starts running its smoke test on every relevant change. Until then,
the prek hook's structural check is the only automation that touches
it.

Where things live (quick reference)
-----------------------------------

A declarative map of the moving parts in an overlay and its smoke test,
so authors can answer "where does X go?" without grepping. Everything
in this table is auto-wired by the framework once it sits in the right
place - there is no central registry to also update.

+--------------------------+-----------------------------------------------------------------+
| Thing                    | Where it lives                                                  |
+==========================+=================================================================+
| Kubernetes resources the | ``chart/kustomize-overlays/<name>/*.yaml`` referenced from      |
| overlay produces         | the overlay's ``kustomization.yaml``.                           |
+--------------------------+-----------------------------------------------------------------+
| Container images the     | Inline ``image:`` fields on containers / initContainers /       |
| overlay uses             | sidecars in the overlay YAMLs above. **No second list.**        |
|                          | ``breeze k8s smoke-test-overlay`` discovers them by walking     |
|                          | the rendered manifest and ``docker pull`` + ``kind load``       |
|                          | each one before applying. Always pair with                      |
|                          | ``imagePullPolicy: IfNotPresent``.                              |
+--------------------------+-----------------------------------------------------------------+
| Resources to wait for    | ``verify:`` block in                                            |
| after apply              | ``chart/kustomize-overlays/<name>/STATUS.yaml`` (see "STATUS    |
|                          | file format" above). The smoke-test runner walks this list.     |
+--------------------------+-----------------------------------------------------------------+
| Behavioural assertions   | ``chart/tests/overlay_tests/test_<name>.py``                    |
| beyond "resource exists" | Auto-discovered by the smoke-test runner if present.            |
|                          | Use the fixtures in the sibling ``conftest.py``                 |
|                          | (``overlay_namespace``, ``overlay_release_name``, ``kubectl``,  |
|                          | ``get_secret_data``, ``run_throwaway_pod``) - no copy-paste     |
|                          | boilerplate.                                                    |
+--------------------------+-----------------------------------------------------------------+
| Images for a per-overlay | Reuse an ``image:`` already declared by the overlay so the      |
| test pod                 | auto-preload covers it. Do **not** add to                       |
|                          | ``K8S_TEST_IMAGES_TO_PRELOAD`` - that list is only for the      |
|                          | regular K8s system tests under ``kubernetes-tests/``.           |
+--------------------------+-----------------------------------------------------------------+
| Which overlays CI runs   | Auto-discovered from the filesystem by                          |
| the smoke test against   | ``SelectiveChecks.kustomize_overlay_names`` and piped to the    |
|                          | matrix via ``.github/workflows/ci-amd.yml`` /                   |
|                          | ``ci-arm.yml``. Adding a new overlay with a ``verify:`` block   |
|                          | is enough - no workflow edit required.                          |
+--------------------------+-----------------------------------------------------------------+

Lifecycle
---------

The lifecycle mirrors how providers work, just on a smaller scale. Two
checks gate the ``STATUS`` field, and they are deliberately separate.

The ``build_kustomize_overlays`` prek hook
(``scripts/ci/prek/build_kustomize_overlays.py``) runs on every commit and
applies a generic structural check to every overlay: the build succeeds, the
output parses as valid YAML, every resource has ``apiVersion``, ``kind`` and
``metadata.name``, there are no duplicate resource keys, and the
``STATUS.yaml`` schema parses (including the optional ``verify:`` block).
This is enough to catch most authoring mistakes but it does not validate
against the CRD schemas of the controllers the overlay targets, and nothing
is ever applied to a live cluster.

A functional integration test is the separate, stronger check. The
``breeze k8s smoke-test-overlay <name>`` command applies the overlay
against a running kind cluster (with the chart already installed), walks
the ``verify:`` resources from the overlay's ``STATUS.yaml``, and runs the
optional per-overlay pytest module at
``chart/tests/overlay_tests/test_<name>.py`` for
behavioural assertions. Until ``breeze k8s smoke-test-overlay <name>``
exits 0, the overlay's ``STATUS`` must stay at ``not-tested``.

In short:

* **prek hook** - structural, runs on every commit, cannot promote ``STATUS``.
* **breeze k8s smoke-test-overlay** - functional, runs against a real
  cluster locally and in CI, is the gate for advancing ``STATUS`` to
  ``tested``.

What ``smoke-test-overlay`` does for every overlay
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The runner is overlay-agnostic - everything below applies to any overlay
under ``chart/kustomize-overlays/`` with a ``verify:`` block:

1. **Render and substitute.** ``kubectl kustomize <overlay-dir>`` is run
   and the ``RELEASE-NAME`` / ``NAMESPACE`` placeholders are replaced in
   the rendered manifest.
2. **Auto-preload images into kind.** Every ``image:`` reference in the
   rendered manifest (across containers, initContainers, and any
   pod-spec-bearing kind: Deployment, StatefulSet, DaemonSet, Job,
   CronJob, Pod) is pulled to the host with the same retry-on-429 logic
   the regular K8s test suite uses, and then loaded into every kind node
   with ``kind load docker-image``. With the overlay's pods set to
   ``imagePullPolicy: IfNotPresent`` (the convention), kubelet then
   never reaches a registry during the test - so the smoke test does
   not flake on Docker Hub rate limits, registry outages, or images
   that turn private mid-flight. Image discovery is driven entirely by
   what the overlay declares; no per-overlay images list is needed.
   Discovered images are checked against the ``ALLOWED_OVERLAY_IMAGES``
   allow-list in ``dev/breeze/src/airflow_breeze/commands/kubernetes_commands.py``
   and the smoke test fails fast if an overlay references anything not on
   it, so CI never pulls an arbitrary, unreviewed image. Adding an image
   means editing both the overlay (owned by ``/chart/`` in CODEOWNERS) and
   the allow-list (owned by ``/dev/``), so a maintainer must approve it;
   and because the smoke test only runs in the committer-approved PROD
   image / k8s flow, a fork PR cannot pull a new image without that
   approval.
3. **Apply.** The substituted manifest is fed to ``kubectl apply -f -``.
4. **Walk the ``verify:`` block with fail-fast pod checks.** For each
   declared resource the runner polls the success condition (rollout
   complete / Job complete / resource created) on a tight cycle. In the
   same cycle it inspects backing pods (if any) for terminal waiting
   reasons - ``ImagePullBackOff``, ``ErrImagePull``, ``InvalidImageName``,
   ``ImageInspectError``, ``CrashLoopBackOff``,
   ``CreateContainerConfigError``, ``CreateContainerError``. The moment
   any of those appears the runner aborts with the offending reason and
   a ``kubectl describe`` dump, rather than waiting out the full
   ``timeout_seconds``.
5. **Run the optional per-overlay pytest module.** If
   ``chart/tests/overlay_tests/test_<name>.py``
   exists and ``--no-pytest`` was not passed, it is executed with
   ``OVERLAY_UNDER_TEST``, ``OVERLAY_NAMESPACE``, and
   ``OVERLAY_RELEASE_NAME`` in the environment. Tests that need to spin
   up an ad-hoc client pod (for example to exercise the overlay's data
   plane) should prefer reusing an image already declared by the
   overlay so they inherit the auto-preload for free.
6. **Clean up.** The same substituted manifest is fed to
   ``kubectl delete -f -`` (with ``--ignore-not-found``) unless
   ``--skip-cleanup`` was passed.

Rules for images and per-overlay tests
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The image-handling rules below keep the lifecycle simple for every
overlay - there is no second list to maintain, and per-overlay tests do
not need their own preload step.

* **Overlay images are auto-discovered.** Any new ``image:`` reference
  added to an overlay (containers, initContainers, sidecars, under any
  pod-spec-bearing kind) is automatically pulled and uploaded
  into kind cluster by ``smoke-test-overlay`` on the next run.
  There is no per-overlay images list. Do not add overlay images to
  ``K8S_TEST_IMAGES_TO_PRELOAD`` in
  ``dev/breeze/src/airflow_breeze/commands/kubernetes_commands.py`` -
  that list is only for images consumed by the regular K8s system tests.
* **Set ``imagePullPolicy: IfNotPresent`` on every overlay container.**
  This is what makes the preload effective: kubelet uses the
  already-loaded image and never reaches a registry at run time. Without
  it, ``:latest`` tags default to ``Always`` and pull anyway.
* **Per-overlay test pods should reuse an overlay-declared image.** When
  the optional pytest module needs to spawn a throwaway client pod (for
  example to exercise the overlay's data plane), prefer reusing the
  exact image the overlay's own containers use. That way the test
  inherits the auto-preload for free and you do not introduce a third
  image dependency. The kerberos test does this: its client pod runs the
  same ``gcavalcante8808/krb5-server`` image the KDC Deployment uses.
* **Do not assume bash or coreutils in third-party images.** Many CI-
  friendly upstream images are Alpine-based and ship only busybox sh.
  In a test pod's ``command:``, default to ``["/bin/sh", "-c", "..."]``
  and only use ``/bin/bash`` if the chosen image is documented to ship
  it. If a test fails with ``stat /bin/bash: no such file or directory``
  this is the cause.

Lifecycle steps:

* A new overlay is proposed via a PR and lands with ``status: not-tested``.
  The prek hook automatically applies the generic structural check; if the
  overlay needs invariants beyond that (for example a cross-reference
  between resources), they belong in the integration test, not in the prek
  hook.
* The same PR (or a follow-up) adds a ``verify:`` block to
  ``STATUS.yaml`` and, optionally, a per-overlay pytest module under
  ``chart/tests/overlay_tests/``. Once
  ``breeze k8s smoke-test-overlay <name>`` runs green locally **and** in
  CI, ``STATUS`` is flipped to ``tested`` with ``chart-version`` and
  ``last-verified`` filled in.
* An overlay is deprecated by setting ``status: deprecated`` together with a
  ``message`` field pointing to the replacement.
* Deprecated overlays remain for one chart major version before they are
  removed, so users always have a window to migrate.

Running the smoke test locally
------------------------------

You can advance an overlay's ``STATUS`` to ``tested`` yourself, in the
same PR that introduces the overlay, by running the smoke test against
a local kind cluster. The full sequence mirrors what
``breeze k8s run-complete-tests`` does in CI:

.. code-block:: bash

    breeze k8s deploy-cluster --rebuild-base-image
    breeze k8s deploy-airflow
    breeze k8s smoke-test-overlay <name>

If that exits 0, edit ``chart/kustomize-overlays/<name>/STATUS.yaml`` to:

.. code-block:: yaml

    status: tested
    chart-version: "<the version in chart/Chart.yaml>"
    last-verified: "<today's date, YYYY-MM-DD>"
    verify:
      ...

Commit the change, push, and CI re-runs the same command via the
``kustomize-overlays-tests`` workflow.

For iterative development, ``breeze k8s smoke-test-overlay`` accepts flags
such as ``--skip-cleanup`` (leave the overlay applied so you can poke at
resources with ``kubectl`` / ``k9s`` between runs) and ``--no-pytest`` (skip
the per-overlay pytest module while you iterate on the ``verify:`` block).
Run ``breeze k8s smoke-test-overlay --help`` for the full, authoritative
list of options.

Adding a new overlay
--------------------

1. Confirm the component meets the Kustomize criteria above.
2. Create ``chart/kustomize-overlays/<name>/`` with the required files.
3. Use placeholders such as ``RELEASE-NAME`` for values the user must fill in,
   and document the substitutions in the overlay's ``README.rst``.
4. Land the PR with ``status: not-tested``.
5. Add a row to the table in ``chart/kustomize-overlays/README.rst``.
6. Follow up with a CI test and flip ``STATUS`` to ``tested``.

Migration guide pattern
-----------------------

Each overlay ``README.rst`` should include a migration guide section with
exactly three parts:

1. **What the chart currently does** - the relevant ``values.yaml`` keys and
   the Kubernetes resources they produce today.
2. **What the overlay provides** - the equivalent resources rendered from the
   overlay.
3. **How to switch** - step-by-step instructions, with the explicit order of
   operations.

The guide must be written against the current chart template. It is not
speculative documentation.
