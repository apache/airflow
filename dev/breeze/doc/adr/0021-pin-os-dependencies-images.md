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

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->

- [21. Pin the OS dependencies of the CI and release images](#21-pin-the-os-dependencies-of-the-ci-and-release-images)
  - [Status](#status)
  - [Context](#context)
  - [Decision](#decision)
  - [Consequences](#consequences)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# 21. Pin the OS dependencies of the CI and release images

Date: 2026-11-01

## Status

Accepted

## Context

The CI image and the default PROD image are built on Docker Hardened Images: a mirror of
`dhi.io/python` in `ghcr.io/apache/airflow/base/python`, pinned to a digest that only the
"Upgrade important CI environment" PR moves, after the full CI passed on it. The pin was meant to
make a commit build the same image every time.

It cannot. The OS dependencies - system packages, compilers, the database clients - are installed
with `apt-get` from the `dhi.io` apt repository, and that repository is live. Docker rebuilds the
hardened images several times a day and moves the repository on with them, dropping the package
versions it replaced. In the two weeks before this decision the CI image build on `main` broke three
times without any change in the repository, most recently when Docker renumbered its builds
(`14.2.0-19+dhi5` became `14.2.0-19+deb13u0+dhi5`, which sorts lower) and the pinned image kept the
old numbers the repository no longer offered (docker-hardened-images/catalog#689,
[#76200](https://github.com/apache/airflow/pull/76200)). Each such break blocks every PR at once,
and the same commit can pass on one day and fail on the next.

The project's way of taking in new, potentially breaking dependencies is to synchronize with them
*eventually*: an upgrade PR moves them, the full CI runs on it, and only a green run reaches `main`,
so a breaking upstream release breaks that one PR rather than everyone's. The OS dependencies
bypassed that path entirely.

## Decision

1. **The OS dependencies are installed once, into images of their own, and pinned like the base
   images.** `install_os_dependencies.sh` - which now also installs the MySQL, MSSQL and Postgres
   clients - runs in dedicated Dockerfile stages (`ci-os-deps` in `Dockerfile.ci`, `dev-os-deps` and
   `runtime-os-deps` in `Dockerfile`). Those stages start from `CI_OS_DEPS_IMAGE`, `DEV_OS_DEPS_IMAGE`
   and `RUNTIME_OS_DEPS_IMAGE`, which default to `BASE_IMAGE`. The images built from them are
   published as `ghcr.io/apache/airflow/base/os-deps` and pinned in
   `dev/breeze/src/airflow_breeze/files/hardened_os_deps_image_digests.json`. CI and release builds
   pass the pinned images, so day-to-day CI installs no system package from a live repository.

2. **They are rebuilt daily, and pinned only through the upgrade PR.** The "Mirror hardened base
   images" workflow builds and publishes them once a day, natively per platform, and only when an
   input changed: every input - the base image digest, the scripts and keys, the Dockerfile stage and
   the build args - is part of the image's tag. The upgrade check moves their pins together with the
   base image pins, in the PR that must pass the full CI. A `dhi.io` change can then break the daily
   build or the upgrade PR, but never `main`.

3. **A marker makes a pinned image a cache, not a constraint.** The script records a hash of
   everything that decides what it installs. On an image whose marker matches, it installs nothing;
   on a mismatch it installs on top, so no build runs on OS dependencies that do not match its
   inputs. A PR that changes the OS-dependency scripts or repository keys does not use the pins at
   all in CI: installing on top would only test additions, as whatever the PR drops would still be
   there from the pinned layer. It builds from `BASE_IMAGE` and tests its change in full, and the
   next upgrade pins a fresh image.

4. **Only the default build uses the pins.** Builds with a custom base image, a non-default Debian
   version, the legacy flavor, custom apt dependencies or custom database clients start from
   `BASE_IMAGE` and install live, as before. `--no-use-pinned-os-deps-images`
   (`USE_PINNED_OS_DEPS_IMAGES=false`) turns the pins off; in CI the "disable image cache" label and
   a change to the OS-dependency scripts or keys (point 3) do. A plain `docker build` of either Dockerfile is unchanged.

5. **Only `main` carries pins.** The images are built from `main`'s script and Dockerfiles; on a
   release branch their marker would never match and the branch would ship both sets of
   dependencies. The upgrade leaves the pins file empty on any other branch until images are built
   per branch.

### Alternatives considered

* **Pin every package version.** A lock file of `package=version` fails as soon as the repository
  drops a pinned version, which is exactly the failure being fixed.
* **Install from a dated snapshot of the repository**, as `snapshot.debian.org` offers for Debian.
  `dhi.io` publishes no such snapshot.
* **Mirror the `.deb` files ourselves.** It works, but means operating, signing and cleaning up an
  apt repository - more infrastructure than the problem warrants, when an image registry and the
  existing upgrade flow already provide pinning and promotion.
* **Only repair renumbered packages at build time** (the alignment step from
  [#76200](https://github.com/apache/airflow/pull/76200)). It keeps a live install working through
  one class of change, but the image still differs from day to day and the next kind of change breaks
  `main` again. It stays as the safety net of every live install.

## Consequences

* CI and release builds are reproducible for a given commit, and a `dhi.io` change can no longer
  break every PR at once. New system packages arrive within days, through the upgrade PR.
* The Dockerfiles gain three stages and three build args, and every image records where its OS
  dependencies came from (`AIRFLOW_OS_DEPS_IMAGE`, `/usr/share/airflow/os-dependencies.base-image`),
  as that can be an older base digest than `BASE_IMAGE`.
* The daily build runs on two platforms for every supported Python version; it is skipped for any
  image whose inputs are unchanged.
* The `ghcr.io/apache/airflow/base/os-deps` package has to be public, like `base/python`, so
  contributors and CI can pull it without credentials.
* Release branches keep installing live until images are built per branch.
