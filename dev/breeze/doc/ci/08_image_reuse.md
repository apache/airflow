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

- [Publishing reusable main images](#publishing-reusable-main-images)
  - [Build inputs and verification](#build-inputs-and-verification)
  - [Reruns and retention](#reruns-and-retention)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Publishing reusable main images

The `.github/workflows/publish-main-images.yml` workflow publishes CI images, complete production
images, and production dependency layers for both AMD64 and ARM64. It runs daily, on changes to
its build inputs, and on manual dispatch. Publication requires the `apache/airflow` repository
and the `main` branch. The concurrency group lets an active publication finish before another starts.

## Build inputs and verification

Each platform builds CI environments with frozen dependencies, generates constraints, and builds
production packages and images. A failed frozen CI install fails publication rather than resolving
a different dependency set. CI and production images are verified before upload.

Artifact names include the image kind, Python version, architecture, and a fingerprint of the
checkout inputs, build options, immutable Debian base digest, and applicable constraints.
Unknown inputs invalidate compatibility. Production dependency artifacts contain a BuildKit local
cache exported with `mode=max`; they are not archives accepted by `docker load`.

The [production dependency layer](02_images.md#reusing-production-dependency-layers) retains
resolver metadata while allowing application wheels to be installed separately. Publishers build
without importing an older shared dependency cache, allowing mutable external inputs to refresh.

## Reruns and retention

Package and constraint intermediates have an architecture prefix and may be replaced on partial
reruns. Published images and dependency caches remain immutable: a retry preserves an existing
artifact with the exact identity instead of replacing it. Artifact selection validates the successful
publisher attempt, repository, branch, event, workflow path, source commit, and archive digest.

Shared artifacts have seven-day retention. The selection utility accepts new candidates only within
48 hours of creation, while retained selections can still download their pinned artifact afterwards.
Record archive sizes and publication frequency to budget storage, including intermediate BuildKit
layers. Retention bounds storage without deleting an artifact as soon as a newer publication appears.
