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
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Debugging CI Jobs in Github Actions](#debugging-ci-jobs-in-github-actions)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Debugging CI Jobs in Github Actions

The CI jobs are notoriously difficult to test, because you can only
really see results of it when you run them in CI environment, and the
environment in which they run depend on who runs them (they might be
either run in our Self-Hosted runners (with 64 GB RAM 8 CPUs) or in the
GitHub Public runners (6 GB of RAM, 2 CPUs) and the results will vastly
differ depending on which environment is used. We are utilizing
parallelism to make use of all the available CPU/Memory but sometimes
you need to enable debugging and force certain environments. Additional
difficulty is that `Build Images` workflow is `pull-request-target`
type, which means that it will always run using the `main` version - no
matter what is in your Pull Request.

There are several ways how you can debug the CI jobs when you are
maintainer.

- When you want to tests the build with all combinations of all python,
  backends etc on regular PR, add `full tests needed` label to the PR.
- When you want to test maintainer PR using public runners, add
  `public runners` label to the PR
- When you want to see resources used by the run, add
  `debug ci resources` label to the PR
- When you want to test changes to breeze that include changes to how
  images are build you should push your PR to `apache` repository not to
  your fork. This will run the images as part of the `CI` workflow
  rather than using `Build images` workflow and use the same breeze
  version for building image and testing
- When you want to test changes to `build-images.yml` workflow you
  should push your branch as `main` branch in your local fork. This will
  run changed `build-images.yml` workflow as it will be in `main` branch
  of your fork

-----

Read next about [Running CI locally](08_running_ci_locally.md)
