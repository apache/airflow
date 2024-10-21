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

- [Debugging CI Jobs in Github Actions and changing their behaviour](#debugging-ci-jobs-in-github-actions-and-changing-their-behaviour)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Debugging CI Jobs in Github Actions and changing their behaviour

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

There are several ways how you can debug the CI jobs and modify their
behaviour when you are maintainer.

When you create the PR you can set one of the labels below, also
in some cases, you need to run the PR as coming from the "apache"
repository rather than from your fork.

You can also apply the label later and rebase the PR or close/reopen
the PR to apply the label to the PR.

| Action to perform                                                                                                                                                | Label to set          | PR from "apache" repo |
|------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------------|:---------------------:|
| Run the build with all combinations of all<br>python, backends, kubernetes etc on PR, <br>and run all types of tests for all test<br>groups.                     | full tests needed     |                       |
| Force to use public runners for the build                                                                                                                        | use public runners    |                       |
| Debug resources used during the build for <br>parallel jobs                                                                                                      | debug ci resources    |                       |
| Force running PR on latest versions of<br>python, backends, kubernetes etc. when you<br>want to save resources and test only latest<br>versions                  | latest versions only  |                       |
| Force running PR on minimal (default) <br>versions of python, backends, kubernetes etc.<br>in order to save resources and run tests only<br>for minimum versions | default versions only |                       |
| Make sure to clean dependency cache<br>usually when removing dependencies<br>You also need to increase<br> `DEPENDENCIES_EPOCH_NUMBER` in `Dockerfile.ci`        | disable image cache   |                       |
| Change build images workflows, breeze code or<br>scripts that are used during image build<br>so that the scripts can be modified by PR<br>                       |                       |          Yes          |
| Treat your build as "canary" build - including<br>updating constraints and pushing "main"<br>documentation.                                                      |                       |          Yes          |
| Remove any behaviour specific for the committers<br>such as using different runners by default.                                                                  | non committer build   |                       |


-----

Read next about [Running CI locally](08_running_ci_locally.md)
