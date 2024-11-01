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

- [GitHub Registry Variables](#github-registry-variables)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# GitHub Registry Variables

Our CI uses GitHub Registry to pull and push images to/from by default.
Those variables are set automatically by GitHub Actions when you run
Airflow workflows in your fork, so they should automatically use your
own repository as GitHub Registry to build and keep the images as build
image cache.

The variables are automatically set in GitHub actions

| Variable                      | Default          | Comment                                                               |
|-------------------------------|------------------|-----------------------------------------------------------------------|
| GITHUB_REPOSITORY             | `apache/airflow` | Prefix of the image. It indicates which registry from GitHub to use.  |
| CONSTRAINTS_GITHUB_REPOSITORY | `apache/airflow` | Repository where constraints are stored                               |
| GITHUB_USERNAME               |                  | Username to use to login to GitHub                                    |
| GITHUB_TOKEN                  |                  | Token to use to login to GitHub. Only used when pushing images on CI. |

The Variables beginning with `GITHUB_` cannot be overridden in GitHub
Actions by the workflow. Those variables are set by GitHub Actions
automatically and they are immutable. Therefore if you want to override
them in your own CI workflow and use `breeze`, you need to pass the
values by corresponding `breeze` flags `--github-repository`,
`--github-token` rather than by setting them as environment variables in
your workflow. Unless you want to keep your own copy of constraints in
orphaned `constraints-*` branches, the `CONSTRAINTS_GITHUB_REPOSITORY`
should remain `apache/airflow`, regardless in which repository the CI
job is run.

One of the variables you might want to override in your own GitHub
Actions workflow when using `breeze` is `--github-repository` - you
might want to force it to `apache/airflow`, because then the cache from
`apache/airflow` repository will be used and your builds will be much
faster.

Example command to build your CI image efficiently in your own CI
workflow:

``` bash
# GITHUB_REPOSITORY is set automatically in Github Actions so we need to override it with flag
#
breeze ci-image build --github-repository apache/airflow --python 3.10
docker tag ghcr.io/apache/airflow/main/ci/python3.10 your-image-name:tag
```

-----

Read next about [Selective checks](04_selective_checks.md)
