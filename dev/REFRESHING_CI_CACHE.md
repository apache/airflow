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

- [Automated cache refreshing in CI](#automated-cache-refreshing-in-ci)
- [Manually generating constraint files](#manually-generating-constraint-files)
- [Manually refreshing the images](#manually-refreshing-the-images)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Automated cache refreshing in CI

Our [CI system](../CI.rst) is build in the way that it self-maintains. Regular scheduled builds and
merges to `main` branch have separate maintenance step that take care about refreshing the cache that is
used to speed up our builds and to speed up rebuilding of [Breeze](../BREEZE.rst) images for development
purpose. This is all happening automatically, usually:

* The latest [constraints](../CONTRIBUTING.rst#pinned-constraint-files) are pushed to appropriate branch
  after all tests succeeded in `main` merge or in `scheduled` build

* The [images](../IMAGES.rst) in `ghcr.io` registry are refreshed after every successful merge to `main`
  or `scheduled` build and after pushing the constraints, this means that the latest image cache uses
  also the latest tested constraints

Sometimes however, when we have prolonged period of fighting with flakiness of GitHub Actions runners or our
tests, the refresh might not be triggered - because tests will not succeed for some time. In this case
manual refresh might be needed.

# Manually generating constraint files

```bash
export CURRENT_PYTHON_MAJOR_MINOR_VERSIONS_AS_STRING="3.7 3.8 3.9 3.6"
for python_version in $(echo "${CURRENT_PYTHON_MAJOR_MINOR_VERSIONS_AS_STRING}")
do
  ./breeze build-image --upgrade-to-newer-dependencies --python ${python_version}
done

GENERATE_CONSTRAINTS_MODE="pypi-providers" ./scripts/ci/constraints/ci_generate_all_constraints.sh
GENERATE_CONSTRAINTS_MODE="source-providers" ./scripts/ci/constraints/ci_generate_all_constraints.sh
GENERATE_CONSTRAINTS_MODE="no-providers" ./scripts/ci/constraints/ci_generate_all_constraints.sh

AIRFLOW_SOURCES=$(pwd)
```

The constraints will be generated in `files/constraints-PYTHON_VERSION/constraints-*.txt` files. You need to
check out the right 'constraints-' branch in a separate repository, and then you can copy, commit and push the
generated files:

```bash
cd <AIRFLOW_WITH_CONSTRAINTS-MAIN_DIRECTORY>
git pull
cp ${AIRFLOW_SOURCES}/files/constraints-*/constraints*.txt .
git diff
git add .
git commit -m "Your commit message here" --no-verify
git push
```

# Manually refreshing the images

Note that in order to refresh images you have to not only have `buildx` command installed for docker,
but you should also make sure that you have the buildkit builder configured and set.

More information can be found [here](https://docs.docker.com/engine/reference/commandline/buildx_create/)

The images can be rebuilt and refreshed after the constraints are pushed. Refreshing image for particular
python version is a simple as running the [refresh_images.sh](refresh_images.sh) script with python version
as parameter:

```bash
./dev/refresh_images.sh 3.9
```

If you have fast network and powerful computer, you can refresh the images in parallel running the
[refresh_images.sh](refresh_images.sh) with all python versions. You might do it with `tmux` manually
or with gnu parallel:

```bash
parallel -j 4 --linebuffer --tagstring '{}' ./dev/refresh_images.sh ::: 3.7 3.8 3.9 3.6
```
