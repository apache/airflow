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
  - [Setting up cache refreshing with emulation](#setting-up-cache-refreshing-with-emulation)
  - [Setting up cache refreshing with hardware ARM/AMD support](#setting-up-cache-refreshing-with-hardware-armamd-support)
  - [How to refresh the image](#how-to-refresh-the-image)

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
breeze build-image --run-in-parallel --upgrade-to-newer-dependencies --answer yes
breeze generate-constraints --airflow-constraints-mode constraints --run-in-parallel --answer yes
breeze generate-constraints --airflow-constraints-mode constraints-source-providers --run-in-parallel --answer yes
breeze generate-constraints --airflow-constraints-mode constraints-no-providers --run-in-parallel --answer yes

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
but you should also make sure that you have the buildkit builder configured and set. Since we also build
multi-platform images (for both AMD and ARM), you need to have support for qemu or hardware ARM/AMD builders
configured.



## Setting up cache refreshing with emulation

According to the [official installation instructions](https://docs.docker.com/buildx/working-with-buildx/#build-multi-platform-images)
this can be achieved via:

```shell
docker run --privileged --rm tonistiigi/binfmt --install all
```

More information can be found [here](https://docs.docker.com/engine/reference/commandline/buildx_create/)

However, emulation is very slow - more than 10x slower than hardware-backed builds.

## Setting up cache refreshing with hardware ARM/AMD support

If you plan to build  a number of images, probably better solution is to set up a hardware remote builder
for your ARM or AMD builds (depending which platform you build images on - the "other" platform should be
remote.

This  can be achieved by settings build as described in
[this guideline](https://www.docker.com/blog/speed-up-building-with-docker-buildx-and-graviton2-ec2/) and
adding it to docker buildx `airflow_cache` builder.

This usually can be done with those two commands:

```bash
docker buildx create --name airflow_cache   # your local builder
docker buildx create --name airflow_cache --append HOST:PORT  # your remote builder
```

One of the ways to have HOST:PORT is to login to the remote machine via SSH and forward the port to
the docker engine running on the remote machine.

When everything is fine you should see both local and remote builder configured and reporting status:

```bash
docker buildx ls

  airflow_cache          docker-container
       airflow_cache0    unix:///var/run/docker.sock
       airflow_cache1    tcp://127.0.0.1:2375
```

## How to refresh the image

The images can be rebuilt and refreshed after the constraints are pushed. Refreshing image for all
python version sis a simple as running the [refresh_images.sh](refresh_images.sh) script which will
sequentially rebuild all the images. Usually building several images in parallel on one machine does not
speed up the build significantly, that's why the images are build sequentially.

```bash
./dev/refresh_images.sh
```
