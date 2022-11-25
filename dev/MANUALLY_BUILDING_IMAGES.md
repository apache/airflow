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

- [Building docker images](#building-docker-images)
- [Setting environment with emulation](#setting-environment-with-emulation)
- [Setting up cache refreshing with hardware ARM/AMD support](#setting-up-cache-refreshing-with-hardware-armamd-support)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

## Building docker images

In order to build images on local hardware, you need to have the buildx plugin installed to run the build.
Also, you need to have regctl installed from https://github.com/regclient/regclient in order to tag
the multi-platform images in DockerHub. The script to build images will refuse to work if
you do not have those two installed.

You also need to have the right permissions to push the images, so you should run
`docker login` before and authenticate with your DockerHub token.

## Setting environment with emulation

According to the [official installation instructions](https://docs.docker.com/buildx/working-with-buildx/#build-multi-platform-images)
this can be achieved via:

```shell
docker run --privileged --rm tonistiigi/binfmt --install all
```

More information can be found [here](https://docs.docker.com/engine/reference/commandline/buildx_create/).

However, emulation is very slow - more than 10x slower than hardware-backed builds.

## Setting up cache refreshing with hardware ARM/AMD support

If you plan to build a number of images, it's probably better to set up a hardware remote builder
for your ARM or AMD builds (depending which platform you build images on - the "other" platform should be
remote).

This can be achieved by settings build as described in
[this blog post](https://www.docker.com/blog/speed-up-building-with-docker-buildx-and-graviton2-ec2/) and
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

Preparing regular images:

```shell script
breeze release-management release-prod-images --airflow-version "${VERSION}"
```

Preparing slim images:

```shell script
breeze release-management release-prod-images --airflow-version "${VERSION}" --slim-images
```

This will wipe Breeze cache and docker-context-files in order to make sure the build is "clean". It
also performs image verification after pushing the images.
