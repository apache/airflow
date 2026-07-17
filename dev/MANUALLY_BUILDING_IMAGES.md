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
- [Keeping your docker context small](#keeping-your-docker-context-small)
- [Setting environment with emulation](#setting-environment-with-emulation)
- [Setting up airflow_cache builder for your local hardware](#setting-up-airflow_cache-builder-for-your-local-hardware)
- [Building images separately on different hardware and merging them into multi-platform image](#building-images-separately-on-different-hardware-and-merging-them-into-multi-platform-image)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

## Building docker images

In order to build images on local hardware, you need to have the buildx plugin installed to run the build.
Also, you need to have regctl installed from https://github.com/regclient/regclient in order to tag
the multi-platform images in DockerHub. The script to build images will refuse to work if
you do not have those two installed.

You also need to have the right permissions to push the images, so you should run
`docker login` before and authenticate with your DockerHub token.

## Keeping your docker context small

Sometimes, especially when you generate node assets, some of the files generated are kept in the source
directory. This can make the docker context very large when building images, because the whole context
is transferred to the docker daemon. In order to avoid this we have .dockerignore where we exclude certain
paths from being treated as part of the context - similar to .gitignore that keeps them away from git.

If your context gets large you see a long (minutes) preliminary step before docker build is run
where the context is being transmitted.

You can see all the context files by running:

```shell script
printf 'FROM scratch\nCOPY . /' | DOCKER_BUILDKIT=1 docker build -q -f- -o- . | tar t
```

Once you see something that should be excluded from the context, you should add it to `.dockerignore` file.

You can also check the size of the context by running:

```shell script
printf 'FROM scratch\nCOPY . /' | DOCKER_BUILDKIT=1 docker build -q -f- -o- . | wc -c | numfmt --to=iec --suffix=B
```


## Setting environment with emulation

According to the [official installation instructions](https://docs.docker.com/buildx/working-with-buildx/#build-multi-platform-images)
this can be achieved via:

```shell
docker run --privileged --rm tonistiigi/binfmt --install all
```

More information can be found [here](https://docs.docker.com/engine/reference/commandline/buildx_create/).

However, emulation is very slow - more than 10x slower than hardware-backed builds.

## Setting up airflow_cache builder for your local hardware

> [!WARNING]
>
> You need to have at least 0.13 version of the docker buildx plugin installed with your container engine to
> perform manual release of the images. See https://github.com/docker/buildx?tab=readme-ov-file#installing
> for instructions on how to install and upgrade the plugin.

If you plan to build a number of images and have two hardware (AMD linux and ARM Mac for example available in
the same network, you need to set up a hardware builder that will build them in parallel using both hardware
options. This usually can be done with those two commands - providing that you have HOST:PORT of the
remote hardware available to securely connect to.

The benefit of this method is that the images can be built and pushed in a single step - there is no need
to build and push them separately and perform the additional manifest merge step.

```bash
docker buildx create --name airflow_cache --driver docker-container unix:///var/run/docker.sock  # your local builder (you might want to use
docker buildx create --name airflow_cache --append HOST:PORT  # your remote builder
```

One of the ways to have HOST:PORT is to login to the remote machine via SSH and forward the port to
the docker engine running on the remote machine.

Note that you need relatively new `buildx plugin` installed (0.23 is used at the moment of writing this
in order to use `docker-container` driver - which is necessary to build multi-platform images).
Images build with this driver are built in a separate container - and unless you use `--load` or `--push` options
of building the image, they are not available in the local docker daemon.

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


## Building images separately on different hardware and merging them into multi-platform image

You can also build your multi-platform images separately on different hardware and then push digest-only
images that can later be merged with a manifest into a single multi-platform image. This is what
we are using in the CI pipeline where we build our images on separate hardware instances and merge the images
into a multi-platform image as a separate step.

On each hardware you build you must set up the same `airflow_cache` docker-container builder as in the
previous step - but you do not need to append the remote builder to the local one.

```bash
docker buildx create --name airflow_cache --driver docker-container unix:///var/run/docker.sock
```

You prepare the images separately by specifying the folder where digest-metadata of the images should be stored

Preparing AMD image (on AMD hardware):

```shell script
breeze release-management release-prod-images --airflow-version "${VERSION}" --platform linux/amd64 --metadata-folder dist
```

Preparing ARM image (on ARM hardware):

```shell script
breeze release-management release-prod-images --airflow-version "${VERSION}" --platform linux/arm64 --metadata-folder dist
```

These commands will push the digest-only image to the registry and store the metadata in the specified folder
with the `metadata-AIRFLOW_VERSION-linux_amd64-PYTHON_VERSION.json` and
`metadata-AIRFLOW_VERSION-linux_arm64-PYTHON_VERSION.json` names (AIRFLOW_VERSION/PYTHON_VERSION replaced
with appropriate versions).
You can then merge the images into a single multi-platform image with:

```bash
docker release-management merge-prod-images --airflow-version "${VERSION}" --metadata-folder dist
```

The same can be repeated for slim images by adding `--slim-images` option to the command, the manifests are
named `metadata-AIRFLOW_VERSION-slim-linux_amd64-PYTHON_VERSION.json` and
`metadata-AIRFLOW_VERSION-slim-linux_arm64-PYTHON_VERSION.json`.

Preparing AMD image (on AMD hardware):

```shell script
breeze release-management release-prod-images --slim-images --airflow-version "${VERSION}" --platform linux/amd64 --metadata-folder dist
```

Preparing ARM image (on ARM hardware):

```shell script
breeze release-management release-prod-images --slim-images --airflow-version "${VERSION}" --platform linux/arm64 --metadata-folder dist
```

Merging the images:

```bash
docker release-management merge-prod-images --slim-images --airflow-version "${VERSION}" --metadata-folder dist
```
