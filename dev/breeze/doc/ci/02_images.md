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

- [Airflow Docker images](#airflow-docker-images)
  - [PROD image](#prod-image)
  - [CI image](#ci-image)
- [Building docker images from current sources](#building-docker-images-from-current-sources)
- [Building PROD docker images from released PIP packages](#building-prod-docker-images-from-released-pip-packages)
- [Using docker cache during builds](#using-docker-cache-during-builds)
- [Naming conventions](#naming-conventions)
- [Customizing the CI image](#customizing-the-ci-image)
  - [CI image build arguments](#ci-image-build-arguments)
  - [Running the CI image](#running-the-ci-image)
- [Naming conventions for stored images](#naming-conventions-for-stored-images)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Airflow Docker images

Airflow has two main images (build from Dockerfiles):

- Production image (Dockerfile) - that can be used to build your own
  production-ready Airflow installation. You can read more about
  building and using the production image in the
  [Docker stack](https://airflow.apache.org/docs/docker-stack/index.html)
  documentation. The image is built using [Dockerfile](../../../../Dockerfile).
- CI image (Dockerfile.ci) - used for running tests and local
  development. The image is built using [Dockerfile.ci](../../../../Dockerfile.ci).

## PROD image

The PROD image is a multi-segment image. The first segment
`airflow-build-image` contains all the build essentials and related
dependencies that allow to install airflow locally. By default the image
is built from a released version of Airflow from GitHub, but by
providing some extra arguments you can also build it from local sources.
This is particularly useful in CI environment where we are using the
image to run Kubernetes tests. See below for the list of arguments that
should be provided to build production image from the local sources.

The image is primarily optimised for size of the final image, but also
for speed of rebuilds - the `airflow-build-image` segment uses the same
technique as the CI jobs for pre-installing dependencies. It first
pre-installs them from the right GitHub branch and only after that final
airflow installation is done from either local sources or remote
location (PyPI or GitHub repository).

You can read more details about building, extending and customizing the
PROD image in the [Latest
documentation](https://airflow.apache.org/docs/docker-stack/index.html)

## CI image

The CI image is used by [Breeze](../README.rst) as the shell
image but it is also used during CI tests. The image is single segment
image that contains Airflow installation with "all" dependencies
installed. It is optimised for rebuild speed. It installs PIP
dependencies from the current branch first -so that any changes in
`pyproject.toml` do not trigger reinstalling of all dependencies. There
is a second step of installation that re-installs the dependencies from
the latest sources so that we are sure that latest dependencies are
installed.

# Building docker images from current sources

The easy way to build the CI/PROD images is to use
[Breeze](../README.rst). It uses a number
of optimization and caches to build it efficiently and fast when you are
developing Airflow and need to update to latest version.

For CI image: Airflow package is always built from sources. When you
execute the image, you can however use the `--use-airflow-version` flag
(or `USE_AIRFLOW_VERSION` environment variable) to remove the
preinstalled source version of Airflow and replace it with one of the
possible installation methods:

- "none" - airflow is removed and not installed
- "wheel" - airflow is removed and replaced with "wheel" version
  available in dist
- "sdist" - airflow is removed and replaced with "sdist" version
  available in dist
- "\<VERSION\>" - airflow is removed and installed from PyPI (with the
  specified version)

For PROD image: By default production image is built from the latest
sources when using Breeze, but when you use it via docker build command,
it uses the latest installed version of airflow and providers. However,
you can choose different installation methods as described in [Building
PROD docker images from released PIP packages](#building-prod-docker-images-from-released-pip-packages). Detailed
reference for building production image from different sources can be
found in: [Build Args reference](../../../../docs/docker-stack/build-arg-ref.rst#installing-airflow-using-different-methods)

You can build the CI image using current sources this command:

``` bash
breeze ci-image build
```

You can build the PROD image using current sources with this command:

``` bash
breeze prod-image build
```

By adding `--python <PYTHON_MAJOR_MINOR_VERSION>` parameter you can
build the image version for the chosen Python version.

The images are built with default extras - different extras for CI and
production image and you can change the extras via the `--airflow-extras`
parameters and add new ones with `--additional-airflow-extras`.

For example if you want to build Python 3.10 version of production image
with "all" extras installed you should run this command:

``` bash
breeze prod-image build --python 3.10 --airflow-extras "all"
```

If you just want to add new extras you can add them like that:

``` bash
breeze prod-image build --python 3.10 --additional-airflow-extras "all"
```

The command that builds the CI image is optimized to minimize the time
needed to rebuild the image when the source code of Airflow evolves.
This means that if you already have the image locally downloaded and
built, the scripts will determine whether the rebuild is needed in the
first place. Then the scripts will make sure that minimal number of
steps are executed to rebuild parts of the image (for example, PIP
dependencies) and will give you an image consistent with the one used
during Continuous Integration.

The command that builds the production image is optimised for size of
the image.

# Building PROD docker images from released PIP packages

You can also build production images from PIP packages via providing
`--install-airflow-version` parameter to Breeze:

``` bash
breeze prod-image build --python 3.10 --additional-airflow-extras=trino --install-airflow-version=2.0.0
```

This will build the image using command similar to:

``` bash
pip install \
  apache-airflow[async,amazon,celery,cncf.kubernetes,docker,elasticsearch,ftp,grpc,hashicorp,http,ldap,google,microsoft.azure,mysql,postgres,redis,sendgrid,sftp,slack,ssh,statsd,virtualenv]==2.0.0 \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.0.0/constraints-3.10.txt"
```

> [!NOTE]
> Only `pip` installation is currently officially supported.
>
> While they are some successes with using other tools like
> [poetry](https://python-poetry.org/) or
> [pip-tools](https://pypi.org/project/pip-tools/), they do not share
> the same workflow as `pip` - especially when it comes to constraint
> vs. requirements management. Installing via `Poetry` or `pip-tools` is
> not currently supported.
>
> There are known issues with `bazel` that might lead to circular
> dependencies when using it to install Airflow. Please switch to `pip`
> if you encounter such problems. `Bazel` community works on fixing the
> problem in [this
> PR](https://github.com/bazelbuild/rules_python/pull/1166) so it might
> be that newer versions of `bazel` will handle it.
>
> If you wish to install airflow using those tools you should use the
> constraint files and convert them to appropriate format and workflow
> that your tool requires.

You can also build production images from specific Git version via
providing `--install-airflow-reference` parameter to Breeze (this time
constraints are taken from the `constraints-main` branch which is the
HEAD of development for constraints):

``` bash
pip install "https://github.com/apache/airflow/archive/<tag>.tar.gz#egg=apache-airflow" \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-main/constraints-3.10.txt"
```

You can also skip installing airflow and install it from locally
provided files by using `--install-distributions-from-context` parameter to
Breeze:

``` bash
breeze prod-image build --python 3.10 --additional-airflow-extras=trino --install-distributions-from-context
```

In this case you airflow and all packages (.whl files) should be placed
in `docker-context-files` folder.

# Using docker cache during builds

Default mechanism used in Breeze for building CI images locally uses images
pulled from GitHub Container Registry combined with locally mounted cache
folders where `uv` cache is stored. This is done to speed up local
builds and building images for CI runs - instead of \> 12 minutes for
rebuild of CI images, it takes usually less than a minute when cache is
used. For CI images this is usually the best strategy - to use default
"pull" cache. This is default strategy when [Breeze](../README.rst)
builds are performed.

For Production Image - which is far smaller and faster to build, it's
better to use local build cache (the standard mechanism that docker
uses. This is the default strategy for production images when
[Breeze](../README.rst) builds are
performed. The local `uv` cache is used from mounted sources.
The first time you run it, it will take considerably longer
time than if you use the pull mechanism, but then when you do small,
incremental changes to local sources, Dockerfile image and scripts,
further rebuilds with local build cache will be considerably faster.

You can also disable build cache altogether. This is the strategy used
by the scheduled builds in CI - they will always rebuild all the images
from scratch.

You can change the strategy by providing one of the `--build-cache`
flags: `registry` (default), `local`, or `disabled` flags when you run
Breeze commands. For example:

``` bash
breeze ci-image build --python 3.10 --docker-cache local
```

Will build the CI image using local build cache (note that it will take
quite a long time the first time you run it).

``` bash
breeze prod-image build --python 3.10 --docker-cache registry
```

Will build the production image with cache used from registry.

``` bash
breeze prod-image build --python 3.10 --docker-cache disabled
```

Will build the production image from the scratch.

You can also turn local docker caching by setting `DOCKER_CACHE`
variable to `local`, `registry`, `disabled` and exporting it.

``` bash
export DOCKER_CACHE="registry"
```

or

``` bash
export DOCKER_CACHE="local"
```

or

``` bash
export DOCKER_CACHE="disabled"
```

# Naming conventions

By default images we are using cache for images in GitHub Container
registry. We are using GitHub Container Registry as development image
cache and CI registry for build images. The images are all in
organization wide "apache/" namespace. We are adding "airflow-" as
prefix for the image names of all Airflow images. The images are linked
to the repository via `org.opencontainers.image.source` label in the
image.

See
<https://docs.github.com/en/packages/learn-github-packages/connecting-a-repository-to-a-package>

Naming convention for the GitHub packages.

``` bash
ghcr.io/apache/airflow/<BRANCH>/ci/python<X.Y>         - for CI images
ghcr.io/apache/airflow/<BRANCH>/prod/python<X.Y>       - for production images
```

You can see all the current GitHub images at <https://github.com/apache/airflow/packages>

Note that you need to be committer and have the right to refresh the
images in the GitHub Registry with latest sources from main via
(./dev/refresh_images.sh). Only committers can push images directly. You
need to login with your Personal Access Token with "packages" write
scope to be able to push to those repositories or pull from them in case
of GitHub Packages.

You need to login to GitHub Container Registry with your API token
if you want to interact with the GitHub Registry for writing (only
committers).

``` bash
docker login ghcr.io
```

Note that when your token is expired and you are still
logged in, you are not able to interact even with read-only operations
like pulling images. You need to logout and login again to refresh the
token.

``` bash
docker logout ghcr.io
```

Since there are different naming conventions used for Airflow images and
there are multiple images used, [Breeze](../README.rst)
provides easy to use management interface for the images. The CI
is designed in the way that it should automatically
refresh caches, rebuild the images periodically and update them whenever
new version of base Python is released. However, occasionally, you might
need to rebuild images locally and push them directly to the registries
to refresh them.

Every contributor can also pull and run images being result of a specific
CI run in GitHub Actions. This is a powerful tool that allows to
reproduce CI failures locally, enter the images and fix them much
faster. It is enough to download and uncompress the artifact that stores the
image and run ``breeze ci-image load -i <path-to-image.tar>`` to load the
image and mark the image as refreshed in the local cache.

You can see more details and examples in[Breeze](../06_managing_docker_images.rst)

# Customizing the CI image

Customizing the CI image allows to add your own dependencies to the
image.

The easiest way to build the customized image is to use `breeze` script,
but you can also build suc customized image by running appropriately
crafted docker build in which you specify all the `build-args` that you
need to add to customize it. You can read about all the args and ways
you can build the image in the
[\#ci-image-build-arguments](#ci-image-build-arguments) chapter below.

Here just a few examples are presented which should give you general
understanding of what you can customize.

This builds the production image in version 3.10 with additional airflow
extras from 2.0.0 PyPI package and additional apt dev and runtime
dependencies.

As of Airflow 2.3.0, it is required to build images with
`DOCKER_BUILDKIT=1` variable (Breeze sets `DOCKER_BUILDKIT=1` variable
automatically) or via `docker buildx build` command if you have `buildx`
plugin installed.

``` bash
DOCKER_BUILDKIT=1 docker build . -f Dockerfile.ci \
  --pull \
  --build-arg PYTHON_BASE_IMAGE="python:3.10-slim-bookworm" \
  --build-arg ADDITIONAL_AIRFLOW_EXTRAS="jdbc" \
  --build-arg ADDITIONAL_PYTHON_DEPS="pandas" \
  --build-arg ADDITIONAL_DEV_APT_DEPS="gcc g++" \
  --tag my-image:0.0.1
```

the same image can be built using `breeze` (it supports auto-completion
of the options):

``` bash
breeze ci-image build --python 3.10 --additional-airflow-extras=jdbc --additional-python-deps="pandas" \
    --additional-dev-apt-deps="gcc g++"
```

You can customize more aspects of the image - such as additional
commands executed before apt dependencies are installed, or adding extra
sources to install your dependencies from. You can see all the arguments
described below but here is an example of rather complex command to
customize the image based on example in [this
comment](https://github.com/apache/airflow/issues/8605#issuecomment-690065621):

``` bash
DOCKER_BUILDKIT=1 docker build . -f Dockerfile.ci \
  --pull \
  --build-arg PYTHON_BASE_IMAGE="python:3.10-slim-bookworm" \
  --build-arg AIRFLOW_INSTALLATION_METHOD="apache-airflow" \
  --build-arg ADDITIONAL_AIRFLOW_EXTRAS="slack" \
  --build-arg ADDITIONAL_PYTHON_DEPS="apache-airflow-providers-odbc \
      azure-storage-blob \
      sshtunnel \
      google-api-python-client \
      oauth2client \
      beautifulsoup4 \
      dateparser \
      rocketchat_API \
      typeform" \
  --build-arg ADDITIONAL_DEV_APT_DEPS="msodbcsql17 unixodbc-dev g++" \
  --build-arg ADDITIONAL_DEV_APT_COMMAND="curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add --no-tty - && curl https://packages.microsoft.com/config/debian/12/prod.list > /etc/apt/sources.list.d/mssql-release.list" \
  --build-arg ADDITIONAL_DEV_ENV_VARS="ACCEPT_EULA=Y"
  --tag my-image:0.0.1
```

## CI image build arguments

The following build arguments (`--build-arg` in docker build command)
can be used for CI images:

| Build argument                    | Default value               | Description                                                                                                       |
|-----------------------------------|-----------------------------|-------------------------------------------------------------------------------------------------------------------|
| `PYTHON_BASE_IMAGE`               | `python:3.10-slim-bookworm` | Base Python image                                                                                                 |
| `PYTHON_MAJOR_MINOR_VERSION`      | `3.10`                      | major/minor version of Python (should match base image)                                                           |
| `DEPENDENCIES_EPOCH_NUMBER`       | `2`                         | increasing this number will reinstall all apt dependencies                                                        |
| `ADDITIONAL_PIP_INSTALL_FLAGS`    |                             | additional `pip` flags passed to the installation commands (except when reinstalling `pip` itself)                |
| `HOME`                            | `/root`                     | Home directory of the root user (CI image has root user as default)                                               |
| `AIRFLOW_HOME`                    | `/root/airflow`             | Airflow's HOME (that's where logs and sqlite databases are stored)                                                |
| `AIRFLOW_SOURCES`                 | `/opt/airflow`              | Mounted sources of Airflow                                                                                        |
| `AIRFLOW_REPO`                    | `apache/airflow`            | the repository from which PIP dependencies are pre-installed                                                      |
| `AIRFLOW_BRANCH`                  | `main`                      | the branch from which PIP dependencies are pre-installed                                                          |
| `AIRFLOW_CI_BUILD_EPOCH`          | `1`                         | increasing this value will reinstall PIP dependencies from the repository from scratch                            |
| `AIRFLOW_CONSTRAINTS_LOCATION`    |                             | If not empty, it will override the source of the constraints with the specified URL or file.                      |
| `AIRFLOW_CONSTRAINTS_REFERENCE`   | `constraints-main`          | reference (branch or tag) from GitHub repository from which constraints are used.                                 |
| `AIRFLOW_EXTRAS`                  | `all`                       | extras to install                                                                                                 |
| `UPGRADE_RANDOM_INDICATOR_STRING` |                             | If set to any random value the dependencies are upgraded to newer versions. In CI it is set to build id.          |
| `ADDITIONAL_AIRFLOW_EXTRAS`       |                             | additional extras to install                                                                                      |
| `ADDITIONAL_PYTHON_DEPS`          |                             | additional Python dependencies to install                                                                         |
| `DEV_APT_COMMAND`                 |                             | Dev apt command executed before dev deps are installed in the first part of image                                 |
| `ADDITIONAL_DEV_APT_COMMAND`      |                             | Additional Dev apt command executed before dev dep are installed in the first part of the image                   |
| `DEV_APT_DEPS`                    |                             | Dev APT dependencies installed in the first part of the image (default empty means default dependencies are used) |
| `ADDITIONAL_DEV_APT_DEPS`         |                             | Additional apt dev dependencies installed in the first part of the image                                          |
| `ADDITIONAL_DEV_APT_ENV`          |                             | Additional env variables defined when installing dev deps                                                         |
| `AIRFLOW_PIP_VERSION`             | `25.3`                      | `pip` version used.                                                                                               |
| `AIRFLOW_UV_VERSION`              | `0.9.17`                    | `uv` version used.                                                                                                |
| `AIRFLOW_PREK_VERSION`            | `0.2.21`                    | `prek` version used.                                                                                              |
| `AIRFLOW_USE_UV`                  | `true`                      | Whether to use UV for installation.                                                                               |
| `PIP_PROGRESS_BAR`                | `on`                        | Progress bar for PIP installation                                                                                 |


Here are some examples of how CI images can built manually. CI is always
built from local sources.

This builds the CI image in version 3.10 with default extras ("all").

``` bash
DOCKER_BUILDKIT=1 docker build . -f Dockerfile.ci \
   --pull \
   --build-arg PYTHON_BASE_IMAGE="python:3.10-slim-bookworm" --tag my-image:0.0.1
```

This builds the CI image in version 3.10 with "gcp" extra only.

``` bash
DOCKER_BUILDKIT=1 docker build . -f Dockerfile.ci \
  --pull \
  --build-arg PYTHON_BASE_IMAGE="python:3.10-slim-bookworm" \
  --build-arg AIRFLOW_EXTRAS=gcp --tag my-image:0.0.1
```

This builds the CI image in version 3.10 with "apache-beam" extra added.

``` bash
DOCKER_BUILDKIT=1 docker build . -f Dockerfile.ci \
  --pull \
  --build-arg PYTHON_BASE_IMAGE="python:3.10-slim-bookworm" \
  --build-arg ADDITIONAL_AIRFLOW_EXTRAS="apache-beam" --tag my-image:0.0.1
```

This builds the CI image in version 3.10 with "mssql" additional package
added.

``` bash
DOCKER_BUILDKIT=1 docker build . -f Dockerfile.ci \
  --pull \
  --build-arg PYTHON_BASE_IMAGE="python:3.10-slim-bookworm" \
  --build-arg ADDITIONAL_PYTHON_DEPS="mssql" --tag my-image:0.0.1
```

This builds the CI image in version 3.10 with "gcc" and "g++" additional
apt dev dependencies added.

```
DOCKER_BUILDKIT=1 docker build . -f Dockerfile.ci \
  --pull
  --build-arg PYTHON_BASE_IMAGE="python:3.10-slim-bookworm" \
  --build-arg ADDITIONAL_DEV_APT_DEPS="gcc g++" --tag my-image:0.0.1
```

This builds the CI image in version 3.10 with "jdbc" extra and
"default-jre-headless" additional apt runtime dependencies added.

```
DOCKER_BUILDKIT=1 docker build . -f Dockerfile.ci \
  --pull \
  --build-arg PYTHON_BASE_IMAGE="python:3.10-slim-bookworm" \
  --build-arg AIRFLOW_EXTRAS=jdbc \
  --tag my-image:0.0.1
```

## Running the CI image

The entrypoint in the CI image contains all the initialisation needed
for tests to be immediately executed. It is copied from
`scripts/docker/entrypoint_ci.sh`.

The default behaviour is that you are dropped into bash shell. However
if RUN_TESTS variable is set to "true", then tests passed as arguments
are executed

The entrypoint performs those operations:

- checks if the environment is ready to test (including database and all
  integrations). It waits until all the components are ready to work
- removes and re-installs another version of Airflow (if another version
  of Airflow is requested to be reinstalled via
  `USE_AIRFLOW_PYPI_VERSION` variable.
- Sets up Kerberos if Kerberos integration is enabled (generates and
  configures Kerberos token)
- Sets up ssh keys for ssh tests and restarts the SSH server
- Sets all variables and configurations needed for unit tests to run
- Reads additional variables set in
  `files/airflow-breeze-config/environment_variables.env` by sourcing that file
- In case of CI run sets parallelism to 2 to avoid excessive number of
  processes to run
- In case of CI run sets default parameters for pytest
- In case of running integration/long_running/quarantined tests - it
  sets the right pytest flags
- Sets default "tests" target in case the target is not explicitly set
  as additional argument
- Runs system tests if TEST_GROUP is "system-core" or "system-providers"
  otherwise runs regular unit and integration tests


# Naming conventions for stored images

The images produced during the `Build Images` workflow of CI jobs are
stored in the [GitHub Container
Registry](https://github.com/orgs/apache/packages?repo_name=airflow)

The image names follow the patterns (except the Python image, all the
images are stored in <https://ghcr.io/> in `apache` organization.

The packages are available under (CONTAINER_NAME is url-encoded name of
the image). Note that "/" are supported now in the `ghcr.io` as a part
of the image name within the `apache` organization, but they have to be
percent-encoded when you access them via UI (/ = %2F)

`https://github.com/apache/airflow/pkgs/container/<CONTAINER_NAME>`

| Image                    | Name                                   | Description                                                   |
|--------------------------|----------------------------------------|---------------------------------------------------------------|
| Python image (DockerHub) | python:\<X.Y\>-slim-bookworm           | Base Python image used by both production and CI image.       |
| CI image                 | airflow/\<BRANCH\>/ci/python\<X.Y\>    | CI image - this is the image used for most of the tests.      |
| PROD image               | airflow/\<BRANCH\>/prod/python\<X.Y\>  | faster to build or pull. Production image optimized for size. |

- \<BRANCH\> might be either "main" or "v2-\*-test"
- \<X.Y\> - Python version (Major + Minor).Should be one of \["3.10", "3.11", "3.12", "3.13" \].


----

Read next about [GitHub Variables](03_github_variables.md)
