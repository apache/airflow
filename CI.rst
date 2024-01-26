 .. Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

 ..   http://www.apache.org/licenses/LICENSE-2.0

 .. Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

.. contents:: :local:

CI Environment
==============

Continuous Integration is important component of making Apache Airflow robust and stable. We are running
a lot of tests for every pull request, for main and v2-*-test branches and regularly as scheduled jobs.

Our execution environment for CI is `GitHub Actions <https://github.com/features/actions>`_. GitHub Actions
(GA) are very well integrated with GitHub code and Workflow and it has evolved fast in 2019/202 to become
a fully-fledged CI environment, easy to use and develop for, so we decided to switch to it. Our previous
CI system was Travis CI.

However part of the philosophy we have is that we are not tightly coupled with any of the CI
environments we use. Most of our CI jobs are written as bash scripts which are executed as steps in
the CI jobs. And we have  a number of variables determine build behaviour.

You can also take a look at the `CI Sequence Diagrams <CI_DIAGRAMS.md>`_ for more graphical overview
of how Airflow CI works.

GitHub Actions runs
-------------------

Our CI builds are highly optimized, leveraging the latest features provided
by the GitHub Actions environment to reuse parts of the build process across
different jobs.

A significant portion of our CI runs utilize container images. Given that
Airflow has numerous dependencies, we use Docker containers to ensure tests
run in a well-configured and consistent environment. This approach is used
for most tests, documentation building, and some advanced static checks.
The environment comprises two types of images: CI images and PROD images.
CI images are used for most tests and checks, while PROD images are used for
Kubernetes tests.

To run the tests, we need to ensure that the images are built using the
latest sources and that the build process is efficient. A full rebuild of
such an image from scratch might take approximately 15 minutes. Therefore,
we've implemented optimization techniques that efficiently use the cache
from the GitHub Docker registry. In most cases, this reduces the time
needed to rebuild the image to about 4 minutes. However, when
dependencies change, it can take around 6-7 minutes, and if the base
image of Python releases a new patch-level, it can take approximately
12 minutes.

Container Registry used as cache
--------------------------------

We are using GitHub Container Registry to store the results of the ``Build Images``
workflow which is used in the ``Tests`` workflow.

Currently in main version of Airflow we run tests in all versions of Python supported,
which means that we have to build multiple images (one CI and one PROD for each Python version).
Yet we run many jobs (>15) - for each of the CI images. That is a lot of time to just build the
environment to run. Therefore we are utilising ``pull_request_target`` feature of GitHub Actions.

This feature allows to run a separate, independent workflow, when the main workflow is run -
this separate workflow is different than the main one, because by default it runs using ``main`` version
of the sources but also - and most of all - that it has WRITE access to the GitHub Container Image registry.

This is especially important in our case where Pull Requests to Airflow might come from any repository,
and it would be a huge security issue if anyone from outside could
utilise the WRITE access to the Container Image Registry via external Pull Request.

Thanks to the WRITE access and fact that the ``pull_request_target`` by default uses the ``main`` version of the
sources, we can safely run some logic there will checkout the incoming Pull Request, build the container
image from the sources from the incoming PR and push such image to an GitHub Docker Registry - so that
this image can be built only once and used by all the jobs running tests. The image is tagged with unique
``COMMIT_SHA`` of the incoming Pull Request and the tests run in the Pull Request can simply pull such image
rather than build it from the scratch. Pulling such image takes ~ 1 minute, thanks to that we are saving
a lot of precious time for jobs.

We use `GitHub Container Registry <https://docs.github.com/en/packages/guides/about-github-container-registry>`_.
``GITHUB_TOKEN`` is needed to push to the registry and we configured scopes of the tokens in our jobs
to be able to write to the registry.

The latest cache is kept as ``:cache-amd64`` and ``:cache-arm64`` tagged cache (suitable for
``--cache-from`` directive of buildx - it contains metadata and cache for all segments in the image,
and cache is separately kept for different platform.

The ``latest`` images of CI and PROD are ``amd64`` only images for CI, because there is no very easy way
to push multiplatform images without merging the manifests and it is not really needed nor used for cache.


Naming conventions for stored images
====================================

The images produced during the ``Build Images`` workflow of CI jobs are stored in the
`GitHub Container Registry <https://github.com/orgs/apache/packages?repo_name=airflow>`_

The images are stored with both "latest" tag (for last main push image that passes all the tests as well
with the COMMIT_SHA id for images that were used in particular build.

The image names follow the patterns (except the Python image, all the images are stored in
https://ghcr.io/ in ``apache`` organization.

The packages are available under (CONTAINER_NAME is url-encoded name of the image). Note that "/" are
supported now in the ``ghcr.io`` as a part of the image name within the ``apache`` organization, but they
have to be percent-encoded when you access them via UI (/ = %2F)

``https://github.com/apache/airflow/pkgs/container/<CONTAINER_NAME>``

+--------------+----------------------------------------------------------+----------------------------------------------------------+
| Image        | Name:tag (both cases latest version and per-build)       | Description                                              |
+==============+==========================================================+==========================================================+
| Python image | python:<X.Y>-slim-bookworm                               | Base Python image used by both production and CI image.  |
| (DockerHub)  |                                                          | Python maintainer release new versions of those image    |
|              |                                                          | with security fixes every few weeks in DockerHub.        |
+--------------+----------------------------------------------------------+----------------------------------------------------------+
| CI image     | airflow/<BRANCH>/ci/python<X.Y>:latest                   | CI image - this is the image used for most of the tests. |
|              | or                                                       | Contains all provider dependencies and tools useful      |
|              | airflow/<BRANCH>/ci/python<X.Y>:<COMMIT_SHA>             | For testing. This image is used in Breeze.               |
+--------------+----------------------------------------------------------+----------------------------------------------------------+
|              |                                                          | faster to build or pull.                                 |
| PROD image   | airflow/<BRANCH>/prod/python<X.Y>:latest                 | Production image. This is the actual production image    |
|              | or                                                       | optimized for size.                                      |
|              | airflow/<BRANCH>/prod/python<X.Y>:<COMMIT_SHA>           | It contains only compiled libraries and minimal set of   |
|              |                                                          | dependencies to run Airflow.                             |
+--------------+----------------------------------------------------------+----------------------------------------------------------+

* <BRANCH> might be either "main" or "v2-*-test"
* <X.Y> - Python version (Major + Minor).Should be one of ["3.8", "3.9", "3.10", "3.11"].
* <COMMIT_SHA> - full-length SHA of commit either from the tip of the branch (for pushes/schedule) or
  commit from the tip of the branch used for the PR.

GitHub Registry Variables
=========================

Our CI uses GitHub Registry to pull and push images to/from by default. Those variables are set automatically
by GitHub Actions when you run Airflow workflows in your fork, so they should automatically use your
own repository as GitHub Registry to build and keep the images as build image cache.

The variables are automatically set in GitHub actions

+--------------------------------+---------------------------+----------------------------------------------+
| Variable                       | Default                   | Comment                                      |
+================================+===========================+==============================================+
| GITHUB_REPOSITORY              | ``apache/airflow``        | Prefix of the image. It indicates which.     |
|                                |                           | registry from GitHub to use for image cache  |
|                                |                           | and to determine the name of the image.      |
+--------------------------------+---------------------------+----------------------------------------------+
| CONSTRAINTS_GITHUB_REPOSITORY  | ``apache/airflow``        | Repository where constraints are stored      |
+--------------------------------+---------------------------+----------------------------------------------+
| GITHUB_USERNAME                |                           | Username to use to login to GitHub           |
|                                |                           |                                              |
+--------------------------------+---------------------------+----------------------------------------------+
| GITHUB_TOKEN                   |                           | Token to use to login to GitHub.             |
|                                |                           | Only used when pushing images on CI.         |
+--------------------------------+---------------------------+----------------------------------------------+

The Variables beginning with ``GITHUB_`` cannot be overridden in GitHub Actions by the workflow.
Those variables are set by GitHub Actions automatically and they are immutable. Therefore if
you want to override them in your own CI workflow and use ``breeze``, you need to pass the
values by corresponding ``breeze`` flags ``--github-repository``,
``--github-token`` rather than by setting them as environment variables in your workflow.
Unless you want to keep your own copy of constraints in orphaned ``constraints-*``
branches, the ``CONSTRAINTS_GITHUB_REPOSITORY`` should remain ``apache/airflow``, regardless in which
repository the CI job is run.

One of the variables you might want to override in your own GitHub Actions workflow when using ``breeze`` is
``--github-repository`` - you might want to force it to ``apache/airflow``, because then the cache from
``apache/airflow`` repository will be used and your builds will be much faster.

Example command to build your CI image efficiently in your own CI workflow:

.. code-block:: bash

   # GITHUB_REPOSITORY is set automatically in Github Actions so we need to override it with flag
   #
   breeze ci-image build --github-repository apache/airflow --python 3.10
   docker tag ghcr.io/apache/airflow/main/ci/python3.10 your-image-name:tag


Authentication in GitHub Registry
=================================

We are using GitHub Container Registry as cache for our images. Authentication uses GITHUB_TOKEN mechanism.
Authentication is needed for pushing the images (WRITE) only in "push", "pull_request_target" workflows.
When you are running the CI jobs in GitHub Actions, GITHUB_TOKEN is set automatically by the actions.


CI run types
============

The Apache Airflow project utilizes several types of Continuous Integration (CI)
jobs, each with a distinct purpose and context. These jobs are executed by the
``ci.yaml`` workflow.

In addition to the standard "PR" runs, we also execute "Canary" runs.
These runs are designed to detect potential issues that could affect
regular PRs early on, without causing all PRs to fail when such problems
arise. This strategy ensures a more stable environment for contributors
submitting their PRs. At the same time, it allows maintainers to proactively
address issues highlighted by the "Canary" builds.

Pull request run
----------------

These runs are triggered by pull requests from contributors' forks. The majority of
Apache Airflow builds fall into this category. They are executed in the context of
the contributor's "Fork", not the main Airflow Code Repository, meaning they only have
"read" access to all GitHub resources, such as the container registry and code repository.
This is necessary because the code in these PRs, including the CI job definition,
might be modified by individuals who are not committers to the Apache Airflow Code Repository.

The primary purpose of these jobs is to verify if the PR builds cleanly, if the tests
run correctly, and if the PR is ready for review and merge. These runs utilize cached
images from the Private GitHub registry, including CI, Production Images, and base
Python images. Furthermore, for these builds, we only execute Python tests if
significant files have changed. For instance, if the PR involves a "no-code" change,
no tests will be executed.

Regular PR builds run in a "stable" environment:

* fixed set of constraints (constraints that passed the tests) - except the PRs that change dependencies
* limited matrix and set of tests (determined by selective checks based on what changed in the PR)
* no ARM image builds are build in the regular PRs
* lower probability of flaky tests for non-committer PRs (public runners and less parallelism)

Maintainers can also run the "Pull Request run" from the "apache/airflow" repository by pushing
to a branch in the "apache/airflow" repository. This is useful when you want to test a PR that
changes the CI/CD infrastructure itself (for example changes to the CI/CD scripts or changes to
the CI/CD workflows). In this case the PR is run in the context of the "apache/airflow" repository
and has WRITE access to the GitHub Container Registry.

Canary run
----------

This workflow is triggered when a pull request is merged into the "main" branch or pushed to any of
the "v2-*-test" branches. The "Canary" run aims to upgrade dependencies to their latest versions
and promptly pushes a preview of the CI/PROD image cache to the GitHub Registry. This allows pull
requests to quickly utilize the new cache, which is particularly beneficial when the Dockerfile or
installation scripts have been modified. Even if some tests fail, this cache will already include the
latest Dockerfile and scripts.Upon successful execution, the run updates the constraint files in the
"constraints-main" branch with the latest constraints and pushes both the cache and the latest CI/PROD
images to the GitHub Registry.

If the "Canary" build fails, it often indicates that a new version of our dependencies is incompatible
with the current tests or Airflow code. Alternatively, it could mean that a breaking change has been
merged into "main". Both scenarios require prompt attention from the maintainers. While a "broken main"
due to our code should be fixed quickly, "broken dependencies" may take longer to resolve. Until the tests
pass, the constraints will not be updated, meaning that regular PRs will continue using the older version
of dependencies that passed one of the previous "Canary" runs.

Scheduled runs
--------------

The "scheduled" workflow, which is designed to run regularly (typically overnight),
is triggered when a scheduled run occurs. This workflow is largely identical to the
"Canary" run, with one key difference: the image is always built from scratch, not
from a cache. This approach ensures that we can verify whether any "system" dependencies
in the Debian base image have changed, and confirm that the build process remains reproducible.
Since the process for a scheduled run mirrors that of a "Canary" run, no separate diagram is
necessary to illustrate it.

Workflows
=========

A general note about cancelling duplicated workflows: for the ``Build Images``, ``Tests`` and ``CodeQL``
workflows we use the ``concurrency`` feature of GitHub actions to automatically cancel "old" workflow runs
of each type -- meaning if you push a new commit to a branch or to a pull request and there is a workflow
running, GitHub Actions will cancel the old workflow run automatically.

Build Images Workflow
---------------------

This workflow builds images for the CI Workflow for Pull Requests coming from forks.

It's a special type of workflow: ``pull_request_target`` which means that it is triggered when a pull request
is opened. This also means that the workflow has Write permission to push to the GitHub registry the images
used by CI jobs which means that the images can be built only once and reused by all the CI jobs
(including the matrix jobs). We've implemented it so that the ``Tests`` workflow waits
until the images are built by the ``Build Images`` workflow before running.

Those "Build Image" steps are skipped in case Pull Requests do not come from "forks" (i.e. those
are internal PRs for Apache Airflow repository. This is because in case of PRs coming from
Apache Airflow (only committers can create those) the "pull_request" workflows have enough
permission to push images to GitHub Registry.

This workflow is not triggered on normal pushes to our "main" branches, i.e. after a
pull request is merged and whenever ``scheduled`` run is triggered. Again in this case the "CI" workflow
has enough permissions to push the images. In this case we simply do not run this workflow.

The workflow has the following jobs:

+---------------------------+---------------------------------------------+
| Job                       | Description                                 |
|                           |                                             |
+===========================+=============================================+
| Build Info                | Prints detailed information about the build |
+---------------------------+---------------------------------------------+
| Build CI images           | Builds all configured CI images             |
+---------------------------+---------------------------------------------+
| Build PROD images         | Builds all configured PROD images           |
+---------------------------+---------------------------------------------+

The images are stored in the `GitHub Container Registry <https://github.com/orgs/apache/packages?repo_name=airflow>`_
and the names of those images follow the patterns described in
`Naming conventions for stored images <#naming-conventions-for-stored-images>`_

Image building is configured in "fail-fast" mode. When any of the images
fails to build, it cancels other builds and the source ``Tests`` workflow run
that triggered it.


Differences for main and release branches
-----------------------------------------

The type of tests executed varies depending on the version or branch under test. For the "main" development branch,
we run all tests to maintain the quality of Airflow. However, when releasing patch-level updates on older
branches, we only run a subset of these tests. This is because older branches are exclusively used for releasing
Airflow and its corresponding image, not for releasing providers or helm charts.

This behaviour is controlled by ``default-branch`` output of the build-info job. Whenever we create a branch for old version
we update the ``AIRFLOW_BRANCH`` in ``airflow_breeze/branch_defaults.py`` to point to the new branch and there are a few
places where selection of tests is based on whether this output is ``main``. They are marked as - in the "Release branches"
column of the table below.

Tests Workflow
--------------

This workflow is a regular workflow that performs all checks of Airflow code.

+---------------------------------+----------------------------------------------------------+----------+----------+-----------+-------------------+
| Job                             | Description                                              | PR       | Canary   | Scheduled | Release branches  |
+=================================+==========================================================+==========+==========+===========+===================+
| Build info                      | Prints detailed information about the build              | Yes      | Yes      | Yes       | Yes               |
+---------------------------------+----------------------------------------------------------+----------+----------+-----------+-------------------+
| Push early cache & images       | Pushes early cache/images to GitHub Registry and test    | -        | Yes      | -         | -                 |
|                                 | speed of building breeze images from scratch             |          |          |           |                   |
+---------------------------------+----------------------------------------------------------+----------+----------+-----------+-------------------+
| Check that image builds quickly | Checks that image builds quickly without taking a lot of | -        | Yes      | -         | Yes               |
|                                 | time for ``pip`` to figure out the right set of deps.    |          |          |           |                   |
+---------------------------------+----------------------------------------------------------+----------+----------+-----------+-------------------+
| Build CI images                 | Builds images in-workflow (not in the ``build images``)  | -        | Yes      | Yes (1)   | Yes (4)           |
+---------------------------------+----------------------------------------------------------+----------+----------+-----------+-------------------+
| Generate constraints/CI verify  | Generate constraints for the build and verify CI image   | Yes (2)  | Yes (2)  | Yes (2)   | Yes (2)           |
+---------------------------------+----------------------------------------------------------+----------+----------+-----------+-------------------+
| Build PROD images               | Builds images in-workflow (not in the ``build images``)  | -        | Yes      | Yes (1)   | Yes (4)           |
+---------------------------------+----------------------------------------------------------+----------+----------+-----------+-------------------+
| Build Bullseye PROD images      | Builds images based on Bullseye debian                   | -        | Yes      | Yes       | Yes               |
+---------------------------------+----------------------------------------------------------+----------+----------+-----------+-------------------+
| Run breeze tests                | Run unit tests for Breeze                                | Yes      | Yes      | Yes       | Yes               |
+---------------------------------+----------------------------------------------------------+----------+----------+-----------+-------------------+
| Test OpenAPI client gen         | Tests if OpenAPIClient continues to generate             | Yes      | Yes      | Yes       | Yes               |
+---------------------------------+----------------------------------------------------------+----------+----------+-----------+-------------------+
| React WWW tests                 | React UI tests for new Airflow UI                        | Yes      | Yes      | Yes       | Yes               |
+---------------------------------+----------------------------------------------------------+----------+----------+-----------+-------------------+
| Test examples image building    | Tests if PROD image build examples work                  | Yes      | Yes      | Yes       | Yes               |
+---------------------------------+----------------------------------------------------------+----------+----------+-----------+-------------------+
| Test git clone on Windows       | Tests if Git clone for for Windows                       | Yes (5)  | Yes (5)  | Yes (5)   | Yes (5)           |
+---------------------------------+----------------------------------------------------------+----------+----------+-----------+-------------------+
| Waits for CI Images             | Waits for and verify CI Images                           | Yes (2)  | Yes (2)  | Yes (2)   | Yes (2)           |
+---------------------------------+----------------------------------------------------------+----------+----------+-----------+-------------------+
| Static checks                   | Performs full static checks                              | Yes (6)  | Yes      | Yes       | Yes (7)           |
+---------------------------------+----------------------------------------------------------+----------+----------+-----------+-------------------+
| Basic static checks             | Performs basic static checks (no image)                  | Yes (6)  | -        | -         | -                 |
+---------------------------------+----------------------------------------------------------+----------+----------+-----------+-------------------+
| Build docs                      | Builds and tests publishing of the documentation         | Yes      | Yes      | Yes       | Yes               |
+---------------------------------+----------------------------------------------------------+----------+----------+-----------+-------------------+
| Spellcheck docs                 | Spellcheck docs                                          | Yes      | Yes      | Yes       | Yes               |
+---------------------------------+----------------------------------------------------------+----------+----------+-----------+-------------------+
| Tests wheel provider packages   | Tests if provider packages can be built and released     | Yes      | Yes      | Yes       | -                 |
+---------------------------------+----------------------------------------------------------+----------+----------+-----------+-------------------+
| Tests Airflow compatibility     | Compatibility of provider packages with older Airflow    | Yes      | Yes      | Yes       | -                 |
+---------------------------------+----------------------------------------------------------+----------+----------+-----------+-------------------+
| Tests dist provider packages    | Tests if dist provider packages can be built             | -        | Yes      | Yes       | -                 |
+---------------------------------+----------------------------------------------------------+----------+----------+-----------+-------------------+
| Tests airflow release commands  | Tests if airflow release command works                   | -        | Yes      | Yes       | -                 |
+---------------------------------+----------------------------------------------------------+----------+----------+-----------+-------------------+
| Tests (Backend/Python matrix)   | Run the Pytest unit DB tests (Backend/Python matrix)     | Yes      | Yes      | Yes       | Yes (8)           |
+---------------------------------+----------------------------------------------------------+----------+----------+-----------+-------------------+
| No DB tests                     | Run the Pytest unit Non-DB tests (with pytest-xdist)     | Yes      | Yes      | Yes       | Yes (8)           |
+---------------------------------+----------------------------------------------------------+----------+----------+-----------+-------------------+
| Integration tests               | Runs integration tests (Postgres/Mysql)                  | Yes      | Yes      | Yes       | Yes (9)           |
+---------------------------------+----------------------------------------------------------+----------+----------+-----------+-------------------+
| Quarantined tests               | Runs quarantined tests (with flakiness and side-effects) | Yes      | Yes      | Yes       | Yes (8)           |
+---------------------------------+----------------------------------------------------------+----------+----------+-----------+-------------------+
| Test airflow packages           | Tests that Airflow package can be built and released     | Yes      | Yes      | Yes       | Yes               |
+---------------------------------+----------------------------------------------------------+----------+----------+-----------+-------------------+
| Helm tests                      | Run the Helm integration tests                           | Yes      | Yes      | Yes       | -                 |
+---------------------------------+----------------------------------------------------------+----------+----------+-----------+-------------------+
| Helm release tests              | Run the tests for Helm releasing                         | Yes      | Yes      | Yes       | -                 |
+---------------------------------+----------------------------------------------------------+----------+----------+-----------+-------------------+
| Summarize warnings              | Summarizes warnings from all other tests                 | Yes      | Yes      | Yes       | Yes               |
+---------------------------------+----------------------------------------------------------+----------+----------+-----------+-------------------+
| Wait for PROD Images            | Waits for and verify PROD Images                         | Yes (2)  | Yes (2)  | Yes (2)   | Yes (2)           |
+---------------------------------+----------------------------------------------------------+----------+----------+-----------+-------------------+
| Docker Compose test/PROD verify | Tests quick-start Docker Compose and verify PROD image   | Yes      | Yes      | Yes       | Yes               |
+---------------------------------+----------------------------------------------------------+----------+----------+-----------+-------------------+
| Tests Kubernetes                | Run Kubernetes test                                      | Yes      | Yes      | Yes       | -                 |
+---------------------------------+----------------------------------------------------------+----------+----------+-----------+-------------------+
| Update constraints              | Upgrade constraints to latest ones                       | Yes  (3) | Yes (3)  | Yes (3)   | Yes (3)           |
+---------------------------------+----------------------------------------------------------+----------+----------+-----------+-------------------+
| Push cache & images             | Pushes cache/images to GitHub Registry (3)               | -        | Yes (3)  | -         | Yes               |
+---------------------------------+----------------------------------------------------------+----------+----------+-----------+-------------------+
| Build CI ARM images             | Builds CI images for ARM to detect any problems which    | Yes (10) | -        | Yes       | -                 |
|                                 | would only appear if we install all dependencies on ARM  |          |          |           |                   |
+---------------------------------+----------------------------------------------------------+----------+----------+-----------+-------------------+

``(1)`` Scheduled jobs builds images from scratch - to test if everything works properly for clean builds

``(2)`` The jobs wait for CI images to be available. It only actually runs when build image is needed (in
  case of simpler PRs that do not change dependencies or source code, images are not build)

``(3)`` PROD and CI cache & images are pushed as "cache" (both AMD and ARM) and "latest" (only AMD)
to GitHub Container registry and constraints are upgraded only if all tests are successful.
The images are rebuilt in this step using constraints pushed in the previous step.
Constraints are only actually pushed in the ``canary/scheduled`` runs.

``(4)`` In main, PROD image uses locally build providers using "latest" version of the provider code. In the
non-main version of the build, the latest released providers from PyPI are used.

``(5)`` Always run with public runners to test if Git clone works on Windows.

``(6)`` Run full set of static checks when selective-checks determine that they are needed (basically, when
Python code has been modified).

``(7)`` On non-main builds some of the static checks that are related to Providers are skipped via selective checks
(``skip-pre-commits`` check).

``(8)`` On non-main builds the unit tests for providers are skipped via selective checks removing the
"Providers" test type.

``(9)`` On non-main builds the integration tests for providers are skipped via ``skip-provider-tests`` selective
check output.

``(10)`` Only run the builds in case PR is run by a committer from "apache" repository and in scheduled build.


CodeQL scan
-----------

The `CodeQL <https://securitylab.github.com/tools/codeql>`_ security scan uses GitHub security scan framework to scan our code for security violations.
It is run for JavaScript and Python code.

Publishing documentation
------------------------

Documentation from the ``main`` branch is automatically published on Amazon S3.

To make this possible, GitHub Action has secrets set up with credentials
for an Amazon Web Service account - ``DOCS_AWS_ACCESS_KEY_ID`` and ``DOCS_AWS_SECRET_ACCESS_KEY``.

This account has permission to write/list/put objects to bucket ``apache-airflow-docs``. This bucket has
public access configured, which means it is accessible through the website endpoint.
For more information, see:
`Hosting a static website on Amazon S3 <https://docs.aws.amazon.com/AmazonS3/latest/dev/WebsiteHosting.html>`_

Website endpoint: http://apache-airflow-docs.s3-website.eu-central-1.amazonaws.com/


Debugging CI Jobs in Github Actions
===================================

The CI jobs are notoriously difficult to test, because you can only really see results of it when you run them
in CI environment, and the environment in which they run depend on who runs them (they might be either run
in our Self-Hosted runners (with 64 GB RAM 8 CPUs) or in the GitHub Public runners (6 GB of RAM, 2 CPUs) and
the results will vastly differ depending on which environment is used. We are utilizing parallelism to make
use of all the available CPU/Memory but sometimes you need to enable debugging and force certain environments.
Additional difficulty is that ``Build Images`` workflow is ``pull-request-target`` type, which means that it
will always run using the ``main`` version - no matter what is in your Pull Request.

There are several ways how you can debug the CI jobs when you are maintainer.

* When you want to tests the build with all combinations of all python, backends etc on regular PR,
  add ``full tests needed`` label to the PR.
* When you want to test maintainer PR using public runners, add ``public runners`` label to the PR
* When you want to see resources used by the run, add ``debug ci resources`` label to the PR
* When you want to test changes to breeze that include changes to how images are build you should push
  your PR to ``apache`` repository not to your fork. This will run the images as part of the ``CI`` workflow
  rather than using ``Build images`` workflow and use the same breeze version for building image and testing
* When you want to test changes to ``build-images.yml`` workflow you should push your branch as ``main``
  branch in your local fork. This will run changed ``build-images.yml`` workflow as it will be in ``main``
  branch of your fork

Replicating the CI Jobs locally
===============================

The main goal of the CI philosophy we have that no matter how complex the test and integration
infrastructure, as a developer you should be able to reproduce and re-run any of the failed checks
locally. One part of it are pre-commit checks, that allow you to run the same static checks in CI
and locally, but another part is the CI environment which is replicated locally with Breeze.

You can read more about Breeze in `README.rst <dev/breeze/doc/README.rst>`_ but in essence it is a script that allows
you to re-create CI environment in your local development instance and interact with it. In its basic
form, when you do development you can run all the same tests that will be run in CI - but locally,
before you submit them as PR. Another use case where Breeze is useful is when tests fail on CI. You can
take the full ``COMMIT_SHA`` of the failed build pass it as ``--image-tag`` parameter of Breeze and it will
download the very same version of image that was used in CI and run it locally. This way, you can very
easily reproduce any failed test that happens in CI - even if you do not check out the sources
connected with the run.

All our CI jobs are executed via ``breeze`` commands. You can replicate exactly what our CI is doing
by running the sequence of corresponding ``breeze`` command. Make sure however that you look at both:

* flags passed to ``breeze`` commands
* environment variables used when ``breeze`` command is run - this is useful when we want
  to set a common flag for all ``breeze`` commands in the same job or even the whole workflow. For
  example ``VERBOSE`` variable is set to ``true`` for all our workflows so that more detailed information
  about internal commands executed in CI is printed.

In the output of the CI jobs, you will find both  - the flags passed and environment variables set.

You can read more about it in `Breeze <dev/breeze/doc/README.rst>`_ and `Testing <contributing-docs/09_testing.rst>`_

Since we store images from every CI run, you should be able easily reproduce any of the CI tests problems
locally. You can do it by pulling and using the right image and running it with the right docker command,
For example knowing that the CI job was for commit ``cd27124534b46c9688a1d89e75fcd137ab5137e3``:

.. code-block:: bash

  docker pull ghcr.io/apache/airflow/main/ci/python3.8:cd27124534b46c9688a1d89e75fcd137ab5137e3

  docker run -it ghcr.io/apache/airflow/main/ci/python3.8:cd27124534b46c9688a1d89e75fcd137ab5137e3


But you usually need to pass more variables and complex setup if you want to connect to a database or
enable some integrations. Therefore it is easiest to use `Breeze <dev/breeze/doc/README.rst>`_ for that.
For example if you need to reproduce a MySQL environment in python 3.8 environment you can run:

.. code-block:: bash

  breeze --image-tag cd27124534b46c9688a1d89e75fcd137ab5137e3 --python 3.8 --backend mysql

You will be dropped into a shell with the exact version that was used during the CI run and you will
be able to run pytest tests manually, easily reproducing the environment that was used in CI. Note that in
this case, you do not need to checkout the sources that were used for that run - they are already part of
the image - but remember that any changes you make in those sources are lost when you leave the image as
the sources are not mapped from your host machine.

Depending whether the scripts are run locally via `Breeze <dev/breeze/doc/README.rst>`_ or whether they
are run in ``Build Images`` or ``Tests`` workflows they can take different values.

You can use those variables when you try to reproduce the build locally (alternatively you can pass
those via corresponding command line flags passed to ``breeze shell`` command.

+-----------------------------------------+-------------+--------------+------------+-------------------------------------------------+
| Variable                                | Local       | Build Images | CI         | Comment                                         |
|                                         | development | workflow     | Workflow   |                                                 |
+=========================================+=============+==============+============+=================================================+
|                                                           Basic variables                                                           |
+-----------------------------------------+-------------+--------------+------------+-------------------------------------------------+
| ``PYTHON_MAJOR_MINOR_VERSION``          |             |              |            | Major/Minor version of Python used.             |
+-----------------------------------------+-------------+--------------+------------+-------------------------------------------------+
| ``DB_RESET``                            |    false    |     true     |    true    | Determines whether database should be reset     |
|                                         |             |              |            | at the container entry. By default locally      |
|                                         |             |              |            | the database is not reset, which allows to      |
|                                         |             |              |            | keep the database content between runs in       |
|                                         |             |              |            | case of Postgres or MySQL. However,             |
|                                         |             |              |            | it requires to perform manual init/reset        |
|                                         |             |              |            | if you stop the environment.                    |
+-----------------------------------------+-------------+--------------+------------+-------------------------------------------------+
|                                                      Forcing answer                                                                 |
+-----------------------------------------+-------------+--------------+------------+-------------------------------------------------+
| ``ANSWER``                              |             |     yes      |     yes    | This variable determines if answer to questions |
|                                         |             |              |            | during the build process should be              |
|                                         |             |              |            | automatically given. For local development,     |
|                                         |             |              |            | the user is occasionally asked to provide       |
|                                         |             |              |            | answers to questions such as - whether          |
|                                         |             |              |            | the image should be rebuilt. By default         |
|                                         |             |              |            | the user has to answer but in the CI            |
|                                         |             |              |            | environment, we force "yes" answer.             |
+-----------------------------------------+-------------+--------------+------------+-------------------------------------------------+
|                                                           Host variables                                                            |
+-----------------------------------------+-------------+--------------+------------+-------------------------------------------------+
| ``HOST_USER_ID``                        |             |              |            | User id of the host user.                       |
+-----------------------------------------+-------------+--------------+------------+-------------------------------------------------+
| ``HOST_GROUP_ID``                       |             |              |            | Group id of the host user.                      |
+-----------------------------------------+-------------+--------------+------------+-------------------------------------------------+
| ``HOST_OS``                             |             |    linux     |    linux   | OS of the Host (darwin/linux/windows).          |
+-----------------------------------------+-------------+--------------+------------+-------------------------------------------------+
|                                                            Git variables                                                            |
+-----------------------------------------+-------------+--------------+------------+-------------------------------------------------+
| ``COMMIT_SHA``                          |             | GITHUB_SHA   | GITHUB_SHA | SHA of the commit of the build is run           |
+-----------------------------------------+-------------+--------------+------------+-------------------------------------------------+
|                                             In container environment initialization                                                 |
+-----------------------------------------+-------------+--------------+------------+-------------------------------------------------+
| ``SKIP_ENVIRONMENT_INITIALIZATION``     |   false\*   |    false\*   |   false\*  | Skip initialization of test environment         |
|                                         |             |              |            |                                                 |
|                                         |             |              |            | \* set to true in pre-commits                   |
+-----------------------------------------+-------------+--------------+------------+-------------------------------------------------+
| ``SKIP_IMAGE_UPGRADE_CHECK``            |   false\*   |    false\*   |   false\*  | Skip checking if image should be upgraded       |
|                                         |             |              |            |                                                 |
|                                         |             |              |            | \* set to true in pre-commits                   |
+-----------------------------------------+-------------+--------------+------------+-------------------------------------------------+
| ``SKIP_PROVIDER_TESTS``                 |   false\*   |    false\*   |   false\*  | Skip running provider integration tests         |
+-----------------------------------------+-------------+--------------+------------+-------------------------------------------------+
| ``SKIP_SSH_SETUP``                      |   false\*   |    false\*   |   false\*  | Skip setting up SSH server for tests.           |
|                                         |             |              |            |                                                 |
|                                         |             |              |            | \* set to true in GitHub CodeSpaces             |
+-----------------------------------------+-------------+--------------+------------+-------------------------------------------------+
| ``VERBOSE_COMMANDS``                    |    false    |    false     |    false   | Determines whether every command                |
|                                         |             |              |            | executed in docker should also be printed       |
|                                         |             |              |            | before execution. This is a low-level           |
|                                         |             |              |            | debugging feature of bash (set -x) enabled in   |
|                                         |             |              |            | entrypoint and it should only be used if you    |
|                                         |             |              |            | need to debug the bash scripts in container.    |
+-----------------------------------------+-------------+--------------+------------+-------------------------------------------------+
|                                                        Image build variables                                                        |
+-----------------------------------------+-------------+--------------+------------+-------------------------------------------------+
| ``UPGRADE_TO_NEWER_DEPENDENCIES``       |    false    |    false     |   false\*  | Determines whether the build should             |
|                                         |             |              |            | attempt to upgrade Python base image and all    |
|                                         |             |              |            | PIP dependencies to latest ones matching        |
|                                         |             |              |            | ``pyproject.toml`` limits. Tries to replicate   |
|                                         |             |              |            | the situation of "fresh" user who just installs |
|                                         |             |              |            | airflow and uses latest version of matching     |
|                                         |             |              |            | dependencies. By default we are using a         |
|                                         |             |              |            | tested set of dependency constraints            |
|                                         |             |              |            | stored in separated "orphan" branches           |
|                                         |             |              |            | of the airflow repository                       |
|                                         |             |              |            | ("constraints-main, "constraints-2-0")          |
|                                         |             |              |            | but when this flag is set to anything but false |
|                                         |             |              |            | (for example random value), they are not used   |
|                                         |             |              |            | used and "eager" upgrade strategy is used       |
|                                         |             |              |            | when installing dependencies. We set it         |
|                                         |             |              |            | to true in case of direct pushes (merges)       |
|                                         |             |              |            | to main and scheduled builds so that            |
|                                         |             |              |            | the constraints are tested. In those builds,    |
|                                         |             |              |            | in case we determine that the tests pass        |
|                                         |             |              |            | we automatically push latest set of             |
|                                         |             |              |            | "tested" constraints to the repository.         |
|                                         |             |              |            |                                                 |
|                                         |             |              |            | Setting the value to random value is best way   |
|                                         |             |              |            | to assure that constraints are upgraded even if |
|                                         |             |              |            | there is no change to ``pyproject.toml``        |
|                                         |             |              |            |                                                 |
|                                         |             |              |            | This way our constraints are automatically      |
|                                         |             |              |            | tested and updated whenever new versions        |
|                                         |             |              |            | of libraries are released.                      |
|                                         |             |              |            |                                                 |
|                                         |             |              |            | \* true in case of direct pushes and            |
|                                         |             |              |            |    scheduled builds                             |
+-----------------------------------------+-------------+--------------+------------+-------------------------------------------------+

Adding new Python versions to CI
================================

In order to add a new version the following operations should be done (example uses Python 3.10)

* copy the latest constraints in ``constraints-main`` branch from previous versions and name it
  using the new Python version (``constraints-3.10.txt``). Commit and push

* build image locally for both prod and CI locally using Breeze:

.. code-block:: bash

  breeze ci-image build --python 3.10

* Find the 2 new images (prod, ci) created in
  `GitHub Container registry <https://github.com/orgs/apache/packages?tab=packages&ecosystem=container&q=airflow>`_
  go to Package Settings and turn on ``Public Visibility`` and set "Inherit access from Repository" flag.
