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

Our builds on CI are highly optimized. They utilise some of the latest features provided by GitHub Actions
environment that make it possible to reuse parts of the build process across different Jobs.

Big part of our CI runs use Container Images. Airflow has a lot of dependencies and in order to make
sure that we are running tests in a well configured and repeatable environment, most of the tests,
documentation building, and some more sophisticated static checks are run inside a docker container
environment. This environment consist of two types of images: CI images and PROD images. CI Images
are used for most of the tests and checks where PROD images are used in the Kubernetes tests.

In order to run the tests, we need to make sure that the images are built using latest sources and that it
is done quickly (full rebuild of such image from scratch might take ~15 minutes). Therefore optimisation
techniques have been implemented that use efficiently cache from the GitHub Docker registry - in most cases
this brings down the time needed to rebuild the image to ~4 minutes. In some cases (when dependencies change)
it can be ~6-7 minutes and in case base image of Python releases new patch-level, it can be ~12 minutes.

Container Registry used as cache
--------------------------------

We are using GitHub Container Registry to store the results of the ``Build Images``
workflow which is used in the ``Tests`` workflow.

Currently in main version of Airflow we run tests in 4 different versions of Python (3.7, 3.8, 3.9, 3.10)
which means that we have to build 8 images (4 CI ones and 4 PROD ones). Yet we run around 12 jobs
with each of the CI images. That is a lot of time to just build the environment to run. Therefore
we are utilising ``pull_request_target`` feature of GitHub Actions.

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
supported now in the ``ghcr.io`` as apart of the image name within ``apache`` organization, but they
have to be percent-encoded when you access them via UI (/ = %2F)

``https://github.com/apache/airflow/pkgs/container/<CONTAINER_NAME>``

+--------------+----------------------------------------------------------+----------------------------------------------------------+
| Image        | Name:tag (both cases latest version and per-build)       | Description                                              |
+==============+==========================================================+==========================================================+
| Python image | python:<X.Y>-slim-bullseye                               | Base Python image used by both production and CI image.  |
| (DockerHub)  |                                                          | Python maintainer release new versions of those image    |
|              |                                                          | with security fixes every few weeks in DockerHub.        |
+--------------+----------------------------------------------------------+----------------------------------------------------------+
| Airflow      | airflow/<BRANCH>/python:<X.Y>-slim-bullseye              | Version of python base image used in Airflow Builds      |
| python base  |                                                          | We keep the "latest" version only to mark last "good"    |
| image        |                                                          | python base that went through testing and was pushed.    |
+--------------+----------------------------------------------------------+----------------------------------------------------------+
| PROD Build   | airflow/<BRANCH>/prod-build/python<X.Y>:latest           | Production Build image - this is the "build" stage of    |
| image        |                                                          | production image. It contains build-essentials and all   |
|              |                                                          | necessary apt packages to build/install PIP packages.    |
|              |                                                          | We keep the "latest" version only to speed up builds.    |
+--------------+----------------------------------------------------------+----------------------------------------------------------+
| Manifest     | airflow/<BRANCH>/ci-manifest/python<X.Y>:latest          | CI manifest image - this is the image used to optimize   |
| CI image     |                                                          | pulls and builds for Breeze development environment      |
|              |                                                          | They store hash indicating whether the image will be     |
|              |                                                          | faster to build or pull.                                 |
|              |                                                          | We keep the "latest" version only to help breeze to      |
|              |                                                          | check if new image should be pulled.                     |
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
* <X.Y> - Python version (Major + Minor).Should be one of ["3.7", "3.8", "3.9"].
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
values by corresponding ``breeze`` flags ``--github-repository``, ``--github-username``,
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

The following CI Job run types are currently run for Apache Airflow (run by ci.yaml workflow)
and each of the run types has different purpose and context.

Besides the regular "PR" runs we also have "Canary" runs that are able to detect most of the
problems that might impact regular PRs early, without necessarily failing all PRs when those
problems happen. This allows to provide much more stable environment for contributors, who
contribute their PR, while giving a chance to maintainers to react early on problems that
need reaction, when the "canary" builds fail.

Pull request run
----------------

Those runs are results of PR from the forks made by contributors. Most builds for Apache Airflow fall
into this category. They are executed in the context of the "Fork", not main
Airflow Code Repository which means that they have only "read" permission to all the GitHub resources
(container registry, code repository). This is necessary as the code in those PRs (including CI job
definition) might be modified by people who are not committers for the Apache Airflow Code Repository.

The main purpose of those jobs is to check if PR builds cleanly, if the test run properly and if
the PR is ready to review and merge. The runs are using cached images from the Private GitHub registry -
CI, Production Images as well as base Python images that are also cached in the Private GitHub registry.
Also for those builds we only execute Python tests if important files changed (so for example if it is
"no-code" change, no tests will be executed.

Regular PR builds run in a "stable" environment:

* fixed set of constraints (constraints that passed the tests) - except the PRs that change dependencies
* limited matrix and set of tests (determined by selective checks based on what changed in the PR)
* no ARM image builds are build in the regular PRs
* lower probability of flaky tests for non-committer PRs (public runners and less parallelism)

Canary run
----------

Those runs are results of direct pushes done by the committers - basically merging of a Pull Request
by the committers. Those runs execute in the context of the Apache Airflow Code Repository and have also
write permission for GitHub resources (container registry, code repository).

The main purpose for the run is to check if the code after merge still holds all the assertions - like
whether it still builds, all tests are green. This is a "Canary" build that helps us to detect early
problems with dependencies, image building, full matrix of tests in case they passed through selective checks.

This is needed because some of the conflicting changes from multiple PRs might cause build and test failures
after merge even if they do not fail in isolation. Also those runs are already reviewed and confirmed by the
committers so they can be used to do some housekeeping:

- pushing most recent image build in the PR to the GitHub Container Registry (for caching) including recent
  Dockerfile changes and setup.py/setup.cfg changes (Early Cache)
- test that image in ``breeze`` command builds quickly
- run full matrix of tests to detect any tests that will be mistakenly missed in ``selective checks``
- upgrading to latest constraints and pushing those constraints if all tests succeed
- refresh latest Python base images in case new patch-level is released

The housekeeping is important - Python base images are refreshed with varying frequency (once every few months
usually but sometimes several times per week) with the latest security and bug fixes.

Scheduled runs
--------------

Those runs are results of (nightly) triggered job - only for ``main`` branch. The
main purpose of the job is to check if there was no impact of external dependency changes on the Apache
Airflow code (for example transitive dependencies released that fail the build). It also checks if the
Docker images can be built from the scratch (again - to see if some dependencies have not changed - for
example downloaded package releases etc.

All runs consist of the same jobs, but the jobs behave slightly differently or they are skipped in different
run categories. Here is a summary of the run categories with regards of the jobs they are running.
Those jobs often have matrix run strategy which runs several different variations of the jobs
(with different Backend type / Python version, type of the tests to run for example). The following chapter
describes the workflows that execute for each run.

Those runs and their corresponding ``Build Images`` runs are only executed in main ``apache/airflow``
repository, they are not executed in forks - we want to be nice to the contributors and not use their
free build minutes on GitHub Actions.

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


Tests Workflow
--------------

This workflow is a regular workflow that performs all checks of Airflow code.

+-----------------------------+----------------------------------------------------------+---------+----------+-----------+
| Job                         | Description                                              | PR      | Canary   | Scheduled |
+=============================+==========================================================+=========+==========+===========+
| Build info                  | Prints detailed information about the build              | Yes     | Yes      | Yes       |
+-----------------------------+----------------------------------------------------------+---------+----------+-----------+
| Build CI/PROD images        | Builds images in-workflow (not in the build images one)  | -       | Yes      | Yes (1)   |
+-----------------------------+----------------------------------------------------------+---------+----------+-----------+
| Push early cache & images   | Pushes early cache/images to GitHub Registry and test    | -       | Yes      | -         |
|                             | speed of building breeze images from scratch             |         |          |           |
+-----------------------------+----------------------------------------------------------+---------+----------+-----------+
| Test OpenAPI client gen     | Tests if OpenAPIClient continues to generate             | Yes     | Yes      | Yes       |
+-----------------------------+----------------------------------------------------------+---------+----------+-----------+
| UI tests                    | React UI tests for new Airflow UI                        | Yes     | Yes      | Yes       |
+-----------------------------+----------------------------------------------------------+---------+----------+-----------+
| Test image building         | Tests if PROD image build examples work                  | Yes     | Yes      | Yes       |
+-----------------------------+----------------------------------------------------------+---------+----------+-----------+
| CI Images                   | Waits for and verify CI Images (2)                       | Yes     | Yes      | Yes       |
+-----------------------------+----------------------------------------------------------+---------+----------+-----------+
| (Basic) Static checks       | Performs static checks (full or basic)                   | Yes     | Yes      | Yes       |
+-----------------------------+----------------------------------------------------------+---------+----------+-----------+
| Build docs                  | Builds documentation                                     | Yes     | Yes      | Yes       |
+-----------------------------+----------------------------------------------------------+---------+----------+-----------+
| Tests                       | Run all the Pytest tests for Python code                 | Yes     | Yes      | Yes       |
+-----------------------------+----------------------------------------------------------+---------+----------+-----------+
| Tests provider packages     | Tests if provider packages work                          | Yes     | Yes      | Yes       |
+-----------------------------+----------------------------------------------------------+---------+----------+-----------+
| Upload coverage             | Uploads test coverage from all the tests                 | -       | Yes      | -         |
+-----------------------------+----------------------------------------------------------+---------+----------+-----------+
| PROD Images                 | Waits for and verify PROD Images (2)                     | Yes     | Yes      | Yes       |
+-----------------------------+----------------------------------------------------------+---------+----------+-----------+
| Tests Kubernetes            | Run Kubernetes test                                      | Yes     | Yes      | Yes       |
+-----------------------------+----------------------------------------------------------+---------+----------+-----------+
| Constraints                 | Upgrade constraints to latest ones (3)                   | -       | Yes      | Yes       |
+-----------------------------+----------------------------------------------------------+---------+----------+-----------+
| Push cache & images         | Pushes cache/images to GitHub Registry (3)               | -       | Yes      | Yes       |
+-----------------------------+----------------------------------------------------------+---------+----------+-----------+

``(1)`` Scheduled jobs builds images from scratch - to test if everything works properly for clean builds

``(2)`` The jobs wait for CI images to be available.

``(3)`` PROD and CI cache & images are pushed as "latest" to GitHub Container registry and constraints are
upgraded only if all tests are successful. The images are rebuilt in this step using constraints pushed
in the previous step.

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

You can read more about Breeze in `BREEZE.rst <BREEZE.rst>`_ but in essence it is a script that allows
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

You can read more about it in `BREEZE.rst <BREEZE.rst>`_ and `TESTING.rst <TESTING.rst>`_

Since we store images from every CI run, you should be able easily reproduce any of the CI tests problems
locally. You can do it by pulling and using the right image and running it with the right docker command,
For example knowing that the CI job was for commit ``cd27124534b46c9688a1d89e75fcd137ab5137e3``:

.. code-block:: bash

  docker pull ghcr.io/apache/airflow/main/ci/python3.7:cd27124534b46c9688a1d89e75fcd137ab5137e3

  docker run -it ghcr.io/apache/airflow/main/ci/python3.7:cd27124534b46c9688a1d89e75fcd137ab5137e3


But you usually need to pass more variables and complex setup if you want to connect to a database or
enable some integrations. Therefore it is easiest to use `Breeze <BREEZE.rst>`_ for that. For example if
you need to reproduce a MySQL environment in python 3.8 environment you can run:

.. code-block:: bash

  breeze --image-tag cd27124534b46c9688a1d89e75fcd137ab5137e3 --python 3.8 --backend mysql

You will be dropped into a shell with the exact version that was used during the CI run and you will
be able to run pytest tests manually, easily reproducing the environment that was used in CI. Note that in
this case, you do not need to checkout the sources that were used for that run - they are already part of
the image - but remember that any changes you make in those sources are lost when you leave the image as
the sources are not mapped from your host machine.

Depending whether the scripts are run locally via `Breeze <BREEZE.rst>`_ or whether they
are run in ``Build Images`` or ``Tests`` workflows they can take different values.

You can use those variables when you try to reproduce the build locally (alternatively you can pass
those via command line flags passed to ``breeze`` command.

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
|                                         |             |              |            | ``setup.py`` limits. This tries to replicate    |
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
|                                         |             |              |            | there is no change to setup.py                  |
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
