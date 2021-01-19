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
**Table of contents**

- [Regular provider packages](#regular-provider-packages)
- [Decide when to release](#decide-when-to-release)
- [Regular provider packages versioning](#regular-provider-packages-versioning)
- [Prepare Regular Providers (RC)](#prepare-regular-providers-rc)
  - [Generate release notes](#generate-release-notes)
  - [Build regular provider packages for SVN apache upload](#build-regular-provider-packages-for-svn-apache-upload)
  - [Build and sign the source and convenience packages](#build-and-sign-the-source-and-convenience-packages)
  - [Commit the source packages to Apache SVN repo](#commit-the-source-packages-to-apache-svn-repo)
  - [Publish the Regular convenience package to PyPI](#publish-the-regular-convenience-package-to-pypi)
  - [Publish documentation](#publish-documentation)
  - [Notify developers of release](#notify-developers-of-release)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

------------------------------------------------------------------------------------------------------------

# Regular provider packages

The prerequisites to release Apache Airflow are described in [README.md](README.md).

You can read more about the command line tools used to generate the packages and the two types of
packages we have (Backport and Regular Provider Packages) in [Provider packages](PROVIDER_PACKAGES.md).

# Decide when to release

You can release provider packages separately from the main Airflow on an ad-hoc basis, whenever we find that
a given provider needs to be released - due to new features or due to bug fixes.
You can release each provider package separately, but due to voting and release overhead we try to group
releases of provider packages together.

# Regular provider packages versioning

We are using the [SEMVER](https://semver.org/) versioning scheme for the regular packages. This is in order
to give the users confidence about maintaining backwards compatibility in the new releases of those
packages.

Details about maintaining the SEMVER version are going to be discussed and implemented in
[the related issue](https://github.com/apache/airflow/issues/11425)

# Prepare Regular Providers (RC)

## Generate release notes

Prepare release notes for all the packages you plan to release. When the provider package version
has not been updated since the latest version, the release notes are not generated. Release notes
are only generated, when the latest version of the package does not yet have a corresponding TAG.
The tags for providers is of the form ``providers-<PROVIDER_ID>/<VERSION>`` for example
``providers-amazon/1.0.0``. During releasing, the RC1/RC2 tags are created (for example
``providers-amazon/1.0.0rc1``).

You should start preparing the release notes by determining whether the

Details about maintaining the SEMVER version are going to be discussed and implemented in
[the related issue](https://github.com/apache/airflow/issues/11425)


```shell script
./breeze prepare-provider-documentation [packages]
```

You can iterate and re-generate the same readme content as many times as you want.
Generated readme files should be eventually committed to the repository.

## Build regular provider packages for SVN apache upload

Those packages might get promoted  to "final" packages by just renaming the files, so internally they
should keep the final version number without the rc suffix, even if they are rc1/rc2/... candidates.

They also need to be signed and have checksum files. You can generate the checksum/signature files by running
the "dev/sign.sh" script (assuming you have the right PGP key set-up for signing). The script
generates corresponding .asc and .sha512 files for each file to sign.

## Build and sign the source and convenience packages

* Release candidate packages:

```shell script
export VERSION=1.1.0rc1

./breeze prepare-provider-packages --version-suffix-for-svn rc1
```

if you ony build few packages, run:

```shell script
./breeze prepare-provider-packages --version-suffix-for-svn rc1 PACKAGE PACKAGE ....
```

* Sign all your packages

```shell script
pushd dist
../dev/sign.sh *
popd
```

## Commit the source packages to Apache SVN repo

* Push the artifacts to ASF dev dist repo

```shell script
# First clone the repo if you do not have it
svn checkout https://dist.apache.org/repos/dist/dev/airflow airflow-dev

# update the repo in case you have it already
cd airflow-dev
svn update

# Create a new folder for the release.
cd airflow-dev/providers
svn mkdir ${VERSION}

# Move the artifacts to svn folder
mv ${AIRFLOW_REPO_ROOT}/dist/* ${VERSION}/

# Add and commit
svn add ${VERSION}/*
svn commit -m "Add artifacts for Airflow Providers ${VERSION}"

cd ${AIRFLOW_REPO_ROOT}
```

Verify that the files are available at
[backport-providers](https://dist.apache.org/repos/dist/dev/airflow/backport-providers/)

## Publish the Regular convenience package to PyPI

In case of pre-release versions you build the same packages for both PyPI and SVN so you can simply use
packages generated in the previous step, and you can skip the "prepare" step below.

In order to publish release candidate to PyPI you just need to build and release packages.
The packages should however contain the rcN suffix in the version file name but not internally in the package,
so you need to use `--version-suffix-for-pypi` switch to prepare those packages.
Note that these are different packages than the ones used for SVN upload
though they should be generated from the same sources.

* Generate the packages with the right RC version (specify the version suffix with PyPI switch). Note that
this will clean up dist folder before generating the packages, so you will only have the right packages there.

```shell script
./breeze prepare-provider-packages --version-suffix-for-pypi a1 --version-suffix-for-SVN a1
```

if you ony build few packages, run:

```shell script
./breeze prepare-provider-packages --version-suffix-for-pypi a1 \
    PACKAGE PACKAGE ....
```

* Verify the artifacts that would be uploaded:

```shell script
twine check dist/*
```

* Upload the package to PyPi's test environment:

```shell script
twine upload -r pypitest dist/*
```

* Verify that the test packages look good by downloading it and installing them into a virtual environment.
Twine prints the package links as output - separately for each package.

* Upload the package to PyPi's production environment:

```shell script
twine upload -r pypi dist/*
```

* Again, confirm that the packages are available under the links printed.

## Publish documentation

Documentation is an essential part of the product and should be made available to users.
In our cases, documentation  for the released versions is published in a separate repository - [`apache/airflow-site`](https://github.com/apache/airflow-site), but the documentation source code and build tools are available in the `apache/airflow` repository, so you have to coordinate between the two repositories to be able to build the documentation.

Documentation for providers can be found in the `/docs/apache-airflow-providers` directory and the `/docs/apache-airflow-providers-*/` directory. The first directory contains the package contents lists and should be updated every time a new version of provider packages is released.

- First, copy the airflow-site repository and set the environment variable ``AIRFLOW_SITE_DIRECTORY``.

    ```shell script
    git clone https://github.com/apache/airflow-site.git airflow-site
    cd airflow-site
    export AIRFLOW_SITE_DIRECTORY="$(pwd)"
    ```

- Then you can go to the directory and build the necessary documentation packages

    ```shell script
    cd "${AIRFLOW_REPO_ROOT}"
    ./breeze build-docs -- \
      --package-filter apache-airflow-providers \
      --package-filter apache-airflow-providers-apache-airflow \
      --package-filter apache-airflow-providers-telegram \
      --for-production
    ```

- Now you can preview the documentation.

    ```shell script
    ./docs/start_doc_server.sh
    ```

- Copy the documentation to the ``airflow-site`` repository

    ```shell script
    ./docs/publish_docs.py \
        --package-filter apache-airflow-providers \
        --package-filter apache-airflow-providers-apache-airflow \
        --package-filter apache-airflow-providers-telegram \

    cd "${AIRFLOW_SITE_DIRECTORY}"
    ```

- If you publish a new package, you must add it to [the docs index](https://github.com/apache/airflow-site/blob/master/landing-pages/site/content/en/docs/_index.md):

- Create commit and push changes.

    ```shell script
    git commit -m "Add documentation for backport packages - $(date "+%Y-%m-%d%n")"
    git push
    ```

## Notify developers of release

- Notify users@airflow.apache.org (cc'ing dev@airflow.apache.org and announce@apache.org) that
the artifacts have been published:

Subject:

```shell script
cat <<EOF
Airflow Providers are released
EOF
```

Body:

```shell script
cat <<EOF
Dear Airflow community,

I'm happy to announce that new version of Airflow Providers packages were just released.

The source release, as well as the binary releases, are available here:

https://dist.apache.org/repos/dist/release/airflow/providers/

We also made those versions available on PyPi for convenience ('pip install apache-airflow-providers-*'):

https://pypi.org/search/?q=apache-airflow-providers

The documentation and changelogs are available in the PyPI packages:

<PASTE TWINE UPLOAD LINKS HERE. SORT THEM BEFORE!>

Cheers,
<your name>
EOF
```
