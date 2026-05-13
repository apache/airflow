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

- [What the apache-airflow-mypy distribution is](#what-the-apache-airflow-mypy-distribution-is)
- [Decide when to release](#decide-when-to-release)
- [Versioning](#versioning)
- [Prepare Regular apache-airflow-mypy distributions (RC)](#prepare-regular-apache-airflow-mypy-distributions-rc)
  - [Generate release notes](#generate-release-notes)
  - [Build apache-airflow-mypy distributions for SVN apache upload](#build-apache-airflow-mypy-distributions-for-svn-apache-upload)
  - [Build and sign the source and convenience packages](#build-and-sign-the-source-and-convenience-packages)
  - [Add tags in git](#add-tags-in-git)
  - [Commit the source packages to Apache SVN repo](#commit-the-source-packages-to-apache-svn-repo)
  - [Publish the distributions to PyPI (release candidates)](#publish-the-distributions-to-pypi-release-candidates)
  - [Prepare voting email](#prepare-voting-email)
  - [Verify the release candidate](#verify-the-release-candidate)
- [Publish release](#publish-release)
  - [Summarize the voting](#summarize-the-voting)
  - [Publish release to SVN](#publish-release-to-svn)
  - [Publish the packages to PyPI](#publish-the-packages-to-pypi)
  - [Add tags in git](#add-tags-in-git-1)
  - [Notify developers of release](#notify-developers-of-release)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

------------------------------------------------------------------------------------------------------------

# What the apache-airflow-mypy distribution is

The `apache-airflow-mypy` package provides Mypy plugins for Apache Airflow to enhance type checking capabilities.
It includes plugins for typed decorators and operator output type handling.

The Release Manager prepares `apache-airflow-mypy` packages separately from the main Airflow Release, using
`breeze` commands and accompanying scripts. This document provides an overview of the command line tools
needed to prepare the packages.

# Decide when to release

You can release `apache-airflow-mypy` distributions separately from the main Airflow on an ad-hoc basis,
whenever we find that the mypy plugins need to be released - due to new features, bug fixes, or improvements
to type checking support.

# Versioning

We are using the [SEMVER](https://semver.org/) versioning scheme for the `apache-airflow-mypy` distributions.
This is to give users confidence about maintaining backwards compatibility in new releases.

- **Major version** (X.0.0): Breaking changes to plugin interfaces or behavior
- **Minor version** (0.X.0): New features, new plugins, or significant enhancements
- **Patch version** (0.0.X): Bug fixes and minor improvements

# Prepare Regular apache-airflow-mypy distributions (RC)

## Generate release notes

Before releasing, update the `RELEASE_NOTES.rst` file with the changes since the last release.
You can use towncrier to generate release notes from newsfragments:

```bash
cd dev/mypy
towncrier build --version <VERSION>
```

To preview the release notes without writing to the file:

```bash
towncrier build --version <VERSION> --draft
```

Review and edit the generated release notes as needed.

## Build apache-airflow-mypy distributions for SVN apache upload

The Release Manager can use the `breeze` tool to build the package:

```bash
breeze release-management prepare-mypy-distributions \
    --distribution-format both \
    --version-suffix rc1
```

## Build and sign the source and convenience packages

Follow the same signing process as other Airflow packages:

```bash
cd dist
for file in *.tar.gz *.whl; do
    gpg --armor --detach-sign $file
    sha512sum $file > $file.sha512
done
```

## Add tags in git

Tag the release candidate in git:

```bash
git tag -s apache-airflow-mypy-<VERSION>rc<RC> -m "Apache Airflow Mypy <VERSION>rc<RC>"
git push origin apache-airflow-mypy-<VERSION>rc<RC>
```

## Commit the source packages to Apache SVN repo

Follow the standard Apache release process for committing to the SVN repository.

## Publish the distributions to PyPI (release candidates)

Release candidates should be published to TestPyPI first:

```bash
twine upload --repository testpypi dist/apache_airflow_mypy-<VERSION>rc<RC>*
```

## Prepare voting email

Send a voting email to dev@airflow.apache.org with the following template:

```
Subject: [VOTE] Release Apache Airflow Mypy <VERSION> based on <VERSION>rc<RC>

Hello Apache Airflow Community,

This is a call for the vote to release Apache Airflow Mypy version <VERSION>.

The release candidate is available at:
https://dist.apache.org/repos/dist/dev/airflow/apache-airflow-mypy-<VERSION>rc<RC>/

The packages are available at TestPyPI:
https://test.pypi.org/project/apache-airflow-mypy/<VERSION>rc<RC>/

The vote will be open for at least 72 hours.

[ ] +1 Approve the release
[ ] +0 No opinion
[ ] -1 Do not release (please provide specific comments)

Only PMC votes are binding, but everyone is welcome to check and vote.

Best regards,
<YOUR NAME>
```

## Verify the release candidate

Verify the release by:

1. Installing from TestPyPI:

   ```bash
   pip install --index-url https://test.pypi.org/simple/ apache-airflow-mypy==<VERSION>rc<RC>
   ```

2. Testing the plugins in a mypy configuration:

   ```ini
   [mypy]
   plugins = airflow_mypy.plugins.decorators, airflow_mypy.plugins.outputs
   ```

3. Running the test suite if available

# Publish release

## Summarize the voting

Once the vote passes, summarize the results in a reply to the voting thread.

## Publish release to SVN

Move the release from dev to release in SVN:

```bash
svn mv https://dist.apache.org/repos/dist/dev/airflow/apache-airflow-mypy-<VERSION>rc<RC> \
       https://dist.apache.org/repos/dist/release/airflow/apache-airflow-mypy-<VERSION> \
       -m "Release Apache Airflow Mypy <VERSION>"
```

## Publish the packages to PyPI

Publish the final release to PyPI:

```bash
twine upload dist/apache_airflow_mypy-<VERSION>-py3-none-any.whl
twine upload dist/apache_airflow_mypy-<VERSION>.tar.gz
```

## Add tags in git

Tag the final release:

```bash
git tag -s apache-airflow-mypy-<VERSION> -m "Apache Airflow Mypy <VERSION>"
git push origin apache-airflow-mypy-<VERSION>
```

## Notify developers of release

Send an announcement email to dev@airflow.apache.org and announce@apache.org:

```
Subject: [ANNOUNCE] Apache Airflow Mypy <VERSION> released

The Apache Airflow team is pleased to announce the release of Apache Airflow Mypy <VERSION>.

Apache Airflow Mypy provides Mypy plugins for Apache Airflow to enhance type checking capabilities.

The release is available at:
https://pypi.org/project/apache-airflow-mypy/<VERSION>/

Release notes:
https://github.com/apache/airflow/blob/main/dev/mypy/RELEASE_NOTES.rst

Installation:
pip install apache-airflow-mypy

Usage:
Add to your mypy configuration:
[mypy]
plugins = airflow_mypy.plugins.decorators, airflow_mypy.plugins.outputs

Cheers,
The Apache Airflow Team
```
