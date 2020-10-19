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
# Development Tools

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of contents**

- [Airflow Jira utility](#airflow-jira-utility)
- [Airflow Pull Request Tool](#airflow-pull-request-tool)
- [Airflow release signing tool](#airflow-release-signing-tool)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

## Airflow Jira utility

The `airflow-jira` script interact with the Airflow project in <https://issues.apache.org/jira/>. There are two modes of operation


- `compare` will examine issues in Jira based on the "Fix Version" field.

  This is useful for preparing releases, and also has an `--unmerged` flag to
  only show issues that aren't detected in the current branch.

  To run this check out the release branch (for instance `v1-10-test`) and run:

  ```
  ./dev/airflow-jira compare --unmerged --previous-version 1.10.6 1.10.7
  ```

  The `--previous-version` is optional, but might speed up operation. That
  should be a tag reachable from the current HEAD, and will limit the script to
  look for cherry-picks in the commit range `$PREV_VERSION..HEAD`

- `changelog` will create a _rough_ output for creating the changelog file for a release

  This output will not be perfect and will need manual processing to make sure
  the descriptions make sense, and that the items are in the right section (for
  instance you might want to create 'Doc-only' and 'Misc/Internal' section.)

## Airflow Pull Request Tool

The `airflow-pr` tool interactively guides committers through the process of merging GitHub PRs into Airflow and closing associated JIRA issues.

It is very important that PRs reference a JIRA issue. The preferred way to do that is for the PR title to begin with [AIRFLOW-XXX]. However, the PR tool can recognize and parse many other JIRA issue formats in the title and will offer to correct them if possible.

__Please note:__ this tool will restore your current branch when it finishes, but you will lose any uncommitted changes. Make sure you commit any changes you wish to keep before proceeding.


### Execution
Simply execute the `airflow-pr` tool:
```
$ ./airflow-pr
Usage: airflow-pr [OPTIONS] COMMAND [ARGS]...

  This tool should be used by Airflow committers to test PRs, merge them
  into the master branch, and close related JIRA issues.

  Before you begin, make sure you have created the 'apache' and 'github' git
  remotes. You can use the "setup_git_remotes" command to do this
  automatically. If you do not want to use these remote names, you can tell
  the PR tool by setting the appropriate environment variables. For more
  information, run:

      airflow-pr merge --help

Options:
  --help  Show this message and exit.

Commands:
  close_jira         Close a JIRA issue (without merging a PR)
  merge              Merge a GitHub PR into Airflow master
  setup_git_remotes  Set up default git remotes
  work_local         Clone a GitHub PR locally for testing (no push)
```

#### Commands

Execute `airflow-pr merge` to be interactively guided through the process of merging a PR, pushing changes to master, and closing JIRA issues.

Execute `airflow-pr work_local` to only merge the PR locally. The tool will pause once the merge is complete, allowing the user to explore the PR, and then will delete the merge and restore the original development environment.

Execute `airflow-pr close_jira` to close a JIRA issue without needing to merge a PR. You will be prompted for an issue number and close comment.

Execute `airflow-pr setup_git_remotes` to configure the default (expected) git remotes. See below for details.

### Configuration

#### Python Libraries
The merge tool requires the `click` and `jira` libraries to be installed. If the libraries are not found, the user will be prompted to install them:
```bash
pip install click jira
```

#### git Remotes
tl;dr run `airflow-pr setup_git_remotes` before using the tool for the first time.

Before using the merge tool, users need to make sure their git remotes are configured. By default, the tool assumes a setup like the one below, where the github repo remote is named `github`. If users have other remote names, they can be supplied by setting environment variables `GITHUB_REMOTE_NAME`.

Users can configure this automatically by running `airflow-pr setup_git_remotes`.

```bash
$ git remote -v
github https://github.com/apache/airflow.git (fetch)
github https://github.com/apache/airflow.git (push)
origin https://github.com/<USER>/airflow (fetch)
origin https://github.com/<USER>/airflow (push)
```

You can iterate and re-generate the same readme content as many times as you want.
Generated readme files should be eventually committed to the repository.

### Build regular provider packages for SVN apache upload

There is a slightly different procedure if you build pre-release (alpha/beta) packages and the
release candidates. For the Alpha artifacts there is no voting and signature/checksum check, so
we do not need to care about this part. For release candidates - those packages might get promoted
to "final" packages by just renaming the files, so internally they should keep the final version
number without the rc suffix, even if they are rc1/rc2/... candidates.

They also need to be signed and have checksum files. You can generate the checksum/signature files by running
the "dev/sign.sh" script (assuming you have the right PGP key set-up for signing). The script
generates corresponding .asc and .sha512 files for each file to sign.

## Airflow release signing tool
The release signing tool can be used to create the SHA512/MD5 and ASC files that required for Apache releases.

### Execution
To create a release tar ball execute following command from Airflow's root.

For alpha/beta releases you need to specify both - svn and pyp i - suffixes, and they have to match. This is
verified by the breeze script. Note that the script will clean up dist folder before generating the
packages, so it will only contain the packages you intended to build.

* Pre-release packages:

```shell script
export VERSION=0.0.1alpha1

./breeze prepare-provider-packages --version-suffix-for-svn a1 --version-suffix-for-pypi a1
```

if you ony build few packages, run:

```shell script
./breeze prepare-provider-packages --version-suffix-for-svn a1 --version-suffix-for-pypi a1 \
    PACKAGE PACKAGE ....
```

* Release candidate packages:

*Note: `compile_assets` command build the frontend assets (JS and CSS) files for the
Web UI using webpack and yarn. Please make sure you have `yarn` installed on your local machine globally.
Details on how to install `yarn` can be found in CONTRIBUTING.rst file.*

./breeze prepare-provider-packages --version-suffix-for-svn rc1
```

`../dev/sign.sh <the_created_tar_ball.tar.gz`

```shell script
./breeze prepare-provider-packages --version-suffix-for-svn rc1 PACKAGE PACKAGE ....
```

* Sign all your packages

```shell script
pushd dist
../dev/sign.sh *
popd
```

#### Commit the source packages to Apache SVN repo

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

### Publish the Regular convenience package to PyPI


In case of pre-release versions you build the same packages for both PyPI and SVN so you can simply use
packages generated in the previous step and you can skip the "prepare" step below.

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

* Copy the list of links to the uploaded packages - they will be useful in preparing VOTE email.

* Again, confirm that the packages are available under the links printed.
