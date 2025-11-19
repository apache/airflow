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

- [What the airflow-ctl distribution is](#what-the-airflow-ctl-distribution-is)
- [The airflow-ctl distributions](#the-airflow-ctl-distributions)
- [Perform review of security issues that are marked for the release](#perform-review-of-security-issues-that-are-marked-for-the-release)
- [Decide when to release](#decide-when-to-release)
- [Airflow-ctl versioning](#airflow-ctl-versioning)
- [Prepare Regular airflow-ctl distributions (RC)](#prepare-regular-airflow-ctl-distributions-rc)
  - [Generate release notes](#generate-release-notes)
  - [Build airflow-ctl distributions for SVN apache upload](#build-airflow-ctl-distributions-for-svn-apache-upload)
  - [Build and sign the source and convenience packages](#build-and-sign-the-source-and-convenience-packages)
  - [Add tags in git](#add-tags-in-git)
  - [Commit the source packages to Apache SVN repo](#commit-the-source-packages-to-apache-svn-repo)
  - [Publish the Regular distributions to PyPI (release candidates)](#publish-the-regular-distributions-to-pypi-release-candidates)
  - [Prepare documentation in Staging](#prepare-documentation-in-staging)
  - [Prepare issue in GitHub to keep status of testing](#prepare-issue-in-github-to-keep-status-of-testing)
  - [Prepare voting email for airflow-ctl release candidate](#prepare-voting-email-for-airflow-ctl-release-candidate)
  - [Verify the release candidate by PMC members](#verify-the-release-candidate-by-pmc-members)
  - [Verify the release candidate by Contributors](#verify-the-release-candidate-by-contributors)
- [Publish release](#publish-release)
- [Set variables and](#set-variables-and)
  - [Summarize the voting for the Apache Airflow release](#summarize-the-voting-for-the-apache-airflow-release)
  - [Publish release to SVN](#publish-release-to-svn)
  - [Publish the packages to PyPI](#publish-the-packages-to-pypi)
  - [Add tags in git](#add-tags-in-git-1)
  - [Publish documentation](#publish-documentation)
  - [Notify developers of release](#notify-developers-of-release)
  - [Send announcements about security issues fixed in the release](#send-announcements-about-security-issues-fixed-in-the-release)
  - [Announce about the release in social media](#announce-about-the-release-in-social-media)
  - [Announce about the release in Apache Airflow Slack](#announce-about-the-release-in-apache-airflow-slack)
  - [Add Blog post about the release](#add-blog-post-about-the-release)
  - [Add release data to Apache Committee Report Helper](#add-release-data-to-apache-committee-report-helper)
  - [Close the testing status issue](#close-the-testing-status-issue)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

------------------------------------------------------------------------------------------------------------

# What the airflow-ctl distribution is

The distribution is separate packages that implement remote CLI for Apache Airflow.

The Release Manager prepares `airflow-ctl` packages separately from the main Airflow Release, using
`breeze` commands and accompanying scripts. This document provides an overview of the command line tools
needed to prepare the packages.

NOTE!! When you have problems with any of those commands that run inside `breeze` docker image, you
can run the command with `--debug` flag that will drop you in the shell inside the image and will
print the command that you should run.

# The airflow-ctl distributions

The prerequisites to release airflow-ctl are described in [README.md](README.md).

# Perform review of security issues that are marked for the release

We are keeping track of security issues in the [Security Issues](https://github.com/airflow-s/airflow-s/issues)
repository currently. As a release manager, you should have access to the repository.
Please review and ensure that all security issues marked for the release have been
addressed and resolved. Ping security team (comment in the issues) if anything missing or
the issue does not seem to be addressed.

Additionally, the [dependabot alerts](https://github.com/apache/airflow/security/dependabot) and
code [scanning alerts](https://github.com/apache/airflow/security/code-scanning) should be reviewed
and security team should be pinged to review and resolve them.

# Decide when to release

You can release `airflow-ctl` distributions separately from the main Airflow on an ad-hoc basis,
whenever we find that airflow-ctl needs to be released - due to new features or due to bug fixes.

# Airflow-ctl versioning

We are using the [SEMVER](https://semver.org/) versioning scheme for the `airflow-ctl` distributions. This is in order
to give the users confidence about maintaining backwards compatibility in the new releases of those
packages.

Decision made in [VOTE for RC Release](https://lists.apache.org/thread/cnz3k2pox69ddkk647mt8gpfy0t70f94) made the starting version from `1.*` to `0.*`.
This caused a side effect where we won't be able to use following versions in further releases which are yanked.

- [1.0.0b1](https://pypi.org/project/apache-airflow-ctl/1.0.0b1/)
- [1.0.0rc1](https://pypi.org/project/apache-airflow-ctl/1.0.0rc1/)
- [1.0.0rc2](https://pypi.org/project/apache-airflow-ctl/1.0.0rc2/)

Set version env variable

```shell script
VERSION=0.1.0
VERSION_SUFFIX=rc1
VERSION_RC=${VERSION}${VERSION_SUFFIX}
```

# Prepare Regular airflow-ctl distributions (RC)

## Generate release notes

TODO: Describe release notes preparation

## Build airflow-ctl distributions for SVN apache upload

Those packages might get promoted  to "final" packages by just renaming the files, so internally they
should keep the final version number without the rc suffix, even if they are rc1/rc2/... candidates.

They also need to be signed and have checksum files. You can generate the checksum/signature files by running
the "dev/sign.sh" script (assuming you have the right PGP key set-up for signing). The script
generates corresponding .asc and .sha512 files for each file to sign.
note: sign script uses `libassuan` and `gnupg` if you don't have them installed run:

```shell script
brew install libassuan
brew install gnupg
```

## Build and sign the source and convenience packages

* Cleanup dist folder:

```shell script
export AIRFLOW_REPO_ROOT=$(pwd -P)
rm -rf ${AIRFLOW_REPO_ROOT}/dist/*
```

## Add tags in git

Assume that your remote for apache repository is called `apache` you should now
set tags for the airflow-ctl in the repo.

Sometimes in cases when there is a connectivity issue to GitHub, it might be possible that local tags get created
and lead to annoying errors. The default behaviour would be to clean such local tags up.

```shell script
git tag -s "airflow-ctl/${VERSION_RC}"
git push apache "airflow-ctl/${VERSION_RC}"
```

* Release candidate packages:

```shell script
breeze release-management prepare-airflow-ctl-distributions --distribution-format both
breeze release-management prepare-tarball --tarball-type apache_airflow_ctl --version "${VERSION}" --version-suffix "${VERSION_SUFFIX}"
```

The `prepare-*-distributions` by default will use Dockerized approach and building of the packages
will be done in a docker container.  However, if you have  `hatch` installed locally you can use
`--use-local-hatch` flag and it will build and use  docker image that has `hatch` installed.


```shell script
breeze release-management prepare-airflow-ctl-distributions --distribution-format both --use-local-hatch
breeze release-management prepare-tarball --tarball-type apache_airflow_ctl --version "${VERSION}" --version-suffix "${VERSION_SUFFIX}"
```


The `prepare-*-distributions` commands (no matter if docker or local hatch is used) should produce the
reproducible `.whl`, `.tar.gz` packages in the dist folder.
The `prepare-tarball` command should produce reproducible `-source.tar.gz` tarball of sources.


* Sign all your packages

```shell script
pushd dist
../dev/sign.sh *
popd
```

If you see ``Library not loaded error`` it means that you are missing `libassuan` and `gnupg`.
check above steps to install them.

## Commit the source packages to Apache SVN repo

* Push the artifacts to ASF dev dist repo

```shell script
# First clone the repo if you do not have it
cd ..
[ -d asf-dist ] || svn checkout --depth=immediates https://dist.apache.org/repos/dist asf-dist
svn update --set-depth=infinity asf-dist/dev/airflow

# Create a new folder for the release.
cd asf-dist/dev/airflow/airflow-ctl

# Remove previously released versions
svn rm *

mkdir -p ${VERSION_RC}
cd ${VERSION_RC}

# Move the artifacts to svn folder
mv ${AIRFLOW_REPO_ROOT}/dist/* .

cd ..

# Add and commit
svn add *
svn commit -m "Add artifacts for Airflow CTL ${VERSION_RC}"

cd ${AIRFLOW_REPO_ROOT}
```

Verify that the files are available at
[airflow-ctl](https://dist.apache.org/repos/dist/dev/airflow/airflow-ctl/)

You should see only airflow-ctl that you are about to release.
If you are seeing others there is an issue.
You can remove the redundant airflow-ctl files manually with:

```shell script
svn rm -rf file_name  // repeat that for every folder/file
svn commit -m "delete old airflow-ctl"
```

## Publish the Regular distributions to PyPI (release candidates)

In order to publish release candidate to PyPI you just need to build and release packages.
The packages should however contain the rcN suffix in the version file name but not internally in the package,
so you need to use `--version-suffix` switch to prepare those packages.
Note that these are different packages than the ones used for SVN upload
though they should be generated from the same sources.

* Generate the packages with the rc<X> version (specify the version suffix with PyPI switch). Note that
you should clean up dist folder before generating the packages, so you will only have the right packages there.

```shell script
rm -rf ${AIRFLOW_REPO_ROOT}/dist/*

breeze release-management prepare-airflow-ctl-distributions --version-suffix "${VERSION_SUFFIX}" --distribution-format both
```

* Verify the artifacts that would be uploaded:

```shell script
twine check ${AIRFLOW_REPO_ROOT}/dist/*
```

* Upload the package to PyPi:

```shell script
twine upload -r pypi ${AIRFLOW_REPO_ROOT}/dist/*
```

* Confirm that the packages are available under the links printed and look good.


## Prepare documentation in Staging

Documentation is an essential part of the product and should be made available to users.
In our cases, documentation for the released versions is published in the staging S3 bucket, and the site is
kept in a separate repository - [`apache/airflow-site`](https://github.com/apache/airflow-site),
but the documentation source code and build tools are available in the `apache/airflow` repository, so
you need to run several workflows to publish the documentation. More details about it can be found in
[Docs README](../docs/README.md) showing the architecture and workflows including manual workflows for
emergency cases.

We have two options publishing the documentation 1. Using breeze commands 2. Manually using GitHub Actions.:

### Using breeze commands

You can use the `breeze` command to publish the documentation.
The command does the following:

1. Triggers [Publish Docs to S3](https://github.com/apache/airflow/actions/workflows/publish-docs-to-s3.yml).
2. Triggers workflow in apache/airflow-site to refresh
3. Triggers S3 to GitHub Sync

```shell script
breeze workflow-run publish-docs --ref airflow-ctl/${VERSION_RC} apache-airflow-ctl
```

The `--ref` parameter should be the tag of the release candidate you are publishing.

You can also add the `--site-env` parameter should be set to `staging` for pre-release
versions or `live` for final releases. The default option is `auto` if the tag is rc it
publishes to `staging` bucket, otherwise it publishes to `live` bucket.

One of the interesting features of publishing this way is that you can also rebuild historical version of
the documentation with patches applied to the documentation (if they can be applied cleanly).

Yoy should specify the `--apply-commits` parameter with the list of commits you want to apply
separated by commas and the workflow will apply those commits to the documentation before
building it (don't forget to add --skip-write-to-stable-folder if you are publishing
previous version of the distribution). Example:

```shell script
breeze workflow-run publish-docs --ref airflow-ctl/1.0.0 --site-env live \
  --apply-commits 4ae273cbedec66c87dc40218c7a94863390a380d --skip-write-to-stable-folder \
  apache.hive
```

Other available parameters can be found with:

```shell
breeze workflow-run publish-docs --help
```

### Manually using GitHub Actions

There are two steps to publish the documentation:

1. Publish the documentation to the staging S3 bucket.

The release manager publishes the documentation using GitHub Actions workflow
[Publish Docs to S3](https://github.com/apache/airflow/actions/workflows/publish-docs-to-s3.yml).

You should specify the final tag to use to build the docs and `apache-airflow-ctl` as package.

After that step, the documentation should be available under the http://airflow.staged.apache.org URL
(also present in the PyPI packages) but stable links and drop-down boxes should not be yet updated.

2. Invalidate Fastly cache, update version drop-down and stable links with the new versions of the documentation.

Before doing it - review the state of removed, suspended, new packages in
[the docs index](https://github.com/apache/airflow-site/blob/master/landing-pages/site/content/en/docs/_index.md):
Make sure to use `staging` branch to run the workflow.

```shell script
cd "${AIRFLOW_SITE_DIRECTORY}"
branch="add-documentation-airflow-ctl-${VERSION_RC}"
git checkout -b "${branch}"
git add .
git commit -m "Add documentation for airflow-ctl - ${VERSION_RC}"
git push --set-upstream origin "${branch}"
```

Merging the PR with the index changes to `staging` will trigger site publishing.

If you do not need to merge a PR, you should manually run the
[Build docs](https://github.com/apache/airflow-site/actions/workflows/build.yml)
workflow in `airflow-site` repository to refresh indexes and drop-downs.

After that build from PR or workflow completes, the new version should be available in the drop-down
list and stable links should be updated, also Fastly cache will be invalidated.

## Prepare issue in GitHub to keep status of testing

TODO: prepare an issue

## Prepare voting email for airflow-ctl release candidate

Make sure the packages are in https://dist.apache.org/repos/dist/dev/airflow/airflow-ctl/


Subject:

```shell script
cat <<EOF
[VOTE] Release Airflow CTL ${VERSION} from ${VERSION_RC}
EOF
```

Body:

```shell script
cat <<EOF
The release candidate for **Apache Airflow Ctl**: ${VERSION_RC}  is now available for testing!

This email is calling for a vote on the release, which will last at least until the
DATE_HERE and until 3 binding +1 votes have been received.

Consider this my +1 (binding) vote.

The apache-airflow-ctl ${VERSION_RC} package is available at: https://dist.apache.org/repos/dist/dev/airflow/airflow-ctl/${VERSION_RC}/

The "apache-airflow-ctl" packages are:

   - *apache_airfow_ctl-${VERSION}-source.tar.gz* is a source release that comes
     with INSTALL instructions.
   - *apache_airfow_ctl-${VERSION}.tar.gz* is the binary Python "sdist" release.
   - *apache_airfow_ctl-${VERSION}-py3-none-any.whl* is the binary Python wheel "binary" release.

Public keys are available at: https://dist.apache.org/repos/dist/release/airflow/KEYS

Please vote accordingly:

[ ] +1 approve
[ ] +0 no opinion
[ ] -1 disapprove with the reason

Only votes from PMC members are binding, but all members of the community are encouraged to test the release and vote with "(non-binding)".

The test procedure for PMC members is described in: https://github.com/apache/airflow/blob/main/dev/README_RELEASE_AIRFLOWCTL.md#verify-the-release-candidate-by-pmc-members

The test procedure for contributors and members of the community who would like to test this RC is described in:
https://github.com/apache/airflow/blob/main/dev/README_RELEASE_AIRFLOWCTL.md#verify-the-release-candidate-by-contributors

Please note that the version number excludes the 'rcX' string, so it's now simply ${VERSION} for the apache-airflow-ctl package.
This will allow us to rename the artifact without modifying the artifact checksums when we actually release.

*Docs* (for preview): https://airflow.staged.apache.org/docs/apache-airflow-ctl/${VERSION}/index.html

*Release Notes*: https://github.com/apache/airflow/blob/airflow-ctl/${VERSION_RC}/airflow-ctl/RELEASE_NOTES.rst

*Testing Instructions using PyPI*:

The packages are available in PyPI: https://pypi.org/project/apache-airflow-ctl/${VERSION_RC}/

You can build a virtualenv that installs this and other required packages like this:

uv venv
uv pip install -U apache-airflow-ctl==${VERSION_RC}

Regards,
<Your name>

EOF
```


## Verify the release candidate by PMC members

### SVN check

The files should be present in
[Airflow dist](https://dist.apache.org/repos/dist/dev/airflow/airflow-ctl/)

The following files should be present (6 files):

* .tar.gz + .asc + .sha512 (one set of files)
* -py3-none-any.whl + .asc + .sha512 (one set of files)

As a PMC member, you should be able to clone the SVN repository:

```shell script
cd ..
[ -d asf-dist ] || svn checkout --depth=immediates https://dist.apache.org/repos/dist asf-dist
svn update --set-depth=infinity asf-dist/dev/airflow
```

Or update it if you already checked it out:

```shell script
cd asf-dist/dev/airflow
svn update .
```

Set an environment variable: PATH_TO_SVN to the root of folder where you have airflow-ctl

```shell script
cd asf-dist/dev/airflow
export PATH_TO_SVN=$(pwd -P)
```

TODO: implement check in ``check_files.py``

### Reproducible package builds checks

For Airflow-ctl distributions we introduced a reproducible build mechanism - which means that whoever wants
to use sources of Airflow from the release tag, can reproducibly build the same "wheel" and "sdist"
packages as the release manager and they will be byte-by-byte identical, which makes them easy to
verify - if they came from the same sources. This build is only done using released dependencies
from PyPI and source code in our repository - no other binary dependencies are used during the build
process and if the packages produced are byte-by-byte identical with the one we create from tagged sources
it means that the build has a verified provenance.

How to verify it:

1) Set variables and change directory where your airflow sources are checked out

```shell
VERSION=0.1.0
VERSION_SUFFIX=rc1
VERSION_RC=${VERSION}${VERSION_SUFFIX}
cd "${AIRFLOW_REPO_ROOT}"
```

Choose the tag you used for release:

```shell
git fetch apache --tags --force
git checkout airflow-ctl/${VERSION_RC}
```

3) Remove all the packages you have in dist folder

```shell
rm -rf dist/*
```

4) Build the packages using checked out sources

```shell
breeze release-management prepare-airflow-ctl-distributions --distribution-format both
breeze release-management prepare-tarball --tarball-type apache_airflow_ctl --version "${VERSION}" --version-suffix "${VERSION_SUFFIX}"
```

5) Switch to the folder where you checked out the SVN dev files

```shell
cd ${PATH_TO_SVN}/airflow-ctl/${VERSION_RC}
```

6) Compare the packages in SVN to the ones you just built

```shell
for i in *.tar.gz *.whl
do
   echo -n "$i:"; diff $i ${AIRFLOW_REPO_ROOT}/dist/$i && echo "No diff found"
done
```

You should see output similar to:

```
apache_airflow_airflow_ctl-1.0.0.tar.gz:No diff found
```

### Licence check

This can be done with the Apache RAT tool.

Download the latest jar from https://creadur.apache.org/rat/download_rat.cgi (unpack the binary, the jar is inside)

You can run this command to do it for you:

```shell script
wget -qO- https://dlcdn.apache.org//creadur/apache-rat-0.17/apache-rat-0.17-bin.tar.gz | gunzip | tar -C /tmp -xvf -
```

Unpack the release source archive (the `<package + version>-source.tar.gz` file) to a folder

```shell script
rm -rf /tmp/apache/airflow-src && mkdir -p /tmp/apache-airflow-src && tar -xzf ${PATH_TO_SVN}/${VERSION_RC}/apache_airflow*-source.tar.gz --strip-components 1 -C /tmp/apache-airflow-src
```

Run the check:

```shell script
java -jar /tmp/apache-rat-0.17/apache-rat-0.17.jar --input-exclude-file /tmp/apache-airflow-src/.rat-excludes /tmp/apache-airflow-src/ | grep -E "! |INFO: "
```

You should see no files reported as Unknown or with wrong licence and summary of the check similar to:

```
INFO: Apache Creadur RAT 0.17 (Apache Software Foundation)
INFO: Excluding patterns: .git-blame-ignore-revs, .github/*, .git ...
INFO: Excluding MISC collection.
INFO: Excluding HIDDEN_DIR collection.
SLF4J(W): No SLF4J providers were found.
SLF4J(W): Defaulting to no-operation (NOP) logger implementation
SLF4J(W): See https://www.slf4j.org/codes.html#noProviders for further details.
INFO: RAT summary:
INFO:   Approved:  15615
INFO:   Archives:  2
INFO:   Binaries:  813
INFO:   Document types:  5
INFO:   Ignored:  2392
INFO:   License categories:  2
INFO:   License names:  2
INFO:   Notices:  216
INFO:   Standards:  15609
INFO:   Unapproved:  0
INFO:   Unknown:  0
```

There should be no files reported as Unknown or Unapproved. The files that are unknown or unapproved should be shown with a line starting with `!`.

For example:

```
! Unapproved:         1    A count of unapproved licenses.
! /CODE_OF_CONDUCT.md
```


### Signature check

Make sure you have imported into your GPG the PGP key of the person signing the release. You can find the valid keys in
[KEYS](https://dist.apache.org/repos/dist/release/airflow/KEYS).

Download the KEYS file from the above link and save it locally.

You can import the whole KEYS file into gpg by running the following command:

```shell script
gpg --import KEYS
```

You can also import the keys individually from a keyserver. The below one uses Kaxil's key and
retrieves it from the default GPG keyserver
[OpenPGP.org](https://keys.openpgp.org):

```shell script
gpg --keyserver keys.openpgp.org --receive-keys CDE15C6E4D3A8EC4ECF4BA4B6674E08AD7DE406F
```

You should choose to import the key when asked.

Note that by being default, the OpenPGP server tends to be overloaded often and might respond with
errors or timeouts. Many of the release managers also uploaded their keys to the
[GNUPG.net](https://keys.gnupg.net) keyserver, and you can retrieve it from there.

```shell script
gpg --keyserver keys.gnupg.net --receive-keys CDE15C6E4D3A8EC4ECF4BA4B6674E08AD7DE406F
```

Once you have the keys, the signatures can be verified by running this:

```shell script
for i in *.asc
do
   echo -e "Checking $i\n"; gpg --verify $i
done
```

This should produce results similar to the below. The "Good signature from ..." is indication
that the signatures are correct. Do not worry about the "not certified with a trusted signature"
warning. Most of the certificates used by release managers are self-signed, and that's why you get this
warning. By importing the key either from the server in the previous step or from the
[KEYS](https://dist.apache.org/repos/dist/release/airflow/KEYS) page, you know that
this is a valid key already.  To suppress the warning you may edit the key's trust level
by running `gpg --edit-key <key id> trust` and entering `5` to assign trust level `ultimate`.

```
Checking apache-airflow-ctl-1.0.0rc1.tar.gz.asc
gpg: assuming signed data in 'apache-airflow-2.0.2rc4.tar.gz'
gpg: Signature made sob, 22 sie 2020, 20:28:28 CEST
gpg:                using RSA key 12717556040EEF2EEAF1B9C275FCCD0A25FA0E4B
gpg: Good signature from "Kaxil Naik <kaxilnaik@gmail.com>" [unknown]
gpg: WARNING: This key is not certified with a trusted signature!
gpg:          There is no indication that the signature belongs to the owner.
Primary key fingerprint: 1271 7556 040E EF2E EAF1  B9C2 75FC CD0A 25FA 0E4B
```

### SHA512 check

Run this:

```shell script
for i in *.sha512
do
    echo "Checking $i"; shasum -a 512 `basename $i .sha512 ` | diff - $i
done
```

You should get output similar to:

```
Checking apache-airflow_ctl-1.0.0.tar.gz.sha512
Checking apache_airflow_ctl-1.0.0-py3-none-any.whl.sha512
Checking apache_airflow_ctl-1.0.0-source.tar.gz.sha512

```

## Verify the release candidate by Contributors

This can be done (and we encourage to) by any of the Contributors. In fact, it's best if the
actual users of Apache Airflow test it in their own staging/test installations. Each release candidate
is available on PyPI apart from SVN packages, so everyone should be able to install
the release candidate version.

Breeze allows you to easily install and run pre-release candidates by following simple instructions
described in
[Manually testing release candidate packages](https://github.com/apache/airflow/blob/main/contributing-docs/testing/testing_packages.rst)

But you can use any of the installation methods you prefer (you can even install it via the binary wheels
downloaded from the SVN).

### Installing in your local virtualenv

```shell
pip install apache-airflow-ctl==<VERSION>rc<X>
```

### Additional Verification

Once you install and run Airflow, you can perform any verification you see as necessary to check
that the Airflow works as you expected.


# Publish release

# Set variables and

```shell
VERSION=0.1.0
VERSION_SUFFIX=rc1
VERSION_RC=${VERSION}${VERSION_SUFFIX}
export RELEASE_MANAGER_NAME="Buğra Öztürk"
```

## Summarize the voting for the Apache Airflow release

Once the vote has been passed, you will need to send a result vote to dev@airflow.apache.org:

Email subject:

```
cat <<EOF
[RESULT][VOTE] Airflow Ctl - release ${VERSION} from ${VERSION_RC}
EOF
```

Email content:

```
cat <<EOF
Hello,

Apache Airflow Ctl prepared with version ${VERSION} from ${VERSION_RC} have been accepted.

3 "+1" binding votes received:
- FIRST LAST NAME (binding)
- FIRST LAST NAME (binding)
- FIRST LAST NAME (binding)

2 "+1" non-binding votes received:
- FIRST LAST NAME
- FIRST LAST NAME

Vote thread: https://lists.apache.org/thread/cs6mcvpn2lk9w2p4oz43t20z3fg5nl7l

I'll continue with the release process, and the release announcement will follow shortly.

Cheers,
${RELEASE_MANAGER_NAME}
EOF
```

## Publish release to SVN

The best way of doing this is to svn cp  between the two repos (this avoids having to upload the binaries
again, and gives a clearer history in the svn commit logs.

We also need to archive older releases before copying the new ones
[Release policy](http://www.apache.org/legal/release-policy.html#when-to-archive)

```bash
cd "<ROOT_OF_YOUR_AIRFLOW_REPO>"
# Set AIRFLOW_REPO_ROOT to the path of your git repo
export AIRFLOW_REPO_ROOT="$(pwd -P)"

# Go the folder where you have checked out the release repo from SVN
# Make sure this is direct directory and a symbolic link
# Otherwise 'svn mv' errors out if it is with "E200033: Another process is blocking the working copy database
cd "<ROOT_WHERE_YOUR_ASF_DIST_IS_CREATED>"

export ASF_DIST_PARENT="$(pwd -P)"
# make sure physical path is used, in case original directory is symbolically linked
cd "${ASF_DIST_PARENT}"

# or clone it if it's not done yet
[ -d asf-dist ] || svn checkout --depth=immediates https://dist.apache.org/repos/dist asf-dist
# Update to latest version
svn update --set-depth=infinity asf-dist/dev/airflow asf-dist/release/airflow

SOURCE_DIR="${ASF_DIST_PARENT}/asf-dist/dev/airflow/airflow-ctl"

# Create airflow-ctl folder if it does not exist
# All latest releases are kept in this one folder without version sub-folder
cd "${ASF_DIST_PARENT}/asf-dist/release/airflow"
mkdir -pv airflow-ctl/${VERSION}
cd airflow-ctl/${VERSION}

# Copy your airflow-ctl with the target name to dist directory and to SVN
rm -rf "${AIRFLOW_REPO_ROOT}"/dist/*

for file in "${SOURCE_DIR}"/${VERSION_RC}/*
do
 base_file=$(basename ${file})
 cp -v "${file}" "${AIRFLOW_REPO_ROOT}/dist/${base_file//rc[0-9]/}"
 svn mv "${file}" "${base_file//rc[0-9]/}"
done

# TODO: add cleanup

# You need to do go to the asf-dist directory in order to commit both dev and release together
cd ${ASF_DIST_PARENT}/asf-dist
# Commit to SVN
svn commit -m "Release Airflow Ctl ${VERSION}"
```

Verify that the packages appear in
[airflow-ctl](https://dist.apache.org/repos/dist/release/airflow/airflow-ctl)

You are expected to see all latest versions of airflow-ctl.
The ones you are about to release (with new version) and the ones that are not part of the current release.

## Publish the packages to PyPI

By that time the packages should be in your dist folder.

```shell script
cd ${AIRFLOW_REPO_ROOT}
git checkout airflow-ctl/${VERSION_RC}
```

example `git checkout airflow-ctl/1.0.0rc1`

Note you probably will see message `You are in 'detached HEAD' state.`
This is expected, the RC tag is most likely behind the main branch.

* Verify the artifacts that would be uploaded:

```shell script
twine check ${AIRFLOW_REPO_ROOT}/dist/*.whl ${AIRFLOW_REPO_ROOT}/dist/*.tar.gz
```

* Remove the source tarball from dist folder as we do not upload it to PyPI

```shell script
rm -f ${AIRFLOW_REPO_ROOT}/dist/*-source.tar.gz*
```


* Upload the package to PyPi:

```shell script
twine upload -r pypi ${AIRFLOW_REPO_ROOT}/dist/*.whl ${AIRFLOW_REPO_ROOT}/dist/*.tar.gz
```

* Verify that the packages are available under the links printed.

Copy links to updated package and save it on the side. You will need it for the announcement message.

* Again, confirm that the packages are available under the links printed.


## Add tags in git

Assume that your remote for apache repository is called `apache` you should now
set tags for airflow-ctl in the repo.

Sometimes in cases when there is a connectivity issue to GitHub, it might be possible that local tags get created
and lead to annoying errors. The default behaviour would be to clean such local tags up.

If you want to disable this behaviour, set the env **CLEAN_LOCAL_TAGS** to false.

```shell script
git tag -s airflow-ctl/${VERSION}
git push apache airflow-ctl/${VERSION}
```

## Publish documentation

Documentation is an essential part of the product and should be made available to users.
In our cases, documentation for the released versions is published in the `live` S3 bucket, and the site is
kept in a separate repository - [`apache/airflow-site`](https://github.com/apache/airflow-site),
but the documentation source code and build tools are available in the `apache/airflow` repository, so
you need to run several workflows to publish the documentation. More details about it can be found in
[Docs README](../docs/README.md) showing the architecture and workflows including manual workflows for
emergency cases.

We have two options publishing the documentation 1. Using breeze commands 2. Manually using GitHub Actions.:

### Using breeze commands

You can use the `breeze` command to publish the documentation.
The command does the following:

1. Triggers [Publish Docs to S3](https://github.com/apache/airflow/actions/workflows/publish-docs-to-s3.yml).
2. Triggers workflow in apache/airflow-site to refresh
3. Triggers S3 to GitHub Sync

```shell script
  unset GITHUB_TOKEN
  breeze workflow-run publish-docs --ref airflow-ctl/${VERSION}  apache-airflow-ctl
```

The `--ref` parameter should be the tag of the final candidate you are publishing.

The `--site-env` parameter should be set to `staging` for pre-release versions or `live` for final releases. the default option is `auto`
if the tag is rc it publishes to `staging` bucket, otherwise it publishes to `live` bucket.

Other available parameters can be found with:

```shell
breeze workflow-run publish-docs --help
```

### Manually using GitHub Actions

There are two steps to publish the documentation:

1. Publish the documentation to the live S3 bucket.

The release manager publishes the documentation using GitHub Actions workflow
[Publish Docs to S3](https://github.com/apache/airflow/actions/workflows/publish-docs-to-s3.yml).

After that step, the documentation should be available under the http://airflow.apache.org URL
(also present in the PyPI packages) but stable links and drop-down boxes should not be yet updated.

After that build from PR or workflow completes, the new version should be available in the drop-down
list and stable links should be updated, also Fastly cache will be invalidated.

## Notify developers of release

Notify users@airflow.apache.org (cc'ing dev@airflow.apache.org) that
the artifacts have been published.

Subject:

```
cat <<EOF
[ANNOUNCE] Apache Airflow CTl ${VERSION} from ${VERSION_RC} released
EOF
```

Body:

```
cat <<EOF
Dear Airflow community,

I'm happy to announce that new version of the Airflow Ctl package prepared: ${VERSION} from ${VERSION_RC} were just released.

The source release, as well as the binary releases, are available here:

https://airflow.apache.org/docs/apache-airflow-ctl/stable/installation/installing-from-sources.html

You can install the ctl via PyPI: https://airflow.apache.org/docs/apache-airflow-ctl/stable/installation/installing-from-pypi.html

The documentation is available at https://airflow.apache.org/docs/ and linked from the PyPI packages.

----

The package can be found in PyPI at this link: https://pypi.org/project/apache-airflow-ctl/${VERSION}/

Cheers,
${RELEASE_MANAGER_NAME}
EOF
```

Send the same email to announce@apache.org, except change the opening line to `Dear community,`.
It is more reliable to send it via the web ui at https://lists.apache.org/list.html?announce@apache.org
(press "c" to compose a new thread)

Note If you choose sending it with your email client make sure the email is set to plain text mode.
Trying to send HTML content will result in failure.

## Send announcements about security issues fixed in the release

The release manager should review and mark as READY all the security issues fixed in the release.
Such issues are marked as affecting `< <JUST_RELEASED_VERSION>` in the CVE management tool
at https://cveprocess.apache.org/. Then the release manager should announced the issues via the tool.

Once announced, each of the issue should be linked with a 'reference' with tag 'vendor advisory' with the
URL to the announcement published automatically by the CVE management tool.
Note that the announce@apache.org is moderated, and the link to the email thread will not be published
immediately, that's why it is recommended to add the link to users@airflow.apache.org which takes usually
few seconds to be published after the CVE tool sends them.

The ASF Security will be notified and will submit to the CVE project and will set the state to 'PUBLIC'.


## Announce about the release in social media

```
📣 We've just released Apache Airflow CTL 0.1.0 🎉

This is the first official release of the `airflowctl` - new tool to remotely interact with your Airflow 3

📦 PyPI: https://lnkd.in/dXaiFa2H
📚 Docs: https://lnkd.in/dYEaSkuT
🛠 Release Notes: https://lnkd.in/dzibW7W8

Thanks to all the contributors who made this possible.
```

------------------------------------------------------------------------------------------------------------
Announcement is done from official Apache-Airflow accounts.

* LinkedIn: https://www.linkedin.com/company/apache-airflow/
* Fosstodon: https://fosstodon.org/@airflow
* Bluesky: https://bsky.app/profile/apache-airflow.bsky.social

Make sure attach the release image generated with Figma to the post.
If you don't have access to the account ask a PMC member to post.

------------------------------------------------------------------------------------------------------------

## Announce about the release in Apache Airflow Slack

Post the same announcement in the `#announcements` channel of the Apache Airflow Slack workspace.

## Add Blog post about the release

Add a blog post about the release in https://apache.airflow.org/ by modifying the
`landing-pages/site/content/en/announcements/_index.md` in the `apache/airflow-site` repository.

## Add release data to Apache Committee Report Helper

Add the release data (version and date) at: https://reporter.apache.org/addrelease.html?airflow

## Close the testing status issue

Don't forget to thank the folks who tested and close the issue tracking the testing status.

```
Thank you everyone. Airflow-ctl is released.
```
