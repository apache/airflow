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

- [Selecting what to put into the release](#selecting-what-to-put-into-the-release)
  - [Selecting what to cherry-pick](#selecting-what-to-cherry-pick)
- [Prepare the Apache Airflow Package RC](#prepare-the-apache-airflow-package-rc)
  - [Build RC artifacts](#build-rc-artifacts)
  - [[\Optional\] Create new release branch](#%5Coptional%5C-create-new-release-branch)
  - [Prepare PyPI convenience "snapshot" packages](#prepare-pypi-convenience-snapshot-packages)
  - [Prepare production Docker Image](#prepare-production-docker-image)
  - [Prepare Vote email on the Apache Airflow release candidate](#prepare-vote-email-on-the-apache-airflow-release-candidate)
- [Verify the release candidate by PMCs](#verify-the-release-candidate-by-pmcs)
  - [SVN check](#svn-check)
  - [Licence check](#licence-check)
  - [Signature check](#signature-check)
  - [SHA512 sum check](#sha512-sum-check)
- [Verify release candidates by Contributors](#verify-release-candidates-by-contributors)
- [Publish the final Apache Airflow release](#publish-the-final-apache-airflow-release)
  - [Summarize the voting for the Apache Airflow release](#summarize-the-voting-for-the-apache-airflow-release)
  - [Publish release to SVN](#publish-release-to-svn)
  - [Prepare PyPI "release" packages](#prepare-pypi-release-packages)
  - [Manually prepare production Docker Image](#manually-prepare-production-docker-image)
  - [Publish documentation](#publish-documentation)
  - [Notify developers of release](#notify-developers-of-release)
  - [Update Announcements page](#update-announcements-page)
  - [Update the bug issue template](#update-the-bug-issue-template)
  - [Update default Airflow version in the helm chart](#update-default-airflow-version-in-the-helm-chart)
  - [Update airflow/config_templates/config.yml file](#update-airflowconfig_templatesconfigyml-file)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

You can find the prerequisites to release Apache Airflow in [README.md](README.md).

# Selecting what to put into the release

The first step of a release is to work out what is being included. This differs based on whether it is a major/minor or a patch release.

- For a *major* or *minor* release, you want to include everything in `main` at the time of release; you'll turn this into a new release branch as part of the rest of the process.

- For a *patch* release, you will be selecting specific commits to cherry-pick and backport into the existing release branch.

## Selecting what to cherry-pick

For obvious reasons, you can't cherry-pick every change from `main` into the release branch - some are incompatible without a large set of other changes, some are brand-new features, and some just don't need to be in a release.

In general only security fixes, data-loss bugs and regression fixes are essential to bring into a patch release; other bugfixes can be added on a best-effort basis, but if something is going to be very difficult to backport (maybe it has a lot of conflicts, or heavily depends on a new feature or API that's not being backported), it's OK to leave it out of the release at your sole discretion as the release manager - if you do this, update the milestone in the issue to the "next" minor release.

Many issues will be marked with the target release as their Milestone; this is a good shortlist to start with for what to cherry-pick.

When you cherry-pick, pick in chronological order onto the `vX-Y-test` release branch. You'll move them over to be on `vX-Y-stable` once the release is cut.

# Prepare the Apache Airflow Package RC

## Build RC artifacts

The Release Candidate artifacts we vote upon should be the exact ones we vote against, without any modification other than renaming – i.e. the contents of the files must be the same between voted release candidate and final release. Because of this the version in the built artifacts that will become the official Apache releases must not include the rcN suffix.

- Set environment variables

    ```shell script
    # Set Version
    export VERSION=2.1.2rc3
    export VERSION_SUFFIX=rc3
    export VERSION_CONSTRAINT_BRANCH=2-1
    export VERSION_WITHOUT_RC=${VERSION/rc?/}

    # Set AIRFLOW_REPO_ROOT to the path of your git repo
    export AIRFLOW_REPO_ROOT=$(pwd)


    # Example after cloning
    git clone https://github.com/apache/airflow.git airflow
    cd airflow
    export AIRFLOW_REPO_ROOT=$(pwd)
    ```

- Set your version to 2.0.N in `setup.py` (without the RC tag)
- Replace the version in `README.md` and verify that installation instructions work fine.
- Add a commit that updates `CHANGELOG.md` to add changes from previous version if it has not already added.
For now this is done manually, example run  `git log --oneline v2-2-test..HEAD --pretty='format:- %s'` and categorize them.
- Add section for the release in `UPDATING.md`. If no new entries exist, put "No breaking changes" (e.g. `2.1.4`).
- Commit the version change.

- Tag your release

    ```shell script
    git tag -s ${VERSION}
    ```

- Clean the checkout: the sdist step below will

    ```shell script
    git clean -fxd
    ```

- Tarball the repo

    ```shell script
    mkdir dist
    git archive --format=tar.gz ${VERSION} \
        --prefix=apache-airflow-${VERSION_WITHOUT_RC}/ \
        -o dist/apache-airflow-${VERSION_WITHOUT_RC}-source.tar.gz
    ```

- Generate SHA512/ASC (If you have not generated a key yet, generate it by following instructions on http://www.apache.org/dev/openpgp.html#key-gen-generate-key)

    ```shell script
    ./breeze prepare-airflow-packages --package-format both
    pushd dist
    ${AIRFLOW_REPO_ROOT}/dev/sign.sh *
    popd
    ```

- If you aren't using Breeze for packaging, build the distribution and wheel files directly

    ```shell script
    python setup.py compile_assets sdist bdist_wheel
    pushd dist
    ${AIRFLOW_REPO_ROOT}/dev/sign.sh *
    popd
    ```

- Tag & Push the latest constraints files. This pushes constraints with rc suffix (this is expected)!

    ```shell script
    git checkout origin/constraints-${VERSION_CONSTRAINT_BRANCH}
    git tag -s "constraints-${VERSION}"
    git push origin "constraints-${VERSION}"
    ```

- Push the artifacts to ASF dev dist repo

    ```shell script
    # First clone the repo
    svn checkout https://dist.apache.org/repos/dist/dev/airflow airflow-dev

    # Create new folder for the release
    cd airflow-dev
    svn mkdir ${VERSION}

    # Move the artifacts to svn folder & commit
    mv ${AIRFLOW_REPO_ROOT}/dist/* ${VERSION}/
    cd ${VERSION}
    svn add *
    svn commit -m "Add artifacts for Airflow ${VERSION}"
    ```

## [\Optional\] Create new release branch

When you just released the `X.Y.0` version (first release of new minor version) you need to create release
branches: `vX-Y-test` and `vX-Y-stable` (for example with `2.1.0rc1` release you need to create v2-1-test and
`v2-1-stable` branches):


   ```shell script
   # First clone the repo
   export BRANCH_PREFIX=v2-1
   git branch ${BRANCH_PREFIX}-test
   git branch ${BRANCH_PREFIX}-stable
   git push origin ${BRANCH_PREFIX}-test ${BRANCH_PREFIX}-stable
   ```

Search and replace all the vX-Y for previous branches (TODO: we should likely automate this a bit more)

Run script to re-tag images from the ``main`` branch to the  ``vX-Y-test`` branch:

   ```shell script
   ./dev/retag_docker_images.py --source-branch main --target-branch ${BRANCH_PREFIX}-test
   ```


## Prepare PyPI convenience "snapshot" packages

At this point we have the artefact that we vote on, but as a convenience to developers we also want to
publish "snapshots" of the RC builds to PyPI for installing via pip:

To do this we need to

- Build the package:

    ```shell script
    ./breeze prepare-airflow-packages --version-suffix-for-pypi "${VERSION_SUFFIX}" --package-format both
    ```

- Verify the artifacts that would be uploaded:

    ```shell script
    twine check dist/*
    ```

- Upload the package to PyPI's test environment:

    ```shell script
    twine upload -r pypitest dist/*
    ```

- Verify that the test package looks good by downloading it and installing it into a virtual environment. The package download link is available at:
https://test.pypi.org/project/apache-airflow/#files

- Upload the package to PyPI's production environment:
`twine upload -r pypi dist/*`

- Again, confirm that the package is available here:
https://pypi.python.org/pypi/apache-airflow

It is important to stress that this snapshot should not be named "release", and it
is not supposed to be used by and advertised to the end-users who do not read the devlist.

- Push Tag for the release candidate

    This step should only be done now and not before, because it triggers an automated build of
    the production docker image, using the packages that are currently released in PyPI
    (both airflow and latest provider packages).

    ```shell script
    git push origin ${VERSION}
    ```

## Prepare production Docker Image

Production Docker images should be manually prepared and pushed by the release manager.

```shell script
./dev/prepare_prod_docker_images.sh ${VERSION}
```

This will wipe Breeze cache and docker-context-files in order to make sure the build is "clean". It
also performs image verification before pushing the images.

## Prepare Vote email on the Apache Airflow release candidate

- Use the dev/airflow-jira script to generate a list of Airflow JIRAs that were closed in the release.

- Send out a vote to the dev@airflow.apache.org mailing list:

Subject:

```shell script
cat <<EOF
[VOTE] Release Airflow ${VERSION_WITHOUT_RC} from ${VERSION}
EOF
```

Body:

```shell script
cat <<EOF
Hey fellow Airflowers,

I have cut Airflow ${VERSION}. This email is calling a vote on the release,
which will last for 72 hours, from Friday, October 8, 2021 at 4:00 pm UTC
until Monday, October 11, 2021 at 4:00 pm UTC, or until 3 binding +1 votes have been received.

https://www.timeanddate.com/worldclock/fixedtime.html?msg=8&iso=20211011T1600&p1=1440

Consider this my (binding) +1.

Airflow ${VERSION} is available at:
https://dist.apache.org/repos/dist/dev/airflow/$VERSION/

*apache-airflow-${VERSION_WITHOUT_RC}-source.tar.gz* is a source release that comes with INSTALL instructions.
*apache-airflow-${VERSION_WITHOUT_RC}.tar.gz* is the binary Python "sdist" release.
*apache_airflow-${VERSION_WITHOUT_RC}-py3-none-any.whl* is the binary Python wheel "binary" release.

Public keys are available at:
https://dist.apache.org/repos/dist/release/airflow/KEYS

Please vote accordingly:

[ ] +1 approve
[ ] +0 no opinion
[ ] -1 disapprove with the reason

Only votes from PMC members are binding, but all members of the community
are encouraged to test the release and vote with "(non-binding)".

The test procedure for PMCs and Contributors who would like to test this RC are described in
https://github.com/apache/airflow/blob/main/dev/README_RELEASE_AIRFLOW.md#verify-the-release-candidate-by-pmcs

Please note that the version number excludes the \`rcX\` string, so it's now
simply ${VERSION_WITHOUT_RC}. This will allow us to rename the artifact without modifying
the artifact checksums when we actually release.

Full Changelog: https://github.com/apache/airflow/blob/${VERSION}/CHANGELOG.txt

Changes since ${VERSION}:
*Bugs*:
[AIRFLOW-3732] Fix issue when trying to edit connection in RBAC UI
[AIRFLOW-2866] Fix missing CSRF token head when using RBAC UI (#3804)
...


*Improvements*:
[AIRFLOW-3302] Small CSS fixes (#4140)
[Airflow-2766] Respect shared datetime across tabs
...


*New features*:
[AIRFLOW-2874] Enables FAB's theme support (#3719)
[AIRFLOW-3336] Add new TriggerRule for 0 upstream failures (#4182)
...


*Doc-only Change*:
[AIRFLOW-XXX] Fix BashOperator Docstring (#4052)
[AIRFLOW-3018] Fix Minor issues in Documentation
...

Cheers,
<your name>
EOF
```


# Verify the release candidate by PMCs

The PMCs should verify the releases in order to make sure the release is following the
[Apache Legal Release Policy](http://www.apache.org/legal/release-policy.html).

At least 3 (+1) votes should be recorded in accordance to
[Votes on Package Releases](https://www.apache.org/foundation/voting.html#ReleaseVotes)

The legal checks include:

* checking if the packages are present in the right dist folder on svn
* verifying if all the sources have correct licences
* verifying if release manager signed the releases with the right key
* verifying if all the checksums are valid for the release

## SVN check

The files should be present in the sub-folder of
[Airflow dist](https://dist.apache.org/repos/dist/dev/airflow/)

The following files should be present (9 files):

* -bin-tar.gz + .asc + .sha512
* -source.tar.gz + .asc + .sha512
* -.whl + .asc + .sha512

As a PMC you should be able to clone the SVN repository:

```shell script
svn co https://dist.apache.org/repos/dist/dev/airflow
```

Or update it if you already checked it out:

```shell script
svn update .
```

Optionally you can use `check_files.py` script to verify that all expected files are
present in SVN. This script may help also with verifying installation of the packages.

```shell script
python check_files.py -v {VERSION} -t airflow -p {PATH_TO_SVN}
```

## Licence check

This can be done with the Apache RAT tool.

* Download the latest jar from https://creadur.apache.org/rat/download_rat.cgi (unpack the binary,
  the jar is inside)
* Unpack the binary (`-bin.tar.gz`) to a folder
* Enter the folder and run the check (point to the place where you extracted the .jar)

```shell script
java -jar ../../apache-rat-0.13/apache-rat-0.13.jar -E .rat-excludes -d .
```

where `.rat-excludes` is the file in the root of Airflow source code.

## Signature check

Make sure you have the key of person signed imported in your GPG. You can find the valid keys in
[KEYS](https://dist.apache.org/repos/dist/release/airflow/KEYS).

You can import the whole KEYS file:

```shell script
gpg --import KEYS
```

You can also import the keys individually from a keyserver. The below one uses Kaxil's key and
retrieves it from the default GPG keyserver
[OpenPGP.org](https://keys.openpgp.org):

```shell script
gpg --receive-keys 12717556040EEF2EEAF1B9C275FCCD0A25FA0E4B
```

You should choose to import the key when asked.

Note that by being default, the OpenPGP server tends to be overloaded often and might respond with
errors or timeouts. Many of the release managers also uploaded their keys to the
[GNUPG.net](https://keys.gnupg.net) keyserver, and you can retrieve it from there.

```shell script
gpg --keyserver keys.gnupg.net --receive-keys 12717556040EEF2EEAF1B9C275FCCD0A25FA0E4B
```

Once you have the keys, the signatures can be verified by running this:

```shell script
for i in *.asc
do
   echo "Checking $i"; gpg --verify $i
done
```

This should produce results similar to the below. The "Good signature from ..." is indication
that the signatures are correct. Do not worry about the "not certified with a trusted signature"
warning. Most of the certificates used by release managers are self signed, that's why you get this
warning. By importing the server in the previous step and importing it via ID from
[KEYS](https://dist.apache.org/repos/dist/release/airflow/KEYS) page, you know that
this is a valid Key already.

```
Checking apache-airflow-2.0.2rc4-bin.tar.gz.asc
gpg: assuming signed data in 'apache-airflow-2.0.2rc4-bin.tar.gz'
gpg: Signature made sob, 22 sie 2020, 20:28:28 CEST
gpg:                using RSA key 12717556040EEF2EEAF1B9C275FCCD0A25FA0E4B
gpg: Good signature from "Kaxil Naik <kaxilnaik@gmail.com>" [unknown]
gpg: WARNING: This key is not certified with a trusted signature!
gpg:          There is no indication that the signature belongs to the owner.
Primary key fingerprint: 1271 7556 040E EF2E EAF1  B9C2 75FC CD0A 25FA 0E4B
Checking apache_airflow-2.0.2rc4-py2.py3-none-any.whl.asc
gpg: assuming signed data in 'apache_airflow-2.0.2rc4-py2.py3-none-any.whl'
gpg: Signature made sob, 22 sie 2020, 20:28:31 CEST
gpg:                using RSA key 12717556040EEF2EEAF1B9C275FCCD0A25FA0E4B
gpg: Good signature from "Kaxil Naik <kaxilnaik@gmail.com>" [unknown]
gpg: WARNING: This key is not certified with a trusted signature!
gpg:          There is no indication that the signature belongs to the owner.
Primary key fingerprint: 1271 7556 040E EF2E EAF1  B9C2 75FC CD0A 25FA 0E4B
Checking apache-airflow-2.0.2rc4-source.tar.gz.asc
gpg: assuming signed data in 'apache-airflow-2.0.2rc4-source.tar.gz'
gpg: Signature made sob, 22 sie 2020, 20:28:25 CEST
gpg:                using RSA key 12717556040EEF2EEAF1B9C275FCCD0A25FA0E4B
gpg: Good signature from "Kaxil Naik <kaxilnaik@gmail.com>" [unknown]
gpg: WARNING: This key is not certified with a trusted signature!
gpg:          There is no indication that the signature belongs to the owner.
Primary key fingerprint: 1271 7556 040E EF2E EAF1  B9C2 75FC CD0A 25FA 0E4B
```

## SHA512 sum check

Run this:

```shell script
for i in *.sha512
do
    echo "Checking $i"; shasum -a 512 `basename $i .sha512 ` | diff - $i
done
```

You should get output similar to:

```
Checking apache-airflow-2.0.2rc4-bin.tar.gz.sha512
Checking apache_airflow-2.0.2rc4-py2.py3-none-any.whl.sha512
Checking apache-airflow-2.0.2rc4-source.tar.gz.sha512
```

# Verify release candidates by Contributors

This can be done (and we encourage to) by any of the Contributors. In fact, it's best if the
actual users of Apache Airflow test it in their own staging/test installations. Each release candidate
is available on PyPI apart from SVN packages, so everyone should be able to install
the release candidate version of Airflow via simply (<VERSION> is 2.0.2 for example, and <X> is
release candidate number 1,2,3,....).

```shell script
pip install apache-airflow==<VERSION>rc<X>
```

Optionally it can be followed with constraints

```shell script
pip install apache-airflow==<VERSION>rc<X> \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-<VERSION>/constraints-3.6.txt"`
```

Note that the constraints contain python version that you are installing it with.

You can use any of the installation methods you prefer (you can even install it via the binary wheel
downloaded from the SVN).

There is also an easy way of installation with Breeze if you have the latest sources of Apache Airflow.
Running the following command will use tmux inside breeze, create `admin` user and run Webserver & Scheduler:

```shell script
./breeze start-airflow --use-airflow-version <VERSION>rc<X> --python 3.7 --backend postgres
```

Once you install and run Airflow, you should perform any verification you see as necessary to check
that the Airflow works as you expected.

# Publish the final Apache Airflow release

## Summarize the voting for the Apache Airflow release

Once the vote has been passed, you will need to send a result vote to dev@airflow.apache.org:

Subject:

```
[RESULT][VOTE] Release Airflow 2.0.2 from 2.0.2rc3
```

Message:

```
Hello,

Apache Airflow 2.0.2 (based on RC3) has been accepted.

4 “+1” binding votes received:
- Kaxil Naik
- Bolke de Bruin
- Ash Berlin-Taylor
- Tao Feng


4 "+1" non-binding votes received:

- Deng Xiaodong
- Stefan Seelmann
- Joshua Patchus
- Felix Uellendall

Vote thread:
https://lists.apache.org/thread.html/736404ca3d2b2143b296d0910630b9bd0f8b56a0c54e3a05f4c8b5fe@%3Cdev.airflow.apache.org%3E

I'll continue with the release process, and the release announcement will follow shortly.

Cheers,
<your name>
```


## Publish release to SVN

You need to migrate the RC artifacts that passed to this repository:
https://dist.apache.org/repos/dist/release/airflow/
(The migration should include renaming the files so that they no longer have the RC number in their filenames.)

The best way of doing this is to svn cp between the two repos (this avoids having to upload the binaries again, and gives a clearer history in the svn commit logs):

```shell script
# GO to Airflow Sources first
cd <YOUR_AIRFLOW_SOURCES>
export AIRFLOW_SOURCES=$(pwd)

# GO to Checked out DEV repo. Should be checked out before via:
# svn checkout https://dist.apache.org/repos/dist/dev/airflow airflow-dev
cd <YOUR_AIFLOW_DEV_SVN>
svn update
export AIRFLOW_DEV_SVN=$(pwd)

# GO to Checked out RELEASE repo. Should be checked out before via:
# svn checkout https://dist.apache.org/repos/dist/release/airflow airflow-release
cd <YOUR_AIFLOW_RELEASE_SVN>
svn update
export AIRFLOW_RELEASE_SVN=$(pwd)

export RC=2.0.2rc5
export VERSION=${RC/rc?/}

# Create new folder for the release
svn mkdir "${VERSION}"
cd "${VERSION}"

# Move the artifacts to svn folder & commit
for f in ${AIRFLOW_DEV_SVN}/$RC/*; do
    svn cp "$f" "${$(basename $f)/}"
    # Those will be used to upload to PyPI
    cp "$f" "${AIRFLOW_SOURCES}/dist/${$(basename $f)/}"
done
svn commit -m "Release Airflow ${VERSION} from ${RC}"

# Remove old release
# See http://www.apache.org/legal/release-policy.html#when-to-archive
cd ..
export PREVIOUS_VERSION=2.0.2
svn rm "${PREVIOUS_VERSION}"
svn commit -m "Remove old release: ${PREVIOUS_VERSION}"
```

Verify that the packages appear in [airflow](https://dist.apache.org/repos/dist/release/airflow/)

## Prepare PyPI "release" packages

At this point we release an official package (they should be copied and renamed from the
previously released RC candidates in "${AIRFLOW_SOURCES}/dist":

- Verify the artifacts that would be uploaded:

    ```shell script
    cd "${AIRFLOW_RELEASE_SVN}/${VERSION}"
    twine check dist/*
    ```

- Upload the package to PyPI's test environment:

    ```shell script
    twine upload -r pypitest dist/*
    ```

- Verify that the test package looks good by downloading it and installing it into a virtual environment.
    The package download link is available at: https://test.pypi.org/project/apache-airflow/#files

- Upload the package to PyPI's production environment:

    ```shell script
    twine upload -r pypi dist/*
    ```

- Again, confirm that the package is available here: https://pypi.python.org/pypi/apache-airflow

- Re-Tag & Push the constraints files with the final release version.

    ```shell script
    git checkout constraints-${RC}
    git tag -s "constraints-${VERSION}"
    git push origin "constraints-${VERSION}"
    ```

- Push Tag for the final version

    This step should only be done now and not before, because it triggers an automated build of
    the production docker image, using the packages that are currently released in PyPI
    (both airflow and latest provider packages).

    ```shell script
    git checkout ${VERSION}
    git push origin ${VERSION}
    ```

## Manually prepare production Docker Image


```shell script
./dev/prepare_prod_docker_images.sh ${VERSION}
```

If you release 'official' (non-rc) version you will be asked if you want to
tag the images as latest - if you are releasing the latest stable branch, you
should answer y and tags will be created and pushed. If you are releasing a
patch release from an older branch, you should answer n and creating tags will
be skipped.

## Publish documentation

Documentation is an essential part of the product and should be made available to users.
In our cases, documentation for the released versions is published in a separate repository - [`apache/airflow-site`](https://github.com/apache/airflow-site), but the documentation source code and build tools are available in the `apache/airflow` repository, so you have to coordinate between the two repositories to be able to build the documentation.

Documentation for providers can be found in the ``/docs/apache-airflow`` directory.

- First, copy the airflow-site repository and set the environment variable ``AIRFLOW_SITE_DIRECTORY``.

    ```shell script
    git clone https://github.com/apache/airflow-site.git airflow-site
    cd airflow-site
    git checkout -b ${VERSION}-docs
    export AIRFLOW_SITE_DIRECTORY="$(pwd)"
    ```

- Then you can go to the directory and build the necessary documentation packages

    ```shell script
    cd "${AIRFLOW_REPO_ROOT}"
    ./breeze build-docs -- --package-filter apache-airflow --package-filter docker-stack --for-production
    ```

- Now you can preview the documentation.

    ```shell script
    ./docs/start_doc_server.sh
    ```

- Copy the documentation to the ``airflow-site`` repository, create commit, push changes and open a PR.

    ```shell script
    ./docs/publish_docs.py --package-filter apache-airflow --package-filter docker-stack
    cd "${AIRFLOW_SITE_DIRECTORY}"
    git add .
    git commit -m "Add documentation for Apache Airflow ${VERSION}"
    git push
    # and finally open a PR
    ```

## Notify developers of release

- Notify users@airflow.apache.org (cc'ing dev@airflow.apache.org and announce@apache.org) that
the artifacts have been published:

Subject:

```shell script
cat <<EOF
Airflow ${VERSION} is released
EOF
```

Body:

```shell script
cat <<EOF
Dear Airflow community,

I'm happy to announce that Airflow ${VERSION} was just released.

The released sources and packages can be downloaded via https://airflow.apache.org/docs/apache-airflow/stable/installation/installing-from-sources.html

Other installation methods are described in https://airflow.apache.org/docs/apache-airflow/stable/installation/

We also made this version available on PyPI for convenience:
\`pip install apache-airflow\`
https://pypi.org/project/apache-airflow/${VERSION}/

The documentation is available on:
https://airflow.apache.org/
https://airflow.apache.org/docs/apache-airflow/${VERSION}/

Find the CHANGELOG here for more details:
https://airflow.apache.org/docs/apache-airflow/${VERSION}/changelog.html

Container images are published at:
https://hub.docker.com/r/apache/airflow/tags/?page=1&name=${VERSION}

Cheers,
<your name>
EOF
```

## Update Announcements page

Update "Announcements" page at the [Official Airflow website](https://airflow.apache.org/announcements/)

## Update the bug issue template

Make sure the version you just released is listed in the bug issue template in `.github/ISSUE_TEMPLATE/airflow_bug_report.yml`.

## Update default Airflow version in the helm chart

Update the values of `airflowVersion`, `defaultAirflowTag` and `appVersion` in the helm chart so the next helm chart release
will use the latest released version. You'll need to update `chart/values.yaml`, `chart/values.schema.json` and
`chart/Chart.yaml`.

## Update airflow/config_templates/config.yml file

File `airflow/config_templates/config.yml` contains documentation on all configuration options available in Airflow. The `version_added` fields must be updated when a new Airflow version is released.

- Get a diff between the released versions and the current local file on `main` branch:

    ```shell script
    ./dev/validate_version_added_fields_in_config.py
    ```

- Update `airflow/config_templates/config.yml` with the details, and commit it.
