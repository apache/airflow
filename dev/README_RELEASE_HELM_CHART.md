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

- [Perform review of security issues that are marked for the release](#perform-review-of-security-issues-that-are-marked-for-the-release)
- [Prepare the Apache Airflow Helm Chart Release Candidate](#prepare-the-apache-airflow-helm-chart-release-candidate)
  - [Pre-requisites](#pre-requisites)
  - [Set environment variables](#set-environment-variables)
  - [Setup k8s environment (mainly helm chart)](#setup-k8s-environment-mainly-helm-chart)
  - [Build Release Notes](#build-release-notes)
  - [Update minimum version of Kubernetes](#update-minimum-version-of-kubernetes)
  - [Build RC artifacts](#build-rc-artifacts)
  - [Publish rc documentation](#publish-rc-documentation)
  - [Prepare issue for testing status of rc](#prepare-issue-for-testing-status-of-rc)
  - [Prepare Vote email on the Apache Airflow release candidate](#prepare-vote-email-on-the-apache-airflow-release-candidate)
- [Verify the release candidate by PMC members](#verify-the-release-candidate-by-pmc-members)
  - [SVN check](#svn-check)
  - [Source tarball reproducibility check](#source-tarball-reproducibility-check)
  - [Licence check](#licence-check)
  - [Signature check](#signature-check)
  - [SHA512 sum check](#sha512-sum-check)
- [Verify release candidates by Contributors](#verify-release-candidates-by-contributors)
- [Publish the final release](#publish-the-final-release)
  - [Summarize the voting for the release](#summarize-the-voting-for-the-release)
  - [Publish release to SVN](#publish-release-to-svn)
  - [Publish release tag](#publish-release-tag)
  - [Publish final documentation](#publish-final-documentation)
  - [Update `index.yaml` in airflow-site](#update-indexyaml-in-airflow-site)
  - [Wait for ArtifactHUB to discover new release](#wait-for-artifacthub-to-discover-new-release)
  - [Notify developers of release](#notify-developers-of-release)
  - [Send announcements about security issues fixed in the release](#send-announcements-about-security-issues-fixed-in-the-release)
  - [Add release data to Apache Committee Report Helper](#add-release-data-to-apache-committee-report-helper)
  - [Update Announcements page](#update-announcements-page)
  - [Create release on GitHub](#create-release-on-github)
  - [Close the milestone](#close-the-milestone)
  - [Close the testing status issue](#close-the-testing-status-issue)
  - [Update issue template with the new release](#update-issue-template-with-the-new-release)
  - [Announce the release on the community slack](#announce-the-release-on-the-community-slack)
  - [Announce about the release in social media](#announce-about-the-release-in-social-media)
  - [Bump chart version in Chart.yaml](#bump-chart-version-in-chartyaml)
  - [Remove old releases](#remove-old-releases)
- [Additional processes](#additional-processes)
  - [Fixing released documentation](#fixing-released-documentation)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

You can find the prerequisites to release Apache Airflow in [README.md](README.md). This document
details the steps for releasing Helm Chart.

# Perform review of security issues that are marked for the release

We are keeping track of security issues in the [Security Issues](https://github.com/airflow-s/airflow-s/issues)
repository currently. As a release manager, you should have access to the repository.
Please review and ensure that all security issues marked for the release have been
addressed and resolved. Ping security team (comment in the issues) if anything missing or
the issue does not seem to be addressed.

Additionally, the [dependabot alerts](https://github.com/apache/airflow/security/dependabot) and
code [scanning alerts](https://github.com/apache/airflow/security/code-scanning) should be reviewed
and security team should be pinged to review and resolve them.

# Prepare the Apache Airflow Helm Chart Release Candidate

## Pre-requisites

- Helm version == 3.14.0
- towncrier version == 23.11.0
- The `helm gpg` plugin to sign the chart. It can be found at: https://github.com/technosophos/helm-gpg

Instructions for installing the pre-requisites are explained in the steps below.

## Set environment variables

- Set environment variables

```shell
# Set Version
export VERSION=1.1.0
export VERSION_SUFFIX=rc1

# Set AIRFLOW_REPO_ROOT to the path of your git repo
export AIRFLOW_REPO_ROOT=$(pwd -P)

# Example after cloning
git clone https://github.com/apache/airflow.git airflow
cd airflow
export AIRFLOW_REPO_ROOT=$(pwd -P)
```

## Setup k8s environment (mainly helm chart)

This will install Helm in the recent version and enter the environment where you can run `helm` commands
and installs the necessary python dependencies (including `towncrier` in the k8s virtual environment).

```shell
breeze k8s setup-env
breeze k8s shell
```

Install the helm-gpg plugin, if you have not installed it already. This command will install the plugin
using commit sha of version of the plugin that is known to work with latest openssl and reviewed by us.

```shell
helm plugin install https://github.com/technosophos/helm-gpg --version 6303407eb63deaeb1b2f24de611e3468a27ec05b
```

You can also update/uninstall/list the plugin with other `helm plugin` commands. For more information, run:

```shell
helm plugin --help
```

## Build Release Notes

Before creating the RC, you need to build and commit the release notes for the release:

Preview with:

```shell script
towncrier build --draft --version=${VERSION} --date=2021-12-15 --dir chart --config chart/newsfragments/config.toml
```

Then remove the `--draft` flag to have towncrier build the release notes for real.

The significant changes section does require some reformatting - look at prior releases as an example.

If no significant changes where added in this release, add the header and put "No significant changes.".

Then, get the rest of the entries, categorize them into the appropriate sections, and add it to the release notes.

``` shell script
git log --oneline helm-chart/1.1.0..main --pretty='format:- %s' -- chart/ docs/helm-chart/
```

### Add changelog annotations to `Chart.yaml`

Once the release notes have been built, run the script to generate the changelog annotations.

```shell
./dev/chart/build_changelog_annotations.py
```

Verify the output looks right (only entries from this release), then put them in `Chart.yaml`, for example:

```yaml
annotations:
  artifacthub.io/changes: |
    - kind: added
      description: Add resources for `cleanup` and `createuser` jobs
      links:
        - name: "#19263"
          url: https://github.com/apache/airflow/pull/19263
```

Make sure that all the release notes changes are submitted as PR and merged. Changes in release notes should
also automatically (via `prek` trigger updating of the [reproducible_build.yaml](../chart/reproducible_build.yaml))
file which is uses to reproducibly build the chart package and source tarball.

You can leave the k8s environment now:

```shell
exit
```

## Update minimum version of Kubernetes

The minimum version of Kubernetes should be updated according to
https://github.com/apache/airflow/blob/main/README.md#requirements in two places:

* [../../helm-chart/README.md](../chart/README.md)
* [../docs/helm-chart/index.rst](../docs/helm-chart/index.rst)


## Build RC artifacts

The Release Candidate artifacts we vote upon should be the exact ones we vote against,
without any modification than renaming – i.e. the contents of the files must be
the same between voted release candidate and final release.
Because of this the version in the built artifacts that will become the
official Apache releases must not include the rcN suffix.

Make sure you have `apache` remote set up pointing to the apache git repo.
If needed, add it with:

```shell
git remote add apache git@github.com:apache/airflow.git
git fetch apache
```

- We currently release Helm Chart from `main` branch:

```shell
git checkout apache/main
```

- Clean the checkout: (note that this step will also clean any IDE settings you might have so better to
  do it in a checked out version you only use for releasing)

```shell
git clean -fxd
rm -rf dist/*
```

- Generate the source tarball:

```shell
breeze release-management prepare-helm-chart-tarball --version ${VERSION} --version-suffix ${VERSION_SUFFIX}
```

- Generate the binary Helm Chart release:

```shell
breeze release-management prepare-helm-chart-package --sign-email jedcunningham@apache.org --version-suffix ${VERSION_SUFFIX}
```

Note: The `--version-suffix` parameter is optional but recommended for RC releases to ensure proper documentation
links generation. When specified, it will generate staging documentation links instead of production ones.

Warning: you need the `helm gpg` plugin to sign the chart (instructions to install it above)

This should generate two files:

- `dist/airflow-${VERSION}.tgz`
- `dist/airflow-${VERSION}.tgz.prov`

The second one is a provenance file as described in
https://helm.sh/docs/topics/provenance/. It can be used to verify integrity of the Helm chart.

- Generate SHA512/ASC

```shell
pushd ${AIRFLOW_REPO_ROOT}/dist
${AIRFLOW_REPO_ROOT}/dev/sign.sh airflow-*.tgz airflow-*-source.tar.gz
popd
```

- Move the artifacts to ASF dev dist repo, Generate convenience `index.yaml` & Publish them

  ```shell
  # First clone the repo
  svn checkout https://dist.apache.org/repos/dist/dev/airflow airflow-dev

  # Create new folder for the release
  cd airflow-dev/helm-chart
  svn mkdir ${VERSION}${VERSION_SUFFIX}

  # Move the artifacts to svn folder
  mv ${AIRFLOW_REPO_ROOT}/dist/airflow-${VERSION}.tgz* ${VERSION}${VERSION_SUFFIX}/
  mv ${AIRFLOW_REPO_ROOT}/dist/airflow-chart-${VERSION}-source.tar.gz* ${VERSION}${VERSION_SUFFIX}/
  cd ${VERSION}${VERSION_SUFFIX}

  ###### Generate index.yaml file - Start
  # Download the latest index.yaml on Airflow Website
  curl https://airflow.apache.org/index.yaml --output index.yaml

  # Replace the URLs from "https://downloads.apache.org" to "https://archive.apache.org"
  # as the downloads.apache.org only contains latest releases.
  sed -i 's|https://downloads.apache.org/airflow/helm-chart/|https://archive.apache.org/dist/airflow/helm-chart/|' index.yaml

  # Generate / Merge the new version with existing index.yaml
  helm repo index --merge ./index.yaml . --url "https://dist.apache.org/repos/dist/dev/airflow/helm-chart/${VERSION}${VERSION_SUFFIX}"

  ###### Generate index.yaml file - End

  # Commit the artifacts
  svn add *
  svn commit -m "Add artifacts for Helm Chart ${VERSION}${VERSION_SUFFIX}"
  ```

- Remove old Helm Chart versions from the dev repo

  ```shell
  cd ..
  export PREVIOUS_VERSION_WITH_SUFFIX=1.0.0rc1
  svn rm ${PREVIOUS_VERSION_WITH_SUFFIX}
  svn commit -m "Remove old Helm Chart release: ${PREVIOUS_VERSION_WITH_SUFFIX}"
  ```

- Push Tag for the release candidate

  ```shell
  cd ${AIRFLOW_REPO_ROOT}
  git push apache tag helm-chart/${VERSION}${VERSION_SUFFIX}
  ```

## Publish rc documentation

Documentation is an essential part of the product and should be made available to users.
In our cases, documentation for the released versions is published in S3 bucket, and the site is
kept in a separate repository - [`apache/airflow-site`](https://github.com/apache/airflow-site),
but the documentation source code and build tools are available in the `apache/airflow` repository, so
you need to run several workflows to publish the documentation. More details about it can be found in
[Docs README](../docs/README.md) showing the architecture and workflows including manual workflows for
emergency cases.

There are two steps to publish the documentation:

1. Publish the documentation to the `staging` S3 bucket.

The release manager publishes the documentation using GitHub Actions workflow
[Publish Docs to S3](https://github.com/apache/airflow/actions/workflows/publish-docs-to-s3.yml).

You can specify the RC tag to use to build the docs and 'helm-chart' passed as packages to be built.

The release manager publishes the documentation using GitHub Actions workflow
[Publish Docs to S3](https://github.com/apache/airflow/actions/workflows/publish-docs-to-s3.yml). By
default `auto` selection should publish to the `staging` bucket - based on
the tag you use - pre-release tags go to staging. But you can also override it and specify the destination
manually to be `live` or `staging`.

After that step, the provider documentation should be available under the https://airflow.staged.apache.org
(same as in the helm chart documentation).

2. Invalidate Fastly cache for the documentation.

In order to do it, you need to run the [Build docs](https://github.com/apache/airflow-site/actions/workflows/build.yml)
workflow in `airflow-site` repository. Make sure to use `staging` branch.

After that workflow completes, the new version should be available in the drop-down list and stable links
should be updated and Fastly cache should be invalidated.

## Prepare issue for testing status of rc

Create an issue for testing status of the RC (PREVIOUS_RELEASE should be the previous release version
(for example 1.4.0).

```shell script
cat <<EOF
Status of testing of Apache Airflow Helm Chart ${VERSION}${VERSION_SUFFIX}
EOF
```

Content is generated with:

```shell
breeze release-management generate-issue-content-helm-chart \
--previous-release helm-chart/<PREVIOUS_RELEASE> --current-release helm-chart/${VERSION}${VERSION_SUFFIX}
```

Copy the URL of the issue.

## Prepare Vote email on the Apache Airflow release candidate

- Send out a vote to the dev@airflow.apache.org mailing list:

Subject:

```shell
cat <<EOF
[VOTE] Release Apache Airflow Helm Chart ${VERSION} based on ${VERSION}${VERSION_SUFFIX}
EOF
```

```shell
export VOTE_END_TIME=$(date --utc -d "now + 72 hours + 10 minutes" +'%Y-%m-%d %H:%M')
export TIME_DATE_URL="to?iso=$(date --utc -d "now + 72 hours + 10 minutes" +'%Y%m%dT%H%M')&p0=136&font=cursive"
```

Body:

```shell
cat <<EOF
Hello Apache Airflow Community,

This is a call for the vote to release Helm Chart version ${VERSION}.

The release candidate is available at:
https://dist.apache.org/repos/dist/dev/airflow/helm-chart/${VERSION}${VERSION_SUFFIX}/

airflow-chart-${VERSION}-source.tar.gz - is the "main source release" that comes with INSTALL instructions.
airflow-${VERSION}.tgz - is the binary Helm Chart release.

Public keys are available at: https://www.apache.org/dist/airflow/KEYS

For convenience "index.yaml" has been uploaded (though excluded from voting), so you can also run the below commands.

helm repo add apache-airflow-dev https://dist.apache.org/repos/dist/dev/airflow/helm-chart/${VERSION}${VERSION_SUFFIX}/
helm repo update
helm install airflow apache-airflow-dev/airflow

airflow-${VERSION}.tgz.prov - is also uploaded for verifying Chart Integrity, though not strictly required for releasing the artifact based on ASF Guidelines.

$ helm gpg verify airflow-${VERSION}.tgz
gpg: Signature made Thu Jan  6 21:33:35 2022 MST
gpg:                using RSA key E1A1E984F55B8F280BD9CBA20BB7163892A2E48E
gpg: Good signature from "Jed Cunningham <jedcunningham@apache.org>" [ultimate]
plugin: Chart SHA verified. sha256:b33eac716e0416a18af89fb4fa1043fcfcf24f9f903cda3912729815213525df

The vote will be open for at least 72 hours ($VOTE_END_TIME UTC) or until the necessary number of votes is reached.

https://www.timeanddate.com/countdown/$TIME_DATE_URL

Please vote accordingly:

[ ] +1 approve
[ ] +0 no opinion
[ ] -1 disapprove with the reason

Only votes from PMC members are binding, but members of the community are
encouraged to test the release and vote with "(non-binding)".

Consider this my (binding) +1.

For license checks, the .rat-excludes files is included, so you can run the following to verify licenses (just update your path to rat):

tar -xvf airflow-chart-${VERSION}-source.tar.gz
cd airflow-chart-${VERSION}
java -jar apache-rat-0.13.jar chart -E .rat-excludes

Please note that the version number excludes the \`rcX\` string, so it's now
simply ${VERSION}. This will allow us to rename the artifact without modifying
the artifact checksums when we actually release it.

The status of testing the Helm Chart by the community is kept here:
<TODO COPY LINK TO THE ISSUE CREATED>

Thanks,
<your name>
EOF
```

Note, you need to update the `helm gpg verify` output and verify the end of the voting period in the body.

Note, For RC2/3 you may refer to shorten vote period as agreed in mailing list [thread](https://lists.apache.org/thread/cv194w1fqqykrhswhmm54zy9gnnv6kgm).

# Verify the release candidate by PMC members

The PMC members should verify the releases in order to make sure the release is following the
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

The following files should be present (7 files):

* `airflow-chart-${VERSION}-source.tar.gz` + .asc + .sha512
* `airflow-{VERSION}.tgz` + .asc + .sha512
* `airflow-{VERSION}.tgz.prov`

As a PMC member, you should be able to clone the SVN repository:

```shell
svn co https://dist.apache.org/repos/dist/dev/airflow
```

Or update it if you already checked it out:

```shell
svn update .
```

While in the directory, save the path to the repository root:

```shell
SVN_REPO_ROOT=$(pwd -P)
```

## Source tarball reproducibility check

The source tarball should be reproducible. This means that if you build it twice, you should get
the same result. This is important for security reasons, as it ensures that the source code
has not been tampered with.

1. Go to airflow repository root (for example if you cloned it to `../airflow` then `cd ../airflow`)

```shell
cd ../airflow
AIRFLOW_REPO_ROOT=$(pwd -P)
```

2. Set the version of the release you are checking

```shell
VERSION=12.0.1
VERSION_SUFFIX=rc1
```

3. Check-out the branch from which the release was made and cleanup dist folder:

```shell
git checkout helm-chart/${VERSION}${VERSION_SUFFIX}
rm -rf dist/*
```

4. Build the source tarball and package. Since you are not releasing the package, you should ignore version
   check and skip tagging. There is no need to specify version as it is stored in Chart.yaml of the rc tag.

```shell
breeze release-management prepare-helm-chart-tarball --version-suffix rc1 --ignore-version-check --skip-tagging
breeze release-management prepare-helm-chart-package --version-suffix rc1
```

5. Compare the produced tarball binary with ones in SVN:

```shell

diff ${AIRFLOW_REPO_ROOT}/dist/airflow-chart-${VERSION}-source.tar.gz ${SVN_REPO_ROOT}/dev/airflow/helm-chart/${VERSION}${VERSION_SUFFIX}/airflow-chart-${VERSION}-source.tar.gz
diff ${AIRFLOW_REPO_ROOT}/dist/airflow-${VERSION}.tgz ${SVN_REPO_ROOT}/dev/airflow/helm-chart/${VERSION}${VERSION_SUFFIX}/airflow-${VERSION}.tgz
```

There should be no differences reported. If you see "binary files differ" message, it means that
the source tarball is not reproducible. This is not a blocker for the release, you can unpack the sources
of the tarball and compare the two tarballs and check the differences for example
with `diff -R <DIR1> <DIR2>`. It could be that our reproducible build script is not working correctly yet,
and we need to fix it (so checking the differences would be helpful also to find out what is wrong).

Before proceeding next you want to go to the SVN directory

```shell
cd ${SVN_REPO_ROOT}/dev/airflow/helm-chart/${VERSION}${VERSION_SUFFIX}
```

## Licence check

This can be done with the Apache RAT tool.

* Download the latest jar from https://creadur.apache.org/rat/download_rat.cgi (unpack the binary,
  the jar is inside)
* Unpack the release source archive (the `<package + version>-source.tar.gz` file) to a folder
* Enter the sources folder run the check

```shell
java -jar ${PATH_TO_RAT}/apache-rat-0.13/apache-rat-0.13.jar chart -E .rat-excludes
```

where `.rat-excludes` is the file in the root of Chart source code.

## Signature check

Make sure you have imported into your GPG the PGP key of the person signing the release. You can find the valid keys in
[KEYS](https://dist.apache.org/repos/dist/release/airflow/KEYS).

You can import the whole KEYS file:

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
Checking airflow-1.0.0.tgz.asc
gpg: assuming signed data in 'airflow-1.0.0.tgz'
gpg: Signature made Sun 16 May 01:25:24 2021 BST
gpg:                using RSA key CDE15C6E4D3A8EC4ECF4BA4B6674E08AD7DE406F
gpg:                issuer "kaxilnaik@apache.org"
gpg: Good signature from "Kaxil Naik <kaxilnaik@apache.org>" [unknown]
gpg:                 aka "Kaxil Naik <kaxilnaik@gmail.com>" [unknown]
gpg: WARNING: The key's User ID is not certified with a trusted signature!
gpg:          There is no indication that the signature belongs to the owner.
Primary key fingerprint: CDE1 5C6E 4D3A 8EC4 ECF4  BA4B 6674 E08A D7DE 406F

Checking airflow-chart-1.0.0-source.tar.gz.asc
gpg: assuming signed data in 'airflow-chart-1.0.0-source.tar.gz'
gpg: Signature made Sun 16 May 02:24:09 2021 BST
gpg:                using RSA key CDE15C6E4D3A8EC4ECF4BA4B6674E08AD7DE406F
gpg:                issuer "kaxilnaik@apache.org"
gpg: Good signature from "Kaxil Naik <kaxilnaik@apache.org>" [unknown]
gpg:                 aka "Kaxil Naik <kaxilnaik@gmail.com>" [unknown]
gpg: WARNING: The key's User ID is not certified with a trusted signature!
gpg:          There is no indication that the signature belongs to the owner.
Primary key fingerprint: CDE1 5C6E 4D3A 8EC4 ECF4  BA4B 6674 E08A D7DE 406F
```

## SHA512 sum check

Run this:

```shell
for i in *.sha512
do
    echo "Checking $i"; shasum -a 512 `basename $i .sha512 ` | diff - $i
done
```

You should get output similar to:

```
Checking airflow-1.0.0.tgz.sha512
Checking airflow-chart-1.0.0-source.tar.gz.sha512
```

# Verify release candidates by Contributors

Contributors can run below commands to test the Helm Chart

```shell
helm repo add apache-airflow-dev https://dist.apache.org/repos/dist/dev/airflow/helm-chart/1.1.0rc1/
helm repo update
helm install airflow apache-airflow-dev/airflow
```

You can then perform any other verifications to check that it works as you expected by
upgrading the Chart or installing by overriding default of `values.yaml`.

# Publish the final release

## Summarize the voting for the release

Once the vote has been passed, you will need to send a result vote to dev@airflow.apache.org:

Subject:

```
[RESULT][VOTE] Release Apache Airflow Helm Chart 1.1.0 based on 1.1.0rc1
```

Message:

```
Hello all,

The vote to release Apache Airflow Helm Chart version 1.1.0 based on 1.1.0rc1 is now closed.

The vote PASSED with 4 binding "+1", 4 non-binding "+1" and 0 "-1" votes:

"+1" Binding votes:

  - Kaxil Naik
  - Jarek Potiuk
  - Ash Berlin-Taylor
  - Xiaodong Deng

"+1" Non-Binding votes:

  - Jed Cunningham
  - Ephraim Anierobi
  - Dennis Akpenyi
  - Ian Stanton

Vote thread:
https://lists.apache.org/thread.html/r865f041e491a2a7a52e17784abf0d0f2e35c3bac5ae8a05927285558%40%3Cdev.airflow.apache.org%3E

I'll continue with the release process and the release announcement will follow shortly.

Thanks,
<your name>
```

## Publish release to SVN

You need to migrate the RC artifacts that passed to this repository:
https://dist.apache.org/repos/dist/release/airflow/helm-chart/
(The migration should include renaming the files so that they no longer have the RC number in their filenames.)

The best way of doing this is to svn cp between the two repos (this avoids having to upload
the binaries again, and gives a clearer history in the svn commit logs):

```shell
# First clone the repo
export VERSION=1.1.0
export VERSION_SUFFIX=rc1
svn checkout https://dist.apache.org/repos/dist/release/airflow airflow-release

# Create new folder for the release
cd airflow-release/helm-chart
export AIRFLOW_SVN_RELEASE_HELM=$(pwd -P)
svn mkdir ${VERSION}
cd ${VERSION}

# Move the artifacts to svn folder & commit (don't copy or copy & remove - index.yaml)
for f in ../../../airflow-dev/helm-chart/${VERSION}${VERSION_SUFFIX}/*; do svn cp $f ${$(basename $f)/}; done
svn rm index.yaml
svn commit -m "Release Airflow Helm Chart Check ${VERSION} from ${VERSION}${VERSION_SUFFIX}"

```

Verify that the packages appear in [Airflow Helm Chart](https://dist.apache.org/repos/dist/release/airflow/helm-chart/).

## Publish release tag

Create and push the release tag:

```shell
cd "${AIRFLOW_REPO_ROOT}"
git checkout helm-chart/${VERSION}${VERSION_SUFFIX}
git tag -s helm-chart/${VERSION} -m "Apache Airflow Helm Chart ${VERSION}"
git push apache helm-chart/${VERSION}
```

## Publish final documentation

Documentation is an essential part of the product and should be made available to users.
In our cases, documentation for the released versions is published in S3 bucket, and the site is
kept in a separate repository - [`apache/airflow-site`](https://github.com/apache/airflow-site),
but the documentation source code and build tools are available in the `apache/airflow` repository, so
you need to run several workflows to publish the documentation. More details about it can be found in
[Docs README](../docs/README.md) showing the architecture and workflows including manual workflows for
emergency cases.

You should use the `breeze` command to publish the documentation.
The command does the following:

1. Triggers [Publish Docs to S3](https://github.com/apache/airflow/actions/workflows/publish-docs-to-s3.yml).
2. Triggers workflow in apache/airflow-site to refresh
3. Triggers S3 to GitHub Sync

```shell script
breeze workflow-run publish-docs --ref <tag> --site-env <staging/live/auto> helm-chart
```

The `--ref` parameter should be the tag of the release candidate you are publishing. This should be a
release tag like `helm-chart/1.1.0`

The `--site-env` parameter should be set to `staging` for pre-release versions or `live` for final releases.
The default option is `auto` which should automatically select the right environment based on the tag name.

Other available parameters can be found with:

```shell script
breeze workflow-run publish-docs --help
```

## Update `index.yaml` in airflow-site

Regenerate `index.yaml` so it can be added to the Airflow website to allow: `helm repo add apache-airflow https://airflow.apache.org`.

```shell
git clone https://github.com/apache/airflow-site.git airflow-site
cd airflow-site
curl https://dist.apache.org/repos/dist/dev/airflow/helm-chart/${VERSION}${VERSION_SUFFIX}/index.yaml -o index.yaml
cp ${AIRFLOW_SVN_RELEASE_HELM}/${VERSION}/airflow-${VERSION}.tgz .
helm repo index --merge ./index.yaml . --url "https://downloads.apache.org/airflow/helm-chart/${VERSION}"
rm airflow-${VERSION}.tgz
mv index.yaml landing-pages/site/static/index.yaml
git add p . # leave the license at the top
git commit -m "Add Apache Airflow Helm Chart Release ${VERSION} to chart index file"
git push
# and finally open a PR
```

## Wait for ArtifactHUB to discover new release

As we link out to ArtifactHUB in all of our release communications, we now wait until ArtifactHUB has discovered the new release. This can take 30 minutes or so to happen after the index change PR from above is merged.

## Notify developers of release

- Notify users@airflow.apache.org (cc'ing dev@airflow.apache.org) that
the artifacts have been published:

Subject:

```shell
cat <<EOF
[ANNOUNCE] Apache Airflow Helm Chart version ${VERSION} Released
EOF
```

Body:

```shell
cat <<EOF
Dear Airflow community,

I am pleased to announce that we have released Apache Airflow Helm chart ${VERSION} 🎉 🎊

The source release, as well as the "binary" Helm Chart release, are available:

📦   Official Sources: https://airflow.apache.org/docs/helm-chart/${VERSION}/installing-helm-chart-from-sources.html
📦   ArtifactHub: https://artifacthub.io/packages/helm/apache-airflow/airflow
📚   Docs: https://airflow.apache.org/docs/helm-chart/${VERSION}/
🚀   Quick Start Installation Guide: https://airflow.apache.org/docs/helm-chart/${VERSION}/quick-start.html
🛠️   Release Notes: https://airflow.apache.org/docs/helm-chart/${VERSION}/release_notes.html

Thanks to all the contributors who made this possible.

Cheers,
<your name>
EOF
```

Send the same email to announce@apache.org, except change the opening line to `Dear community,`.
It is more reliable to send it via the web ui at https://lists.apache.org/list.html?announce@apache.org
(press "c" to compose a new thread)

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

## Add release data to Apache Committee Report Helper

Add the release data (version and date) at: https://reporter.apache.org/addrelease.html?airflow

## Update Announcements page

Update "Announcements" page at the [Official Airflow website](https://airflow.apache.org/announcements/)

## Create release on GitHub

Create a new release on GitHub with the release notes and assets from the release svn.

## Close the milestone

Before closing the milestone on GitHub, make sure that all PR marked for it are either part of the release (was cherry picked) or
postponed to the next release, then close the milestone. Create the next one if it hasn't been already (it probably has been).
Update the new milestone in the [*Currently we are working on* issue](https://github.com/apache/airflow/issues/10176)
make sure to update the last updated timestamp as well.

## Close the testing status issue

Don't forget to thank the folks who tested and close the issue tracking the testing status.

## Update issue template with the new release

Updating issue templates in `.github/ISSUE_TEMPLATE/4-airflow_helmchart_bug_report.yml` with the new version

## Announce the release on the community slack

Post this in the #announce channel:

```shell
cat <<EOF
We've just released Apache Airflow Helm Chart ${VERSION} 🎉

📦 ArtifactHub: https://artifacthub.io/packages/helm/apache-airflow/airflow
📚 Docs: https://airflow.apache.org/docs/helm-chart/${VERSION}/
🚀 Quick Start Installation Guide: https://airflow.apache.org/docs/helm-chart/${VERSION}/quick-start.html
🛠 Release Notes: https://airflow.apache.org/docs/helm-chart/${VERSION}/release_notes.html

Thanks to all the contributors who made this possible.
EOF
```

## Announce about the release in social media

------------------------------------------------------------------------------------------------------------
Announcement is done from official Apache-Airflow accounts.

* LinkedIn: https://www.linkedin.com/company/apache-airflow/
* Fosstodon: https://fosstodon.org/@airflow
* Bluesky: https://bsky.app/profile/apache-airflow.bsky.social

Make sure attach the release image generated with Figma to the post.
If you don't have access to the account ask a PMC member to post.

------------------------------------------------------------------------------------------------------------

Tweet and post on Linkedin about the release:

```shell
cat <<EOF
We've just released Apache Airflow Helm chart ${VERSION} 🎉

📦 ArtifactHub: https://artifacthub.io/packages/helm/apache-airflow/airflow
📚 Docs: https://airflow.apache.org/docs/helm-chart/${VERSION}/
🛠️ Release Notes: https://airflow.apache.org/docs/helm-chart/${VERSION}/release_notes.html

Thanks to all the contributors who made this possible.
EOF
```

## Bump chart version in Chart.yaml

Bump the chart version to the next version in `chart/Chart.yaml` in main.


## Remove old releases

We should keep the old version a little longer than a day or at least until the updated
``index.yaml`` is published. This is to avoid errors for users who haven't run ``helm repo update``.

It is probably ok if we leave last 2 versions on release svn repo too.

```shell
# http://www.apache.org/legal/release-policy.html#when-to-archive
cd airflow-release/helm-chart
export PREVIOUS_VERSION=1.0.0
svn rm ${PREVIOUS_VERSION}
svn commit -m "Remove old Helm Chart release: ${PREVIOUS_VERSION}"
```

# Additional processes

## Fixing released documentation

Sometimes we want to rebuild the documentation with some fixes that were merged in main
branch, for example when there are html layout changes or typo fixes, or formatting issue fixes.

In this case the process is as follows:

* When you want to re-publish `helm-chart/X.Y.Z` docs, create (or pull if already created)
  `helm-chart/X.Y.Z-docs` branch
* Cherry-pick changes you want to add and push to the main `apache/airflow` repo
* Run the publishing workflow.

In case you are releasing latest released version of helm-chart (which should be most of the cases), run this:

```bash
breeze workflow-run publish-docs --site-env live --ref helm-chart/X.Y.Z-docs \
   --skip-tag-validation \
   helm-chart
```

In case you are releasing an older version of helm-chart, you should skip writing to the stable folder

```bash
breeze workflow-run publish-docs --site-env live --ref helm-chart/X.Y.Z-docs \
   --skip-tag-validation \
   --skip-write-to-stable-folder \
   helm-chart
```
