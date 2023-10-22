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
  - [Making the cherry picking](#making-the-cherry-picking)
  - [Reviewing cherry-picked PRs and assigning labels](#reviewing-cherry-picked-prs-and-assigning-labels)
- [Prepare the Apache Airflow Package RC](#prepare-the-apache-airflow-package-rc)
  - [Update the milestone](#update-the-milestone)
  - [Build RC artifacts](#build-rc-artifacts)
  - [Prepare production Docker Image RC](#prepare-production-docker-image-rc)
  - [Prepare Vote email on the Apache Airflow release candidate](#prepare-vote-email-on-the-apache-airflow-release-candidate)
- [Verify the release candidate by PMC members](#verify-the-release-candidate-by-pmc-members)
  - [SVN check](#svn-check)
  - [Licence check](#licence-check)
  - [Signature check](#signature-check)
  - [SHA512 sum check](#sha512-sum-check)
  - [Source code check](#source-code-check)
- [Verify the release candidate by Contributors](#verify-the-release-candidate-by-contributors)
  - [Installing release candidate in your local virtual environment](#installing-release-candidate-in-your-local-virtual-environment)
- [Publish the final Apache Airflow release](#publish-the-final-apache-airflow-release)
  - [Summarize the voting for the Apache Airflow release](#summarize-the-voting-for-the-apache-airflow-release)
  - [Publish release to SVN](#publish-release-to-svn)
  - [Manually prepare production Docker Image](#manually-prepare-production-docker-image)
  - [Verify production images](#verify-production-images)
  - [Publish documentation](#publish-documentation)
  - [Notify developers of release](#notify-developers-of-release)
  - [Send announcements about security issues fixed in the release](#send-announcements-about-security-issues-fixed-in-the-release)
  - [Add release data to Apache Committee Report Helper](#add-release-data-to-apache-committee-report-helper)
  - [Update Announcements page](#update-announcements-page)
  - [Create release on GitHub](#create-release-on-github)
  - [Close the milestone](#close-the-milestone)
  - [Close the testing status issue](#close-the-testing-status-issue)
  - [Announce the release on the community slack](#announce-the-release-on-the-community-slack)
  - [Announce about the release in social media](#announce-about-the-release-in-social-media)
  - [Update `main` with the latest release details](#update-main-with-the-latest-release-details)
  - [Update default Airflow version in the helm chart](#update-default-airflow-version-in-the-helm-chart)
  - [Update airflow/config_templates/config.yml file](#update-airflowconfig_templatesconfigyml-file)
  - [API clients](#api-clients)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

You can find the prerequisites to release Apache Airflow in [README.md](README.md).

# Selecting what to put into the release

The first step of a release is to work out what is being included. This differs based on whether it is a major/minor or a patch release.

- For a *major* or *minor* release, you want to include everything in `main` at the time of release; you'll turn this into a new release branch as part of the rest of the process.

- For a *patch* release, you will be selecting specific commits to cherry-pick and backport into the existing release branch.

## Selecting what to cherry-pick

For obvious reasons, you can't cherry-pick every change from `main` into the release branch -
some are incompatible without a large set of other changes, some are brand-new features, and some just don't need to be in a release.

In general only security fixes, data-loss bugs and regression fixes are essential to bring into a patch release;
also changes in dependencies (setup.py, setup.cfg) resulting from releasing newer versions of packages that Airflow depends on.
Other bugfixes can be added on a best-effort basis, but if something is going to be very difficult to backport
(maybe it has a lot of conflicts, or heavily depends on a new feature or API that's not being backported),
it's OK to leave it out of the release at your sole discretion as the release manager -
if you do this, update the milestone in the issue to the "next" minor release.

Many issues will be marked with the target release as their Milestone; this is a good shortlist
to start with for what to cherry-pick.

For a patch release, find out other bug fixes that are not marked with the target release as their Milestone
and mark those as well. You can accomplish this by running the following command:

```
./dev/airflow-github needs-categorization 2.3.2 HEAD
```

Often you also want to cherry-pick changes related to CI and development tools, to include the latest
stability fixes in CI and improvements in development tools. Usually you can see the list of such
changes via (this will exclude already merged changes):

```shell
git fetch apache
git log --oneline apache/v2-2-test | sed -n 's/.*\((#[0-9]*)\)$/\1/p' > /tmp/merged
git log --oneline --decorate apache/v2-2-stable..apache/main -- Dockerfile* scripts breeze* .github/ setup* dev | grep -vf /tmp/merged
```

Most of those PRs should be marked with `changelog:skip` label, so that they are excluded from the
user-facing changelog as they only matter for developers of Airflow. We have a tool
that allows to easily review the cherry-picked PRs and mark them with the right label - see below.

You also likely want to cherry-pick some of the latest doc changes in order to bring clarification and
explanations added to the documentation. Usually you can see the list of such changes via:

```shell
git fetch apache
git log --oneline apache/v2-2-test | sed -n 's/.*\((#[0-9]*)\)$/\1/p' > /tmp/merged
git log --oneline --decorate apache/v2-2-stable..apache/main -- docs/apache-airflow docs/docker-stack/ | grep -vf /tmp/merged
```

Those changes that are "doc-only" changes should be marked with `type:doc-only` label so that they
land in documentation part of the changelog. The tool to review and assign the labels is described below.

## Making the cherry picking

It is recommended to clone Airflow upstream (not your fork) and run the commands on
the relevant test branch in this clone. That way origin points to the upstream repo.

To see cherry picking candidates (unmerged PR with the appropriate milestone), from the test
branch you can run:

```shell
./dev/airflow-github compare 2.1.2 --unmerged
```

You can start cherry picking from the bottom of the list. (older commits first)

When you cherry-pick, pick in chronological order onto the `vX-Y-test` release branch.
You'll move them over to be on `vX-Y-stable` once the release is cut. Use the `-x` option
to keep a reference to the original commit we cherry picked from. ("cherry picked from commit ...")

```shell
git cherry-pick <hash-commit> -x
```

## Reviewing cherry-picked PRs and assigning labels

We have the tool that allows to review cherry-picked PRs and assign the labels
[./assign_cherry_picked_prs_with_milestone.py](./assign_cherry_picked_prs_with_milestone.py)

It allows to manually review and assign milestones and labels to cherry-picked PRs:

```shell
./dev/assign_cherry_picked_prs_with_milestone.py assign-prs --previous-release v2-2-stable --current-release apache/v2-2-test --milestone-number 48
```

It summarises the state of each cherry-picked PR including information whether it is going to be
excluded or included in changelog or included in doc-only part of it. It also allows to re-assign
the PRs to the target milestone and apply the `changelog:skip` or `type:doc-only` label.

You can also add `--skip-assigned` flag if you want to automatically skip the question of assignment
for the PRs that are already correctly assigned to the milestone. You can also avoid the "Are you OK?"
question with `--assume-yes` flag.

You can review the list of PRs cherry-picked and produce a nice summary with `--print-summary` (this flag
assumes the `--skip-assigned` flag, so that the summary can be produced without questions:

```shell
./dev/assign_cherry_picked_prs_with_milestone.py assign-prs --previous-release v2-2-stable \
  --current-release apache/v2-2-test --milestone-number 48 --skip-assigned --assume-yes --print-summary \
  --output-folder /tmp
```

This will produce summary output with nice links that you can use to review the cherry-picked changes,
but it also produces files with list of commits separated by type in the folder specified. In the case
above, it will produce three files that you can use in the next step:

```
Changelog commits written in /tmp/changelog-changes.txt

Doc only commits written in /tmp/doc-only-changes.txt

Excluded commits written in /tmp/excluded-changes.txt
```

You can see for example which files have been changed by "doc-only" or "excluded" changes, to make sure
that no "sneaky" changes were by mistake classified wrongly.

```shell
git show --format=tformat:"" --stat --name-only $(cat /tmp/doc-only-changes.txt) | sort | uniq
```

Then if you see suspicious file (example airflow/sensors/base.py) you can find details on where they came from:

```shell
git log apache/v2-2-test --format="%H" -- airflow/sensors/base.py | grep -f /tmp/doc-only-changes.txt | xargs git show
```

And the URL to the PR it comes from:

```shell
git log apache/v2-2-test --format="%H" -- airflow/sensors/base.py | grep -f /tmp/doc-only-changes.txt | \
    xargs -n 1 git log --oneline --max-count=1 | \
    sed s'/.*(#\([0-9]*\))$/https:\/\/github.com\/apache\/airflow\/pull\/\1/'
```

# Prepare the Apache Airflow Package RC

## Update the milestone

Before cutting an RC, we should look at the milestone and merge anything ready, or if we aren't going to include it in the release we should update the milestone for those issues. We should do that before cutting the RC so the milestone gives us an accurate view of what is going to be in the release as soon as we know what it will be.

## Build RC artifacts

The Release Candidate artifacts we vote upon should be the exact ones we vote against, without any modification other than renaming – i.e. the contents of the files must be the same between voted release candidate and final release. Because of this the version in the built artifacts that will become the official Apache releases must not include the rcN suffix.

- Set environment variables

    ```shell script

    # You can avoid repeating this command for every release if you will set it in .zshrc
    # see https://unix.stackexchange.com/questions/608842/zshrc-export-gpg-tty-tty-says-not-a-tty
    export GPG_TTY=$(tty)

    # Set Version
    export VERSION=2.1.2rc3
    export VERSION_SUFFIX=rc3
    export VERSION_BRANCH=2-1
    export VERSION_WITHOUT_RC=${VERSION/rc?/}

    # Set AIRFLOW_REPO_ROOT to the path of your git repo
    export AIRFLOW_REPO_ROOT=$(pwd)


    # Example after cloning
    git clone https://github.com/apache/airflow.git airflow
    cd airflow
    export AIRFLOW_REPO_ROOT=$(pwd)
    ```

- Install `breeze` command:

    ```shell script
    pipx install -e ./dev/breeze
    ```



- For major/minor version release, run the following commands to create the 'test' and 'stable' branches.

    ```shell script
    breeze release-management create-minor-branch --version-branch ${VERSION_BRANCH}
    ```

- Check out the 'test' branch

    ```shell script
    git checkout v${VERSION_BRANCH}-test
    git reset --hard origin/v${VERSION_BRANCH}-test
    ```

- Set your version in `airflow/__init__.py`, `airflow/api_connexion/openapi/v1.yaml` and `docs/` (without the RC tag).
- Add supported Airflow version to `./scripts/ci/pre_commit/pre_commit_supported_versions.py` and let pre-commit do the job.
- Replace the version in `README.md` and verify that installation instructions work fine.
- Check `Apache Airflow is tested with` (stable version) in `README.md` has the same tested versions as in the tip of
  the stable branch in `dev/breeze/src/airflow_breeze/global_constants.py`
- Build the release notes:

  Preview with:

    ```shell script
    towncrier build --draft --version=${VERSION_WITHOUT_RC} --date=2021-12-15 --dir . --config newsfragments/config.toml
    ```


  Then remove the `--draft` flag to have towncrier build the release notes for real.

  If no significant changes were added in this release, add the header and put "No significant changes." (e.g. `2.1.4`).

  This will partly generate the release note based on the fragments (adjust to rst format). All PRs does not necessarily
  create a fragment to document its change, to generate the body of the release note based on the cherry picked commits:

  ```
  ./dev/airflow-github changelog v2-3-stable v2-3-test
  ```

- Commit the version change.

- PR from the 'test' branch to the 'stable' branch

- When the PR is approved, install `dev/breeze` in a virtualenv:

    ```shell script
    pip install -e ./dev/breeze
    ```

- Set `GITHUB_TOKEN` environment variable. Needed in patch release for generating issue for testing of the RC.
    You can generate the token by following [this link](https://github.com/settings/tokens/new?description=Read%20sssues&scopes=repo:status)

    ```shell script
    export GITHUB_TOKEN="my_token"
    ```

- Start the release candidate process by running the below command (If you have not generated a key yet, generate it by following instructions on
    http://www.apache.org/dev/openpgp.html#key-gen-generate-key):

    ```shell script
    git checkout main
    git pull # Ensure that the script is up-to-date
    breeze release-management start-rc-process --version ${VERSION} --previous-version <PREVIOUS_VERSION>
    ```

## Prepare production Docker Image RC

Production Docker images should be manually prepared and pushed by the release manager or another committer
who has access to Airflow's DockerHub. Note that we started releasing a multi-platform build, so you need
to have an environment prepared to build multi-platform images. You can achieve it with:

* GitHub Actions Manual Job (easiest)
* Emulation (very slow)
* Hardware builders if you have both AMD64 and ARM64 hardware locally

Building the image is triggered by running the
[Release PROD Images](https://github.com/apache/airflow/actions/workflows/release_dockerhub_image.yml) workflow.

When you trigger it you need to pass Airflow Version (including the right rc suffix). Make sure to use the
``v2-*-stable`` branch for the workflow.

You can leave the "skip latest" field empty.


![Release prod image](images/release_prod_image_rc.png)

The manual building is described in [MANUALLY_BUILDING_IMAGES.md](MANUALLY_BUILDING_IMAGES.md).

## Prepare Vote email on the Apache Airflow release candidate

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
which will last at least 72 hours, from Friday, October 8, 2021 at 4:00 pm UTC
until Monday, October 11, 2021 at 4:00 pm UTC, and until 3 binding +1 votes have been received.

https://www.timeanddate.com/worldclock/fixedtime.html?msg=8&iso=20211011T1600&p1=1440

Status of testing of the release is kept in TODO:URL_OF_THE_ISSUE_HERE

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

The test procedure for PMC members is described in:
https://github.com/apache/airflow/blob/main/dev/README_RELEASE_AIRFLOW.md#verify-the-release-candidate-by-pmc-members

The test procedure for and Contributors who would like to test this RC is described in:
https://github.com/apache/airflow/blob/main/dev/README_RELEASE_AIRFLOW.md#verify-the-release-candidate-by-contributors


Please note that the version number excludes the \`rcX\` string, so it's now
simply ${VERSION_WITHOUT_RC}. This will allow us to rename the artifact without modifying
the artifact checksums when we actually release.

Release Notes: https://github.com/apache/airflow/blob/${VERSION}/RELEASE_NOTES.rst

Changes since PREVIOUS_VERSION_OR_RC:
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

Note, For RC2/3 you may refer to shorten vote period as agreed in mailing list [thread](https://lists.apache.org/thread/cv194w1fqqykrhswhmm54zy9gnnv6kgm).

# Verify the release candidate by PMC members

PMC members should verify the releases in order to make sure the release is following the
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

* -source.tar.gz + .asc + .sha512
* .tar.gz + .asc + .sha512
* -py3-none-any.whl + .asc + .sha512

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
python check_files.py airflow -v {VERSION} -p {PATH_TO_SVN}
```

## Licence check

This can be done with the Apache RAT tool.

* Download the latest jar from https://creadur.apache.org/rat/download_rat.cgi (unpack the binary,
  the jar is inside)
* Unpack the release source archive (the `<package + version>-source.tar.gz` file) to a folder
* Enter the sources folder run the check

```shell script
java -jar ../../apache-rat-0.13/apache-rat-0.13.jar -E .rat-excludes -d .
```

where `.rat-excludes` is the file in the root of Airflow source code.

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
Checking apache-airflow-2.0.2rc4.tar.gz.asc
gpg: assuming signed data in 'apache-airflow-2.0.2rc4.tar.gz'
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
Checking apache-airflow-2.0.2rc4.tar.gz.sha512
Checking apache_airflow-2.0.2rc4-py2.py3-none-any.whl.sha512
Checking apache-airflow-2.0.2rc4-source.tar.gz.sha512
```

## Source code check

You should check if the sources in the packages produced are the same as coming from the tag in git.

In checked out sources of Airflow:

```bash
git checkout X.Y.Zrc1
export SOURCE_DIR=$(pwd)
```

Change to the directory where you have the packages from svn:

Check if sources are the same as in the tag:

```bash
cd X.Y.Zrc1
tar -xvzf *-source.tar.gz
pushd apache-airflow-X.Y.Z
diff -r airflow "${SOURCE_DIR}"
popd && rm -rf apache-airflow-X.Y.Z
```

The output should only miss some files - but they should not show any differences in the files:

```
Only in /Users/jarek/code/airflow: .DS_Store
Only in /Users/jarek/code/airflow: .asf.yaml
Only in /Users/jarek/code/airflow: .bash_aliases
Only in /Users/jarek/code/airflow: .bash_completion
Only in /Users/jarek/code/airflow: .bash_history
...
```


Check if .whl is the same as in tag:

```
unzip -d a *.whl
pushd a
diff -r airflow "${SOURCE_DIR}"
popd && rm -rf a
```

The output should only miss some files - but they should not show any differences in the files:

```
Only in /Users/jarek/code/airflow: .DS_Store
Only in /Users/jarek/code/airflow: .asf.yaml
Only in /Users/jarek/code/airflow: .bash_aliases
Only in /Users/jarek/code/airflow: .bash_completion
Only in /Users/jarek/code/airflow: .bash_history
...
```

Check if sdist are the same as in the tag:

```bash
cd X.Y.Zrc1
tar -xvzf apache-airflow-X.Y.Z.tar.gz
pushd apache-airflow-X.Y.Z
diff -r airflow "${SOURCE_DIR}"
popd && rm -rf apache-airflow-X.Y.Z
```

The output should only miss some files - but they should not show any differences in the files:

```
Only in /Users/jarek/code/airflow: .DS_Store
Only in /Users/jarek/code/airflow: .asf.yaml
Only in /Users/jarek/code/airflow: .bash_aliases
Only in /Users/jarek/code/airflow: .bash_completion
Only in /Users/jarek/code/airflow: .bash_history
...
```

# Verify the release candidate by Contributors

This can be done (and we encourage to) by any of the Contributors. In fact, it's best if the
actual users of Apache Airflow test it in their own staging/test installations. Each release candidate
is available on PyPI apart from SVN packages, so everyone should be able to install
the release candidate version.

But you can use any of the installation methods you prefer (you can even install it via the binary wheels
downloaded from the SVN).

## Installing release candidate in your local virtual environment

```shell script
pip install apache-airflow==<VERSION>rc<X>
```

Optionally it can be followed with constraints

```shell script
pip install apache-airflow==<VERSION>rc<X> \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-<VERSION>/constraints-3.8.txt"`
```

Note that the constraints contain python version that you are installing it with.

You can use any of the installation methods you prefer (you can even install it via the binary wheel
downloaded from the SVN).

There is also an easy way of installation with Breeze if you have the latest sources of Apache Airflow.
Running the following command will use tmux inside breeze, create `admin` user and run Webserver & Scheduler:

```shell script
breeze start-airflow --use-airflow-version 2.7.0rc1 --python 3.8 --backend postgres
```

You can also choose different executors and extras to install when you are installing airflow this way. For
example in order to run Airflow with CeleryExecutor and install celery, google and amazon provider (as of
Airflow 2.7.0, you need to have celery provider installed to run Airflow with CeleryExecutor) you can run:

```shell script
breeze start-airflow --use-airflow-version 2.7.0rc1 --python 3.8 --backend postgres \
  --executor CeleryExecutor --airflow-extras "celery,google,amazon"
```


Once you install and run Airflow, you should perform any verification you see as necessary to check
that the Airflow works as you expected.

Breeze also allows you to easily build and install pre-release candidates including providers by following
simple instructions described in
[Manually testing release candidate packages](https://github.com/apache/airflow/blob/main/TESTING.rst#manually-testing-release-candidate-packages)

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

4 "+1" binding votes received:
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
export RC=2.0.2rc5
export VERSION=${RC/rc?/}
# cd to the airflow repo directory and set the environment variable below
export AIRFLOW_REPO_ROOT=$(pwd)
# start the release process by running the below command
breeze release-management start-release --release-candidate ${RC} --previous-release <PREVIOUS RELEASE>
```

## Manually prepare production Docker Image

Building the image is triggered by running the
[Release PROD Images](https://github.com/apache/airflow/actions/workflows/release_dockerhub_image.yml) workflow.

When you trigger it you need to pass:

* Airflow Version
* Optional "true" in skip latest field if you do not want to re-tag the latest image

Make sure you use v2-*-stable branch to run the workflow.

![Release prod image](images/release_prod_image.png)

Note that by default the `latest` images tagged are aliased to the just released image which is the usual
way we release. For example when you are releasing 2.3.N image and 2.3 is our latest branch the new image is
marked as "latest".

In case we are releasing (which almost never happens so far) a critical bugfix release in one of
the older branches, you should set the "skip" field to true.

## Verify production images

```shell script
for PYTHON in 3.8 3.9 3.10 3.11
do
    docker pull apache/airflow:${VERSION}-python${PYTHON}
    breeze prod-image verify --image-name apache/airflow:${VERSION}-python${PYTHON}
done
docker pull apache/airflow:${VERSION}
breeze prod-image verify --image-name apache/airflow:${VERSION}
```

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
    breeze build-docs --package-filter apache-airflow --package-filter docker-stack --clean-build
    ```

- Now you can preview the documentation.

    ```shell script
    ./docs/start_doc_server.sh
    ```

- Copy the documentation to the ``airflow-site`` repository, create commit, push changes, open a PR and merge it when the build is green.

    ```shell script
    breeze release-management publish-docs --package-filter apache-airflow --package-filter docker-stack
    breeze release-management add-back-references apache-airflow --airflow-site-directory "${AIRFLOW_SITE_DIRECTORY}"
    breeze sbom update-sbom-information --airflow-version ${VERSION} --airflow-site-directory ${AIRFLOW_SITE_DIRECTORY} --force
    cd "${AIRFLOW_SITE_DIRECTORY}"
    git add .
    git commit -m "Add documentation for Apache Airflow ${VERSION}"
    git push
    # and finally open a PR
    ```


## Notify developers of release

- Notify users@airflow.apache.org (cc'ing dev@airflow.apache.org) that
the artifacts have been published:

Subject:

```shell script
cat <<EOF
[ANNOUNCE] Apache Airflow ${VERSION} Released
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

The documentation is available at:
https://airflow.apache.org/docs/apache-airflow/${VERSION}/

Find the release notes here for more details:
https://airflow.apache.org/docs/apache-airflow/${VERSION}/release_notes.html

Container images are published at:
https://hub.docker.com/r/apache/airflow/tags/?page=1&name=${VERSION}

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

Before closing the milestone on Github, make sure that all PR marked for it are either part of the release (was cherry picked) or
postponed to the next release, then close the milestone. Create the next one if it hasn't been already (it probably has been).
Update the new milestone in the [*Currently we are working on* issue](https://github.com/apache/airflow/issues/10176)
make sure to update the last updated timestamp as well.

## Close the testing status issue

Don't forget to thank the folks who tested and close the issue tracking the testing status.

## Announce the release on the community slack

Post this in the #announce channel:

```shell
cat <<EOF
We've just released Apache Airflow $VERSION 🎉

📦 PyPI: https://pypi.org/project/apache-airflow/$VERSION/
📚 Docs: https://airflow.apache.org/docs/apache-airflow/$VERSION/
🛠 Release Notes: https://airflow.apache.org/docs/apache-airflow/$VERSION/release_notes.html
🐳 Docker Image: "docker pull apache/airflow:$VERSION"
🚏 Constraints: https://github.com/apache/airflow/tree/constraints-$VERSION

Thanks to all the contributors who made this possible.
EOF
```

## Announce about the release in social media

------------------------------------------------------------------------------------------------------------
Announcement is done from official Apache-Airflow accounts.

* Twitter: https://twitter.com/ApacheAirflow
* Linkedin: https://www.linkedin.com/company/apache-airflow/
* Fosstodon: https://fosstodon.org/@airflow

Make sure attach the release image generated with Figma to the post.
If you don't have access to the account ask PMC to post.

------------------------------------------------------------------------------------------------------------

Tweet and post on Linkedin about the release:

```shell
cat <<EOF
We've just released Apache Airflow $VERSION 🎉

📦 PyPI: https://pypi.org/project/apache-airflow/$VERSION/
📚 Docs: https://airflow.apache.org/docs/apache-airflow/$VERSION/
🛠 Release Notes: https://airflow.apache.org/docs/apache-airflow/$VERSION/release_notes.html
🐳 Docker Image: "docker pull apache/airflow:$VERSION"

Thanks to all the contributors who made this possible.
EOF
```

## Update `main` with the latest release details

This includes:

- Modify `./scripts/ci/pre_commit/pre_commit_supported_versions.py` and let pre-commit do the job.
- For major/minor release, update version in `airflow/__init__.py`, `docs/docker-stack/` and `airflow/api_connexion/openapi/v1.yaml` to the next likely minor version release.
- Sync `RELEASE_NOTES.rst` (including deleting relevant `newsfragments`) and `README.md` changes.
- Updating `Dockerfile` with the new version.
- Updating `airflow_bug_report.yml` issue template in `.github/ISSUE_TEMPLATE/` with the new version.

## Update default Airflow version in the helm chart

Update the values of `airflowVersion`, `defaultAirflowTag` and `appVersion` in the helm chart so the next helm chart release
will use the latest released version. You'll need to update `chart/values.yaml`, `chart/values.schema.json` and
`chart/Chart.yaml`.

Add or adjust significant `chart/newsfragments` to express that the default version of Airflow has changed.

In `chart/Chart.yaml`, make sure the screenshot annotations are still all valid URLs.

## Update airflow/config_templates/config.yml file

File `airflow/config_templates/config.yml` contains documentation on all configuration options available in Airflow. The `version_added` fields must be updated when a new Airflow version is released.

- Get a diff between the released versions and the current local file on `main` branch:

    ```shell script
    ./dev/validate_version_added_fields_in_config.py
    ```

- Update `airflow/config_templates/config.yml` with the details, and commit it.


## API clients

After releasing airflow core, we need to check if we have to follow up with API clients release.

Clients are released in a separate process, with their own vote mostly because their version can mismatch the core release.
ASF policy does not allow to vote against multiple artifacts with different versions.

### API Clients versioning policy

For major/minor version release, always release new versions of the API clients.

- [Python client](https://github.com/apache/airflow-client-python)
- [Go client](https://github.com/apache/airflow-client-go)

For patch version release, you can also release patch versions of clients **only** if the patch is relevant to the clients.
A patch is considered relevant to the clients if it updates the [openapi specification](https://github.com/apache/airflow/blob/main/airflow/api_connexion/openapi/v1.yaml).
There are other external reasons for which we might want to release a patch version for clients only, but they are not
tied to an airflow release and therefore out of scope.

To determine if you should also release API clients you can run:

```shell
./dev/airflow-github api-clients-policy 2.3.2 2.3.3
```

> The patch version of each API client is not necessarily in sync with the patch that you are releasing.
> You need to check for each client what is the next patch version to be released.

### Releasing the clients

According to the policy above, if we have to release clients:

- Follow the specific release process for each API client:

    - [Python client](https://github.com/apache/airflow-client-python/blob/master/dev/README_RELEASE_CLIENT.md)
    - [Go client](https://github.com/apache/airflow-client-go/blob/master/dev/README_RELEASE_CLIENT.md)
