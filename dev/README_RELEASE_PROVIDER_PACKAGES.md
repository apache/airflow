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

- [What the provider packages are](#what-the-provider-packages-are)
- [Provider packages](#provider-packages)
- [Decide when to release](#decide-when-to-release)
- [Provider packages versioning](#provider-packages-versioning)
- [Prepare Regular Provider packages (RC)](#prepare-regular-provider-packages-rc)
  - [Increasing version number](#increasing-version-number)
  - [Generate release notes](#generate-release-notes)
  - [Open PR with suggested version releases](#open-pr-with-suggested-version-releases)
  - [Build provider packages for SVN apache upload](#build-provider-packages-for-svn-apache-upload)
  - [Build and sign the source and convenience packages](#build-and-sign-the-source-and-convenience-packages)
  - [Commit the source packages to Apache SVN repo](#commit-the-source-packages-to-apache-svn-repo)
  - [Publish the Regular convenience package to PyPI](#publish-the-regular-convenience-package-to-pypi)
  - [Add tags in git](#add-tags-in-git)
  - [Prepare documentation](#prepare-documentation)
  - [Prepare issue in GitHub to keep status of testing](#prepare-issue-in-github-to-keep-status-of-testing)
  - [Prepare voting email for Providers release candidate](#prepare-voting-email-for-providers-release-candidate)
  - [Verify the release by PMC members](#verify-the-release-by-pmc-members)
  - [Verify by Contributors](#verify-by-contributors)
- [Publish release](#publish-release)
  - [Summarize the voting for the Apache Airflow release](#summarize-the-voting-for-the-apache-airflow-release)
  - [Publish release to SVN](#publish-release-to-svn)
  - [Publish the packages to PyPI](#publish-the-packages-to-pypi)
  - [Publish documentation prepared before](#publish-documentation-prepared-before)
  - [Add tags in git](#add-tags-in-git-1)
  - [Notify developers of release](#notify-developers-of-release)
  - [Send announcements about security issues fixed in the release](#send-announcements-about-security-issues-fixed-in-the-release)
  - [Announce about the release in social media](#announce-about-the-release-in-social-media)
  - [Add release data to Apache Committee Report Helper](#add-release-data-to-apache-committee-report-helper)
  - [Close the testing status issue](#close-the-testing-status-issue)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

------------------------------------------------------------------------------------------------------------

# What the provider packages are

The Provider packages are separate packages (one package per provider) that implement
integrations with external services for Airflow in the form of installable Python packages.

The Release Manager prepares packages separately from the main Airflow Release, using
`breeze` commands and accompanying scripts. This document provides an overview of the command line tools
needed to prepare the packages.

NOTE!! When you have problems with any of those commands that run inside `breeze` docker image, you
can run the command with `--debug` flag that will drop you in the shell inside the image and will
print the command that you should run.

# Provider packages

The prerequisites to release Apache Airflow are described in [README.md](README.md).

You can read more about the command line tools used to generate the packages in the
[Provider packages](PROVIDER_PACKAGE_DETAILS.md).

# Decide when to release

You can release provider packages separately from the main Airflow on an ad-hoc basis, whenever we find that
a given provider needs to be released - due to new features or due to bug fixes.
You can release each provider package separately, but due to voting and release overhead we try to group
releases of provider packages together.

# Provider packages versioning

We are using the [SEMVER](https://semver.org/) versioning scheme for the provider packages. This is in order
to give the users confidence about maintaining backwards compatibility in the new releases of those
packages.

Details about maintaining the SEMVER version are going to be discussed and implemented in
[the related issue](https://github.com/apache/airflow/issues/11425)

# Prepare Regular Provider packages (RC)

## Increasing version number

First thing that release manager has to do is to change version of the provider to a target
version. Each provider has a `provider.yaml` file that, among others, stores information
about provider versions. When you attempt to release a provider you should update that
information based on the changes for the provider, and it's `CHANGELOG.rst`. It might be that
`CHANGELOG.rst` already contains the right target version. This will be especially true if some
changes in the provider add new features (then minor version is increased) or when the changes
introduce backwards-incompatible, breaking change in the provider (then major version is
incremented). Committers, when approving and merging changes to the providers, should pay attention
that the `CHANGELOG.rst` is updated whenever anything other than bugfix is added.

If there are no new features or breaking changes, the release manager should simply increase the
patch-level version for the provider.

The new version should be first on the list.

## Generate release notes

Each of the provider packages contains Release notes in the form of the `CHANGELOG.rst` file that is
automatically generated from history of the changes and code of the provider.
They are stored in the documentation directory. The `README.md` file generated during package
preparation is not stored anywhere in the repository - it contains however link to the Changelog
generated.

When the provider package version has not been updated since the latest version, the release notes
are not generated. Release notes are only generated, when the latest version of the package does not
yet have a corresponding TAG.

The tags for providers is of the form ``providers-<PROVIDER_ID>/<VERSION>`` for example
``providers-amazon/1.0.0``. During releasing, the RC1/RC2 tags are created (for example
``providers-amazon/1.0.0rc1``).

Details about maintaining the SEMVER version are going to be discussed and implemented in
[the related issue](https://github.com/apache/airflow/issues/11425)

```shell script
breeze release-management prepare-provider-documentation [packages]
```

This command will not only prepare documentation but will also help the release manager to review
changes implemented in all providers, and determine which of the providers should be released. For each
provider details will be printed on what changes were implemented since the last release including
links to particular commits.

This should help to determine which version of provider should be released:

* increased patch-level for bugfix-only change
* increased minor version if new features are added
* increased major version if breaking changes are added

It also helps the release manager to update CHANGELOG.rst where high-level overview of the changes should be documented for the providers released.
You should iterate and re-generate the same content after any change as many times as you want.
The generated files should be added and committed to the repository.

When you want to regenerate the changes before the release and make sure all changelogs
are updated, run it in non-interactive mode:

```shell script
breeze release-management prepare-provider-documentation --answer yes [packages]
```

NOTE!! In case you prepare provider's documentation in a branch different than main, you need to manually
specify the base branch via `--base-branch` parameter.
For example if you try to build a `cncf.kubernetes` provider that is build from `provider-cncf-kubernetes/v4-4`
branch should be prepared like this:

```shell script
breeze release-management prepare-provider-documentation \
 --base-branch provider-cncf-kubernetes/v4-4 cncf.kubernetes
```

## Open PR with suggested version releases

At this point you should have providers yaml files and changelog updated.
You should go over the change log and place changes in their relevant section (breaking change, feature, bugs, etc...)
Once finished you should raise a PR : Prepare docs for MM YYYY wave of Providers
In the PR we will verify if we want to release a specific package or if the versions chosen are right.
Only after PR is merged you should proceed to next steps.


## Build provider packages for SVN apache upload

Those packages might get promoted  to "final" packages by just renaming the files, so internally they
should keep the final version number without the rc suffix, even if they are rc1/rc2/... candidates.

They also need to be signed and have checksum files. You can generate the checksum/signature files by running
the "dev/sign.sh" script (assuming you have the right PGP key set-up for signing). The script
generates corresponding .asc and .sha512 files for each file to sign.

## Build and sign the source and convenience packages

* Cleanup dist folder:

```shell script
export AIRFLOW_REPO_ROOT=$(pwd)
rm -rf ${AIRFLOW_REPO_ROOT}/dist/*
```


* Release candidate packages:

```shell script
breeze release-management prepare-provider-packages --package-format both
```

if you only build few packages, run:

```shell script
breeze release-management prepare-provider-packages --package-format both PACKAGE PACKAGE ....
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
cd ..
[ -d asf-dist ] || svn checkout --depth=immediates https://dist.apache.org/repos/dist asf-dist
svn update --set-depth=infinity asf-dist/dev/airflow

# Create a new folder for the release.
cd asf-dist/dev/airflow/providers

# Remove previously released providers
svn rm *

# Move the artifacts to svn folder
mv ${AIRFLOW_REPO_ROOT}/dist/* .

# Add and commit
svn add *
svn commit -m "Add artifacts for Airflow Providers $(date "+%Y-%m-%d%n")"

cd ${AIRFLOW_REPO_ROOT}
```

Verify that the files are available at
[providers](https://dist.apache.org/repos/dist/dev/airflow/providers/)

You should see only providers that you are about to release.
If you are seeing others there is an issue.
You can remove the redundant provider files manually with:

```shell script
svn rm file_name  // repeate that for every file
svn commit -m "delete old providers"
```

## Publish the Regular convenience package to PyPI

In order to publish release candidate to PyPI you just need to build and release packages.
The packages should however contain the rcN suffix in the version file name but not internally in the package,
so you need to use `--version-suffix-for-pypi` switch to prepare those packages.
Note that these are different packages than the ones used for SVN upload
though they should be generated from the same sources.

* Generate the packages with the right RC version (specify the version suffix with PyPI switch). Note that
this will clean up dist folder before generating the packages, so you will only have the right packages there.

```shell script
rm -rf ${AIRFLOW_REPO_ROOT}/dist/*

breeze release-management prepare-provider-packages --version-suffix-for-pypi rc1 --package-format both
```

if you only build few packages, run:

```shell script
breeze release-management prepare-provider-packages --version-suffix-for-pypi rc1 --package-format both PACKAGE PACKAGE ....
```

* Verify the artifacts that would be uploaded:

```shell script
twine check ${AIRFLOW_REPO_ROOT}/dist/*
```

* Upload the package to PyPi's test environment:

```shell script
twine upload -r pypitest ${AIRFLOW_REPO_ROOT}/dist/*
```

If you see
> WARNING  Error during upload. Retry with the --verbose option for more details.
ERROR   HTTPError: 403 Forbidden from https://test.pypi.org/legacy/
     The user [user_name] isn't allowed to upload to project [provider_name]

It means that you don't have permission to upload providers.
Please ask one of the Admins to grant you permissions on the packages you wish to release.


* Verify that the test packages look good by downloading it and installing them into a virtual environment.
Twine prints the package links as output - separately for each package.

* Upload the package to PyPi's production environment:

```shell script
twine upload -r pypi ${AIRFLOW_REPO_ROOT}/dist/*
```

* Again, confirm that the packages are available under the links printed.


## Add tags in git

Assume that your remote for apache repository is called `apache` you should now
set tags for the providers in the repo.

```shell script
./dev/provider_packages/tag_providers.sh
```

## Prepare documentation

Documentation is an essential part of the product and should be made available to users.
In our cases, documentation  for the released versions is published in a separate repository -
[`apache/airflow-site`](https://github.com/apache/airflow-site), but the documentation source code
and build tools are available in the `apache/airflow` repository, so you have to coordinate between
the two repositories to be able to build the documentation.

Documentation for providers can be found in the `/docs/apache-airflow-providers` directory
and the `/docs/apache-airflow-providers-*/` directory. The first directory contains the package contents
lists and should be updated every time a new version of provider packages is released.

- First, copy the airflow-site repository and set the environment variable ``AIRFLOW_SITE_DIRECTORY``.

```shell script
git clone https://github.com/apache/airflow-site.git airflow-site
cd airflow-site
export AIRFLOW_SITE_DIRECTORY="$(pwd)"
```

Note if this is not the first time you clone the repo make sure main branch is rebased:

```shell script
cd "${AIRFLOW_SITE_DIRECTORY}"
git checkout main
git pull --rebase
```

- Then you can go to the directory and build the necessary documentation packages

```shell script
cd "${AIRFLOW_REPO_ROOT}"
breeze build-docs --clean-build --for-production --package-filter apache-airflow-providers \
   --package-filter 'apache-airflow-providers-*'
```

Usually when we release packages we also build documentation for the "documentation-only" packages. This
means that unless we release just few selected packages or if we need to deliberately skip some packages
we should release documentation for all provider packages and the above command is the one to use.

If we want to just release some providers you can release them in this way:

```shell script
cd "${AIRFLOW_REPO_ROOT}"
breeze build-docs --clean-build --for-production \
  --package-filter apache-airflow-providers \
  --package-filter 'apache-airflow-providers-PACKAGE1' \
  --package-filter 'apache-airflow-providers-PACKAGE2' \
  ...
```


If you have providers as list of provider ids because you just released them, you can build them with

```shell script
./dev/provider_packages/build_provider_documentation.sh amazon apache.beam google ....
```


- Now you can preview the documentation.

```shell script
./docs/start_doc_server.sh
```

You should navigate the providers and make sure the docs render properly.
Note: if you used ``--for-production`` then default of url paths goes to ``latest``
thus viewing the pages will result in 404 file not found error.
You will need to change it manually to see the docs

- Copy the documentation to the ``airflow-site`` repository

**NOTE** In order to run the publish documentation you need to activate virtualenv where you installed
apache-airflow with doc extra:

* `pip install 'apache-airflow[doc_gen]'`

If you don't have virtual env set you can do:

```shell script
cd <path_you_want_to_save_your_virtual_env>
virtualenv providers

source venv/providers/bin/activate

pip install 'apache-airflow[doc_gen]'
```

All providers (including overriding documentation for doc-only changes):

```shell script
cd "${AIRFLOW_REPO_ROOT}"

./docs/publish_docs.py \
    --package-filter apache-airflow-providers \
    --package-filter 'apache-airflow-providers-*' \
    --override-versioned

cd "${AIRFLOW_SITE_DIRECTORY}"
```

If you see `ModuleNotFoundError: No module named 'docs'`, set:

```
export PYTHONPATH=.:${PYTHONPATH}
```

If you have providers as list of provider ids because you just released them you can build them with

```shell script
cd "${AIRFLOW_REPO_ROOT}"

./dev/provider_packages/publish_provider_documentation.sh amazon apache.beam google ....
```


- If you publish a new package, you must add it to
  [the docs index](https://github.com/apache/airflow-site/blob/master/landing-pages/site/content/en/docs/_index.md):

- Create the commit and push changes.

```shell script
branch="add-documentation-$(date "+%Y-%m-%d%n")"
git checkout -b "${branch}"
git add .
git commit -m "Add documentation for packages - $(date "+%Y-%m-%d%n")"
git push --set-upstream origin "${branch}"
```

## Prepare issue in GitHub to keep status of testing

Create a GitHub issue with the content generated via manual
execution of the script below. You will use link to that issue in the next step. You need a GITHUB_TOKEN
set as your environment variable.

You can also pass the token as `--github-token` option in the script.

```shell script
breeze release-management generate-issue-content-providers --only-available-in-dist
```

You can also generate the token by following
[this link](https://github.com/settings/tokens/new?description=Read%20sssues&scopes=repo:status)

If you are preparing release for RC2/RC3 candidates, you should add `--suffix` parameter:

```shell script
breeze release-management generate-issue-content-providers --only-available-in-dist --suffix rc2
```


## Prepare voting email for Providers release candidate

Make sure the packages are in https://dist.apache.org/repos/dist/dev/airflow/providers/

Send out a vote to the dev@airflow.apache.org mailing list. Here you can prepare text of the
email.

subject:


```shell script
cat <<EOF
[VOTE] Airflow Providers prepared on $(date "+%B %d, %Y")
EOF
```

```shell script
cat <<EOF
Hey all,

I have just cut the new wave Airflow Providers packages. This email is calling a vote on the release,
which will last for 72 hours - which means that it will end on $(date -d '+3 days').

Consider this my (binding) +1.

<ADD ANY HIGH-LEVEL DESCRIPTION OF THE CHANGES HERE!>

Airflow Providers are available at:
https://dist.apache.org/repos/dist/dev/airflow/providers/

*apache-airflow-providers-<PROVIDER>-*.tar.gz* are the binary
 Python "sdist" release - they are also official "sources" for the provider packages.

*apache_airflow_providers_<PROVIDER>-*.whl are the binary
 Python "wheel" release.

The test procedure for PMC members who would like to test the RC candidates are described in
https://github.com/apache/airflow/blob/main/dev/README_RELEASE_PROVIDER_PACKAGES.md#verify-the-release-by-pmc-members

and for Contributors:

https://github.com/apache/airflow/blob/main/dev/README_RELEASE_PROVIDER_PACKAGES.md#verify-by-contributors


Public keys are available at:
https://dist.apache.org/repos/dist/release/airflow/KEYS

Please vote accordingly:

[ ] +1 approve
[ ] +0 no opinion
[ ] -1 disapprove with the reason


Only votes from PMC members are binding, but members of the community are
encouraged to test the release and vote with "(non-binding)".

Please note that the version number excludes the 'rcX' string.
This will allow us to rename the artifact without modifying
the artifact checksums when we actually release.

The status of testing the providers by the community is kept here:
<TODO COPY LINK TO THE ISSUE CREATED>

You can find packages as well as detailed changelog following the below links:

<PASTE TWINE UPLOAD LINKS HERE. SORT THEM BEFORE!>

Cheers,
<TODO: Your Name>

EOF
```

Due to the nature of packages, not all packages have to be released as convenience
packages in the final release. During the voting process
the voting PMCs might decide to exclude certain packages from the release if some critical
problems have been found in some packages.

Please modify the message above accordingly to clearly exclude those packages.

Note, For RC2/3 you may refer to shorten vote period as agreed in mailing list [thread](https://lists.apache.org/thread/cv194w1fqqykrhswhmm54zy9gnnv6kgm).

## Verify the release by PMC members

### SVN check

The files should be present in
[Airflow dist](https://dist.apache.org/repos/dist/dev/airflow/providers/)

The following files should be present (6 files):

* .tar.gz + .asc + .sha512 (one set of files per provider)
* -py3-none-any.whl + .asc + .sha512 (one set of files per provider)

As a PMC you should be able to clone the SVN repository:

```shell script
svn co https://dist.apache.org/repos/dist/dev/airflow/
```

Or update it if you already checked it out:

```shell script
svn update .
```

Optionally you can use the [`check_files.py`](https://github.com/apache/airflow/blob/main/dev/check_files.py)
script to verify that all expected files are present in SVN. This script will produce a `Dockerfile.pmc` which
may help with verifying installation of the packages.

```shell script
# Copy the list of packages (pypi urls) into `packages.txt` then run:
python check_files.py providers -p {PATH_TO_SVN}
```

After the above script completes you can build `Dockerfile.pmc` to trigger an installation of each provider
package and verify the correct versions are installed:

```shell script
docker build -f Dockerfile.pmc --tag local/airflow .
docker run --rm --entrypoint "airflow" local/airflow info
docker image rm local/airflow
```

### Licences check

This can be done with the Apache RAT tool.

* Download the latest jar from https://creadur.apache.org/rat/download_rat.cgi (unpack the binary,
  the jar is inside)
* Unpack the release source archive (the `<package + version>.tar.gz` file) to a folder
* Enter the sources folder run the check

```shell script
java -jar ../../apache-rat-0.13/apache-rat-0.13.jar -E .rat-excludes -d .
```

where `.rat-excludes` is the file in the root of Airflow source code.

### Signature check

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
Checking apache-airflow-providers-google-1.0.0rc1.tar.gz.sha512
Checking apache_airflow-providers-google-1.0.0rc1-py3-none-any.whl.sha512
```

## Verify by Contributors

This can be done (and we encourage to) by any of the Contributors. In fact, it's best if the
actual users of Apache Airflow test it in their own staging/test installations. Each release candidate
is available on PyPI apart from SVN packages, so everyone should be able to install
the release candidate version.

You can use any of the installation methods you prefer (you can even install it via the binary wheels
downloaded from the SVN).

### Installing in your local virtualenv

You have to make sure you have Airflow 2* installed in your PIP virtualenv
(the version you want to install providers with).

```shell
pip install apache-airflow-providers-<provider>==<VERSION>rc<X>
```

### Installing with Breeze

```shell
breeze start-airflow --use-airflow-version 2.2.4 --python 3.8 --backend postgres \
    --load-example-dags --load-default-connections
```

After you are in Breeze:

```shell
pip install apache-airflow-providers-<provider>==<VERSION>rc<X>
```

NOTE! You should `Ctrl-C` and restart the connections to restart airflow components and make sure new
provider packages is used.

### Building your own docker image

If you prefer to build your own image, you can also use the official image and PyPI packages to test
provider packages. This is especially helpful when you want to test integrations, but you need to install
additional tools. Below is an example Dockerfile, which installs providers for Google/

```dockerfile
FROM apache/airflow:2.2.3

RUN pip install  --user apache-airflow-providers-google==2.2.2.rc1

USER ${AIRFLOW_UID}
```

To build an image build and run a shell, run:

```shell script
docker build . --tag my-image:0.0.1
docker run  -ti \
    --rm \
    -v "$PWD/data:/opt/airflow/" \
    -v "$PWD/keys/:/keys/" \
    -p 8080:8080 \
    -e AIRFLOW__CORE__LOAD_EXAMPLES=True \
    my-image:0.0.1 bash
```

### Additional Verification

Once you install and run Airflow, you can perform any verification you see as necessary to check
that the Airflow works as you expected.


# Publish release

## Summarize the voting for the Apache Airflow release

Once the vote has been passed, you will need to send a result vote to dev@airflow.apache.org:

Subject:

```
[RESULT][VOTE] Airflow Providers - release of DATE OF RELEASE
```

Message:

```
Hello,

Apache Airflow Providers (based on RC1) have been accepted.

3 "+1" binding votes received:
- Jarek Potiuk  (binding)
- Kaxil Naik (binding)
- Tomasz Urbaszek (binding)


Vote thread:
https://lists.apache.org/thread.html/736404ca3d2b2143b296d0910630b9bd0f8b56a0c54e3a05f4c8b5fe@%3Cdev.airflow.apache.org%3E

I'll continue with the release process, and the release announcement will follow shortly.

Cheers,
<your name>
```



## Publish release to SVN

The best way of doing this is to svn cp  between the two repos (this avoids having to upload the binaries
again, and gives a clearer history in the svn commit logs.

We also need to archive older releases before copying the new ones
[Release policy](http://www.apache.org/legal/release-policy.html#when-to-archive)

```bash
cd "<ROOT_OF_YOUR_AIRFLOW_REPO>"
# Set AIRFLOW_REPO_ROOT to the path of your git repo
export AIRFLOW_REPO_ROOT="$(pwd)"

# Go the folder where you have checked out the release repo from SVN
# Make sure this is direct directory and a symbolic link
# Otherwise 'svn mv' errors out if it is with "E200033: Another process is blocking the working copy database
cd "<ROOT_WHERE_YOUR_ASF_DIST_IS_CREATED>"

export ASF_DIST_PARENT="$(pwd)"

# or clone it if it's not done yet
[ -d asf-dist ] || svn checkout --depth=immediates https://dist.apache.org/repos/dist asf-dist
# Update to latest version
svn update --set-depth=infinity asf-dist/dev/airflow asf-dist/release/airflow

SOURCE_DIR="${ASF_DIST_PARENT}/asf-dist/dev/airflow/providers"

# If some packages have been excluded, remove them now
# Check the packages are there (replace <provider> with the name of the provider that you remove)
ls ${SOURCE_DIR}/*<provider>*
# Remove them
svn rm ${SOURCE_DIR}/*<provider>*

# Create providers folder if it does not exist
# All latest releases are kept in this one folder without version sub-folder
cd asf-dist/release/airflow
mkdir -pv providers
cd providers

# Copy your providers with the target name to dist directory and to SVN
rm -rf "${AIRFLOW_REPO_ROOT}"/dist/*

for file in "${SOURCE_DIR}"/*
do
 base_file=$(basename ${file})
 cp -v "${file}" "${AIRFLOW_REPO_ROOT}/dist/${base_file//rc[0-9]/}"
 svn mv "${file}" "${base_file//rc[0-9]/}"
done

# Check which old packages will be removed (you need Python 3.8+ and dev/requirements.txt installed)
python ${AIRFLOW_REPO_ROOT}/dev/provider_packages/remove_old_releases.py --directory .

# Remove those packages
python ${AIRFLOW_REPO_ROOT}/dev/provider_packages/remove_old_releases.py --directory . --execute

# You need to do go to the asf-dist directory in order to commit both dev and release together
cd ${ASF_DIST_PARENT}/asf-dist
# Commit to SVN
svn commit -m "Release Airflow Providers on $(date "+%Y-%m-%d%n")"
```

Verify that the packages appear in
[providers](https://dist.apache.org/repos/dist/release/airflow/providers)

You are expected to see all latest versions of providers.
The ones you are about to release (with new version) and the ones that are not part of the current release.

Troubleshoot:
In case that while viewing the packages in dist/release you see that a provider has files from current version and release version it probably means that you wanted to exclude the new version of provider from release but didn't remove all providers files as expected in previous step.
Since you already commit to SVN you need to recover files from previous version with svn copy (svn merge will not work since you don't have copy of the file locally)
for example:

```
svn copy https://dist.apache.org/repos/dist/release/airflow/providers/apache_airflow_providers_docker-3.4.0-py3-none-any.whl@59404
https://dist.apache.org/repos/dist/release/airflow/providers/apache_airflow_providers_docker-3.4.0-py3-none-any.whl
```

Where `59404` is the revision we want to copy the file from. Then you can commit again.
You can also add  `-m "undeleted file"` to the `svn copy` to commit in 1 step.

Then remove from svn the files of the new provider version that you wanted to exclude from release.
If you had this issue you will need also to make adjustments in the next step to remove the provider from listed in twine check.
This is simply by removing the relevant files locally.


## Publish the packages to PyPI

By that time the packages should be in your dist folder.

```shell script
cd ${AIRFLOW_REPO_ROOT}
git checkout <ONE_OF_THE_RC_TAGS_FOR_ONE_OF_THE_RELEASED_PROVIDERS>
```

example `git checkout providers-amazon/7.0.0rc2`

Note you probably will see message `You are in 'detached HEAD' state.`
This is expected, the RC tag is most likely behind the main branch.

* Verify the artifacts that would be uploaded:

```shell script
twine check ${AIRFLOW_REPO_ROOT}/dist/*.whl ${AIRFLOW_REPO_ROOT}/dist/*.tar.gz
```

* Upload the package to PyPi's test environment:

```shell script
twine upload -r pypitest ${AIRFLOW_REPO_ROOT}/dist/*.whl ${AIRFLOW_REPO_ROOT}/dist/*.tar.gz
```

* Verify that the test packages look good by downloading it and installing them into a virtual environment.
  Twine prints the package links as output - separately for each package.

* Upload the package to PyPi's production environment:

```shell script
twine upload -r pypi ${AIRFLOW_REPO_ROOT}/dist/*.whl ${AIRFLOW_REPO_ROOT}/dist/*.tar.gz
```

Copy links to updated packages, sort it aphabeticly and save it on the side. You will need it for the announcement message.

* Again, confirm that the packages are available under the links printed.

## Publish documentation prepared before

Merge the PR that you prepared before with the documentation.

If you decided to remove some packages from the release make sure to do amend the commit in this way:

* find the packages you removed in `docs-archive/apache-airflow-providers-<PROVIDER>`
* remove the latest version (the one you were releasing)
* update `stable.txt` to the previous version
* in the (unlikely) event you are removing first version of package:
   * remove whole `docs-archive/apache-airflow-providers-<PROVIDER>` folder
   * remove package from `docs-archive/apache-airflow-providers/core-extensions/index.html` (2 places)
   * remove package from `docs-archive/apache-airflow-providers/core-extensions/connections.html` (2 places)
   * remove package from `docs-archive/apache-airflow-providers/core-extensions/extra-links.html` (2 places)
   * remove package from `docs-archive/apache-airflow-providers/core-extensions/packages-ref.html` (5 places)

## Add tags in git

Assume that your remote for apache repository is called `apache` you should now
set tags for the providers in the repo.

```shell script
./dev/provider_packages/tag_providers.sh
```

## Notify developers of release

Notify users@airflow.apache.org (cc'ing dev@airflow.apache.org) that
the artifacts have been published.

Subject:

[ANNOUNCE] Apache Airflow Providers prepared on <DATE OF CUT RC> are released

Body:

```shell script
cat <<EOF
Dear Airflow community,

I'm happy to announce that new versions of Airflow Providers packages were just released.

TODO: If there is just a few packages to release - paste the links to PyPI packages. Otherwise delete this TODO (too many links make the message unclear).

The source release, as well as the binary releases, are available here:

https://airflow.apache.org/docs/apache-airflow-providers/installing-from-sources

You can install the providers via PyPI: https://airflow.apache.org/docs/apache-airflow-providers/installing-from-pypi

The documentation is available at https://airflow.apache.org/docs/ and linked from the PyPI packages.

Cheers,
<your name>
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

------------------------------------------------------------------------------------------------------------
Announcement is done from official Apache-Airflow accounts.

* Twitter: https://twitter.com/ApacheAirflow
* Linkedin: https://www.linkedin.com/company/apache-airflow/

If you don't have access to the account ask PMC to post.

------------------------------------------------------------------------------------------------------------

Normally we do not announce on providers in social media other than a new provider added which doesn't happen often.
If you believe there is a reason to announce in social media for another case consult with PMCs about it.
Example for special case: an exciting new capability that the community waited for and should have big impact.

## Add release data to Apache Committee Report Helper

Add the release data (version and date) at: https://reporter.apache.org/addrelease.html?airflow

## Close the testing status issue

Don't forget to thank the folks who tested and close the issue tracking the testing status.

```shell script
Thank you everyone.
Providers are released
I invite everyone to help improve providers for the next release, a list of open issues can be found [here](https://github.com/apache/airflow/issues?q=is%3Aopen+is%3Aissue+label%3Aarea%3Aproviders).
```
