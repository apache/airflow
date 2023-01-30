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

============================================
Airflow's release process and version policy
============================================

Since Airflow 2.0.0 and provider packages 1.0.0 we aim to follow SemVer, meaning the release numbering works as follows:

- Versions are numbered in the form X.Y.Z.
- X is the major version number.
- Y is the minor version number, also called the *feature release* version number.
- Z is the patch number, which is incremented for bugfix and security releases.
  Before every new release, we'll make a release candidate available, and often alpha or beta release too.
  These are of the form X.Y.Z alpha/beta/rc N, which means the Nth alpha/beta/release candidate of version X.Y.Z

In git, each minor version will have its own branch, called ``vX-Y-stable`` where bugfix/security releases will be issued from.
Commits and PRs should not normally go direct to these branches, but instead should target the main branch and then be cherry-picked by the release managers to these release branches.

Each Airflow release will also have a tag in git indicating its version number, signed with the release manager's key.
Tags for the main Airflow release have the form ``X.Y.Z`` (no leading ``v``) and provider packages are tagged with the form ``providers-<name>/X.Y.Z``.

Although Airflow follows SemVer this is not a promise of 100% compatibility between minor or patch releases, simply because this is not possible: what is a bug to one person might be a feature another person is depending on.

  Knowing the *intentions* of a maintainer can be valuable – especially *when* things break. Because that's all SemVer is: **a TL;DR of the changelog**.

  -- Hynek Schlawack https://hynek.me/articles/semver-will-not-save-you/

That is all SemVer is -- it's a statement of our intent as package authors, and a clear statement of our goals.

.. glossary::

    Major release
      Major releases (X.0.0, X+1.0.0 etc.) indicate a backwards-incompatible change.

      These releases do not happen with any regular interval or on any predictable schedule.

      Each time a new major version is released previously deprecated features will be removed.

    Feature releases
      Feature releases (X.Y.0, X.Y+1.0, etc.) will happen roughly every two or three months – see release process for details.
      These releases will contain new features, improvements to existing features, and such.

    Patch releases
      Patch releases (X.Y.Z, X.Y.Z+1, etc.) will happen, on an as-needed basis, as issues are reported and fixed.

      These releases will be 100% compatible with the associated feature release.
      So the answer to "should I upgrade to the latest patch release?" will always be "yes."

      The only exception to the above with respect to 100% backwards compatibility is when a security or data loss issue can't be fixed without breaking backwards-compatibility.
      If this happens, the release notes will provide detailed upgrade instructions.
      **No new features will be added in patch releases**


Deprecation policy
==================

From time-to-time existing features will be deprecated, or modules will be renamed.

When this happens, the existing code will continue to work but will issue a DeprecationWarning (or a subclass) when the code is executed.
This code will continue to work for the rest of the current major version -- if it works on 2.0.0, it will work for every 2.Y.Z release.

So, for example, if we decided to start the deprecation of a function in Airflow 2.2.4:

* Airflow 2.2 will contain a backwards-compatible replica of the function which will raise a DeprecationWarning
* Airflow 2.3 will continue to work and issue a warning
* Airflow 3.0 (the major version that follows 2.2) will remove the feature outright

The exception to this deprecation policy is any feature which is marked as "experimental", which *may* suffer breaking changes or complete removal in a Feature release.

.. _experimental:

Experimental features
=====================

From time-to-time a new feature will be added to Airflow that will be marked as experimental.

Experimental features do not have any guarantees about deprecation, and *can* be changed in a breaking way between feature releases, or even removed entirely.

We always aim to maintain compatibility even for experimental features, but make no promises. By having this "get out" it lets us build new features more quickly and get them in the hand of users without having to worry about making a feature perfect.
