
<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# The `_shared` package — Agent Instructions

Each shared library is a symbolic link to the library package sources from the shared library
located in the [shared folder](../../../../shared). In the shared folder each library is a separate
distribution that has it's own tests and dependencies. Those dependencies and links to those
libraries are maintained by `prek` hook automatically.

When you modify any of the files in place here - because those are symbolic links - they are
modified in the corresponding shared library, so any modification here should result in modifying
and running tests in the shared library the folder points to.
