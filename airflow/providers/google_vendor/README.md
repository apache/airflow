# Vendor package

## What vendored packages are for

The `_vendor` package is foreseen for vendoring in packages, that we have to modify ourselves
because authors of the packages do not have time to modify them themselves. This is often temporary
and once the packages implement fixes that we need, and then we remove the packages from
the `_vendor` package.

All Vendored libraries must follow these rules:

1. Vendored libraries must be pure Python--no compiling (so that we do not have to release multi-platform airflow packages on PyPI).
2. Source code for the libary is included in this directory.
3. License must be included in this repo and in the [LICENSE](../../LICENSE) file and in the
   [licenses](../../licenses) folder.
4. Requirements of the library become requirements of airflow core.
5. Version of the library should be included in the [vendor.md](vendor.md) file.
6. No modifications to the library may be made in the initial commit.
7. Apply the fixes necessary to use the vendored library as separate commits - each package separately,
   so that they can be cherry-picked later if we upgrade the vendored package. Changes to airflow code to
   use the vendored packages should be applied as separate commits/PRs.
8. The `_vendor` packages should be excluded from any refactorings, static checks and automated fixes.

## Adding and upgrading a vendored package

Way to vendor a library or update a version:

1. Update ``vendor.md`` with the library, version, and SHA256 (`pypi` provides hashes as of recently)
2. Remove all old files and directories of the old version.
3. Replace them with new files (only replace relevant python packages:move LICENSE )
   * move licence files to [licenses](../../licenses) folder
   * remove README and any other supporting files (they can be found in PyPI)
   * make sure to add requirements from setup.py to airflow's setup.py with appropriate comment stating
     why the requirements are added and when they should be removed
4. If you replace previous version, re-apply historical fixes from the "package" folder by
   cherry-picking them.

