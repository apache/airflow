# Vendor package

## What vendored packages are for

The `airflow._vendor` package is foreseen for vendoring in packages, that we have to modify ourselves
because authors of the packages do not have time to modify them themselves. This is often temporary
and once the packages implement fixes that we need, and then we remove the packages from
the `_vendor` package.

All Vendored libraries must follow these rules:

1. Vendored libraries must be pure Python--no compiling (so that we do not have to release multi-platform airflow packages on PyPI).
2. Source code is managed by the [`vendoring`][vendoring] tool
3. Requirements of the library become requirements of airflow core.
4. Any modifictions to the libary must be maintained as patch files applied via `vendoring`
5. The `_vendor` packages should be excluded from any refactoring, static checks and automated fixes.

[vendoring]: https://github.com/pradyunsg/vendoring
