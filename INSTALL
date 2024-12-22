INSTALL / BUILD instructions for Apache Airflow

Basic installation of Airflow from sources and development environment setup
============================================================================

This is a generic installation method that requires minimum standard tools to develop Airflow and
test it in a local virtual environment (using standard CPython installation and `pip`).

Depending on your system, you might need different prerequisites, but the following
systems/prerequisites are known to work:

Linux (Debian Bookworm):

    sudo apt install -y --no-install-recommends apt-transport-https apt-utils ca-certificates \
    curl dumb-init freetds-bin krb5-user libgeos-dev \
    ldap-utils libsasl2-2 libsasl2-modules libxmlsec1 locales libffi8 libldap-2.5-0 libssl3 netcat-openbsd \
    lsb-release openssh-client python3-selinux rsync sasl2-bin sqlite3 sudo unixodbc

You might need to install MariaDB development headers to build some of the dependencies

    sudo apt-get install libmariadb-dev libmariadbclient-dev

MacOS (Mojave/Catalina) you might need to install XCode command line tools and brew and those packages:

    brew install sqlite mysql postgresql

The `pip` is one of the build packaging front-ends that might be used to install Airflow. It's the one
that we recommend (see below) for reproducible installation of specific versions of Airflow.

As of version 2.8, Airflow follows PEP 517/518 and uses `pyproject.toml` file to define build dependencies
and build process, and it requires relatively modern versions of packaging tools to get airflow built from
local sources or sdist packages, as PEP 517 compliant build hooks are used to determine dynamic build
dependencies. In the case of `pip` it means that at least version 22.1.0 is needed (released at the beginning of
2022) to build or install Airflow from sources. This does not affect the ability to install Airflow from
released wheel packages.

Downloading and installing Airflow from sources
-----------------------------------------------

While you can get Airflow sources in various ways (including cloning https://github.com/apache/airflow/), the
canonical way to download it is to fetch the tarball (published at https://downloads.apache.org), after
verifying the checksum and signatures of the downloaded file.

When you download source packages from https://downloads.apache.org, you download sources of Airflow and
all providers separately. However, when you clone the GitHub repository at https://github.com/apache/airflow/
you get all sources in one place. This is the most convenient way to develop Airflow and Providers together.
Otherwise, you have to install Airflow and Providers separately from sources in the same environment, which
is not as convenient.

Creating virtualenv
-------------------

Airflow pulls in quite a lot of dependencies to connect to other services. You generally want to
test or run Airflow from a virtualenv to ensure those dependencies are separated from your system-wide versions. Using system-installed Python installation is strongly discouraged as the versions of Python
shipped with the operating system often have some limitations and are not up to date. It is recommended to install Python using the official release (https://www.python.org/downloads/), or Python project management tools such as Hatch. See later
for a description of `Hatch` as one of the tools that is Airflow's tool of choice to build Airflow packages.

Once you have a suitable Python version installed, you can create a virtualenv and activate it:

    python3 -m venv PATH_TO_YOUR_VENV
    source PATH_TO_YOUR_VENV/bin/activate

Installing Airflow locally
--------------------------

Installing Airflow locally can be done using pip - note that this will install "development" version of
Airflow, where all providers are installed from local sources (if available), not from `pypi`.
It will also not include pre-installed providers installed from PyPI. If you install from sources of
just Airflow, you need to install separately each provider you want to develop. If you install
from the GitHub repository, all the current providers are available after installing Airflow.

    pip install .

If you develop Airflow and iterate on it, you should install it in editable mode (with -e) flag, and then
you do not need to re-install it after each change to sources. This is useful if you want to develop and
iterate on Airflow and Providers (together) if you install sources from the cloned GitHub repository.

Note that you might want to install `devel` extra when you install airflow for development in editable env
this one contains the minimum set of tools and dependencies needed to run unit tests.


    pip install -e ".[devel]"

You can also install optional packages that are needed to run certain tests. In case of local installation
for example, you can install all prerequisites for Google provider, tests, and
all Hadoop providers with this command:

    pip install -e ".[google,devel-tests,devel-hadoop]"


or you can install all packages needed to run tests for core, providers, and all extensions of airflow:

    pip install -e ".[devel-all]"

You can see the list of all available extras below.

Additionally when you want to develop providers you need to install providers code in editable mode:

    pip install -e "./providers"

Using ``uv`` to manage your Python, virtualenvs, and install airflow for development
====================================================================================

While you can manually install airflow locally from sources, Airflow committers recommend using
[uv](https://docs.astral.sh/uv/) as a build and development tool. It is a modern,
recently introduced popular packaging front-end tool and environment managers for Python.
It is an optional tool that is only really needed when you want to build packages from sources, you can use
many other packaging frontends (for example ``hatch``) but ``uv`` is very fast and convenient to manage
also your Python versions and virtualenvs. Also we use ``

Installing ``uv``
-----------------

You can install uv following [the instructions](https://docs.astral.sh/uv/getting-started/installation/ on

Using ``uv`` to manage your virtualenvs
---------------------------------------

You can create a virtualenv with ``uv`` using the following command:

    uv venv create

You can sync to latest versions of airflow dependencies using:

    uv sync

And if you want to use some extras (for example because you want to develop providers) you can add
extras to the command:

    uv sync --extra devel

You can also synchronize all extras:

    uv sync --all-extras

Building airflow packages with Hatch
====================================

While building packages will work with any compliant packaging front-end tool, for reproducibility, we
recommend using ``hatch``. It is a modern, fast, and convenient tool to build packages from sources managed
by the Python Packaging Authority. It is also used by Airflow to build packages in CI/CD as well as by
release managers to build locally packages for verification of reproducibility of the build.

Installing ``hatch``
--------------------

More information about hatch can be found in https://hatch.pypa.io/

We recommend to install ``hatch`` using ```uv tool`` command which will make hatch available as a CLI
command globally:

    uv tool install hatch

You can still install ``hatch`` using ``pipx`` if you prefer:

    pipx install hatch


It's important to keep your hatch up to date. You can do this by running:

    uv tool upgrade hatch


Using Hatch to build your packages
----------------------------------

You can use Hatch to build installable packages from the Airflow sources. Such package will
include all metadata configured in `pyproject.toml` and will be installable with ``pip`` and and any other
PEP-compliant packaging front-end.

The packages will have pre-installed dependencies for providers that are available when Airflow is i
onstalled from PyPI. Both `wheel` and `sdist` packages are built by default.

    hatch build

You can also build only `wheel` or `sdist` packages:

    hatch build -t wheel
    hatch build -t sdist

Installing recommended version of dependencies
==============================================

Whatever virtualenv solution you use, when you want to make sure you are using the same
version of dependencies as in main, you can install the recommended version of the dependencies by using
constraint-python<PYTHON_MAJOR_MINOR_VERSION>.txt files as `constraint` file. This might be useful
to avoid "works-for-me" syndrome, where you use different versions of dependencies than the ones
that are used in main CI tests and by other contributors.

There are different constraint files for different Python versions. For example, this command will install
all basic devel requirements and requirements of Google provider as last successfully tested for Python 3.9:

    pip install -e ".[devel,google]"" \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-main/constraints-3.9.txt"

Using the 'constraints-no-providers' constraint files, you can upgrade Airflow without paying attention to the provider's dependencies. This allows you to keep installed provider dependencies and install the latest supported ones using pure Airflow core.

pip install -e ".[devel]" \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-main/constraints-no-providers-3.9.txt"

Airflow extras
==============

Airflow has several extras that you can install to get additional dependencies. They sometimes install
providers, sometimes enable other features where packages are not installed by default.

You can read more about those extras in the extras reference:
https://airflow.apache.org/docs/apache-airflow/stable/extra-packages-ref.html

The list of available extras is below.

Core extras
-----------

Those extras are available as regular core airflow extras - they install optional features of Airflow.

# START CORE EXTRAS HERE

aiobotocore, apache-atlas, apache-webhdfs, async, cgroups, cloudpickle, github-enterprise, google-
auth, graphviz, kerberos, ldap, leveldb, otel, pandas, password, rabbitmq, s3fs, sentry, statsd, uv

# END CORE EXTRAS HERE

Provider extras
---------------

Those extras are available as regular Airflow extras; they install provider packages in standard builds
or dependencies that are necessary to enable the feature in an editable build.

# START PROVIDER EXTRAS HERE

airbyte, alibaba, amazon, apache.beam, apache.cassandra, apache.drill, apache.druid, apache.flink,
apache.hdfs, apache.hive, apache.iceberg, apache.impala, apache.kafka, apache.kylin, apache.livy,
apache.pig, apache.pinot, apache.spark, apprise, arangodb, asana, atlassian.jira, celery, cloudant,
cncf.kubernetes, cohere, common.compat, common.io, common.sql, databricks, datadog, dbt.cloud,
dingding, discord, docker, edge, elasticsearch, exasol, fab, facebook, ftp, github, google, grpc,
hashicorp, http, imap, influxdb, jdbc, jenkins, microsoft.azure, microsoft.mssql, microsoft.psrp,
microsoft.winrm, mongo, mysql, neo4j, odbc, openai, openfaas, openlineage, opensearch, opsgenie,
oracle, pagerduty, papermill, pgvector, pinecone, postgres, presto, qdrant, redis, salesforce,
samba, segment, sendgrid, sftp, singularity, slack, smtp, snowflake, sqlite, ssh, standard, tableau,
telegram, teradata, trino, vertica, weaviate, yandex, ydb, zendesk

# END PROVIDER EXTRAS HERE

Devel extras
------------

The `devel` extras are not available in the released packages. They are only available when you install
Airflow from sources in `editable` installation - i.e., one that you are usually using to contribute to
Airflow. They provide tools like `pytest` and `mypy` for general-purpose development and testing.

# START DEVEL EXTRAS HERE

devel, devel-all-dbs, devel-ci, devel-debuggers, devel-devscripts, devel-duckdb, devel-hadoop,
devel-mypy, devel-sentry, devel-static-checks, devel-tests

# END DEVEL EXTRAS HERE

Bundle extras
-------------

Those extras are bundles dynamically generated from other extras.

# START BUNDLE EXTRAS HERE

all, all-core, all-dbs, devel-all, devel-ci

# END BUNDLE EXTRAS HERE


Doc extras
----------

Doc extras are used to install dependencies that are needed to build documentation. Only available during
editable install.

# START DOC EXTRAS HERE

doc, doc-gen

# END DOC EXTRAS HERE

Deprecated extras
-----------------

The deprecated extras are from Airflow 1 and will be removed in future versions.

# START DEPRECATED EXTRAS HERE

atlas, aws, azure, cassandra, crypto, druid, gcp, gcp-api, hdfs, hive, kubernetes, mssql, pinot, s3,
spark, webhdfs, winrm

# END DEPRECATED EXTRAS HERE

Compiling front-end assets
--------------------------

Sometimes, you can see that front-end assets are missing, and the website looks broken. This is because
you need to compile front-end assets. This is done automatically when you create a virtualenv
with hatch, but if you want to do it manually, you can do it after installing node and yarn and running:

    yarn install --frozen-lockfile
    yarn run build

Currently, we are running yarn coming with note 18.6.0, but you should check the version in
our `.pre-commit-config.yaml` file (node version).

Installing yarn is described in https://classic.yarnpkg.com/en/docs/install

Also - in case you use `breeze` or have `pre-commit` installed, you can build the assets with the following:

    pre-commit run --hook-stage manual compile-www-assets --all-files

or

    breeze compile-www-assets

Both commands will install node and yarn, if needed, to a dedicated pre-commit node environment and
then build the assets.

Finally, you can also clean and recompile assets with `custom` build target when running the Hatch build

    hatch build -t custom -t wheel -t sdist

This will also update `git_version` file in the Airflow package that should contain the git commit hash of the
build. This is used to display the commit hash in the UI.
