#!/usr/bin/env python

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import argparse
import fnmatch
import os

# pylint: disable=no-name-in-module
from docs.exts.docs_build.docs_builder import AirflowDocsBuilder
from docs.exts.docs_build.package_filter import process_package_filters
from docs.exts.provider_yaml_utils import load_package_data

# pylint: enable=no-name-in-module

AIRFLOW_SITE_DIR = os.environ.get('AIRFLOW_SITE_DIRECTORY')

if __name__ != "__main__":
    raise SystemExit(
        "This file is intended to be executed as an executable program. You cannot use it as a module."
        "To run this script, run the ./build_docs.py command"
    )

if not (
    AIRFLOW_SITE_DIR
    and os.path.isdir(AIRFLOW_SITE_DIR)
    and os.path.isdir(os.path.join(AIRFLOW_SITE_DIR, 'docs-archive'))
):
    raise SystemExit(
        'Before using this script, set the environment variable AIRFLOW_SITE_DIRECTORY. This variable '
        'should contain the path to the airflow-site repository directory. '
        '${AIRFLOW_SITE_DIRECTORY}/docs-archive must exists.'
    )

ALL_PROVIDER_YAMLS = load_package_data()


def get_available_packages():
    """Get list of all available packages to build."""
    provider_package_names = [provider['package-name'] for provider in ALL_PROVIDER_YAMLS]
    return ["apache-airflow", *provider_package_names, "apache-airflow-providers"]


def _get_parser():
    available_packages_list = " * " + "\n * ".join(get_available_packages())
    parser = argparse.ArgumentParser(
        description='Copies the built documentation to airflow-site repository.',
        epilog=f"List of supported documentation packages:\n{available_packages_list}" "",
    )
    parser.formatter_class = argparse.RawTextHelpFormatter
    parser.add_argument(
        '--disable-checks', dest='disable_checks', action='store_true', help='Disables extra checks'
    )
    parser.add_argument(
        "--package-filter",
        action="append",
        help=(
            "Filter specifying for which packages the documentation is to be built. Wildcard are supported."
        ),
    )

    return parser


def main():
    """Main code"""
    args = _get_parser().parse_args()
    available_packages = get_available_packages()

    package_filters = args.package_filter

    current_packages = process_package_filters(available_packages, package_filters)
    print(f"Publishing docs for {len(current_packages)} package(s)")
    for pkg in current_packages:
        print(f" - {pkg}")
    print()
    for package_name in current_packages:
        builder = AirflowDocsBuilder(package_name=package_name, for_production=True)
        builder.publish()


main()
