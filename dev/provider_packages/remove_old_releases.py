#
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
"""
Removes older releases of provider packages from the folder using svn rm.

It iterates over the folder specified as first parameter and removes all but latest releases of
packages found in that directory.

"""
import argparse
import glob
import operator
import os
import subprocess
from collections import defaultdict
from typing import Dict, List, NamedTuple

from packaging.version import Version


class VersionedFile(NamedTuple):
    base: str
    version: str
    suffix: str
    type: str
    comparable_version: Version


def split_version_and_suffix(file_name: str, suffix: str) -> VersionedFile:
    no_suffix_file = file_name[: -len(suffix)]
    no_version_file, version = no_suffix_file.rsplit("-", 1)
    return VersionedFile(
        base=no_version_file + "-",
        version=version,
        suffix=suffix,
        type=no_version_file + "-" + suffix,
        comparable_version=Version(version),
    )


def process_all_files(directory: str, suffix: str, execute: bool):
    package_types_dicts: Dict[str, List[VersionedFile]] = defaultdict(list)
    os.chdir(directory)

    for file in glob.glob("*" + suffix):
        versioned_file = split_version_and_suffix(file, suffix)
        package_types_dicts[versioned_file.type].append(versioned_file)

    for package_types in package_types_dicts.values():
        package_types.sort(key=operator.attrgetter("comparable_version"))

    for package_types in package_types_dicts.values():
        if len(package_types) == 1:
            versioned_file = package_types[0]
            print(
                "Leaving the only version: "
                f"{versioned_file.base + versioned_file.version + versioned_file.suffix}"
            )
        # Leave only last version from each type
        for versioned_file in package_types[:-1]:
            command = ["svn", "rm", versioned_file.base + versioned_file.version + versioned_file.suffix]
            if not execute:
                print(command)
            else:
                subprocess.run(command, check=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Removes old releases.')
    parser.add_argument(
        '--directory',
        dest='directory',
        action='store',
        required=True,
        help='Directory to remove old releases in',
    )
    parser.add_argument(
        '--execute', dest='execute', action='store_true', help='Execute the removal rather than dry run'
    )
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_args()
    process_all_files(args.directory, ".tar.gz", args.execute)
    process_all_files(args.directory, ".tar.gz.sha512", args.execute)
    process_all_files(args.directory, ".tar.gz.asc", args.execute)
    process_all_files(args.directory, "-py3-none-any.whl", args.execute)
    process_all_files(args.directory, "-py3-none-any.whl.sha512", args.execute)
    process_all_files(args.directory, "-py3-none-any.whl.asc", args.execute)
