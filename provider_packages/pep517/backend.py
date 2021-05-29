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
import subprocess
import sys

from setuptools import build_meta


def _run_setup(*args):
    subprocess.check_call([sys.executable, "setup.py", *args])


def _tag_build_if_needed(config_settings):
    """Tag build before building a dist."""
    if not config_settings:
        return
    tag_build = config_settings.get("airflow-tag-build")
    if not tag_build:
        return
    return _run_setup("egg_info", "--tag-build", tag_build)


def build_sdist(sdist_directory, config_settings=None):
    _tag_build_if_needed(config_settings)
    return build_meta.build_sdist(
        sdist_directory=sdist_directory,
        config_settings=config_settings,
    )


def build_wheel(wheel_directory, config_settings=None, metadata_directory=None):
    _tag_build_if_needed(config_settings)
    return build_meta.build_wheel(
        wheel_directory=wheel_directory,
        config_settings=config_settings,
        metadata_directory=metadata_directory,
    )


def prepare_metadata_for_build_wheel(metadata_directory, config_settings=None):
    _tag_build_if_needed(config_settings)
    return build_meta.prepare_metadata_for_build_wheel(
        metadata_directory=metadata_directory,
        config_settings=config_settings,
    )


get_requires_for_build_sdist = build_meta.get_requires_for_build_sdist
get_requires_for_build_wheel = build_meta.get_requires_for_build_wheel


__all__ = [
    "build_sdist",
    "build_wheel",
    "get_requires_for_build_sdist",
    "get_requires_for_build_wheel",
    "prepare_metadata_for_build_wheel",
]
