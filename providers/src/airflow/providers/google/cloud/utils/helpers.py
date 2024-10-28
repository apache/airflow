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
"""This module contains helper functions for Google Cloud operators."""

from __future__ import annotations


def normalize_directory_path(source_object: str | None) -> str | None:
    """Make sure dir path ends with a slash."""
    return (
        source_object + "/"
        if source_object and not source_object.endswith("/")
        else source_object
    )


def resource_path_to_dict(resource_name: str) -> dict[str, str]:
    """
    Convert a path-like GCP resource name into a dictionary.

    For example, the path `projects/my-project/locations/my-location/instances/my-instance` will be converted
    to a dict:
    `{"projects": "my-project",
    "locations": "my-location",
    "instances": "my-instance",}`
    """
    if not resource_name:
        return {}
    path_items = resource_name.split("/")
    if len(path_items) % 2:
        raise ValueError(
            "Invalid resource_name. Expected the path-like name consisting of key/value pairs "
            "'key1/value1/key2/value2/...', for example 'projects/<project>/locations/<location>'."
        )
    iterator = iter(path_items)
    return dict(zip(iterator, iterator))
