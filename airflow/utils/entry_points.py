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

try:
    import importlib_metadata
except ImportError:
    from importlib import metadata as importlib_metadata  # type: ignore


def entry_points_with_dist(group: str):
    """
    Return EntryPoint objects of the given group, along with the distribution information.

    This is like the ``entry_points()`` function from importlib.metadata,
    except it also returns the distribution the entry_point was loaded from.

    :param group: Filter results to only this entrypoint group
    :return: Generator of (EntryPoint, Distribution) objects for the specified groups
    """
    for dist in importlib_metadata.distributions():
        for e in dist.entry_points:
            if e.group != group:
                continue
            yield e, dist
