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

# A collection of fake models used for unit testing


class FakeTI:

    def __init__(self, **kwds):
        self.__dict__.update(kwds)

    def get_dagrun(self, _):
        return self.dagrun  # pylint: disable=no-member

    def are_dependents_done(self, session):  # pylint: disable=unused-argument
        return self.dependents_done  # pylint: disable=no-member


class FakeTask:

    def __init__(self, **kwds):
        self.__dict__.update(kwds)


class FakeDag:

    def __init__(self, **kwds):
        self.__dict__.update(kwds)

    def get_running_dagruns(self, _):
        return self.running_dagruns  # pylint: disable=no-member


class FakeContext:

    def __init__(self, **kwds):
        self.__dict__.update(kwds)
