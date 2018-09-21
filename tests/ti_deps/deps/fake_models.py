# -*- coding: utf-8 -*-
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


class FakeTI(object):

    def __init__(self, **kwds):
        self.__dict__.update(kwds)

    def pool_full(self):
        # Allow users of this fake to set pool_filled in the contructor to make this
        # return True
        try:
            return self.pool_filled
        except AttributeError:
            # If pool_filled was not set default to false
            return False

    def get_dagrun(self, _):
        return self.dagrun

    def are_dependents_done(self, session):
        return self.dependents_done


class FakeTask(object):

    def __init__(self, **kwds):
        self.__dict__.update(kwds)


class FakeDag(object):

    def __init__(self, **kwds):
        self.__dict__.update(kwds)

    def get_running_dagruns(self, _):
        return self.running_dagruns


class FakeContext(object):

    def __init__(self, **kwds):
        self.__dict__.update(kwds)
