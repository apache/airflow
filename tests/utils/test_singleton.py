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
from __future__ import annotations

from airflow.utils.singleton import Singleton


class A(metaclass=Singleton):
    pass


def test_singleton_refers_to_same_instance():
    a, b = A(), A()
    assert a == b


def test_singleton_out_of_context_does_refer_to_same_instance():
    def x():
        a = A()
        a.a = "a"

    x()
    b = A()
    assert b.a == "a"
