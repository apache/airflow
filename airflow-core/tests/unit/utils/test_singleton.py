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


class Counter(metaclass=Singleton):
    """Singleton class that counts how much __init__ and count was called."""

    counter = 0

    def __init__(self):
        self.counter += 1

    def count(self):
        self.counter += 1


def test_singleton_refers_to_same_instance():
    a, b = A(), A()
    assert a is b


def test_singleton_after_out_of_context_does_refer_to_same_instance():
    # check if setting something on singleton is preserved after instance goes out of context
    def x():
        a = A()
        a.a = "a"

    x()
    b = A()
    assert b.a == "a"


def test_singleton_does_not_call_init_second_time():
    # first creation of Counter, check if __init__ is called
    c = Counter()
    assert c.counter == 1

    # check if "new instance" calls __init__ - it shouldn't
    d = Counter()
    assert c.counter == 1

    # check if incrementing "new instance" increments counter on previous one
    d.count()
    assert c.counter == 2
