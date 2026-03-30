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

from airflow._shared.module_loading import qualname


def test_builtin_timetable_type_name_returns_class_name():
    """Built-in timetables should return just the class name as type_name."""
    from airflow.timetables.simple import NullTimetable

    tt = NullTimetable()
    assert tt.type_name == "NullTimetable"


def test_custom_timetable_type_name_returns_qualname():
    """Custom/user-defined timetables should return the full import path (qualname)."""
    # Define a custom timetable class that inherits from Timetable so it uses
    # the default property implementation. Its module will not start with
    # "airflow.timetables." so type_name should be the qualname.
    from airflow.timetables.base import Timetable

    class CustomTimetable(Timetable):
        pass

    inst = CustomTimetable()
    expected = qualname(inst.__class__)

    # The Timetable.type_name logic should return qualname for custom timetables
    assert inst.type_name == expected
