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

import os
from copy import copy
from pathlib import Path
from typing import TypeVar, Union

import pytest

T = TypeVar("T", bound=Union[list, tuple, dict])


class Appendable:
    """
    To be used in flat lists, tuples and dictionaries. Intended to
    be used with pytest.mark.parametrize. For example, when values
    want to be relative to `tmp_path`.
    """

    def __init__(self, path: str, *, should_touch: bool = True):
        self.path = path
        self.should_touch = should_touch

    @classmethod
    def onto(cls, appendables: T, base_path: Path) -> T:
        """
        Returns `appendables` where `Appendable` elements are appended
        onto `base_path`. The return is not a full copy, and it is not
        recursive.
        """
        if isinstance(appendables, list):
            return cls._onto_list(appendables, base_path)
        elif isinstance(appendables, tuple):
            return cls._onto_tuple(appendables, base_path)
        elif isinstance(appendables, dict):
            return cls._onto_dict(appendables, base_path)

    def _onto_single(self, base_path: Path) -> str:
        new_path = base_path / self.path
        if self.should_touch:
            new_path.touch()
        return str(new_path)

    @classmethod
    def _onto_list(cls, appendables: list, base_path: Path) -> list:
        appendables = copy(appendables)
        for index, potentially_appendable in enumerate(appendables):
            if isinstance(potentially_appendable, cls):
                new_path = potentially_appendable._onto_single(base_path)
                appendables[index] = str(new_path)
        return appendables

    @classmethod
    def _onto_tuple(cls, appendables: tuple, base_path: Path) -> tuple:
        return tuple(cls._onto_list(list(appendables), base_path))

    @classmethod
    def _onto_dict(cls, appendables: dict, base_path: Path) -> dict:
        appended = {}
        for key, value in appendables.items():
            if isinstance(key, cls):
                key = key._onto_single(base_path)
            if isinstance(value, cls):
                value = value._onto_single(base_path)
            appended[key] = value
        return appended


def test_onto(tmp_path):
    A = Appendable
    onto = A.onto

    def t(filename):
        return str(tmp_path / filename)

    assert onto([A("a")], tmp_path) == [t("a")]
    assert onto(["--path", A("a")], tmp_path) == ["--path", t("a")]

    assert onto((A("a"), A("b")), tmp_path) == (t("a"), t("b"))

    assert onto({"path": A("a")}, tmp_path) == {"path": t("a")}
    assert onto({"something": "else", "path": A("a")}, tmp_path) == {"something": "else", "path": t("a")}


@pytest.mark.parametrize(
    "appendables, should_exist",
    [
        ([Appendable("a")], True),
        ([Appendable("a", should_touch=False)], False),
    ],
)
def test_onto_should_touch(appendables, should_exist, tmp_path):
    [path] = Appendable.onto(appendables, tmp_path)
    assert os.path.isfile(path) == should_exist
