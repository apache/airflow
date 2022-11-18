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


def representative_combos(list_1: list[str], list_2: list[str]) -> list[tuple[str, str]]:
    """
    Include only representative combos from the matrix of the two lists - making sure that each of the
    elements contributing is present at least once.
    :param list_1: first list
    :param list_2: second list
    :return: list of combinations with guaranteed at least one element from each of the list
    """
    all_selected_combinations: list[tuple[str, str]] = []
    for i in range(max(len(list_1), len(list_2))):
        all_selected_combinations.append((list_1[i % len(list_1)], list_2[i % len(list_2)]))
    return all_selected_combinations


def excluded_combos(list_1: list[str], list_2: list[str]) -> list[tuple[str, str]]:
    """
    Return exclusion lists of elements that should be excluded from the matrix of the two list of items
    if what's left should be representative list of combos (i.e. each item from both lists,
    has to be present at least once in the combos).
    :param list_1: first list
    :param list_2: second list
    :return: list of exclusions = list 1 x list 2 - representative_combos
    """
    all_combos: list[tuple[str, str]] = []
    for item_1 in list_1:
        for item_2 in list_2:
            all_combos.append((item_1, item_2))
    return [item for item in all_combos if item not in set(representative_combos(list_1, list_2))]
