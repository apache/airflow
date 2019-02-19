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
#
from argparse import Namespace
from mock import Mock


def empty_list_handler():
    return []


def dict_handler():
    return dict(foo='bar_dict')


def obj_handler():
    # use Namespace instead of Mock, as calling hasattr() on Mock
    # will return true, even if the attr is not specified
    return Namespace(foo='bar_obj')


def to_json_handler():
    return Mock(
        to_json=lambda: {'foo': 'bar_json'}
    )


def list_dict_handler():
    return [
        dict_handler()
    ]


def to_json_list_handler():
    return [
        to_json_handler()
    ]
