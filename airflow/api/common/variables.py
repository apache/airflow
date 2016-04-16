# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
from __future__ import absolute_import

from airflow.settings import Session
from airflow.models import Variable


def get_var(key, default_var=None, deserialize_json=False):
    var = Variable.get(key, default_var, deserialize_json)
    return var


def set_var(key, value, serialize_json=False):
    Variable.set(key, value, serialize_json)


def list_var():
    session = Session()
    variables = session.query(Variable)
    return variables
