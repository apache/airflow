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


import sys


# Imports the hooks dynamically while keeping the package API clean,
# abstracting the underlying modules

def _integrate_plugins():
    """Integrate plugins to the context"""
    from airflow.plugins_manager import hooks_modules
    for hooks_module in hooks_modules:
        sys.modules[hooks_module.__name__] = hooks_module
        globals()[hooks_module._name] = hooks_module
