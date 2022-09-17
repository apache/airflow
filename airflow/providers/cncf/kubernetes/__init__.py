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
from __future__ import annotations

import sys

if sys.version_info < (3, 7):
    # This is needed because the Python Kubernetes client >= 12.0 contains a logging object, meaning that
    # v1.Pod et al. are not pickleable on Python 3.6.

    # Python 3.7 added this via https://bugs.python.org/issue30520 in 2017 -- but Python 3.6 doesn't have this
    # method.

    # This is duplicated/backported from airflow.logging_config in 2.2, but by having it here as well it means
    # that we can update the version used in this provider and have it work for older versions
    import copyreg
    import logging

    def _reduce_Logger(logger):
        if logging.getLogger(logger.name) is not logger:
            import pickle

            raise pickle.PicklingError('logger cannot be pickled')
        return logging.getLogger, (logger.name,)

    def _reduce_RootLogger(logger):
        return logging.getLogger, ()

    if logging.Logger not in copyreg.dispatch_table:
        copyreg.pickle(logging.Logger, _reduce_Logger)
        copyreg.pickle(logging.RootLogger, _reduce_RootLogger)
