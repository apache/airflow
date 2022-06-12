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
import logging


class TimezoneAware(logging.Formatter):
    """
    Override default_time_format and default_msec_format to specify utc offset.

    With this Formatter, %(asctime)s will be formatted with utc offset(e.g [2022-06-12 13:00:00+0000 123ms] ).
    moments.js object in www/static/js/datetime_utils.js require this utc offset to format timezone properly.
    """

    default_time_format = '%Y-%m-%d %H:%M:%S%z'
    default_msec_format = '%s %03dms'
