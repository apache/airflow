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
    Override `default_time_format` and `default_msec_format` to specify utc offset.

    utc offset is the matter, without it,  time conversion could be wrong. With this Formatter, `%(asctime)s`
    will be formatted containing utc offset. (e.g. 2022-06-12 13:00:00+0000 123ms)

    moments.js couldn't parse milliseconds comes after utc offset, so it would be ideal `%(asctime)s`
    formatted with millisecond comes before utc offset in th first place. (e.g 2022-06-12 13:00:00.123+0000)
    But python standard lib doesn't support format like that.

    Omitting milliseconds is possible by assigning `default_msec_format` to `None`. But this requires
    python3.9 or higher, so we can't omit milliseconds until dropping support 3.8 or under.

    Therefore, to use in moments.js, formatted `%(asctime)s` has to be re-formatted by javascript side.
    """

    default_time_format = '%Y-%m-%d %H:%M:%S%z'
    default_msec_format = '%s %03dms'
