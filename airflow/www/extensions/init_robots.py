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

import logging

log = logging.getLogger(__name__)


def init_robots(app):
    """
    Add X-Robots-Tag header. Use it to avoid search engines indexing airflow. This mitigates some
    of the risk associated with exposing Airflow to the public internet, however it does not
    address the real security risks associated with such a deployment.

    See also: https://developers.google.com/search/docs/advanced/robots/robots_meta_tag#xrobotstag
    """

    def apply_robot_tag(response):
        response.headers['X-Robots-Tag'] = 'noindex, nofollow'
        return response

    app.after_request(apply_robot_tag)
