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

from airflow.exceptions import AirflowOptionalProviderFeatureException

try:
    from airflow.www.security_manager import AirflowSecurityManagerV2
except ImportError:
    raise AirflowOptionalProviderFeatureException(
        "Failed to import AirflowSecurityManagerV2. This feature is only available in Airflow versions >= 2.8.0"
    )


class AwsSecurityManagerOverride(AirflowSecurityManagerV2):
    """
    The security manager override specific to AWS auth manager.

    This class is only used in Airflow 2. This can be safely be removed when min Airflow version >= 3
    """

    def register_views(self):
        """Register views specific to AWS auth manager."""
        from airflow.providers.amazon.aws.auth_manager.views.auth import (
            AwsAuthManagerAuthenticationViews,
        )

        self.appbuilder.add_view_no_menu(AwsAuthManagerAuthenticationViews())
