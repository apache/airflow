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

from typing import List, Optional, Union

from flask import Markup, flash


class FlashMessage:
    """
    Helper for Flask flash messages

    :param message: The message to flash, either a string or Markup
    :type message: Union[str,Markup]
    :param category: The category of the message, one of "info", "warning", "error", or any custom category.
        Defaults to "info".
    :type category: str
    :param roles: List of roles that should be shown the message. If ``None``, show to all users.
    :type roles: Optional[List[str]]
    :param html: Whether the message has safe html markup in it. Defaults to False.
    :type html: bool


    For example, so show a message to all users:

    .. code-block:: python
        fm = FlashMessage("Welcome to Airflow")
        fm.flash()

    Or only users to with the User role:

    .. code-block:: python
        fm = FlashMessage("Airflow update happening next week", roles=["User"])
        if fm.should_show(securitymanager):
            fm.flash()

    You can also pass html in the message:

    .. code-block:: python
        fm = FlashMessage(
            'Visit <a href="http://airflow.apache.org">airflow.apache.org</a>', html=True
        )
        fm.flash()

        # or safely escape part of the message
        # (more details: https://markupsafe.palletsprojects.com/en/2.0.x/formatting/)
        fm = FlashMessage(Markup("Welcome <em>%s</em>") % ("John Doe",))
        fm.flash()
    """

    def __init__(
        self,
        message: Union[str, Markup],
        category: str = "info",
        roles: Optional[List[str]] = None,
        html: bool = False,
    ):
        self.category = category
        self.roles = roles
        self.html = html
        self.message = Markup(message) if html else message

    def should_show(self, securitymanager) -> bool:
        """Determine if the user should see the message based on their role membership"""
        if self.roles:
            user_roles = {r.name for r in securitymanager.get_user_roles()}
            if not user_roles.intersection(set(self.roles)):
                return False
        return True

    def flash(self) -> None:
        flash(self.message, self.category)
