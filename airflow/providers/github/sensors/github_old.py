# #
# # Licensed to the Apache Software Foundation (ASF) under one
# # or more contributor license agreements.  See the NOTICE file
# # distributed with this work for additional information
# # regarding copyright ownership.  The ASF licenses this file
# # to you under the Apache License, Version 2.0 (the
# # "License"); you may not use this file except in compliance
# # with the License.  You may obtain a copy of the License at
# #
# #   http://www.apache.org/licenses/LICENSE-2.0
# #
# # Unless required by applicable law or agreed to in writing,
# # software distributed under the License is distributed on an
# # "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# # KIND, either express or implied.  See the License for the
# # specific language governing permissions and limitations
# # under the License.
# from typing import Any, Callable, Dict, Optional
#
# from github import Github as GithubClient
#
# from airflow.providers.github.hooks.github import GithubHook
# from airflow.sensors.base import BaseSensorOperator
#
#
# class GithubTagSensor (BaseSensorOperator):
#     """
#     Monitors a creation of Github tag.
#
#     :param github_conn_id: reference to a pre-defined Github Connection
#     :type github_conn_id: str
#     """
#
#     def __init__(
#         self,
#         *,
#         repository: str,
#         tag_name: str,
#         github_conn_id: str = 'github_default',
#         **kwargs,
#     ) -> None:
#         super().__init__(**kwargs)
#         self.github_conn_id = github_conn_id
#         self.repository = repository
#         self.tag_name = tag_name
#         self.hook = GithubHook(github_conn_id=github_conn_id, **kwargs)
#
#     def poke(self, context: Dict[Any, Any]) -> bool:
#
#         self.log.info('Poking for tag: %s in repository: %s', self.tag_name, self.repository)
#         try:
#             response = self.hook.run(
#                 self.endpoint,
#                 data=self.request_params,
#                 headers=self.headers,
#                 extra_options=self.extra_options,
#             )
#             if self.response_check:
#                 kwargs = determine_kwargs(self.response_check, [response], context)
#                 return self.response_check(response, **kwargs)
#         except AirflowException as exc:
#             if str(exc).startswith("404"):
#                 return False
#
#             raise exc
#
#         return True
#
