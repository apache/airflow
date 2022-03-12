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

from airflow_breeze.global_constants import (
    FLOWER_HOST_PORT,
    MSSQL_HOST_PORT,
    MYSQL_HOST_PORT,
    POSTGRES_HOST_PORT,
    REDIS_HOST_PORT,
    SSH_PORT,
    WEBSERVER_HOST_PORT,
)
from airflow_breeze.utils.path_utils import get_airflow_sources_root

ASCIIART = """




                                  @&&&&&&@
                                 @&&&&&&&&&&&@
                                &&&&&&&&&&&&&&&&
                                        &&&&&&&&&&
                                            &&&&&&&
                                             &&&&&&&
                           @@@@@@@@@@@@@@@@   &&&&&&
                          @&&&&&&&&&&&&&&&&&&&&&&&&&&
                         &&&&&&&&&&&&&&&&&&&&&&&&&&&&
                                         &&&&&&&&&&&&
                                             &&&&&&&&&
                                           &&&&&&&&&&&&
                                      @@&&&&&&&&&&&&&&&@
                   @&&&&&&&&&&&&&&&&&&&&&&&&&&&&  &&&&&&
                  &&&&&&&&&&&&&&&&&&&&&&&&&&&&    &&&&&&
                 &&&&&&&&&&&&&&&&&&&&&&&&         &&&&&&
                                                 &&&&&&
                                               &&&&&&&
                                            @&&&&&&&&
            @&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
           &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
          &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&



     @&&&@       &&  @&&&&&&&&&&&   &&&&&&&&&&&&  &&            &&&&&&&&&&  &&&     &&&     &&&
    &&& &&&      &&  @&&       &&&  &&            &&          &&&       &&&@ &&&   &&&&&   &&&
   &&&   &&&     &&  @&&&&&&&&&&&&  &&&&&&&&&&&   &&          &&         &&&  &&& &&& &&@ &&&
  &&&&&&&&&&&    &&  @&&&&&&&&&     &&            &&          &&@        &&&   &&@&&   &&@&&
 &&&       &&&   &&  @&&     &&&@   &&            &&&&&&&&&&&  &&&&&&&&&&&&     &&&&   &&&&

&&&&&&&&&&&&   &&&&&&&&&&&&   &&&&&&&&&&&@  &&&&&&&&&&&&   &&&&&&&&&&&   &&&&&&&&&&&
&&&       &&&  &&        &&&  &&            &&&                  &&&&    &&
&&&&&&&&&&&&@  &&&&&&&&&&&&   &&&&&&&&&&&   &&&&&&&&&&&       &&&&       &&&&&&&&&&
&&&        &&  &&   &&&&      &&            &&&             &&&&         &&
&&&&&&&&&&&&&  &&     &&&&@   &&&&&&&&&&&@  &&&&&&&&&&&&  @&&&&&&&&&&&   &&&&&&&&&&&

"""

ASCIIART_STYLE = "white"


CHEATSHEET = f"""
Airflow Breeze CHEATSHEET
Adding breeze to your path:
   When you exit the environment, you can add sources of Airflow to the path - you can
   run breeze or the scripts above from any directory by calling 'breeze' commands directly

   \'{str(get_airflow_sources_root())}\' is exported into PATH

    Port forwarding:
      Ports are forwarded to the running docker containers for webserver and database
        * {SSH_PORT} -> forwarded to Airflow ssh server -> airflow:22
        * {WEBSERVER_HOST_PORT} -> forwarded to Airflow webserver -> airflow:8080
        * {FLOWER_HOST_PORT} -> forwarded to Flower dashboard -> airflow:5555
        * {POSTGRES_HOST_PORT} -> forwarded to Postgres database -> postgres:5432
        * {MYSQL_HOST_PORT} -> forwarded to MySQL database  -> mysql:3306
        * {MSSQL_HOST_PORT} -> forwarded to MSSQL database  -> mssql:1443
        * {REDIS_HOST_PORT} -> forwarded to Redis broker -> redis:6379
      Here are links to those services that you can use on host:"
        * ssh connection for remote debugging: ssh -p {SSH_PORT} airflow@127.0.0.1 pw: airflow"
        * Webserver: http://127.0.0.1:{WEBSERVER_HOST_PORT}"
        * Flower:    http://127.0.0.1:{FLOWER_HOST_PORT}"
        * Postgres:  jdbc:postgresql://127.0.0.1:{POSTGRES_HOST_PORT}/airflow?user=postgres&password=airflow"
        * Mysql:     jdbc:mysql://127.0.0.1:{MYSQL_HOST_PORT}/airflow?user=root"
        * Redis:     redis://127.0.0.1:{REDIS_HOST_PORT}/0"

"""

CHEATSHEET_STYLE = "white"
