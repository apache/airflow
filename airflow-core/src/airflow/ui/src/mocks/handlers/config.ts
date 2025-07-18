/*!
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
import { http, HttpResponse, type HttpHandler } from "msw";

export const handlers: Array<HttpHandler> = [
  http.get("/ui/config", () =>
    HttpResponse.json({
      auto_refresh_interval: 3,
      default_wrap: false,
      enable_swagger_ui: true,
      hide_paused_dags_by_default: false,
      instance_name: "Airflow",
      page_size: 15,
      require_confirmation_dag_change: false,
      test_connection: "Disabled",
    }),
  ),
];
