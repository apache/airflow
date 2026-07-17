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
import { createBrowserRouter } from "react-router-dom";

import { Login } from "src/login/Login";

export const routerConfig = [
  {
    children: [
      {
        element: <Login />,
        path: "login",
      },
    ],
    path: "/",
  },
];

const baseHref = document.querySelector("head>base")?.getAttribute("href") ?? "";

// Resolve the scheme-relative URL from the base relative to the current URL
const baseUrl = new URL(baseHref, globalThis.location.origin);
const basename = new URL("auth", baseUrl).pathname;

export const router = createBrowserRouter(routerConfig, { basename });
