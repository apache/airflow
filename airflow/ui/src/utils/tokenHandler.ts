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
import type { InternalAxiosRequestConfig } from "axios";

export const TOKEN_STORAGE_KEY = "token";
export const TOKEN_QUERY_PARAM_NAME = "token";

export const tokenHandler = (config: InternalAxiosRequestConfig) => {
  const searchParams = new URLSearchParams(globalThis.location.search);

  const tokenUrl = searchParams.get(TOKEN_QUERY_PARAM_NAME);

  let token: string | null;

  if (tokenUrl === null) {
    token = localStorage.getItem(TOKEN_STORAGE_KEY);
  } else {
    localStorage.setItem(TOKEN_QUERY_PARAM_NAME, tokenUrl);
    searchParams.delete(TOKEN_QUERY_PARAM_NAME);
    globalThis.location.search = searchParams.toString();
    token = tokenUrl;
  }

  if (token !== null) {
    config.headers.Authorization = `Bearer ${token}`;
  }

  return config;
};
