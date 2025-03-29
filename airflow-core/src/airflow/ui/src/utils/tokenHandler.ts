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
const getTokenFromCookies = (): string | undefined => {
  const cookies = document.cookie.split(";");

  for (const cookie of cookies) {
    const [name, token] = cookie.split("=");

    if (name?.trim() === "_token" && token !== undefined) {
      localStorage.setItem(TOKEN_STORAGE_KEY, token);
      document.cookie = "_token=; expires=Sat, 01 Jan 2000 00:00:00 UTC; path=/;";

      return token;
    }
  }

  return undefined;
};

export const tokenHandler = (config: InternalAxiosRequestConfig) => {
  const token = localStorage.getItem(TOKEN_STORAGE_KEY) ?? getTokenFromCookies();

  if (token !== undefined) {
    config.headers.Authorization = `Bearer ${token}`;
  }

  return config;
};

export const clearToken = () => {
  localStorage.removeItem(TOKEN_STORAGE_KEY);
};
