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

const ROUTE_MODULE_RELOAD_KEY = "airflow-route-module-reload";
const ROUTE_MODULE_RELOAD_ATTEMPTED = "attempted";
const ROUTE_MODULE_RELOAD_PENDING = "pending";

export const loadRouteModule = async <Module>(factory: () => Promise<Module>) => {
  try {
    const module = await factory();

    globalThis.sessionStorage.removeItem(ROUTE_MODULE_RELOAD_KEY);

    return module;
  } catch (error) {
    if (globalThis.sessionStorage.getItem(ROUTE_MODULE_RELOAD_KEY) !== ROUTE_MODULE_RELOAD_ATTEMPTED) {
      globalThis.sessionStorage.setItem(ROUTE_MODULE_RELOAD_KEY, ROUTE_MODULE_RELOAD_PENDING);
    }

    throw error;
  }
};

export const reloadAfterFailedRouteModuleLoad = () => {
  if (globalThis.sessionStorage.getItem(ROUTE_MODULE_RELOAD_KEY) === ROUTE_MODULE_RELOAD_PENDING) {
    globalThis.sessionStorage.setItem(ROUTE_MODULE_RELOAD_KEY, ROUTE_MODULE_RELOAD_ATTEMPTED);
    globalThis.location.reload();
  }
};
