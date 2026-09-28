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
import { TabEntity, TabName } from "src/constants/tab";

type RouteMatch = {
  readonly handle: unknown;
};

type TabRouteHandle = {
  readonly entity: TabEntity;
  readonly tab: TabName;
};

const tabEntities = new Set<string>(Object.values(TabEntity));
const tabNames = new Set<string>(Object.values(TabName));

const isTabRouteHandle = (handle: unknown): handle is TabRouteHandle =>
  typeof handle === "object" &&
  handle !== null &&
  "entity" in handle &&
  "tab" in handle &&
  typeof handle.entity === "string" &&
  tabEntities.has(handle.entity) &&
  typeof handle.tab === "string" &&
  tabNames.has(handle.tab);

export const getTabPath = (matches: Array<RouteMatch>, entities: Array<TabEntity> | TabEntity): string => {
  const targetEntities = new Set(Array.isArray(entities) ? entities : [entities]);
  const tabMatch = [...matches]
    .reverse()
    .find((match) => isTabRouteHandle(match.handle) && targetEntities.has(match.handle.entity));

  if (tabMatch?.handle === undefined || !isTabRouteHandle(tabMatch.handle)) {
    return "";
  }

  if (tabMatch.handle.tab === TabName.Overview) {
    return "";
  }

  return `/${tabMatch.handle.tab}`;
};
