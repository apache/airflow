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
import type { DAGResponse, ExternalViewResponse, ReactAppResponse } from "openapi/requests/types.gen";

type PluginView = ExternalViewResponse | ReactAppResponse;

const isDefined = <T>(value: T | null | undefined): value is T => value !== undefined && value !== null;

// Translate a shell-style glob into an anchored RegExp. Only `*` and `?` are
// special — note this is shell-glob syntax, NOT the SQL-LIKE (`%`/`_`) syntax
// the platform's `dag_id_pattern` query params use, since this match runs
// client-side. We avoid pulling in a glob dependency as the patterns are trivial.
const globToRegExp = (pattern: string): RegExp => {
  const escaped = pattern.replaceAll(/[.+^${}()|[\]\\]/gu, "\\$&");
  const translated = escaped.replaceAll("*", ".*").replaceAll("?", ".");

  return new RegExp(`^${translated}$`, "u");
};

/**
 * Decide whether a plugin view's Dag-scoped tab should be shown for the current Dag.
 *
 * Filters (`dag_tags`, `dag_ids`, `dag_id_pattern`) are combined with OR: the tab
 * is shown if the Dag matches at least one configured filter. When no filter is
 * set the tab is shown on every Dag (backward compatible), and when the Dag is not
 * available (e.g. nav/base/dashboard destinations) filters are ignored.
 */
export const matchesDagFilter = (view: PluginView, dag: DAGResponse | undefined): boolean => {
  const { dag_id_pattern: dagIdPattern, dag_ids: dagIds, dag_tags: dagTags } = view;
  const hasTags = isDefined(dagTags);
  const hasIds = isDefined(dagIds);
  const hasPattern = isDefined(dagIdPattern);

  if ((!hasTags && !hasIds && !hasPattern) || dag === undefined) {
    return true;
  }

  if (hasTags && dag.tags.some((tag) => dagTags.includes(tag.name))) {
    return true;
  }

  if (hasIds && dagIds.includes(dag.dag_id)) {
    return true;
  }

  if (hasPattern && globToRegExp(dagIdPattern).test(dag.dag_id)) {
    return true;
  }

  return false;
};
