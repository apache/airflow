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
import { usePluginAppliesToContext } from "src/hooks/usePluginAppliesToContext";
import {
  hasAppliesToCriteria,
  isAppliesToPending,
  matchesAppliesTo,
  type PluginView,
} from "src/utils/pluginAppliesTo";

/**
 * Filter plugin views to those that belong on the current route: matching `destination` and
 * admitted by their `applies_to` scoping. The Dag/task context backing `applies_to` is only
 * resolved when some matching view is actually scoped, so unscoped pages issue no extra
 * requests. Views are withheld until that context resolves, avoiding a first-paint flicker.
 */
export const useScopedPluginViews = <View extends PluginView>(
  views: Array<View>,
  destination: string,
): Array<View> => {
  const context = usePluginAppliesToContext(
    views.some((view) => view.destination === destination && hasAppliesToCriteria(view)),
  );

  return views.filter(
    (view) =>
      view.destination === destination &&
      !isAppliesToPending(view, context) &&
      matchesAppliesTo(view, context),
  );
};
