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
import { useAutoRefresh } from "src/utils/query";

// A bundle row changes at most once per the bundle's `refresh_interval` (default 300s), so
// `[api] auto_refresh_interval` (default 3s, tuned for Grid/Graph run state) is far too fast here.
// Floor it rather than ignore it, so turning auto-refresh off still turns these pages off.
const MIN_REFETCH_INTERVAL_MS = 10_000;

/**
 * How often the Dag bundle pages should poll, or false when polling is off.
 *
 * `useAutoRefresh` with no dagId reduces to `[api] auto_refresh_interval`, where 0 is how an
 * operator turns auto-refresh off -- so it has to short-circuit before the floor.
 */
export const useDagBundleRefetchInterval = (): number | false => {
  const configuredInterval = useAutoRefresh({});

  return configuredInterval === false || configuredInterval === 0
    ? false
    : Math.max(configuredInterval, MIN_REFETCH_INTERVAL_MS);
};
