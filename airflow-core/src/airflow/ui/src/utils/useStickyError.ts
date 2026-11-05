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
import { useRef } from "react";

/**
 * The latest failure, held onto while another attempt is in flight.
 *
 * React Query clears the error and returns a query to pending whenever it starts a fetch and has
 * no data to show meanwhile (`fetchState` in `@tanstack/query-core`). A request that never
 * succeeds has no data, so every refetch wipes the reason it failed and the query looks untried
 * again until the next attempt fails. Nothing has to be polling for that to happen: a query with
 * no data is always stale, so any component mounting a new observer on the same key refetches it.
 *
 * A view reading the error alone therefore flips between the failure and its loading state for as
 * long as the page is open. It reads the failure from here instead, and the next success clears it.
 *
 * `subject` names what is being fetched. Route elements stay mounted when only a param changes, so
 * without it the failure of one run or task would be shown against the next while that one loads.
 */
export const useStickyError = (error: unknown, hasData: boolean, subject: string): unknown => {
  const held = useRef<{ error: unknown; subject: string }>({ error: undefined, subject });

  if (held.current.subject !== subject) {
    held.current = { error: undefined, subject };
  }

  if (hasData) {
    held.current.error = undefined;
  } else if (Boolean(error)) {
    held.current.error = error;
  }

  return Boolean(error) ? error : held.current.error;
};
