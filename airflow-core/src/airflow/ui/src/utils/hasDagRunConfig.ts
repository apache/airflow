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

// A Dag run that was triggered without a config comes back from the API as an empty object `{}`,
// not null/undefined, so a presence check has to treat `{}` as "no config supplied" too.
export const hasDagRunConfig = (
  conf: Record<string, unknown> | null | undefined,
): conf is Record<string, unknown> => conf !== null && conf !== undefined && Object.keys(conf).length > 0;
