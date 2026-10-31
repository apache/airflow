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
export const BAR_WIDTH = 12;
export const BAR_GAP = { base: 2, lg: 4 };

export const MAX_RECENT_RUNS = 14;
const getFullRowWidth = (gap: number) => `${MAX_RECENT_RUNS * BAR_WIDTH + (MAX_RECENT_RUNS - 1) * gap}px`;

// A full row of bars, so a Dag card keeps its column widths whatever its run count.
export const RECENT_RUNS_WIDTH = { base: getFullRowWidth(BAR_GAP.base), lg: getFullRowWidth(BAR_GAP.lg) };
