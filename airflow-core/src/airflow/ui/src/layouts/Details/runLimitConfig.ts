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
import { createListCollection } from "@chakra-ui/react";

export const getWidthBasedConfig = (width: number, enableResponsiveOptions: boolean) => {
  const breakpoints = enableResponsiveOptions
    ? [
        { limit: 100, min: 1600, options: ["1", "5", "10", "25", "50"] }, // xl: extra large screens
        { limit: 25, min: 1024, options: ["1", "5", "10", "25"] }, // lg: large screens
        { limit: 10, min: 384, options: ["1", "5", "10"] }, // md: medium screens
        { limit: 5, min: 0, options: ["1", "5"] }, // sm: small screens and below
      ]
    : [{ limit: 5, min: 0, options: ["1", "5", "10", "25", "50"] }];

  const config = breakpoints.find(({ min }) => width >= min) ?? breakpoints[breakpoints.length - 1];

  return {
    displayRunOptions: createListCollection({
      items: config?.options.map((value) => ({ label: value, value })) ?? [],
    }),
    limit: config?.limit ?? 5,
  };
};

// The stored preference is never overwritten by this cap, so it comes back on a wider screen.
export const getEffectiveLimit = (limit: number, width: number, enableResponsiveOptions: boolean) =>
  enableResponsiveOptions ? Math.min(limit, getWidthBasedConfig(width, true).limit) : limit;
