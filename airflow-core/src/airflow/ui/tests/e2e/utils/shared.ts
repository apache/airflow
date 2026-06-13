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

/**
 * Shared primitives for E2E API helpers.
 */
import type { APIRequestContext, Page } from "@playwright/test";
import { randomUUID } from "node:crypto";
import { testConfig } from "playwright.config";

export type RequestLike = APIRequestContext | Page;

export function getRequestContext(source: RequestLike): APIRequestContext {
  if ("request" in source) {
    return source.request;
  }

  return source;
}

export const { baseUrl } = testConfig.connection;

/** Generate a unique run ID: `{prefix}_{uuid8}`. */
export function uniqueRunId(prefix: string): string {
  return `${prefix}_${randomUUID().slice(0, 8)}`;
}
