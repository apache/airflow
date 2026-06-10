/* eslint-disable react-refresh/only-export-components */

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
import { Text } from "@chakra-ui/react";

import type { JsonValue } from "openapi/requests";
import RenderedJsonField from "src/components/RenderedJsonField";

export const resolveStoreValue = (value: JsonValue): { json: object } | { text: string } => {
  if (typeof value === "object" && value !== null) {
    return { json: value };
  }

  if (typeof value === "string") {
    try {
      const parsed: unknown = JSON.parse(value);

      if (typeof parsed === "object" && parsed !== null) {
        return { json: parsed };
      }
    } catch {
      // Not JSON — fall through and render the raw string.
    }
  }

  return { text: String(value) };
};

export const StoreValueCell = ({ value }: { readonly value: JsonValue }) => {
  const resolved = resolveStoreValue(value);

  return "json" in resolved ? (
    <RenderedJsonField content={resolved.json} enableClipboard={false} />
  ) : (
    <Text>{resolved.text}</Text>
  );
};
