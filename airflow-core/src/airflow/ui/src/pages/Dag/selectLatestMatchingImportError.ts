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
import type { ImportErrorResponse } from "openapi/requests/types.gen";

const sameBundle = (left: string | null | undefined, right: string | null | undefined): boolean =>
  (left ?? undefined) === (right ?? undefined);

/**
 * Pick the most recent import error row for this DAG's bundle + relative file path.
 * The list API may return other files when `filename_pattern` is a substring match.
 */
export const selectLatestMatchingImportError = (
  importErrors: Array<ImportErrorResponse> | undefined,
  relativeFileloc: string,
  bundleName: string | null | undefined,
): ImportErrorResponse | undefined => {
  if (importErrors === undefined || importErrors.length === 0) {
    return undefined;
  }

  const matching = importErrors.filter(
    (row) => row.filename === relativeFileloc && sameBundle(row.bundle_name, bundleName),
  );

  if (matching.length === 0) {
    return undefined;
  }

  return matching.reduce((latest, current) =>
    Date.parse(current.timestamp) > Date.parse(latest.timestamp) ? current : latest,
  );
};
