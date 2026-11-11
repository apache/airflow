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
import type { TFunction } from "i18next";

import type { DAGWarningResponse, ImportErrorResponse } from "openapi/requests/types.gen";

type DagStatus = {
  /** Stated as one more warning, for a Dag that is no longer in its file. */
  deactivatedMessage?: string;
  /** The page's own load failure, which the Dag's problems are listed beside. */
  error?: unknown;
  importError?: ImportErrorResponse;
  warnings: Array<DAGWarningResponse>;
};

export type DagStatusSummary = {
  /** Names the worst problem alone: the chip has room for one thing. */
  chipLabel: string;
  /** Names everything the modal lists. */
  heading: string;
  severity: "error" | "warning";
  warningCount: number;
};

/**
 * One reading of everything wrong with a Dag. The chip and the modal that opens from it both
 * describe the same set, so they derive it here rather than each walking the severity order
 * themselves and disagreeing the moment one of them changes.
 */
export const summarizeDagStatus = (
  translate: TFunction,
  { deactivatedMessage, error, importError, warnings }: DagStatus,
): DagStatusSummary => {
  const hasError = importError !== undefined || Boolean(error);
  const warningCount = warnings.length + Number(deactivatedMessage !== undefined);
  const errorLabel =
    importError === undefined
      ? translate("components:dagWarnings.error_one")
      : translate("dashboard:importErrors.dagImportError", { count: 1 });
  const warningLabel = translate("components:dagWarnings.warning", { count: warningCount });
  let heading = "";

  if (hasError) {
    heading =
      warningCount > 0
        ? translate("components:dagWarnings.errorAndWarning", { warningText: warningLabel })
        : errorLabel;
  } else if (warningCount > 0) {
    heading = warningLabel;
  }

  return {
    // A deactivation speaks for itself on the chip; in the list it is one warning among others.
    chipLabel: hasError ? errorLabel : (deactivatedMessage ?? warningLabel),
    heading,
    severity: hasError ? "error" : "warning",
    warningCount,
  };
};
